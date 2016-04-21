package config

import (
	"fmt"
	"io/ioutil"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

// Define Application Settings Structure
type CassabonConfig struct {
	Logging struct {
		Logdir   string // Log Directory
		Loglevel string // Level to log at.
	}
	Statsd   StatsdSettings
	Channels struct {
		MetricStoreChanLen   int // Length of the MetricStore channel
		MetricRequestChanLen int // Length of the MetricRequest channel
		IndexStoreChanLen    int // Length of the IndexStore channel
		IndexRequestChanLen  int // Length of the IndexRequest channel
	}
	Carbon struct {
		Listen     string // ip:port on which to listen for Carbon stats
		Protocol   string // "tcp", "udp" or "both" are acceptable
		Parameters struct {
			TCPTimeout int
			UDPTimeout int
		}
		Peers map[string]string // All servers in the Cassabon array, as "ip:port"
	}
	API struct {
		Listen          string // HTTP API listens on this address:port
		HealthCheckFile string // Location of healthcheck file.
		Timeouts        struct {
			GetIndex     uint
			DeleteIndex  uint
			GetMetric    uint
			DeleteMetric uint
		}
	}
	Cassandra     CassandraSettings
	ElasticSearch ElasticSearchSettings
	Rollups       map[string]RollupSettings // Map of regex and rollups
}

// Definition of each rollup
type RollupSettings struct {
	Retention   []string
	Aggregation string
}

// Cassandra connection and schema information
type CassandraSettings struct {
	Hosts      []string // List of hostnames or IP addresses of Cassandra ring
	Port       string   // Cassandra port
	Keyspace   string   // Name of the Cassandra keyspace
	Strategy   string   // Replication class of the keyspace
	CreateOpts string   // CQL text for the strategy options
	BatchSize  int      // The maximum number of insert statements to use in a batch
}

// ElasticSearchSettings struct for ES connection information
type ElasticSearchSettings struct {
	BaseURL   string // URL/Port of ElasticSearch REST API
	Index     string // ElasticSearch Index
	PutURL    string // URL for indexing paths
	SearchURL string // URL for searching paths.
	CountURL  string // URL for getting a count for the search path
	MapURL    string // URL for ElasticSearch mapping.
}

type StatsdSettings struct {
	Host   string // Host or IP address of statsd server
	Port   string // Port that statsd server listens on
	Events struct {
		ReceiveOK struct {
			Key        string
			SampleRate float32
		}
		ReceiveFail struct {
			Key        string
			SampleRate float32
		}
	}
}

// rawCassabonConfig is the decoded YAML from the configuration file.
var rawCassabonConfig *CassabonConfig

// ReadConfigurationFile reads the contents of the specified file from disk, and unmarshals it.
func ReadConfigurationFile(configFile string) error {

	// Read the configuration file.
	yamlConfig, err := ioutil.ReadFile(configFile)
	if err == nil {
		// Unmarshal config file contents into raw config struct.
		err = yaml.Unmarshal(yamlConfig, &rawCassabonConfig)
	}
	return err
}

// LoadStartupValues populates the global config object with values that are used only once.
func LoadStartupValues() {

	// Copy in the logging configuration.
	G.Log.Logdir = rawCassabonConfig.Logging.Logdir
	G.Log.Loglevel = rawCassabonConfig.Logging.Loglevel

	// Copy in the statsd configuration.
	G.Statsd = rawCassabonConfig.Statsd

	// Copy in the Cassandra database connection values.
	G.Cassandra = rawCassabonConfig.Cassandra
	if G.Cassandra.Keyspace == "" {
		G.Cassandra.Keyspace = "cassabon"
	}

	// Copy in the ElasticSearch connection values and generate URLs from BaseURL
	G.ElasticSearch = rawCassabonConfig.ElasticSearch
	if G.ElasticSearch.BaseURL == "" {
		panic("No ElasticSearch URL provided, aborting.")
	}
	if G.ElasticSearch.Index == "" {
		G.ElasticSearch.Index = "cassabon"
	}
	G.ElasticSearch.MapURL = strings.Join([]string{G.ElasticSearch.BaseURL, G.ElasticSearch.Index}, "/")
	G.ElasticSearch.PutURL = strings.Join([]string{G.ElasticSearch.MapURL, "path"}, "/")
	G.ElasticSearch.SearchURL = strings.Join([]string{G.ElasticSearch.PutURL, "_search"}, "/")
	G.ElasticSearch.CountURL = strings.Join([]string{G.ElasticSearch.SearchURL, "search_type=count"}, "?")

	// Copy in and sanitize the channel lengths.
	G.Channels.MetricStoreChanLen = rawCassabonConfig.Channels.MetricStoreChanLen
	if G.Channels.MetricStoreChanLen < 10 {
		G.Channels.MetricStoreChanLen = 10
	}
	if G.Channels.MetricStoreChanLen > 1000 {
		G.Channels.MetricStoreChanLen = 1000
	}
	G.Channels.MetricRequestChanLen = rawCassabonConfig.Channels.MetricRequestChanLen
	if G.Channels.MetricRequestChanLen < 10 {
		G.Channels.MetricRequestChanLen = 10
	}
	if G.Channels.MetricRequestChanLen > 1000 {
		G.Channels.MetricRequestChanLen = 1000
	}
	G.Channels.IndexStoreChanLen = rawCassabonConfig.Channels.IndexStoreChanLen
	if G.Channels.IndexStoreChanLen < 10 {
		G.Channels.IndexStoreChanLen = 10
	}
	if G.Channels.IndexStoreChanLen > 1000 {
		G.Channels.IndexStoreChanLen = 1000
	}
	G.Channels.IndexRequestChanLen = rawCassabonConfig.Channels.IndexRequestChanLen
	if G.Channels.IndexRequestChanLen < 10 {
		G.Channels.IndexRequestChanLen = 10
	}
	if G.Channels.IndexRequestChanLen > 1000 {
		G.Channels.IndexRequestChanLen = 1000
	}
}

// ValidatePeerList ensures addresses are valid, and that the local address is in the peer list.
func ValidatePeerList(localHostPort string, peers map[string]string) error {

	if len(peers) < 1 {
		return fmt.Errorf("No peers in peer list")
	}
	for _, v := range peers {
		if _, err := net.ResolveTCPAddr("tcp4", v); err != nil {
			return fmt.Errorf("Invalid host:port \"%s\" in peer list: %s", v, err.Error())
		}
		if v == localHostPort {
			localHostPort = ""
		}
	}
	if localHostPort != "" {
		return fmt.Errorf("Local host:port %s is not in peer list: %v", localHostPort, peers)
	}

	return nil
}

// LoadRefreshableValues populates the global config objectwith values that
// take effect again on receipt of a SIGHUP.
func LoadRefreshableValues() {

	// Copy in the logging level (can be changed while running).
	G.Log.Loglevel = rawCassabonConfig.Logging.Loglevel

	// If the listen address is "0.0.0.0", replace it with the address of
	// the first non-localhost, non-IPv6 address found for this machine.
	if lh, lp, err := net.SplitHostPort(rawCassabonConfig.Carbon.Listen); err == nil {
		// Is this "0.0.0.0"?
		if net.ParseIP(lh).IsUnspecified() {
			// Enumerate all local addresses in CIDR format.
			if addr, err := net.InterfaceAddrs(); err == nil {
				for _, v := range addr {
					if ip, _, err := net.ParseCIDR(v.String()); err == nil {
						// Skip loopback and IPv6 addresses.
						if !ip.IsLoopback() && !strings.Contains(ip.String(), ":") {
							rawCassabonConfig.Carbon.Listen = net.JoinHostPort(ip.String(), lp)
							// That's the one we want, we're done.
							break
						}
					}
				}
			}
		}
	}

	// Copy in the Carbon listener configuration.
	// NOTE: If any of these change, all rollup accumulators must be flushed.
	G.Carbon.Listen = rawCassabonConfig.Carbon.Listen
	G.Carbon.Protocol = rawCassabonConfig.Carbon.Protocol
	G.Carbon.Peers = rawCassabonConfig.Carbon.Peers

	// Ensure addresses are valid, and that the local address:port is in the peer list.
	if err := ValidatePeerList(G.Carbon.Listen, G.Carbon.Peers); err != nil {
		G.Log.System.LogFatal(err.Error())
	}

	// Copy in and sanitize the Carbon TCP listener timeout.
	G.Carbon.Parameters.TCPTimeout = rawCassabonConfig.Carbon.Parameters.TCPTimeout
	if G.Carbon.Parameters.TCPTimeout < 1 {
		G.Carbon.Parameters.TCPTimeout = 1
	}
	if G.Carbon.Parameters.TCPTimeout > 30 {
		G.Carbon.Parameters.TCPTimeout = 30
	}

	// Copy in and sanitize the Carbon UDP listener timeout.
	G.Carbon.Parameters.UDPTimeout = rawCassabonConfig.Carbon.Parameters.UDPTimeout
	if G.Carbon.Parameters.UDPTimeout < 1 {
		G.Carbon.Parameters.UDPTimeout = 1
	}
	if G.Carbon.Parameters.UDPTimeout > 30 {
		G.Carbon.Parameters.UDPTimeout = 30
	}

	// Copy in the API configuration values.
	G.API.Listen = rawCassabonConfig.API.Listen
	G.API.HealthCheckFile = rawCassabonConfig.API.HealthCheckFile
	if rawCassabonConfig.API.Timeouts.GetIndex < 1 {
		rawCassabonConfig.API.Timeouts.GetIndex = 1
	}
	if rawCassabonConfig.API.Timeouts.DeleteIndex < 1 {
		rawCassabonConfig.API.Timeouts.DeleteIndex = 1
	}
	if rawCassabonConfig.API.Timeouts.GetMetric < 1 {
		rawCassabonConfig.API.Timeouts.GetMetric = 1
	}
	if rawCassabonConfig.API.Timeouts.DeleteMetric < 1 {
		rawCassabonConfig.API.Timeouts.DeleteMetric = 1
	}
	G.API.Timeouts.GetIndex = time.Duration(time.Duration(rawCassabonConfig.API.Timeouts.GetIndex) * time.Second)
	G.API.Timeouts.DeleteIndex = time.Duration(time.Duration(rawCassabonConfig.API.Timeouts.DeleteIndex) * time.Second)
	G.API.Timeouts.GetMetric = time.Duration(time.Duration(rawCassabonConfig.API.Timeouts.GetMetric) * time.Second)
	G.API.Timeouts.DeleteMetric = time.Duration(time.Duration(rawCassabonConfig.API.Timeouts.DeleteMetric) * time.Second)
}

// LoadRollups populates the global config object with the rollup definitions,
// which should only happen when there are no accumulated stats.
func LoadRollups() bool {

	// Return success if there were no parse or configuration errors.
	var configIsClean bool = true

	// Validate, copy in and normalize the rollup definitions.
	G.RollupPriority = make([]string, 0, len(rawCassabonConfig.Rollups))
	G.Rollup = make(map[string]RollupDef)

	var method RollupMethod
	var window, retention time.Duration
	var err error
	var intRetention int64
	var rd *RollupDef
	reDuration := regexp.MustCompile("([0-9]+)([a-z])")

	var retentionToTablename = func(retention time.Duration) string {
		return fmt.Sprintf("rollup_%09d", uint64(retention.Seconds()))
	}

	// Inspect each rollup found in the configuration.
	// Note: YAML decode has already folded duplicate path expressions.
	for expression, v := range rawCassabonConfig.Rollups {

		// Validate and decode the aggregation method.
		switch strings.ToLower(v.Aggregation) {
		case "average":
			method = AVERAGE
		case "max":
			method = MAX
		case "min":
			method = MIN
		case "sum":
			method = SUM
		case "last":
			method = LAST
		default:
			G.Log.System.LogWarn("Invalid aggregation method for \"%s\": %s", expression, v.Aggregation)
			configIsClean = false
			continue
		}

		// Build up the rollup definitions for this regular expression.
		rd = new(RollupDef)
		rd.Method = method
		rd.Windows = make([]RollupWindow, 0)
		if expression != ROLLUP_CATCHALL {
			if re, err := regexp.Compile(expression); err == nil {
				rd.Expression = re
			} else {
				G.Log.System.LogWarn("Malformed regular expression for \"%s\": %s", expression, err.Error())
				configIsClean = false
				continue
			}
		}

		// Parse and validate each window:retention pair.
		for _, s := range v.Retention {

			// Split the value on the colon between the parts.
			couplet := strings.Split(s, ":")
			if len(couplet) != 2 {
				G.Log.System.LogWarn("Malformed definition for \"%s\": %s", expression, s)
				configIsClean = false
				continue
			}

			// Convert the window to a time.Duration.
			if window, err = time.ParseDuration(couplet[0]); err != nil {
				G.Log.System.LogWarn("Malformed window for \"%s\": %s %s", expression, s, couplet[0])
				configIsClean = false
				continue
			}

			// Don't permit windows shorter than 1 second.
			if window < time.Second {
				G.Log.System.LogWarn("Duration less than minimum 1 second for \"%s\": %v", expression, window)
				configIsClean = false
				continue
			}

			// Convert the retention to a time.Duration (max: 720 years).
			// ParseDuration doesn't handle anything longer than hours, so do it manually.
			matches := reDuration.FindStringSubmatch(couplet[1]) // "1d" -> [ 1d 1 d ]
			if len(matches) != 3 {
				G.Log.System.LogWarn("Malformed retention for \"%s\": %s %s", expression, s, couplet[1])
				configIsClean = false
				continue
			}
			if intRetention, err = strconv.ParseInt(matches[1], 10, 64); err != nil {
				G.Log.System.LogWarn("Malformed retention for \"%s\": %s %s", expression, s, couplet[1])
				configIsClean = false
				continue
			}
			switch matches[2] {
			case "m":
				retention = time.Minute * time.Duration(intRetention)
			case "h":
				retention = time.Hour * time.Duration(intRetention)
			case "d":
				retention = time.Hour * 24 * time.Duration(intRetention)
			case "w":
				retention = time.Hour * 24 * 7 * time.Duration(intRetention)
			case "y":
				retention = time.Hour * 24 * 365 * time.Duration(intRetention)
			default:
				G.Log.System.LogWarn("Malformed retention for \"%s\": %s %s", expression, s, couplet[1])
				configIsClean = false
				continue
			}

			// Record this table name in the master list of table names.
			table := retentionToTablename(retention)
			found := false
			for _, v := range G.RollupTables {
				if table == v {
					found = true
					break
				}
			}
			if !found {
				G.RollupTables = append(G.RollupTables, table)
			}

			// Append to the rollups for this expression.
			rd.Windows = append(rd.Windows, RollupWindow{window, retention, table})
		}

		// If any of the rollup window definitions were valid, add expression to the list.
		if len(rd.Windows) > 0 {

			// Sort windows into ascending duration order, and validate.
			sort.Sort(ByWindow(rd.Windows))
			shortestDuration := rd.Windows[0].Window
			expressionOK := true
			var tables = make(map[string]string)
			for i, v := range rd.Windows {
				if i > 0 {
					remainder := v.Window % shortestDuration
					if remainder != 0 {
						G.Log.System.LogWarn(
							"Next duration is not a multiple for \"%s\": %v %% %v remainder is %v",
							expression, v.Window, shortestDuration, remainder)
						expressionOK = false
					}
				}
				if _, found := tables[v.Table]; !found {
					tables[v.Table] = ""
				} else {
					G.Log.System.LogWarn(
						"Next retention is duplicate for \"%s\": %v %v table %s",
						expression, v.Window, v.Retention, v.Table)
					expressionOK = false
				}
			}

			// If all durations are exact multiples of the shortest duration, save this expression.
			if expressionOK {
				G.Rollup[expression] = *rd
				G.RollupPriority = append(G.RollupPriority, expression)
			} else {
				configIsClean = false
				G.Log.System.LogWarn("Rollup expression rejected due to previous errors: \"%s\"", expression)
			}
		}
	}

	// If no default has been defined (or it was rejected), create one.
	if _, found := G.Rollup[ROLLUP_CATCHALL]; !found {
		// Default rollup method is "average".
		rd = new(RollupDef)
		rd.Method = AVERAGE
		rd.Windows = make([]RollupWindow, 0)
		// 10s:1h
		retention := time.Hour
		table := retentionToTablename(retention)
		rd.Windows = append(rd.Windows, RollupWindow{time.Second * 10, retention, table})
		// 1m:30d
		retention = time.Hour * 24 * 30
		table = retentionToTablename(retention)
		rd.Windows = append(rd.Windows, RollupWindow{time.Minute, retention, table})
		// Append to rollup list.
		G.Rollup[ROLLUP_CATCHALL] = *rd
		G.RollupPriority = append(G.RollupPriority, ROLLUP_CATCHALL)
		G.Log.System.LogWarn("Default rollup missing or rejected, using \"10s:1h | 1m:30d | average\"")
	}

	// Sort the path expressions into priority order.
	sort.Sort(ByPriority(G.RollupPriority))

	// Sort the table names.
	sort.Strings(G.RollupTables)

	return configIsClean
}
