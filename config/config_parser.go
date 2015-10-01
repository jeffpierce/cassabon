package config

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
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
		DataStoreChanLen  int // Length of the DataStore channel
		IndexStoreChanLen int // Length of the Indexer channel
		GopherChanLen     int // Length of the StatGopher channel
	}
	Carbon struct {
		Listen     string // ip:port on which to listen for Carbon stats
		Protocol   string // "tcp", "udp" or "both" are acceptable
		Parameters struct {
			TCPTimeout int
			UDPTimeout int
		}
		Peers []string // All servers in the Cassabon array, as "ip:port"
	}
	API struct {
		Listen          string // HTTP API listens on this address:port
		HealthCheckFile string // Location of healthcheck file.
	}
	Cassandra struct {
		Hosts []string // List of hostnames or IP addresses of Cassandra ring
		Port  string   // Cassandra port
	}
	Redis struct {
		Index RedisSettings // Settings for Redis Index
		Queue RedisSettings // Settings for Redis Queue
	}
	Rollups map[string]RollupSettings // Map of regex and rollups
}

// Definition of each rollup
type RollupSettings struct {
	Retention   []string
	Aggregation string
}

// Redis struct for redis connection information
type RedisSettings struct {
	Sentinel bool     // True if sentinel, false if standalone.
	Addr     []string // List of addresses in host:port format
	DB       int64    // Redis DB number for the index.
	Pwd      string   // Password for Redis.
	Master   string   // Master config name for sentinel settings.
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
// First time through we have no logger, so we can't log warnings or errors. Panic instead.
func ReadConfigurationFile(configFile string, haveLogger bool) error {

	// Read the configuration file.
	yamlConfig, err := ioutil.ReadFile(configFile)
	if err != nil {
		if haveLogger {
			return err
		} else {
			panic(err)
		}
	}

	// Unmarshal config file contents into raw config struct.
	err = yaml.Unmarshal(yamlConfig, &rawCassabonConfig)
	if err != nil {
		if haveLogger {
			return err
		} else {
			panic(err)
		}
	}
	return nil
}

// LoadStartupValues populates the global config object with values that are used only once.
func LoadStartupValues() {

	// Copy in the logging configuration.
	G.Log.Logdir = rawCassabonConfig.Logging.Logdir
	G.Log.Loglevel = rawCassabonConfig.Logging.Loglevel

	// Copy in the statsd configuration.
	G.Statsd = rawCassabonConfig.Statsd

	// Copy in and sanitize the channel lengths.
	G.Channels.DataStoreChanLen = rawCassabonConfig.Channels.DataStoreChanLen
	if G.Channels.DataStoreChanLen < 10 {
		G.Channels.DataStoreChanLen = 10
	}
	if G.Channels.DataStoreChanLen > 1000 {
		G.Channels.DataStoreChanLen = 1000
	}
	G.Channels.IndexStoreChanLen = rawCassabonConfig.Channels.IndexStoreChanLen
	if G.Channels.IndexStoreChanLen < 10 {
		G.Channels.IndexStoreChanLen = 10
	}
	if G.Channels.IndexStoreChanLen > 1000 {
		G.Channels.IndexStoreChanLen = 1000
	}
	G.Channels.GopherChanLen = rawCassabonConfig.Channels.GopherChanLen
	if G.Channels.GopherChanLen < 10 {
		G.Channels.GopherChanLen = 10
	}
	if G.Channels.GopherChanLen > 1000 {
		G.Channels.GopherChanLen = 1000
	}
}

// ValidatePeerList ensures addresses are valid, and that the local address is in the peer list.
func ValidatePeerList(localHostPort string, peers []string) error {

	for _, v := range peers {
		if _, err := net.ResolveTCPAddr("tcp4", v); err != nil {
			return fmt.Errorf("Invalid host:port \"%s\" in peer list: %v", v, err)
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

	// Ensure a canonical order for Cassabon peers, as we will allocate
	// metrics paths to a peer by indexing into this array.
	sort.Strings(G.Carbon.Peers)

	// Ensure addresses are valid, and that the local address:port is in the peer list.
	if err := ValidatePeerList(G.Carbon.Listen, G.Carbon.Peers); err != nil {
		G.Log.System.LogFatal(err.Error())
		os.Exit(1)
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

	// Copy in the Cassandra database connection values.
	G.Cassandra = rawCassabonConfig.Cassandra

	// Copy in the Redis database connection values.
	G.Redis.Index = rawCassabonConfig.Redis.Index
	G.Redis.Queue = rawCassabonConfig.Redis.Queue
}

// LoadRollups populates the global config object with the rollup definitions,
// which should only happen when there are no accumulated stats.
func LoadRollups() {

	// Validate, copy in and normalize the rollup definitions.
	G.RollupPriority = make([]string, 0, len(rawCassabonConfig.Rollups))
	G.Rollup = make(map[string]RollupDef)

	var method RollupMethod
	var window, retention time.Duration
	var err error
	var intRetention int64
	var rd *RollupDef
	reDuration := regexp.MustCompile("([0-9]+)([a-z])")

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
		default:
			G.Log.System.LogWarn("Invalid aggregation method for \"%s\": %s", expression, v.Aggregation)
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
				G.Log.System.LogWarn("Malformed regular expression for \"%s\": %v", expression, err)
				continue
			}
		}

		// Parse and validate each window:retention pair.
		for _, s := range v.Retention {

			// Split the value on the colon between the parts.
			couplet := strings.Split(s, ":")
			if len(couplet) != 2 {
				G.Log.System.LogWarn("Malformed definition for \"%s\": %s", expression, s)
				continue
			}

			// Convert the window to a time.Duration.
			if window, err = time.ParseDuration(couplet[0]); err != nil {
				G.Log.System.LogWarn("Malformed window for \"%s\": %s %s", expression, s, couplet[0])
				continue
			}

			// Don't permit windows shorter than 1 second.
			if window < time.Second {
				G.Log.System.LogWarn("Duration less than minimum 1 second for \"%s\": %v", expression, window)
				continue
			}

			// Convert the retention to a time.Duration (max: 720 years).
			// ParseDuration doesn't handle anything longer than hours, so do it manually.
			matches := reDuration.FindStringSubmatch(couplet[1]) // "1d" -> [ 1d 1 d ]
			if len(matches) != 3 {
				G.Log.System.LogWarn("Malformed retention for \"%s\": %s %s", expression, s, couplet[1])
				continue
			}
			if intRetention, err = strconv.ParseInt(matches[1], 10, 64); err != nil {
				G.Log.System.LogWarn("Malformed retention for \"%s\": %s %s", expression, s, couplet[1])
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
				continue
			}

			// Append to the rollups for this expression.
			rd.Windows = append(rd.Windows, RollupWindow{window, retention})
		}

		// If any of the rollup window definitions were valid, add expression to the list.
		if len(rd.Windows) > 0 {

			// Sort windows into ascending duration order, and validate.
			sort.Sort(ByWindow(rd.Windows))
			shortestDuration := rd.Windows[0].Window
			allExactMultiples := true
			for i, v := range rd.Windows {
				if i > 0 {
					remainder := v.Window % shortestDuration
					if remainder != 0 {
						G.Log.System.LogWarn(
							"Next duration is not a multiple for \"%s\": %v %% %v remainder is %v",
							expression, v.Window, shortestDuration, remainder)
						allExactMultiples = false
					}
				}
			}

			// If all durations are exact multiples of the shortest duration, save this expression.
			if allExactMultiples {
				G.Rollup[expression] = *rd
				G.RollupPriority = append(G.RollupPriority, expression)
			} else {
				G.Log.System.LogWarn("Rollup expression rejected due to previous errors: \"%s\"", expression)
			}
		}
	}

	// Sort the path expressions into priority order.
	sort.Sort(ByPriority(G.RollupPriority))
}
