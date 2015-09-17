package config

import (
	"io/ioutil"
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
	Cassandra struct {
		Hosts []string // List of hostnames or IP addresses of Cassandra ring
		Port  int      // Cassandra port
	}
	API struct {
		Address         string // HTTP API listens on this address
		Port            string // HTTP API listens on this port
		HealthCheckFile string // Location of healthcheck file.
	}
	Redis struct {
		Index RedisSettings // Settings for Redis Index
		Queue RedisSettings // Settings for Redis Queue
	}
	Carbon struct {
		Address  string // Address for Carbon Receiver to listen on
		Port     string // Port for Carbon Receiver to listen on
		Protocol string // "tcp", "udp" or "both" are acceptable
	}
	Statsd   StatsdSettings
	Rollups  map[string]RollupSettings // Map of regex and rollups
	Channels struct {
		DataStoreChanLen  int // Length of the DataStore channel
		IndexStoreChanLen int // Length of the Indexer channel
		GopherChanLen     int // Length of the StatGopher channel
	}
	Parameters struct {
		Listener struct {
			TCPTimeout int
			UDPTimeout int
		}
	}
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

func ReadConfigurationFile(configFile string, haveLogger bool) error {

	// Load config file.
	// This happens before the logger is initialized because we also read in
	// logger configuration, causing a cycle. So, we panic on errors.
	yamlConfig, err := ioutil.ReadFile(configFile)
	if err != nil {
		if haveLogger {
			G.Log.System.LogError("Configuration not available: %v", err)
			return err
		} else {
			panic(err)
		}
	}

	// Unmarshal config file into config struct.
	err = yaml.Unmarshal(yamlConfig, &rawCassabonConfig)
	if err != nil {
		if haveLogger {
			G.Log.System.LogError("Configuration parse error: %v", err)
			return err
		} else {
			panic(err)
		}
	}
	return nil
}

func ParseStartupValues() {

	// Fill in arguments not provided on the command line.
	if G.Log.Logdir == "" {
		G.Log.Logdir = rawCassabonConfig.Logging.Logdir
	}

	// We need the log level at startup to initialize the loggers.
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

func ParseRefreshableValues() {

	// We can dynamically change the logging level.
	G.Log.Loglevel = rawCassabonConfig.Logging.Loglevel

	// Copy in the addresses of the services we offer.
	G.API.Address = rawCassabonConfig.API.Address
	G.API.Port = rawCassabonConfig.API.Port
	G.API.HealthCheckFile = rawCassabonConfig.API.HealthCheckFile

	G.Carbon.Address = rawCassabonConfig.Carbon.Address
	G.Carbon.Port = rawCassabonConfig.Carbon.Port
	G.Carbon.Protocol = rawCassabonConfig.Carbon.Protocol

	// Copy in the addresses of the services we use.
	G.Redis.Index = rawCassabonConfig.Redis.Index
	G.Redis.Queue = rawCassabonConfig.Redis.Queue

	// Copy in and sanitize the Listener internal parameters.
	G.Parameters.Listener.TCPTimeout = rawCassabonConfig.Parameters.Listener.TCPTimeout
	if G.Parameters.Listener.TCPTimeout < 1 {
		G.Parameters.Listener.TCPTimeout = 1
	}
	if G.Parameters.Listener.TCPTimeout > 30 {
		G.Parameters.Listener.TCPTimeout = 30
	}
	G.Parameters.Listener.UDPTimeout = rawCassabonConfig.Parameters.Listener.UDPTimeout
	if G.Parameters.Listener.UDPTimeout < 1 {
		G.Parameters.Listener.UDPTimeout = 1
	}
	if G.Parameters.Listener.UDPTimeout > 30 {
		G.Parameters.Listener.UDPTimeout = 30
	}

	// Validate, copy in and normalize the rollup definitions.
	G.RollupPriority = make([]string, 0, len(rawCassabonConfig.Rollups))
	G.Rollup = make(map[string]RollupDef)

	var method RollupMethod
	var window, retention time.Duration
	var err error
	var intRetention int64
	var rd *RollupDef
	re := regexp.MustCompile("([0-9]+)([a-z])")

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
		if expression != CATCHALL_EXPRESSION {
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
			matches := re.FindStringSubmatch(couplet[1]) // "1d" -> [ 1d 1 d ]
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

// ByWindow is used to sort retention definitions by window duration.
type ByWindow []RollupWindow

// Implementation of sort.Interface.
func (w ByWindow) Len() int {
	return len(w)
}
func (w ByWindow) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}
func (w ByWindow) Less(i, j int) bool {
	return w[i].Window < w[j].Window
}

// ByPriority is used to provide a consistent order for processing path expressions.
type ByPriority []string

// Implementation of sort.Interface.
func (p ByPriority) Len() int {
	return len(p)
}
func (p ByPriority) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p ByPriority) Less(i, j int) bool {

	// "default" is always last in priority.
	if p[i] == CATCHALL_EXPRESSION {
		return false
	}
	if p[j] == CATCHALL_EXPRESSION {
		return true
	}

	// Longer strings sort earlier.
	if len(p[i]) > len(p[j]) {
		return true
	} else if len(p[i]) < len(p[j]) {
		return false
	}

	// Same-length strings are ordered lexically.
	return p[i] < p[j]
}
