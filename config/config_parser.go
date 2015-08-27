package config

import (
	"io/ioutil"

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
	Rollups  map[string][]string // Map of regex and default rollups
	Channels struct {
		DataStoreChanLen int // Length of the DataStore channel
		IndexerChanLen   int // Length of the Indexer channel
		GopherChanLen    int // Length of the StatGopher channel
	}
	Parameters struct {
		Listener struct {
			TCPTimeout int
			UDPTimeout int
		}
		DataStore struct {
			MaxPendingMetrics int
			MaxFlushDelay     int
			TodoChanLen       int
		}
	}
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

// Get Rollup Settings
func parseConfig(configFile string) *CassabonConfig {

	// Load config file.
	// This happens before the logger is initialized because we also read in
	// logger configuration, causing a cycle. So, we panic on errors.
	yamlConfig, err := ioutil.ReadFile(configFile)

	if err != nil {
		panic(err)
	}

	// Initialize config struct
	var config *CassabonConfig

	// Unmarshal config file into config struct
	err = yaml.Unmarshal(yamlConfig, &config)

	if err != nil {
		panic(err)
	}

	// Send back config struct
	return config
}

func GetConfiguration(confFile string) {

	// Open, read, and parse the YAML.
	cnf := parseConfig(confFile)

	// Fill in arguments not provided on the command line.
	if G.Log.Logdir == "" {
		G.Log.Logdir = cnf.Logging.Logdir
	}
	if G.Log.Loglevel == "" {
		G.Log.Loglevel = cnf.Logging.Loglevel
	}

	// Copy in values sourced solely from the configuration file.
	G.Statsd = cnf.Statsd

	G.API.Address = cnf.API.Address
	G.API.Port = cnf.API.Port
	G.API.HealthCheckFile = cnf.API.HealthCheckFile

	G.Redis.Index = cnf.Redis.Index
	G.Redis.Queue = cnf.Redis.Queue

	G.Carbon.Address = cnf.Carbon.Address
	G.Carbon.Port = cnf.Carbon.Port
	G.Carbon.Protocol = cnf.Carbon.Protocol

	// Copy in and sanitize the channel lengths.
	G.Channels.DataStoreChanLen = cnf.Channels.DataStoreChanLen
	if G.Channels.DataStoreChanLen < 10 {
		G.Channels.DataStoreChanLen = 10
	}
	if G.Channels.DataStoreChanLen > 1000 {
		G.Channels.DataStoreChanLen = 1000
	}
	G.Channels.IndexerChanLen = cnf.Channels.IndexerChanLen
	if G.Channels.IndexerChanLen < 10 {
		G.Channels.IndexerChanLen = 10
	}
	if G.Channels.IndexerChanLen > 1000 {
		G.Channels.IndexerChanLen = 1000
	}
	G.Channels.GopherChanLen = cnf.Channels.GopherChanLen
	if G.Channels.GopherChanLen < 10 {
		G.Channels.GopherChanLen = 10
	}
	if G.Channels.GopherChanLen > 1000 {
		G.Channels.GopherChanLen = 1000
	}

	// Copy in and sanitize the Listener internal parameters.
	G.Parameters.Listener.TCPTimeout = cnf.Parameters.Listener.TCPTimeout
	if G.Parameters.Listener.TCPTimeout < 1 {
		G.Parameters.Listener.TCPTimeout = 1
	}
	if G.Parameters.Listener.TCPTimeout > 30 {
		G.Parameters.Listener.TCPTimeout = 30
	}
	G.Parameters.Listener.UDPTimeout = cnf.Parameters.Listener.UDPTimeout
	if G.Parameters.Listener.UDPTimeout < 1 {
		G.Parameters.Listener.UDPTimeout = 1
	}
	if G.Parameters.Listener.UDPTimeout > 30 {
		G.Parameters.Listener.UDPTimeout = 30
	}

	// Copy in and sanitize the DataStore internal parameters.
	G.Parameters.DataStore.MaxPendingMetrics = cnf.Parameters.DataStore.MaxPendingMetrics
	if G.Parameters.DataStore.MaxPendingMetrics < 1 {
		G.Parameters.DataStore.MaxPendingMetrics = 1
	}
	if G.Parameters.DataStore.MaxPendingMetrics > 500 {
		G.Parameters.DataStore.MaxPendingMetrics = 500
	}
	G.Parameters.DataStore.MaxFlushDelay = cnf.Parameters.DataStore.MaxFlushDelay
	if G.Parameters.DataStore.MaxFlushDelay < 1 {
		G.Parameters.DataStore.MaxFlushDelay = 1
	}
	if G.Parameters.DataStore.MaxFlushDelay > 30 {
		G.Parameters.DataStore.MaxFlushDelay = 30
	}
	G.Parameters.DataStore.TodoChanLen = cnf.Parameters.DataStore.TodoChanLen
	if G.Parameters.DataStore.TodoChanLen < 5 {
		G.Parameters.DataStore.TodoChanLen = 5
	}
	if G.Parameters.DataStore.TodoChanLen > 100 {
		G.Parameters.DataStore.TodoChanLen = 100
	}
}
