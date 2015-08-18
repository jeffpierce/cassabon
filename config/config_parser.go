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
	Api struct {
		Address string // HTTP API listens on this address
		Port    int    // HTTP API listens on this port
	}
	Redis struct {
		Index RedisSettings // Settings for Redis Index
		Queue RedisSettings // Settings for Redis Queue
	}
	Carbon struct {
		Address  string // Address for Carbon Receiver to listen on
		Port     int    // Port for Carbon Receiver to listen on
		Protocol string // "tcp", "udp" or "both" are acceptable
	}
	Statsd struct {
		Host string // Host or IP address of statsd server
		Port int    // Port that statsd server listens on
	}
	Rollups map[string][]string // Map of regex and default rollups
}

// Redis struct for redis connection information
type RedisSettings struct {
	Sentinel bool     // True if sentinel, false if standalone.
	Addr     []string // List of addresses in host:port format
	DB       int64    // Redis DB number for the index.
}

// Get Rollup Settings
func ParseConfig(configFile string) *CassabonConfig {

	// Load config file
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
