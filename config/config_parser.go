package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

// Define Application Settings Structure
type CassabonConfig struct {
	Logging struct {
		LogFiles struct {
			Api    string // Path to log API requests to
			Carbon string // Path to log Carbon-related items to
			System string // Path to log general times to
		}
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
	ElasticSearch struct {
		Host string // Hostname or IP address of ElasticSearch
		Port int    // ElasticSearch port
	}
	Carbon struct {
		Address  string // Address for Carbon Receiver to listen on
		Port     int    // Port for Carbon Receiver to listen on
		Protocol string // "tcp", "udp" or "both" are acceptable
	}
	Rollups map[string][]string // Map of regex and default rollups
}

// Get Rollup Settings
func ParseConfig(configFile string) CassabonConfig {
	yamlConfig, err := ioutil.Readfile(configFile)

	if err != nil {
		panic(err)
	}

	var config CassabonConfig

	err = yaml.Unmarshal(yamlConfig, &config)

	if err != nil {
		panic(err)
	}

	return config
}
