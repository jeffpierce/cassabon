package config

import (
	"sync"

	"github.com/jeffpierce/cassabon/logging"
)

// CarbonMetric is the canonical representation of Carbon data.
type CarbonMetric struct {
	Path      string  // Metric path
	Value     float64 // Metric Value
	Timestamp float64 // Epoch timestamp
}

// The globally accessible configuration and state object.
var G Globals

// Define Application Settings Structure
type Globals struct {

	// Goroutine management.
	Quit chan struct{}
	WG   sync.WaitGroup

	// Channel for sending metrics to the datastore.
	MetricsInput chan CarbonMetric

	// Integration into local filesystem and remote services.
	Log struct {
		Logdir   string // Log Directory
		Loglevel string // Level to log at.
		System   *logging.FileLogger
		Carbon   *logging.FileLogger
		API      *logging.FileLogger
	}
	Statsd struct {
		Host string // Host or IP address of statsd server
		Port string // Port that statsd server listens on
	}
	Cassandra struct {
		Hosts []string // List of hostnames or IP addresses of Cassandra ring
		Port  string   // Cassandra port
	}
	Redis struct {
		Index RedisSettings // Settings for Redis Index
		Queue RedisSettings // Settings for Redis Queue
	}

	// Configuration of the services offered to clients.
	API struct {
		Address string // HTTP API listens on this address
		Port    string // HTTP API listens on this port
	}
	Carbon struct {
		Address  string // Address for Carbon Receiver to listen on
		Port     string // Port for Carbon Receiver to listen on
		Protocol string // "tcp", "udp" or "both" are acceptable
	}

	// Configuration of internal elements.
	Rollups map[string][]string // Map of regex and default rollups
}
