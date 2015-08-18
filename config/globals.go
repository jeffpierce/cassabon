package config

import (
	"sync"

	"github.com/jeffpierce/cassabon/logging"
)

// The globally accessible configuration and state object.
var G Globals

// Define Application Settings Structure
type Globals struct {

	// Goroutine management.
	Quit chan struct{}
	WG   sync.WaitGroup

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
		Host string // Hostname or IP address of Redis
		Port string // Redis port
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
