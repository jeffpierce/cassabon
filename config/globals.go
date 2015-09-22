package config

import (
	"regexp"
	"sync"
	"time"

	"github.com/jeffpierce/cassabon/logging"
)

// CarbonMetric is the canonical representation of Carbon data.
type CarbonMetric struct {
	Path      string  // Metric path
	Value     float64 // Metric Value
	Timestamp float64 // Epoch timestamp
}

type IndexQuery struct {
	Query   string      // Query
	Channel chan []byte // Channel to send response back on.
}

// RollupMethod is the way in which data points are combined in a time interval.
type RollupMethod int

// The valid rollup methods.
const (
	AVERAGE RollupMethod = iota
	MAX
	MIN
	SUM
)

// The string that represents the catchall rollup.
const CATCHALL_EXPRESSION = "default"

// RollupWindow is the definition of one rollup interval.
type RollupWindow struct {
	Window    time.Duration
	Retention time.Duration
}

// RollupDef is the definition of how to process a path expression.
type RollupDef struct {
	Method     RollupMethod
	Expression *regexp.Regexp
	Windows    []RollupWindow
}

// The globally accessible configuration and state object.
var G Globals

// Define Application Settings Structure
type Globals struct {

	// Goroutine management.
	// Note: Anything that accepts input should shut down first, so it should
	// monitor OnReload1. Everything else should monitor OnReload2.
	OnReload1   chan struct{}
	OnReload2   chan struct{}
	OnExit      chan struct{}
	OnReload1WG sync.WaitGroup
	OnReload2WG sync.WaitGroup
	OnExitWG    sync.WaitGroup

	// Channels for sending metrics between modules.
	Channels struct {
		DataStore         chan CarbonMetric
		DataStoreChanLen  int
		IndexStore        chan CarbonMetric
		IndexStoreChanLen int
		Gopher            chan IndexQuery
		GopherChanLen     int
	}

	// Integration into local filesystem and remote services.
	Log struct {
		Logdir   string // Log Directory
		Loglevel string // Level to log at.
		System   *logging.FileLogger
		Carbon   *logging.FileLogger
		API      *logging.FileLogger
	}
	Statsd    StatsdSettings
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
		Address         string // HTTP API listens on this address
		Port            string // HTTP API listens on this port
		HealthCheckFile string // Health check file.
	}
	Carbon struct {
		Address  string   // Address for Carbon Receiver to listen on
		Port     string   // Port for Carbon Receiver to listen on
		Protocol string   // "tcp", "udp" or "both" are acceptable
		Peers    []string // All servers in the Cassabon array, as "ip:port"
	}

	// Configuration of data point aggregations.
	RollupPriority []string             // First matched expression wins
	Rollup         map[string]RollupDef // Rollup processing definitions by path expression

	// Configuration of internal elements.
	Parameters struct {
		Listener struct {
			TCPTimeout int
			UDPTimeout int
		}
	}
}
