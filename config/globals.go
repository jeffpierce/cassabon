package config

import (
	"fmt"
	"os"
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

type DataQueryStatus int

const (
	DQS_OK DataQueryStatus = iota
	DQS_NOTFOUND
	DQS_BADREQUEST
	DQS_ERROR
)

type DataQuery struct {
	Query   string                 // Query
	Channel chan DataQueryResponse // Channel to send response back on.
}

type DataQueryResponse struct {
	Status  DataQueryStatus // Either DQS_OK, or one of the error codes
	Message string          // If Status != DQS_OK, a description of the error
	Payload []byte          // If Status == DQS_OK, a well-formed JSON payload
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
const ROLLUP_CATCHALL = "default"

// RollupWindow is the definition of one rollup interval.
type RollupWindow struct {
	Window    time.Duration
	Retention time.Duration
	Table     string // The Cassandra table to which this window is written
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
	OnPeerChange    chan struct{}
	OnPeerChangeReq chan struct{}
	OnPeerChangeRsp chan struct{}
	OnReload1       chan struct{}
	OnReload2       chan struct{}
	OnExit          chan struct{}
	OnReload1WG     sync.WaitGroup
	OnReload2WG     sync.WaitGroup
	OnExitWG        sync.WaitGroup

	// Channels for communicating between modules.
	Channels struct {
		DataStore         chan CarbonMetric
		DataStoreChanLen  int
		DataFetch         chan DataQuery
		DataFetchChanLen  int
		IndexStore        chan CarbonMetric
		IndexStoreChanLen int
		IndexFetch        chan DataQuery
		IndexFetchChanLen int
	}

	// Logger configuration and runtime properties.
	Log struct {
		Logdir   string // Log Directory
		Loglevel string // Level to log at.
		System   *logging.FileLogger
		Carbon   *logging.FileLogger
		API      *logging.FileLogger
	}

	// Statsd configuration.
	Statsd StatsdSettings

	// Configuration of the Carbon protocol listener.
	Carbon struct {
		Listen     string // ip:port on which to listen for Carbon stats
		Protocol   string // "tcp", "udp" or "both" are acceptable
		Parameters struct {
			TCPTimeout int
			UDPTimeout int
		}
		Peers []string // All servers in the Cassabon array, as "ip:port"
	}

	// Configuration of the API.
	API struct {
		Listen          string // HTTP API listens on this address:port
		HealthCheckFile string // Health check file.
	}

	Cassandra CassandraSettings

	Redis RedisSettings

	// Configuration of data rollups.
	RollupPriority []string             // First matched expression wins
	Rollup         map[string]RollupDef // Rollup processing definitions by path expression
	RollupTables   []string             // The Cassandra table names derived from extant durations
}

func (g *Globals) OnPanic() {
	if err := recover(); err != nil {
		switch err.(type) {
		case logging.FatalError:
			// This is an error that we generated to terminate the program.
			e := err.(logging.FatalError)
			fmt.Fprintf(os.Stderr, "%s\n", e.Error())
			os.Exit(1) // Let OS know we aborted
		default:
			// This is an error due to a bug; print full details and terminate.
			// Note: panic() writes to stderr.
			panic(err)
		}
	}
}
