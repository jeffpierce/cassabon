package config

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/jeffpierce/cassabon/logging"
)

// CarbonMetric is the canonical representation of Carbon data.
type CarbonMetric struct {
	Path      string  // Metric path
	Value     float64 // Metric Value
	Timestamp float64 // Epoch timestamp
}

type APIQueryStatus int

const (
	AQS_OK APIQueryStatus = iota
	AQS_NOTFOUND
	AQS_BADREQUEST
	AQS_ERROR
)

type IndexQuery struct {
	Method  string                // The HTTP method from the request
	Query   string                // Query
	Channel chan APIQueryResponse // Channel to send response back on.
}

type MetricQuery struct {
	Method  string                // The HTTP method from the request
	Query   []string              // Query
	From    uint64                // Start of time window for metrics range
	To      uint64                // End of time window for metrics range
	Channel chan APIQueryResponse // Channel to send response back on.
}

type APIQueryResponse struct {
	Status  APIQueryStatus // Either AQS_OK, or one of the error codes
	Message string         // If Status != AQS_OK, a description of the error
	Payload []byte         // If Status == AQS_OK, a well-formed JSON payload
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

	// Channels for communicating between modules.
	Channels struct {
		MetricStore          chan CarbonMetric
		MetricStoreChanLen   int
		MetricRequest        chan MetricQuery
		MetricRequestChanLen int
		IndexStore           chan CarbonMetric
		IndexStoreChanLen    int
		IndexRequest         chan IndexQuery
		IndexRequestChanLen  int
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
