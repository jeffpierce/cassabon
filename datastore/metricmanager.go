package datastore

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/logging"
	"github.com/jeffpierce/cassabon/middleware"
)

// rollup contains the accumulated metrics data for a path.
type rollup struct {
	expr  string    // The text form of the path expression, to locate the definition
	count []uint64  // The number of data points accumulated (for averaging)
	value []float64 // One rollup per window definition
}

// runlist contains the paths to be written for an expression, and when to write the rollups.
type runlist struct {
	nextWriteTime []time.Time        // The next write time for each rollup bucket
	path          map[string]*rollup // The rollup data for each path matched by the expression
}

// MetricResponse defines the response structure that will be converted into JSON before being returned.
type MetricResponse struct {
	From   int64                    `json:"from"`
	To     int64                    `json:"to"`
	Step   int64                    `json:"step"`
	Series map[string][]interface{} `json:"series"`
}

type MetricManager struct {

	// Wait Group for managing orderly reloads and termination.
	wg *sync.WaitGroup

	// The writer must finish last of all, so it gets its own signalling channel and wait group.
	writerWG     sync.WaitGroup
	writerOnExit chan struct{}

	// Rollup configuration.
	// Note: Does not reload on SIGHUP.
	rollupPriority []string                    // First matched expression wins
	rollup         map[string]config.RollupDef // Rollup processing definitions by path expression

	// Timer management.
	setTimeout chan time.Duration // Write a duration to this to get a notification on timeout channel
	timeout    chan struct{}      // Timeout notifications arrive on this channel

	// Database connection.
	dbClient *gocql.Session

	// Channel for async processing of Cassandra batches.
	insert chan *gocql.Batch

	// Rollup data.
	byPath map[string]*rollup  // Stats, by path, for rollup accumulation
	byExpr map[string]*runlist // Stats, by path within expression, for rollup processing
}

func (mm *MetricManager) Init(bootstrap bool, im IndexManager) {

	// Copy in the configuration (requires hard restart to refresh).
	mm.rollupPriority = config.G.RollupPriority
	mm.rollup = config.G.Rollup

	// Initialize private objects.
	mm.setTimeout = make(chan time.Duration, 0)
	mm.timeout = make(chan struct{}, 1)
	mm.insert = make(chan *gocql.Batch, 5000)

	// Perform first-time initialization of rollup data accumulation structures.
	mm.resetRollupData()

	// Reinitialize maps from ES, if they exist.
	if !bootstrap {
		leafnodes := im.getAllLeafNodes()
		for _, node := range leafnodes {
			mm.addToMaps(node)
		}
	}
}

func (mm *MetricManager) Start(wg *sync.WaitGroup) {

	// Start the persistent goroutines.
	mm.wg = wg

	mm.writerOnExit = make(chan struct{}, 1)
	mm.writerWG.Add(1)
	go mm.writer()

	mm.wg.Add(2)
	go mm.timer()
	go mm.run()

	// Kick off the timer.
	mm.setTimeout <- time.Second
}

func (mm *MetricManager) resetRollupData() {

	// Initialize rollup data structures.
	mm.byPath = make(map[string]*rollup)
	mm.byExpr = make(map[string]*runlist)
	baseTime := time.Now()
	for expr, rollupdef := range mm.rollup {
		// For each expression, provide a place to record all the paths that it matches.
		rl := new(runlist)
		rl.nextWriteTime = make([]time.Time, len(rollupdef.Windows))
		rl.path = make(map[string]*rollup)
		// Establish the next time boundary on which each write will take place.
		for i, v := range rollupdef.Windows {
			rl.nextWriteTime[i] = nextTimeBoundary(baseTime, v.Window)
		}
		mm.byExpr[expr] = rl
	}
}

// populateSchema ensures that all necessary Cassandra setup has been completed.
func (mm *MetricManager) populateSchema() {

	// Create the keyspace if it does not exist.
	if _, err := mm.dbClient.KeyspaceMetadata(config.G.Cassandra.Keyspace); err != nil {
		// Note: "USE <keyspace>" isn't allowed, and conn.UseKeyspace() isn't sticky.
		config.G.Log.System.LogInfo("Keyspace not found: %s", err.Error())
		var options string
		if len(config.G.Cassandra.CreateOpts) > 0 {
			options = "," + config.G.Cassandra.CreateOpts
		}
		query := fmt.Sprintf(
			"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'%s'%s}",
			config.G.Cassandra.Keyspace, config.G.Cassandra.Strategy, options)
		config.G.Log.System.LogDebug(query)
		if err := mm.dbClient.Query(query).Exec(); err != nil {
			config.G.Log.System.LogFatal("Could not create keyspace: %s", err.Error())
		}
		config.G.Log.System.LogInfo("Keyspace %q created", config.G.Cassandra.Keyspace)
	}

	// Create tables if they do not exist
	ksmd, _ := mm.dbClient.KeyspaceMetadata(config.G.Cassandra.Keyspace)
	for _, table := range config.G.RollupTables {
		if ksmd != nil {
			if _, found := ksmd.Tables[table]; found {
				continue
			}
		}
		var ttlfloat float64
		ttl := strings.Split(table, "_")[1]
		ttlfloat, _ = strconv.ParseFloat(ttl, 64)
		query := fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s.%s
                (path text, time timestamp, stat double, PRIMARY KEY (path, time))
            WITH COMPACT STORAGE
                AND CLUSTERING ORDER BY (time ASC)
                AND compaction = {'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy'}
                AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                AND dclocal_read_repair_chance = 0.1
                AND default_time_to_live = %v
                AND gc_grace_seconds = 864000
                AND memtable_flush_period_in_ms = 0
                AND read_repair_chance = 0.0
                AND speculative_retry = '99.0PERCENTILE';`,
			config.G.Cassandra.Keyspace, table, int(ttlfloat*1.1))

		config.G.Log.System.LogDebug(query)
		config.G.Log.System.LogInfo("Creating table %q", table)

		if err := mm.dbClient.Query(query).Exec(); err != nil {
			config.G.Log.System.LogFatal("Table %q creation failed: %s", table, err.Error())
		}
	}
}

func (mm *MetricManager) writer() {

	// We associate a number of retries with each Cassandra batch we receive.
	type queueEntry struct {
		tries int
		batch *gocql.Batch
	}

	// The queue for the batches we receive on the insert channel.
	var queue []queueEntry
	const numberOfRetries = 5

	var readAllChanneleEntries = func() {
		checkForMore := true
		for checkForMore {
			select {
			case batch := <-mm.insert:
				queue = append(queue, queueEntry{numberOfRetries, batch})
			default:
				checkForMore = false
			}
		}
	}

	var writeAllQueueEntries = func() {
		for len(queue) > 0 {
			qe := queue[0]
			queue = queue[1:]
			writeCount := qe.batch.Size()
			if err := mm.dbClient.ExecuteBatch(qe.batch); err != nil {
				config.G.Log.System.LogWarn("MetricManager::writer retrying write: %s", err.Error())
				logging.Statsd.Client.Inc("metricmgr.db.retry", 1, 1.0)
				qe.tries--
				if qe.tries > 0 {
					queue = append(queue, qe) // Stick it back in the queue
				}
				break // On errors, wait for the next timeout before retrying
			} else {
				config.G.Log.System.LogDebug("MetricManager::writer wrote batch. Remaining: %d", len(queue))
				logging.Statsd.Client.Inc("metricmgr.db.insert", int64(writeCount), 1.0)
			}
			// Drain the channel after each write, so it can't fill up.
			readAllChanneleEntries()
		}
	}

	for {
		select {
		case <-mm.writerOnExit:
			config.G.Log.System.LogDebug("MetricManager::writer received QUIT message")
			readAllChanneleEntries()
			writeAllQueueEntries()
			mm.writerWG.Done()
			return
		case batch := <-mm.insert:
			queue = append(queue, queueEntry{numberOfRetries, batch})
		case <-time.After(time.Second):
			writeAllQueueEntries()
		}
	}
}

func (mm *MetricManager) run() {

	defer config.G.OnPanic()

	// Open connection to the Cassandra database here, so we can defer the close.
	var err error
	config.G.Log.System.LogDebug("MetricManager initializing Cassandra client")
	mm.dbClient, err = middleware.CassandraSession(
		config.G.Cassandra.Hosts,
		config.G.Cassandra.Port,
		"",
	)
	if err != nil {
		// Without Cassandra client we can't do our job, so log, whine, and crash.
		config.G.Log.System.LogFatal("MetricManager unable to connect to Cassandra at %v, port %s: %s",
			config.G.Cassandra.Hosts, config.G.Cassandra.Port, err.Error())
	}

	defer mm.dbClient.Close()
	config.G.Log.System.LogDebug("MetricManager Cassandra client initialized")

	config.G.Log.System.LogDebug("MetricManager Cassandra Keyspace configuration starting...")
	mm.populateSchema()

	for {
		select {
		case <-config.G.OnPeerChangeReq:
			config.G.Log.System.LogDebug("MetricManager::run received PEERCHANGE message")
			mm.flush(true)
			mm.resetRollupData()
			config.G.OnPeerChangeRsp <- struct{}{} // Unblock sender
		case <-config.G.OnExit:
			config.G.Log.System.LogDebug("MetricManager::run received QUIT message")
			mm.flush(true)
			close(mm.writerOnExit)
			mm.writerWG.Wait()
			mm.wg.Done()
			return
		case metric := <-config.G.Channels.MetricStore:
			mm.accumulate(metric)
		case query := <-config.G.Channels.MetricRequest:
			go mm.query(query)
		case <-mm.timeout:
			mm.flush(false)
		}
	}
}

// timer sends a message on the "timeout" channel after the specified duration.
func (mm *MetricManager) timer() {
	for {
		select {
		case <-config.G.OnExit:
			config.G.Log.System.LogDebug("MetricManager::timer received QUIT message")
			mm.wg.Done()
			return
		case duration := <-mm.setTimeout:
			// Block in this state until a new entry is received.
			select {
			case <-config.G.OnExit:
				// Nothing; do handling above on next iteration.
			case <-time.After(duration):
				select {
				case mm.timeout <- struct{}{}:
					// Timeout sent.
				default:
					// Do not block.
				}
			}
		}
	}
}
