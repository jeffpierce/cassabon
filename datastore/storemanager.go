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

type StoreManager struct {

	// Wait Group for managing orderly reloads and termination.
	wg *sync.WaitGroup

	// Rollup configuration.
	// Note: Does not reload on SIGHUP.
	rollupPriority []string                    // First matched expression wins
	rollup         map[string]config.RollupDef // Rollup processing definitions by path expression

	// Timer management.
	setTimeout chan time.Duration // Write a duration to this to get a notification on timeout channel
	timeout    chan struct{}      // Timeout notifications arrive on this channel

	// Database connection.
	dbClient *gocql.Session

	// Rollup data.
	byPath map[string]*rollup  // Stats, by path, for rollup accumulation
	byExpr map[string]*runlist // Stats, by path within expression, for rollup processing
}

func (sm *StoreManager) Init() {

	// Copy in the configuration (requires hard restart to refresh).
	sm.rollupPriority = config.G.RollupPriority
	sm.rollup = config.G.Rollup

	// Initialize private objects.
	sm.setTimeout = make(chan time.Duration, 0)
	sm.timeout = make(chan struct{}, 1)
}

func (sm *StoreManager) Start(wg *sync.WaitGroup) {

	// Start the persistent goroutines.
	sm.wg = wg
	sm.wg.Add(2)
	go sm.timer()
	go sm.run()

	// Kick off the timer.
	sm.setTimeout <- time.Second
}

func (sm *StoreManager) resetRollupData() {

	// Initialize rollup data structures.
	sm.byPath = make(map[string]*rollup)
	sm.byExpr = make(map[string]*runlist)
	baseTime := time.Now()
	for expr, rollupdef := range sm.rollup {
		// For each expression, provide a place to record all the paths that it matches.
		rl := new(runlist)
		rl.nextWriteTime = make([]time.Time, len(rollupdef.Windows))
		rl.path = make(map[string]*rollup)
		// Establish the next time boundary on which each write will take place.
		for i, v := range rollupdef.Windows {
			rl.nextWriteTime[i] = nextTimeBoundary(baseTime, v.Window)
		}
		sm.byExpr[expr] = rl
	}
}

// populateSchema ensures that all necessary Cassandra setup has been completed.
func (sm *StoreManager) populateSchema() {

	// Create the keyspace if it does not exist.
	conn := sm.dbClient.Pool.Pick(sm.dbClient.Query(""))
	if err := conn.UseKeyspace(config.G.Cassandra.Keyspace); err != nil {
		// Note: "USE <keyspace>" isn't allowed, and conn.UseKeyspace() isn't sticky.
		config.G.Log.System.LogInfo("Keyspace not found: %s", err.Error())
		var options string
		if len(config.G.Cassandra.CreateOpts) > 0 {
			options = "," + config.G.Cassandra.CreateOpts
		}
		query := fmt.Sprintf(
			"CREATE KEYSPACE %s WITH replication = {'class':'%s'%s}",
			config.G.Cassandra.Keyspace, config.G.Cassandra.Strategy, options)
		config.G.Log.System.LogDebug(query)
		if err := sm.dbClient.Query(query).Exec(); err != nil {
			config.G.Log.System.LogFatal("Could not create keyspace: %s", err.Error())
		}
		config.G.Log.System.LogInfo("Keyspace %q created", config.G.Cassandra.Keyspace)
	}

	// Create tables if they do not exist
	ksmd, _ := sm.dbClient.KeyspaceMetadata(config.G.Cassandra.Keyspace)
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
                (path text, timestamp timestamp, stat double, PRIMARY KEY (path, timestamp))
            WITH COMPACT STORAGE
                AND CLUSTERING ORDER BY (timestamp ASC)
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

		if err := sm.dbClient.Query(query).Exec(); err != nil {
			config.G.Log.System.LogFatal("Table %q creation failed: %s", table, err.Error())
		}
	}
}

func (sm *StoreManager) run() {

	defer config.G.OnPanic()

	// Perform first-time initialization of rollup data accumulation structures.
	sm.resetRollupData()

	// Open connection to the Cassandra database here, so we can defer the close.
	var err error
	config.G.Log.System.LogDebug("StoreManager initializing Cassandra client")
	sm.dbClient, err = middleware.CassandraSession(
		config.G.Cassandra.Hosts,
		config.G.Cassandra.Port,
		"",
	)
	if err != nil {
		// Without Cassandra client we can't do our job, so log, whine, and crash.
		config.G.Log.System.LogFatal("StoreManager unable to connect to Cassandra at %v, port %s: %s",
			config.G.Cassandra.Hosts, config.G.Cassandra.Port, err.Error())
	}

	defer sm.dbClient.Close()
	config.G.Log.System.LogDebug("StoreManager Cassandra client initialized")

	config.G.Log.System.LogDebug("StoreManager Cassandra Keyspace configuration starting...")
	sm.populateSchema()

	for {
		select {
		case <-config.G.OnPeerChangeReq:
			config.G.Log.System.LogDebug("StoreManager::run received PEERCHANGE message")
			sm.flush(true)
			sm.resetRollupData()
			config.G.OnPeerChangeRsp <- struct{}{} // Unblock sender
		case <-config.G.OnExit:
			config.G.Log.System.LogDebug("StoreManager::run received QUIT message")
			sm.flush(true)
			sm.wg.Done()
			return
		case metric := <-config.G.Channels.MetricStore:
			sm.accumulate(metric)
		case query := <-config.G.Channels.MetricRequest:
			go sm.query(query)
		case <-sm.timeout:
			sm.flush(false)
		}
	}
}

// timer sends a message on the "timeout" channel after the specified duration.
func (sm *StoreManager) timer() {
	for {
		select {
		case <-config.G.OnExit:
			config.G.Log.System.LogDebug("StoreManager::timer received QUIT message")
			sm.wg.Done()
			return
		case duration := <-sm.setTimeout:
			// Block in this state until a new entry is received.
			select {
			case <-config.G.OnExit:
				// Nothing; do handling above on next iteration.
			case <-time.After(duration):
				select {
				case sm.timeout <- struct{}{}:
					// Timeout sent.
				default:
					// Do not block.
				}
			}
		}
	}
}

// accumulate records a metric according to the rollup definitions.
func (sm *StoreManager) accumulate(metric config.CarbonMetric) {
	config.G.Log.System.LogDebug("StoreManager::accumulate %s=%v", metric.Path, metric.Value)

	// Locate the metric in the map.
	var currentRollup *rollup
	var found bool
	if currentRollup, found = sm.byPath[metric.Path]; !found {

		// Determine which expression matches this path.
		var expr string
		for _, expr = range sm.rollupPriority {
			if expr != config.ROLLUP_CATCHALL {
				if sm.rollup[expr].Expression.MatchString(metric.Path) {
					break
				}
			}
			// Catchall always appears last, and is therefore the default value.
		}

		// Initialize, and insert the new rollup into both maps.
		currentRollup = new(rollup)
		currentRollup.expr = expr
		currentRollup.count = make([]uint64, len(sm.rollup[expr].Windows))
		currentRollup.value = make([]float64, len(sm.rollup[expr].Windows))
		sm.byPath[metric.Path] = currentRollup
		sm.byExpr[expr].path[metric.Path] = currentRollup

		// Send the entry off for writing to the path index.
		config.G.Channels.IndexStore <- metric
	}

	// Apply the incoming metric to each rollup bucket.
	switch sm.rollup[currentRollup.expr].Method {
	case config.AVERAGE:
		for i, v := range currentRollup.value {
			currentRollup.value[i] = (v*float64(currentRollup.count[i]) + metric.Value) /
				float64(currentRollup.count[i]+1)
		}
	case config.MAX:
		for i, v := range currentRollup.value {
			if v < metric.Value {
				currentRollup.value[i] = metric.Value
			}
		}
	case config.MIN:
		for i, v := range currentRollup.value {
			if v > metric.Value || currentRollup.count[i] == 0 {
				currentRollup.value[i] = metric.Value
			}
		}
	case config.SUM:
		for i, v := range currentRollup.value {
			currentRollup.value[i] = v + metric.Value
		}
	}

	// Note that we added a data point into each bucket.
	for i, _ := range currentRollup.count {
		currentRollup.count[i]++
	}
}

// flush persists the accumulated metrics to the database.
func (sm *StoreManager) flush(terminating bool) {
	config.G.Log.System.LogDebug("StoreManager::flush terminating=%v", terminating)

	// Report the current length of the list of unique paths seen.
	logging.Statsd.Client.Gauge("path.count", int64(len(sm.byPath)), 1.0)

	// Use a consistent current time for all tests in this cycle.
	baseTime := time.Now()

	// Use a reasonable default value for setting the next timer delay.
	nextFlush := baseTime.Add(time.Minute)

	// Walk the set of expressions, looking for closed rollup windows.
	for expr, rl := range sm.byExpr {

		// Inspect each rollup window defined for this expression.
		for i, windowEnd := range rl.nextWriteTime {

			// If the window has closed, or if terminating, process and clear the data.
			if windowEnd.Before(baseTime) || terminating {

				var statTime time.Time
				if terminating {
					statTime = baseTime
				} else {
					statTime = windowEnd
				}

				// Iterate over all the paths that match the current expression.
				for path, rollup := range rl.path {

					if rollup.count[i] > 0 {
						// Data has accumulated while this window was open; write it.
						sm.write(expr, sm.rollup[expr].Windows[i], path, statTime, rollup.value[i])
					}

					// Ensure the bucket is empty for the next open window.
					rollup.count[i] = 0
					rollup.value[i] = 0
				}

				// Set a new window closing time for the just-cleared window.
				rl.nextWriteTime[i] = nextTimeBoundary(baseTime, sm.rollup[expr].Windows[i].Window)
			}
			// ASSERT: rl.nextWriteTime[i] time is in the future (later than baseTime).

			// Adjust the timer delay downwards if this window closing time is
			// earlier than all others seen so far.
			if nextFlush.After(rl.nextWriteTime[i]) {
				nextFlush = rl.nextWriteTime[i]
			}
		}
	}

	// Set a timer to expire when the earliest future window closing occurs.
	if !terminating {

		// Convert window closing time to a duration, and do a sanity check.
		delay := nextFlush.Sub(baseTime)
		if delay.Nanoseconds() < 0 {
			delay = time.Millisecond
		}

		// Perform a non-blocking write to the timeout channel.
		select {
		case sm.setTimeout <- delay:
			// Notification sent
		default:
			// Do not block if channel is at capacity
		}
	}
}

// flush persists the accumulated metrics to the database.
func (sm *StoreManager) write(expr string, w config.RollupWindow, path string, ts time.Time, value float64) {

	config.G.Log.Carbon.LogInfo("match=%q tbl=%s ts=%v path=%s val=%.4f win=%v ret=%v ",
		expr, w.Table, ts.Format("15:04:05.000"), path, value, w.Window, w.Retention)

	query := fmt.Sprintf(
		`INSERT INTO %s.%s (path, timestamp, stat) VALUES (?, ?, ?)`,
		config.G.Cassandra.Keyspace, w.Table)

	if err := sm.dbClient.Query(query, path, ts, value).Exec(); err != nil {
		// Could not write to Cassandra cluster...we should scream loudly about this.  Possibly a failure case?.
		config.G.Log.System.LogError("Cassandra write error: %s", err.Error())
	}
}

// query returns the data matched by the supplied query.
func (sm *StoreManager) query(q config.MetricQuery) {
	switch strings.ToLower(q.Method) {
	case "delete":
		// TODO
	default:
		sm.queryGET(q)
	}
}

// query returns the data matched by the supplied query.
func (sm *StoreManager) queryGET(q config.MetricQuery) {

	config.G.Log.System.LogDebug("StoreManager::query %v", q.Query)

	// Query particulars are mandatory.
	if q.Query == "" {
		q.Channel <- config.APIQueryResponse{config.AQS_BADREQUEST, "no query specified", []byte{}}
		return
	}

	var resp config.APIQueryResponse

	// TODO: Build the query response.
	resp = config.APIQueryResponse{config.AQS_OK, "", []byte{'[', ']'}}

	// For testing: time.Sleep(time.Second * 2)

	// If the API gave up on us because we took too long, writing to the channel
	// will cause first a data race, and then a panic (write on closed channel).
	// We check, but if we lose a race we will need to recover.
	defer func() {
		_ = recover()
	}()

	// Check whether the channel is closed before attempting a write.
	select {
	case <-q.Channel:
		// Immediate return means channel is closed (we know there is no data in it).
	default:
		// If the channel would have blocked, it is open, we can write to it.
		q.Channel <- resp
	}
}
