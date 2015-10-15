package datastore

import (
	"encoding/json"
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
	From   int64                `json:"from"`
	To     int64                `json:"to"`
	Step   int                  `json:"step"`
	Series map[string][]float64 `json:"series"`
}

type MetricManager struct {

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

func (mm *MetricManager) Init() {

	// Copy in the configuration (requires hard restart to refresh).
	mm.rollupPriority = config.G.RollupPriority
	mm.rollup = config.G.Rollup

	// Initialize private objects.
	mm.setTimeout = make(chan time.Duration, 0)
	mm.timeout = make(chan struct{}, 1)
}

func (mm *MetricManager) Start(wg *sync.WaitGroup) {

	// Start the persistent goroutines.
	mm.wg = wg
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
	conn := mm.dbClient.Pool.Pick(mm.dbClient.Query(""))
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

func (mm *MetricManager) run() {

	defer config.G.OnPanic()

	// Perform first-time initialization of rollup data accumulation structures.
	mm.resetRollupData()

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

// getExpression returns the first expression that matches the supplied path.
func (mm *MetricManager) getExpression(path string) string {
	var expr string
	for _, expr = range mm.rollupPriority {
		if expr != config.ROLLUP_CATCHALL {
			if mm.rollup[expr].Expression.MatchString(path) {
				break
			}
		}
		// Catchall always appears last, and is therefore the default value.
	}
	return expr
}

// accumulate records a metric according to the rollup definitions.
func (mm *MetricManager) accumulate(metric config.CarbonMetric) {
	config.G.Log.System.LogDebug("MetricManager::accumulate %s=%v", metric.Path, metric.Value)

	// Locate the metric in the map.
	var currentRollup *rollup
	var found bool
	if currentRollup, found = mm.byPath[metric.Path]; !found {

		// Initialize, and insert the new rollup into both maps.
		expr := mm.getExpression(metric.Path)
		currentRollup = new(rollup)
		currentRollup.expr = expr
		currentRollup.count = make([]uint64, len(mm.rollup[expr].Windows))
		currentRollup.value = make([]float64, len(mm.rollup[expr].Windows))
		mm.byPath[metric.Path] = currentRollup
		mm.byExpr[expr].path[metric.Path] = currentRollup

		// Send the entry off for writing to the path index.
		config.G.Channels.IndexStore <- metric
	}

	// Apply the incoming metric to each rollup bucket.
	switch mm.rollup[currentRollup.expr].Method {
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
func (mm *MetricManager) flush(terminating bool) {
	config.G.Log.System.LogDebug("MetricManager::flush terminating=%v", terminating)

	// Report the current length of the list of unique paths seen.
	logging.Statsd.Client.Gauge("path.count", int64(len(mm.byPath)), 1.0)

	// Use a consistent current time for all tests in this cycle.
	baseTime := time.Now()

	// Use a reasonable default value for setting the next timer delay.
	nextFlush := baseTime.Add(time.Minute)

	// Set up the database batch writer.
	bw := batchWriter{}
	bw.Init(mm.dbClient, config.G.Cassandra.Keyspace, config.G.Cassandra.BatchSize)

	// Walk the set of expressions.
	for expr, runList := range mm.byExpr {

		// For each expression, inspect each rollup window.
		// Note: Each window is written to a different table.
		for i, windowEnd := range runList.nextWriteTime {

			// If the window has closed, or if terminating, process and clear the data.
			if windowEnd.Before(baseTime) || terminating {

				var statTime time.Time
				if terminating {
					statTime = baseTime
				} else {
					statTime = windowEnd
				}

				// Every row in the batch has the same timestamp, is written to the same
				// table, has the same retention period, and matches the same expression.
				bw.Prepare(mm.rollup[expr].Windows[i].Table)

				// Iterate over all the paths that match the current expression.
				for path, rollup := range runList.path {

					if rollup.count[i] > 0 {
						// Data has accumulated while this window was open; write it.
						config.G.Log.Carbon.LogInfo(
							"match=%q tbl=%s ts=%v path=%s val=%.4f win=%v ret=%v ",
							expr, mm.rollup[expr].Windows[i].Table,
							statTime.Format("15:04:05.000"), path, rollup.value[i],
							mm.rollup[expr].Windows[i].Window, mm.rollup[expr].Windows[i].Retention)

						// This will cause a write if we have reached our maximum batch size.
						if err := bw.Append(path, statTime, rollup.value[i]); err != nil {
							config.G.Log.System.LogError("Cassandra write error: %s", err.Error())
							logging.Statsd.Client.Inc("metricmgr.db.err.write", 1, 1.0)
						}
					}

					// Ensure the bucket is empty for the next open window.
					rollup.count[i] = 0
					rollup.value[i] = 0
				}
				if bw.Size() > 0 {
					if err := bw.Write(); err != nil {
						config.G.Log.System.LogError("Cassandra write error: %s", err.Error())
						logging.Statsd.Client.Inc("metricmgr.db.err.write", 1, 1.0)
					}
				}

				// Set a new window closing time for the just-cleared window.
				runList.nextWriteTime[i] = nextTimeBoundary(baseTime, mm.rollup[expr].Windows[i].Window)
			}
			// ASSERT: runList.nextWriteTime[i] time is in the future (later than baseTime).

			// Adjust the timer delay downwards if this window closing time is
			// earlier than all others seen so far.
			if nextFlush.After(runList.nextWriteTime[i]) {
				nextFlush = runList.nextWriteTime[i]
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
		case mm.setTimeout <- delay:
			// Notification sent
		default:
			// Do not block if channel is at capacity
		}
	}
}

// query returns the data matched by the supplied query.
func (mm *MetricManager) query(q config.MetricQuery) {
	switch strings.ToLower(q.Method) {
	case "delete":
		mqdt := time.Now()
		// TODO
		logging.Statsd.Client.TimingDuration("metricmgr.query.delete", time.Since(mqdt), 1.0)
	default:
		mqgt := time.Now()
		mm.queryGET(q)
		logging.Statsd.Client.TimingDuration("metricmgr.query.get", time.Since(mqgt), 1.0)
	}
}

// query returns the data matched by the supplied query.
func (mm *MetricManager) queryGET(q config.MetricQuery) {

	config.G.Log.System.LogDebug("MetricManager::query %v", q.Query)

	// Query particulars are mandatory.
	if len(q.Query) == 0 || q.Query[0] == "" {
		q.Channel <- config.APIQueryResponse{config.AQS_BADREQUEST, "no query specified", []byte{}}
		return
	}

	var resp config.APIQueryResponse
	var step int
	var table string
	var singleStat float64
	var statList []float64

	// Initialize series map
	series := map[string][]float64{}

	// Get difference between now and q.From to determine which rollup table to query
	timeDelta := time.Since(time.Unix(q.From, 0))

	// Determine lookup table and data point step
	for _, path := range q.Query {
		expr := mm.getExpression(path)
		for _, window := range config.G.Rollup[expr].Windows {
			config.G.Log.System.LogDebug("evaluating %v", window)
			config.G.Log.System.LogDebug("timeDelta: %d, window.Retention: %d", timeDelta.Seconds(), window.Retention.Seconds())
			if timeDelta < window.Retention {
				table = window.Table
				step = int(window.Window.Seconds())
				break
			}
		}

		// Build query for this stat path
		query := fmt.Sprintf(`SELECT stat FROM %s.%s WHERE path = ? AND time >= ? AND time <= ?`,
			config.G.Cassandra.Keyspace, table)

		config.G.Log.System.LogDebug("Query: %s", query)

		// Populate statList with returned stats.
		iter := mm.dbClient.Query(query, path, time.Unix(q.From, 0), time.Unix(q.To, 0)).Iter()
		for iter.Scan(&singleStat) {
			config.G.Log.System.LogDebug("singleStat: %d", singleStat)
			statList = append(statList, singleStat)
		}

		config.G.Log.System.LogDebug("statList: %v", statList)

		if err := iter.Close(); err != nil {
			config.G.Log.System.LogError("Error closing stat iteration: %s", err.Error())
			logging.Statsd.Client.Inc("metricmgr.db.err.read", 1, 1.0)
		}

		// Append to series portion of response.
		series[path] = statList
	}

	response := MetricResponse{
		q.From,
		q.To,
		step,
		series,
	}

	jsonResp, _ := json.Marshal(response)

	resp = config.APIQueryResponse{config.AQS_OK, "", jsonResp}

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
