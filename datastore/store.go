package datastore

import (
	"time"

	"github.com/jeffpierce/cassabon/config"
)

// rollup contains the accumulated metrics data for a path.
type rollup struct {
	expr  string    // The text form of the path expression, to locate the definition
	count []uint64  // The number of data points accumulated (for averaging)
	value []float64 // One rollup per window definition
}

// runlist contains the paths to be written for an expression, and when to write the rollups.
type runlist struct {
	nextWrite []time.Time        // The next write time for each rollup bucket
	path      map[string]*rollup // The rollup data for each path matched by the expression
}

type StoreManager struct {

	// Timer management.
	setTimeout chan time.Duration // Write a duration to this to get a notification on timeout channel
	timeout    chan struct{}      // Timeout notifications arrive on this channel

	// Rollup data.
	byPath     map[string]*rollup  // Stats, by path, for rollup accumulation
	byExpr     map[string]*runlist // Stats, by path within expression, for rollup processing
	maxTimeout time.Duration       // The duration of the shortest rollup window.
}

func (sm *StoreManager) Init() {

	// Initialize private objects.
	sm.setTimeout = make(chan time.Duration, 0)
	sm.timeout = make(chan struct{}, 1)

	// Initialize rollup data structures.
	sm.byPath = make(map[string]*rollup)
	sm.byExpr = make(map[string]*runlist)
	for expr, rollupdef := range config.G.Rollup {
		// For each expression, provide a place to record all the paths that it matches.
		rl := new(runlist)
		rl.nextWrite = make([]time.Time, len(rollupdef.Windows))
		rl.path = make(map[string]*rollup)
		sm.byExpr[expr] = rl
	}
	sm.maxTimeout = 5

	// Start the persistent goroutines.
	config.G.OnExitWG.Add(2)
	go sm.timer()
	go sm.insert()

	// Kick off the timer.
	sm.setTimeout <- time.Duration(sm.maxTimeout) * time.Second
}

func (sm *StoreManager) Start() {
	config.G.OnReload2WG.Add(1)
	go sm.run()
}

func (sm *StoreManager) run() {

	// Wait for metrics entries to arrive, and process them.
	for {
		select {
		case <-config.G.OnReload2:
			config.G.Log.System.LogDebug("StoreManager::run received QUIT message")
			config.G.OnReload2WG.Done()
			return
		case metric := <-config.G.Channels.DataStore:
			config.G.Log.System.LogDebug("StoreManager received metric: %v", metric)

			// Send the entry off for writing to the path index.
			config.G.Channels.IndexStore <- metric

			// Send the entry off for writing to the stats store.
			config.G.Channels.StatStore <- metric
		}
	}
}

// timer sends a message on the "timeout" channel after the specified duration.
func (sm *StoreManager) timer() {
	for {
		select {
		case <-config.G.OnExit:
			config.G.Log.System.LogDebug("StoreManager::timer received QUIT message")
			config.G.OnExitWG.Done()
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

// insert accumulates metrics at each rollup level, and writes them out at defined intervals.
func (sm *StoreManager) insert() {

	// TODO: Open connection to the Cassandra database here, so we can defer the close.

	for {
		select {
		case <-config.G.OnExit:
			config.G.Log.System.LogDebug("StoreManager::insert received QUIT message")
			sm.flush()
			config.G.OnExitWG.Done()
			return
		case metric := <-config.G.Channels.StatStore:
			config.G.Log.System.LogDebug("StoreManager::insert received metric: %v", metric)
			sm.accumulate(metric)
		case <-sm.timeout:
			config.G.Log.System.LogDebug("StoreManager::insert received timeout")
			sm.flush()
			select {
			case sm.setTimeout <- time.Duration(sm.maxTimeout) * time.Second:
				// Notification sent
			default:
				// Do not block if channel is at capacity
			}
		}
	}
}

// accumulate records a metric according to the rollup definitions.
func (sm *StoreManager) accumulate(metric config.CarbonMetric) {
	config.G.Log.System.LogDebug("StoreManager::accumulate")

	// Locate the metric in the map.
	var currentRollup *rollup
	var found bool
	if currentRollup, found = sm.byPath[metric.Path]; !found {

		// Determine which expression matches this path.
		var expr string
		for _, expr = range config.G.RollupPriority {
			if expr != config.CATCHALL_EXPRESSION {
				if config.G.Rollup[expr].Expression.MatchString(metric.Path) {
					break
				}
			}
			// Catchall always appears last, and is therefore the default value.
		}

		// Initialize, and insert the new rollup into both maps.
		currentRollup = new(rollup)
		currentRollup.expr = expr
		currentRollup.count = make([]uint64, len(config.G.Rollup[expr].Windows))
		currentRollup.value = make([]float64, len(config.G.Rollup[expr].Windows))
		sm.byPath[metric.Path] = currentRollup
		sm.byExpr[expr].path[metric.Path] = currentRollup
	}

	// Apply the incoming metric to each rollup bucket.
	switch config.G.Rollup[currentRollup.expr].Method {
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
			if v > metric.Value {
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
func (sm *StoreManager) flush() {
	config.G.Log.System.LogDebug("StoreManager::flush")
}
