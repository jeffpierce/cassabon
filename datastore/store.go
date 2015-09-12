package datastore

import (
	"time"

	"github.com/jeffpierce/cassabon/config"
)

// rollups contains the accumulated metrics data for a path.
type rollups struct {
	expr  string    // The text form of the path expression, to locate the definition
	count []uint64  // The number of data points accumulated (for averaging)
	value []float64 // One rollup per window definition
}

type StoreManager struct {

	// Timer management.
	setTimeout chan time.Duration // Write a duration to this to get a notification on timeout channel
	timeout    chan struct{}      // Timeout notifications arrive on this channel

	// Rollup data.
	metricData map[string]rollups // Stats, by path, broken out by rollup window duration.
	maxTimeout time.Duration      // The duration of the shortest rollup window.
}

func (sm *StoreManager) Init() {

	// Initialize private objects.
	sm.setTimeout = make(chan time.Duration, 0)
	sm.timeout = make(chan struct{}, 1)

	// Initialize rollup data structures.
	sm.metricData = make(map[string]rollups)
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
	var currentRollups rollups
	var found bool
	if currentRollups, found = sm.metricData[metric.Path]; !found {

		// Determine which expression matches this path.
		for _, expr := range config.G.RollupPriority {
			if expr != config.CATCHALL_EXPRESSION {
				if config.G.Rollup[expr].Expression.MatchString(metric.Path) {
					currentRollups.expr = expr
					break
				}
			} else {
				currentRollups.expr = config.CATCHALL_EXPRESSION
			}
		}

		currentRollups.count = make([]uint64, len(config.G.Rollup[currentRollups.expr].Windows))
		currentRollups.value = make([]float64, len(config.G.Rollup[currentRollups.expr].Windows))
	}

	// Apply the incoming metric to each rollup bucket.
	switch config.G.Rollup[currentRollups.expr].Method {
	case config.AVERAGE:
		for i, v := range currentRollups.value {
			currentRollups.value[i] = (v*float64(currentRollups.count[i]) + metric.Value) /
				float64(currentRollups.count[i]+1)
		}
	case config.MAX:
		for i, v := range currentRollups.value {
			if v < metric.Value {
				currentRollups.value[i] = metric.Value
			}
		}
	case config.MIN:
		for i, v := range currentRollups.value {
			if v > metric.Value {
				currentRollups.value[i] = metric.Value
			}
		}
	case config.SUM:
		for i, v := range currentRollups.value {
			currentRollups.value[i] = v + metric.Value
		}
	}
	for i, _ := range currentRollups.count {
		currentRollups.count[i]++
	}

	// Save the updated structure.
	sm.metricData[metric.Path] = currentRollups
}

// flush persists the accumulated metrics to the database.
func (sm *StoreManager) flush() {
	config.G.Log.System.LogDebug("StoreManager::flush")
}
