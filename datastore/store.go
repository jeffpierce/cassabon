package datastore

import (
	"time"

	"github.com/jeffpierce/cassabon/config"
)

type StoreManager struct {
	setTimeout chan time.Duration // Write a duration to this to get a notification on timeout channel
	timeout    chan struct{}      // Timeout notifications arrive on this channel
}

func (sm *StoreManager) Init() {

	// Initialize private objects.
	sm.setTimeout = make(chan time.Duration, 0)
	sm.timeout = make(chan struct{}, 1)

	// Start the persistent goroutines.
	config.G.OnExitWG.Add(1)
	go sm.timer()

	// Kick off the timer.
	sm.setTimeout <- time.Duration(config.G.Parameters.DataStore.MaxFlushDelay) * time.Second
}

func (sm *StoreManager) Start() {
	config.G.OnReload2WG.Add(2)
	go sm.insert()
	go sm.run()
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

func (sm *StoreManager) run() {

	// Open connection to the Cassandra database here, so we can defer the close.

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

// insert accumulates metrics entries up to a count or up to a timeout, and writes them.
func (sm *StoreManager) insert() {
	for {
		select {
		case <-config.G.OnReload2:
			config.G.Log.System.LogDebug("StoreManager::insert received QUIT message")
			sm.flush()
			config.G.OnReload2WG.Done()
			return
		case metric := <-config.G.Channels.StatStore:
			config.G.Log.System.LogDebug("StoreManager::insert received metric: %v", metric)
			sm.accumulate(metric)
		case <-sm.timeout:
			config.G.Log.System.LogDebug("StoreManager::insert received timeout")
			sm.flush()
			select {
			case sm.setTimeout <- time.Duration(config.G.Parameters.DataStore.MaxFlushDelay) * time.Second:
				// Notification sent
			default:
				// Do not block if channel is at capacity
			}
		}
	}
}

// accumulate records a metric for subsequent flush to the database.
func (sm *StoreManager) accumulate(metric config.CarbonMetric) {
	config.G.Log.System.LogDebug("StoreManager::accumulate")
}

// flush persists the accumulated metrics to the database.
func (sm *StoreManager) flush() {
	config.G.Log.System.LogDebug("StoreManager::flush")
}
