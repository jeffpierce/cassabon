package datastore

import (
	"time"

	"github.com/jeffpierce/cassabon/config"
)

type StoreManager struct {
	todo       chan config.CarbonMetric
	setTimeout chan time.Duration // Write a duration to this to get a notification on timeout channel
	timeout    chan struct{}      // Timeout notifications arrive on this channel
}

func (sm *StoreManager) Init() {

	// Initialize private objects.
	sm.todo = make(chan config.CarbonMetric, config.G.Parameters.DataStore.TodoChanLen)
	sm.setTimeout = make(chan time.Duration, 0)
	sm.timeout = make(chan struct{}, 1)

	// Start the goroutines.
	config.G.WG.Add(3)
	go sm.timer()
	go sm.insert()
	go sm.run()

	// Kick off the timer.
	sm.setTimeout <- time.Duration(config.G.Parameters.DataStore.MaxFlushDelay) * time.Second
}

func (sm *StoreManager) run() {

	// Open connection to the Cassandra database here, so we can defer the close.

	// Wait for metrics entries to arrive, and process them.
	for {
		select {
		case <-config.G.Quit:
			config.G.Log.System.LogInfo("StoreManager::run received QUIT message")
			config.G.WG.Done()
			return
		case metric := <-config.G.Channels.DataStore:
			config.G.Log.Carbon.LogDebug("StoreManager received metric: %v", metric)

			// Send the path off to the indexer.
			// TODO: config.G.Channels.Indexer <- metric

			// Accumulate the entry for writing to Cassandra.
			sm.todo <- metric
		}
	}
}

// insert accumulates metrics entries up to a count or up to a timeout, and writes them.
func (sm *StoreManager) insert() {
	for {
		select {
		case <-config.G.Quit:
			config.G.Log.System.LogInfo("StoreManager::insert received QUIT message")
			config.G.WG.Done()
			return
		case metric := <-sm.todo:
			config.G.Log.Carbon.LogDebug("StoreManager::insert received metric: %v", metric)
			sm.accumulate(metric)
		case <-sm.timeout:
			config.G.Log.Carbon.LogDebug("StoreManager::insert received timeout")
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

// timeout sends a message on the "timeout" channel after the specified duration.
func (sm *StoreManager) timer() {
	for {
		select {
		case <-config.G.Quit:
			config.G.Log.System.LogInfo("StoreManager::timer received QUIT message")
			config.G.WG.Done()
			return
		case duration := <-sm.setTimeout:
			// Block in this state until a new entry is received.
			select {
			case <-config.G.Quit:
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

// accumulate records a metric for subsequent flush to the database.
func (sm *StoreManager) accumulate(metric config.CarbonMetric) {
}

// flush persists the accumulated metrics to the database.
func (sm *StoreManager) flush() {
}
