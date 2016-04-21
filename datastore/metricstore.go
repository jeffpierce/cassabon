package datastore

import (
	"time"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/logging"
)

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

// applyMethod combines values using the appropriate rollup method.
func (mm *MetricManager) applyMethod(method config.RollupMethod, currentVal, newVal float64, count uint64) float64 {
	switch method {
	case config.AVERAGE:
		currentVal = currentVal + newVal
	case config.MAX:
		if currentVal < newVal {
			currentVal = newVal
		}
	case config.MIN:
		if currentVal > newVal || count == 0 {
			currentVal = newVal
		}
	case config.SUM:
		currentVal = currentVal + newVal
	case config.LAST:
		currentVal = newVal
	}
	return currentVal
}

// addToMaps adds a rollup into the mm.byPath and mm.byExpr maps.
func (mm *MetricManager) addToMaps(metricPath string) *rollup {
	var currentRollup *rollup

	expr := mm.getExpression(metricPath)
	currentRollup = new(rollup)
	currentRollup.expr = expr
	currentRollup.count = make([]uint64, len(mm.rollup[expr].Windows))
	currentRollup.value = make([]float64, len(mm.rollup[expr].Windows))
	mm.byPath[metricPath] = currentRollup
	mm.byExpr[expr].path[metricPath] = currentRollup

	return currentRollup
}

// accumulate records a metric according to the rollup definitions.
func (mm *MetricManager) accumulate(metric config.CarbonMetric) {
	config.G.Log.System.LogDebug("MetricManager::accumulate %s=%v", metric.Path, metric.Value)

	// Locate the metric in the map.
	var currentRollup *rollup
	var found bool
	if currentRollup, found = mm.byPath[metric.Path]; !found {

		// Initialize, and insert the new rollup into both maps.
		currentRollup = mm.addToMaps(metric.Path)

		// Send the entry off for writing to the path index.
		config.G.Channels.IndexStore <- metric
	}

	// Apply the incoming metric to each rollup bucket.
	for i, v := range currentRollup.value {
		currentRollup.value[i] = mm.applyMethod(
			mm.rollup[currentRollup.expr].Method, v, metric.Value, currentRollup.count[i])
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
	bw.Init(mm.dbClient, config.G.Cassandra.Keyspace, config.G.Cassandra.BatchSize, mm.insert)

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
						var value float64
						if mm.rollup[expr].Method == config.AVERAGE {
							// Calculate averages by dividing by the count.
							value = rollup.value[i] / float64(rollup.count[i])
						} else {
							// Other rollup methods use the value as-is.
							value = rollup.value[i]
						}

						if config.G.Log.System.GetLogLevel() < logging.Info {
							config.G.Log.Carbon.LogInfo(
								"match=%q tbl=%s ts=%v path=%s val=%.4f win=%v ret=%v ",
								expr, mm.rollup[expr].Windows[i].Table,
								statTime.UTC().Format("15:04:05.000"), path, value,
								mm.rollup[expr].Windows[i].Window, mm.rollup[expr].Windows[i].Retention)
						}

						bw.Append(path, statTime, value)
					}

					// Ensure the bucket is empty for the next open window.
					rollup.count[i] = 0
					rollup.value[i] = 0
				}
				if bw.Size() > 0 {
					bw.Write()
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
