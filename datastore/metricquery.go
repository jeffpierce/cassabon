package datastore

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/logging"
)

// query returns the data matched by the supplied query.
func (mm *MetricManager) query(q config.MetricQuery) {
	switch strings.ToLower(q.Method) {
	case "delete":
		mm.queryDELETE(q)
	default:
		mm.queryGET(q)
	}
}

// delete removes rows matching a key from the metrics store.
func (mm *MetricManager) queryDELETE(q config.MetricQuery) {

	config.G.Log.System.LogDebug("MetricManager::queryDELETE %v", q)

	// Query particulars are mandatory.
	if len(q.Query) == 0 || q.Query[0] == "" {
		q.Channel <- config.APIQueryResponse{config.AQS_BADREQUEST, "no query specified", []byte{}}
		return
	}

	type deleteResponseDetails struct {
		Deleted uint64            `json:"approximate_total_deleted"`
		ByTable map[string]uint64 `json:"approximate_total_by_table"`
		Errors  map[string]string `json:"delete_errors"`
	}

	type deleteResponse struct {
		Dryrun bool                             `json:"dryrun"`
		Paths  map[string]deleteResponseDetails `json:"paths"`
	}

	// Repeat for each path listed in the request.
	var delResp deleteResponse = deleteResponse{q.DryRun, make(map[string]deleteResponseDetails)}
	for _, path := range q.Query {
		var drDetails deleteResponseDetails = deleteResponseDetails{0, make(map[string]uint64), make(map[string]string)}

		// The path could exist in any table, so look in all of them.
		for _, table := range config.G.RollupTables {

			// Get counts of the number of rows affected for providing dry-run analysis.
			drDetails.ByTable[table] = 0
			query := fmt.Sprintf(`SELECT COUNT(*) FROM %s.%s WHERE path=? AND time>=? AND time<=?`,
				config.G.Cassandra.Keyspace, table)
			config.G.Log.System.LogDebug("Querying for %q with: %q", path, query)
			iter := mm.dbClient.Query(query, path, time.Unix(q.From, 0), time.Unix(q.To, 0)).Iter()
			var count uint64
			for iter.Scan(&count) {
				drDetails.ByTable[table] = count
			}
			drDetails.Deleted += drDetails.ByTable[table]

			// If this isn't a dry run, do the deletions.
			// Note: Cassandra provides no feedback on how may rows were actually deleted,
			//       so we return the counts obtained above as an approximation.
			if !q.DryRun && drDetails.ByTable[table] > 0 {
				query := fmt.Sprintf(`DELETE FROM %s.%s WHERE path=? AND time>=? AND time<=?`,
					config.G.Cassandra.Keyspace, table)
				config.G.Log.System.LogDebug("Deleting %q with: %q", path, query)
				if err := mm.dbClient.Query(query, path, time.Unix(q.From, 0), time.Unix(q.To, 0)).Exec(); err != nil {
					drDetails.Errors[table] = err.Error()
				}
			}
		}

		delResp.Paths[path] = drDetails
	}

	// Send the response payload.
	mm.sendResponse(q.Channel, &delResp)
}

// query returns the data matched by the supplied query.
func (mm *MetricManager) queryGET(q config.MetricQuery) {

	config.G.Log.System.LogDebug("MetricManager::queryGET %v", q)

	// Query particulars are mandatory.
	if len(q.Query) == 0 || q.Query[0] == "" {
		q.Channel <- config.APIQueryResponse{config.AQS_BADREQUEST, "no query specified", []byte{}}
		return
	}

	// Variables to be returned in the response payload.
	var step int64
	var normalFrom int64
	series := map[string][]interface{}{}

	// Get difference between now and q.From to determine which rollup table to query
	timeDelta := time.Since(time.Unix(q.From, 0))

	// Repeat for each path listed in the request.
	for _, path := range q.Query {

		// Determine lookup table name and data point step from config of rollup.
		var table string
		expr := mm.getExpression(path)
		config.G.Log.System.LogDebug("Determining step/table for path %q, expr %q", path, expr)
		for _, window := range mm.rollup[expr].Windows {
			config.G.Log.System.LogDebug("eval timeDelta: %v, ret: %v win: %v table: %s",
				timeDelta, window.Retention, window.Window, window.Table)
			if timeDelta < window.Retention {
				table = window.Table
				step = int64(window.Window.Seconds())
				config.G.Log.System.LogDebug("Using step=%d seconds, table=%s", step, table)
				break
			}
		}

		// Generate normalized from so that items graph correctly.
		normalFrom = q.From + (step - (q.From % step))

		// Build query for this stat path
		query := fmt.Sprintf(`SELECT stat,time FROM %s.%s WHERE path=? AND time>=? AND time<=?`,
			config.G.Cassandra.Keyspace, table)
		config.G.Log.System.LogDebug("Querying for %q with: %q", path, query)

		// Populate statList with returned stats.
		var statList []interface{} = make([]interface{}, 0)
		var stat float64
		var mergeCount uint64
		var mergeValue float64
		var ts, nextTS time.Time
		nextTS = nextTimeBoundary(time.Unix(normalFrom, 0), time.Duration(step)*time.Second)
		iter := mm.dbClient.Query(query, path, time.Unix(normalFrom, 0), time.Unix(q.To, 0)).Iter()
		for iter.Scan(&stat, &ts) {

			// Fill in any gaps in the series.
			for nextTS.Before(ts) {
				if ts.Sub(nextTS) >= time.Duration(step)*time.Second {
					if mergeCount > 0 {
						if mm.rollup[expr].Method == config.AVERAGE {
							// Calculate averages by dividing by the count.
							mergeValue = mergeValue / float64(mergeCount)
						}
						config.G.Log.System.LogDebug("ins: %14.8f %v ( %v )", mergeValue,
							nextTS.UTC().Format("15:04:05.000"), ts.Format("15:04:05.000"))
						statList = append(statList, mergeValue)
						mergeValue = 0
						mergeCount = 0
					} else {
						config.G.Log.System.LogDebug("ins: %14s %v ( %v )", "nil",
							nextTS.UTC().Format("15:04:05.000"), ts.Format("15:04:05.000"))
						statList = append(statList, nil)
					}
				}
				nextTS = nextTS.Add(time.Duration(step) * time.Second)
			}

			// Append the current stat.
			if ts.Equal(nextTS) {
				if mergeCount > 0 {
					config.G.Log.System.LogDebug("---: %14.8f %v ( %v )", stat,
						ts.Format("15:04:05.000"), nextTS.UTC().Format("15:04:05.000"))
					mergeValue = mm.applyMethod(mm.rollup[expr].Method, mergeValue, stat, mergeCount)
					mergeCount++
					if mm.rollup[expr].Method == config.AVERAGE {
						mergeValue = mergeValue / float64(mergeCount)
					}
					stat = mergeValue
					mergeValue = 0
					mergeCount = 0
				}
				config.G.Log.System.LogDebug("row: %14.8f %v ( %v )", stat,
					ts.Format("15:04:05.000"), nextTS.UTC().Format("15:04:05.000"))
				if math.IsNaN(stat) {
					statList = append(statList, nil)
				} else {
					statList = append(statList, stat)
				}
				nextTS = ts.Add(time.Duration(step) * time.Second)
			} else {
				config.G.Log.System.LogDebug("---: %14.8f %v ( %v )", stat,
					ts.Format("15:04:05.000"), nextTS.UTC().Format("15:04:05.000"))
				mergeValue = mm.applyMethod(mm.rollup[expr].Method, mergeValue, stat, mergeCount)
				mergeCount++
				nextTS = nextTimeBoundary(ts, time.Duration(step)*time.Second)
			}
		}

		if err := iter.Close(); err != nil {
			config.G.Log.System.LogError("Error closing stat iteration: %s", err.Error())
			logging.Statsd.Client.Inc("metricmgr.db.err.read", 1, 1.0)
		}

		// Write final data point, if there is one.
		if mergeCount > 0 {
			if mm.rollup[expr].Method == config.AVERAGE {
				// Calculate averages by dividing by the count.
				mergeValue = mergeValue / float64(mergeCount)
			}
			config.G.Log.System.LogDebug("ins: %14.8f %v ( %v )", mergeValue,
				nextTS.UTC().Format("15:04:05.000"), ts.Format("15:04:05.000"))
			statList = append(statList, mergeValue)
			mergeValue = 0
			mergeCount = 0
		}

		// Fill in gaps after the last data point.
		to := time.Unix(q.To, 0)
		nextTS = nextTS.Add(time.Duration(step) * time.Second)
		for nextTS.Before(to) {
			config.G.Log.System.LogDebug("pad: %14s %v ( %v )", "nil",
				nextTS.UTC().Format("15:04:05.000"), to.UTC().Format("15:04:05.000"))
			statList = append(statList, nil)
			nextTS = nextTS.Add(time.Duration(step) * time.Second)
		}

		// Append to series portion of response.
		config.G.Log.System.LogDebug("Result: %s=%v", path, statList)
		series[path] = statList
	}

	// Build the response payload and wrap it in the channel reply struct.
	payload := MetricResponse{normalFrom, q.To, step, series}
	mm.sendResponse(q.Channel, &payload)
}

// sendResponse takes care of the details of returning a response to the API code.
func (mm *MetricManager) sendResponse(respChannel chan config.APIQueryResponse, payload interface{}) {

	// If the API gave up on us because we took too long, writing to the channel
	// will cause first a data race, and then a panic (write on closed channel).
	// We check, but if we lose a race we will need to recover.
	defer func() {
		_ = recover()
	}()

	// Wrap the response payload in the channel reply struct.
	var resp config.APIQueryResponse
	if jsonResp, err := json.Marshal(payload); err == nil {
		resp = config.APIQueryResponse{config.AQS_OK, "", jsonResp}
	} else {
		resp = config.APIQueryResponse{config.AQS_ERROR, "JSON encoding error", []byte{}}
		config.G.Log.System.LogError("JSON encoding error: %s", err.Error())
		logging.Statsd.Client.Inc("metricmgr.db.err.read", 1, 1.0)
	}

	// Check whether the channel is closed before attempting a write.
	select {
	case <-respChannel:
		// Immediate return means channel is closed (we know there is no data in it).
	default:
		// If the channel would have blocked, it is open, we can write to it.
		respChannel <- resp
	}
}
