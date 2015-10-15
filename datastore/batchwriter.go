package datastore

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/logging"
)

type batchWriter struct {
	dbClient  *gocql.Session
	keyspace  string
	batchSize int

	batch     *gocql.Batch
	stmtCount int
	stmt      string
}

// Init
func (bw *batchWriter) Init(dbClient *gocql.Session, keyspace string, batchSize int) {
	bw.dbClient = dbClient
	bw.keyspace = keyspace
	bw.batchSize = batchSize
}

// Size
func (bw *batchWriter) Size() int {
	return bw.stmtCount
}

// Prepare
func (bw *batchWriter) Prepare(table string) {
	bw.batch = gocql.NewBatch(gocql.LoggedBatch)
	bw.stmtCount = 0
	bw.stmt = fmt.Sprintf(
		`INSERT INTO %s.%s (path, time, stat) VALUES (?, ?, ?)`, bw.keyspace, table)
}

// Append
func (bw *batchWriter) Append(path string, ts time.Time, value float64) error {
	bw.batch.Query(bw.stmt, path, ts, value)
	bw.stmtCount++
	if bw.stmtCount >= bw.batchSize {
		return bw.Write()
	} else {
		return nil
	}
}

// Write
func (bw *batchWriter) Write() error {
	if bw.stmtCount > 0 {
		bwt := time.Now()
		bw.stmtCount = 0
		err := bw.dbClient.ExecuteBatch(bw.batch)
		if err != nil {
			// Retry with exponential backoff.
			config.G.Log.System.LogWarn("Retrying MetricManager write...")
			go bw.retryWrite(bw.batch)
			logging.Statsd.Client.Inc("metricmgr.db.retry", 1, 1.0)
		}
		logging.Statsd.Client.TimingDuration("metricmgr.db.write", time.Since(bwt), 1.0)
	}
	return nil
}

func (bw *batchWriter) retryWrite(batch *gocql.Batch) error {
	var i time.Duration
	i = 0
	var err error
	for i < 5 {
		err = bw.dbClient.ExecuteBatch(batch)
		if err == nil {
			return err
		}
		i++
		time.Sleep(i * time.Second)
	}
	// Failed 5x times, give up and log the error.
	logging.Statsd.Client.Inc("metricmgr.db.err.write", 1, 1.0)
	config.G.Log.System.LogError("Could not write batch to database: %v", err.Error())
	return err
}
