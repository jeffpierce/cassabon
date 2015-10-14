package datastore

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"

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
	bwt := time.Now()
	if bw.stmtCount > 0 {
		bw.stmtCount = 0
		return bw.dbClient.ExecuteBatch(bw.batch)
	} else {
		return nil
	}
	logging.Statsd.Client.TimingDuration("db.write", time.Since(bwt), 1.0)
}
