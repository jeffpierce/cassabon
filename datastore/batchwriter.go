package datastore

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

type batchWriter struct {
	dbClient  *gocql.Session
	keyspace  string
	batchSize int
	insert    chan *gocql.Batch

	batch     *gocql.Batch
	stmtCount int
	stmt      string
}

// Init
func (bw *batchWriter) Init(dbClient *gocql.Session, keyspace string, batchSize int, insert chan *gocql.Batch) {
	bw.dbClient = dbClient
	bw.keyspace = keyspace
	bw.batchSize = batchSize
	bw.insert = insert
}

// Size
func (bw *batchWriter) Size() int {
	return bw.stmtCount
}

// Prepare
func (bw *batchWriter) Prepare(table string) {
	bw.batch = nil
	bw.stmtCount = 0
	bw.stmt = fmt.Sprintf(
		`INSERT INTO %s.%s (path, time, stat) VALUES (?, ?, ?)`, bw.keyspace, table)
}

// Append
func (bw *batchWriter) Append(path string, ts time.Time, value float64) {
	if bw.batch == nil {
		bw.batch = gocql.NewBatch(gocql.LoggedBatch)
	}
	bw.batch.Query(bw.stmt, path, ts, value)
	bw.stmtCount++
	if bw.stmtCount >= bw.batchSize {
		bw.Write()
	}
}

// Write
func (bw *batchWriter) Write() {
	if bw.stmtCount > 0 && bw.batch != nil {
		batch := bw.batch
		bw.stmtCount = 0
		bw.batch = nil
		select {
		case bw.insert <- batch:
			// Sent.
		default:
			// Don't block.
			// Shouldn't happen, but just in case, don't hang on termination.
		}
	}
}
