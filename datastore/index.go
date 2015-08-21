// Datastore implements workers that work with the Redis Index and Cassandra Metric datastores
package datastore

import (
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"

	"gopkg.in/redis.v3"
)

// IndexMetricPath takes a metric path string and redis client, starts a pipeline, splits the metric,
// and sends it off to be processed by processMetricPath().
func IndexMetricPath(path string, rc *redis.Client) {
	splitPath := strings.Split(path, '.')
	pipe := processMetricPath(&splitPath, len(splitPath), true, rc.Pipeline())
	pipe.Exec()
}

// processMetricPath recursively indexes the metric path via the redis pipeline.
func processMetricPath(splitPath *[]string, pathLen int, isLeaf bool, pipe *redis.Pipeline) *redis.Pipeline {
	// Process the metric path one node at a time.  We store metrics in Redis as a sorted set with score
	// 0 so that lexicographical search works.  Metrics are in the format of:
	//
	// big_endian_length:metric.path:true_or_false
	//
	// This keeps them ordered so that a ZRANGEBYLEX works when finding the next nodes in a path branch.

	// If we've processed the whole thing, return the pipe to be executed.
	for pathLen > 0 {
		// Let's get our big endian representation of the length.
		a := make([]byte, 2)
		binary.BigEndian.PutUint16(a, uint16(pathLen))
		bigE := hex.EncodeToString(a)

		// Construct the metric string
		metric := strings.Join([]string{
			bigE,
			strings.Join(splitPath),
			strconv.FormatBool(isLeaf)}, ':')

		config.G.Log.System.LogDebug("Indexing metric %s", metric)

		// Put it in the pipeline.
		pipe.ZAdd("cassabon", 0, metric)

		// Pop the last node of the metric off, set isLeaf to false, and resume loop.
		_, splitPath = splitPath[len(splitPath)-1], splitPath[:len(a)-1]
		isLeaf = false
		pathLen = len(splitPath)
	}

	config.G.Log.System.LogDebug("Done processing metric, returning pipeline for execution.")
	return pipe
}
