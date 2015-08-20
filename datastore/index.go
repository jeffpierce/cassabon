// Datastore implements workers that work with the Redis Index and Cassandra Metric datastores
package datastore

import (
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/middleware"
	"gopkg.in/redis.v3"
)

// IndexMetricPath takes a metric path string, splits it, and sends it off to be recursively parsed and indexed.
func IndexMetricPath(path string) {
	// We've got a path!
	config.G.Log.System.LogDebug("Indexer received path %s.")

	// Split the metric into a slice of its parts
	splitPath := strings.Split(path, '.')

	// Initialize a Redis client.
	if config.G.Redis.Index.Sentinel {
		config.G.Log.System.LogDebug("Initializing Redis client (Sentinel)")
		rc := middleware.RedisFailoverClient(
			config.G.Redis.Index.Addr,
			config.G.Redis.Index.Pwd,
			config.G.Redis.Index.Master,
			config.G.Redis.Index.DB,
		)
	} else {
		config.G.Log.System.LogDebug("Initializing Redis client...")
		rc := middleware.RedisClient(
			config.G.Redis.Index.Addr,
			config.G.Redis.Index.Pwd,
			config.G.Redis.Index.DB,
		)
	}

	// Ship the metric and a Redis pipeline to the metric processor.
	pipe := rc.Pipeline()

	pipe = processMetricPath(&splitPath, len(splitPath), true, pipe)

	pipe.Exec()
	pipe.Close()
}

// processMetricPath recursively processes
func processMetricPath(splitPath *[]string, pathLen int, isLeaf bool, pipe *redis.Pipeline) *redis.Pipeline {
	// Process the metric path one node at a time.  We store metrics in Redis as a sorted set with score
	// 0 so that lexicographical search works.  Metrics are in the format of:
	//
	// big_endian_length:metric.path:true_or_false
	//
	// This keeps them ordered so that a ZRANGEBYLEX works when finding the next nodes in a path branch.

	// If we've processed the whole thing, return the pipe to be executed.
	if pathLen == 0 {
		config.G.Log.System.LogDebug("Done processing metric, returning pipeline for execution.")
		return pipe
	}

	// Let's get our big endian representation of the length.
	a := make([]byte, 2)
	binary.BigEndian.PutUint16(a, uint16(pathLen))
	bigE = hex.EncodeToString(a)

	// Construct the metric string
	metric := strings.Join([]string{
		bigE,
		strings.Join(splitPath),
		strconv.FormatBool(isLeaf)}, ':')

	config.G.Log.System.LogDebug("Indexing metric %s", metric)

	// Put it in the pipeline.
	pipe.ZAdd("cassabon", 0, metric)

	// Pop the last node of the metric off, and send back for processing the rest of the node.
	_, splitPath = splitPath[len(splitPath)-1], splitPath[:len(a)-1]

	// Decrement length rather than counting the length of the new slice, we know it's just 1 less
	pathLen--

	processMetricPath(splitPath, pathLen, false, pipe)
}
