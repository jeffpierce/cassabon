// Datastore implements workers that work with the Redis Index and Cassandra Metric datastores
package datastore

import (
	"encoding/binary"
	"encoding/hex"
	"os"
	"strconv"
	"strings"

	"gopkg.in/redis.v3"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/middleware"
)

type MetricsIndexer struct {
	rc *redis.Client
}

func (indexer *MetricsIndexer) Init() {
	// Start the goroutines.
	config.G.WG.Add(1)
	go indexer.run()
}

func (indexer *MetricsIndexer) run() {

	// Initialize Redis client pool.
	if config.G.Redis.Index.Sentinel {
		config.G.Log.System.LogDebug("Initializing Indexer Redis client (Sentinel)...")
		indexer.rc = middleware.RedisFailoverClient(
			config.G.Redis.Index.Addr,
			config.G.Redis.Index.Pwd,
			config.G.Redis.Index.Master,
			config.G.Redis.Index.DB,
		)
	} else {
		config.G.Log.System.LogDebug("Initializing Indexer Redis client...")
		indexer.rc = middleware.RedisClient(
			config.G.Redis.Index.Addr,
			config.G.Redis.Index.Pwd,
			config.G.Redis.Index.DB,
		)
	}

	if indexer.rc == nil {
		// Make sure we have a good Redis client.  Without it, we can't do our job, so log, whine, and crash.
		config.G.Log.System.LogFatal("Unable to connect to Redis for indexer at %v.", config.G.Redis.Index.Addr)
		os.Exit(10)
	}

	config.G.Log.System.LogDebug("Indexer client initialized.")
	defer indexer.rc.Close()

	// Wait for entries to arrive, and process them.
	for {
		select {
		case <-config.G.QuitListener:
			config.G.Log.System.LogInfo("Indexer::run received QUIT message")
			config.G.WG.Done()
			return
		case metric := <-config.G.Channels.Indexer:
			config.G.Log.Carbon.LogDebug("Indexer received metric: %v", metric)
			go indexer.indexMetricPath(metric.Path)
		}
	}
}

// IndexMetricPath takes a metric path string and redis client, starts a pipeline, splits the metric,
// and sends it off to be processed by processMetricPath().
func (indexer *MetricsIndexer) indexMetricPath(path string) {
	splitPath := strings.Split(path, ".")
	indexer.processMetricPath(splitPath, len(splitPath), true)
}

// processMetricPath recursively indexes the metric path via the redis pipeline.
func (indexer *MetricsIndexer) processMetricPath(splitPath []string, pathLen int, isLeaf bool) {
	// Process the metric path one node at a time.  We store metrics in Redis as a sorted set with score
	// 0 so that lexicographical search works.  Metrics are in the format of:
	//
	// big_endian_length:metric.path:true_or_false
	//
	// This keeps them ordered so that a ZRANGEBYLEX works when finding the next nodes in a path branch.

	pipe := indexer.rc.Pipeline()

	for pathLen > 0 {
		// Let's get our big endian representation of the length.
		a := make([]byte, 2)
		binary.BigEndian.PutUint16(a, uint16(pathLen))
		bigE := hex.EncodeToString(a)

		// Construct the metric string
		metricPath := strings.Join([]string{
			bigE,
			strings.Join(splitPath, "."),
			strconv.FormatBool(isLeaf)}, ":")

		z := redis.Z{0, metricPath}

		config.G.Log.System.LogDebug("Indexing metric %s", metricPath)

		// Put it in the pipeline.
		pipe.ZAdd("cassabon", z)

		// Pop the last node of the metric off, set isLeaf to false, and resume loop.
		_, splitPath = splitPath[len(splitPath)-1], splitPath[:len(splitPath)-1]
		isLeaf = false
		pathLen = len(splitPath)
	}

	_, err := pipe.Exec()
	if err != nil {
		// Warn for now, we can change this later if we care.
		config.G.Log.System.LogWarn("Received error when communicating with Redis: %v", err.Error())
	} else {
		config.G.Log.System.LogDebug("Finished processing full metric path.")
	}
}
