// Datastore implements workers that work with the Redis Index and Cassandra Metric datastores
package datastore

import (
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
}

func (indexer *MetricsIndexer) Start() {
	config.G.OnReload2WG.Add(1)
	go indexer.run()
}

func (indexer *MetricsIndexer) run() {

	// Initialize Redis client pool.
	var err error
	if config.G.Redis.Index.Sentinel {
		config.G.Log.System.LogDebug("Indexer initializing Redis client (Sentinel)")
		indexer.rc, err = middleware.RedisFailoverClient(
			config.G.Redis.Index.Addr,
			config.G.Redis.Index.Pwd,
			config.G.Redis.Index.Master,
			config.G.Redis.Index.DB,
		)
	} else {
		config.G.Log.System.LogDebug("Indexer initializing Redis client")
		indexer.rc, err = middleware.RedisClient(
			config.G.Redis.Index.Addr,
			config.G.Redis.Index.Pwd,
			config.G.Redis.Index.DB,
		)
	}

	if err != nil {
		// Without Redis client we can't do our job, so log, whine, and crash.
		config.G.Log.System.LogFatal("Indexer unable to connect to Redis at %v: %v",
			config.G.Redis.Index.Addr, err)
		os.Exit(10)
	}

	defer indexer.rc.Close()
	config.G.Log.System.LogDebug("Indexer Redis client initialized")

	// Wait for entries to arrive, and process them.
	for {
		select {
		case <-config.G.OnReload2:
			config.G.Log.System.LogDebug("Indexer::run received QUIT message")
			config.G.OnReload2WG.Done()
			return
		case metric := <-config.G.Channels.IndexStore:
			indexer.index(metric.Path)
		}
	}
}

// IndexMetricPath takes a metric path string and redis client, starts a pipeline, splits the metric,
// and sends it off to be processed by processMetricPath().
func (indexer *MetricsIndexer) index(path string) {
	config.G.Log.System.LogDebug("Indexer::index path=%s", path)
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

		// Construct the metric string
		metricPath := strings.Join([]string{
			middleware.ToBigEndianString(pathLen),
			strings.Join(splitPath, "."),
			strconv.FormatBool(isLeaf)}, ":")
		config.G.Log.System.LogDebug("Indexer indexing \"%s\"", metricPath)

		z := redis.Z{0, metricPath}

		// Put it in the pipeline.
		pipe.ZAdd("cassabon", z)

		// Pop the last node of the metric off, set isLeaf to false, and resume loop.
		_, splitPath = splitPath[len(splitPath)-1], splitPath[:len(splitPath)-1]
		isLeaf = false
		pathLen = len(splitPath)
	}

	_, err := pipe.Exec()
	if err != nil {
		// How do we want to degrade gracefully when this fails?
		config.G.Log.System.LogError("Redis error: %v", err)
	}
}
