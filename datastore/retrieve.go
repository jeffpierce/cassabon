// Datastore implements workers that work with the Redis Index and Cassandra Metric datastores
package datastore

import (
	//	"encoding/binary"
	//	"encoding/hex"
	"os"
	//	"strconv"
	//	"strings"

	"gopkg.in/redis.v3"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/middleware"
)

type StatPathGopher struct {
	rc   *redis.Client          // Redis client connection
	todo chan config.IndexQuery // Query receiver
}

func (gopher *StatPathGopher) Init() {
	// Initalize redis client and work channel
	if config.G.Redis.Index.Sentinel {
		config.G.Log.System.LogDebug("Initializing Stat Path Gopher Redis client (Sentinel)...")
		gopher.rc = middleware.RedisFailoverClient(
			config.G.Redis.Index.Addr,
			config.G.Redis.Index.Pwd,
			config.G.Redis.Index.Master,
			config.G.Redis.Index.DB,
		)
	} else {
		config.G.Log.System.LogDebug("Initializing Stat Path Gopher Redis client...")
		gopher.rc = middleware.RedisClient(
			config.G.Redis.Index.Addr,
			config.G.Redis.Index.Pwd,
			config.G.Redis.Index.DB,
		)
	}

	if gopher.rc == nil {
		// Can't connect to Redis.  Log, whine, crash.
		config.G.Log.System.LogFatal("Unable to connect to Redis for Gopher at %v", config.G.Redis.Index.Addr)
		os.Exit(11)
	}

	defer gopher.rc.Close()
	config.G.Log.System.LogDebug("Gopher redis client initialized.")

	config.G.WG.Add(1)
	go gopher.run()
}

func (gopher *StatPathGopher) run() StatPathGopher.result {
	// Wait for a stat path query to arrive, then process it.
	for {
		select {
		case <-config.G.QuitListener:
			config.G.Log.System.LogInfo("Gopher::run received QUIT message")
			config.G.WG.Done()
			return

		case gopherQuery := <-config.G.Channels.Gopher:
			config.G.Log.System.LogDebug("Gopher received a stat path query: %v", gopherQuery)

			// Send to processor function.
			resp := lookup(gopherQuery)

			// Send it back down the channel.
			config.G.Channels.Gopher <- resp
		}
	}
}

func (gopher *StatPathGopher) lookup(query config.IndexQuery) {
	// Parse query and send to redis
	config.G.Log.System.LogDebug("Received query: %v", config.IndexQuery)
}
