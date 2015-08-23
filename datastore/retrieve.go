// Datastore implements workers that work with the Redis Index and Cassandra Metric datastores
package datastore

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/redis.v3"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/middleware"
)

type StatPathGopher struct {
	rc *redis.Client // Redis client connection
}

func (gopher *StatPathGopher) Init() {
	// Add to waitgroup and run goroutine.
	config.G.WG.Add(1)
	go gopher.run()
}

func (gopher *StatPathGopher) run() {
	// Initalize redis client
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

	// Wait for a stat path query to arrive, then process it.
	for {
		select {
		case <-config.G.QuitListener:
			config.G.Log.System.LogInfo("Gopher::run received QUIT message")
			config.G.WG.Done()
			return

		case gopherQuery := <-config.G.Channels.Gopher:
			config.G.Log.System.LogDebug("Gopher received a channel, processing...")
			go gopher.query(gopherQuery)
		}
	}
}

func (gopher *StatPathGopher) query(q config.IndexQuery) {
	// Listen to the channel, get string query.
	statQuery := q.Query

	// Split it since we need the node length for the Redis Query
	queryNodes := strings.Split(statQuery, ".")

	// Split on wildcards.
	splitWild := strings.Split(statQuery, "*")

	// Determine if we need a simple query or a complex one.
	// len(splitWild) == 2 and splitWild[-1] == "" means we have an ending wildcard only.
	if len(splitWild) == 1 {
		q.Channel <- gopher.noWild(statQuery, len(queryNodes))
	} else if len(splitWild) == 2 && splitWild[1] == "" {
		q.Channel <- gopher.simpleWild(splitWild[0], len(queryNodes))
	} else {
		q.Channel <- gopher.complexWild(splitWild, len(queryNodes))
	}
}

func (gopher *StatPathGopher) bigEit(i int) string {
	a := make([]byte, 2)
	binary.BigEndian.PutUint16(a, uint16(i))
	return hex.EncodeToString(a)
}

func (gopher *StatPathGopher) simpleWild(q string, l int) string {
	// Queries with an ending wild card only are easy, as the response from
	// ZRANGEBYLEX cassabon [bigE_len:path [bigE_len:path\xff is the answer.
	var queryStringMax string

	queryString := strings.Join([]string{"[", gopher.bigEit(l), ":", q}, "")
	if queryString[len(queryString)-1:] == "." {
		// If a dot is on the end, the max path has to have a \ put before the final dot.
		queryStringMax = strings.Join([]string{
			queryString[:len(queryString)-1],
			"\\.\xff",
		}, "",
		)
	} else {
		// If a dot's not on the end, just append "\xff"
		queryStringMax = strings.Join([]string{queryString, "\xff"}, "")
	}

	// Perform the query.
	resp, err := gopher.rc.ZRangeByLex("cassabon", redis.ZRangeByScore{
		queryString, queryStringMax, 0, 0,
	}).Result()

	if err != nil {
		// Errored, return empty set.
		return "[]"
	}

	// Send query results off to be processed into a string and return them.
	return gopher.processQueryResults(resp, l)
}

func (gopher *StatPathGopher) noWild(q string, l int) string {
	// No wild card means we should be retrieving one stat, or none at all.
	queryString := strings.Join([]string{"[", gopher.bigEit(l), ":", q, ":"}, "")

	resp, err := gopher.rc.ZRangeByLex("cassabon", redis.ZRangeByScore{
		queryString, strings.Join([]string{queryString, "\xff"}, ""), 0, 0,
	}).Result()

	if err != nil || len(resp) == 0 {
		// Error or empty set, return an empty set.
		return "[]"
	}

	return gopher.processQueryResults(resp, l)
}

func (gopher *StatPathGopher) complexWild(splitWild []string, l int) string {
	// Stub.
	return "[]"
}

func (gopher *StatPathGopher) processQueryResults(results []string, l int) string {
	var parsedResponse []map[string]interface{}
	// Process the result into a map, make it a string, send it along is the goal here.
	for _, match := range results {
		decodedString := strings.Split(match, ":")
		isLeaf, _ := strconv.ParseBool(decodedString[2])
		stringMap := make(map[string]interface{})
		stringMap["path"] = decodedString[1]
		stringMap["depth"] = l
		stringMap["tenant"] = ""
		stringMap["leaf"] = isLeaf
		parsedResponse = append(parsedResponse, stringMap)
	}

	r := fmt.Sprintf("%v", parsedResponse)
	return r
}
