// Datastore implements workers that work with the Redis Index and Cassandra Metric datastores
package datastore

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/redis.v3"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/middleware"
)

type StatPathGopher struct {
	rc *redis.Client // Redis client connection
}

type MetricResponse struct {
	Path   string `json:"path"`
	Depth  int    `json:"depth"`
	Tenant string `json:"tenant"`
	Leaf   bool   `json:"leaf"`
}

func (gopher *StatPathGopher) Init() {
}

func (gopher *StatPathGopher) Start() {
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
			config.G.Log.System.LogDebug("Gopher::run received QUIT message")
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

func (gopher *StatPathGopher) getMax(s string) string {
	// Returns the max range parameter for a ZRANGEBYLEX
	var max string

	if s[len(s)-1:] == "." || s[len(s)-1:] == ":" {
		// If a dot is on the end, the max path has to have a \ put before the final dot.
		max = strings.Join([]string{s[:len(s)-1], `\`, s[len(s)-1:], `\xff`}, "")
	} else {
		// If a dot's not on the end, just append "\xff"
		max = strings.Join([]string{s, `\xff`}, "")
	}

	return max
}

func (gopher *StatPathGopher) simpleWild(q string, l int) []byte {
	// Queries with an ending wild card only are easy, as the response from
	// ZRANGEBYLEX cassabon [bigE_len:path [bigE_len:path\xff is the answer.
	queryString := strings.Join([]string{"[", gopher.bigEit(l), ":", q}, "")
	queryStringMax := gopher.getMax(queryString)

	// Perform the query.
	resp, err := gopher.rc.ZRangeByLex("cassabon", redis.ZRangeByScore{
		queryString, queryStringMax, 0, 0,
	}).Result()

	if err != nil || len(resp) == 0 {
		// Errored, return empty set.
		config.G.Log.System.LogWarn("Redis error or zero length response.")
		return nil
	}

	// Send query results off to be processed into a string and return them.
	return gopher.processQueryResults(resp, l)
}

func (gopher *StatPathGopher) noWild(q string, l int) []byte {
	// No wild card means we should be retrieving one stat, or none at all.
	queryString := strings.Join([]string{"[", gopher.bigEit(l), ":", q, ":"}, "")
	queryStringMax := gopher.getMax(queryString)

	resp, err := gopher.rc.ZRangeByLex("cassabon", redis.ZRangeByScore{
		queryString, queryStringMax, 0, 0,
	}).Result()

	if err != nil || len(resp) == 0 {
		// Error or empty set, return an empty set.
		config.G.Log.System.LogInfo("Redis error or zero length response.")
		return nil
	}

	return gopher.processQueryResults(resp, l)
}

func (gopher *StatPathGopher) complexWild(splitWild []string, l int) []byte {
	// Resolve multiple wildcards by pulling in the nodes with length l that start with
	// the first part of the non-wildcard, then filter that set with a regex match.
	var matches []string

	queryString := strings.Join([]string{"[", gopher.bigEit(l), ":", splitWild[0]}, "")
	queryStringMax := gopher.getMax(queryString)

	config.G.Log.System.LogDebug(
		"complexWild querying redis with %s, %s as range", queryString, queryStringMax)

	resp, err := gopher.rc.ZRangeByLex("cassabon", redis.ZRangeByScore{
		queryString, queryStringMax, 0, 0,
	}).Result()

	config.G.Log.System.LogDebug(
		"Received %v as response from redis.", resp)

	if err != nil || len(resp) == 0 {
		config.G.Log.System.LogInfo("Redis error or zero length response.")
		return nil
	}

	// Build regular expression to match against results.
	rawRegex := strings.Join(splitWild, `.*`)
	config.G.Log.System.LogDebug("Attempting to compile %s into regex", rawRegex)

	regex, err := regexp.Compile(rawRegex)
	if err != nil {
		config.G.Log.System.LogError("Could not compile %s into regex, %v", rawRegex, err)
		return nil
	}

	for _, iter := range resp {
		config.G.Log.System.LogDebug("Attempting to match %s against %s", rawRegex, iter)
		if regex.MatchString(iter) {
			matches = append(matches, iter)
		}
	}

	return gopher.processQueryResults(matches, l)
}

func (gopher *StatPathGopher) processQueryResults(results []string, l int) []byte {
	var responseList []MetricResponse
	// Process the result into a map, make it a string, send it along is the goal here.
	for _, match := range results {
		decodedString := strings.Split(match, ":")
		isLeaf, _ := strconv.ParseBool(decodedString[2])
		m := MetricResponse{decodedString[1], l, "", isLeaf}
		responseList = append(responseList, m)
	}

	j, _ := json.Marshal(responseList)
	return j
}
