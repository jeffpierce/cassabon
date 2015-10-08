// Datastore implements workers that work with the Redis Index and Cassandra Metric datastores
package datastore

import (
	"encoding/json"
	"fmt"
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

// MetricResponse defines the individual elements returned as an array by "GET /paths".
type MetricResponse struct {
	Path   string `json:"path"`
	Depth  int    `json:"depth"`
	Tenant string `json:"tenant"`
	Leaf   bool   `json:"leaf"`
}

func (gopher *StatPathGopher) Init() {
}

func (gopher *StatPathGopher) Start() {
	config.G.OnReload2WG.Add(1)
	go gopher.run()
}

func (gopher *StatPathGopher) run() {

	defer config.G.OnPanic()

	// Initalize Redis client pool.
	var err error
	if config.G.Redis.Sentinel {
		config.G.Log.System.LogDebug("Gopher initializing Redis client (Sentinel)")
		gopher.rc, err = middleware.RedisFailoverClient(
			config.G.Redis.Addr,
			config.G.Redis.Pwd,
			config.G.Redis.Master,
			config.G.Redis.DB,
		)
	} else {
		config.G.Log.System.LogDebug("Gopher initializing Redis client")
		gopher.rc, err = middleware.RedisClient(
			config.G.Redis.Addr,
			config.G.Redis.Pwd,
			config.G.Redis.DB,
		)
	}

	if err != nil {
		// Without Redis client we can't do our job, so log, whine, and crash.
		config.G.Log.System.LogFatal("Gopher unable to connect to Redis at %v: %v",
			config.G.Redis.Addr, err)
	}

	defer gopher.rc.Close()
	config.G.Log.System.LogDebug("Gopher Redis client initialized")

	// Wait for queries to arrive, and process them.
	for {
		select {
		case <-config.G.OnReload2:
			config.G.Log.System.LogDebug("Gopher::run received QUIT message")
			config.G.OnReload2WG.Done()
			return
		case gopherQuery := <-config.G.Channels.Gopher:
			go gopher.query(gopherQuery)
		}
	}
}

func (gopher *StatPathGopher) query(q config.IndexQuery) {
	config.G.Log.System.LogDebug("Gopher::query %v", q.Query)

	// Listen to the channel, get string query.
	statQuery := q.Query

	// Split it since we need the node length for the Redis Query
	queryNodes := strings.Split(statQuery, ".")

	// Split on wildcards.
	splitWild := strings.Split(statQuery, "*")

	// Determine if we need a simple query or a complex one.
	// len(splitWild) == 2 and splitWild[1] == "" means we have an ending wildcard only.
	if len(splitWild) == 1 {
		q.Channel <- gopher.noWild(statQuery, len(queryNodes))
	} else if len(splitWild) == 2 && splitWild[1] == "" {
		q.Channel <- gopher.simpleWild(splitWild[0], len(queryNodes))
	} else {
		q.Channel <- gopher.complexWild(splitWild, len(queryNodes))
	}
}

// getMax returns the max range parameter for a ZRANGEBYLEX.
func (gopher *StatPathGopher) getMax(s string) string {

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

// noWild performs fetches for strings with no wildcard characters.
func (gopher *StatPathGopher) noWild(q string, l int) config.IndexQueryResponse {

	// No wild card means we should be retrieving one stat, or none at all.
	queryString := strings.Join([]string{"[", ToBigEndianString(l), ":", q, ":"}, "")
	queryStringMax := gopher.getMax(queryString)

	resp, err := gopher.rc.ZRangeByLex(config.G.Redis.PathKeyname, redis.ZRangeByScore{
		queryString, queryStringMax, 0, 0,
	}).Result()

	if err != nil {
		config.G.Log.System.LogWarn("Redis read error: %v", err)
		return config.IndexQueryResponse{config.IQS_ERROR, []byte("")}
	}
	if len(resp) == 0 {
		// Return an empty array.
		return config.IndexQueryResponse{config.IQS_OK, []byte{'[', ']'}}
	}

	return config.IndexQueryResponse{config.IQS_OK, gopher.processQueryResults(resp, l)}
}

// simpleWild performs fetches for strings with one wildcard char, in the last position.
func (gopher *StatPathGopher) simpleWild(q string, l int) config.IndexQueryResponse {

	// Queries with an ending wild card only are easy, as the response from
	// ZRANGEBYLEX <key> [bigE_len:path [bigE_len:path\xff is the answer.
	queryString := strings.Join([]string{"[", ToBigEndianString(l), ":", q}, "")
	queryStringMax := gopher.getMax(queryString)

	// Perform the query.
	resp, err := gopher.rc.ZRangeByLex(config.G.Redis.PathKeyname, redis.ZRangeByScore{
		queryString, queryStringMax, 0, 0,
	}).Result()

	if err != nil {
		config.G.Log.System.LogWarn("Redis read error: %v", err)
		return config.IndexQueryResponse{config.IQS_ERROR, []byte("")}
	}
	if len(resp) == 0 {
		// Return an empty array.
		return config.IndexQueryResponse{config.IQS_OK, []byte{'[', ']'}}
	}

	return config.IndexQueryResponse{config.IQS_OK, gopher.processQueryResults(resp, l)}
}

// complexWild performs fetches for strings with multiple wildcard characters.
func (gopher *StatPathGopher) complexWild(splitWild []string, l int) config.IndexQueryResponse {

	// Resolve multiple wildcards by pulling in the nodes with length l that start with
	// the first part of the non-wildcard, then filter that set with a regex match.
	var matches []string

	queryString := strings.Join([]string{"[", ToBigEndianString(l), ":", splitWild[0]}, "")
	queryStringMax := gopher.getMax(queryString)

	config.G.Log.System.LogDebug(
		"complexWild querying redis with %s, %s as range", queryString, queryStringMax)

	resp, err := gopher.rc.ZRangeByLex(config.G.Redis.PathKeyname, redis.ZRangeByScore{
		queryString, queryStringMax, 0, 0,
	}).Result()

	config.G.Log.System.LogDebug(
		"Received %v as response from redis.", resp)

	if err != nil {
		config.G.Log.System.LogWarn("Redis read error: %v", err)
		return config.IndexQueryResponse{config.IQS_ERROR, []byte("")}
	}
	if len(resp) == 0 {
		// Return an empty array.
		return config.IndexQueryResponse{config.IQS_OK, []byte{'[', ']'}}
	}

	// Build regular expression to match against results.
	rawRegex := strings.Join(splitWild, `.*`)
	config.G.Log.System.LogDebug("Attempting to compile %s into regex", rawRegex)
	regex, err := regexp.Compile(rawRegex)
	if err != nil {
		errMsg := fmt.Sprintf("Could not compile %s into regex: %v", rawRegex, err)
		config.G.Log.System.LogWarn(errMsg)
		var emsg = struct {
			Errmsg string `json:"errmsg"`
		}{errMsg}
		jsonText, _ := json.Marshal(emsg)
		return config.IndexQueryResponse{config.IQS_BADREQUEST, jsonText}
	}

	for _, iter := range resp {
		config.G.Log.System.LogDebug("Attempting to match %s against %s", rawRegex, iter)
		if regex.MatchString(iter) {
			matches = append(matches, iter)
		}
	}

	if len(matches) > 0 {
		return config.IndexQueryResponse{config.IQS_OK, gopher.processQueryResults(matches, l)}
	} else {
		// Return an empty array.
		return config.IndexQueryResponse{config.IQS_OK, []byte{'[', ']'}}
	}
}

// processQueryResults converts the results to JSON text.
func (gopher *StatPathGopher) processQueryResults(results []string, l int) []byte {

	var responseList []MetricResponse

	// Process the result into a map, make it a string, send it along is the goal here.
	for _, match := range results {
		decodedString := strings.Split(match, ":")
		isLeaf, _ := strconv.ParseBool(decodedString[2])
		m := MetricResponse{decodedString[1], l, "", isLeaf}
		responseList = append(responseList, m)
	}

	jsonText, _ := json.Marshal(responseList)
	return jsonText
}
