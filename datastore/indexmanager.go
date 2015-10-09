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

// MetricResponse defines the individual elements returned as an array by "GET /paths".
type MetricResponse struct {
	Path   string `json:"path"`
	Depth  int    `json:"depth"`
	Tenant string `json:"tenant"`
	Leaf   bool   `json:"leaf"`
}

type IndexManager struct {
	rc *redis.Client
}

func (im *IndexManager) Init() {
}

func (im *IndexManager) Start() {
	config.G.OnReload2WG.Add(1)
	go im.run()
}

func (im *IndexManager) run() {

	defer config.G.OnPanic()

	// Initialize Redis client pool.
	var err error
	if config.G.Redis.Sentinel {
		config.G.Log.System.LogDebug("IndexManager initializing Redis client (Sentinel)")
		im.rc, err = middleware.RedisFailoverClient(
			config.G.Redis.Addr,
			config.G.Redis.Pwd,
			config.G.Redis.Master,
			config.G.Redis.DB,
		)
	} else {
		config.G.Log.System.LogDebug("IndexManager initializing Redis client")
		im.rc, err = middleware.RedisClient(
			config.G.Redis.Addr,
			config.G.Redis.Pwd,
			config.G.Redis.DB,
		)
	}

	if err != nil {
		// Without Redis client we can't do our job, so log, whine, and crash.
		config.G.Log.System.LogFatal("IndexManager unable to connect to Redis at %v: %s",
			config.G.Redis.Addr, err.Error())
	}

	defer im.rc.Close()
	config.G.Log.System.LogDebug("IndexManager Redis client initialized")

	// Wait for entries to arrive, and process them.
	for {
		select {
		case <-config.G.OnReload2:
			config.G.Log.System.LogDebug("IndexManager::run received QUIT message")
			config.G.OnReload2WG.Done()
			return
		case metric := <-config.G.Channels.IndexStore:
			im.index(metric.Path)
		case query := <-config.G.Channels.IndexFetch:
			go im.query(query)
		}
	}
}

// IndexMetricPath takes a metric path string and redis client, starts a pipeline, splits the metric,
// and sends it off to be processed by processMetricPath().
func (im *IndexManager) index(path string) {
	config.G.Log.System.LogDebug("IndexManager::index path=%s", path)
	splitPath := strings.Split(path, ".")
	im.processMetricPath(splitPath, len(splitPath), true)
}

// processMetricPath recursively indexes the metric path via the redis pipeline.
func (im *IndexManager) processMetricPath(splitPath []string, pathLen int, isLeaf bool) {
	// Process the metric path one node at a time.  We store metrics in Redis as a sorted set with score
	// 0 so that lexicographical search works.  Metrics are in the format of:
	//
	// big_endian_length:metric.path:true_or_false
	//
	// This keeps them ordered so that a ZRANGEBYLEX works when finding the next nodes in a path branch.

	pipe := im.rc.Pipeline()

	for pathLen > 0 {

		// Construct the metric string
		metricPath := strings.Join([]string{
			ToBigEndianString(pathLen),
			strings.Join(splitPath, "."),
			strconv.FormatBool(isLeaf)}, ":")
		config.G.Log.System.LogDebug("IndexManager indexing \"%s\"", metricPath)

		z := redis.Z{0, metricPath}

		// Put it in the pipeline.
		pipe.ZAdd(config.G.Redis.PathKeyname, z)

		// Pop the last node of the metric off, set isLeaf to false, and resume loop.
		_, splitPath = splitPath[len(splitPath)-1], splitPath[:len(splitPath)-1]
		isLeaf = false
		pathLen = len(splitPath)
	}

	_, err := pipe.Exec()
	if err != nil {
		// How do we want to degrade gracefully when this fails?
		config.G.Log.System.LogError("Redis error: %s", err.Error())
	}
}

// query returns the data matched by the supplied query.
func (im *IndexManager) query(q config.IndexQuery) {

	config.G.Log.System.LogDebug("IndexManager::query %v", q.Query)

	// Query particulars are mandatory.
	if q.Query == "" {
		q.Channel <- config.IndexQueryResponse{config.IQS_BADREQUEST, "no query specified", []byte{}}
		return
	}

	// Split it since we need the node length for the Redis Query
	queryNodes := strings.Split(q.Query, ".")

	// Split on wildcards.
	splitWild := strings.Split(q.Query, "*")

	// Determine if we need a simple query or a complex one.
	// len(splitWild) == 2 and splitWild[1] == "" means we have an ending wildcard only.
	var resp config.IndexQueryResponse
	if len(splitWild) == 1 {
		resp = im.noWild(q.Query, len(queryNodes))
	} else if len(splitWild) == 2 && splitWild[1] == "" {
		resp = im.simpleWild(splitWild[0], len(queryNodes))
	} else {
		resp = im.complexWild(splitWild, len(queryNodes))
	}

	// If the API gave up on us because we took too long, writing to the channel
	// will cause first a data race, and then a panic (write on closed channel).
	// We check, but if we lose a race we will need to recover.
	defer func() {
		_ = recover()
	}()

	// Check whether the channel is closed before attempting a write.
	select {
	case <-q.Channel:
		// Immediate return means channel is closed (we know there is no data in it).
	default:
		// If the channel would have blocked, it is open, we can write to it.
		q.Channel <- resp
	}
}

// getMax returns the max range parameter for a ZRANGEBYLEX.
func (im *IndexManager) getMax(s string) string {

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
func (im *IndexManager) noWild(q string, l int) config.IndexQueryResponse {

	// No wild card means we should be retrieving one stat, or none at all.
	queryString := strings.Join([]string{"[", ToBigEndianString(l), ":", q, ":"}, "")
	queryStringMax := im.getMax(queryString)

	resp, err := im.rc.ZRangeByLex(config.G.Redis.PathKeyname, redis.ZRangeByScore{
		queryString, queryStringMax, 0, 0,
	}).Result()

	if err != nil {
		config.G.Log.System.LogWarn("Redis read error: %s", err.Error())
		return config.IndexQueryResponse{config.IQS_ERROR, err.Error(), []byte{}}
	}
	if len(resp) == 0 {
		// Return an empty array.
		return config.IndexQueryResponse{config.IQS_OK, "", []byte{'[', ']'}}
	}

	return config.IndexQueryResponse{config.IQS_OK, "", im.processQueryResults(resp, l)}
}

// simpleWild performs fetches for strings with one wildcard char, in the last position.
func (im *IndexManager) simpleWild(q string, l int) config.IndexQueryResponse {

	// Queries with an ending wild card only are easy, as the response from
	// ZRANGEBYLEX <key> [bigE_len:path [bigE_len:path\xff is the answer.
	queryString := strings.Join([]string{"[", ToBigEndianString(l), ":", q}, "")
	queryStringMax := im.getMax(queryString)

	// Perform the query.
	resp, err := im.rc.ZRangeByLex(config.G.Redis.PathKeyname, redis.ZRangeByScore{
		queryString, queryStringMax, 0, 0,
	}).Result()

	if err != nil {
		config.G.Log.System.LogWarn("Redis read error: %s", err.Error())
		return config.IndexQueryResponse{config.IQS_ERROR, err.Error(), []byte{}}
	}
	if len(resp) == 0 {
		// Return an empty array.
		return config.IndexQueryResponse{config.IQS_OK, "", []byte{'[', ']'}}
	}

	return config.IndexQueryResponse{config.IQS_OK, "", im.processQueryResults(resp, l)}
}

// complexWild performs fetches for strings with multiple wildcard characters.
func (im *IndexManager) complexWild(splitWild []string, l int) config.IndexQueryResponse {

	// Resolve multiple wildcards by pulling in the nodes with length l that start with
	// the first part of the non-wildcard, then filter that set with a regex match.
	var matches []string

	queryString := strings.Join([]string{"[", ToBigEndianString(l), ":", splitWild[0]}, "")
	queryStringMax := im.getMax(queryString)

	config.G.Log.System.LogDebug(
		"complexWild querying redis with %s, %s as range", queryString, queryStringMax)

	resp, err := im.rc.ZRangeByLex(config.G.Redis.PathKeyname, redis.ZRangeByScore{
		queryString, queryStringMax, 0, 0,
	}).Result()

	config.G.Log.System.LogDebug(
		"Received %v as response from redis.", resp)

	if err != nil {
		config.G.Log.System.LogWarn("Redis read error: %s", err.Error())
		return config.IndexQueryResponse{config.IQS_ERROR, err.Error(), []byte{}}
	}
	if len(resp) == 0 {
		// Return an empty array.
		return config.IndexQueryResponse{config.IQS_OK, "", []byte{'[', ']'}}
	}

	// Build regular expression to match against results.
	rawRegex := strings.Join(splitWild, `.*`)
	config.G.Log.System.LogDebug("Attempting to compile %s into regex", rawRegex)
	regex, err := regexp.Compile(rawRegex)
	if err != nil {
		errMsg := fmt.Sprintf("Could not compile %s into regex: %s", rawRegex, err.Error())
		config.G.Log.System.LogWarn(errMsg)
		return config.IndexQueryResponse{config.IQS_BADREQUEST, errMsg, []byte{}}
	}

	for _, iter := range resp {
		config.G.Log.System.LogDebug("Attempting to match %s against %s", rawRegex, iter)
		if regex.MatchString(iter) {
			matches = append(matches, iter)
		}
	}

	if len(matches) > 0 {
		return config.IndexQueryResponse{config.IQS_OK, "", im.processQueryResults(matches, l)}
	} else {
		// Return an empty array.
		return config.IndexQueryResponse{config.IQS_OK, "", []byte{'[', ']'}}
	}
}

// processQueryResults converts the results to JSON text.
func (im *IndexManager) processQueryResults(results []string, l int) []byte {

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
