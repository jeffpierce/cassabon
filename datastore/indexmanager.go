package datastore

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/otium/queue"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/logging"
)

// IndexResponse defines the individual elements returned as an array by "GET /paths".
type IndexResponse struct {
	Path   string `json:"path"`
	Depth  int    `json:"depth"`
	Tenant string `json:"tenant"`
	Leaf   bool   `json:"leaf"`
}

// ElasticResponse is the struct we unmarshal the response from an ElasticSearch query to.
type ElasticResponse struct {
	Took     int      `json:"took"`
	TimedOut bool     `json:"timed_out"`
	Shards   ERShards `json:"_shards"`
	Hits     ERHits   `json:"hits"`
}

type ERShards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type ERHits struct {
	Total    int           `json:"total"`
	MaxScore float32       `json:"max_score"`
	Hits     []ERSearchHit `json:"hits"`
}

type ERSearchHit struct {
	Index  string        `json:"_index"`
	Type   string        `json:"_type"`
	ID     string        `json:"_id"`
	Score  float32       `json:"_score"`
	Source IndexResponse `json:"_source"`
}

type ERQuery struct {
	Sort  []map[string]map[string]string                            `json:"sort"`
	Query map[string]map[string][]map[string]map[string]interface{} `json:"query"`
}

type IndexManager struct {
	wg         *sync.WaitGroup
	IndexQueue *queue.Queue
}

func (im *IndexManager) Init(bootstrap bool) {
	// If bootstrap is true, initialize mapping in ElasticSearch
	if bootstrap {
		im.initMapping()
	}

	// Initialize index worker queue.
	im.IndexQueue = queue.NewQueue(func(metricPath interface{}) {
		if path, ok := metricPath.(string); ok {
			im.index(path)
		}
	}, 100)
}

func (im *IndexManager) Start(wg *sync.WaitGroup) {
	im.wg = wg
	im.wg.Add(1)
	go im.run()
}

func (im *IndexManager) run() {

	defer config.G.OnPanic()

	// Wait for entries to arrive, and process them.
	for {
		select {
		case <-config.G.OnReload2:
			config.G.Log.System.LogDebug("IndexManager::run received QUIT message")
			im.wg.Done()
			return
		case metric := <-config.G.Channels.IndexStore:
			im.IndexQueue.Push(metric.Path)
		case query := <-config.G.Channels.IndexRequest:
			go im.query(query)
		}
	}
}

// initMapping initializes ElasticSearch for cassabon.
func (im *IndexManager) initMapping() {
	mapping := map[string]map[string]map[string]map[string]map[string]string{
		"mappings": map[string]map[string]map[string]map[string]string{
			"path": map[string]map[string]map[string]string{
				"properties": map[string]map[string]string{
					"path": map[string]string{
						"type":  "string",
						"index": "not_analyzed",
					},
					"depth": map[string]string{
						"type": "long",
					},
					"tenant": map[string]string{
						"type": "string",
					},
					"leaf": map[string]string{
						"type": "boolean",
					},
				},
			},
		},
	}

	jsonMap, _ := json.Marshal(mapping)
	config.G.Log.System.LogDebug("%s", string(jsonMap))

	putreq, _ := http.NewRequest("PUT", config.G.ElasticSearch.MapURL, bytes.NewBuffer(jsonMap))
	r := im.httpRequest(putreq)

	config.G.Log.System.LogDebug("%v", string(r))

	if r == nil {
		config.G.Log.System.LogFatal("Could not initialize mapping for ElasticSearch.")
	}
}

// getAllLeafNodes queries ElasticSearch for all leaf nodes. Used for populating metric manager's stat paths on reboot.
func (im *IndexManager) getAllLeafNodes() []string {
	sort := []map[string]map[string]string{
		{
			"path": map[string]string{
				"order": "asc",
			},
		},
	}
	query := map[string]map[string][]map[string]map[string]interface{}{
		"bool": map[string][]map[string]map[string]interface{}{
			"must": []map[string]map[string]interface{}{
				{
					"match": map[string]interface{}{
						"leaf": true,
					},
				},
			},
		},
	}

	fullQuery := ERQuery{sort, query}
	getreq := im.prepRequest(fullQuery)
	r := im.httpRequest(getreq)

	var esResp ElasticResponse
	var pathList []string

	if r != nil {
		_ = json.Unmarshal(r, &esResp)

		config.G.Log.System.LogDebug("esResp: %v", esResp)

		for _, hit := range esResp.Hits.Hits {
			pathList = append(pathList, hit.Source.Path)
		}
	} else {
		logging.Statsd.Client.Inc("indexmgr.es.err.get", 1, 1.0)
		config.G.Log.System.LogError("Error querying ES.")
	}

	config.G.Log.System.LogDebug("Retrieved %v stat paths.", len(pathList))
	return pathList
}

func (im *IndexManager) prepRequest(fullQuery ERQuery) *http.Request {
	jsonQuery, _ := json.Marshal(fullQuery)
	config.G.Log.System.LogDebug("%s", string(jsonQuery))

	// Get the count so that we capture all of the possible paths.
	countreq, _ := http.NewRequest("GET", config.G.ElasticSearch.CountURL, strings.NewReader(string(jsonQuery)))
	size := "size=" + im.getCount(countreq)

	searchURL := strings.Join([]string{config.G.ElasticSearch.SearchURL, size}, "?")
	getreq, _ := http.NewRequest("GET", searchURL, strings.NewReader(string(jsonQuery)))

	return getreq
}

// IndexMetricPath takes a metric path string and sends it off to be processed by processMetricPath().
func (im *IndexManager) index(path string) {
	it := time.Now()
	config.G.Log.System.LogDebug("IndexManager::index path=%s", path)
	splitPath := strings.Split(path, ".")
	im.processMetricPath(splitPath, len(splitPath), true)
	logging.Statsd.Client.TimingDuration("indexmgr.index", time.Since(it), 1.0)
}

func (im *IndexManager) httpRequest(req *http.Request) []byte {
	client := &http.Client{Timeout: time.Duration(15 * time.Second)}
	resp, err := client.Do(req)

	if err != nil {
		logging.Statsd.Client.Inc("indexmgr.es.err.httpreq", 1, 1.0)
		config.G.Log.System.LogError("Received error from ElasticSearch: %v, request: %v", err.Error(), req)
		return nil
	}

	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	return body
}

// processMetricPath recursively indexes the metric path via the ElasticSearch REST API.
func (im *IndexManager) processMetricPath(splitPath []string, pathLen int, isLeaf bool) {
	// Process the metric path one node at a time.  We store metrics in ElasticSearch.
	retries := 0
	for pathLen > 0 {

		// Construct the metric string
		metricPath := strings.Join(splitPath, ".")

		// Strip % off the end to avoid invalid escape errors.
		if string(metricPath[len(metricPath)-1]) == "%" {
			metricPath = metricPath[:len(metricPath)-1]
		}
		config.G.Log.System.LogDebug("IndexManager indexing \"%s\"", metricPath)

		pathToIndex := IndexResponse{
			metricPath,
			pathLen,
			"",
			isLeaf,
		}

		urlToPut := strings.Join([]string{config.G.ElasticSearch.PutURL, metricPath}, "/")

		// Marshal the struct into JSON
		jsonPath, err := json.Marshal(pathToIndex)

		if err != nil {
			logging.Statsd.Client.Inc("indexmgr.es.err.json", 1, 1.0)
			config.G.Log.System.LogError("Unable to marshal pathToIndex of %v, error is %v", pathToIndex, err.Error())
			return // Let's not fill up ES with junk if we can't marshal our path struct.
		}

		putreq, err := http.NewRequest("PUT", urlToPut, bytes.NewBuffer(jsonPath))

		if err != nil {
			logging.Statsd.Client.Inc("indexmgr.es.err.put", 1, 1.0)
			config.G.Log.System.LogError("Error when attempting to index %v: %v", metricPath, err.Error())
		}

		r := im.httpRequest(putreq)
		if r != nil {
			// Pop the last node of the metric off, set isLeaf to false, and resume loop.
			_, splitPath = splitPath[len(splitPath)-1], splitPath[:len(splitPath)-1]
			isLeaf = false
			pathLen = len(splitPath)
			retries = 0
		} else {
			logging.Statsd.Client.Inc("indexmgr.es.err.pmr.req", 1, 1.0)
			retries++
			config.G.Log.System.LogWarn("processMetricPath's httprequest to ES came back as nil, sending to retry in %d seconds.", retries)
			time.Sleep(time.Duration(retries) * time.Second)
		}
	}
}

func (im *IndexManager) getCount(req *http.Request) string {
	var resp ElasticResponse
	r := im.httpRequest(req)
	if r != nil {
		_ = json.Unmarshal(r, &resp)
		config.G.Log.System.LogDebug("total: %v", resp.Hits.Total)
		return strconv.Itoa(resp.Hits.Total)
	} else {
		return "0"
	}
}

// query returns the data matched by the supplied query.
func (im *IndexManager) query(q config.IndexQuery) {
	switch strings.ToLower(q.Method) {
	case "delete":
		// TODO
	default:
		im.queryGET(q)
	}
}

// query returns the data matched by the supplied query.
func (im *IndexManager) queryGET(q config.IndexQuery) {

	config.G.Log.System.LogDebug("IndexManager::query %v", q.Query)

	// Query particulars are mandatory.
	if q.Query == "" {
		q.Channel <- config.APIQueryResponse{config.AQS_BADREQUEST, "no query specified", []byte{}}
		return
	}
	// Convert query to form suitable for Elasticsearch regexp search.
	regexpQuery := strings.Replace(q.Query, ".", "\\.", -1)
	regexpQuery = strings.Replace(regexpQuery, "*", ".*", -1)

	// Get number of nodes in the path for the ElasticSearch Query
	pathDepth := len(strings.Split(q.Query, "."))

	var esResp ElasticResponse
	var respList []IndexResponse
	var resp config.APIQueryResponse

	// It's turtles all the way down!  This is totally Vijay's fault.
	// http://github.com/vijaykramesh -- JP
	sort := []map[string]map[string]string{
		{
			"path": map[string]string{
				"order": "asc",
			},
		},
	}
	query := map[string]map[string][]map[string]map[string]interface{}{
		"bool": map[string][]map[string]map[string]interface{}{
			"must": []map[string]map[string]interface{}{
				{
					"regexp": map[string]interface{}{
						"path": regexpQuery,
					},
				},
				{
					"match": map[string]interface{}{
						"depth": pathDepth,
					},
				},
			},
		},
	}

	fullQuery := ERQuery{sort, query}
	getreq := im.prepRequest(fullQuery)
	r := im.httpRequest(getreq)

	if r != nil {
		_ = json.Unmarshal(r, &esResp)

		config.G.Log.System.LogDebug("esResp: %v", esResp)

		for _, hit := range esResp.Hits.Hits {
			respList = append(respList, hit.Source)
		}

		jsonResp, _ := json.Marshal(respList)

		resp = config.APIQueryResponse{config.AQS_OK, "", jsonResp}
	} else {
		logging.Statsd.Client.Inc("indexmgr.es.err.get", 1, 1.0)
		config.G.Log.System.LogError("Error querying ES.")
		resp = config.APIQueryResponse{config.AQS_ERROR, "Error querying ES", []byte{}}
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
