package datastore

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

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

type IndexManager struct {
	wg *sync.WaitGroup
}

func (im *IndexManager) Init() {
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
			im.index(metric.Path)
		case query := <-config.G.Channels.IndexRequest:
			go im.query(query)
		}
	}
}

// IndexMetricPath takes a metric path string and sends it off to be processed by processMetricPath().
func (im *IndexManager) index(path string) {
	it := time.Now()
	config.G.Log.System.LogDebug("IndexManager::index path=%s", path)
	splitPath := strings.Split(path, ".")
	im.processMetricPath(splitPath, len(splitPath), true)
	logging.Statsd.Client.TimingDuration("indexmgr.index", time.Since(it), 1.0)
}

func (im *IndexManager) httpRequest(req *http.Request) *http.Response {
	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		logging.Statsd.Client.Inc("indexmgr.es.request.err", 1, 1.0)
		config.G.Log.System.LogError("Received error from ElasticSearch: %v, request: %v", err.Error(), req)
		return nil
	}

	return resp
}

// processMetricPath recursively indexes the metric path via the ElasticSearch REST API.
func (im *IndexManager) processMetricPath(splitPath []string, pathLen int, isLeaf bool) {
	// Process the metric path one node at a time.  We store metrics in ElasticSearch.
	for pathLen > 0 {

		// Construct the metric string
		metricPath := strings.Join(splitPath, ".")
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
		defer r.Body.Close()

		// Pop the last node of the metric off, set isLeaf to false, and resume loop.
		_, splitPath = splitPath[len(splitPath)-1], splitPath[:len(splitPath)-1]
		isLeaf = false
		pathLen = len(splitPath)
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

	// Get number of nodes in the path for the ElasticSearch Query
	pathDepth := len(strings.Split(q.Query, "."))

	var esResp ElasticResponse
	var respList []IndexResponse
	var resp config.APIQueryResponse

	// It's turtles all the way down!  This is totalle Vijay's fault.
	// http://github.com/vijaykramesh -- JP
	query := map[string]map[string]map[string][]map[string]map[string]interface{}{
		"query": map[string]map[string][]map[string]map[string]interface{}{
			"bool": map[string][]map[string]map[string]interface{}{
				"must": []map[string]map[string]interface{}{
					{
						"wildcard": map[string]interface{}{
							"path": q.Query,
						},
					},
					{
						"match": map[string]interface{}{
							"depth": pathDepth,
						},
					},
				},
			},
		},
	}

	jsonQuery, _ := json.Marshal(query)
	config.G.Log.System.LogDebug("%s", string(jsonQuery))

	getreq, _ := http.NewRequest("GET", config.G.ElasticSearch.SearchURL, strings.NewReader(string(jsonQuery)))
	r := im.httpRequest(getreq)
	defer r.Body.Close()

	if r == nil {
		logging.Statsd.Client.Inc("indexmgr.es.err.get", 1, 1.0)
		config.G.Log.System.LogError("Error querying ES.")
		resp = config.APIQueryResponse{config.AQS_ERROR, "Error querying ES", []byte{}}
	} else {
		body, _ := ioutil.ReadAll(r.Body)
		config.G.Log.System.LogDebug("body: %v", string(body))
		_ = json.Unmarshal(body, &esResp)

		config.G.Log.System.LogDebug("esResp: %v", esResp)

		for _, hit := range esResp.Hits.Hits {
			respList = append(respList, hit.Source)
		}

		jsonResp, _ := json.Marshal(respList)

		resp = config.APIQueryResponse{config.AQS_OK, "", jsonResp}
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
