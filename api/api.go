// Package api implements the HTTP API for Cassabon
package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zenazn/goji/graceful"
	"github.com/zenazn/goji/web"

	"github.com/change/cassabon/config"
	"github.com/change/cassabon/logging"
)

type CassabonAPI struct {
	wg       *sync.WaitGroup
	server   *web.Mux
	hostPort string
}

func (api *CassabonAPI) Start(wg *sync.WaitGroup) {
	// Add to waitgroup and run go routine.
	api.hostPort = config.G.API.Listen
	api.wg = wg
	api.wg.Add(1)
	go api.run()
}

func (api *CassabonAPI) Stop() {
	config.G.Log.System.LogInfo("API received Stop command, gracefully shutting down.")
	graceful.Shutdown()
	api.wg.Done()
}

func (api *CassabonAPI) run() {
	// Initialize API server
	api.server = web.New()

	// Define routes
	api.server.Get("/", api.rootHandler)
	api.server.Get("/paths", api.getPathHandler)
	api.server.Get("/metrics", api.getMetricHandler)
	api.server.Get("/healthcheck", api.healthHandler)
	api.server.Delete("/paths", api.deletePathHandler)
	api.server.Delete("/metrics", api.deleteMetricHandler)
	api.server.NotFound(api.notFoundHandler)

	api.server.Use(requestLogger)

	config.G.Log.System.LogInfo("API initialized, serving!")
	graceful.ListenAndServe(api.hostPort, api.server)
}

// notFoundHandler is the global 404 handler, used by Goji.
func (api *CassabonAPI) notFoundHandler(w http.ResponseWriter, r *http.Request) {
	api.sendErrorResponse(w, http.StatusNotFound, "not found", r.RequestURI)
}

// healthHandler responds with either ALIVE or DEAD, for use by the load balancer.
func (api *CassabonAPI) healthHandler(w http.ResponseWriter, r *http.Request) {

	// We are alive, unless the healthcheck file says we are dead.
	var alive bool = true

	if health, err := ioutil.ReadFile(config.G.API.HealthCheckFile); err == nil {
		if strings.ToUpper(strings.TrimSpace(string(health))) == "DEAD" {
			alive = false
		}
	}

	if alive {
		fmt.Fprint(w, "ALIVE")
	} else {
		fmt.Fprint(w, "DEAD")
	}
}

// rootHandler provides information about the application, served from "/".
func (api *CassabonAPI) rootHandler(w http.ResponseWriter, r *http.Request) {

	resp := struct {
		Message string `json:"message"`
		Github  string `json:"github"`
		Version string `json:"version"`
	}{}
	resp.Message = "Cassabon.  You know, for stats!"
	resp.Github = "https://github.com/change/cassabon"
	resp.Version = config.Version
	jsonText, _ := json.Marshal(resp)
	w.Write(jsonText)
}

// getPathHandler processes requests like "GET /paths?query=foo".
func (api *CassabonAPI) getPathHandler(w http.ResponseWriter, r *http.Request) {

	// Create the channel on which the response will be received.
	ch := make(chan config.APIQueryResponse)

	// Extract the query from the request URI.
	_ = r.ParseForm()
	q := config.IndexQuery{r.Method, r.Form.Get("query"), ch}
	config.G.Log.System.LogDebug("Received paths query: %s %s", q.Method, q.Query)

	// Forward the query.
	select {
	case config.G.Channels.IndexRequest <- q:
	default:
		config.G.Log.System.LogWarn(
			"Index query discarded, IndexRequest channel is full (max %d entries)",
			config.G.Channels.IndexRequestChanLen)
		logging.Statsd.Client.Inc("api.err.path.get", 1, 1.0)
	}

	// Send the response to the client.
	api.sendResponse(w, ch, config.G.API.Timeouts.GetIndex)
}

// deletePathHandler removes paths from the index store.
func (api *CassabonAPI) deletePathHandler(c web.C, w http.ResponseWriter, r *http.Request) {

	// Create the channel on which the response will be received.
	ch := make(chan config.APIQueryResponse)

	// Extract the query from the request URI.
	_ = r.ParseForm()
	q := config.IndexQuery{r.Method, r.Form.Get("query"), ch}
	config.G.Log.System.LogDebug("Received paths query: %s %s", q.Method, q.Query)

	// Forward the query.
	select {
	case config.G.Channels.IndexRequest <- q:
	default:
		config.G.Log.System.LogWarn(
			"Index DELETE query discarded, IndexRequest channel is full (max %d entries)",
			config.G.Channels.IndexRequestChanLen)
		logging.Statsd.Client.Inc("api.err.path.delete", 1, 1.0)
	}

	// Send the response to the client.
	api.sendResponse(w, ch, config.G.API.Timeouts.DeleteIndex)
}

// getMetricHandler processes requests like "GET /metrics?query=foo".
func (api *CassabonAPI) getMetricHandler(w http.ResponseWriter, r *http.Request) {

	// Create the channel on which the response will be received.
	ch := make(chan config.APIQueryResponse)

	// Extract the query from the request URI.
	_ = r.ParseForm()
	from, _ := strconv.Atoi(r.Form.Get("from"))
	to, _ := strconv.Atoi(r.Form.Get("to"))
	q := config.MetricQuery{r.Method, r.Form["path"], int64(from), int64(to), false, ch}
	config.G.Log.System.LogDebug("Received metrics query: %s %v %d %d", q.Method, q.Query, q.From, q.To)

	// Forward the query.
	select {
	case config.G.Channels.MetricRequest <- q:
	default:
		config.G.Log.System.LogWarn(
			"Metrics query discarded, MetricRequest channel is full (max %d entries)",
			config.G.Channels.MetricRequestChanLen)
		logging.Statsd.Client.Inc("api.err.metrics.get", 1, 1.0)
	}

	// Send the response to the client.
	api.sendResponse(w, ch, config.G.API.Timeouts.GetMetric)
}

// deleteMetricHandler removes data from the metrics store.
func (api *CassabonAPI) deleteMetricHandler(c web.C, w http.ResponseWriter, r *http.Request) {

	// Create the channel on which the response will be received.
	ch := make(chan config.APIQueryResponse)

	// Extract the query from the request URI.
	_ = r.ParseForm()
	metric := r.Form["path"]
	from, _ := strconv.Atoi(r.Form.Get("from"))
	to, _ := strconv.Atoi(r.Form.Get("to"))
	dryrunText := r.Form.Get("dryrun")
	dryrun := true
	if strings.ToLower(dryrunText) == "false" || strings.ToLower(dryrunText) == "no" {
		dryrun = false
	}
	q := config.MetricQuery{r.Method, metric, int64(from), int64(to), dryrun, ch}
	config.G.Log.System.LogDebug("Received metrics query: %s %v %d %d %v", q.Method, q.Query, q.From, q.To, dryrun)

	// Forward the query.
	select {
	case config.G.Channels.MetricRequest <- q:
	default:
		config.G.Log.System.LogWarn(
			"Metric DELETE query discarded, IndexRequest channel is full (max %d entries)",
			config.G.Channels.IndexRequestChanLen)
		logging.Statsd.Client.Inc("api.err.metrics.delete", 1, 1.0)
	}

	// Send the response to the client.
	api.sendResponse(w, ch, config.G.API.Timeouts.DeleteMetric)
}

func (api *CassabonAPI) sendResponse(w http.ResponseWriter, ch chan config.APIQueryResponse, timeout time.Duration) {

	// Read the response.
	var resp config.APIQueryResponse
	select {
	case resp = <-ch:
		// Nothing, we have our response.
	case <-time.After(timeout):
		// The query died or wedged; simulate a timeout response.
		resp = config.APIQueryResponse{config.AQS_ERROR, fmt.Sprintf("query timed out after %v", timeout), []byte{}}
	}
	close(ch)

	// Inspect the response status, and send appropriate response headers/data to client.
	switch resp.Status {
	case config.AQS_OK:
		if len(resp.Payload) > 0 {
			w.Write(resp.Payload)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	case config.AQS_NOTFOUND:
		api.sendErrorResponse(w, http.StatusNotFound, "not found", resp.Message)
	case config.AQS_BADREQUEST:
		api.sendErrorResponse(w, http.StatusBadRequest, "bad request", resp.Message)
	case config.AQS_ERROR:
		api.sendErrorResponse(w, http.StatusInternalServerError, "internal error", resp.Message)
	}
}

func (api *CassabonAPI) sendErrorResponse(w http.ResponseWriter, status int, text string, message string) {

	resp := struct {
		Status     int    `json:"status"`
		StatusText string `json:"statustext"`
		Message    string `json:"message"`
	}{}

	resp.Status = status
	resp.StatusText = text
	resp.Message = message
	jsonText, _ := json.Marshal(resp)

	w.WriteHeader(status)
	w.Write(jsonText)
}
