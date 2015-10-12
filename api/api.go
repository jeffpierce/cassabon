// Package api implements the HTTP API for Cassabon
package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/zenazn/goji/graceful"
	"github.com/zenazn/goji/web"

	"github.com/jeffpierce/cassabon/config"
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
	api.server.Delete("/paths/:path", api.deletePathHandler)
	api.server.Delete("/metrics/:metric", api.deleteMetricHandler)
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
	resp.Github = "https://github.com/jeffpierce/cassabon"
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
	q := config.DataQuery{r.Method, r.Form.Get("query"), ch}
	config.G.Log.System.LogDebug("Received paths query: %s %s", q.Method, q.Query)

	// Forward the query.
	select {
	case config.G.Channels.IndexRequest <- q:
	default:
		config.G.Log.System.LogWarn(
			"Index query discarded, IndexRequest channel is full (max %d entries)",
			config.G.Channels.IndexRequestChanLen)
	}

	// Send the response to the client.
	api.sendResponse(w, q)
}

// deletePathHandler removes paths from the index store.
func (api *CassabonAPI) deletePathHandler(c web.C, w http.ResponseWriter, r *http.Request) {

	// Create the channel on which the response will be received.
	ch := make(chan config.APIQueryResponse)

	// Build the query.
	q := config.DataQuery{r.Method, c.URLParams["path"], ch}
	config.G.Log.System.LogDebug("Received paths query: %s %s", q.Method, q.Query)

	// Forward the query.
	select {
	case config.G.Channels.IndexRequest <- q:
	default:
		config.G.Log.System.LogWarn(
			"Index DELETE query discarded, IndexRequest channel is full (max %d entries)",
			config.G.Channels.IndexRequestChanLen)
	}

	// Send the response to the client.
	api.sendResponse(w, q)
}

// getMetricHandler processes requests like "GET /metrics?query=foo".
func (api *CassabonAPI) getMetricHandler(w http.ResponseWriter, r *http.Request) {

	// Create the channel on which the response will be received.
	ch := make(chan config.APIQueryResponse)

	// Extract the query from the request URI.
	_ = r.ParseForm()
	q := config.DataQuery{r.Method, r.Form.Get("query"), ch}
	config.G.Log.System.LogDebug("Received metrics query: %s %s", q.Method, q.Query)

	// Forward the query.
	select {
	case config.G.Channels.DataRequest <- q:
	default:
		config.G.Log.System.LogWarn(
			"Metrics query discarded, DataRequest channel is full (max %d entries)",
			config.G.Channels.DataRequestChanLen)
	}

	// Send the response to the client.
	api.sendResponse(w, q)
}

// deleteMetricHandler removes data from the metrics store.
func (api *CassabonAPI) deleteMetricHandler(c web.C, w http.ResponseWriter, r *http.Request) {

	// Create the channel on which the response will be received.
	ch := make(chan config.APIQueryResponse)

	// Build the query.
	q := config.DataQuery{r.Method, c.URLParams["metric"], ch}
	config.G.Log.System.LogDebug("Received metrics query: %s %s", q.Method, q.Query)

	// Forward the query.
	select {
	case config.G.Channels.IndexRequest <- q:
	default:
		config.G.Log.System.LogWarn(
			"Metric DELETE query discarded, IndexRequest channel is full (max %d entries)",
			config.G.Channels.IndexRequestChanLen)
	}

	// Send the response to the client.
	api.sendResponse(w, q)
}

func (api *CassabonAPI) sendResponse(w http.ResponseWriter, q config.DataQuery) {

	// Read the response.
	var resp config.APIQueryResponse
	select {
	case resp = <-q.Channel:
		// Nothing, we have our response.
	case <-time.After(time.Second):
		// The query died or wedged; simulate a timeout response.
		resp = config.APIQueryResponse{config.AQS_ERROR, "query timed out", []byte{}}
	}
	close(q.Channel)

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
