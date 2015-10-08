// Package api implements the HTTP API for Cassabon
package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/zenazn/goji/graceful"
	"github.com/zenazn/goji/web"

	"github.com/jeffpierce/cassabon/config"
)

type CassabonAPI struct {
	server   *web.Mux
	hostPort string
}

func (api *CassabonAPI) Start() {
	// Add to waitgroup and run go routine.
	api.hostPort = config.G.API.Listen
	config.G.OnReload1WG.Add(1)
	go api.run()
}

func (api *CassabonAPI) Stop() {
	config.G.Log.System.LogInfo("API received Stop command, gracefully shutting down.")
	graceful.Shutdown()
	config.G.OnReload1WG.Done()
}

func (api *CassabonAPI) run() {
	// Initialize API server
	api.server = web.New()

	// Define routes
	api.server.Get("/", api.rootHandler)
	api.server.Get("/paths", api.pathsHandler)
	api.server.Get("/metrics", api.metricsHandler)
	api.server.Get("/healthcheck", api.healthHandler)
	api.server.Delete("/remove/path/:path", api.deletePathHandler)
	api.server.Delete("/remove/metric/:metric", api.deleteMetricHandler)
	api.server.NotFound(api.notFoundHandler)

	api.server.Use(requestLogger)

	config.G.Log.System.LogInfo("API initialized, serving!")
	graceful.ListenAndServe(api.hostPort, api.server)
}

// pathsHandler processes requests like "GET /paths?query=foo".
func (api *CassabonAPI) pathsHandler(w http.ResponseWriter, r *http.Request) {

	// Create channel for use in communicating with the statGopher
	ch := make(chan config.IndexQueryResponse)

	// Extract the query, and build the query to be sent to the indexer.
	_ = r.ParseForm()
	pathQuery := config.IndexQuery{r.Form.Get("query"), ch}
	config.G.Log.System.LogDebug("Received query: %s", pathQuery.Query)

	// Pass on the query, and read the response.
	config.G.Channels.Gopher <- pathQuery
	resp := <-pathQuery.Channel
	close(pathQuery.Channel)

	// Send the response to the client.
	switch resp.Status {
	case config.IQS_OK:
		w.Write(resp.Payload)
	case config.IQS_NOCONTENT:
		w.WriteHeader(http.StatusNoContent)
	case config.IQS_NOTFOUND:
		api.sendErrorResponse(w, http.StatusNotFound, "not found", resp.Message)
	case config.IQS_BADREQUEST:
		api.sendErrorResponse(w, http.StatusBadRequest, "bad request", resp.Message)
	case config.IQS_ERROR:
		api.sendErrorResponse(w, http.StatusInternalServerError, "internal error", resp.Message)
	}
}

func (api *CassabonAPI) metricsHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Have to wait for the Cassandra worker to get implemented.
	api.sendErrorResponse(w, http.StatusNotImplemented, "not implemented", "")
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

func (api *CassabonAPI) deletePathHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	// TODO:  Implement this in datastore.  c.URLParams["path"]
	api.sendErrorResponse(w, http.StatusNotImplemented, "not implemented", "")
}

func (api *CassabonAPI) deleteMetricHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	// TODO:  Implement this in datastore.  c.URLParams["metric"]
	api.sendErrorResponse(w, http.StatusNotImplemented, "not implemented", "")
}

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

func (api *CassabonAPI) notFoundHandler(w http.ResponseWriter, r *http.Request) {
	api.sendErrorResponse(w, http.StatusNotFound, "not found", r.RequestURI)
}
