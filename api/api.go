// Package api implements the HTTP API for Cassabon
package api

import (
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
	api.server.Get("/paths", api.pathsHandler)
	api.server.Get("/metrics", api.metricsHandler)
	api.server.Get("/healthcheck", api.healthHandler)
	api.server.Delete("/remove/path/:path", api.deletePathHandler)
	api.server.Delete("/remove/metric/:metric", api.deleteMetricHandler)
	api.server.Get("/", api.notFound)

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
		w.WriteHeader(http.StatusNotFound)
		w.Write(resp.Payload)
	case config.IQS_BADREQUEST:
		w.WriteHeader(http.StatusBadRequest)
		w.Write(resp.Payload)
	case config.IQS_ERROR:
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(resp.Payload)
	}
}

func (api *CassabonAPI) metricsHandler(w http.ResponseWriter, r *http.Request) {
	// Have to wait for the Cassandra worker to get implemented.
	fmt.Fprintf(w, "Not yet implemented.")
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
	// TODO:  Implement this in datastore.
	fmt.Fprintf(w, "Not yet implemented, would have deleted %s", c.URLParams["path"])
}

func (api *CassabonAPI) deleteMetricHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	// TODO:  Implement this in datastore.
	fmt.Fprintf(w, "Not yet implemented, would have deleted %s", c.URLParams["metric"])
}

func (api *CassabonAPI) notFound(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Cassabon.  You know, for stats!")
}
