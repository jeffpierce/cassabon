package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/mutil"

	"github.com/change/cassabon/config"
	"github.com/change/cassabon/logging"
)

// requestLogger handler emits access and trace log entries.
func requestLogger(c *web.C, h http.Handler) http.Handler {

	fn := func(w http.ResponseWriter, r *http.Request) {

		// Instrument the ResponseWriter with a wrapper, and time the rest of the handler chain.
		lw := mutil.WrapWriter(w)
		t1 := time.Now()
		h.ServeHTTP(lw, r)
		t2 := time.Now()
		if lw.Status() == 0 {
			lw.WriteHeader(http.StatusOK)
		}

		// Assemble the data for the access log entry.
		remoteHost := strings.Split(r.RemoteAddr, ":")[0]
		status := lw.Status()
		size := lw.BytesWritten()
		duration := t2.Sub(t1)

		// Assemble the data for the stats entry.
		stats := strings.Split(r.RequestURI, "?")
		stats = strings.Split(stats[0], "/")
		if stats[1] == "" {
			stats[1] = "root"
		}
		stats = []string{"api", stats[1], strings.ToLower(r.Method)}

		// Write the stats entry.
		logging.Statsd.Client.TimingDuration(strings.Join(stats, "."), duration, 1.0)

		// Write the log entry to the access log.
		config.G.Log.API.LogInfo("%s %s %s %s status=%d size=%d dur=%d",
			remoteHost, r.Method, r.Proto, r.RequestURI, status, size, duration.Nanoseconds()/1000)
	}

	return http.HandlerFunc(fn)
}
