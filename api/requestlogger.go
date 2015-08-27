package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/mutil"

	"github.com/jeffpierce/cassabon/config"
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
		duration := t2.Sub(t1).Nanoseconds() / 1000

		// Write the log entry to the access log.
		config.G.Log.API.LogInfo("%s %s %s %s status=%d size=%d dur=%d",
			remoteHost, r.Method, r.Proto, r.RequestURI, status, size, duration)
	}

	return http.HandlerFunc(fn)
}
