// Middleware contains the drivers for the various services Cassabon leverages.
package middleware

import (
	"strconv"
	"time"

	"github.com/gocql/gocql"
)

// Returns a round-robin simple connection pool to the Cassandra cluster.
func CassandraSession(chosts []string, cport string, ckeyspace string) (*gocql.Session, error) {

	// Port must be numeric. Parse error will result in invalid port, which is reported.
	port, _ := strconv.ParseInt(cport, 10, 64)

	// Build a cluster configuration.
	clusterCfg := gocql.NewCluster(chosts...)
	clusterCfg.Port = int(port)
	clusterCfg.DiscoverHosts = true
	clusterCfg.Keyspace = ckeyspace
	clusterCfg.Timeout = time.Duration(time.Second)

	// Create session.
	return clusterCfg.CreateSession()
}
