// Middleware contains the drivers for the various services Cassabon leverages.
package middleware

import "github.com/gocql/gocql"

// Returns a round-robin simple connection pool to the Cassandra cluster.
func CassandraSession(chosts []string, cport int, ckeyspace string) *gocql.Session {

	// Retrieve cluster configuration.
	cass := gocql.NewCluster(chosts...)

	// Set port and host discovery.
	cass.Port = cport
	cass.DiscoverHosts = true

	// Set the metrics keyspace (default: "cassabon")
	cass.Keyspace = ckeyspace

	// Create session
	csession, _ := cass.CreateSession()

	// Defer closing
	defer csession.Close()

	// And return the session.
	return csession
}
