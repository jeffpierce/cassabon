// Middleware contains the drivers for the various services Cassabon leverages.
package middleware

import (
	"strings"

	"gopkg.in/redis.v3"
)

// RedisClient is used to initialize a connection to a stand-alone Redis server
// that is not clustered or in a Sentinel pool. Returns the working Redis client
// if good, otherwise, it returns nil.
func RedisClient(raddr []string, rpwd string, rdb int64) *redis.Client {
	// Initialize Redis client.  redis.NewClient returns a pointer.
	rclient := redis.NewClient(&redis.Options{
		Addr:     strings.Join(raddr, ","),
		Password: rpwd,
		DB:       rdb,
	})

	// Return the result of the client test.
	return testRedisClient(rclient)
}

// RedisFailoverClient is used to initialize a connection to a Redis server pool
// managed by Redis Sentinel.
func RedisFailoverClient(raddr []string, rpwd, master string, rdb int64) *redis.Client {
	// Initialize Redis connection via Redis Sentinels
	rclient := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    master,
		SentinelAddrs: raddr,
		Password:      rpwd,
		DB:            rdb,
	})

	// Return the result of the client test.
	return testRedisClient(rclient)
}

// testRedisClient tests an initialized connection to Redis via the PING built-in.
// Returns true if client is good, returns false if that's not the case.
func testRedisClient(rclient *redis.Client) *redis.Client {
	// Test client.
	_, err := rclient.Ping().Result()
	if err != nil {
		// TODO:  Implement our logging/stats after merge -- JRP
		// Return nil, connection didn't ping correctly.
		return nil
	}

	// Return good connection.
	return rclient
}
