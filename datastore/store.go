package datastore

import (
	"github.com/jeffpierce/cassabon/config"
)

func StoreManager() {
	for {
		select {
		case <-config.G.Quit:
			config.G.Log.System.LogInfo("QueueManager received QUIT message")
			config.G.WG.Done()
			return
		case metric := <-config.G.MetricInput:
			config.G.Log.Carbon.LogDebug("QueueManager received metric: %v", metric)
		}
	}
}
