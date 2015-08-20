package engine

import (
	"github.com/jeffpierce/cassabon/config"
)

func QueueManager() {
	for {
		select {
		case <-config.G.Quit:
			config.G.Log.System.LogInfo("QueueManager received QUIT message")
			config.G.WG.Done()
			return
		case metric := <-config.G.QueueManager:
			config.G.Log.Carbon.LogDebug("QueueManager received metric: %v", metric)
		}
	}
}
