package listener

import (
	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/pearson"
)

type indexedLine struct {
	peerIndex int
	statLine  string
}

// PeerList contains an ordered list of Cassabon peers.
type PeerList struct {
	target   chan indexedLine // Channel for forwarding a stat line to a Cassabon peer
	hostPort string           // Host:port on which the local server is listening
	peers    []string         // Host:port information for all Cassabon peers (inclusive)
	conns    map[string]*StubbornTCPConn
}

func (pl *PeerList) Init() {
	pl.conns = make(map[string]*StubbornTCPConn, 0)
}

// isInitialized indicates whether the structure has ever been updated.
func (pl *PeerList) IsInitialized() bool {
	return pl.hostPort != ""
}

// start records the current peer list and starts the forwarder goroutine.
func (pl *PeerList) Start(hostPort string, peers []string) {

	// Create the channel on which stats to forward are received.
	pl.target = make(chan indexedLine, 1)

	// Dispose of any peer connections that are obsolete.
	for _, existing := range pl.peers {
		found := false
		for _, incoming := range peers {
			if existing == incoming {
				found = true
				break
			}
		}
		if !found && existing != pl.hostPort {
			pl.conns[existing].Close()
			delete(pl.conns, existing)
		}
	}

	// Record the current set of peers.
	pl.hostPort = hostPort
	pl.peers = make([]string, len(peers))
	for i, v := range peers {
		pl.peers[i] = v
		// Set up peer connections for newly added peers.
		if _, found := pl.conns[v]; !found && v != pl.hostPort {
			pl.conns[v] = new(StubbornTCPConn)
			pl.conns[v].Open(v)
		}
	}

	// Start the forwarder goroutine.
	config.G.OnReload2WG.Add(1)
	go pl.run()
}

// isEqual indicates whether the given new configuration is equal to the current.
func (pl *PeerList) IsEqual(hostPort string, peers []string) bool {
	if pl.hostPort != hostPort {
		return false
	}
	if len(pl.peers) != len(peers) {
		return false
	}
	for i, v := range pl.peers {
		if peers[i] != v {
			return false
		}
	}
	return true
}

// ownerOf determines which host owns a particular stats path.
func (pl *PeerList) OwnerOf(statPath string) (int, bool) {
	peerIndex := int(pearson.Hash8(statPath)) % len(pl.peers)
	if pl.hostPort == pl.peers[peerIndex] {
		return peerIndex, true
	} else {
		return peerIndex, false
	}
}

// run listens for stat lines on a channel and sends them to the appropriate Cassabon peer.
func (pl *PeerList) run() {

	defer close(pl.target)

	for {
		select {
		case <-config.G.OnReload2:
			config.G.Log.System.LogDebug("PeerList::run received QUIT message")
			config.G.OnReload2WG.Done()
			return
		case il := <-pl.target:
			if pl.hostPort != pl.peers[il.peerIndex] {
				pl.conns[pl.peers[il.peerIndex]].Send(il.statLine)
			}
		}
	}
}
