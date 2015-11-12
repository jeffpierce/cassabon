package listener

import (
	"encoding/json"
	"sort"
	"sync"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/pearson"
)

type indexedLine struct {
	peerIndex int
	statLine  string
}

// PeerList contains an ordered list of Cassabon peers.
type PeerList struct {
	wg       *sync.WaitGroup
	target   chan indexedLine  // Channel for forwarding a stat line to a Cassabon peer
	hostPort string            // Host:port on which the local server is listening
	peersMap map[string]string // Peer list as stored in the configuration
	peers    []string          // Host:port information for all Cassabon peers (inclusive)
	conns    map[string]*StubbornTCPConn
	self     sync.RWMutex
}

func (pl *PeerList) Init() {
	pl.conns = make(map[string]*StubbornTCPConn, 0)
}

// IsStarted indicates whether the structure has ever been updated.
func (pl *PeerList) IsStarted() bool {
	return pl.hostPort != ""
}

// Start records the current peer list and starts the forwarder goroutine.
func (pl *PeerList) Start(wg *sync.WaitGroup, hostPort string, peersMap map[string]string) {

	// Synchronize access by other goroutines.
	pl.self.Lock()
	defer pl.self.Unlock()

	pl.wg = wg
	pl.hostPort = hostPort
	pl.peersMap = peersMap

	// Create the channel on which stats to forward are received.
	pl.target = make(chan indexedLine, 1)

	// Dispose of any peer connections that are obsolete.
	peers := sortedMapToArray(pl.peersMap)
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
	pl.wg.Add(1)
	go pl.run()
}

// IsEqual indicates whether the given new configuration is equal to the current.
func (pl *PeerList) IsEqual(hostPort string, peersMap map[string]string) bool {

	// Synchronize access by other goroutines.
	pl.self.RLock()
	defer pl.self.RUnlock()

	if pl.hostPort != hostPort {
		return false
	}

	peers := sortedMapToArray(peersMap)
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

// OwnerOf determines which host owns a particular stats path.
func (pl *PeerList) OwnerOf(statPath string) (int, bool) {
	peerIndex := int(pearson.Hash8(statPath)) % len(pl.peers)
	if pl.hostPort == pl.peers[peerIndex] {
		return peerIndex, true
	} else {
		return peerIndex, false
	}
}

// PropagatePeerList sends the current peer list to all known peers.
func (pl *PeerList) PropagatePeerList() {

	// Build the command to be sent.
	var cmd string
	var buf []byte
	buf, _ = json.Marshal(pl.peersMap)
	cmd = "<<peerlist=" + string(buf) + ">>"

	// Send the command to each peer.
	for i, v := range pl.peers {
		if v != pl.hostPort {
			pl.target <- indexedLine{i, cmd}
		}
	}
}

// run listens for stat lines on a channel and sends them to the appropriate Cassabon peer.
func (pl *PeerList) run() {

	defer close(pl.target)

	for {
		select {
		case <-config.G.OnReload2:
			config.G.Log.System.LogDebug("PeerList::run received QUIT message")
			pl.wg.Done()
			return
		case il := <-pl.target:
			if pl.hostPort != pl.peers[il.peerIndex] {
				pl.conns[pl.peers[il.peerIndex]].Send(il.statLine)
			}
		}
	}
}

// sortedMapToArray converts a map to an array of its values, ordered by key.
func sortedMapToArray(m map[string]string) []string {
	var a, t []string
	for k, _ := range m {
		t = append(t, k)
	}
	sort.Strings(t)
	for _, v := range t {
		a = append(a, m[v])
	}
	return a
}
