// Package listener implements the listener pool and handlers for metrics.
package listener

import (
	"bufio"
	"bytes"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/logging"
	"github.com/jeffpierce/cassabon/pearson"
)

// peerState is an object used to detect changes in local host:port or in peer list.
type peerState struct {
	lastHost  string   // For detection of whether our local host has changed
	lastPort  string   // For detection of whether our local port has changed
	lastPeers []string // For detection of whether the peer list has changed
}

// isInitialized indicates whether the structure has been initialized.
func (ps *peerState) isInitialized() bool {
	return ps.lastHost != ""
}

// setData initializes the structure with the given data.
func (ps *peerState) setData(host, port string, peers []string) {
	ps.lastHost = host
	ps.lastPort = port
	ps.lastPeers = make([]string, len(peers))
	for i, v := range peers {
		ps.lastPeers[i] = v
	}
}

// isEqual indicates whether the current configuration is equal to the prior.
func (ps *peerState) isEqual(host, port string, peers []string) bool {
	if ps.lastHost != host || ps.lastPort != port {
		return false
	}
	if len(ps.lastPeers) != len(peers) {
		return false
	}
	for i, v := range ps.lastPeers {
		if peers[i] != v {
			return false
		}
	}
	return true
}

type CarbonPlaintextListener struct {
	myHostPort    string    // host:port as actually resolved, for matching self
	lastPeerState peerState // State of peer list for comparison after a reload
}

func (cpl *CarbonPlaintextListener) Init() {
	cpl.lastPeerState = peerState{}
}

func (cpl *CarbonPlaintextListener) Start() {

	// Determine whether we need to clear the rollup accumulators.
	if !cpl.lastPeerState.isInitialized() {
		// On first time here, initialize with current peer state.
		cpl.lastPeerState.setData(config.G.Carbon.Address, config.G.Carbon.Port, config.G.Carbon.Peers)
	} else {
		// Clear out our local accumulators if the peer list changed in any way.
		if !cpl.lastPeerState.isEqual(config.G.Carbon.Address, config.G.Carbon.Port, config.G.Carbon.Peers) {
			config.G.Log.System.LogDebug("peerState::isEqual(): false")
			config.G.OnPeerChangeReq <- struct{}{}
			<-config.G.OnPeerChangeRsp
			cpl.lastPeerState.setData(config.G.Carbon.Address, config.G.Carbon.Port, config.G.Carbon.Peers)
		}
	}
	cpl.myHostPort = config.G.Carbon.Address + ":" + config.G.Carbon.Port

	// Kick off goroutines to list for TCP and/or UDP traffic as specified.
	switch config.G.Carbon.Protocol {
	case "tcp":
		config.G.OnReload1WG.Add(1)
		go cpl.carbonTCP(config.G.Carbon.Address, config.G.Carbon.Port)
	case "udp":
		config.G.OnReload1WG.Add(1)
		go cpl.carbonUDP(config.G.Carbon.Address, config.G.Carbon.Port)
	default:
		config.G.OnReload1WG.Add(2)
		go cpl.carbonTCP(config.G.Carbon.Address, config.G.Carbon.Port)
		go cpl.carbonUDP(config.G.Carbon.Address, config.G.Carbon.Port)
	}
}

// carbonTCP listens for incoming Carbon TCP traffic and dispatches it.
func (cpl *CarbonPlaintextListener) carbonTCP(addr string, port string) {

	// Resolve the address:port, and start listening for TCP connections.
	tcpaddr, _ := net.ResolveTCPAddr("tcp4", net.JoinHostPort(addr, port))
	tcpListener, err := net.ListenTCP("tcp4", tcpaddr)
	if err != nil {
		// If we can't grab a port, we can't do our job.  Log, whine, and crash.
		config.G.Log.System.LogFatal("Cannot listen for Carbon on TCP: %v", err)
		os.Exit(3)
	}
	defer tcpListener.Close()
	config.G.Log.System.LogInfo("Listening on %s TCP for Carbon plaintext protocol", tcpListener.Addr().String())

	// Start listener and pass incoming connections to handler.
	for {
		select {
		case <-config.G.OnReload1:
			config.G.Log.System.LogDebug("CarbonTCP received QUIT message")
			config.G.OnReload1WG.Done()
			return
		default:
			// On receipt of a connection, spawn a goroutine to handle it.
			tcpListener.SetDeadline(time.Now().Add(time.Duration(config.G.Parameters.Listener.TCPTimeout) * time.Second))
			if conn, err := tcpListener.Accept(); err == nil {
				go cpl.getTCPData(conn)
			} else {
				if err.(net.Error).Timeout() {
					config.G.Log.System.LogDebug("CarbonTCP Accept() timed out")
				} else {
					config.G.Log.System.LogWarn("CarbonTCP Accept() error: %v", err)
				}
			}
		}
	}
}

// getTCPData reads a line from a TCP connection and dispatches it.
func (cpl *CarbonPlaintextListener) getTCPData(conn net.Conn) {

	// Carbon metrics are terminated by newlines. Read line-by-line, and dispatch.
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		cpl.metricHandler(scanner.Text())
	}
	config.G.Log.System.LogDebug("Returning from getTCPData")
}

// carbonUDP listens for incoming Carbon UDP traffic and dispatches it.
func (cpl *CarbonPlaintextListener) carbonUDP(addr string, port string) {

	// Resolve the address:port, and start listening for UDP connections.
	udpaddr, _ := net.ResolveUDPAddr("udp4", net.JoinHostPort(addr, port))
	udpConn, err := net.ListenUDP("udp", udpaddr)
	if err != nil {
		// If we can't grab a port, we can't do our job.  Log, whine, and crash.
		config.G.Log.System.LogFatal("Cannot listen for Carbon on UDP: %v", err)
		os.Exit(3)
	}
	defer udpConn.Close()
	config.G.Log.System.LogInfo("Listening on %s UDP for Carbon plaintext protocol", udpConn.LocalAddr().String())

	/* Read UDP packets and pass data to handler.
	 *
	 * Individual metrics lines may be spread across packet boundaries. This means that
	 * we must avoid dispatching partial lines, because they will ilikely be invalid,
	 * and certainly wrong.
	 *
	 * To resolve this, we only dispatch the part of the buffer up to the last newline,
	 * and save the remainder for prepending to the next incoming buffer.
	 */
	line := ""                    // The (possibly concatenated) line to be dispatched
	buf := make([]byte, 16384)    // The buffer into which UDP messages will be read
	remBuf := make([]byte, 16384) // The buffer into which data following last newline will be copied
	remBytes := 0                 // The number of data bytes in remBuf
	for {
		select {
		case <-config.G.OnReload1:
			config.G.Log.System.LogDebug("CarbonUDP received QUIT message")
			config.G.OnReload1WG.Done()
			return
		default:
			udpConn.SetDeadline(time.Now().Add(time.Duration(config.G.Parameters.Listener.UDPTimeout) * time.Second))
			bytesRead, _, err := udpConn.ReadFromUDP(buf)
			if err == nil {

				// Capture the position of the last newline in the input buffer.
				lastNewline := bytes.LastIndex(buf[:bytesRead], []byte("\n"))
				if remBytes > 0 {
					// Concatenate previous remainder and current input.
					line = string(append(remBuf[:remBytes], buf[:lastNewline]...))
				} else {
					// Use current input up to last newline present.
					line = string(buf[:lastNewline])
				}

				// Is there a truncated metric in the current input buffer?
				if lastNewline < bytesRead-1 {
					// Save the unterminated data for prepending to next input.
					remBytes = (bytesRead - 1) - lastNewline
					copy(remBuf, buf[lastNewline+1:])
				} else {
					// Current input buffer ends on a metrics boundary.
					remBytes = 0
				}

				go cpl.getUDPData(line)

			} else {
				if err.(net.Error).Timeout() {
					config.G.Log.System.LogDebug("CarbonUDP Read() timed out")
				} else {
					config.G.Log.System.LogWarn("CarbonUDP Read() error: %v", err)
				}
			}
		}
	}
}

// getUDPData scans data received from a UDP connection and dispatches it.
func (cpl *CarbonPlaintextListener) getUDPData(buf string) {

	// Carbon metrics are terminated by newlines. Read line-by-line, and dispatch.
	scanner := bufio.NewScanner(strings.NewReader(buf))
	for scanner.Scan() {
		cpl.metricHandler(scanner.Text())
	}
	config.G.Log.System.LogDebug("Returning from getUDPData")
}

// metricHandler reads, parses, and sends on a Carbon data packet.
func (cpl *CarbonPlaintextListener) metricHandler(line string) {

	// Examine metric to ensure that it's a valid carbon metric triplet.
	splitMetric := strings.Fields(line)
	if len(splitMetric) != 3 {
		// Log this as a Warn, because it's the client's error, not ours.
		config.G.Log.System.LogWarn("Malformed Carbon metric, expected 3 fields, found %d: \"%s\"", len(splitMetric), line)
		logging.Statsd.Client.Inc(config.G.Statsd.Events.ReceiveFail.Key, 1, config.G.Statsd.Events.ReceiveFail.SampleRate)
		return
	}

	// Pull out the first field from the triplet.
	statPath := splitMetric[0]

	// Pull out and validate the second field from the triplet.
	val, err := strconv.ParseFloat(splitMetric[1], 64)
	if err != nil {
		config.G.Log.System.LogWarn("Malformed Carbon metric, cannnot parse value as float: \"%s\"", splitMetric[1])
		logging.Statsd.Client.Inc(config.G.Statsd.Events.ReceiveFail.Key, 1, config.G.Statsd.Events.ReceiveFail.SampleRate)
		return
	}

	// Pull out and validate the third field from the triplet.
	ts, err := strconv.ParseFloat(splitMetric[2], 64)
	if err != nil {
		config.G.Log.System.LogWarn("Malformed Carbon metric, cannnot parse timestamp as float: \"%s\"", splitMetric[2])
		logging.Statsd.Client.Inc(config.G.Statsd.Events.ReceiveFail.Key, 1, config.G.Statsd.Events.ReceiveFail.SampleRate)
		return
	}

	// Assemble into canonical struct and send to queue manager.
	hash := int(pearson.Hash8(statPath)) // for debugging
	index := int(pearson.Hash8(statPath)) % len(config.G.Carbon.Peers)
	if cpl.myHostPort == config.G.Carbon.Peers[index] {
		config.G.Log.System.LogInfo("Mine! %-30s %3d %d %s", statPath, hash, index, config.G.Carbon.Peers[index])
		config.G.Channels.DataStore <- config.CarbonMetric{statPath, val, ts}
	} else {
		config.G.Log.System.LogInfo("      %-30s %3d %d %s", statPath, hash, index, config.G.Carbon.Peers[index])
		// TODO: Send to appropriate peer.
	}
	logging.Statsd.Client.Inc(config.G.Statsd.Events.ReceiveOK.Key, 1, config.G.Statsd.Events.ReceiveOK.SampleRate)
}
