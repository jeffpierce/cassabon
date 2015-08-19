// Package listener implements the listener pool and handlers for metrics.
package listener

import (
	"bufio"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/logging"
)

// CarbonMetric is the canonical representation of Carbon data.
type CarbonMetric struct {
	Path      string  // Metric path
	Value     float64 // Metric Value
	Timestamp float64 // Epoch timestamp
}

// CarbonTCP listens for incoming Carbon TCP traffic and dispatches it.
func CarbonTCP(addr string, port string) {

	// Resolve the address:port, and start listening for TCP connections.
	tcpaddr, _ := net.ResolveTCPAddr("tcp4", net.JoinHostPort(addr, port))
	tcpListener, err := net.ListenTCP("tcp4", tcpaddr)
	if err != nil {
		// If we can't grab a port, we can't do our job.  Log, whine, and crash.
		config.G.Log.System.LogFatal("Cannot listen for Carbon on TCP address %s: %v", tcpListener.Addr().String(), err)
		os.Exit(3)
	}
	defer tcpListener.Close()
	config.G.Log.System.LogInfo("Listening on %s TCP for Carbon plaintext protocol", tcpListener.Addr().String())

	// Start listener and pass incoming connections to handler.
	for {
		select {
		case <-config.G.Quit:
			config.G.Log.System.LogInfo("CarbonTCP received QUIT message")
			config.G.WG.Done()
			return
		default:
			// On receipt of a connection, spawn a goroutine to handle it.
			tcpListener.SetDeadline(time.Now().Add(5 * time.Second))
			if conn, err := tcpListener.Accept(); err == nil {
				go getTCPData(conn)
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
func getTCPData(conn net.Conn) {

	// Carbon metrics are terminated by newlines. Read line-by-line, and dispatch.
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		metricHandler(scanner.Text())
	}
	config.G.Log.Carbon.LogDebug("Returning from getTCPData")
}

// CarbonUDP listens for incoming Carbon UDP traffic and dispatches it.
func CarbonUDP(addr string, port string) {

	// Resolve the address:port, and start listening for UDP connections.
	udpaddr, _ := net.ResolveUDPAddr("udp4", net.JoinHostPort(addr, port))
	udpConn, err := net.ListenUDP("udp", udpaddr)
	if err != nil {
		// If we can't grab a port, we can't do our job.  Log, whine, and crash.
		config.G.Log.System.LogFatal("Cannot listen for Carbon on UDP address %s: %v", udpConn.LocalAddr().String(), err)
		os.Exit(3)
	}
	defer udpConn.Close()
	config.G.Log.System.LogInfo("Listening on %s UDP for Carbon plaintext protocol", udpConn.LocalAddr().String())

	// Start reading UDP packets and pass data to handler.
	buf := make([]byte, 150)
	for {
		select {
		case <-config.G.Quit:
			config.G.Log.System.LogInfo("CarbonUDP received QUIT message")
			config.G.WG.Done()
			return
		default:
			udpConn.SetDeadline(time.Now().Add(5 * time.Second))
			_, _, err := udpConn.ReadFromUDP(buf)
			if err == nil {
				go getUDPData(string(buf))
			} else {
				if err.(net.Error).Timeout() {
					config.G.Log.System.LogDebug("CarbonUDP Read() timed out")
				} else {
					config.G.Log.System.LogDebug("CarbonUDP Read() error: %v", err)
				}
			}
		}
	}
}

// getUDPData scans data received from a UDP connection and dispatches it.
func getUDPData(buf string) {

	// Carbon metrics are terminated by newlines. Read line-by-line, and dispatch.
	scanner := bufio.NewScanner(strings.NewReader(buf))
	for scanner.Scan() {
		// Scanner returns nulls remaining in fixed-size buffer at end-of-data; skip them.
		if scanner.Bytes()[0] != byte(0) {
			metricHandler(scanner.Text())
		}
	}
	config.G.Log.Carbon.LogDebug("Returning from getUDPData")
}

// metricHandler reads, parses, and sends on a Carbon data packet.
func metricHandler(line string) {

	// Examine metric to ensure that it's a valid carbon metric triplet.
	splitMetric := strings.Fields(line)
	if len(splitMetric) != 3 {
		// Log this as a Warn, because it's the client's error, not ours.
		config.G.Log.Carbon.LogWarn("Malformed metric, expected 3 fields, found %d: \"%s\"", len(splitMetric), line)
		logging.Statsd.Client.Inc("cassabon.carbon.received.failure", 1, 1.0)
		return
	}

	// Pull out the first field from the triplet.
	statPath := splitMetric[0]

	// Pull out and validate the second field from the triplet.
	val, err := strconv.ParseFloat(splitMetric[1], 64)
	if err != nil {
		config.G.Log.Carbon.LogWarn("Malformed metric, cannnot parse value as float: \"%s\"", splitMetric[1])
		logging.Statsd.Client.Inc("cassabon.carbon.received.failure", 1, 1.0)
		return
	}

	// Pull out and validate the third field from the triplet.
	ts, err := strconv.ParseFloat(splitMetric[2], 64)
	if err != nil {
		config.G.Log.Carbon.LogWarn("Malformed metric, cannnot parse timestamp as float: \"%s\"", splitMetric[2])
		logging.Statsd.Client.Inc("cassabon.carbon.received.failure", 1, 1.0)
		return
	}

	// Assemble into canonical struct and send to enqueueing worker.
	parsedMetric := CarbonMetric{statPath, val, ts}
	config.G.Log.Carbon.LogDebug("Woohoo! Pushing metric into channel: %v", parsedMetric)
	logging.Statsd.Client.Inc("cassabon.carbon.received.success", 1, 1.0)
}
