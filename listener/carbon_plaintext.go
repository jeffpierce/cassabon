// Package listener implements the listener pool and handlers for metrics.
package listener

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jeffpierce/cassabon/config"
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
				go metricHandler(conn)
			} else {
				config.G.Log.System.LogDebug("CarbonTCP Accept() timed out")
			}
		}
	}
}

// UDP totally blocks hard.  Need to figure this out. -- Jeff 2015/08/14

/* func CarbonUDP(addr string, port int) {
	udpaddr := net.UDPAddr{Port: port, IP: net.ParseIP(addr)}
	carbonUDPSocket, err := net.ListenUDP("udp", &udpaddr)
	if err != nil {
		// TODO:  Move to our own logger.
		panic(err)
	}

	defer carbonUDPSocket.Close()

	fmt.Printf("Carbon UDP plaintext listener now listening on %s:%d\n", addr, port)

	for {
		go metricHandler(carbonUDPSocket)
	}
} */

func metricHandler(conn net.Conn) {
	// Carbon metrics are terminated by newlines.  Listed for it.
	metric, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		// TODO:  Handle with actual logger/stats.
		fmt.Println("Received this error:", err.Error())
		metric = ""
	}

	// Close connection.
	conn.Close()

	// Examine metric to ensure that it's a valid carbon metric
	for len(metric) != 0 {
		splitMetric := strings.Fields(metric)
		if len(splitMetric) != 3 {
			// TODO:  Handle with actual logger/stats.
			fmt.Println("Bad metric:", metric)
			metric = ""
		}

		statPath := splitMetric[0]
		val, err := strconv.ParseFloat(splitMetric[1], 64)
		if err != nil {
			// TODO:  Handle with actual logger/stats.
			fmt.Printf("Cannot convert value %s to float.\n", splitMetric[1])
			break
		}
		ts, err := strconv.ParseFloat(splitMetric[2], 64)
		if err != nil {
			// TODO:  Handle with actual logger/stats.
			fmt.Printf("Cannot convert timestamp %s to float.\n", splitMetric[2])
			break
		}

		parsedMetric := CarbonMetric{statPath, val, ts}

		// Metric parsed, place in queue, handoff to receiving worker.
		// TODO:  Implement receiving worker
		fmt.Printf("Would queue parsed metric: %+v\n", parsedMetric)
		break
	}
}
