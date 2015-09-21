// Generate dummy Carbon stats for exercising Cassabon.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	// Get options provided on the command line.
	var protocol string
	var rate uint64
	flag.StringVar(&protocol, "protocol", "tcp", "[tcp|udp]")
	flag.Uint64Var(&rate, "rate", 30, "number of stats entries per second to generate")
	flag.Parse()

	// Validate the protocol and the rate.
	if protocol != "tcp" && protocol != "udp" {
		fmt.Printf("protocol must be \"tcp\" or \"udp\"\n")
		os.Exit(1)
	}
	if rate < 5 {
		fmt.Printf("rate must be greater than or equal to 5\n")
		os.Exit(1)
	}
	if rate%5 != 0 {
		fmt.Printf("rate must be divisible by 5\n")
		os.Exit(1)
	}

	// We emit 5 stats entries per tick, so factor the rate and calculate the tick interval.
	rate = rate / 5
	var tickInterval time.Duration
	var perTick int
	if rate >= 2000 {
		// For high tick rates, increase accuracy by scaling down the rate and
		// doing more work per tick.
		if rate%10 != 0 {
			fmt.Printf("rate must be divisible by 10\n")
			os.Exit(1)
		}
		rate = rate / 10
		perTick = 10
	} else {
		perTick = 1
	}
	tickInterval = time.Second / time.Duration(rate)

	// Respond gracefully to Ctrl+C.
	var sigterm = make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	pc := patientConn{}
	pc.Init()
	if protocol == "tcp" {
		// Open a TCP connection to Cassabon.
		if err := pc.OpenTCP("127.0.0.1", 2003); err != nil {
			fmt.Printf("%v\nDid you start Cassabon?\n", err)
			os.Exit(1)
		}
	} else {
		// Open a UDP connection to Cassabon (doesn't fail if Cassabon is not running).
		if err := pc.OpenUDP("127.0.0.1", 2003); err != nil {
			fmt.Printf("%v\n", err) // Must be some other error
			os.Exit(1)
		}
	}
	defer pc.Close()

	// Set the tick frequency (smaller numbers == faster).
	statsTicker := time.NewTicker(tickInterval)
	defer statsTicker.Stop()

	// Emit 5 stats on every tick.
	var ts, counter float64
	var countUp bool = true
	for _ = range statsTicker.C {
		select {
		case <-sigterm:
			// CTRL+C
			fmt.Printf("INTERRUPT\n")
			return
		default:

			// Use a common time for all entries in this tick.
			ts = float64(time.Now().Unix())

			for i := 0; i < perTick; i++ {
				// Counter varies from 1..100, alternating between counting up and down.
				// Note: this is floating point, don't attempt "==".
				if countUp {
					if counter > 98.0 {
						countUp = false
					}
					counter += 1.0
				} else {
					if counter < 3.0 {
						countUp = true
					}
					counter -= 1.0
				}

				// Exercise the different aggregation methods.
				if !pc.Emit("foo.bar.baz.average %f %f\n", counter, ts) {
					break
				}
				if !pc.Emit("foo.bar.baz.max %f %f\n", counter, ts) {
					break
				}
				if !pc.Emit("foo.bar.baz.min %f %f\n", counter, ts) {
					break
				}
				if !pc.Emit("foo.bar.baz.sum %f %f\n", counter, ts) {
					break
				}

				// Provide a true count of the number of stats emitted.
				if !pc.Emit("foo.bar.baz.count %f %f\n", 5.0, ts) {
					break
				}
			}
		}
	}
}

// patientConn detects write failures and retries the connection until it succeeds.
type patientConn struct {
	sigterm chan os.Signal
	proto   string
	tcp     struct {
		isOpen bool
		addr   *net.TCPAddr
		conn   *net.TCPConn
	}
	udp struct {
		isOpen bool
		addr   *net.UDPAddr
		conn   *net.UDPConn
	}
}

// Init performs one-time initialization.
func (pc *patientConn) Init() {
	pc.sigterm = make(chan os.Signal, 1)
	signal.Notify(pc.sigterm, syscall.SIGINT, syscall.SIGTERM)
}

func (pc *patientConn) internalOpenTCP() error {
	var err error
	if pc.tcp.conn, err = net.DialTCP("tcp4", nil, pc.tcp.addr); err == nil {
		pc.tcp.isOpen = true
	}
	return err
}

func (pc *patientConn) internalOpenUDP() error {
	var err error
	if pc.udp.conn, err = net.DialUDP("udp4", nil, pc.udp.addr); err == nil {
		pc.udp.isOpen = true
	}
	return err
}

// OpenTCP makes the initial TCP connection to Cassabon.
func (pc *patientConn) OpenTCP(host string, port int) error {
	fmt.Printf("Connecting to Cassabon at %s:%04d over TCP\n", host, port)
	pc.proto = "tcp"
	pc.tcp.addr = &net.TCPAddr{IP: net.ParseIP(host), Port: port}
	return pc.internalOpenTCP()
}

// OpenUDP makes the initial UDP connection to Cassabon.
// Note: This does NOT return an error if Cassabon isn't running.
func (pc *patientConn) OpenUDP(host string, port int) error {
	fmt.Printf("Connecting to Cassabon at %s:%04d over UDP\n", host, port)
	pc.proto = "udp"
	pc.udp.addr = &net.UDPAddr{IP: net.ParseIP(host), Port: port}
	return pc.internalOpenUDP()
}

// Close performs a guarded close of the connection.
func (pc *patientConn) Close() {
	if pc.tcp.isOpen {
		pc.tcp.conn.Close()
	}
	if pc.udp.isOpen {
		pc.udp.conn.Close()
	}
	pc.tcp.isOpen = false
	pc.udp.isOpen = false
}

// Emit closes the connection if a write fails, and attempts a re-open on each
// subsequent call. Returns FALSE if interrupted while retrying.
func (pc *patientConn) Emit(format string, a ...interface{}) bool {

	wait := false

	if pc.proto == "tcp" {
		if pc.tcp.isOpen {
			if _, err := fmt.Fprintf(pc.tcp.conn, format, a...); err != nil {
				fmt.Printf("%v\nRetrying connection...", err)
				pc.Close()
			}
		} else {
			if err := pc.internalOpenTCP(); err == nil {
				fmt.Printf(" connection resumed\n")
			} else {
				wait = true
			}
		}
	} else {
		if _, err := fmt.Fprintf(pc.udp.conn, format, a...); err != nil {
			wait = true // No recovery needed; simply avoid tight retry loop.
		}
	}

	if wait {
		select {
		case <-pc.sigterm:
			return false
		case <-time.After(time.Second):
		}
	}
	return true
}
