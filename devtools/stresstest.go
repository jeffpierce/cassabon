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
	flag.Uint64Var(&rate, "rate", 1000, "number of stats entries per second to generate")
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

	// Open a TCP connection to Cassabon.
	conn, err := net.Dial(protocol, "127.0.0.1:2003")
	if err != nil {
		fmt.Printf("%v\nDid you start Cassabon?\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Set the tick frequency (smaller numbers == faster).
	statsTicker := time.NewTicker(tickInterval)
	defer statsTicker.Stop()

	// Emit 5 stats on every tick.
	var ts, counter float64
	var countUp bool = true
	for _ = range statsTicker.C {
		select {
		case <-sigterm:
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
				fmt.Fprintf(conn, "foo.bar.baz.average %f %f\n", counter, ts)
				fmt.Fprintf(conn, "foo.bar.baz.max %f %f\n", counter, ts)
				fmt.Fprintf(conn, "foo.bar.baz.min %f %f\n", counter, ts)
				fmt.Fprintf(conn, "foo.bar.baz.sum %f %f\n", counter, ts)

				// Provide a true count of the number of stats emitted.
				fmt.Fprintf(conn, "foo.bar.baz.count %f %f\n", 5.0, ts)
			}
		}
	}
}
