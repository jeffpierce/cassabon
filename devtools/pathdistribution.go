package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/pearson"
)

func main() {

	var confFile string
	var printDetail, printSummary, asCSV bool

	flag.StringVar(&confFile, "conf", "config/cassabon.yaml", "Location of YAML configuration file")
	flag.BoolVar(&printDetail, "d", false, "for summaries, also print the detail")
	flag.BoolVar(&printSummary, "s", false, "print summary of hash distribution")
	flag.BoolVar(&asCSV, "csv", false, "print detail in comma-separated format")
	flag.Parse()

	// Read the configuration file.
	if err := config.ReadConfigurationFile(confFile); err != nil {
		config.G.Log.System.LogFatal("Unable to load configuration: %s", err.Error())
	}
	config.LoadStartupValues()
	config.LoadRefreshableValues()

	// Initialize.
	var peers = sortedMapToArray(config.G.Carbon.Peers)
	var numPeers = len(config.G.Carbon.Peers)
	var totalIn int64
	var bucket = make(map[int]int64)

	// Print header matter.
	if printSummary {
		fmt.Printf("%d configured hosts:\n", numPeers)
		for i, v := range peers {
			fmt.Printf("%3d %s\n", i, v)
		}
		if printDetail {
			fmt.Println("")
		}
	}

	// Common routine to accumulate counts and print path detail.
	var printLine = func(p string) {
		p = strings.TrimSpace(p)
		if p == "" {
			return
		}
		totalIn++
		peerIndex := int(pearson.Hash8(p)) % numPeers
		if v, found := bucket[peerIndex]; found {
			bucket[peerIndex] = v + 1
		} else {
			bucket[peerIndex] = 1
		}
		if !printSummary || printDetail {
			if asCSV {
				fmt.Printf("%d,%s\n", peerIndex, p)
			} else {
				fmt.Printf("%5d %s\n", peerIndex, p)
			}
		}
	}

	// Print output for paths found on the command line.
	for _, p := range flag.Args() {
		printLine(p)
	}

	// Print output for paths found on stdin.
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		// Data is being piped to stdin.
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			printLine(scanner.Text())
		}
	}

	// Print the hash breakdown.
	if printSummary {
		fmt.Printf("\nHash distribution:\n")
		fmt.Printf("Index     Count Host\n")
		for i, v := range peers {
			fmt.Printf("%5d %9d %s\n", i, bucket[i], v)
		}
		fmt.Printf("Total %9d\n", totalIn)
	}
}

// sortedMapToArray converts a map to an array of its values, ordered by key.
// (copypasted out of listener/peerlist.go)
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
