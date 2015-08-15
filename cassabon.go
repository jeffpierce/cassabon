package main

import (
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/jeffpierce/cassabon/logging"
)

func main() {

	// Variables populated from the command line.
	var logDir string

	// Get options provided on the command line.
	flag.StringVar(&logDir, "logdir", "", "Name of directory to contain log files (stderr if unspecified)")
	flag.Parse()

	// Set up reload and termination signal handlers.
	var sighup = make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	var sigterm = make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// Set up logging and stats reporting.
	var logfSYS, logfRCV, logfAPI string
	if logDir != "" {
		logDir, _ = filepath.Abs(logDir)
		logfSYS = filepath.Join(logDir, "cassabon.sys.log")
		logfRCV = filepath.Join(logDir, "cassabon.rcv.log")
		logfAPI = filepath.Join(logDir, "cassabon.api.log")
	}
	logSYS := logging.NewLogger("SYS", logfSYS, logging.Debug)
	defer logSYS.Close()
	logRCV := logging.NewLogger("RCV", logfRCV, logging.Debug)
	defer logRCV.Close()
	logAPI := logging.NewLogger("API", logfAPI, logging.Debug)
	defer logAPI.Close()
	if err := logging.S.Open("927.0.0.1:8125", "cassabon"); err != nil {
		logSYS.Log(logging.Error, "Stats disabled: %v", err)
	}
	defer logging.S.Close()

	// Perform initialization that isn't repeated on every SIGHUP.
	logSYS.Log(logging.Info, "Application startup in progress")

	// Repeat until terminated by SIGINT/SIGTERM.
	repeat := true
	for repeat {

		// Perform initialization that is repeated on every SIGHUP.
		logSYS.Log(logging.Info, "Application reading and applying current configuration")

		// Wait for receipt of a recognized signal.
		logSYS.Log(logging.Info, "Application running")
		select {
		case <-sighup:
			logSYS.Log(logging.Info, "Application received SIGHUP")
			logging.Reopen()
		case <-sigterm:
			logSYS.Log(logging.Info, "Application received SIGINT/SIGTERM, preparing to terminate")
			repeat = false
		}
	}

	// Final cleanup.
	logSYS.Log(logging.Info, "Application termination complete")
}
