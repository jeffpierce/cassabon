package main

import (
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/logging"
)

func main() {

	// Variables populated from the command line.
	var confFile, logDir, logLevel string

	// Get options provided on the command line.
	flag.StringVar(&confFile, "conf", "", "Location of YAML configuration file.")
	flag.StringVar(&logDir, "logdir", "", "Name of directory to contain log files (stderr if unspecified)")
	flag.StringVar(&logLevel, "loglevel", "debug", "Log level: debug|info|warn|error|fatal")
	flag.Parse()

	// Load configuration file if specified.
	var cnf config.CassabonConfig
	if confFile != "" {
		cnf = config.ParseConfig(confFile)
	}

	if logDir == "" && confFile != "" {
		logDir = cnf.Logging.Logdir
	}

	// Set up reload and termination signal handlers.
	var sighup = make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	var sigterm = make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// Set up logging and stats reporting.
	var logfSYS, logfRCV, logfAPI string
	if logDir != "" {
		logDir, _ = filepath.Abs(logDir)
		logfSYS = filepath.Join(logDir, "cassabon.system.log")
		logfRCV = filepath.Join(logDir, "cassabon.carbon.log")
		logfAPI = filepath.Join(logDir, "cassabon.api.log")
	}
	sev, errLogLevel := logging.TextToSeverity(logLevel)
	logSYS := logging.NewLogger("system", logfSYS, sev)
	defer logSYS.Close()
	logRCV := logging.NewLogger("carbon", logfRCV, sev)
	defer logRCV.Close()
	logAPI := logging.NewLogger("api", logfAPI, sev)
	defer logAPI.Close()
	if err := logging.S.Open("127.0.0.1:8125", "cassabon"); err != nil {
		logSYS.LogError("Stats disabled: %v", err)
	}
	defer logging.S.Close()

	// Perform initialization that isn't repeated on every SIGHUP.
	logSYS.LogInfo("Application startup in progress")
	if errLogLevel != nil {
		logSYS.LogWarn("Bad command line argument: %v", errLogLevel)
	}

	// Repeat until terminated by SIGINT/SIGTERM.
	repeat := true
	for repeat {

		// Perform initialization that is repeated on every SIGHUP.
		logSYS.LogInfo("Application reading and applying current configuration")

		// Wait for receipt of a recognized signal.
		logSYS.LogInfo("Application running")
		select {
		case <-sighup:
			logSYS.LogInfo("Application received SIGHUP")
			logging.Reopen()
		case <-sigterm:
			logSYS.LogInfo("Application received SIGINT/SIGTERM, preparing to terminate")
			repeat = false
		}
	}

	// Final cleanup.
	logSYS.LogInfo("Application termination complete")
}
