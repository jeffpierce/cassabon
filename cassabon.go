package main

import (
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/listener"
	"github.com/jeffpierce/cassabon/logging"
)

func fetchConfiguration(confFile string) {

	// Open, read, and parse the YAML.
	cnf := config.ParseConfig(confFile)

	// Fill in arguments not provided on the command line.
	if config.G.Log.Logdir == "" {
		config.G.Log.Logdir = cnf.Logging.Logdir
	}
	if config.G.Log.Loglevel == "" {
		config.G.Log.Loglevel = cnf.Logging.Loglevel
	}
	if config.G.Statsd.Host == "" {
		config.G.Statsd.Host = cnf.Statsd.Host
	}

	// Copy in values sourced solely from the configuration file.
	config.G.API.Address = cnf.API.Address
	config.G.API.Port = cnf.API.Port
	config.G.Carbon.Address = cnf.Carbon.Address
	config.G.Carbon.Port = cnf.Carbon.Port
	config.G.Carbon.Protocol = cnf.Carbon.Protocol
}

func main() {

	// The name of the YAML configuration file.
	var confFile string

	// Get options provided on the command line.
	flag.StringVar(&confFile, "conf", "", "Location of YAML configuration file.")
	flag.StringVar(&config.G.Log.Logdir, "logdir", "", "Name of directory to contain log files (stderr if unspecified)")
	flag.StringVar(&config.G.Log.Loglevel, "loglevel", "", "Log level: debug|info|warn|error|fatal")
	flag.StringVar(&config.G.Statsd.Host, "statsdhost", "", "statsd host or IP address")
	flag.StringVar(&config.G.Statsd.Port, "statsdport", "8125", "statsd port")
	flag.Parse()

	// Fill in startup values not provided on the command line, if available.
	if confFile != "" {
		fetchConfiguration(confFile)
	}

	// Set up logging.
	sev, errLogLevel := logging.TextToSeverity(config.G.Log.Loglevel)
	if config.G.Log.Logdir != "" {
		logDir, _ := filepath.Abs(config.G.Log.Logdir)
		config.G.Log.System = logging.NewLogger("system", filepath.Join(logDir, "cassabon.system.log"), sev)
		config.G.Log.Carbon = logging.NewLogger("carbon", filepath.Join(logDir, "cassabon.carbon.log"), sev)
		config.G.Log.API = logging.NewLogger("api", filepath.Join(logDir, "cassabon.api.log"), sev)
	} else {
		config.G.Log.System = logging.NewLogger("system", "", sev)
		config.G.Log.Carbon = logging.NewLogger("carbon", "", sev)
		config.G.Log.API = logging.NewLogger("api", "", sev)
	}
	defer config.G.Log.System.Close()
	defer config.G.Log.Carbon.Close()
	defer config.G.Log.API.Close()

	// Announce the application startup in the logs.
	config.G.Log.System.LogInfo("Application startup in progress")
	if errLogLevel != nil {
		config.G.Log.System.LogWarn("Bad command line argument: %v", errLogLevel)
	}

	// Set up stats reporting.
	if config.G.Statsd.Host != "" {
		if err := logging.Statsd.Open(config.G.Statsd.Host, config.G.Statsd.Port, "cassabon"); err != nil {
			config.G.Log.System.LogError("Not reporting to statsd: %v", err)
		} else {
			config.G.Log.System.LogInfo("Reporting to statsd at %s:%s", config.G.Statsd.Host, config.G.Statsd.Port)
		}
		defer logging.Statsd.Close()
	} else {
		config.G.Log.System.LogInfo("Not reporting to statsd: specify host or IP to enable")
	}

	// Set up reload and termination signal handlers.
	var sighup = make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	var sigterm = make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// Repeat until terminated by SIGINT/SIGTERM.
	configIsStale := false
	repeat := true
	for repeat {

		// Perform initialization that is repeated on every SIGHUP.
		config.G.Log.System.LogInfo("Application reading and applying current configuration")
		config.G.Quit = make(chan struct{}, 1)

		// Re-read the configuration to get any updated values.
		if configIsStale && confFile != "" {
			fetchConfiguration(confFile)
		}

		// Start the services we offer.
		go listener.CarbonTCP(config.G.Carbon.Address, config.G.Carbon.Port)

		// Note how many goroutines we started and must wait for on SIGHUP.
		config.G.WG.Add(1)

		// Wait for receipt of a recognized signal.
		config.G.Log.System.LogInfo("Application running")
		select {
		case <-sighup:
			config.G.Log.System.LogInfo("Application received SIGHUP")
			configIsStale = true
			close(config.G.Quit) // Notify all goroutines to exit
			config.G.WG.Wait()   // Wait for them to exit
			logging.Reopen()
		case <-sigterm:
			config.G.Log.System.LogInfo("Application received SIGINT/SIGTERM, preparing to terminate")
			close(config.G.Quit) // Notify all goroutines to exit - do NOT wait
			repeat = false
		}
	}

	// Final cleanup.
	config.G.Log.System.LogInfo("Application termination complete")
}
