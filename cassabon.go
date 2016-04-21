package main

import (
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/jeffpierce/cassabon/api"
	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/datastore"
	"github.com/jeffpierce/cassabon/listener"
	"github.com/jeffpierce/cassabon/logging"
)

func main() {

	// Recover cleanly from panics with a message to stderr.
	defer config.G.OnPanic()

	// The name of the YAML configuration file.
	var confFile, loglevel string
	var strict, bootstrap bool

	// The WaitGroups for managing orderly goroutine reloads and termination.
	var onReload1WG sync.WaitGroup // Wait on this if you receive external inputs
	var onReload2WG sync.WaitGroup // Wait on this to be certain no more input will arrive
	var onExitWG sync.WaitGroup    // Wait on this for final program termination

	// Get options provided on the command line.
	flag.StringVar(&confFile, "conf", "config/cassabon.yaml", "Location of YAML configuration file")
	flag.StringVar(&loglevel, "loglevel", "", "logging level, to override configuration until SIGHUP")
	flag.BoolVar(&strict, "strict", true, "rollup configuration warnings are fatal")
	flag.BoolVar(&bootstrap, "bootstrap", false, "performs bootstrap on ElasticSearch index.  Run only once.")
	flag.Parse()

	// Create the loggers.
	config.G.Log.System = logging.NewLogger("system")
	config.G.Log.Carbon = logging.NewLogger("carbon")
	config.G.Log.API = logging.NewLogger("api")

	// Read the configuration file from disk.
	if err := config.ReadConfigurationFile(confFile); err != nil {
		config.G.Log.System.LogFatal("Unable to load configuration: %s", err.Error())
	}
	// Populate the global config with values used only once.
	config.LoadStartupValues()

	// Set up logging.
	if len(loglevel) > 0 {
		// This will revert to the configured value at the first SIGHUP.
		config.G.Log.Loglevel = loglevel
	}
	sev, errLogLevel := logging.TextToSeverity(config.G.Log.Loglevel)
	if config.G.Log.Logdir != "" {
		logDir, _ := filepath.Abs(config.G.Log.Logdir)
		config.G.Log.System.Open(filepath.Join(logDir, "system.log"), sev)
		config.G.Log.Carbon.Open(filepath.Join(logDir, "carbon.log"), logging.Unclassified)
		config.G.Log.API.Open(filepath.Join(logDir, "api.log"), logging.Unclassified)
	} else {
		config.G.Log.System.Open("", sev)
		config.G.Log.Carbon.Open("", logging.Unclassified)
		config.G.Log.API.Open("", logging.Unclassified)
	}
	defer config.G.Log.System.Close()
	defer config.G.Log.Carbon.Close()
	defer config.G.Log.API.Close()

	// Announce the application startup in the logs.
	config.G.Log.System.LogInfo("Startup in progress")
	if errLogLevel != nil {
		config.G.Log.System.LogWarn("Configuration error: %s; using %s",
			errLogLevel.Error(), logging.SeverityToText(sev))
	}

	// Now that we have a logger to report warnings, populate the remainder of the global config.
	config.G.Log.System.LogInfo("Reading configuration file %s", confFile)
	config.LoadRefreshableValues()
	if !config.LoadRollups() && strict {
		config.G.Log.System.LogFatal("Errors encountered while loading configuration")
	}

	// Set up reload and termination signal handlers.
	var sighup = make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	var sigterm = make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// Set up stats reporting.
	if config.G.Statsd.Host != "" {
		if err := logging.Statsd.Open(config.G.Statsd.Host, config.G.Statsd.Port, "cassabon"); err != nil {
			config.G.Log.System.LogError("Not reporting to statsd: %s", err.Error())
		} else {
			config.G.Log.System.LogInfo("Reporting to statsd at %s:%s", config.G.Statsd.Host, config.G.Statsd.Port)
		}
	} else {
		logging.Statsd.Open("", "", "cassabon")
		config.G.Log.System.LogInfo("Not reporting to statsd: specify host or IP to enable")
	}
	defer logging.Statsd.Close()

	// Create all the inter-process communication channels.
	config.G.OnPeerChange = make(chan struct{}, 1)
	config.G.OnPeerChangeReq = make(chan struct{}, 1)
	config.G.OnPeerChangeRsp = make(chan struct{}, 1)
	config.G.OnExit = make(chan struct{}, 1)
	config.G.Channels.MetricStore = make(chan config.CarbonMetric, config.G.Channels.MetricStoreChanLen)
	config.G.Channels.MetricRequest = make(chan config.MetricQuery, config.G.Channels.MetricRequestChanLen)
	config.G.Channels.IndexStore = make(chan config.CarbonMetric, config.G.Channels.IndexStoreChanLen)
	config.G.Channels.IndexRequest = make(chan config.IndexQuery, config.G.Channels.IndexRequestChanLen)

	// Create and initialize the internal modules.
	metricManager := new(datastore.MetricManager)
	indexManager := new(datastore.IndexManager)
	carbonListener := new(listener.CarbonPlaintextListener)
	indexManager.Init(bootstrap)
	metricManager.Init(bootstrap, *indexManager)
	carbonListener.Init()

	// MetricManager goroutines persist for the life of the app; start them now.
	metricManager.Start(&onExitWG)

	// Repeat until terminated by SIGINT/SIGTERM.
	configIsStale := false
	repeat := true
	for repeat {

		// Perform initialization that is repeated on every SIGHUP.
		config.G.OnReload1 = make(chan struct{}, 1)
		config.G.OnReload2 = make(chan struct{}, 1)

		// Re-read the configuration to get any updated values.
		if configIsStale {
			config.G.Log.System.LogInfo("Reading configuration file %s", confFile)
			if err := config.ReadConfigurationFile(confFile); err != nil {
				config.G.Log.System.LogError("Unable to load configuration: %s", err.Error())
			} else {
				config.LoadRefreshableValues()
				if sev, err := logging.TextToSeverity(config.G.Log.Loglevel); err == nil {
					config.G.Log.System.SetLogLevel(sev)
				} else {
					config.G.Log.System.LogWarn("Configuration error: %s", err.Error())
				}
			}
		}

		// Start the internal modules, Carbon listener last.
		indexManager.Start(&onReload2WG)
		carbonListener.Start(&onReload1WG, &onReload2WG)

		// Start Cassabon Web API
		api := new(api.CassabonAPI)
		api.Start(&onReload1WG)
		config.G.Log.System.LogInfo("Initialization complete")

		// Wait for receipt of a recognized signal.
		select {

		case <-config.G.OnPeerChange:
			config.G.Log.System.LogInfo("Received OnPeerChange")
			api.Stop()                // Notify API to stop
			close(config.G.OnReload1) // Notify all externally-listening goroutines to exit
			onReload1WG.Wait()        // Wait for them to exit
			close(config.G.OnReload2) // Notify all reloadable goroutines to exit
			onReload2WG.Wait()        // Wait for them to exit

		case <-sighup:
			config.G.Log.System.LogInfo("Received SIGHUP")
			configIsStale = true
			api.Stop()                // Notify API to stop
			close(config.G.OnReload1) // Notify all externally-listening goroutines to exit
			onReload1WG.Wait()        // Wait for them to exit
			close(config.G.OnReload2) // Notify all reloadable goroutines to exit
			onReload2WG.Wait()        // Wait for them to exit
			logging.Reopen()

		case <-sigterm:
			config.G.Log.System.LogInfo("Received SIGINT/SIGTERM, preparing to terminate")
			api.Stop()                // Notify API to stop
			close(config.G.OnReload1) // Notify all externally-listening goroutines to exit
			onReload1WG.Wait()        // Wait for them to exit
			close(config.G.OnReload2) // Notify all reloadable goroutines to exit
			onReload2WG.Wait()        // Wait for them to exit
			close(config.G.OnExit)    // Notify all persistent goroutines to exit
			onExitWG.Wait()           // Wait for them to exit
			repeat = false

		}
	}

	// Final cleanup.
	config.G.Log.System.LogInfo("Termination complete")
}
