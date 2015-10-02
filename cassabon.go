package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/jeffpierce/cassabon/api"
	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/datastore"
	"github.com/jeffpierce/cassabon/listener"
	"github.com/jeffpierce/cassabon/logging"
)

func main() {

	// Recover cleanly from panics with a message to stderr.
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintf(os.Stderr, "ABORT: %v\n", err)
		}
	}()

	// The name of the YAML configuration file.
	var confFile string

	// Get options provided on the command line.
	flag.StringVar(&confFile, "conf", "config/cassabon.yaml", "Location of YAML configuration file.")
	flag.Parse()

	// Read the configuration file from disk.
	if err := config.ReadConfigurationFile(confFile); err != nil {
		panic(fmt.Errorf("Unable to load configuration: %v", err))
	}
	// Populate the global config with values used only once.
	config.LoadStartupValues()

	// Set up logging.
	sev, errLogLevel := logging.TextToSeverity(config.G.Log.Loglevel)
	if config.G.Log.Logdir != "" {
		logDir, _ := filepath.Abs(config.G.Log.Logdir)
		config.G.Log.System = logging.NewLogger("system", filepath.Join(logDir, "cassabon.system.log"), sev)
		config.G.Log.Carbon = logging.NewLogger("carbon", filepath.Join(logDir, "cassabon.carbon.log"), sev)
		config.G.Log.API = logging.NewLogger("api", filepath.Join(logDir, "cassabon.api.log"), logging.Unclassified)
	} else {
		config.G.Log.System = logging.NewLogger("system", "", sev)
		config.G.Log.Carbon = logging.NewLogger("carbon", "", sev)
		config.G.Log.API = logging.NewLogger("api", "", logging.Unclassified)
	}
	defer config.G.Log.System.Close()
	defer config.G.Log.Carbon.Close()
	defer config.G.Log.API.Close()

	// Announce the application startup in the logs.
	config.G.Log.System.LogInfo("Startup in progress")
	if errLogLevel != nil {
		config.G.Log.System.LogWarn("Bad command line argument: %v", errLogLevel)
	}

	// Now that we have a logger to report warnings, populate the remainder of the global config.
	config.G.Log.System.LogInfo("Reading configuration file %s", confFile)
	config.LoadRefreshableValues()
	config.LoadRollups()

	// Set up reload and termination signal handlers.
	var sighup = make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	var sigterm = make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// Set up stats reporting.
	if config.G.Statsd.Host != "" {
		if err := logging.Statsd.Open(config.G.Statsd.Host, config.G.Statsd.Port, "cassabon"); err != nil {
			config.G.Log.System.LogError("Not reporting to statsd: %v", err)
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
	config.G.Channels.DataStore = make(chan config.CarbonMetric, config.G.Channels.DataStoreChanLen)
	config.G.Channels.IndexStore = make(chan config.CarbonMetric, config.G.Channels.IndexStoreChanLen)
	config.G.Channels.Gopher = make(chan config.IndexQuery, config.G.Channels.GopherChanLen)

	// Create and initialize the internal modules.
	storeManager := new(datastore.StoreManager)
	statIndexer := new(datastore.MetricsIndexer)
	statGopher := new(datastore.StatPathGopher)
	carbonListener := new(listener.CarbonPlaintextListener)
	storeManager.Init()
	statIndexer.Init()
	statGopher.Init()
	carbonListener.Init()

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
				config.G.Log.System.LogError("Unable to load configuration: %v", err)
			} else {
				config.LoadRefreshableValues()
				sev, _ := logging.TextToSeverity(config.G.Log.Loglevel)
				config.G.Log.System.SetLogLevel(sev)
			}
		}

		// Start the internal modules, Carbon listener last.
		storeManager.Start()
		statIndexer.Start()
		statGopher.Start()
		carbonListener.Start()

		// Start Cassabon Web API
		api := new(api.CassabonAPI)
		api.Start()
		config.G.Log.System.LogInfo("Initialization complete")

		// Wait for receipt of a recognized signal.
		select {

		case <-config.G.OnPeerChange:
			config.G.Log.System.LogInfo("Received OnPeerChange")
			api.Stop()                  // Notify API to stop
			close(config.G.OnReload1)   // Notify all externally-listening goroutines to exit
			config.G.OnReload1WG.Wait() // Wait for them to exit
			close(config.G.OnReload2)   // Notify all reloadable goroutines to exit
			config.G.OnReload2WG.Wait() // Wait for them to exit

		case <-sighup:
			config.G.Log.System.LogInfo("Received SIGHUP")
			configIsStale = true
			api.Stop()                  // Notify API to stop
			close(config.G.OnReload1)   // Notify all externally-listening goroutines to exit
			config.G.OnReload1WG.Wait() // Wait for them to exit
			close(config.G.OnReload2)   // Notify all reloadable goroutines to exit
			config.G.OnReload2WG.Wait() // Wait for them to exit
			logging.Reopen()

		case <-sigterm:
			config.G.Log.System.LogInfo("Received SIGINT/SIGTERM, preparing to terminate")
			api.Stop()                  // Notify API to stop
			close(config.G.OnReload1)   // Notify all externally-listening goroutines to exit
			config.G.OnReload1WG.Wait() // Wait for them to exit
			close(config.G.OnReload2)   // Notify all reloadable goroutines to exit
			config.G.OnReload2WG.Wait() // Wait for them to exit
			close(config.G.OnExit)      // Notify all persistent goroutines to exit
			config.G.OnExitWG.Wait()    // Wait for them to exit
			repeat = false

		}
	}

	// Final cleanup.
	config.G.Log.System.LogInfo("Termination complete")
}
