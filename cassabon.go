package main

import (
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/jeffpierce/cassabon/config"
	"github.com/jeffpierce/cassabon/datastore"
	"github.com/jeffpierce/cassabon/listener"
	"github.com/jeffpierce/cassabon/logging"
)

func main() {

	// The name of the YAML configuration file.
	var confFile string

	// Get options provided on the command line.
	flag.StringVar(&confFile, "conf", "config/cassabon.yaml", "Location of YAML configuration file.")
	flag.StringVar(&config.G.Log.Logdir, "logdir", "", "Name of directory to contain log files (stderr if unspecified)")
	flag.StringVar(&config.G.Log.Loglevel, "loglevel", "", "Log level: debug|info|warn|error|fatal")
	flag.StringVar(&config.G.Statsd.Host, "statsdhost", "", "statsd host or IP address")
	flag.StringVar(&config.G.Statsd.Port, "statsdport", "8125", "statsd port")
	flag.Parse()

	// Fill in startup values not provided on the command line, if available.
	if confFile != "" {
		config.GetConfiguration(confFile)
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

	config.G.Channels.DataStore = make(chan config.CarbonMetric, config.G.Channels.DataStoreChanLen)
	config.G.Channels.Indexer = make(chan config.CarbonMetric, config.G.Channels.IndexerChanLen)

	// Repeat until terminated by SIGINT/SIGTERM.
	configIsStale := false
	repeat := true
	for repeat {

		// Perform initialization that is repeated on every SIGHUP.
		config.G.Log.System.LogInfo("Application reading and applying current configuration")
		config.G.Quit = make(chan struct{}, 1)

		// Re-read the configuration to get any updated values.
		if configIsStale && confFile != "" {
			config.GetConfiguration(confFile)
		}

		// Start the StoreManager.
		ds := new(datastore.StoreManager)
		ds.Init()

		// Initialize a Redis connection pool.if config.G.Redis.Index.Sentinel {
		if config.G.Redis.Index.Sentinel {
			config.G.Log.System.LogDebug("Initializing Redis client (Sentinel)")
			rc := middleware.RedisFailoverClient(
				config.G.Redis.Index.Addr,
				config.G.Redis.Index.Pwd,
				config.G.Redis.Index.Master,
				config.G.Redis.Index.DB,
			)
		} else {
			config.G.Log.System.LogDebug("Initializing Redis client...")
			rc := middleware.RedisClient(
				config.G.Redis.Index.Addr,
				config.G.Redis.Index.Pwd,
				config.G.Redis.Index.DB,
			)
		}

		defer rc.Close()

		// Start the Carbon listener; TCP, UDP, or both.
		switch config.G.Carbon.Protocol {
		case "tcp":
			go listener.CarbonTCP(config.G.Carbon.Address, config.G.Carbon.Port)
			config.G.WG.Add(1)
		case "udp":
			go listener.CarbonUDP(config.G.Carbon.Address, config.G.Carbon.Port)
			config.G.WG.Add(1)
		default:
			go listener.CarbonTCP(config.G.Carbon.Address, config.G.Carbon.Port)
			go listener.CarbonUDP(config.G.Carbon.Address, config.G.Carbon.Port)
			config.G.WG.Add(2)
		}

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
