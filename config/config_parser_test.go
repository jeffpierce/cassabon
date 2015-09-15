package config

import (
	//"fmt"
	//"reflect"
	"testing"
)

func TestParseConfig(t *testing.T) {
	// Load yaml into config struct
	ReadConfigurationFile("config_test.yaml", false)

	/*
		// Ensure we have a struct
		typevar := reflect.TypeOf(config).Kind()
		fmt.Println("Our config is a struct:", typevar == reflect.Ptr)
		if typevar != reflect.Ptr {
			t.Errorf("We received a %v back from the config parser, expected ptr.", typevar)
		}
		// Test for a value.
		fmt.Println("Statsd port is 8125:", config.Statsd.Port == "8125")
		if config.Statsd.Port != "8125" {
			t.Errorf("Got %s as the Statsd port, expected 8125", config.Statsd.Port)
		}
	*/
}
