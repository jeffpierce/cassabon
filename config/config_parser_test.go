package config

import (
	"fmt"
	"reflect"
	"testing"
)

func TestParseConfig(t *testing.T) {
	// Load yaml into config struct
	config := ParseConfig("config_test.yaml")

	// Ensure we have a struct
	typevar := reflect.TypeOf(config).Kind()
	fmt.Println("Our config is a struct:", typevar == reflect.Struct)
	if typevar != reflect.Struct {
		t.Errorf("We received a %v back from the config parser, expected struct.", typevar)
	}
	// Test for a value.
	fmt.Println("Statsd port is 8125:", config.Statsd.Port == 8125)
	if config.Statsd.Port != 8125 {
		t.Errorf("Got %d as the Statsd port, expected 8125", config.Statsd.Port)
	}
}
