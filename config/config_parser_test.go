package config

import (
	"fmt"
	"reflect"
	"testing"
)

func TestParseConfig(t *testing.T) {
	// Load yaml into config struct
	config := ParseConfig("config_test.yaml")

	// Ensure we have a config.CassabonConfig
	typevar := reflect.TypeOf(config).Kind()
	fmt.Println("Our config is a struct:", typevar == reflect.Struct)
	if typevar != reflect.Struct {
		t.Errorf("We received a %v back from the config parser, expected struct.", typevar)
	}
}
