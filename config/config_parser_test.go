package config

import (
	"reflect"
	"testing"
)

// Check to make sure config parser properly parses test field.  All fields are defined in test yaml.
func TestParseConfig(t *testing.T) {
	// Load yaml into config struct
	config := ParseConfig("config_test.yaml")

	// iterate over struct values to ensure all fields loaded.
	s := reflect.ValueOf(&config).Elem()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		if f.Interface() == nil {
			t.Errorf("Config file did not parse properly, found empty field.")
		}
	}
}
