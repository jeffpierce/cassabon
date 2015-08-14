package logging

import (
	"bufio"
	"os"
	"testing"
)

func TestLogger(t *testing.T) {

	const FACILITY string = "TEST"
	const LOGFILE string = "./test.log"

	os.Remove(LOGFILE)

	L := new(FileLogger)

	L.open(FACILITY, "", Debug)
	L.Log(Debug, "Hello %s!", "world")
	L.Close()

	L.open(FACILITY, LOGFILE, Debug)
	L.Log(Fatal, "Logging at level DEBUG")
	L.Log(Debug, "Hello %s!", "debug")
	L.Log(Info, "Hello %s!", "info")
	L.Log(Warn, "Hello %s!", "warn")
	L.Log(Error, "Hello %s!", "error")
	L.Close()

	L.open(FACILITY, LOGFILE, Info)
	L.Log(Fatal, "Logging at level INFO")
	L.Log(Debug, "Hello %s!", "debug")
	L.Log(Info, "Hello %s!", "info")
	L.Log(Warn, "Hello %s!", "warn")
	L.Log(Error, "Hello %s!", "error")
	L.Close()

	L.open(FACILITY, LOGFILE, Warn)
	L.Log(Fatal, "Logging at level WARN")
	L.Log(Debug, "Hello %s!", "debug")
	L.Log(Info, "Hello %s!", "info")
	L.Log(Warn, "Hello %s!", "warn")
	L.Log(Error, "Hello %s!", "error")
	L.Close()

	L.open(FACILITY, LOGFILE, Error)
	L.Log(Fatal, "Logging at level ERROR")
	L.Log(Debug, "Hello %s!", "debug")
	L.Log(Info, "Hello %s!", "info")
	L.Log(Warn, "Hello %s!", "warn")
	L.Log(Error, "Hello %s!", "error")
	L.Close()

	L.open(FACILITY, LOGFILE, Fatal)
	L.Log(Fatal, "Logging at level FATAL")
	L.Log(Debug, "Hello %s!", "debug")
	L.Log(Info, "Hello %s!", "info")
	L.Log(Warn, "Hello %s!", "warn")
	L.Log(Error, "Hello %s!", "error")
	L.SetLogLevel(Debug)
	L.Log(Debug, "Hello %s!", "debug")
	L.Log(Info, "Hello %s!", "info")
	L.Log(Warn, "Hello %s!", "warn")
	L.Log(Error, "Hello %s!", "error")
	L.Close()

	var lines []string
	var expected = 25

	fp, err := os.Open(LOGFILE)
	if err != nil {
		t.Errorf("Unable to open test log file: %v", err)
	}
	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if len(lines) != expected {
		t.Errorf("Wrong number of log lines were emitted. Found %d, expected %d", len(lines), expected)
	} else {
		os.Remove(LOGFILE)
	}
}
