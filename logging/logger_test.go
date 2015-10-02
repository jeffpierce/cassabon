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
	L.init(FACILITY)

	L.Open("", Debug)
	L.LogDebug("Hello %s!", "world")
	L.Close()

	L.Open(LOGFILE, Debug)
	L.LogFatal("Logging at level DEBUG")
	L.LogDebug("Hello %s!", "debug")
	L.LogInfo("Hello %s!", "info")
	L.LogWarn("Hello %s!", "warn")
	L.LogError("Hello %s!", "error")
	L.Close()

	L.Open(LOGFILE, Info)
	L.LogFatal("Logging at level INFO")
	L.LogDebug("Hello %s!", "debug")
	L.LogInfo("Hello %s!", "info")
	L.LogWarn("Hello %s!", "warn")
	L.LogError("Hello %s!", "error")
	L.Close()

	L.Open(LOGFILE, Warn)
	L.LogFatal("Logging at level WARN")
	L.LogDebug("Hello %s!", "debug")
	L.LogInfo("Hello %s!", "info")
	L.LogWarn("Hello %s!", "warn")
	L.LogError("Hello %s!", "error")
	L.Close()

	L.Open(LOGFILE, Error)
	L.LogFatal("Logging at level ERROR")
	L.LogDebug("Hello %s!", "debug")
	L.LogInfo("Hello %s!", "info")
	L.LogWarn("Hello %s!", "warn")
	L.LogError("Hello %s!", "error")
	L.Close()

	L.Open(LOGFILE, Fatal)
	L.LogFatal("Logging at level FATAL")
	L.LogDebug("Hello %s!", "debug")
	L.LogInfo("Hello %s!", "info")
	L.LogWarn("Hello %s!", "warn")
	L.LogError("Hello %s!", "error")
	L.SetLogLevel(Debug)
	L.LogDebug("Hello %s!", "debug")
	L.LogInfo("Hello %s!", "info")
	L.LogWarn("Hello %s!", "warn")
	L.LogError("Hello %s!", "error")
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
