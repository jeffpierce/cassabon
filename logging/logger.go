// Package logging implements file loggers that support log rotation.
package logging

import (
	"fmt"
	"log"
	"os"
	"sync"
)

// The type for logging level.
type Severity int

// The valid logging severities.
const (
	Debug Severity = iota
	Info
	Warn
	Error
	Fatal
)

// NewLogger instantiates and initializes a logger for the given facility.
func NewLogger(logFacility string, logFilename string, logLevel Severity) *FileLogger {

	if _, ok := loggers[logFacility]; !ok {
		loggers[logFacility] = new(FileLogger)
		loggers[logFacility].open(logFacility, logFilename, logLevel)
	}

	return loggers[logFacility]
}

// Reopen non-destructively re-opens all log files, to support log rotation.
func Reopen() {

	for _, l := range loggers {
		l.reopen()
	}
}

// The loggers, one per facility.
var loggers map[string]*FileLogger = map[string]*FileLogger{}

// The text representations of the logging severities.
var severityText = map[Severity]string{
	Debug: "DEBUG",
	Info:  "INFO",
	Warn:  "WARN",
	Error: "ERROR",
	Fatal: "FATAL",
}

// severityToText maps the severity value to a name for printing.
func severityToText(sev Severity) string {
	s, ok := severityText[sev]
	if !ok {
		s = "UNKNOWN"
	}
	return s
}

// The FileLogger object.
type FileLogger struct {
	m           sync.RWMutex // Serialize access to logger during log rotation
	logFacility string       // The name given to this particular logger
	logFilename string       // The name of the log file
	logLevel    Severity     // The severity threshold for generating output.
	opened      bool         // Whether the logger has been opened or not
	logFile     *os.File     // The file handle of the opened file
	logger      *log.Logger  // The logger that writes to the file
}

// SetLogLevel updates the logging level threshold with imediate effect.
func (l *FileLogger) SetLogLevel(logLevel Severity) {
	l.logLevel = logLevel
	l.Log(Info, "Log level set to %s", severityToText(logLevel))
}

// Close releases all resources associated with the logger.
func (l *FileLogger) Close() {

	if !l.opened {
		return
	}

	// Close the logfile.
	l.closeAndOrOpen(3)
	l.opened = false
}

// Log writes a time-stamped message to the log file, with a mutex guard.
func (l *FileLogger) Log(sev Severity, format string, a ...interface{}) {

	if l.opened {
		l.m.RLock()
		defer l.m.RUnlock()
		l.emit(sev, format, a...)
	}
}

// open allocates resources for the logger.
func (l *FileLogger) open(logFacility string, logFilename string, logLevel Severity) {

	if l.opened {
		return
	}

	l.logFacility = logFacility
	l.logFilename = logFilename
	l.logLevel = logLevel

	// Open the logfile.
	l.closeAndOrOpen(1)
	l.opened = true
}

// reopen closes and re-opens the log file to support log rotation.
func (l *FileLogger) reopen() {

	if !l.opened {
		return
	}

	// Close and re-open the logfile.
	l.closeAndOrOpen(2)
}

// openLogfile opens the configured log file.
func (l *FileLogger) openLogfile() *os.File {

	fp, err := os.OpenFile(l.logFilename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		if l.logger != nil {
			// The log-rotation case.
			// Note that when rotating, the pre-rotation log is still open here.
			// DO NOT call the public Log() method, you will cause a deadlock.
			l.emit(Fatal, "Unable to reopen logfile '%v'. Error: '%v'", l.logFilename, err)
			l.logFile.Close()
		} else {
			// The startup case.
			// Warning: when started as a daemon, this may go to /dev/null.
			fmt.Printf("["+severityText[Fatal]+"] Unable to open logfile '%v'. Error: '%v'\n", l.logFilename, err)
		}
		// Inability to log is a fatal error; die with a non-zero exit code.
		// We do not run blind.
		os.Exit(1)
	}

	return fp
}

// rotate closes any open log files and (re-)opens them with the same name.
func (l *FileLogger) closeAndOrOpen(action int) {

	// Disable the log writer.
	l.m.Lock()
	defer l.m.Unlock()

	// Use stderr and skip messages if no log filename was specified.
	if l.logFilename == "" {
		l.logFile = os.Stderr
		l.logger = log.New(l.logFile, "", log.Ldate|log.Lmicroseconds)
		return
	}

	// DO NOT call the public Log() method, you will cause a deadlock.
	switch action {
	case 1:
		// Initial open of the log file.
		l.logFile = l.openLogfile()
		l.logger = log.New(l.logFile, "", log.Ldate|log.Lmicroseconds)
		l.emit(Info, "Log opened")
	case 2:
		// Close log file, and re-open with the same name.
		l.emit(Info, "Log closed on signal")
		l.logFile.Close()
		l.logFile = l.openLogfile()
		l.logger = log.New(l.logFile, "", log.Ldate|log.Lmicroseconds)
		l.emit(Info, "Log reopened on signal")
	case 3:
		// Close the log file.
		l.emit(Info, "Log closed")
		l.logFile.Close()
	}
}

// emit produces a log line, if the severity meets or exceeds the threshold.
func (l *FileLogger) emit(sev Severity, format string, a ...interface{}) {

	// Filter out messages that do not meet the severity threshold.
	if sev < l.logLevel {
		return
	}

	l.logger.Printf("["+l.logFacility+"]["+severityToText(sev)+"] "+format, a...)
}
