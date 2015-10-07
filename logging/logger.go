// Package logging implements file loggers that support log rotation.
package logging

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
)

// The type for logging level.
type Severity int

// The valid logging severities.
const (
	Unclassified Severity = iota
	Debug
	Info
	Warn
	Error
	Fatal
)

// NewLogger instantiates and initializes a logger for the given facility.
func NewLogger(logFacility string) *FileLogger {

	if _, ok := loggers[logFacility]; !ok {
		loggers[logFacility] = new(FileLogger)
		loggers[logFacility].init(logFacility)
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
	Unclassified: "",
	Debug:        "DEBUG",
	Info:         "INFO",
	Warn:         "WARN",
	Error:        "ERROR",
	Fatal:        "FATAL",
}

// severityToText maps the severity value to a name for printing.
func severityToText(sev Severity) string {
	s, ok := severityText[sev]
	if !ok {
		s = "UNKNOWN"
	}
	return s
}

// TextToSeverity derives a Severity from a text string, ignoring case.
func TextToSeverity(s string) (Severity, error) {
	var sev Severity = Debug
	var err error
	switch strings.ToLower(s) {
	case "":
		sev = Unclassified
	case "unclassified":
		sev = Unclassified
	case "debug":
		sev = Debug
	case "info":
		sev = Info
	case "warn":
		sev = Warn
	case "error":
		sev = Error
	case "fatal":
		sev = Fatal
	default:
		err = fmt.Errorf(`"%s" is not a valid logging level; using DEBUG`, s)
	}
	return sev, err
}

// FatalError is a custom error that can be distinguished in the global panic handler.
type FatalError struct {
	file     string
	line     int
	function string
	msg      string
}

// Error function makes the FatalError object conform to the error interface.
func (e *FatalError) Error() string {
	return fmt.Sprintf("FATAL %s:%d %s() %s", e.file, e.line, e.function, e.msg)
}

// The FileLogger object.
type FileLogger struct {
	m           sync.RWMutex // Serialize access to logger during log rotation
	logFacility string       // The name given to this particular logger
	logFilename string       // The name of the log file
	logLevel    Severity     // The severity threshold for generating output.
	opened      bool         // Whether the logger has been opened or not
	skipEmit    bool         // flag to permit panicing without incurring deadlock
	logFile     *os.File     // The file handle of the opened file
	logger      *log.Logger  // The logger that writes to the file
}

func (l *FileLogger) init(logFacility string) {
	l.logFacility = logFacility
}

// SetLogLevel updates the logging level threshold with imediate effect.
func (l *FileLogger) SetLogLevel(logLevel Severity) {
	l.logLevel = logLevel
	l.LogInfo("Log level set to %s", severityToText(logLevel))
}

// Open allocates resources for the logger.
func (l *FileLogger) Open(logFilename string, logLevel Severity) {

	if l.opened {
		return
	}

	l.logFilename = logFilename
	l.logLevel = logLevel

	// Open the logfile.
	l.closeAndOrOpen(1)
	l.opened = true
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

// LogDebug writes a time-stamped Debug message to the log file, with a mutex guard.
func (l *FileLogger) LogDebug(format string, a ...interface{}) {

	if l.opened {
		l.m.RLock()
		defer l.m.RUnlock()
		l.emit(Debug, format, a...)
	}
}

// LogInfo writes a time-stamped Info message to the log file, with a mutex guard.
func (l *FileLogger) LogInfo(format string, a ...interface{}) {

	if l.opened {
		l.m.RLock()
		defer l.m.RUnlock()
		l.emit(Info, format, a...)
	}
}

// LogWarn writes a time-stamped Warn message to the log file, with a mutex guard.
func (l *FileLogger) LogWarn(format string, a ...interface{}) {

	if l.opened {
		l.m.RLock()
		defer l.m.RUnlock()
		l.emit(Warn, format, a...)
	}
}

// LogError writes a time-stamped Error message to the log file, with a mutex guard.
func (l *FileLogger) LogError(format string, a ...interface{}) {

	if l.opened {
		l.m.RLock()
		defer l.m.RUnlock()
		l.emit(Error, format, a...)
	}
}

// LogFatal writes a time-stamped Fatal message to the log file, with a mutex guard.
func (l *FileLogger) LogFatal(format string, a ...interface{}) {

	if l.opened && !l.skipEmit {
		l.m.RLock()
		defer l.m.RUnlock()
		l.emit(Fatal, format, a...)
	}
	pc, file, line, _ := runtime.Caller(1)
	f := strings.Split(runtime.FuncForPC(pc).Name(), ".")
	panic(FatalError{path.Base(file), line, f[len(f)-1], fmt.Sprintf(format, a...)})
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
		}
		// Inability to log is a fatal error. We do not run blind.
		l.skipEmit = true // Message already emitted, just panic
		l.LogFatal("Unable to reopen logfile '%v'. Error: '%v'", l.logFilename, err)
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

	if l.logLevel == Unclassified {
		l.logger.Printf("["+l.logFacility+"] "+format, a...)
	} else {
		l.logger.Printf("["+l.logFacility+"] ["+severityToText(sev)+"] "+format, a...)
	}
}
