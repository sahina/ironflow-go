package ironflow

import (
	"fmt"
	"os"
	"strings"
)

// Logger is the interface for logging in Ironflow SDK.
// Implement this interface to use your own logger (e.g., slog, zap, zerolog).
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// LogLevel represents the log level.
type LogLevel int

const (
	// LogLevelDebug logs all messages.
	LogLevelDebug LogLevel = iota
	// LogLevelInfo logs info, warn, and error messages.
	LogLevelInfo
	// LogLevelWarn logs warn and error messages.
	LogLevelWarn
	// LogLevelError logs only error messages.
	LogLevelError
	// LogLevelSilent disables all logging.
	LogLevelSilent
)

// ParseLogLevel parses a string log level.
func ParseLogLevel(s string) LogLevel {
	switch strings.ToLower(s) {
	case "debug":
		return LogLevelDebug
	case "info":
		return LogLevelInfo
	case "warn", "warning":
		return LogLevelWarn
	case "error":
		return LogLevelError
	case "silent":
		return LogLevelSilent
	default:
		return LogLevelInfo
	}
}

// GetLogLevel returns the log level from environment variable or the default (info).
func GetLogLevel() LogLevel {
	if levelStr := os.Getenv(EnvLogLevel); levelStr != "" {
		return ParseLogLevel(levelStr)
	}
	return LogLevelInfo
}

// LoggerConfig configures the default logger.
type LoggerConfig struct {
	// Level is the minimum log level (default: from IRONFLOW_LOG_LEVEL env var or info).
	Level LogLevel
	// Prefix is the prefix for log messages (default: "[ironflow]").
	Prefix string
}

// defaultLogger is the default console logger implementation.
type defaultLogger struct {
	level  LogLevel
	prefix string
}

// NewLogger creates a new default logger.
//
// Example:
//
//	logger := ironflow.NewLogger(ironflow.LoggerConfig{
//	    Level:  ironflow.LogLevelDebug,
//	    Prefix: "[my-app]",
//	})
func NewLogger(config LoggerConfig) Logger {
	level := config.Level
	if level == 0 {
		level = GetLogLevel()
	}

	prefix := config.Prefix
	if prefix == "" {
		prefix = "[ironflow]"
	}

	if level == LogLevelSilent {
		return NewNoopLogger()
	}

	return &defaultLogger{
		level:  level,
		prefix: prefix,
	}
}

func (l *defaultLogger) Debug(msg string, args ...any) {
	if l.level <= LogLevelDebug {
		l.log("DEBUG", msg, args...)
	}
}

func (l *defaultLogger) Info(msg string, args ...any) {
	if l.level <= LogLevelInfo {
		l.log("INFO", msg, args...)
	}
}

func (l *defaultLogger) Warn(msg string, args ...any) {
	if l.level <= LogLevelWarn {
		l.log("WARN", msg, args...)
	}
}

func (l *defaultLogger) Error(msg string, args ...any) {
	if l.level <= LogLevelError {
		l.log("ERROR", msg, args...)
	}
}

func (l *defaultLogger) log(level, msg string, args ...any) {
	if len(args) > 0 {
		// Format key-value pairs
		pairs := make([]string, 0, len(args)/2)
		for i := 0; i < len(args)-1; i += 2 {
			pairs = append(pairs, fmt.Sprintf("%v=%v", args[i], args[i+1]))
		}
		if len(pairs) > 0 {
			fmt.Printf("%s [%s] %s %s\n", l.prefix, level, msg, strings.Join(pairs, " "))
		} else {
			fmt.Printf("%s [%s] %s\n", l.prefix, level, msg)
		}
	} else {
		fmt.Printf("%s [%s] %s\n", l.prefix, level, msg)
	}
}

// noopLogger is a logger that discards all messages.
type noopLogger struct{}

// NewNoopLogger creates a logger that discards all messages.
//
// Example:
//
//	// Disable all logging
//	client := ironflow.NewClient(ironflow.ClientConfig{
//	    ServerURL: "http://localhost:9123",
//	    Logger:    ironflow.NewNoopLogger(),
//	})
func NewNoopLogger() Logger {
	return &noopLogger{}
}

func (l *noopLogger) Debug(msg string, args ...any) {}
func (l *noopLogger) Info(msg string, args ...any)  {}
func (l *noopLogger) Warn(msg string, args ...any)  {}
func (l *noopLogger) Error(msg string, args ...any) {}
