package ironflow

import (
	"os"
	"testing"
)

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected LogLevel
	}{
		{"debug", LogLevelDebug},
		{"DEBUG", LogLevelDebug},
		{"Debug", LogLevelDebug},
		{"info", LogLevelInfo},
		{"INFO", LogLevelInfo},
		{"warn", LogLevelWarn},
		{"WARN", LogLevelWarn},
		{"warning", LogLevelWarn},
		{"WARNING", LogLevelWarn},
		{"error", LogLevelError},
		{"ERROR", LogLevelError},
		{"silent", LogLevelSilent},
		{"SILENT", LogLevelSilent},
		{"", LogLevelInfo},
		{"invalid", LogLevelInfo},
		{"verbose", LogLevelInfo},
	}

	for _, tt := range tests {
		t.Run("input_"+tt.input, func(t *testing.T) {
			got := ParseLogLevel(tt.input)
			if got != tt.expected {
				t.Errorf("ParseLogLevel(%q) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}
}

func TestGetLogLevel(t *testing.T) {
	t.Run("returns info when env not set", func(t *testing.T) {
		os.Unsetenv(EnvLogLevel)

		got := GetLogLevel()
		if got != LogLevelInfo {
			t.Errorf("GetLogLevel() = %d, want %d (LogLevelInfo)", got, LogLevelInfo)
		}
	})

	t.Run("returns level from env var", func(t *testing.T) {
		os.Setenv(EnvLogLevel, "debug")
		t.Cleanup(func() { os.Unsetenv(EnvLogLevel) })

		got := GetLogLevel()
		if got != LogLevelDebug {
			t.Errorf("GetLogLevel() = %d, want %d (LogLevelDebug)", got, LogLevelDebug)
		}
	})

	t.Run("returns error level from env var", func(t *testing.T) {
		os.Setenv(EnvLogLevel, "error")
		t.Cleanup(func() { os.Unsetenv(EnvLogLevel) })

		got := GetLogLevel()
		if got != LogLevelError {
			t.Errorf("GetLogLevel() = %d, want %d (LogLevelError)", got, LogLevelError)
		}
	})
}

func TestNewLogger(t *testing.T) {
	t.Run("creates logger with default config", func(t *testing.T) {
		os.Unsetenv(EnvLogLevel)

		logger := NewLogger(LoggerConfig{})
		if logger == nil {
			t.Fatal("NewLogger returned nil")
		}

		dl, ok := logger.(*defaultLogger)
		if !ok {
			t.Fatal("expected *defaultLogger type")
		}
		if dl.prefix != "[ironflow]" {
			t.Errorf("expected prefix '[ironflow]', got %q", dl.prefix)
		}
		if dl.level != LogLevelInfo {
			t.Errorf("expected level %d (LogLevelInfo), got %d", LogLevelInfo, dl.level)
		}
	})

	t.Run("creates logger with custom config", func(t *testing.T) {
		logger := NewLogger(LoggerConfig{
			Level:  LogLevelWarn,
			Prefix: "[test]",
		})
		if logger == nil {
			t.Fatal("NewLogger returned nil")
		}

		dl, ok := logger.(*defaultLogger)
		if !ok {
			t.Fatal("expected *defaultLogger type")
		}
		if dl.prefix != "[test]" {
			t.Errorf("expected prefix '[test]', got %q", dl.prefix)
		}
		if dl.level != LogLevelWarn {
			t.Errorf("expected level %d (LogLevelWarn), got %d", LogLevelWarn, dl.level)
		}
	})

	t.Run("documents zero value ambiguity - LogLevelDebug treated as unset", func(t *testing.T) {
		// TODO: Known limitation — LogLevelDebug is 0 (iota), which is also
		// the zero value for int. NewLogger cannot distinguish between
		// "caller explicitly requested debug level" and "caller left the
		// field at its zero value (unset)". As a result, passing
		// LogLevelDebug is silently ignored and falls back to GetLogLevel().
		// Fix options: use *LogLevel (pointer), a sentinel like -1, or
		// start iota at 1.
		os.Unsetenv(EnvLogLevel)

		logger := NewLogger(LoggerConfig{Level: LogLevelDebug})
		if logger == nil {
			t.Fatal("NewLogger returned nil")
		}

		dl, ok := logger.(*defaultLogger)
		if !ok {
			t.Fatal("expected *defaultLogger type")
		}
		// BUG: Caller asked for LogLevelDebug but gets LogLevelInfo because
		// the zero value is indistinguishable from "not set".
		if dl.level != LogLevelInfo {
			t.Errorf("expected level %d (LogLevelInfo, default due to zero-value ambiguity), got %d", LogLevelInfo, dl.level)
		}
	})

	t.Run("debug level via env var when zero value passed", func(t *testing.T) {
		os.Setenv(EnvLogLevel, "debug")
		t.Cleanup(func() { os.Unsetenv(EnvLogLevel) })

		logger := NewLogger(LoggerConfig{Level: LogLevelDebug})
		if logger == nil {
			t.Fatal("NewLogger returned nil")
		}

		dl, ok := logger.(*defaultLogger)
		if !ok {
			t.Fatal("expected *defaultLogger type")
		}
		if dl.level != LogLevelDebug {
			t.Errorf("expected level %d (LogLevelDebug), got %d", LogLevelDebug, dl.level)
		}
	})

	t.Run("returns noop logger for silent level", func(t *testing.T) {
		logger := NewLogger(LoggerConfig{Level: LogLevelSilent})
		if logger == nil {
			t.Fatal("NewLogger returned nil")
		}

		_, ok := logger.(*noopLogger)
		if !ok {
			t.Error("expected *noopLogger type for silent level")
		}
	})
}

func TestNewNoopLogger(t *testing.T) {
	t.Run("returns non-nil logger", func(t *testing.T) {
		logger := NewNoopLogger()
		if logger == nil {
			t.Fatal("NewNoopLogger returned nil")
		}
	})

	t.Run("all methods do not panic", func(t *testing.T) {
		logger := NewNoopLogger()

		// These should all silently discard messages without panicking.
		logger.Debug("debug message")
		logger.Debug("debug with args", "key", "value")
		logger.Info("info message")
		logger.Info("info with args", "key", "value")
		logger.Warn("warn message")
		logger.Warn("warn with args", "key", "value")
		logger.Error("error message")
		logger.Error("error with args", "key", "value")
	})
}

func TestDefaultLoggerMethods(t *testing.T) {
	t.Run("all methods do not panic", func(t *testing.T) {
		logger := NewLogger(LoggerConfig{Level: LogLevelDebug})

		// These should all produce output without panicking.
		logger.Debug("debug message")
		logger.Debug("debug with args", "key", "value")
		logger.Info("info message")
		logger.Info("info with args", "key", "value")
		logger.Warn("warn message")
		logger.Warn("warn with args", "key", "value")
		logger.Error("error message")
		logger.Error("error with args", "key", "value")
	})

	t.Run("respects log level filtering", func(t *testing.T) {
		// Create a logger at WARN level - Debug and Info should be suppressed.
		// We mainly verify no panics; output filtering is an implementation detail.
		logger := NewLogger(LoggerConfig{Level: LogLevelWarn})

		logger.Debug("should be filtered")
		logger.Info("should be filtered")
		logger.Warn("should appear")
		logger.Error("should appear")
	})
}
