// Package ironflow provides the Go SDK for Ironflow, an event-driven backend platform.
package ironflow

import (
	"os"
	"strings"
	"time"
)

// Default configuration constants
const (
	// DefaultPort is the default HTTP server port for Ironflow.
	DefaultPort = 9123

	// DefaultHost is the default host for Ironflow.
	DefaultHost = "localhost"

	// DefaultServerURL is the default server URL.
	DefaultServerURL = "http://localhost:9123"

	// DefaultWebSocketURL is the default WebSocket URL.
	DefaultWebSocketURL = "ws://localhost:9123/ws"
)

// EventSourceType represents the origin of an event.
type EventSourceType string

// Event source constants
const (
	// EventSourceAPI is the source identifier for events triggered via the REST API.
	EventSourceAPI EventSourceType = "api"
	// EventSourceCron is the source identifier for events triggered by the cron scheduler.
	EventSourceCron EventSourceType = "cron"
	// EventSourceWebhook is the source identifier for events triggered via webhooks.
	EventSourceWebhook EventSourceType = "webhook"
)

// Environment variable names
const (
	// EnvServerURL is the environment variable for server URL.
	EnvServerURL = "IRONFLOW_SERVER_URL"

	// EnvSigningKey is the environment variable for webhook signing key.
	EnvSigningKey = "IRONFLOW_SIGNING_KEY"

	// EnvAPIKey is the environment variable for API key.
	EnvAPIKey = "IRONFLOW_API_KEY" //nolint:gosec // not a credential, just env var name

	// EnvLogLevel is the environment variable for log level (debug, info, warn, error, silent).
	EnvLogLevel = "IRONFLOW_LOG_LEVEL"
)

// Default timeout values
const (
	// DefaultClientTimeout is the default client HTTP timeout.
	DefaultClientTimeout = 30 * time.Second

	// DefaultFunctionTimeout is the default function timeout.
	DefaultFunctionTimeout = 10 * time.Minute

	// DefaultEmitSyncTimeout is the default EmitSync timeout.
	DefaultEmitSyncTimeout = 30 * time.Second
)

// Default retry configuration for function steps
const (
	// DefaultRetryMaxAttempts is the default maximum retry attempts.
	DefaultRetryMaxAttempts = 3

	// DefaultRetryInitialDelay is the default initial retry delay.
	DefaultRetryInitialDelay = 1 * time.Second

	// DefaultRetryBackoffFactor is the default retry backoff factor.
	DefaultRetryBackoffFactor = 2.0

	// DefaultRetryMaxDelay is the default maximum retry delay.
	DefaultRetryMaxDelay = 5 * time.Minute
)

// Default client retry configuration for HTTP requests
const (
	// DefaultClientRetryMaxAttempts is the default max retry attempts for client requests.
	DefaultClientRetryMaxAttempts = 3

	// DefaultClientRetryInitialDelay is the default initial delay for client retries.
	DefaultClientRetryInitialDelay = 100 * time.Millisecond

	// DefaultClientRetryMaxDelay is the default max delay for client retries.
	DefaultClientRetryMaxDelay = 10 * time.Second

	// DefaultClientRetryBackoffMultiplier is the default backoff multiplier for client retries.
	DefaultClientRetryBackoffMultiplier = 2.0

	// DefaultClientRetryConnectionDelay is the fixed delay for connection errors.
	DefaultClientRetryConnectionDelay = 2 * time.Second
)

// Default worker configuration
const (
	// DefaultWorkerMaxConcurrentJobs is the default maximum concurrent jobs.
	DefaultWorkerMaxConcurrentJobs = 10

	// DefaultWorkerHeartbeatInterval is the default heartbeat interval.
	DefaultWorkerHeartbeatInterval = 30 * time.Second

	// DefaultWorkerReconnectDelay is the default reconnect delay.
	DefaultWorkerReconnectDelay = 5 * time.Second
)

// GetServerURL returns the server URL from environment variable or the default.
func GetServerURL() string {
	if url := os.Getenv(EnvServerURL); url != "" {
		return url
	}
	return DefaultServerURL
}

// GetWebSocketURL returns the WebSocket URL from the server URL.
// It converts http(s) URLs to ws(s) URLs.
func GetWebSocketURL(serverURL string) string {
	if serverURL == "" {
		serverURL = GetServerURL()
	}

	// Convert HTTP to WebSocket protocol
	wsURL := serverURL
	if after, ok := strings.CutPrefix(wsURL, "https://"); ok {
		wsURL = "wss://" + after
	} else if after0, ok0 := strings.CutPrefix(wsURL, "http://"); ok0 {
		wsURL = "ws://" + after0
	}

	// Add /ws path if not present
	if !strings.HasSuffix(wsURL, "/ws") {
		wsURL = strings.TrimSuffix(wsURL, "/") + "/ws"
	}

	return wsURL
}

// GetSigningKey returns the signing key from environment variable.
func GetSigningKey() string {
	return os.Getenv(EnvSigningKey)
}

// GetAPIKey returns the API key from environment variable.
func GetAPIKey() string {
	return os.Getenv(EnvAPIKey)
}
