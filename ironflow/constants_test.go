package ironflow

import (
	"os"
	"testing"
)

func TestGetServerURL(t *testing.T) {
	t.Run("returns default when env not set", func(t *testing.T) {
		os.Unsetenv(EnvServerURL)

		got := GetServerURL()
		if got != DefaultServerURL {
			t.Errorf("GetServerURL() = %q, want %q", got, DefaultServerURL)
		}
	})

	t.Run("returns env var value when set", func(t *testing.T) {
		os.Setenv(EnvServerURL, "http://custom:8080")
		t.Cleanup(func() { os.Unsetenv(EnvServerURL) })

		got := GetServerURL()
		if got != "http://custom:8080" {
			t.Errorf("GetServerURL() = %q, want %q", got, "http://custom:8080")
		}
	})
}

func TestGetWebSocketURL(t *testing.T) {
	t.Run("converts http to ws and appends /ws", func(t *testing.T) {
		got := GetWebSocketURL("http://localhost:9123")
		if got != "ws://localhost:9123/ws" {
			t.Errorf("GetWebSocketURL('http://localhost:9123') = %q, want %q", got, "ws://localhost:9123/ws")
		}
	})

	t.Run("converts https to wss and appends /ws", func(t *testing.T) {
		got := GetWebSocketURL("https://example.com")
		if got != "wss://example.com/ws" {
			t.Errorf("GetWebSocketURL('https://example.com') = %q, want %q", got, "wss://example.com/ws")
		}
	})

	t.Run("does not double append /ws", func(t *testing.T) {
		got := GetWebSocketURL("http://localhost:9123/ws")
		if got != "ws://localhost:9123/ws" {
			t.Errorf("GetWebSocketURL('http://localhost:9123/ws') = %q, want %q", got, "ws://localhost:9123/ws")
		}
	})

	t.Run("strips trailing slash before appending /ws", func(t *testing.T) {
		got := GetWebSocketURL("http://localhost:9123/")
		if got != "ws://localhost:9123/ws" {
			t.Errorf("GetWebSocketURL('http://localhost:9123/') = %q, want %q", got, "ws://localhost:9123/ws")
		}
	})

	t.Run("uses default server URL when empty", func(t *testing.T) {
		os.Unsetenv(EnvServerURL)

		got := GetWebSocketURL("")
		if got != DefaultWebSocketURL {
			t.Errorf("GetWebSocketURL('') = %q, want %q", got, DefaultWebSocketURL)
		}
	})

	t.Run("uses env var server URL when empty and env set", func(t *testing.T) {
		os.Setenv(EnvServerURL, "https://prod.example.com")
		t.Cleanup(func() { os.Unsetenv(EnvServerURL) })

		got := GetWebSocketURL("")
		if got != "wss://prod.example.com/ws" {
			t.Errorf("GetWebSocketURL('') = %q, want %q", got, "wss://prod.example.com/ws")
		}
	})

	t.Run("handles ws:// input without conversion", func(t *testing.T) {
		got := GetWebSocketURL("ws://localhost:9123")
		if got != "ws://localhost:9123/ws" {
			t.Errorf("GetWebSocketURL('ws://localhost:9123') = %q, want %q", got, "ws://localhost:9123/ws")
		}
	})
}

func TestGetAPIKey(t *testing.T) {
	t.Run("returns empty when env not set", func(t *testing.T) {
		os.Unsetenv(EnvAPIKey)

		got := GetAPIKey()
		if got != "" {
			t.Errorf("GetAPIKey() = %q, want empty string", got)
		}
	})

	t.Run("returns env var value when set", func(t *testing.T) {
		os.Setenv(EnvAPIKey, "test-api-key-123")
		t.Cleanup(func() { os.Unsetenv(EnvAPIKey) })

		got := GetAPIKey()
		if got != "test-api-key-123" {
			t.Errorf("GetAPIKey() = %q, want %q", got, "test-api-key-123")
		}
	})
}

func TestGetSigningKey(t *testing.T) {
	t.Run("returns empty when env not set", func(t *testing.T) {
		os.Unsetenv(EnvSigningKey)

		got := GetSigningKey()
		if got != "" {
			t.Errorf("GetSigningKey() = %q, want empty string", got)
		}
	})

	t.Run("returns env var value when set", func(t *testing.T) {
		os.Setenv(EnvSigningKey, "signing-secret-456")
		t.Cleanup(func() { os.Unsetenv(EnvSigningKey) })

		got := GetSigningKey()
		if got != "signing-secret-456" {
			t.Errorf("GetSigningKey() = %q, want %q", got, "signing-secret-456")
		}
	})
}

func TestDefaultConstants(t *testing.T) {
	t.Run("default server URL is well-formed", func(t *testing.T) {
		if DefaultServerURL != "http://localhost:9123" {
			t.Errorf("DefaultServerURL = %q, want %q", DefaultServerURL, "http://localhost:9123")
		}
	})

	t.Run("default websocket URL is well-formed", func(t *testing.T) {
		if DefaultWebSocketURL != "ws://localhost:9123/ws" {
			t.Errorf("DefaultWebSocketURL = %q, want %q", DefaultWebSocketURL, "ws://localhost:9123/ws")
		}
	})

	t.Run("default port is 9123", func(t *testing.T) {
		if DefaultPort != 9123 {
			t.Errorf("DefaultPort = %d, want %d", DefaultPort, 9123)
		}
	})

	t.Run("default host is localhost", func(t *testing.T) {
		if DefaultHost != "localhost" {
			t.Errorf("DefaultHost = %q, want %q", DefaultHost, "localhost")
		}
	})
}
