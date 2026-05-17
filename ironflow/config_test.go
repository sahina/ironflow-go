package ironflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// setupMockConfigWatchServer creates a test server that upgrades to WebSocket
// at /api/v1/config/{name}/watch and calls handleFn with the connection.
// Returns the configured Client and a cleanup function.
func setupMockConfigWatchServer(t *testing.T, configName string, handleFn func(conn *websocket.Conn)) (*Client, func()) {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/config/"+configName+"/watch", func(w http.ResponseWriter, r *http.Request) {
		conn, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("websocket upgrade failed: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		handleFn(conn)
	})

	server := httptest.NewServer(mux)
	client := &Client{
		serverURL:  server.URL,
		httpClient: server.Client(),
		retryConfig: &ClientRetryConfig{
			MaxAttempts: 1,
		},
		logger: NewNoopLogger(),
	}
	return client, server.Close
}

// TestConfigWatch_BasicUpdate verifies that a config_update event is delivered
// to the OnUpdate callback with all fields correctly parsed.
func TestConfigWatch_BasicUpdate(t *testing.T) {
	const configName = "app-settings"

	client, cleanup := setupMockConfigWatchServer(t, configName, func(conn *websocket.Conn) {
		// Send one config_update message then close.
		msg := map[string]any{
			"type":      "config_update",
			"name":      configName,
			"data":      map[string]any{"key": "value", "count": float64(42)},
			"revision":  float64(5),
			"updatedAt": "2026-03-28T00:00:00Z",
		}
		if err := conn.WriteJSON(msg); err != nil {
			t.Errorf("server write error: %v", err)
		}
		// Wait a bit so the client can read the message before we close.
		time.Sleep(200 * time.Millisecond)
		_ = conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	})
	defer cleanup()

	received := make(chan ConfigWatchEvent, 1)
	closed := make(chan struct{})

	ctx := context.Background()
	watcher, err := client.Config().Watch(ctx, configName, ConfigWatchCallbacks{
		OnUpdate: func(e ConfigWatchEvent) {
			received <- e
		},
		OnClose: func() {
			close(closed)
		},
	})
	if err != nil {
		t.Fatalf("Watch returned error: %v", err)
	}
	defer watcher.Stop()

	select {
	case event := <-received:
		if event.Type != "config_update" {
			t.Errorf("expected type config_update, got %q", event.Type)
		}
		if event.Name != configName {
			t.Errorf("expected name %q, got %q", configName, event.Name)
		}
		if event.Revision != 5 {
			t.Errorf("expected revision 5, got %d", event.Revision)
		}
		if event.UpdatedAt != "2026-03-28T00:00:00Z" {
			t.Errorf("expected updatedAt 2026-03-28T00:00:00Z, got %q", event.UpdatedAt)
		}
		if event.Data == nil {
			t.Fatal("expected non-nil data")
		}
		if event.Data["key"] != "value" {
			t.Errorf("expected data[key]=value, got %v", event.Data["key"])
		}
		if event.Data["count"] != float64(42) {
			t.Errorf("expected data[count]=42, got %v", event.Data["count"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}

	// Wait for OnClose to be called.
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnClose")
	}
}

// TestConfigWatch_Stop verifies that calling Stop() shuts down the goroutine
// cleanly and the server observes the client disconnect.
func TestConfigWatch_Stop(t *testing.T) {
	const configName = "stop-config"

	// Server stays open until the client closes.
	serverGotClose := make(chan struct{})
	client, cleanup := setupMockConfigWatchServer(t, configName, func(conn *websocket.Conn) {
		defer close(serverGotClose)
		// Drain messages until client disconnects.
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	})
	defer cleanup()

	ctx := context.Background()
	watcher, err := client.Config().Watch(ctx, configName, ConfigWatchCallbacks{})
	if err != nil {
		t.Fatalf("Watch returned error: %v", err)
	}

	// Stop should return promptly.
	done := make(chan struct{})
	go func() {
		watcher.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Good — Stop returned.
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() did not return within 3s")
	}

	// Server should have seen the close.
	select {
	case <-serverGotClose:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not observe client close within 3s")
	}
}

// TestConfigWatch_ErrorMessage verifies that an error message from the server
// triggers the OnError callback with the message text included.
func TestConfigWatch_ErrorMessage(t *testing.T) {
	const configName = "err-config"

	client, cleanup := setupMockConfigWatchServer(t, configName, func(conn *websocket.Conn) {
		_ = conn.WriteJSON(map[string]string{
			"type":    "error",
			"message": "config not found",
		})
		time.Sleep(200 * time.Millisecond)
	})
	defer cleanup()

	errCh := make(chan error, 1)

	ctx := context.Background()
	watcher, err := client.Config().Watch(ctx, configName, ConfigWatchCallbacks{
		OnError: func(e error) {
			errCh <- e
		},
	})
	if err != nil {
		t.Fatalf("Watch returned error: %v", err)
	}
	defer watcher.Stop()

	select {
	case watchErr := <-errCh:
		if !strings.Contains(watchErr.Error(), "config not found") {
			t.Errorf("expected 'config not found' in error, got: %v", watchErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnError")
	}
}

// TestConfigWatch_StopIdempotent verifies calling Stop() twice does not panic.
func TestConfigWatch_StopIdempotent(t *testing.T) {
	const configName = "idempotent-config"

	client, cleanup := setupMockConfigWatchServer(t, configName, func(conn *websocket.Conn) {
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	})
	defer cleanup()

	ctx := context.Background()
	watcher, err := client.Config().Watch(ctx, configName, ConfigWatchCallbacks{})
	if err != nil {
		t.Fatalf("Watch returned error: %v", err)
	}

	watcher.Stop()
	// Second Stop must not panic.
	watcher.Stop()
}
