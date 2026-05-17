package ironflow

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// ConfigResponse represents a config entry with full data.
type ConfigResponse struct {
	Name      string         `json:"name"`
	Data      map[string]any `json:"data"`
	Revision  uint64         `json:"revision"`
	UpdatedAt string         `json:"updatedAt"`
}

// ConfigEntry represents a summary of a config entry (without full data).
type ConfigEntry struct {
	Name      string `json:"name"`
	Revision  uint64 `json:"revision"`
	UpdatedAt string `json:"updatedAt"`
}

// ConfigSetResult is the result of a set or patch operation.
type ConfigSetResult struct {
	Name     string `json:"name"`
	Revision uint64 `json:"revision"`
}

// ConfigClient provides access to the Ironflow Config Management API.
type ConfigClient struct {
	client *Client
}

// Config returns a ConfigClient for interacting with the config management service.
func (c *Client) Config() *ConfigClient {
	return &ConfigClient{client: c}
}

// Set replaces a config entirely (full document replacement).
func (cc *ConfigClient) Set(ctx context.Context, name string, data map[string]any) (*ConfigSetResult, error) {
	var result ConfigSetResult
	if err := cc.client.restRequest(ctx, "POST", "/api/v1/config/"+url.PathEscape(name), data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Get retrieves a config by name.
func (cc *ConfigClient) Get(ctx context.Context, name string) (*ConfigResponse, error) {
	var result ConfigResponse
	if err := cc.client.restRequest(ctx, "GET", "/api/v1/config/"+url.PathEscape(name), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Patch applies a shallow merge to a config.
func (cc *ConfigClient) Patch(ctx context.Context, name string, data map[string]any) (*ConfigSetResult, error) {
	var result ConfigSetResult
	if err := cc.client.restRequest(ctx, "PATCH", "/api/v1/config/"+url.PathEscape(name), data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// List returns all config entries (names and revisions, without full data).
func (cc *ConfigClient) List(ctx context.Context) ([]ConfigEntry, error) {
	var result struct {
		Configs []ConfigEntry `json:"configs"`
	}
	if err := cc.client.restRequest(ctx, "GET", "/api/v1/config", nil, &result); err != nil {
		return nil, err
	}
	return result.Configs, nil
}

// Delete removes a config by name.
func (cc *ConfigClient) Delete(ctx context.Context, name string) error {
	return cc.client.restRequest(ctx, "DELETE", "/api/v1/config/"+url.PathEscape(name), nil, nil)
}

// ============================================================================
// Config Watch
// ============================================================================

// ConfigWatchEvent represents a config change notification.
type ConfigWatchEvent struct {
	// Type is the event type, e.g. "config_update".
	Type string `json:"type"`
	// Name is the name of the config that changed.
	Name string `json:"name"`
	// Data is the new config data.
	Data map[string]any `json:"data"`
	// Revision is the new revision number.
	Revision uint64 `json:"revision"`
	// UpdatedAt is the timestamp of the update.
	UpdatedAt string `json:"updatedAt"`
}

// ConfigWatchCallbacks are called when config changes occur.
type ConfigWatchCallbacks struct {
	// OnUpdate is called when the config is updated.
	OnUpdate func(event ConfigWatchEvent)
	// OnError is called when an error occurs. The watch stops after an error.
	OnError func(err error)
	// OnClose is called when the watch connection is closed normally.
	OnClose func()
}

// ConfigWatcher controls an active config watch connection.
type ConfigWatcher struct {
	stopCh chan struct{}
	doneCh chan struct{}
}

// Stop closes the watch connection and waits for the goroutine to finish.
func (w *ConfigWatcher) Stop() {
	select {
	case <-w.stopCh:
		return // already stopped
	default:
		close(w.stopCh)
	}
	<-w.doneCh // wait for goroutine to finish
}

// Watch connects to the server via WebSocket and calls callbacks when the named
// config changes.
//
// The watch runs in a background goroutine. Call ConfigWatcher.Stop() to end it.
// OnUpdate is called for every config change. OnError is called on errors (the
// watch stops). OnClose is called on normal server-side close.
//
// Example:
//
//	watcher, err := cc.Watch(ctx, "app-settings", ironflow.ConfigWatchCallbacks{
//	    OnUpdate: func(e ironflow.ConfigWatchEvent) {
//	        fmt.Printf("config %s updated to revision %d\n", e.Name, e.Revision)
//	    },
//	})
func (cc *ConfigClient) Watch(ctx context.Context, name string, callbacks ConfigWatchCallbacks) (*ConfigWatcher, error) {
	// Convert HTTP URL to WebSocket URL.
	wsURL := strings.Replace(cc.client.serverURL, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)

	path := "/api/v1/config/" + url.PathEscape(name) + "/watch"

	// Add auth header.
	header := http.Header{}
	if cc.client.apiKey != "" {
		header.Set("Authorization", "Bearer "+cc.client.apiKey)
	}

	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, wsURL+path, header)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("config watch connect: %w", err)
	}

	watcher := &ConfigWatcher{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	go func() {
		defer close(watcher.doneCh)
		defer func() { _ = conn.Close() }()

		for {
			select {
			case <-watcher.stopCh:
				_ = conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			default:
			}

			// Set a short read deadline so we can poll stopCh periodically.
			_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))

			var msg map[string]any
			err := conn.ReadJSON(&msg)
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					if callbacks.OnClose != nil {
						callbacks.OnClose()
					}
					return
				}
				// Timeout means no message yet; check stopCh and retry.
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				// Check if we were asked to stop before reporting the error.
				select {
				case <-watcher.stopCh:
					return
				default:
				}
				if callbacks.OnError != nil {
					callbacks.OnError(err)
				}
				return
			}

			msgType, _ := msg["type"].(string)

			if msgType == "error" {
				if callbacks.OnError != nil {
					message, _ := msg["message"].(string)
					callbacks.OnError(fmt.Errorf("config watch error: %s", message))
				}
				return
			}

			event := ConfigWatchEvent{
				Type:      msgType,
				Name:      func() string { s, _ := msg["name"].(string); return s }(),
				UpdatedAt: func() string { s, _ := msg["updatedAt"].(string); return s }(),
			}

			if d, ok := msg["data"]; ok && d != nil {
				if dataMap, ok := d.(map[string]any); ok {
					event.Data = dataMap
				}
			}

			if r, ok := msg["revision"]; ok {
				if f, ok := r.(float64); ok {
					event.Revision = uint64(f)
				}
			}

			if callbacks.OnUpdate != nil {
				callbacks.OnUpdate(event)
			}
		}
	}()

	return watcher, nil
}
