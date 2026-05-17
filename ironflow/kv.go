package ironflow

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// KVClient provides access to the Ironflow KV API.
type KVClient struct {
	client *Client
}

// KV returns a KVClient for interacting with the KV store.
func (c *Client) KV() *KVClient {
	return &KVClient{client: c}
}

// BucketConfig configures a new KV bucket.
type BucketConfig struct {
	// Name is the bucket name.
	Name string `json:"name"`
	// Description is an optional description.
	Description string `json:"description,omitempty"`
	// TTL is the time-to-live for keys. Zero means no expiry.
	TTL time.Duration `json:"-"`
	// MaxValueSize is the maximum value size in bytes.
	MaxValueSize int32 `json:"max_value_size,omitempty"`
	// MaxBytes is the maximum total size of the bucket.
	MaxBytes int64 `json:"max_bytes,omitempty"`
	// History is the number of historical values to keep per key.
	History uint8 `json:"history,omitempty"`
}

// BucketInfo describes an existing KV bucket.
type BucketInfo struct {
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	TTLSeconds  int64     `json:"ttl_seconds,omitempty"`
	Values      uint64    `json:"values"`
	Bytes       uint64    `json:"bytes"`
	History     int64     `json:"history"`
	CreatedAt   time.Time `json:"created_at"`
}

// KVEntry represents a key-value entry.
type KVEntry struct {
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	Revision  uint64 `json:"revision"`
	CreatedAt string `json:"created_at"`
	Operation string `json:"operation"`
}

// KVBucket provides operations on a specific KV bucket.
type KVBucket struct {
	name   string
	client *Client
}

// CreateBucket creates a new KV bucket.
func (kv *KVClient) CreateBucket(ctx context.Context, cfg BucketConfig) (*BucketInfo, error) {
	body := map[string]any{
		"name": cfg.Name,
	}
	if cfg.Description != "" {
		body["description"] = cfg.Description
	}
	if cfg.TTL > 0 {
		body["ttl_seconds"] = int64(cfg.TTL.Seconds())
	}
	if cfg.MaxValueSize > 0 {
		body["max_value_size"] = cfg.MaxValueSize
	}
	if cfg.MaxBytes > 0 {
		body["max_bytes"] = cfg.MaxBytes
	}
	if cfg.History > 0 {
		body["history"] = cfg.History
	}

	var info BucketInfo
	if err := kv.client.restRequest(ctx, "POST", "/api/v1/kv/buckets", body, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// DeleteBucket deletes a KV bucket.
func (kv *KVClient) DeleteBucket(ctx context.Context, name string) error {
	return kv.client.restRequest(ctx, "DELETE", "/api/v1/kv/buckets/"+url.PathEscape(name), nil, nil)
}

// ListBuckets returns all KV buckets.
func (kv *KVClient) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	var result struct {
		Buckets []BucketInfo `json:"buckets"`
	}
	if err := kv.client.restRequest(ctx, "GET", "/api/v1/kv/buckets", nil, &result); err != nil {
		return nil, err
	}
	return result.Buckets, nil
}

// GetBucketInfo returns information about a specific bucket.
func (kv *KVClient) GetBucketInfo(ctx context.Context, name string) (*BucketInfo, error) {
	var info BucketInfo
	if err := kv.client.restRequest(ctx, "GET", "/api/v1/kv/buckets/"+url.PathEscape(name), nil, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// Bucket returns a KVBucket for operations on a specific bucket.
func (kv *KVClient) Bucket(name string) *KVBucket {
	return &KVBucket{name: name, client: kv.client}
}

// Get retrieves a value by key.
func (b *KVBucket) Get(ctx context.Context, key string) (*KVEntry, error) {
	var entry KVEntry
	path := fmt.Sprintf("/api/v1/kv/buckets/%s/keys/%s", url.PathEscape(b.name), url.PathEscape(key))
	if err := b.client.restRequest(ctx, "GET", path, nil, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// Put stores a value unconditionally and returns the new revision.
func (b *KVBucket) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	return b.putWithHeaders(ctx, key, value, nil)
}

// Create stores a value only if the key does not already exist (if-not-exists).
// Returns ErrKeyExists (HTTP 412) if the key already exists.
func (b *KVBucket) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	headers := map[string]string{"If-None-Match": "*"}
	return b.putWithHeaders(ctx, key, value, headers)
}

// Update stores a value only if the revision matches (compare-and-swap).
// Returns an error (HTTP 412) if the revision doesn't match.
func (b *KVBucket) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	headers := map[string]string{"If-Match": strconv.FormatUint(revision, 10)}
	return b.putWithHeaders(ctx, key, value, headers)
}

// Delete soft-deletes a key (tombstone).
func (b *KVBucket) Delete(ctx context.Context, key string) error {
	path := fmt.Sprintf("/api/v1/kv/buckets/%s/keys/%s", url.PathEscape(b.name), url.PathEscape(key))
	return b.client.restRequest(ctx, "DELETE", path, nil, nil)
}

// Purge permanently removes a key and all its history.
func (b *KVBucket) Purge(ctx context.Context, key string) error {
	path := fmt.Sprintf("/api/v1/kv/buckets/%s/keys/%s?purge=true", url.PathEscape(b.name), url.PathEscape(key))
	return b.client.restRequest(ctx, "DELETE", path, nil, nil)
}

// ListKeys returns keys, optionally filtered by a wildcard pattern.
func (b *KVBucket) ListKeys(ctx context.Context, filter string) ([]string, error) {
	path := fmt.Sprintf("/api/v1/kv/buckets/%s/keys", url.PathEscape(b.name))
	if filter != "" {
		path += "?filter=" + url.QueryEscape(filter)
	}
	var result struct {
		Keys []string `json:"keys"`
	}
	if err := b.client.restRequest(ctx, "GET", path, nil, &result); err != nil {
		return nil, err
	}
	return result.Keys, nil
}

func (b *KVBucket) putWithHeaders(ctx context.Context, key string, value []byte, headers map[string]string) (uint64, error) {
	path := fmt.Sprintf("/api/v1/kv/buckets/%s/keys/%s", url.PathEscape(b.name), url.PathEscape(key))
	reqURL := b.client.serverURL + path

	req, err := http.NewRequestWithContext(ctx, "PUT", reqURL, bytes.NewReader(value))
	if err != nil {
		return 0, WrapError(err, "failed to create request", "REQUEST_ERROR", true)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	if b.client.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+b.client.apiKey)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := b.client.httpClient.Do(req)
	if err != nil {
		return 0, WrapError(err, "request failed", "REQUEST_FAILED", true)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		var errResp struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		return 0, NewError(errResp.Error, fmt.Sprintf("HTTP_%d", resp.StatusCode), resp.StatusCode >= 500)
	}

	var result struct {
		Revision uint64 `json:"revision"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, WrapError(err, "failed to decode response", "DECODE_ERROR", false)
	}

	return result.Revision, nil
}

// ============================================================================
// KV Watch
// ============================================================================

// KVWatchEvent represents a change notification from KV watch.
type KVWatchEvent struct {
	// Type is the event type: "kv_update" (put or delete operations).
	Type string `json:"type"`
	// Key is the key that changed.
	Key string `json:"key"`
	// Value is the new value (nil on delete).
	Value []byte `json:"value,omitempty"`
	// Revision is the new revision number.
	Revision uint64 `json:"revision"`
	// Operation is the change operation: "put" or "delete".
	Operation string `json:"operation"`
	// Bucket is the bucket name.
	Bucket string `json:"bucket"`
}

// KVWatchCallbacks are called when KV changes occur.
type KVWatchCallbacks struct {
	// OnUpdate is called when a key is created, updated, or deleted.
	OnUpdate func(event KVWatchEvent)
	// OnError is called when an error occurs. The watch stops after an error.
	OnError func(err error)
	// OnClose is called when the watch connection is closed normally.
	OnClose func()
}

// KVWatchOption configures a watch.
type KVWatchOption func(*kvWatchOptions)

type kvWatchOptions struct {
	key string // optional: watch a specific key only
}

// WithWatchKey watches a specific key instead of the whole bucket.
func WithWatchKey(key string) KVWatchOption {
	return func(o *kvWatchOptions) { o.key = key }
}

// KVWatcher controls an active watch connection.
type KVWatcher struct {
	stopCh chan struct{}
	doneCh chan struct{}
}

// Stop closes the watch connection and waits for the goroutine to finish.
func (w *KVWatcher) Stop() {
	select {
	case <-w.stopCh:
		return // already stopped
	default:
		close(w.stopCh)
	}
	<-w.doneCh // wait for goroutine to finish
}

// Watch connects to the server via WebSocket and calls callbacks when keys change.
//
// The watch runs in a background goroutine. Call KVWatcher.Stop() to end it.
// OnUpdate is called for every key change (put or delete). OnError is called
// on errors (the watch stops). OnClose is called on normal server-side close.
//
// Use WithWatchKey to filter to a specific key:
//
//	watcher, err := bucket.Watch(ctx, ironflow.KVWatchCallbacks{
//	    OnUpdate: func(e ironflow.KVWatchEvent) {
//	        fmt.Printf("key %s changed to %s (rev %d)\n", e.Key, e.Value, e.Revision)
//	    },
//	}, ironflow.WithWatchKey("mykey"))
func (b *KVBucket) Watch(ctx context.Context, callbacks KVWatchCallbacks, opts ...KVWatchOption) (*KVWatcher, error) {
	options := &kvWatchOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Convert HTTP URL to WebSocket URL.
	wsURL := strings.Replace(b.client.serverURL, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)

	path := fmt.Sprintf("/api/v1/kv/buckets/%s/watch", url.PathEscape(b.name))
	if options.key != "" {
		path += "?key=" + url.QueryEscape(options.key)
	}

	// Add auth header.
	header := http.Header{}
	if b.client.apiKey != "" {
		header.Set("Authorization", "Bearer "+b.client.apiKey)
	}

	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, wsURL+path, header)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("kv watch connect: %w", err)
	}

	watcher := &KVWatcher{
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
					callbacks.OnError(fmt.Errorf("kv watch error: %s", message))
				}
				return
			}

			key, _ := msg["key"].(string)
			event := KVWatchEvent{
				Type:      msgType,
				Key:       key,
				Operation: func() string { s, _ := msg["operation"].(string); return s }(),
				Bucket:    func() string { s, _ := msg["bucket"].(string); return s }(),
			}

			// Value is base64-encoded when sent via JSON (Go's json.Marshal encodes []byte as base64).
			if v, ok := msg["value"]; ok && v != nil {
				if s, ok := v.(string); ok {
					decoded, err := base64.StdEncoding.DecodeString(s)
					if err == nil {
						event.Value = decoded
					} else {
						// Fallback: treat as raw string if not valid base64.
						event.Value = []byte(s)
					}
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

// restRequest makes a REST API request with the standard auth/timeout handling.
func (c *Client) restRequest(ctx context.Context, method, path string, body any, result any) error {
	reqURL := c.serverURL + path

	var bodyReader *bytes.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return WrapError(err, "failed to marshal request body", "MARSHAL_ERROR", false)
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	var req *http.Request
	var err error
	if bodyReader != nil {
		req, err = http.NewRequestWithContext(ctx, method, reqURL, bodyReader)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, reqURL, nil)
	}
	if err != nil {
		return WrapError(err, "failed to create request", "REQUEST_ERROR", true)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return WrapError(err, "request failed", "REQUEST_FAILED", true)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		var errResp struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		msg := errResp.Error
		if msg == "" {
			msg = fmt.Sprintf("request failed with status %d", resp.StatusCode)
		}
		return NewError(msg, fmt.Sprintf("HTTP_%d", resp.StatusCode), resp.StatusCode >= 500)
	}

	if result != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return WrapError(err, "failed to decode response", "DECODE_ERROR", false)
		}
	}

	return nil
}
