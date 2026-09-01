package ironflow

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// #1955. WithSyncVersion is new public API and TriggerSync only gained a
// version field in that change, so neither had coverage. It shares SyncOption
// with InvokeSync, where it is deliberately a no-op (invoking a function
// generates no event, so there is no schema to select).
func TestEmitSync_WithSyncVersion(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()

	client := &Client{
		serverURL:   server.URL,
		httpClient:  &http.Client{},
		retryConfig: &ClientRetryConfig{MaxAttempts: 1},
		logger:      NewNoopLogger(),
	}

	if _, err := client.EmitSync(context.Background(), "order.placed",
		map[string]any{"id": "1"}, time.Second, WithSyncVersion(2)); err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if got, ok := body["version"]; !ok {
		t.Fatal("version absent from the TriggerSync body")
	} else if got != float64(2) {
		t.Fatalf("version = %v, want 2", got)
	}
}

// Omitting it must keep the wire byte-identical to a pre-#1955 client, since
// the server reads an absent value as 1.
func TestEmitSync_OmitsVersionWhenUnset(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"results":[]}`))
	}))
	defer server.Close()

	client := &Client{
		serverURL:   server.URL,
		httpClient:  &http.Client{},
		retryConfig: &ClientRetryConfig{MaxAttempts: 1},
		logger:      NewNoopLogger(),
	}

	if _, err := client.EmitSync(context.Background(), "order.placed",
		map[string]any{"id": "1"}, time.Second); err != nil {
		t.Fatalf("EmitSync: %v", err)
	}
	if _, ok := body["version"]; ok {
		t.Fatalf("version present on the wire when unset: %v", body["version"])
	}
}

// The guard moved from `> 0` to `!= 0` so a negative reaches the server and
// comes back as a 400 with the reason. Under the old guard this drops silently
// and the caller emits at version 1 believing they set -1.
func TestEmit_ForwardsNegativeVersion(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"runIds":[],"eventId":"evt-1"}`))
	}))
	defer server.Close()

	client := &Client{
		serverURL:   server.URL,
		httpClient:  &http.Client{},
		retryConfig: &ClientRetryConfig{MaxAttempts: 1},
		logger:      NewNoopLogger(),
	}

	if _, err := client.Emit(context.Background(), "order.placed",
		map[string]any{"id": "1"}, WithEmitVersion(-1)); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if got := body["version"]; got != float64(-1) {
		t.Fatalf("version = %v, want -1 (client must not swallow it)", got)
	}
}
