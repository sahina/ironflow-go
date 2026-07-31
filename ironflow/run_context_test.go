package ironflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithRunID_RoundTrip(t *testing.T) {
	if got := runIDFromContext(context.Background()); got != "" {
		t.Errorf("no run id: got %q, want empty", got)
	}
	ctx := WithRunID(context.Background(), "run-1")
	if got := runIDFromContext(ctx); got != "run-1" {
		t.Errorf("round-trip: got %q, want run-1", got)
	}
	// Empty run id leaves the context untouched.
	if got := runIDFromContext(WithRunID(context.Background(), "")); got != "" {
		t.Errorf("empty run id: got %q, want empty", got)
	}
}

func TestContext_RunContext(t *testing.T) {
	c := Context{Run: RunInfo{ID: "run-9"}}
	if got := runIDFromContext(c.RunContext()); got != "run-9" {
		t.Errorf("RunContext: got %q, want run-9", got)
	}
}

// TestEmit_SendsRunIDHeader proves the SDK forwards X-Ironflow-Run-ID when the
// context carries a run id (the Go side of #1262 emit-edge learning), and omits
// it otherwise.
func TestEmit_SendsRunIDHeader(t *testing.T) {
	var gotHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get(HeaderRunID)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"runIds":[],"eventId":"evt-1"}`))
	}))
	defer server.Close()

	client := NewClient(ClientConfig{
		ServerURL: server.URL,
		Retry:     &ClientRetryConfig{MaxAttempts: 0},
		Logger:    NewNoopLogger(),
	})

	// With a run-scoped context → header present.
	if _, err := client.Emit(WithRunID(context.Background(), "run-42"), "order.shipped", map[string]any{}); err != nil {
		t.Fatalf("Emit (with run id): %v", err)
	}
	if gotHeader != "run-42" {
		t.Errorf("run-id header: got %q, want run-42", gotHeader)
	}

	// Plain context (API/cron caller) → no header.
	gotHeader = "sentinel"
	if _, err := client.Emit(context.Background(), "order.placed", map[string]any{}); err != nil {
		t.Fatalf("Emit (no run id): %v", err)
	}
	if gotHeader != "" {
		t.Errorf("run-id header should be absent, got %q", gotHeader)
	}
}
