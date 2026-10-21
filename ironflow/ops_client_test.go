package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOpsHelpersPaths(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Method+" "+r.URL.RequestURI())
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = w.Write([]byte(`[{"queue_seq":2,"run_id":"r","lane_id":null,"reservation_owner":null,"first_enqueued_at":"2026-10-16T19:08:16Z"}]`))
	}))
	defer srv.Close()
	c := NewClient(ClientConfig{ServerURL: srv.URL, APIKey: "ifplatform_test"})
	ctx := context.Background()

	queue, err := c.ListCapacityQueue(ctx, CapacityListOptions{Function: "fn a", Limit: 10})
	if err != nil {
		t.Fatalf("ListCapacityQueue: %v", err)
	}
	if len(queue) != 1 || queue[0].RunID != "r" || queue[0].LaneID != nil || queue[0].FirstEnqueuedAt.IsZero() {
		t.Fatalf("queue = %+v", queue)
	}
	if _, err := c.ListCapacityLanes(ctx, CapacityListOptions{}); err != nil {
		t.Fatalf("ListCapacityLanes: %v", err)
	}
	// "user123" is valid base64url by accident: sent raw it would cancel nothing (#2948).
	for _, key := range []string{"seed key/é", "user123"} {
		if err := c.CancelDebounce(ctx, "env_default", "fn/1", key); err != nil {
			t.Fatalf("CancelDebounce(%q): %v", key, err)
		}
	}
	want := []string{
		"GET /api/v1/capacity/queue?function=fn+a&limit=10",
		"GET /api/v1/capacity/lanes",
		"DELETE /api/v1/debounce/entries/env_default/fn%2F1/c2VlZCBrZXkvw6k",
		"DELETE /api/v1/debounce/entries/env_default/fn%2F1/dXNlcjEyMw",
	}
	if len(got) != len(want) {
		t.Fatalf("requests = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("request %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRegisterDeploymentRejectsInvalidSnapshot(t *testing.T) {
	c := NewClient(ClientConfig{ServerURL: "http://127.0.0.1:1"})
	_, err := c.Deployments().Register(context.Background(), RegisterDeploymentInput{Snapshot: json.RawMessage(`{"timeoutMs":"soon"}`)})
	var ie *IronflowError
	if !errors.As(err, &ie) || ie.Code != "invalid_argument" {
		t.Fatalf("Register with a bad snapshot = %v, want invalid_argument before any request", err)
	}
}
