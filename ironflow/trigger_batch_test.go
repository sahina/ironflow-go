package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTriggerBatch(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ironflow.v1.IronflowService/TriggerBatch" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(`{"results":[{"runIds":["run-1"],"eventId":"evt-1"},{"runIds":[],"eventId":"evt-2"}]}`))
	}))
	defer server.Close()

	client := &Client{serverURL: server.URL, httpClient: server.Client(), retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}
	results, err := client.TriggerBatch(context.Background(), []TriggerBatchEvent{
		{Event: "order.placed", Data: map[string]any{"id": "1"}, IdempotencyKey: "order-1", Version: 2},
		{Event: "order.shipped", Data: map[string]any{"id": "1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].EventID != "evt-1" || len(results[0].RunIDs) != 1 {
		t.Fatalf("results = %#v", results)
	}
	events, ok := body["events"].([]any)
	if !ok || len(events) != 2 {
		t.Fatalf("request events = %#v", body["events"])
	}
	first := events[0].(map[string]any)
	if first["idempotencyKey"] != "order-1" || first["version"] != float64(2) {
		t.Fatalf("first event = %#v", first)
	}
}
