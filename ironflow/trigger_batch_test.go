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
		_, _ = w.Write([]byte(`{"results":[{"runIds":["run-1"],"eventId":"evt-1"},{"runIds":[],"eventId":"evt-2"},{"error":{"code":"invalid_argument","message":"reserved root"}}]}`))
	}))
	defer server.Close()

	client := &Client{serverURL: server.URL, httpClient: server.Client(), retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}
	results, err := client.TriggerBatch(context.Background(), []TriggerBatchEvent{
		{Event: "order.placed", Data: map[string]any{"id": "1"}, IdempotencyKey: "order-1", Version: 2},
		{Event: "order.shipped", Data: map[string]any{"id": "1"}},
		{Event: "ironflow.x", Data: map[string]any{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 || results[0].EventID != "evt-1" || len(results[0].RunIDs) != 1 || results[0].Error != nil {
		t.Fatalf("results = %#v", results)
	}
	if e := results[2].Error; e == nil || e.Code != "invalid_argument" || e.Message != "reserved root" || results[2].EventID != "" {
		t.Fatalf("failed item = %#v", results[2])
	}
	events, ok := body["events"].([]any)
	if !ok || len(events) != 3 {
		t.Fatalf("request events = %#v", body["events"])
	}
	first := events[0].(map[string]any)
	if first["idempotencyKey"] != "order-1" || first["version"] != float64(2) {
		t.Fatalf("first event = %#v", first)
	}
}
