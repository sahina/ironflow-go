package ironflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRunIntrospectionMethods(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/runs/run-1/steps":
			_, _ = w.Write([]byte(`{"steps":[{"id":"row-1","run_id":"run-1","step_id":"charge","step_type":"invoke","sequence":1,"status":"completed","output":{"ok":true},"attempt":1,"created_at":"2026-08-27T12:00:00Z","updated_at":"2026-08-27T12:00:01Z"}],"count":1}`))
		case "/api/v1/runs/run-1/streams":
			_, _ = w.Write([]byte(`{"entity_ids":["order-1","customer-1"]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := &Client{serverURL: server.URL, httpClient: server.Client(), retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}

	steps, err := client.GetRunSteps(context.Background(), "run-1")
	if err != nil || steps.Count != 1 || steps.Steps[0].StepID != "charge" {
		t.Fatalf("GetRunSteps = %#v, %v", steps, err)
	}
	streams, err := client.GetRunStreams(context.Background(), "run-1")
	if err != nil || len(streams.EntityIDs) != 2 || streams.EntityIDs[0] != "order-1" {
		t.Fatalf("GetRunStreams = %#v, %v", streams, err)
	}
}
