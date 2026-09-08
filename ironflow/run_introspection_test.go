package ironflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

func TestRunIntrospectionMethods(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case ironflowv1connect.IronflowServiceGetRunStepsProcedure:
			_, handler := ironflowv1connect.NewIronflowServiceHandler(runStepsService{})
			handler.ServeHTTP(w, r)
		case "/api/v1/runs/run-1/streams":
			_, _ = w.Write([]byte(`{"entity_ids":["order-1","customer-1"]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := &Client{serverURL: server.URL, apiKey: "key", httpClient: server.Client(), retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}

	steps, err := client.GetRunSteps(context.Background(), "run-1")
	if err != nil || steps.Count != 1 || steps.Steps[0].StepID != "charge" {
		t.Fatalf("GetRunSteps = %#v, %v", steps, err)
	}
	streams, err := client.GetRunStreams(context.Background(), "run-1")
	if err != nil || len(streams.EntityIDs) != 2 || streams.EntityIDs[0] != "order-1" {
		t.Fatalf("GetRunStreams = %#v, %v", streams, err)
	}
}
