package ironflow

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetAuditTrail(t *testing.T) {
	t.Run("sends correct method path and body", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string
		var receivedBody map[string]any

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("failed to read request body: %v", err)
			}
			if err := json.Unmarshal(body, &receivedBody); err != nil {
				t.Errorf("failed to unmarshal request body: %v", err)
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"events":[],"total_count":0}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.GetAuditTrail(ctx, "run-abc-123")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.AuditService/GetAuditTrail" {
			t.Errorf("expected path /ironflow.v1.AuditService/GetAuditTrail, got %s", receivedPath)
		}

		if receivedBody["run_id"] != "run-abc-123" {
			t.Errorf("expected run_id 'run-abc-123', got %v", receivedBody["run_id"])
		}
	})

	t.Run("sends filter options in request body", func(t *testing.T) {
		var receivedBody map[string]any

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("failed to read request body: %v", err)
			}
			if err := json.Unmarshal(body, &receivedBody); err != nil {
				t.Errorf("failed to unmarshal request body: %v", err)
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"events":[],"total_count":0}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.GetAuditTrail(ctx, "run-abc-123", GetAuditTrailOpts{
			EventType:     "step.completed",
			FromTimestamp: "2026-01-01T00:00:00Z",
			ToTimestamp:   "2026-02-01T00:00:00Z",
			Limit:         50,
			Cursor:        "cursor-abc",
		})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["event_type"] != "step.completed" {
			t.Errorf("expected event_type 'step.completed', got %v", receivedBody["event_type"])
		}
		if receivedBody["from_timestamp"] != "2026-01-01T00:00:00Z" {
			t.Errorf("expected from_timestamp '2026-01-01T00:00:00Z', got %v", receivedBody["from_timestamp"])
		}
		if receivedBody["to_timestamp"] != "2026-02-01T00:00:00Z" {
			t.Errorf("expected to_timestamp '2026-02-01T00:00:00Z', got %v", receivedBody["to_timestamp"])
		}
		// JSON numbers unmarshal as float64
		if receivedBody["limit"] != float64(50) {
			t.Errorf("expected limit 50, got %v", receivedBody["limit"])
		}
		if receivedBody["cursor"] != "cursor-abc" {
			t.Errorf("expected cursor 'cursor-abc', got %v", receivedBody["cursor"])
		}
	})

	t.Run("parses audit trail response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := `{
				"events": [
					{
						"id": "ae-1",
						"run_id": "run-1",
						"function_id": "fn-1",
						"event_type": "step.completed",
						"payload": {"stepId": "s1", "durationMs": 150},
						"created_at": "2026-02-27T00:00:00Z"
					},
					{
						"id": "ae-2",
						"run_id": "run-1",
						"function_id": "fn-1",
						"step_id": "step-1",
						"event_type": "step.started",
						"payload": {},
						"metadata": {"user": "test"},
						"created_at": "2026-02-27T00:01:00Z"
					}
				],
				"total_count": 2,
				"next_cursor": "cursor-next"
			}`
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(resp))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		result, err := client.GetAuditTrail(ctx, "run-1")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if len(result.Events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(result.Events))
		}

		if result.Events[0].ID != "ae-1" {
			t.Errorf("expected first event ID 'ae-1', got '%s'", result.Events[0].ID)
		}
		if result.Events[0].EventType != "step.completed" {
			t.Errorf("expected first event type 'step.completed', got '%s'", result.Events[0].EventType)
		}
		if result.Events[0].FunctionID != "fn-1" {
			t.Errorf("expected first event function_id 'fn-1', got '%s'", result.Events[0].FunctionID)
		}
		if result.Events[0].StepID != "" {
			t.Errorf("expected first event step_id to be empty, got '%s'", result.Events[0].StepID)
		}

		if result.Events[1].StepID != "step-1" {
			t.Errorf("expected second event step_id 'step-1', got '%s'", result.Events[1].StepID)
		}
		if result.Events[1].Metadata["user"] != "test" {
			t.Errorf("expected second event metadata user 'test', got '%s'", result.Events[1].Metadata["user"])
		}

		if result.TotalCount != 2 {
			t.Errorf("expected total_count 2, got %d", result.TotalCount)
		}
		if result.NextCursor != "cursor-next" {
			t.Errorf("expected next_cursor 'cursor-next', got '%s'", result.NextCursor)
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"code":"NOT_FOUND","message":"run not found"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		result, err := client.GetAuditTrail(ctx, "nonexistent-run")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}

		if result != nil {
			t.Errorf("expected nil result on error, got %v", result)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "NOT_FOUND" {
			t.Errorf("expected code 'NOT_FOUND', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"events":[],"total_count":0}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "test-secret-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.GetAuditTrail(ctx, "run-1")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer test-secret-key" {
			t.Errorf("expected 'Bearer test-secret-key', got '%s'", receivedAuth)
		}
	})
}

func TestAuditEventTypes(t *testing.T) {
	t.Run("FunctionConfig has recording fields", func(t *testing.T) {
		config := FunctionConfig{
			Recording:          true,
			RecordingRetention: "90d",
		}

		if !config.Recording {
			t.Error("expected Recording to be true")
		}
		if config.RecordingRetention != "90d" {
			t.Errorf("expected RecordingRetention '90d', got '%s'", config.RecordingRetention)
		}
	})

	t.Run("AuditEvent JSON marshaling", func(t *testing.T) {
		event := AuditEvent{
			ID:         "ae-1",
			RunID:      "run-1",
			FunctionID: "fn-1",
			StepID:     "step-1",
			EventType:  "step.completed",
			Payload:    map[string]any{"durationMs": 150},
			Metadata:   map[string]string{"user": "test"},
			CreatedAt:  "2026-02-27T00:00:00Z",
		}

		data, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded AuditEvent
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if decoded.ID != "ae-1" {
			t.Errorf("expected id 'ae-1', got '%s'", decoded.ID)
		}
		if decoded.StepID != "step-1" {
			t.Errorf("expected step_id 'step-1', got '%s'", decoded.StepID)
		}
		if decoded.Metadata["user"] != "test" {
			t.Errorf("expected metadata user 'test', got '%s'", decoded.Metadata["user"])
		}
	})

	t.Run("AuditEvent omits empty optional fields", func(t *testing.T) {
		event := AuditEvent{
			ID:         "ae-1",
			RunID:      "run-1",
			FunctionID: "fn-1",
			EventType:  "run.created",
			Payload:    map[string]any{},
			CreatedAt:  "2026-02-27T00:00:00Z",
		}

		data, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatalf("failed to unmarshal to map: %v", err)
		}

		if _, exists := raw["step_id"]; exists {
			t.Error("expected step_id to be omitted when empty")
		}
		if _, exists := raw["metadata"]; exists {
			t.Error("expected metadata to be omitted when nil")
		}
	})
}
