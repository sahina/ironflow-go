package ironflow

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// ============================================================================
// PauseRun tests
// ============================================================================

func TestClient_PauseRun(t *testing.T) {
	t.Run("sends correct request and returns status", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string
		var receivedBody map[string]string

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

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"paused"}`))
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

		status, err := client.PauseRun(context.Background(), "run_abc123")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.IronflowService/PauseRun" {
			t.Errorf("expected path /ironflow.v1.IronflowService/PauseRun, got %s", receivedPath)
		}

		if receivedBody["run_id"] != "run_abc123" {
			t.Errorf("expected run_id 'run_abc123', got %v", receivedBody["run_id"])
		}

		if status != "paused" {
			t.Errorf("expected status 'paused', got %q", status)
		}
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"paused"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "test-key-789",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		_, err := client.PauseRun(context.Background(), "run_abc123")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer test-key-789" {
			t.Errorf("expected auth 'Bearer test-key-789', got %q", receivedAuth)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"database unavailable"}`))
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

		status, err := client.PauseRun(context.Background(), "run_abc123")

		if err == nil {
			t.Fatal("expected error for 500 response")
		}

		if status != "" {
			t.Errorf("expected empty status on error, got %q", status)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "INTERNAL" {
			t.Errorf("expected code 'INTERNAL', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("returns error on not found", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
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

		_, err := client.PauseRun(context.Background(), "run_nonexistent")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "NOT_FOUND" {
			t.Errorf("expected code 'NOT_FOUND', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("returns error on bad request", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"code":"INVALID_ARGUMENT","message":"run is not in a pausable state"}`))
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

		_, err := client.PauseRun(context.Background(), "run_already_done")

		if err == nil {
			t.Fatal("expected error for 400 response")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "INVALID_ARGUMENT" {
			t.Errorf("expected code 'INVALID_ARGUMENT', got '%s'", ironflowErr.Code)
		}
		// 400 errors should not be retryable
		if ironflowErr.Retryable {
			t.Error("expected 400 error to be non-retryable")
		}
	})
}

// ============================================================================
// GetPausedState tests
// ============================================================================

func TestClient_GetPausedState(t *testing.T) {
	t.Run("sends correct request and maps response", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string
		var receivedBody map[string]string

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

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"steps": [
					{
						"id": "step-1",
						"name": "fetch-data",
						"output": "eyJ2YWx1ZSI6NDJ9",
						"injected": false,
						"completedAt": "2026-03-08T10:00:00Z"
					},
					{
						"id": "step-2",
						"name": "validate",
						"output": "eyJ2YWxpZCI6dHJ1ZX0=",
						"injected": true,
						"completedAt": "2026-03-08T10:01:00Z"
					}
				],
				"nextStepHint": "process-result",
				"pauseReason": "manual pause for inspection"
			}`))
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

		state, err := client.GetPausedState(context.Background(), "run_abc123")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.IronflowService/GetPausedState" {
			t.Errorf("expected path /ironflow.v1.IronflowService/GetPausedState, got %s", receivedPath)
		}

		if receivedBody["run_id"] != "run_abc123" {
			t.Errorf("expected run_id 'run_abc123', got %v", receivedBody["run_id"])
		}

		if state == nil {
			t.Fatal("expected non-nil state")
		}

		if len(state.Steps) != 2 {
			t.Fatalf("expected 2 steps, got %d", len(state.Steps))
		}

		// Verify first step
		step1 := state.Steps[0]
		if step1.ID != "step-1" {
			t.Errorf("step[0].ID = %q, want %q", step1.ID, "step-1")
		}
		if step1.Name != "fetch-data" {
			t.Errorf("step[0].Name = %q, want %q", step1.Name, "fetch-data")
		}
		if string(step1.Output) != `{"value":42}` {
			t.Errorf("step[0].Output = %s, want %s", step1.Output, `{"value":42}`)
		}
		if step1.Injected != false {
			t.Errorf("step[0].Injected = %v, want false", step1.Injected)
		}
		if step1.CompletedAt != "2026-03-08T10:00:00Z" {
			t.Errorf("step[0].CompletedAt = %q, want %q", step1.CompletedAt, "2026-03-08T10:00:00Z")
		}

		// Verify second step (injected)
		step2 := state.Steps[1]
		if step2.ID != "step-2" {
			t.Errorf("step[1].ID = %q, want %q", step2.ID, "step-2")
		}
		if step2.Name != "validate" {
			t.Errorf("step[1].Name = %q, want %q", step2.Name, "validate")
		}
		if string(step2.Output) != `{"valid":true}` {
			t.Errorf("step[1].Output = %s, want %s", step2.Output, `{"valid":true}`)
		}
		if step2.Injected != true {
			t.Errorf("step[1].Injected = %v, want true", step2.Injected)
		}
		if step2.CompletedAt != "2026-03-08T10:01:00Z" {
			t.Errorf("step[1].CompletedAt = %q, want %q", step2.CompletedAt, "2026-03-08T10:01:00Z")
		}

		// Verify top-level fields
		if state.NextStepHint != "process-result" {
			t.Errorf("NextStepHint = %q, want %q", state.NextStepHint, "process-result")
		}
		if state.PauseReason != "manual pause for inspection" {
			t.Errorf("PauseReason = %q, want %q", state.PauseReason, "manual pause for inspection")
		}
	})

	t.Run("handles empty steps array", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"steps": [],
				"nextStepHint": "first-step",
				"pauseReason": "paused before any steps ran"
			}`))
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

		state, err := client.GetPausedState(context.Background(), "run_empty")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if state == nil {
			t.Fatal("expected non-nil state")
		}

		if len(state.Steps) != 0 {
			t.Errorf("expected 0 steps, got %d", len(state.Steps))
		}

		if state.NextStepHint != "first-step" {
			t.Errorf("NextStepHint = %q, want %q", state.NextStepHint, "first-step")
		}

		if state.PauseReason != "paused before any steps ran" {
			t.Errorf("PauseReason = %q, want %q", state.PauseReason, "paused before any steps ran")
		}
	})

	t.Run("handles null steps in response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"nextStepHint": "first-step",
				"pauseReason": ""
			}`))
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

		state, err := client.GetPausedState(context.Background(), "run_null_steps")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if state == nil {
			t.Fatal("expected non-nil state")
		}

		if len(state.Steps) != 0 {
			t.Errorf("expected 0 steps for null/missing steps, got %d", len(state.Steps))
		}
	})

	t.Run("returns error on not found", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
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

		state, err := client.GetPausedState(context.Background(), "run_nonexistent")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}

		if state != nil {
			t.Errorf("expected nil state on error, got %+v", state)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "NOT_FOUND" {
			t.Errorf("expected code 'NOT_FOUND', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("returns error when run is not paused", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"code":"FAILED_PRECONDITION","message":"run is not paused"}`))
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

		state, err := client.GetPausedState(context.Background(), "run_running")

		if err == nil {
			t.Fatal("expected error for non-paused run")
		}

		if state != nil {
			t.Errorf("expected nil state on error, got %+v", state)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "FAILED_PRECONDITION" {
			t.Errorf("expected code 'FAILED_PRECONDITION', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("step output with empty string is preserved", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"steps": [
					{
						"id": "step-1",
						"name": "no-output-step",
						"output": "",
						"injected": false,
						"completedAt": "2026-03-08T10:00:00Z"
					}
				],
				"nextStepHint": "",
				"pauseReason": ""
			}`))
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

		state, err := client.GetPausedState(context.Background(), "run_empty_output")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if len(state.Steps) != 1 {
			t.Fatalf("expected 1 step, got %d", len(state.Steps))
		}

		// Empty output string becomes empty json.RawMessage
		if string(state.Steps[0].Output) != "" {
			t.Errorf("step[0].Output = %q, want empty string", state.Steps[0].Output)
		}
	})
}

// ============================================================================
// InjectStepOutput tests
// ============================================================================

func TestClient_InjectStepOutput(t *testing.T) {
	t.Run("sends correct request and returns previous output", func(t *testing.T) {
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

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"stepId": "step-xyz",
				"previousOutput": "eyJvbGRfdmFsdWUiOjEwMH0="
			}`))
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

		newOutput := json.RawMessage(`{"corrected":true}`)
		prevOutput, err := client.InjectStepOutput(
			context.Background(),
			"run_abc123",
			"step-xyz",
			newOutput,
			"Manual correction",
		)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		// Verify request
		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.IronflowService/InjectStepOutput" {
			t.Errorf("expected path /ironflow.v1.IronflowService/InjectStepOutput, got %s", receivedPath)
		}

		if receivedBody["run_id"] != "run_abc123" {
			t.Errorf("expected run_id 'run_abc123', got %v", receivedBody["run_id"])
		}

		if receivedBody["step_id"] != "step-xyz" {
			t.Errorf("expected step_id 'step-xyz', got %v", receivedBody["step_id"])
		}

		if receivedBody["new_output"] != "eyJjb3JyZWN0ZWQiOnRydWV9" {
			t.Errorf("expected base64-encoded new_output, got %v", receivedBody["new_output"])
		}

		if receivedBody["reason"] != "Manual correction" {
			t.Errorf("expected reason 'Manual correction', got %v", receivedBody["reason"])
		}

		// Verify response
		if prevOutput == nil {
			t.Fatal("expected non-nil previous output")
		}

		if string(prevOutput) != `{"old_value":100}` {
			t.Errorf("expected previous output '{\"old_value\":100}', got %s", prevOutput)
		}
	})

	t.Run("returns nil when previous output is empty", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"stepId": "step-xyz",
				"previousOutput": ""
			}`))
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

		prevOutput, err := client.InjectStepOutput(
			context.Background(),
			"run_abc123",
			"step-first",
			json.RawMessage(`{"new":"value"}`),
			"inject into step with no prior output",
		)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if prevOutput != nil {
			t.Errorf("expected nil previous output for empty string, got %s", prevOutput)
		}
	})

	t.Run("returns error on not found", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"code":"NOT_FOUND","message":"step not found in run"}`))
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

		prevOutput, err := client.InjectStepOutput(
			context.Background(),
			"run_abc123",
			"step-nonexistent",
			json.RawMessage(`{"value":1}`),
			"reason",
		)

		if err == nil {
			t.Fatal("expected error for 404 response")
		}

		if prevOutput != nil {
			t.Errorf("expected nil previous output on error, got %s", prevOutput)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "NOT_FOUND" {
			t.Errorf("expected code 'NOT_FOUND', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("returns error when run is not paused", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"code":"FAILED_PRECONDITION","message":"run must be paused to inject step output"}`))
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

		_, err := client.InjectStepOutput(
			context.Background(),
			"run_running",
			"step-1",
			json.RawMessage(`{"value":1}`),
			"reason",
		)

		if err == nil {
			t.Fatal("expected error when run is not paused")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "FAILED_PRECONDITION" {
			t.Errorf("expected code 'FAILED_PRECONDITION', got '%s'", ironflowErr.Code)
		}
		if ironflowErr.Retryable {
			t.Error("expected 400 error to be non-retryable")
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"unexpected error"}`))
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

		_, err := client.InjectStepOutput(
			context.Background(),
			"run_abc123",
			"step-1",
			json.RawMessage(`{"value":1}`),
			"reason",
		)

		if err == nil {
			t.Fatal("expected error for 500 response")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "INTERNAL" {
			t.Errorf("expected code 'INTERNAL', got '%s'", ironflowErr.Code)
		}
		// 500 errors should be retryable
		if !ironflowErr.Retryable {
			t.Error("expected 500 error to be retryable")
		}
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"stepId":"step-1","previousOutput":""}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "secret-key-abc",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		_, err := client.InjectStepOutput(
			context.Background(),
			"run_abc123",
			"step-1",
			json.RawMessage(`{"value":1}`),
			"reason",
		)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer secret-key-abc" {
			t.Errorf("expected auth 'Bearer secret-key-abc', got %q", receivedAuth)
		}
	})

	t.Run("handles complex JSON output", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			b, _ := io.ReadAll(r.Body)
			json.Unmarshal(b, &body)

			// Echo back the new_output as previous_output for verification
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			resp := map[string]string{
				"stepId":         "step-complex",
				"previousOutput": "eyJuZXN0ZWQiOnsiYXJyYXkiOlsxLDIsM10sImZsYWciOnRydWV9LCJjb3VudCI6OTl9",
			}
			json.NewEncoder(w).Encode(resp)
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

		complexOutput := json.RawMessage(`{"nested":{"deep":"value"},"list":[1,2,3]}`)
		prevOutput, err := client.InjectStepOutput(
			context.Background(),
			"run_abc123",
			"step-complex",
			complexOutput,
			"complex injection",
		)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if prevOutput == nil {
			t.Fatal("expected non-nil previous output")
		}

		// Verify the previous output can be parsed as JSON
		var parsed map[string]any
		if err := json.Unmarshal(prevOutput, &parsed); err != nil {
			t.Fatalf("previous output is not valid JSON: %v", err)
		}

		nested, ok := parsed["nested"].(map[string]any)
		if !ok {
			t.Fatal("expected nested object in previous output")
		}
		if nested["flag"] != true {
			t.Errorf("expected nested.flag to be true, got %v", nested["flag"])
		}
	})

	t.Run("returns unauthorized error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"code":"UNAUTHENTICATED","message":"invalid API key"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "bad-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		_, err := client.InjectStepOutput(
			context.Background(),
			"run_abc123",
			"step-1",
			json.RawMessage(`{"value":1}`),
			"reason",
		)

		if err == nil {
			t.Fatal("expected error for 401 response")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}

		// The error should wrap ErrUnauthorized
		if ironflowErr.Cause != ErrUnauthorized {
			t.Errorf("expected Cause to be ErrUnauthorized, got %v", ironflowErr.Cause)
		}
	})
}
