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

// ============================================================================
// PatchStep tests
// ============================================================================

func TestPatchStep(t *testing.T) {
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
		output := map[string]any{"result": "fixed-value"}
		err := client.PatchStep(ctx, "step-abc-123", output, "manual hotfix")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/api/v1/steps/patch" {
			t.Errorf("expected path /api/v1/steps/patch, got %s", receivedPath)
		}

		if receivedBody["step_id"] != "step-abc-123" {
			t.Errorf("expected step_id 'step-abc-123', got %v", receivedBody["step_id"])
		}

		if receivedBody["reason"] != "manual hotfix" {
			t.Errorf("expected reason 'manual hotfix', got %v", receivedBody["reason"])
		}

		outputMap, ok := receivedBody["output"].(map[string]any)
		if !ok {
			t.Fatalf("expected output to be a map, got %T", receivedBody["output"])
		}
		if outputMap["result"] != "fixed-value" {
			t.Errorf("expected output.result 'fixed-value', got %v", outputMap["result"])
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"code":"BAD_REQUEST","message":"invalid step ID"}`))
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
		err := client.PatchStep(ctx, "bad-id", nil, "")

		if err == nil {
			t.Fatal("expected error for 400 response")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "BAD_REQUEST" {
			t.Errorf("expected code 'BAD_REQUEST', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
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
		err := client.PatchStep(ctx, "step-1", map[string]any{"x": 1}, "reason")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer test-secret-key" {
			t.Errorf("expected 'Bearer test-secret-key', got '%s'", receivedAuth)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"SERVER_ERROR","message":"internal failure"}`))
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
		err := client.PatchStep(ctx, "step-1", nil, "")

		if err == nil {
			t.Fatal("expected error for 500 response")
		}
	})
}

// ============================================================================
// ListFunctions tests
// ============================================================================

func TestListFunctions(t *testing.T) {
	t.Run("sends correct method and path", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"functions":[]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListFunctions(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "GET" {
			t.Errorf("expected method GET, got %s", receivedMethod)
		}

		if receivedPath != "/api/v1/functions" {
			t.Errorf("expected path /api/v1/functions, got %s", receivedPath)
		}
	})

	t.Run("parses function list response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := `{
				"functions": [
					{
						"id": "fn-001",
						"name": "process-order",
						"status": "active",
						"preferred_mode": "push",
						"created_at": "2025-01-01T00:00:00Z",
						"updated_at": "2025-01-02T00:00:00Z"
					},
					{
						"id": "fn-002",
						"name": "send-email",
						"status": "inactive",
						"preferred_mode": "pull",
						"created_at": "2025-02-01T00:00:00Z",
						"updated_at": "2025-02-02T00:00:00Z"
					}
				]
			}`
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(resp))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		functions, err := client.ListFunctions(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if len(functions) != 2 {
			t.Fatalf("expected 2 functions, got %d", len(functions))
		}

		if functions[0].ID != "fn-001" {
			t.Errorf("expected first function ID 'fn-001', got '%s'", functions[0].ID)
		}
		if functions[0].Name != "process-order" {
			t.Errorf("expected first function name 'process-order', got '%s'", functions[0].Name)
		}
		if functions[0].Status != "active" {
			t.Errorf("expected first function status 'active', got '%s'", functions[0].Status)
		}
		if functions[0].PreferredMode != "push" {
			t.Errorf("expected first function preferred_mode 'push', got '%s'", functions[0].PreferredMode)
		}
		if functions[0].CreatedAt != "2025-01-01T00:00:00Z" {
			t.Errorf("expected created_at '2025-01-01T00:00:00Z', got '%s'", functions[0].CreatedAt)
		}
		if functions[0].UpdatedAt != "2025-01-02T00:00:00Z" {
			t.Errorf("expected updated_at '2025-01-02T00:00:00Z', got '%s'", functions[0].UpdatedAt)
		}

		if functions[1].ID != "fn-002" {
			t.Errorf("expected second function ID 'fn-002', got '%s'", functions[1].ID)
		}
		if functions[1].PreferredMode != "pull" {
			t.Errorf("expected second function preferred_mode 'pull', got '%s'", functions[1].PreferredMode)
		}
	})

	t.Run("returns empty slice for empty response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"functions":[]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		functions, err := client.ListFunctions(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if len(functions) != 0 {
			t.Errorf("expected 0 functions, got %d", len(functions))
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"SERVER_ERROR","message":"database down"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		functions, err := client.ListFunctions(ctx)

		if err == nil {
			t.Fatal("expected error for 500 response")
		}

		if functions != nil {
			t.Errorf("expected nil functions on error, got %v", functions)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "HTTP_ERROR" {
			t.Errorf("expected code 'HTTP_ERROR', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"functions":[]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "my-api-key",
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListFunctions(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer my-api-key" {
			t.Errorf("expected 'Bearer my-api-key', got '%s'", receivedAuth)
		}
	})

	t.Run("does not set auth header when apiKey is empty", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"functions":[]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListFunctions(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "" {
			t.Errorf("expected no Authorization header, got '%s'", receivedAuth)
		}
	})

	t.Run("returns error on invalid JSON", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`not valid json`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListFunctions(ctx)

		if err == nil {
			t.Fatal("expected error for invalid JSON")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "DECODE_ERROR" {
			t.Errorf("expected code 'DECODE_ERROR', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("handles request timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(2 * time.Second)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{Timeout: 50 * time.Millisecond},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListFunctions(ctx)

		if err == nil {
			t.Fatal("expected timeout error")
		}
	})
}

// ============================================================================
// Emit option tests (regression: verifies options formerly on Trigger)
// ============================================================================

func TestEmit_WithIdempotencyKey(t *testing.T) {
	var receivedBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &receivedBody) //nolint:errcheck
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"runIds":["run-001"],"eventId":"evt-001"}`))
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
	result, err := client.Emit(ctx, "order.placed", map[string]any{"orderId": "123"},
		WithEmitIdempotencyKey("idem-key-abc"),
	)

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if result.EventID != "evt-001" {
		t.Errorf("expected event_id 'evt-001', got '%s'", result.EventID)
	}

	if receivedBody["idempotency_key"] != "idem-key-abc" {
		t.Errorf("expected idempotency_key 'idem-key-abc', got %v", receivedBody["idempotency_key"])
	}
}

func TestEmit_WithMetadata(t *testing.T) {
	var receivedBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &receivedBody) //nolint:errcheck
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"runIds":["run-002"],"eventId":"evt-002"}`))
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
	result, err := client.Emit(ctx, "order.shipped", map[string]any{"orderId": "456"},
		WithEmitMetadata(map[string]any{"source": "api", "version": float64(2)}),
	)

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if result.EventID != "evt-002" {
		t.Errorf("expected event_id 'evt-002', got '%s'", result.EventID)
	}

	meta, ok := receivedBody["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata to be a map, got %T", receivedBody["metadata"])
	}
	if meta["source"] != "api" {
		t.Errorf("expected metadata.source 'api', got %v", meta["source"])
	}
}

// ============================================================================
// ListWorkers tests
// ============================================================================

func TestListWorkers(t *testing.T) {
	t.Run("sends correct method and path", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"workers":[]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListWorkers(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "GET" {
			t.Errorf("expected method GET, got %s", receivedMethod)
		}

		if receivedPath != "/api/v1/workers" {
			t.Errorf("expected path /api/v1/workers, got %s", receivedPath)
		}
	})

	t.Run("parses worker list response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := `{
				"workers": [
					{
						"id": "wrk-001",
						"hostname": "worker-host-1",
						"function_ids": ["fn-001", "fn-002"],
						"max_concurrent": 10,
						"labels": {"region": "us-east", "tier": "premium"},
						"active_jobs": 3,
						"registered_at": "2025-01-01T00:00:00Z",
						"last_heartbeat": "2025-01-01T01:00:00Z",
						"transport": "grpc"
					},
					{
						"id": "wrk-002",
						"hostname": "worker-host-2",
						"function_ids": ["fn-003"],
						"max_concurrent": 5,
						"labels": {},
						"active_jobs": 0,
						"registered_at": "2025-02-01T00:00:00Z",
						"last_heartbeat": "2025-02-01T01:00:00Z",
						"transport": "websocket"
					}
				]
			}`
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(resp))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		workers, err := client.ListWorkers(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if len(workers) != 2 {
			t.Fatalf("expected 2 workers, got %d", len(workers))
		}

		w1 := workers[0]
		if w1.ID != "wrk-001" {
			t.Errorf("expected first worker ID 'wrk-001', got '%s'", w1.ID)
		}
		if w1.Hostname != "worker-host-1" {
			t.Errorf("expected hostname 'worker-host-1', got '%s'", w1.Hostname)
		}
		if len(w1.FunctionIDs) != 2 {
			t.Fatalf("expected 2 function IDs, got %d", len(w1.FunctionIDs))
		}
		if w1.FunctionIDs[0] != "fn-001" || w1.FunctionIDs[1] != "fn-002" {
			t.Errorf("expected function IDs [fn-001, fn-002], got %v", w1.FunctionIDs)
		}
		if w1.MaxConcurrent != 10 {
			t.Errorf("expected max_concurrent 10, got %d", w1.MaxConcurrent)
		}
		if w1.Labels["region"] != "us-east" {
			t.Errorf("expected label region 'us-east', got '%s'", w1.Labels["region"])
		}
		if w1.Labels["tier"] != "premium" {
			t.Errorf("expected label tier 'premium', got '%s'", w1.Labels["tier"])
		}
		if w1.ActiveJobs != 3 {
			t.Errorf("expected active_jobs 3, got %d", w1.ActiveJobs)
		}
		if w1.RegisteredAt != "2025-01-01T00:00:00Z" {
			t.Errorf("expected registered_at '2025-01-01T00:00:00Z', got '%s'", w1.RegisteredAt)
		}
		if w1.LastHeartbeat != "2025-01-01T01:00:00Z" {
			t.Errorf("expected last_heartbeat '2025-01-01T01:00:00Z', got '%s'", w1.LastHeartbeat)
		}
		if w1.Transport != "grpc" {
			t.Errorf("expected transport 'grpc', got '%s'", w1.Transport)
		}

		w2 := workers[1]
		if w2.ID != "wrk-002" {
			t.Errorf("expected second worker ID 'wrk-002', got '%s'", w2.ID)
		}
		if w2.Transport != "websocket" {
			t.Errorf("expected transport 'websocket', got '%s'", w2.Transport)
		}
		if w2.ActiveJobs != 0 {
			t.Errorf("expected active_jobs 0, got %d", w2.ActiveJobs)
		}
	})

	t.Run("returns empty slice for empty response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"workers":[]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		workers, err := client.ListWorkers(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if len(workers) != 0 {
			t.Errorf("expected 0 workers, got %d", len(workers))
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"code":"SERVICE_UNAVAILABLE","message":"maintenance"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		workers, err := client.ListWorkers(ctx)

		if err == nil {
			t.Fatal("expected error for 503 response")
		}

		if workers != nil {
			t.Errorf("expected nil workers on error, got %v", workers)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "HTTP_ERROR" {
			t.Errorf("expected code 'HTTP_ERROR', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"workers":[]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "worker-api-key",
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListWorkers(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer worker-api-key" {
			t.Errorf("expected 'Bearer worker-api-key', got '%s'", receivedAuth)
		}
	})

	t.Run("does not set auth header when apiKey is empty", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"workers":[]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListWorkers(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "" {
			t.Errorf("expected no Authorization header, got '%s'", receivedAuth)
		}
	})

	t.Run("returns error on invalid JSON", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{broken json`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListWorkers(ctx)

		if err == nil {
			t.Fatal("expected error for invalid JSON")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "DECODE_ERROR" {
			t.Errorf("expected code 'DECODE_ERROR', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("handles request timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(2 * time.Second)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{Timeout: 50 * time.Millisecond},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListWorkers(ctx)

		if err == nil {
			t.Fatal("expected timeout error")
		}
	})
}

// ============================================================================
// Emit tests
// ============================================================================

func TestEmit(t *testing.T) {
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
			w.Write([]byte(`{"runIds":["run-001","run-002"],"eventId":"evt-abc"}`))
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
		result, err := client.Emit(ctx, "order.placed", map[string]any{"orderId": "123"})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.PubSubService/Emit" {
			t.Errorf("expected path /ironflow.v1.PubSubService/Emit, got %s", receivedPath)
		}

		if receivedBody["event"] != "order.placed" {
			t.Errorf("expected event 'order.placed', got %v", receivedBody["event"])
		}

		if receivedBody["namespace"] != "default" {
			t.Errorf("expected namespace 'default', got %v", receivedBody["namespace"])
		}

		dataMap, ok := receivedBody["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected data to be a map, got %T", receivedBody["data"])
		}
		if dataMap["orderId"] != "123" {
			t.Errorf("expected data.orderId '123', got %v", dataMap["orderId"])
		}

		if len(result.RunIDs) != 2 {
			t.Fatalf("expected 2 run IDs, got %d", len(result.RunIDs))
		}
		if result.RunIDs[0] != "run-001" {
			t.Errorf("expected first run ID 'run-001', got '%s'", result.RunIDs[0])
		}
		if result.RunIDs[1] != "run-002" {
			t.Errorf("expected second run ID 'run-002', got '%s'", result.RunIDs[1])
		}
		if result.EventID != "evt-abc" {
			t.Errorf("expected event ID 'evt-abc', got '%s'", result.EventID)
		}
	})

	t.Run("sends optional fields with emit options", func(t *testing.T) {
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
			w.Write([]byte(`{"runIds":[],"eventId":"evt-123"}`))
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
		_, err := client.Emit(ctx, "payment.processed", map[string]any{"amount": 50},
			WithEmitIdempotencyKey("idem-key-1"),
			WithEmitVersion(2),
			WithEmitMetadata(map[string]any{"source": "test"}),
			WithEmitNamespace("prod"),
		)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["idempotency_key"] != "idem-key-1" {
			t.Errorf("expected idempotency_key 'idem-key-1', got %v", receivedBody["idempotency_key"])
		}

		// JSON numbers are float64
		if receivedBody["version"] != float64(2) {
			t.Errorf("expected version 2, got %v", receivedBody["version"])
		}

		if receivedBody["namespace"] != "prod" {
			t.Errorf("expected namespace 'prod', got %v", receivedBody["namespace"])
		}

		metaMap, ok := receivedBody["metadata"].(map[string]any)
		if !ok {
			t.Fatalf("expected metadata to be a map, got %T", receivedBody["metadata"])
		}
		if metaMap["source"] != "test" {
			t.Errorf("expected metadata.source 'test', got %v", metaMap["source"])
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"code":"BAD_REQUEST","message":"invalid event name"}`))
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
		result, err := client.Emit(ctx, "", nil)

		if err == nil {
			t.Fatal("expected error for 400 response")
		}

		if result != nil {
			t.Errorf("expected nil result on error, got %v", result)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "BAD_REQUEST" {
			t.Errorf("expected code 'BAD_REQUEST', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"database error"}`))
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
		_, err := client.Emit(ctx, "test.event", nil)

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
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"runIds":[],"eventId":"evt-1"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "emit-api-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.Emit(ctx, "test.event", nil)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer emit-api-key" {
			t.Errorf("expected 'Bearer emit-api-key', got '%s'", receivedAuth)
		}
	})
}

// ============================================================================
// GetRun tests
// ============================================================================

func TestGetRun(t *testing.T) {
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

			resp := `{
				"id": "run-abc",
				"function_id": "fn-001",
				"event_id": "evt-xyz",
				"status": "completed",
				"attempt": 1,
				"max_attempts": 3,
				"input": {"key": "value"},
				"output": {"result": "success"},
				"started_at": "2025-01-01T00:00:00Z",
				"ended_at": "2025-01-01T00:01:00Z",
				"created_at": "2025-01-01T00:00:00Z",
				"updated_at": "2025-01-01T00:01:00Z"
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
		run, err := client.GetRun(ctx, "run-abc")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.IronflowService/GetRun" {
			t.Errorf("expected path /ironflow.v1.IronflowService/GetRun, got %s", receivedPath)
		}

		if receivedBody["id"] != "run-abc" {
			t.Errorf("expected id 'run-abc', got %v", receivedBody["id"])
		}

		if run.ID != "run-abc" {
			t.Errorf("expected run ID 'run-abc', got '%s'", run.ID)
		}
		if run.FunctionID != "fn-001" {
			t.Errorf("expected function ID 'fn-001', got '%s'", run.FunctionID)
		}
		if run.EventID != "evt-xyz" {
			t.Errorf("expected event ID 'evt-xyz', got '%s'", run.EventID)
		}
		if run.Status != RunStatusCompleted {
			t.Errorf("expected status 'completed', got '%s'", run.Status)
		}
		if run.Attempt != 1 {
			t.Errorf("expected attempt 1, got %d", run.Attempt)
		}
		if run.MaxAttempts != 3 {
			t.Errorf("expected max_attempts 3, got %d", run.MaxAttempts)
		}

		inputMap, ok := run.Input.(map[string]any)
		if !ok {
			t.Fatalf("expected input to be a map, got %T", run.Input)
		}
		if inputMap["key"] != "value" {
			t.Errorf("expected input.key 'value', got %v", inputMap["key"])
		}

		outputMap, ok := run.Output.(map[string]any)
		if !ok {
			t.Fatalf("expected output to be a map, got %T", run.Output)
		}
		if outputMap["result"] != "success" {
			t.Errorf("expected output.result 'success', got %v", outputMap["result"])
		}

		if run.StartedAt == nil {
			t.Fatal("expected started_at to be non-nil")
		}
		if run.EndedAt == nil {
			t.Fatal("expected ended_at to be non-nil")
		}
		if run.CreatedAt.IsZero() {
			t.Error("expected created_at to be non-zero")
		}
		if run.UpdatedAt.IsZero() {
			t.Error("expected updated_at to be non-zero")
		}
	})

	t.Run("parses run with error info", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resp := `{
				"id": "run-fail",
				"function_id": "fn-002",
				"event_id": "evt-fail",
				"status": "failed",
				"attempt": 3,
				"max_attempts": 3,
				"error": {"message": "step timeout exceeded", "code": "TIMEOUT"},
				"created_at": "2025-01-01T00:00:00Z",
				"updated_at": "2025-01-01T00:02:00Z"
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
		run, err := client.GetRun(ctx, "run-fail")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if run.Status != RunStatusFailed {
			t.Errorf("expected status 'failed', got '%s'", run.Status)
		}

		if run.Error == nil {
			t.Fatal("expected error info to be non-nil")
		}
		if run.Error.Message != "step timeout exceeded" {
			t.Errorf("expected error message 'step timeout exceeded', got '%s'", run.Error.Message)
		}
		if run.Error.Code != "TIMEOUT" {
			t.Errorf("expected error code 'TIMEOUT', got '%s'", run.Error.Code)
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
		run, err := client.GetRun(ctx, "nonexistent")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}

		if run != nil {
			t.Errorf("expected nil run on error, got %v", run)
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
			w.Write([]byte(`{"id":"run-1","status":"pending","created_at":"2025-01-01T00:00:00Z","updated_at":"2025-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "getrun-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.GetRun(ctx, "run-1")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer getrun-key" {
			t.Errorf("expected 'Bearer getrun-key', got '%s'", receivedAuth)
		}
	})
}

// ============================================================================
// ListRuns tests
// ============================================================================

func TestListRuns(t *testing.T) {
	t.Run("sends correct method path and body with options", func(t *testing.T) {
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

			resp := `{
				"runs": [
					{
						"id": "run-001",
						"function_id": "fn-abc",
						"status": "completed",
						"attempt": 1,
						"max_attempts": 3,
						"created_at": "2025-01-01T00:00:00Z",
						"updated_at": "2025-01-01T00:01:00Z"
					},
					{
						"id": "run-002",
						"function_id": "fn-abc",
						"status": "running",
						"attempt": 1,
						"max_attempts": 3,
						"created_at": "2025-01-02T00:00:00Z",
						"updated_at": "2025-01-02T00:01:00Z"
					}
				],
				"nextCursor": "cursor-xyz",
				"totalCount": 42
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
		result, err := client.ListRuns(ctx, &ListRunsOptions{
			FunctionID: "fn-abc",
			Status:     RunStatusCompleted,
			Limit:      10,
			Cursor:     "cursor-start",
		})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.IronflowService/ListRuns" {
			t.Errorf("expected path /ironflow.v1.IronflowService/ListRuns, got %s", receivedPath)
		}

		if receivedBody["function_id"] != "fn-abc" {
			t.Errorf("expected function_id 'fn-abc', got %v", receivedBody["function_id"])
		}
		if receivedBody["status"] != "completed" {
			t.Errorf("expected status 'completed', got %v", receivedBody["status"])
		}
		if receivedBody["limit"] != float64(10) {
			t.Errorf("expected limit 10, got %v", receivedBody["limit"])
		}
		if receivedBody["cursor"] != "cursor-start" {
			t.Errorf("expected cursor 'cursor-start', got %v", receivedBody["cursor"])
		}

		if len(result.Runs) != 2 {
			t.Fatalf("expected 2 runs, got %d", len(result.Runs))
		}

		if result.Runs[0].ID != "run-001" {
			t.Errorf("expected first run ID 'run-001', got '%s'", result.Runs[0].ID)
		}
		if result.Runs[0].Status != RunStatusCompleted {
			t.Errorf("expected first run status 'completed', got '%s'", result.Runs[0].Status)
		}
		if result.Runs[1].ID != "run-002" {
			t.Errorf("expected second run ID 'run-002', got '%s'", result.Runs[1].ID)
		}
		if result.Runs[1].Status != RunStatusRunning {
			t.Errorf("expected second run status 'running', got '%s'", result.Runs[1].Status)
		}

		if result.NextCursor != "cursor-xyz" {
			t.Errorf("expected next_cursor 'cursor-xyz', got '%s'", result.NextCursor)
		}
		if result.TotalCount != 42 {
			t.Errorf("expected total_count 42, got %d", result.TotalCount)
		}
	})

	t.Run("sends empty body with nil options", func(t *testing.T) {
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
			w.Write([]byte(`{"runs":[],"nextCursor":"","totalCount":0}`))
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
		result, err := client.ListRuns(ctx, nil)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		// With nil options, body should be an empty map (no filter keys)
		if len(receivedBody) != 0 {
			t.Errorf("expected empty body with nil options, got %v", receivedBody)
		}

		if len(result.Runs) != 0 {
			t.Errorf("expected 0 runs, got %d", len(result.Runs))
		}
		if result.TotalCount != 0 {
			t.Errorf("expected total_count 0, got %d", result.TotalCount)
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"database error"}`))
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
		result, err := client.ListRuns(ctx, nil)

		if err == nil {
			t.Fatal("expected error for 500 response")
		}

		if result != nil {
			t.Errorf("expected nil result on error, got %v", result)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "INTERNAL" {
			t.Errorf("expected code 'INTERNAL', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"runs":[],"nextCursor":"","totalCount":0}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "listruns-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListRuns(ctx, nil)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer listruns-key" {
			t.Errorf("expected 'Bearer listruns-key', got '%s'", receivedAuth)
		}
	})
}

// ============================================================================
// CancelRun tests
// ============================================================================

func TestCancelRun(t *testing.T) {
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

			resp := `{
				"id": "run-cancel-1",
				"function_id": "fn-001",
				"status": "cancelled",
				"attempt": 1,
				"max_attempts": 3,
				"created_at": "2025-01-01T00:00:00Z",
				"updated_at": "2025-01-01T00:05:00Z"
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
		run, err := client.CancelRun(ctx, "run-cancel-1", "user requested cancellation")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.IronflowService/CancelRun" {
			t.Errorf("expected path /ironflow.v1.IronflowService/CancelRun, got %s", receivedPath)
		}

		if receivedBody["id"] != "run-cancel-1" {
			t.Errorf("expected id 'run-cancel-1', got %v", receivedBody["id"])
		}

		if receivedBody["reason"] != "user requested cancellation" {
			t.Errorf("expected reason 'user requested cancellation', got %v", receivedBody["reason"])
		}

		if run.ID != "run-cancel-1" {
			t.Errorf("expected run ID 'run-cancel-1', got '%s'", run.ID)
		}
		if run.Status != RunStatusCancelled {
			t.Errorf("expected status 'cancelled', got '%s'", run.Status)
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
		run, err := client.CancelRun(ctx, "nonexistent", "cancel it")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}

		if run != nil {
			t.Errorf("expected nil run on error, got %v", run)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "NOT_FOUND" {
			t.Errorf("expected code 'NOT_FOUND', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"internal error"}`))
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
		_, err := client.CancelRun(ctx, "run-1", "reason")

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
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"id":"run-1","status":"cancelled","created_at":"2025-01-01T00:00:00Z","updated_at":"2025-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "cancel-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.CancelRun(ctx, "run-1", "reason")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer cancel-key" {
			t.Errorf("expected 'Bearer cancel-key', got '%s'", receivedAuth)
		}
	})
}

// ============================================================================
// RetryRun tests
// ============================================================================

func TestRetryRun(t *testing.T) {
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

			resp := `{
				"id": "run-retry-1",
				"function_id": "fn-001",
				"status": "running",
				"attempt": 2,
				"max_attempts": 3,
				"created_at": "2025-01-01T00:00:00Z",
				"updated_at": "2025-01-01T00:10:00Z"
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
		run, err := client.RetryRun(ctx, "run-retry-1", "")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.IronflowService/RetryRun" {
			t.Errorf("expected path /ironflow.v1.IronflowService/RetryRun, got %s", receivedPath)
		}

		if receivedBody["id"] != "run-retry-1" {
			t.Errorf("expected id 'run-retry-1', got %v", receivedBody["id"])
		}

		// When fromStep is empty, from_step should not be present
		if _, exists := receivedBody["from_step"]; exists {
			t.Errorf("expected from_step to not be present when empty, got %v", receivedBody["from_step"])
		}

		if run.ID != "run-retry-1" {
			t.Errorf("expected run ID 'run-retry-1', got '%s'", run.ID)
		}
		if run.Status != RunStatusRunning {
			t.Errorf("expected status 'running', got '%s'", run.Status)
		}
		if run.Attempt != 2 {
			t.Errorf("expected attempt 2, got %d", run.Attempt)
		}
	})

	t.Run("sends from_step when specified", func(t *testing.T) {
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
			w.Write([]byte(`{"id":"run-1","status":"running","attempt":2,"max_attempts":3,"created_at":"2025-01-01T00:00:00Z","updated_at":"2025-01-01T00:00:00Z"}`))
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
		_, err := client.RetryRun(ctx, "run-1", "step-failed-abc")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["id"] != "run-1" {
			t.Errorf("expected id 'run-1', got %v", receivedBody["id"])
		}
		if receivedBody["from_step"] != "step-failed-abc" {
			t.Errorf("expected from_step 'step-failed-abc', got %v", receivedBody["from_step"])
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"code":"INVALID_STATE","message":"run is not in failed state"}`))
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
		run, err := client.RetryRun(ctx, "run-active", "")

		if err == nil {
			t.Fatal("expected error for 400 response")
		}

		if run != nil {
			t.Errorf("expected nil run on error, got %v", run)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "INVALID_STATE" {
			t.Errorf("expected code 'INVALID_STATE', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"database error"}`))
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
		_, err := client.RetryRun(ctx, "run-1", "")

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
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"id":"run-1","status":"running","attempt":2,"max_attempts":3,"created_at":"2025-01-01T00:00:00Z","updated_at":"2025-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "retry-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.RetryRun(ctx, "run-1", "")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer retry-key" {
			t.Errorf("expected 'Bearer retry-key', got '%s'", receivedAuth)
		}
	})
}

// ============================================================================
// AppendStreamEvent tests
// ============================================================================

func TestAppendStreamEvent(t *testing.T) {
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
			w.Write([]byte(`{"entityVersion":"3","eventId":"evt-stream-001"}`))
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
		result, err := client.AppendStreamEvent(ctx, "order-123", AppendEventInput{
			Name:       "item.added",
			Data:       map[string]any{"sku": "WIDGET-1", "qty": 2},
			EntityType: "order",
		})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.EntityStreamService/AppendEvent" {
			t.Errorf("expected path /ironflow.v1.EntityStreamService/AppendEvent, got %s", receivedPath)
		}

		if receivedBody["entity_id"] != "order-123" {
			t.Errorf("expected entity_id 'order-123', got %v", receivedBody["entity_id"])
		}
		if receivedBody["entity_type"] != "order" {
			t.Errorf("expected entity_type 'order', got %v", receivedBody["entity_type"])
		}
		if receivedBody["event_name"] != "item.added" {
			t.Errorf("expected event_name 'item.added', got %v", receivedBody["event_name"])
		}

		dataMap, ok := receivedBody["data"].(map[string]any)
		if !ok {
			t.Fatalf("expected data to be a map, got %T", receivedBody["data"])
		}
		if dataMap["sku"] != "WIDGET-1" {
			t.Errorf("expected data.sku 'WIDGET-1', got %v", dataMap["sku"])
		}
		if dataMap["qty"] != float64(2) {
			t.Errorf("expected data.qty 2, got %v", dataMap["qty"])
		}

		// Default expected_version is -1 (skip check)
		if receivedBody["expected_version"] != float64(-1) {
			t.Errorf("expected expected_version -1, got %v", receivedBody["expected_version"])
		}

		// Default version is 1
		if receivedBody["version"] != float64(1) {
			t.Errorf("expected version 1, got %v", receivedBody["version"])
		}

		if result.EntityVersion != 3 {
			t.Errorf("expected entity_version 3, got %d", result.EntityVersion)
		}
		if result.EventID != "evt-stream-001" {
			t.Errorf("expected event_id 'evt-stream-001', got '%s'", result.EventID)
		}
	})

	t.Run("sends optional fields with append options", func(t *testing.T) {
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
			w.Write([]byte(`{"entityVersion":"6","eventId":"evt-stream-002"}`))
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
		_, err := client.AppendStreamEvent(ctx, "order-456", AppendEventInput{
			Name:       "order.shipped",
			Data:       map[string]any{"carrier": "ups"},
			EntityType: "order",
		},
			WithExpectedVersion(5),
			WithAppendIdempotencyKey("idem-append-1"),
			WithEventVersion(2),
		)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["expected_version"] != float64(5) {
			t.Errorf("expected expected_version 5, got %v", receivedBody["expected_version"])
		}
		if receivedBody["idempotency_key"] != "idem-append-1" {
			t.Errorf("expected idempotency_key 'idem-append-1', got %v", receivedBody["idempotency_key"])
		}
		if receivedBody["version"] != float64(2) {
			t.Errorf("expected version 2, got %v", receivedBody["version"])
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"code":"VERSION_CONFLICT","message":"expected version 5, got 6"}`))
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
		result, err := client.AppendStreamEvent(ctx, "order-123", AppendEventInput{
			Name:       "item.added",
			Data:       map[string]any{},
			EntityType: "order",
		}, WithExpectedVersion(5))

		if err == nil {
			t.Fatal("expected error for 409 response")
		}

		if result != nil {
			t.Errorf("expected nil result on error, got %v", result)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "VERSION_CONFLICT" {
			t.Errorf("expected code 'VERSION_CONFLICT', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"storage failure"}`))
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
		_, err := client.AppendStreamEvent(ctx, "order-123", AppendEventInput{
			Name:       "item.added",
			Data:       map[string]any{},
			EntityType: "order",
		})

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
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"entityVersion":"1","eventId":"evt-1"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "stream-api-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.AppendStreamEvent(ctx, "entity-1", AppendEventInput{
			Name:       "test.event",
			Data:       map[string]any{},
			EntityType: "test",
		})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer stream-api-key" {
			t.Errorf("expected 'Bearer stream-api-key', got '%s'", receivedAuth)
		}
	})

	t.Run("sends metadata in body when WithAppendMetadata is set", func(t *testing.T) {
		var receivedBody map[string]any

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &receivedBody)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"entityVersion":"1","eventId":"evt-meta-001"}`))
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
		_, err := client.AppendStreamEvent(ctx, "order-123", AppendEventInput{
			Name:       "order.placed",
			Data:       map[string]any{"total": 99.99},
			EntityType: "order",
		},
			WithAppendMetadata(map[string]any{
				"causationId":   "cmd-abc",
				"correlationId": "corr-xyz",
				"tenantId":      "tenant-42",
			}),
		)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		meta, ok := receivedBody["metadata"].(map[string]any)
		if !ok {
			t.Fatalf("expected metadata to be a map, got %T", receivedBody["metadata"])
		}
		if meta["causationId"] != "cmd-abc" {
			t.Errorf("expected metadata.causationId 'cmd-abc', got %v", meta["causationId"])
		}
		if meta["correlationId"] != "corr-xyz" {
			t.Errorf("expected metadata.correlationId 'corr-xyz', got %v", meta["correlationId"])
		}
		if meta["tenantId"] != "tenant-42" {
			t.Errorf("expected metadata.tenantId 'tenant-42', got %v", meta["tenantId"])
		}
	})

	t.Run("omits metadata when not set", func(t *testing.T) {
		var receivedBody map[string]any

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &receivedBody)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"entityVersion":"1","eventId":"evt-nometa-001"}`))
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
		_, err := client.AppendStreamEvent(ctx, "order-789", AppendEventInput{
			Name:       "order.placed",
			Data:       map[string]any{},
			EntityType: "order",
		})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if _, exists := receivedBody["metadata"]; exists {
			t.Errorf("expected metadata to be omitted, got %v", receivedBody["metadata"])
		}
	})
}

// ============================================================================
// ReadStream tests
// ============================================================================

func TestReadStream(t *testing.T) {
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

			resp := `{
				"events": [
					{
						"id": "evt-001",
						"name": "item.added",
						"data": {"sku": "WIDGET-1", "qty": 2},
						"entityVersion": "1",
						"version": 1,
						"timestamp": "2025-01-01T00:00:00Z"
					},
					{
						"id": "evt-002",
						"name": "item.added",
						"data": {"sku": "GADGET-2", "qty": 1},
						"entityVersion": "2",
						"version": 1,
						"timestamp": "2025-01-01T00:01:00Z",
						"source": "sdk",
						"metadata": {"user": "admin"}
					}
				],
				"totalCount": 2
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
		events, err := client.ReadStream(ctx, "order-123")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.EntityStreamService/ReadStream" {
			t.Errorf("expected path /ironflow.v1.EntityStreamService/ReadStream, got %s", receivedPath)
		}

		if receivedBody["entity_id"] != "order-123" {
			t.Errorf("expected entity_id 'order-123', got %v", receivedBody["entity_id"])
		}

		if len(events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(events))
		}

		if events[0].ID != "evt-001" {
			t.Errorf("expected first event ID 'evt-001', got '%s'", events[0].ID)
		}
		if events[0].Name != "item.added" {
			t.Errorf("expected first event name 'item.added', got '%s'", events[0].Name)
		}
		if events[0].Data["sku"] != "WIDGET-1" {
			t.Errorf("expected first event data.sku 'WIDGET-1', got %v", events[0].Data["sku"])
		}
		if events[0].EntityVersion != 1 {
			t.Errorf("expected first event entity_version 1, got %d", events[0].EntityVersion)
		}
		if events[0].Version != 1 {
			t.Errorf("expected first event version 1, got %d", events[0].Version)
		}
		if events[0].Timestamp != "2025-01-01T00:00:00Z" {
			t.Errorf("expected first event timestamp '2025-01-01T00:00:00Z', got '%s'", events[0].Timestamp)
		}

		if events[1].ID != "evt-002" {
			t.Errorf("expected second event ID 'evt-002', got '%s'", events[1].ID)
		}
		if events[1].Source != "sdk" {
			t.Errorf("expected second event source 'sdk', got '%s'", events[1].Source)
		}
		if events[1].Metadata["user"] != "admin" {
			t.Errorf("expected second event metadata.user 'admin', got %v", events[1].Metadata["user"])
		}
	})

	t.Run("sends optional read options", func(t *testing.T) {
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
			w.Write([]byte(`{"events":[],"totalCount":0}`))
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
		_, err := client.ReadStream(ctx, "order-789", ReadStreamOpts{
			FromVersion: 5,
			Limit:       10,
			Direction:   "backward",
		})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["entity_id"] != "order-789" {
			t.Errorf("expected entity_id 'order-789', got %v", receivedBody["entity_id"])
		}
		if receivedBody["from_version"] != float64(5) {
			t.Errorf("expected from_version 5, got %v", receivedBody["from_version"])
		}
		if receivedBody["limit"] != float64(10) {
			t.Errorf("expected limit 10, got %v", receivedBody["limit"])
		}
		if receivedBody["direction"] != "backward" {
			t.Errorf("expected direction 'backward', got %v", receivedBody["direction"])
		}
	})

	t.Run("returns empty slice for empty stream", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"events":[],"totalCount":0}`))
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
		events, err := client.ReadStream(ctx, "empty-entity")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if len(events) != 0 {
			t.Errorf("expected 0 events, got %d", len(events))
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"code":"NOT_FOUND","message":"entity not found"}`))
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
		events, err := client.ReadStream(ctx, "nonexistent")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}

		if events != nil {
			t.Errorf("expected nil events on error, got %v", events)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "NOT_FOUND" {
			t.Errorf("expected code 'NOT_FOUND', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"storage failure"}`))
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
		_, err := client.ReadStream(ctx, "order-123")

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
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"events":[],"totalCount":0}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "read-stream-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ReadStream(ctx, "entity-1")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer read-stream-key" {
			t.Errorf("expected 'Bearer read-stream-key', got '%s'", receivedAuth)
		}
	})
}

// ============================================================================
// GetStreamInfo tests
// ============================================================================

func TestGetStreamInfo(t *testing.T) {
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

			resp := `{
				"entityId": "order-123",
				"entityType": "order",
				"version": "7",
				"eventCount": "7",
				"createdAt": "2025-01-01T00:00:00Z",
				"updatedAt": "2025-01-01T12:00:00Z"
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
		info, err := client.GetStreamInfo(ctx, "order-123")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.EntityStreamService/GetStreamInfo" {
			t.Errorf("expected path /ironflow.v1.EntityStreamService/GetStreamInfo, got %s", receivedPath)
		}

		if receivedBody["entity_id"] != "order-123" {
			t.Errorf("expected entity_id 'order-123', got %v", receivedBody["entity_id"])
		}

		if info.EntityID != "order-123" {
			t.Errorf("expected entity_id 'order-123', got '%s'", info.EntityID)
		}
		if info.EntityType != "order" {
			t.Errorf("expected entity_type 'order', got '%s'", info.EntityType)
		}
		if info.Version != 7 {
			t.Errorf("expected version 7, got %d", info.Version)
		}
		if info.EventCount != 7 {
			t.Errorf("expected event_count 7, got %d", info.EventCount)
		}
		if info.CreatedAt != "2025-01-01T00:00:00Z" {
			t.Errorf("expected created_at '2025-01-01T00:00:00Z', got '%s'", info.CreatedAt)
		}
		if info.UpdatedAt != "2025-01-01T12:00:00Z" {
			t.Errorf("expected updated_at '2025-01-01T12:00:00Z', got '%s'", info.UpdatedAt)
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"code":"NOT_FOUND","message":"entity stream not found"}`))
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
		info, err := client.GetStreamInfo(ctx, "nonexistent")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}

		if info != nil {
			t.Errorf("expected nil info on error, got %v", info)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "NOT_FOUND" {
			t.Errorf("expected code 'NOT_FOUND', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"database error"}`))
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
		_, err := client.GetStreamInfo(ctx, "order-123")

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
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"entityId":"e1","entityType":"t","version":"1","eventCount":"1","createdAt":"2025-01-01T00:00:00Z","updatedAt":"2025-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "stream-info-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.GetStreamInfo(ctx, "e1")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer stream-info-key" {
			t.Errorf("expected 'Bearer stream-info-key', got '%s'", receivedAuth)
		}
	})
}

// ============================================================================
// CreateConsumerGroup tests
// ============================================================================

func TestCreateConsumerGroup(t *testing.T) {
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

			resp := `{
				"id": "cg-001",
				"namespace": "default",
				"name": "order-processors",
				"pattern": "order.*",
				"filterExpr": "",
				"ackMode": "ACK_MODE_MANUAL",
				"backpressure": "BACKPRESSURE_MODE_BUFFER",
				"maxInflight": 50,
				"maxRedeliveries": 3,
				"redeliverDelayMs": 5000,
				"metadata": {},
				"status": "CONSUMER_GROUP_STATUS_ACTIVE",
				"memberCount": 0,
				"createdAt": "2025-06-01T00:00:00Z",
				"updatedAt": "2025-06-01T00:00:00Z"
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
		group, err := client.CreateConsumerGroup(ctx, ConsumerGroupConfig{
			Name:        "order-processors",
			Pattern:     "order.*",
			AckMode:     AckModeManual,
			MaxInflight: 50,
		})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.PubSubService/CreateConsumerGroup" {
			t.Errorf("expected path /ironflow.v1.PubSubService/CreateConsumerGroup, got %s", receivedPath)
		}

		if receivedBody["name"] != "order-processors" {
			t.Errorf("expected name 'order-processors', got %v", receivedBody["name"])
		}
		if receivedBody["pattern"] != "order.*" {
			t.Errorf("expected pattern 'order.*', got %v", receivedBody["pattern"])
		}
		if receivedBody["namespace"] != "default" {
			t.Errorf("expected namespace 'default', got %v", receivedBody["namespace"])
		}
		if receivedBody["ack_mode"] != "ACK_MODE_MANUAL" {
			t.Errorf("expected ack_mode 'ACK_MODE_MANUAL', got %v", receivedBody["ack_mode"])
		}
		if receivedBody["max_inflight"] != float64(50) {
			t.Errorf("expected max_inflight 50, got %v", receivedBody["max_inflight"])
		}

		if group.ID != "cg-001" {
			t.Errorf("expected ID 'cg-001', got '%s'", group.ID)
		}
		if group.Name != "order-processors" {
			t.Errorf("expected name 'order-processors', got '%s'", group.Name)
		}
		if group.Pattern != "order.*" {
			t.Errorf("expected pattern 'order.*', got '%s'", group.Pattern)
		}
		if group.AckMode != AckModeManual {
			t.Errorf("expected ack_mode 'manual', got '%s'", group.AckMode)
		}
		if group.Backpressure != BackpressureBuffer {
			t.Errorf("expected backpressure 'buffer', got '%s'", group.Backpressure)
		}
		if group.MaxInflight != 50 {
			t.Errorf("expected max_inflight 50, got %d", group.MaxInflight)
		}
		if group.Status != ConsumerGroupStatusActive {
			t.Errorf("expected status 'active', got '%s'", group.Status)
		}
		if group.MemberCount != 0 {
			t.Errorf("expected member_count 0, got %d", group.MemberCount)
		}
		if group.CreatedAt.IsZero() {
			t.Error("expected created_at to be non-zero")
		}
		if group.UpdatedAt.IsZero() {
			t.Error("expected updated_at to be non-zero")
		}
	})

	t.Run("sends all optional config fields", func(t *testing.T) {
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
			w.Write([]byte(`{"id":"cg-002","namespace":"prod","name":"full-config","pattern":"events.*","ackMode":"ACK_MODE_AUTO","backpressure":"BACKPRESSURE_MODE_DROP","maxInflight":25,"maxRedeliveries":5,"redeliverDelayMs":10000,"status":"CONSUMER_GROUP_STATUS_ACTIVE","memberCount":0,"createdAt":"2025-06-01T00:00:00Z","updatedAt":"2025-06-01T00:00:00Z"}`))
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
		_, err := client.CreateConsumerGroup(ctx, ConsumerGroupConfig{
			Name:             "full-config",
			Pattern:          "events.*",
			Namespace:        "prod",
			FilterExpr:       "event.data.priority == 'high'",
			AckMode:          AckModeAuto,
			Backpressure:     BackpressureDrop,
			MaxInflight:      25,
			MaxRedeliveries:  5,
			RedeliverDelayMs: 10000,
			Metadata:         map[string]any{"team": "payments"},
		})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["namespace"] != "prod" {
			t.Errorf("expected namespace 'prod', got %v", receivedBody["namespace"])
		}
		if receivedBody["filter_expr"] != "event.data.priority == 'high'" {
			t.Errorf("expected filter_expr, got %v", receivedBody["filter_expr"])
		}
		if receivedBody["ack_mode"] != "ACK_MODE_AUTO" {
			t.Errorf("expected ack_mode 'ACK_MODE_AUTO', got %v", receivedBody["ack_mode"])
		}
		if receivedBody["backpressure"] != "BACKPRESSURE_MODE_DROP" {
			t.Errorf("expected backpressure 'BACKPRESSURE_MODE_DROP', got %v", receivedBody["backpressure"])
		}
		if receivedBody["max_inflight"] != float64(25) {
			t.Errorf("expected max_inflight 25, got %v", receivedBody["max_inflight"])
		}
		if receivedBody["max_redeliveries"] != float64(5) {
			t.Errorf("expected max_redeliveries 5, got %v", receivedBody["max_redeliveries"])
		}
		if receivedBody["redeliver_delay_ms"] != float64(10000) {
			t.Errorf("expected redeliver_delay_ms 10000, got %v", receivedBody["redeliver_delay_ms"])
		}

		metaMap, ok := receivedBody["metadata"].(map[string]any)
		if !ok {
			t.Fatalf("expected metadata to be a map, got %T", receivedBody["metadata"])
		}
		if metaMap["team"] != "payments" {
			t.Errorf("expected metadata.team 'payments', got %v", metaMap["team"])
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"code":"ALREADY_EXISTS","message":"consumer group already exists"}`))
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
		group, err := client.CreateConsumerGroup(ctx, ConsumerGroupConfig{
			Name:    "existing-group",
			Pattern: "events.*",
		})

		if err == nil {
			t.Fatal("expected error for 409 response")
		}

		if group != nil {
			t.Errorf("expected nil group on error, got %v", group)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "ALREADY_EXISTS" {
			t.Errorf("expected code 'ALREADY_EXISTS', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"database error"}`))
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
		_, err := client.CreateConsumerGroup(ctx, ConsumerGroupConfig{
			Name:    "test-group",
			Pattern: "events.*",
		})

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
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"id":"cg-1","name":"g","pattern":"*","ackMode":"ACK_MODE_AUTO","backpressure":"BACKPRESSURE_MODE_BUFFER","status":"CONSUMER_GROUP_STATUS_ACTIVE","memberCount":0,"createdAt":"2025-01-01T00:00:00Z","updatedAt":"2025-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "cg-create-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.CreateConsumerGroup(ctx, ConsumerGroupConfig{
			Name:    "g",
			Pattern: "*",
		})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer cg-create-key" {
			t.Errorf("expected 'Bearer cg-create-key', got '%s'", receivedAuth)
		}
	})
}

// ============================================================================
// GetConsumerGroup tests
// ============================================================================

func TestGetConsumerGroup(t *testing.T) {
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

			resp := `{
				"id": "cg-001",
				"namespace": "default",
				"name": "order-processors",
				"pattern": "order.*",
				"filterExpr": "event.data.total > 100",
				"ackMode": "ACK_MODE_MANUAL",
				"backpressure": "BACKPRESSURE_MODE_BLOCK",
				"maxInflight": 50,
				"maxRedeliveries": 3,
				"redeliverDelayMs": 5000,
				"metadata": {"team": "orders"},
				"status": "CONSUMER_GROUP_STATUS_ACTIVE",
				"memberCount": 3,
				"createdAt": "2025-06-01T00:00:00Z",
				"updatedAt": "2025-06-01T12:00:00Z"
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
		group, err := client.GetConsumerGroup(ctx, "order-processors")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.PubSubService/GetConsumerGroup" {
			t.Errorf("expected path /ironflow.v1.PubSubService/GetConsumerGroup, got %s", receivedPath)
		}

		if receivedBody["name"] != "order-processors" {
			t.Errorf("expected name 'order-processors', got %v", receivedBody["name"])
		}
		if receivedBody["namespace"] != "default" {
			t.Errorf("expected namespace 'default', got %v", receivedBody["namespace"])
		}

		if group.ID != "cg-001" {
			t.Errorf("expected ID 'cg-001', got '%s'", group.ID)
		}
		if group.Name != "order-processors" {
			t.Errorf("expected name 'order-processors', got '%s'", group.Name)
		}
		if group.Pattern != "order.*" {
			t.Errorf("expected pattern 'order.*', got '%s'", group.Pattern)
		}
		if group.FilterExpr != "event.data.total > 100" {
			t.Errorf("expected filter_expr 'event.data.total > 100', got '%s'", group.FilterExpr)
		}
		if group.AckMode != AckModeManual {
			t.Errorf("expected ack_mode 'manual', got '%s'", group.AckMode)
		}
		if group.Backpressure != BackpressureBlock {
			t.Errorf("expected backpressure 'block', got '%s'", group.Backpressure)
		}
		if group.MaxInflight != 50 {
			t.Errorf("expected max_inflight 50, got %d", group.MaxInflight)
		}
		if group.MaxRedeliveries != 3 {
			t.Errorf("expected max_redeliveries 3, got %d", group.MaxRedeliveries)
		}
		if group.RedeliverDelayMs != 5000 {
			t.Errorf("expected redeliver_delay_ms 5000, got %d", group.RedeliverDelayMs)
		}
		if group.Status != ConsumerGroupStatusActive {
			t.Errorf("expected status 'active', got '%s'", group.Status)
		}
		if group.MemberCount != 3 {
			t.Errorf("expected member_count 3, got %d", group.MemberCount)
		}

		metaMap := group.Metadata
		if metaMap == nil {
			t.Fatal("expected metadata to be non-nil")
		}
		if metaMap["team"] != "orders" {
			t.Errorf("expected metadata.team 'orders', got %v", metaMap["team"])
		}
	})

	t.Run("sends custom namespace via option", func(t *testing.T) {
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
			w.Write([]byte(`{"id":"cg-1","namespace":"prod","name":"g","pattern":"*","ackMode":"ACK_MODE_AUTO","backpressure":"BACKPRESSURE_MODE_BUFFER","status":"CONSUMER_GROUP_STATUS_ACTIVE","memberCount":0,"createdAt":"2025-01-01T00:00:00Z","updatedAt":"2025-01-01T00:00:00Z"}`))
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
		_, err := client.GetConsumerGroup(ctx, "g", WithConsumerGroupNamespace("prod"))

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["namespace"] != "prod" {
			t.Errorf("expected namespace 'prod', got %v", receivedBody["namespace"])
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"code":"NOT_FOUND","message":"consumer group not found"}`))
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
		group, err := client.GetConsumerGroup(ctx, "nonexistent")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}

		if group != nil {
			t.Errorf("expected nil group on error, got %v", group)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "NOT_FOUND" {
			t.Errorf("expected code 'NOT_FOUND', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"database error"}`))
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
		_, err := client.GetConsumerGroup(ctx, "some-group")

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
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"id":"cg-1","name":"g","pattern":"*","ackMode":"ACK_MODE_AUTO","backpressure":"BACKPRESSURE_MODE_BUFFER","status":"CONSUMER_GROUP_STATUS_ACTIVE","memberCount":0,"createdAt":"2025-01-01T00:00:00Z","updatedAt":"2025-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "cg-get-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.GetConsumerGroup(ctx, "g")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer cg-get-key" {
			t.Errorf("expected 'Bearer cg-get-key', got '%s'", receivedAuth)
		}
	})
}

// ============================================================================
// ListConsumerGroups tests
// ============================================================================

func TestListConsumerGroups(t *testing.T) {
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

			resp := `{
				"groups": [
					{
						"id": "cg-001",
						"namespace": "default",
						"name": "order-processors",
						"pattern": "order.*",
						"ackMode": "ACK_MODE_MANUAL",
						"backpressure": "BACKPRESSURE_MODE_BUFFER",
						"maxInflight": 50,
						"maxRedeliveries": 3,
						"redeliverDelayMs": 5000,
						"status": "CONSUMER_GROUP_STATUS_ACTIVE",
						"memberCount": 3,
						"createdAt": "2025-06-01T00:00:00Z",
						"updatedAt": "2025-06-01T12:00:00Z"
					},
					{
						"id": "cg-002",
						"namespace": "default",
						"name": "payment-processors",
						"pattern": "payment.*",
						"ackMode": "ACK_MODE_AUTO",
						"backpressure": "BACKPRESSURE_MODE_DROP",
						"maxInflight": 100,
						"maxRedeliveries": 5,
						"redeliverDelayMs": 10000,
						"status": "CONSUMER_GROUP_STATUS_PAUSED",
						"memberCount": 0,
						"createdAt": "2025-06-02T00:00:00Z",
						"updatedAt": "2025-06-02T12:00:00Z"
					}
				]
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
		groups, err := client.ListConsumerGroups(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.PubSubService/ListConsumerGroups" {
			t.Errorf("expected path /ironflow.v1.PubSubService/ListConsumerGroups, got %s", receivedPath)
		}

		if receivedBody["limit"] != float64(100) {
			t.Errorf("expected limit 100, got %v", receivedBody["limit"])
		}

		if len(groups) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(groups))
		}

		if groups[0].ID != "cg-001" {
			t.Errorf("expected first group ID 'cg-001', got '%s'", groups[0].ID)
		}
		if groups[0].Name != "order-processors" {
			t.Errorf("expected first group name 'order-processors', got '%s'", groups[0].Name)
		}
		if groups[0].AckMode != AckModeManual {
			t.Errorf("expected first group ack_mode 'manual', got '%s'", groups[0].AckMode)
		}
		if groups[0].Status != ConsumerGroupStatusActive {
			t.Errorf("expected first group status 'active', got '%s'", groups[0].Status)
		}
		if groups[0].MemberCount != 3 {
			t.Errorf("expected first group member_count 3, got %d", groups[0].MemberCount)
		}

		if groups[1].ID != "cg-002" {
			t.Errorf("expected second group ID 'cg-002', got '%s'", groups[1].ID)
		}
		if groups[1].Name != "payment-processors" {
			t.Errorf("expected second group name 'payment-processors', got '%s'", groups[1].Name)
		}
		if groups[1].AckMode != AckModeAuto {
			t.Errorf("expected second group ack_mode 'auto', got '%s'", groups[1].AckMode)
		}
		if groups[1].Backpressure != BackpressureDrop {
			t.Errorf("expected second group backpressure 'drop', got '%s'", groups[1].Backpressure)
		}
		if groups[1].Status != ConsumerGroupStatusPaused {
			t.Errorf("expected second group status 'paused', got '%s'", groups[1].Status)
		}
	})

	t.Run("sends custom namespace via option", func(t *testing.T) {
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
			w.Write([]byte(`{"groups":[]}`))
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
		_, err := client.ListConsumerGroups(ctx, WithConsumerGroupNamespace("staging"))

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["namespace"] != "staging" {
			t.Errorf("expected namespace 'staging', got %v", receivedBody["namespace"])
		}
	})

	t.Run("returns empty slice for empty response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"groups":[]}`))
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
		groups, err := client.ListConsumerGroups(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if len(groups) != 0 {
			t.Errorf("expected 0 groups, got %d", len(groups))
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"database error"}`))
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
		groups, err := client.ListConsumerGroups(ctx)

		if err == nil {
			t.Fatal("expected error for 500 response")
		}

		if groups != nil {
			t.Errorf("expected nil groups on error, got %v", groups)
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "INTERNAL" {
			t.Errorf("expected code 'INTERNAL', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"groups":[]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "cg-list-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListConsumerGroups(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer cg-list-key" {
			t.Errorf("expected 'Bearer cg-list-key', got '%s'", receivedAuth)
		}
	})
}

// ============================================================================
// DeleteConsumerGroup tests
// ============================================================================

func TestDeleteConsumerGroup(t *testing.T) {
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
		err := client.DeleteConsumerGroup(ctx, "order-processors")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.PubSubService/DeleteConsumerGroup" {
			t.Errorf("expected path /ironflow.v1.PubSubService/DeleteConsumerGroup, got %s", receivedPath)
		}

		if receivedBody["name"] != "order-processors" {
			t.Errorf("expected name 'order-processors', got %v", receivedBody["name"])
		}
		if receivedBody["namespace"] != "default" {
			t.Errorf("expected namespace 'default', got %v", receivedBody["namespace"])
		}
	})

	t.Run("sends custom namespace via option", func(t *testing.T) {
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
		err := client.DeleteConsumerGroup(ctx, "my-group", WithConsumerGroupNamespace("prod"))

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["namespace"] != "prod" {
			t.Errorf("expected namespace 'prod', got %v", receivedBody["namespace"])
		}
		if receivedBody["name"] != "my-group" {
			t.Errorf("expected name 'my-group', got %v", receivedBody["name"])
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"code":"NOT_FOUND","message":"consumer group not found"}`))
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
		err := client.DeleteConsumerGroup(ctx, "nonexistent")

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

	t.Run("returns error on server error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"INTERNAL","message":"database error"}`))
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
		err := client.DeleteConsumerGroup(ctx, "some-group")

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
	})

	t.Run("sets auth header when apiKey is configured", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "cg-delete-key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		err := client.DeleteConsumerGroup(ctx, "g")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedAuth != "Bearer cg-delete-key" {
			t.Errorf("expected 'Bearer cg-delete-key', got '%s'", receivedAuth)
		}
	})
}

// ============================================================================
// Health tests
// ============================================================================

func TestHealth(t *testing.T) {
	t.Run("sends correct method path and body", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
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
		status, err := client.Health(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/ironflow.v1.IronflowService/Health" {
			t.Errorf("expected path /ironflow.v1.IronflowService/Health, got %s", receivedPath)
		}

		if status != "ok" {
			t.Errorf("expected status 'ok', got '%s'", status)
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"code":"UNAVAILABLE","message":"server not ready"}`))
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
		_, err := client.Health(ctx)

		if err == nil {
			t.Fatal("expected error for 503 response")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "UNAVAILABLE" {
			t.Errorf("expected code 'UNAVAILABLE', got '%s'", ironflowErr.Code)
		}
	})
}

// ============================================================================
// ListStreams tests
// ============================================================================

func TestListStreams(t *testing.T) {
	t.Run("returns streams list", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
			// Server returns snake_case fields from store.StreamInfo.
			w.Write([]byte(`{"streams":[{"entity_id":"order-123","entity_type":"order","version":5,"event_count":10,"created_at":"2025-12-01T00:00:00Z"},{"entity_id":"order-456","entity_type":"order","version":2,"event_count":3,"created_at":"2025-12-15T00:00:00Z"}]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		streams, err := client.ListStreams(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if receivedMethod != "GET" {
			t.Errorf("expected method GET, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/streams" {
			t.Errorf("expected path /api/v1/streams, got %s", receivedPath)
		}
		if len(streams) != 2 {
			t.Fatalf("expected 2 streams, got %d", len(streams))
		}
		if streams[0].EntityID != "order-123" {
			t.Errorf("expected entity_id 'order-123', got '%s'", streams[0].EntityID)
		}
		if streams[0].EntityType != "order" {
			t.Errorf("expected entity_type 'order', got '%s'", streams[0].EntityType)
		}
		if streams[0].Version != 5 {
			t.Errorf("expected version 5, got %d", streams[0].Version)
		}
		if streams[0].EventCount != 10 {
			t.Errorf("expected event_count 10, got %d", streams[0].EventCount)
		}
		if streams[1].EntityID != "order-456" {
			t.Errorf("expected entity_id 'order-456', got '%s'", streams[1].EntityID)
		}
	})

	t.Run("returns empty list when no streams", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"streams":[]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		streams, err := client.ListStreams(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(streams) != 0 {
			t.Errorf("expected empty slice, got %d items", len(streams))
		}
	})

	t.Run("returns empty list when streams field is null", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		streams, err := client.ListStreams(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if streams == nil {
			t.Error("expected non-nil empty slice, got nil")
		}
		if len(streams) != 0 {
			t.Errorf("expected empty slice, got %d items", len(streams))
		}
	})

	t.Run("returns error on non-200 status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error":"store unavailable"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.ListStreams(ctx)

		if err == nil {
			t.Fatal("expected error for 500 response")
		}
		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		// restRequest formats error codes as HTTP_<status>
		if ironflowErr.Code != "HTTP_500" {
			t.Errorf("expected code 'HTTP_500', got '%s'", ironflowErr.Code)
		}
	})
}

// ============================================================================
// GetEntityHistory tests
// ============================================================================

func TestGetEntityHistory(t *testing.T) {
	t.Run("returns event history with correct path encoding", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
			// Server returns snake_case fields wrapped in "entries".
			w.Write([]byte(`{"entries":[{"event_name":"item.added","event_data":{"sku":"WIDGET-1","qty":2},"entity_version":1,"timestamp":"2026-01-01T00:00:00Z"},{"event_name":"item.shipped","event_data":{"trackingId":"TR-123"},"entity_version":2,"timestamp":"2026-01-02T00:00:00Z","metadata":{"source":"warehouse"}}]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		events, err := client.GetEntityHistory(ctx, "order-123")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if receivedMethod != "GET" {
			t.Errorf("expected method GET, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/streams/order-123/history" {
			t.Errorf("expected path /api/v1/streams/order-123/history, got %s", receivedPath)
		}
		if len(events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(events))
		}
		if events[0].EventName != "item.added" {
			t.Errorf("expected event_name 'item.added', got '%s'", events[0].EventName)
		}
		if events[0].EntityVersion != 1 {
			t.Errorf("expected entity_version 1, got %d", events[0].EntityVersion)
		}
		if events[1].EventName != "item.shipped" {
			t.Errorf("expected event_name 'item.shipped', got '%s'", events[1].EventName)
		}
		if events[1].Metadata == nil {
			t.Error("expected non-nil metadata for second event")
		}
	})

	t.Run("URL-encodes entity ID with special characters", func(t *testing.T) {
		var receivedURI string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedURI = r.RequestURI
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"entries":[]}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.GetEntityHistory(ctx, "order/with spaces")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		// RequestURI preserves the raw encoded path: "/" → %2F, space → %20
		expected := "/api/v1/streams/order%2Fwith%20spaces/history"
		if receivedURI != expected {
			t.Errorf("expected RequestURI %q, got %q", expected, receivedURI)
		}
	})

	t.Run("returns error on 404 entity not found", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"entity not found"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		events, err := client.GetEntityHistory(ctx, "nonexistent-entity")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}
		if events != nil {
			t.Errorf("expected nil events on error, got %v", events)
		}
		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		// restRequest formats error codes as HTTP_<status>
		if ironflowErr.Code != "HTTP_404" {
			t.Errorf("expected code 'HTTP_404', got '%s'", ironflowErr.Code)
		}
	})
}

// ============================================================================
// CreateSnapshot tests
// ============================================================================

func TestCreateSnapshot(t *testing.T) {
	t.Run("sends correct method, path, and body", func(t *testing.T) {
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
			// Real server returns snake_case fields.
			w.Write([]byte(`{"snapshot_id":"snap-001","entity_id":"order-123","entity_type":"order","entity_version":10,"state":{"status":"shipped","total":99.99},"created_at":"2026-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		input := CreateSnapshotInput{
			EntityType:    "order",
			EntityVersion: 10,
			State:         map[string]any{"status": "shipped", "total": 99.99},
		}
		snapshot, err := client.CreateSnapshot(ctx, "order-123", input)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/streams/order-123/snapshots" {
			t.Errorf("expected path /api/v1/streams/order-123/snapshots, got %s", receivedPath)
		}
		// SDK now sends snake_case fields matching the server's expected format.
		if receivedBody["entity_type"] != "order" {
			t.Errorf("expected entity_type 'order', got %v", receivedBody["entity_type"])
		}
		if receivedBody["entity_version"] != float64(10) {
			t.Errorf("expected entity_version 10, got %v", receivedBody["entity_version"])
		}
		if snapshot == nil {
			t.Fatal("expected non-nil snapshot")
		}
		if snapshot.SnapshotID != "snap-001" {
			t.Errorf("expected snapshot_id 'snap-001', got '%s'", snapshot.SnapshotID)
		}
		if snapshot.EntityID != "order-123" {
			t.Errorf("expected entity_id 'order-123', got '%s'", snapshot.EntityID)
		}
		if snapshot.EntityVersion != 10 {
			t.Errorf("expected entity_version 10, got %d", snapshot.EntityVersion)
		}
	})

	t.Run("returns error on 404 entity not found", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"entity stream not found"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		snapshot, err := client.CreateSnapshot(ctx, "nonexistent-entity", CreateSnapshotInput{
			EntityType:    "order",
			EntityVersion: 1,
			State:         map[string]any{},
		})

		if err == nil {
			t.Fatal("expected error for 404 response")
		}
		if snapshot != nil {
			t.Errorf("expected nil snapshot on error, got %v", snapshot)
		}
		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		// restRequest formats error codes as HTTP_<status>
		if ironflowErr.Code != "HTTP_404" {
			t.Errorf("expected code 'HTTP_404', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("URL-encodes entity ID in path", func(t *testing.T) {
		var receivedURI string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedURI = r.RequestURI
			w.WriteHeader(http.StatusOK)
			// Real server returns snake_case.
			w.Write([]byte(`{"snapshot_id":"snap-002","entity_id":"org/order-123","entity_type":"order","entity_version":1,"state":{},"created_at":"2026-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.CreateSnapshot(ctx, "org/order-123", CreateSnapshotInput{EntityType: "order", EntityVersion: 1, State: map[string]any{}})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		expected := "/api/v1/streams/org%2Forder-123/snapshots"
		if receivedURI != expected {
			t.Errorf("expected RequestURI %q, got %q", expected, receivedURI)
		}
	})
}

// ============================================================================
// GetSnapshot tests
// ============================================================================

func TestGetSnapshot(t *testing.T) {
	t.Run("returns latest snapshot", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
			// Real server returns snake_case fields.
			w.Write([]byte(`{"snapshot_id":"snap-007","entity_id":"order-123","entity_type":"order","entity_version":15,"state":{"status":"delivered","items":3},"created_at":"2026-01-10T00:00:00Z"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		snapshot, err := client.GetSnapshot(ctx, "order-123")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if receivedMethod != "GET" {
			t.Errorf("expected method GET, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/streams/order-123/snapshots" {
			t.Errorf("expected path /api/v1/streams/order-123/snapshots, got %s", receivedPath)
		}
		if snapshot == nil {
			t.Fatal("expected non-nil snapshot")
		}
		if snapshot.SnapshotID != "snap-007" {
			t.Errorf("expected snapshot_id 'snap-007', got '%s'", snapshot.SnapshotID)
		}
		if snapshot.EntityID != "order-123" {
			t.Errorf("expected entity_id 'order-123', got '%s'", snapshot.EntityID)
		}
		if snapshot.EntityVersion != 15 {
			t.Errorf("expected entity_version 15, got %d", snapshot.EntityVersion)
		}
		if snapshot.CreatedAt != "2026-01-10T00:00:00Z" {
			t.Errorf("expected created_at '2026-01-10T00:00:00Z', got '%s'", snapshot.CreatedAt)
		}
	})

	t.Run("returns error on 404 when no snapshot exists", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"error":"no snapshot found for entity"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		snapshot, err := client.GetSnapshot(ctx, "order-no-snapshot")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}
		if snapshot != nil {
			t.Errorf("expected nil snapshot on error, got %v", snapshot)
		}
		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected IronflowError, got %T", err)
		}
		// restRequest formats error codes as HTTP_<status>
		if ironflowErr.Code != "HTTP_404" {
			t.Errorf("expected code 'HTTP_404', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("URL-encodes entity ID in path", func(t *testing.T) {
		var receivedURI string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedURI = r.RequestURI
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"snapshotId":"snap-003","entityId":"order entity","entityType":"order","entityVersion":1,"state":{},"createdAt":"2026-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()
		_, err := client.GetSnapshot(ctx, "order entity")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		expected := "/api/v1/streams/order%20entity/snapshots"
		if receivedURI != expected {
			t.Errorf("expected RequestURI %q, got %q", expected, receivedURI)
		}
	})

	t.Run("distinguishes GET from POST for same snapshot path as CreateSnapshot", func(t *testing.T) {
		var postMethod string
		var getMethod string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "POST" {
				postMethod = r.Method
				w.WriteHeader(http.StatusOK)
				// Real server returns snake_case fields.
				w.Write([]byte(`{"snapshot_id":"snap-new","entity_id":"order-789","entity_type":"order","entity_version":5,"state":{},"created_at":"2026-01-01T00:00:00Z"}`))
			} else {
				getMethod = r.Method
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"snapshot_id":"snap-existing","entity_id":"order-789","entity_type":"order","entity_version":5,"state":{},"created_at":"2026-01-01T00:00:00Z"}`))
			}
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		ctx := context.Background()

		created, err := client.CreateSnapshot(ctx, "order-789", CreateSnapshotInput{EntityType: "order", EntityVersion: 5, State: map[string]any{}})
		if err != nil {
			t.Fatalf("CreateSnapshot failed: %v", err)
		}
		if postMethod != "POST" {
			t.Errorf("expected POST method for CreateSnapshot, got %s", postMethod)
		}
		if created.SnapshotID != "snap-new" {
			t.Errorf("expected snap-new, got %s", created.SnapshotID)
		}

		retrieved, err := client.GetSnapshot(ctx, "order-789")
		if err != nil {
			t.Fatalf("GetSnapshot failed: %v", err)
		}
		if getMethod != "GET" {
			t.Errorf("expected GET method for GetSnapshot, got %s", getMethod)
		}
		if retrieved.SnapshotID != "snap-existing" {
			t.Errorf("expected snap-existing, got %s", retrieved.SnapshotID)
		}

		_ = time.Now() // suppress unused import if needed
	})
}

// ============================================================================
// Wait-for-projection-catchup tests (#473)
// ============================================================================

func TestWaitForProjection(t *testing.T) {
	t.Run("sends correct path and body, parses response", func(t *testing.T) {
		var receivedPath string
		var receivedBody map[string]any

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &receivedBody)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"caughtUp": true,
				"timedOut": false,
				"currentSeq": 42,
				"targetSeq": 40,
				"behindByEvents": 0,
				"mode": "managed"
			}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:   server.URL,
			httpClient:  &http.Client{},
			retryConfig: &ClientRetryConfig{MaxAttempts: 1},
			logger:      NewNoopLogger(),
		}

		res, err := client.WaitForProjection(context.Background(), "order-detail-view", WaitForProjectionOpts{
			MinSeq:    40,
			Timeout:   2 * time.Second,
			Partition: "order-123",
		})
		if err != nil {
			t.Fatalf("WaitForProjection: %v", err)
		}
		if !res.CaughtUp {
			t.Errorf("expected CaughtUp=true, got %+v", res)
		}
		if res.CurrentSeq != 42 {
			t.Errorf("expected CurrentSeq=42, got %d", res.CurrentSeq)
		}
		if receivedPath != "/ironflow.v1.ProjectionService/WaitProjectionCatchup" {
			t.Errorf("unexpected path: %s", receivedPath)
		}
		if receivedBody["name"] != "order-detail-view" {
			t.Errorf("expected name 'order-detail-view', got %v", receivedBody["name"])
		}
		if receivedBody["partition"] != "order-123" {
			t.Errorf("expected partition 'order-123', got %v", receivedBody["partition"])
		}
		// minSeq is sent as a JSON string (protojson convention for 64-bit
		// ints, avoids JS precision loss for seqs > 2^53).
		if s, _ := receivedBody["minSeq"].(string); s != "40" {
			t.Errorf("expected minSeq \"40\", got %v (%T)", receivedBody["minSeq"], receivedBody["minSeq"])
		}
	})

	t.Run("timedOut response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"caughtUp":false,"timedOut":true,"currentSeq":10,"targetSeq":100,"behindByEvents":90}`))
		}))
		defer server.Close()

		client := &Client{serverURL: server.URL, httpClient: &http.Client{}, retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}
		res, err := client.WaitForProjection(context.Background(), "p1", WaitForProjectionOpts{MinSeq: 100, Timeout: 200 * time.Millisecond})
		if err != nil {
			t.Fatalf("WaitForProjection: %v", err)
		}
		if !res.TimedOut {
			t.Errorf("expected TimedOut=true, got %+v", res)
		}
		if res.BehindByEvents != 90 {
			t.Errorf("expected BehindByEvents=90, got %d", res.BehindByEvents)
		}
	})
}

func TestWaitForProjections_BatchAndPerItemErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"results":[
			{"result":{"caughtUp":true,"timedOut":false,"currentSeq":42,"targetSeq":40}},
			{"error":"projection \"missing\" not found"},
			{"result":{"caughtUp":false,"timedOut":true,"currentSeq":5,"targetSeq":100,"behindByEvents":95}}
		]}`))
	}))
	defer server.Close()

	client := &Client{serverURL: server.URL, httpClient: &http.Client{}, retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}
	results, err := client.WaitForProjections(context.Background(), []WaitItem{
		{Name: "good", MinSeq: 40},
		{Name: "missing", MinSeq: 1},
		{Name: "slow", MinSeq: 100},
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("WaitForProjections: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	if !results[0].Result.CaughtUp {
		t.Errorf("item 0: expected CaughtUp")
	}
	if results[1].Error == "" {
		t.Errorf("item 1: expected per-item error")
	}
	if !results[2].Result.TimedOut {
		t.Errorf("item 2: expected TimedOut")
	}
}

func TestWaitForEvent_ResolvesAndWaits(t *testing.T) {
	var receivedBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"caughtUp":true,"currentSeq":100,"targetSeq":99}`))
	}))
	defer server.Close()

	client := &Client{serverURL: server.URL, httpClient: &http.Client{}, retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}
	res, err := client.WaitForEvent(context.Background(), "evt-abc", "order-detail-view", WaitForProjectionOpts{
		Timeout:   1 * time.Second,
		Partition: "order-123",
	})
	if err != nil {
		t.Fatalf("WaitForEvent: %v", err)
	}
	if !res.CaughtUp {
		t.Errorf("expected CaughtUp, got %+v", res)
	}
	if receivedBody["eventId"] != "evt-abc" {
		t.Errorf("expected eventId 'evt-abc', got %v", receivedBody["eventId"])
	}
	if receivedBody["projection"] != "order-detail-view" {
		t.Errorf("expected projection 'order-detail-view', got %v", receivedBody["projection"])
	}
}
