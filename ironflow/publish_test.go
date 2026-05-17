package ironflow

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPatterns_Topic(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple topic", "order.processed", "topic:order.processed"},
		{"wildcard", "order.*", "topic:order.*"},
		{"deep wildcard", "order.>", "topic:order.>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Patterns.Topic(tt.input)
			if result != tt.expected {
				t.Errorf("Patterns.Topic(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestPatterns_AllTopics(t *testing.T) {
	expected := "topic:>"
	result := Patterns.AllTopics()
	if result != expected {
		t.Errorf("Patterns.AllTopics() = %q, want %q", result, expected)
	}
}

func TestWithPublishIdempotencyKey(t *testing.T) {
	cfg := &publishConfig{}
	WithPublishIdempotencyKey("test-key")(cfg)
	if cfg.idempotencyKey != "test-key" {
		t.Errorf("expected idempotencyKey 'test-key', got %q", cfg.idempotencyKey)
	}
}

func TestPublish_StepName(t *testing.T) {
	// Verify that Publish creates a step named "publish:<topic>"
	// and that the step is recorded even when the HTTP call fails.
	exec := &executionContext{
		runID:          "test-run",
		stepCounters:   make(map[string]int),
		completedSteps: make(map[string]*CompletedStep),
		executedSteps:  make([]*StepResult, 0),
		serverURL:      "http://localhost:1", // unreachable port
	}
	ctx := Context{exec: exec}

	err := Publish(ctx, "order.processed", map[string]any{"orderId": "123"})

	// It will fail because no server is running at that address
	if err == nil {
		t.Error("Expected error because no server is running")
	}

	// Verify step was recorded with correct name
	if len(exec.executedSteps) != 1 {
		t.Fatalf("Expected 1 executed step, got %d", len(exec.executedSteps))
	}
	step := exec.executedSteps[0]
	if step.Name != "publish:order.processed" {
		t.Errorf("Expected step name 'publish:order.processed', got %q", step.Name)
	}
	if step.Status != "failed" {
		t.Errorf("Expected status 'failed', got %q", step.Status)
	}
}

func TestPublish_Memoized(t *testing.T) {
	// If the step was already completed, Publish should return success without calling server
	exec := &executionContext{
		runID:        "test-run",
		stepCounters: make(map[string]int),
		completedSteps: map[string]*CompletedStep{
			"test-run:publish:order.processed:0": {
				ID:     "test-run:publish:order.processed:0",
				Name:   "publish:order.processed",
				Status: "completed",
				Output: map[string]any{"eventId": "evt_123"},
			},
		},
		executedSteps: make([]*StepResult, 0),
	}
	ctx := Context{exec: exec}

	err := Publish(ctx, "order.processed", map[string]any{"orderId": "123"})
	if err != nil {
		t.Errorf("Expected memoized Publish to succeed, got error: %v", err)
	}
}

func TestPublish_MissingServerURL(t *testing.T) {
	exec := &executionContext{
		runID:          "test-run",
		stepCounters:   make(map[string]int),
		completedSteps: make(map[string]*CompletedStep),
		executedSteps:  make([]*StepResult, 0),
		// serverURL intentionally empty
	}
	ctx := Context{exec: exec}

	err := Publish(ctx, "order.processed", map[string]any{"orderId": "123"})
	if err == nil {
		t.Error("Expected error when serverURL is empty")
	}
	if !strings.Contains(err.Error(), "server URL not configured") {
		t.Errorf("Expected 'server URL not configured' error, got: %v", err)
	}
}

func TestPublish_Success(t *testing.T) {
	// Set up a test server that returns a successful publish response
	var receivedTopic string
	var receivedData map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ironflow.v1.PubSubService/Publish" {
			t.Errorf("Expected path '/ironflow.v1.PubSubService/Publish', got %q", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST, got %s", r.Method)
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("Failed to decode request body: %v", err)
		}
		receivedTopic, _ = body["topic"].(string)
		receivedData, _ = body["data"].(map[string]any)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"eventId":  "evt_abc",
			"sequence": "42",
		})
	}))
	defer server.Close()

	exec := &executionContext{
		runID:          "test-run",
		stepCounters:   make(map[string]int),
		completedSteps: make(map[string]*CompletedStep),
		executedSteps:  make([]*StepResult, 0),
		serverURL:      server.URL,
	}
	ctx := Context{exec: exec}

	err := Publish(ctx, "order.processed", map[string]any{"orderId": "123"})
	if err != nil {
		t.Fatalf("Expected Publish to succeed, got error: %v", err)
	}

	// Verify the request was sent correctly
	if receivedTopic != "order.processed" {
		t.Errorf("Expected topic 'order.processed', got %q", receivedTopic)
	}
	if receivedData["orderId"] != "123" {
		t.Errorf("Expected data.orderId '123', got %v", receivedData["orderId"])
	}

	// Verify step was recorded as completed
	if len(exec.executedSteps) != 1 {
		t.Fatalf("Expected 1 executed step, got %d", len(exec.executedSteps))
	}
	step := exec.executedSteps[0]
	if step.Name != "publish:order.processed" {
		t.Errorf("Expected step name 'publish:order.processed', got %q", step.Name)
	}
	if step.Status != "completed" {
		t.Errorf("Expected status 'completed', got %q", step.Status)
	}
}

func TestPublish_WithAPIKey(t *testing.T) {
	// Verify that the Authorization header is sent when apiKey is set
	var receivedAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"eventId": "evt_abc", "sequence": "1"})
	}))
	defer server.Close()

	exec := &executionContext{
		runID:          "test-run",
		stepCounters:   make(map[string]int),
		completedSteps: make(map[string]*CompletedStep),
		executedSteps:  make([]*StepResult, 0),
		serverURL:      server.URL,
		apiKey:         "test-secret-key",
	}
	ctx := Context{exec: exec}

	err := Publish(ctx, "order.processed", map[string]any{"orderId": "123"})
	if err != nil {
		t.Fatalf("Expected Publish to succeed, got error: %v", err)
	}

	if receivedAuth != "Bearer test-secret-key" {
		t.Errorf("Expected Authorization 'Bearer test-secret-key', got %q", receivedAuth)
	}
}

func TestPublish_ServerError(t *testing.T) {
	// Server returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "internal error"}`))
	}))
	defer server.Close()

	exec := &executionContext{
		runID:          "test-run",
		stepCounters:   make(map[string]int),
		completedSteps: make(map[string]*CompletedStep),
		executedSteps:  make([]*StepResult, 0),
		serverURL:      server.URL,
	}
	ctx := Context{exec: exec}

	err := Publish(ctx, "order.processed", map[string]any{"orderId": "123"})
	if err == nil {
		t.Error("Expected error when server returns 500")
	}
	if !strings.Contains(err.Error(), "publish failed") {
		t.Errorf("Expected 'publish failed' in error, got: %v", err)
	}

	// Step should be recorded as failed
	if len(exec.executedSteps) != 1 {
		t.Fatalf("Expected 1 executed step, got %d", len(exec.executedSteps))
	}
	if exec.executedSteps[0].Status != "failed" {
		t.Errorf("Expected status 'failed', got %q", exec.executedSteps[0].Status)
	}
}

// ============================================================================
// Client-level Publish / ListTopics / GetTopicStats tests
// ============================================================================

func newTestClient(t *testing.T, handler http.HandlerFunc) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	client := &Client{
		serverURL:  server.URL,
		httpClient: &http.Client{},
		retryConfig: &ClientRetryConfig{
			MaxAttempts: 1,
		},
		logger: NewNoopLogger(),
	}
	return client, server
}

func TestClientPublish(t *testing.T) {
	t.Run("sends correct request and parses response", func(t *testing.T) {
		var receivedBody map[string]any

		client, server := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/ironflow.v1.PubSubService/Publish" {
				t.Errorf("expected path /ironflow.v1.PubSubService/Publish, got %s", r.URL.Path)
			}
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedBody)

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"eventId":"evt_abc","sequence":"42"}`))
		})
		defer server.Close()

		result, err := client.Publish(context.Background(), "order.processed", map[string]any{"orderId": "123"})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["topic"] != "order.processed" {
			t.Errorf("expected topic 'order.processed', got %v", receivedBody["topic"])
		}
		if result.EventID != "evt_abc" {
			t.Errorf("expected eventId 'evt_abc', got %q", result.EventID)
		}
		if result.Sequence != 42 {
			t.Errorf("expected sequence 42, got %d", result.Sequence)
		}
	})

	t.Run("sends idempotencyKey in camelCase", func(t *testing.T) {
		var receivedBody map[string]any

		client, server := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedBody)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"eventId":"evt_abc","sequence":"1"}`))
		})
		defer server.Close()

		_, err := client.Publish(context.Background(), "test", nil, WithPublishIdempotencyKey("key-123"))
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["idempotencyKey"] != "key-123" {
			t.Errorf("expected idempotencyKey 'key-123', got %v", receivedBody["idempotencyKey"])
		}
	})

	t.Run("returns error on server failure", func(t *testing.T) {
		client, server := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"internal","message":"oops"}`))
		})
		defer server.Close()

		_, err := client.Publish(context.Background(), "test", nil)
		if err == nil {
			t.Fatal("expected error on 500 response")
		}
	})
}

func TestClientListTopics(t *testing.T) {
	t.Run("parses topics list", func(t *testing.T) {
		client, server := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/ironflow.v1.PubSubService/ListTopics" {
				t.Errorf("expected ListTopics path, got %s", r.URL.Path)
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"topics":[{"name":"order.processed","messageCount":10},{"name":"notifications.email","messageCount":5}]}`))
		})
		defer server.Close()

		topics, err := client.ListTopics(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(topics) != 2 {
			t.Fatalf("expected 2 topics, got %d", len(topics))
		}
		if topics[0].Name != "order.processed" {
			t.Errorf("expected first topic 'order.processed', got %q", topics[0].Name)
		}
		if topics[0].MessageCount != 10 {
			t.Errorf("expected messageCount 10, got %d", topics[0].MessageCount)
		}
	})

	t.Run("returns empty slice on null topics", func(t *testing.T) {
		client, server := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{}`))
		})
		defer server.Close()

		topics, err := client.ListTopics(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if topics == nil {
			t.Error("expected non-nil empty slice")
		}
		if len(topics) != 0 {
			t.Errorf("expected 0 topics, got %d", len(topics))
		}
	})
}

func TestClientGetTopicStats(t *testing.T) {
	t.Run("sends topic and parses stats", func(t *testing.T) {
		var receivedBody map[string]any

		client, server := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/ironflow.v1.PubSubService/GetTopicStats" {
				t.Errorf("expected GetTopicStats path, got %s", r.URL.Path)
			}
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &receivedBody)

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"name":"order.processed","messageCount":42,"consumerCount":3,"lag":5,"firstSeq":1,"lastSeq":42}`))
		})
		defer server.Close()

		stats, err := client.GetTopicStats(context.Background(), "order.processed")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["topic"] != "order.processed" {
			t.Errorf("expected topic 'order.processed', got %v", receivedBody["topic"])
		}
		if stats.Name != "order.processed" {
			t.Errorf("expected name 'order.processed', got %q", stats.Name)
		}
		if stats.MessageCount != 42 {
			t.Errorf("expected messageCount 42, got %d", stats.MessageCount)
		}
		if stats.Lag != 5 {
			t.Errorf("expected lag 5, got %d", stats.Lag)
		}
	})
}
