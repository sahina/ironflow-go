package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// ============================================================================
// Test Helpers
// ============================================================================

// mockWSServer creates a mock WebSocket server for testing
type mockWSServer struct {
	server     *httptest.Server
	upgrader   websocket.Upgrader
	mu         sync.Mutex
	writeMu    sync.Mutex // protects websocket writes
	conn       *websocket.Conn
	messages   []json.RawMessage
	onMessage  func([]byte)
	shouldFail bool
}

func newMockWSServer() *mockWSServer {
	m := &mockWSServer{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}

	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.shouldFail {
			http.Error(w, "connection refused", http.StatusInternalServerError)
			return
		}

		conn, err := m.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		m.mu.Lock()
		m.conn = conn
		m.mu.Unlock()

		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}

			m.mu.Lock()
			m.messages = append(m.messages, data)
			handler := m.onMessage
			m.mu.Unlock()

			if handler != nil {
				handler(data)
			}
		}
	}))

	return m
}

func (m *mockWSServer) URL() string {
	return "ws" + strings.TrimPrefix(m.server.URL, "http")
}

func (m *mockWSServer) Close() {
	m.server.Close()
}

func (m *mockWSServer) SendMessage(msg any) error {
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()

	if conn == nil {
		return nil
	}

	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return conn.WriteJSON(msg)
}

func (m *mockWSServer) GetMessages() []json.RawMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]json.RawMessage{}, m.messages...)
}

// ============================================================================
// Pattern Tests
// ============================================================================

func TestPatterns(t *testing.T) {
	tests := []struct {
		name     string
		pattern  func() string
		expected string
	}{
		{
			name:     "AllRuns",
			pattern:  Patterns.AllRuns,
			expected: "system.run.>",
		},
		{
			name:     "Run",
			pattern:  func() string { return Patterns.Run("abc123") },
			expected: "system.run.abc123.>",
		},
		{
			name:     "RunLifecycle",
			pattern:  func() string { return Patterns.RunLifecycle("abc123") },
			expected: "system.run.abc123.*",
		},
		{
			name:     "RunSteps",
			pattern:  func() string { return Patterns.RunSteps("abc123") },
			expected: "system.run.abc123.step.>",
		},
		{
			name:     "AllFunctions",
			pattern:  Patterns.AllFunctions,
			expected: "system.function.>",
		},
		{
			name:     "Function",
			pattern:  func() string { return Patterns.Function("func123") },
			expected: "system.function.func123.>",
		},
		{
			name:     "UserEvent",
			pattern:  func() string { return Patterns.UserEvent("order.placed") },
			expected: "events:order.placed",
		},
		{
			name:     "AllUserEvents",
			pattern:  Patterns.AllUserEvents,
			expected: "events:>",
		},
		{
			name:     "AllSecrets",
			pattern:  Patterns.AllSecrets,
			expected: "system.secret.*",
		},
		{
			name:     "Secret",
			pattern:  func() string { return Patterns.Secret("API_KEY") },
			expected: "system.secret.API_KEY.*",
		},
		{
			name:     "SecretAction",
			pattern:  func() string { return Patterns.SecretAction("updated") },
			expected: "system.secret.*.updated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.pattern()
			if got != tt.expected {
				t.Errorf("got %q, want %q", got, tt.expected)
			}
		})
	}
}

// ============================================================================
// Connection Tests
// ============================================================================

func TestSubscriptionClient_Connect(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if !client.IsConnected() {
		t.Error("expected client to be connected")
	}

	if client.State() != StateConnected {
		t.Errorf("expected state %q, got %q", StateConnected, client.State())
	}

	client.Close()

	if client.IsConnected() {
		t.Error("expected client to be disconnected after close")
	}
}

func TestSubscriptionClient_ConnectAlreadyConnected(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("first Connect failed: %v", err)
	}

	// Second connect should succeed immediately
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("second Connect failed: %v", err)
	}
}

func TestSubscriptionClient_ConnectionCallback(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})

	var connected bool
	var mu sync.Mutex

	client.SetConnectionCallback(func(c bool) {
		mu.Lock()
		connected = c
		mu.Unlock()
	})

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if !connected {
		t.Error("expected connection callback to be called with true")
	}
	mu.Unlock()

	client.Close()
}

// ============================================================================
// Subscribe Tests
// ============================================================================

func TestSubscriptionClient_Subscribe(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	// Auto-respond to subscribe requests
	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_123",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.Subscribe(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if sub.ID != "sub_123" {
		t.Errorf("expected subscription ID %q, got %q", "sub_123", sub.ID)
	}

	if sub.Pattern != "system.run.>" {
		t.Errorf("expected pattern %q, got %q", "system.run.>", sub.Pattern)
	}

	// Verify message was sent
	time.Sleep(50 * time.Millisecond)
	messages := server.GetMessages()
	if len(messages) == 0 {
		t.Fatal("expected at least one message")
	}

	var sentMsg wsSubscribeRequest
	if err := json.Unmarshal(messages[0], &sentMsg); err != nil {
		t.Fatalf("failed to unmarshal message: %v", err)
	}

	if sentMsg.Type != "subscribe" {
		t.Errorf("expected type %q, got %q", "subscribe", sentMsg.Type)
	}

	if sentMsg.Subscription.Pattern != "system.run.>" {
		t.Errorf("expected pattern %q, got %q", "system.run.>", sentMsg.Subscription.Pattern)
	}
}

func TestSubscriptionClient_SubscribeWithOptions(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_456",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	_, err := client.Subscribe(ctx, "system.run.>", &SubscribeOptions{
		Replay:          10,
		IncludeMetadata: true,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Verify options were sent
	time.Sleep(50 * time.Millisecond)
	messages := server.GetMessages()
	if len(messages) == 0 {
		t.Fatal("expected at least one message")
	}

	var sentMsg wsSubscribeRequest
	if err := json.Unmarshal(messages[0], &sentMsg); err != nil {
		t.Fatalf("failed to unmarshal message: %v", err)
	}

	if sentMsg.Subscription.Options == nil {
		t.Fatal("expected options to be set")
	}

	if sentMsg.Subscription.Options.Replay != 10 {
		t.Errorf("expected replay %d, got %d", 10, sentMsg.Subscription.Options.Replay)
	}

	if !sentMsg.Subscription.Options.IncludeMetadata {
		t.Error("expected includeMetadata to be true")
	}
}

func TestSubscriptionClient_SubscribeNotConnected(t *testing.T) {
	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: "ws://localhost:9999",
	})

	ctx := context.Background()

	_, err := client.Subscribe(ctx, "system.run.>", nil)
	if err == nil {
		t.Error("expected error when not connected")
	}
}

func TestSubscriptionClient_SubscribeDuplicate(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_123",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	_, err := client.Subscribe(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("first Subscribe failed: %v", err)
	}

	_, err = client.Subscribe(ctx, "system.run.>", nil)
	if err == nil {
		t.Error("expected error for duplicate subscription")
	}
}

func TestSubscriptionClient_SubscribeError(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern": msg.Subscription.Pattern,
						"status":  "error",
						"code":    "INVALID_PATTERN",
						"message": "Invalid pattern syntax",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	_, err := client.Subscribe(ctx, "invalid::**", nil)
	if err == nil {
		t.Error("expected error for invalid pattern")
	}

	if !strings.Contains(err.Error(), "INVALID_PATTERN") {
		t.Errorf("expected error to contain INVALID_PATTERN, got: %v", err)
	}
}

// ============================================================================
// Event Handling Tests
// ============================================================================

func TestSubscriptionClient_ReceiveEvent(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_123",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.Subscribe(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Send an event
	go func() {
		time.Sleep(50 * time.Millisecond)
		server.SendMessage(map[string]any{
			"type":           "event",
			"subscriptionId": "sub_123",
			"topic":          "system.run.abc123.updated",
			"data":           map[string]any{"id": "abc123", "status": "running"},
		})
	}()

	// Receive event
	select {
	case event := <-sub.Events():
		if event.Topic != "system.run.abc123.updated" {
			t.Errorf("expected topic %q, got %q", "system.run.abc123.updated", event.Topic)
		}

		var data map[string]any
		if err := json.Unmarshal(event.Data, &data); err != nil {
			t.Fatalf("failed to unmarshal data: %v", err)
		}

		if data["id"] != "abc123" {
			t.Errorf("expected id %q, got %q", "abc123", data["id"])
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestSubscriptionClient_ReceiveEventWithMetadata(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_123",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.Subscribe(ctx, "system.run.>", &SubscribeOptions{
		IncludeMetadata: true,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Send an event with metadata
	go func() {
		time.Sleep(50 * time.Millisecond)
		server.SendMessage(map[string]any{
			"type":           "event",
			"subscriptionId": "sub_123",
			"topic":          "system.run.abc123.updated",
			"data":           map[string]any{"id": "abc123"},
			"meta": map[string]any{
				"timestamp": "2025-01-01T00:00:00Z",
				"sequence":  42,
			},
		})
	}()

	// Receive event
	select {
	case event := <-sub.Events():
		if event.Meta == nil {
			t.Fatal("expected metadata")
		}

		if event.Meta.Timestamp != "2025-01-01T00:00:00Z" {
			t.Errorf("expected timestamp %q, got %q", "2025-01-01T00:00:00Z", event.Meta.Timestamp)
		}

		if event.Meta.Sequence != 42 {
			t.Errorf("expected sequence %d, got %d", 42, event.Meta.Sequence)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

// ============================================================================
// Unsubscribe Tests
// ============================================================================

func TestSubscriptionClient_Unsubscribe(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	var unsubscribeReceived bool
	var mu sync.Mutex

	server.onMessage = func(data []byte) {
		var msg map[string]any
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}

		switch msg["type"] {
		case "subscribe":
			var subMsg wsSubscribeRequest
			json.Unmarshal(data, &subMsg)
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        subMsg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_123",
					},
				},
			})
		case "unsubscribe":
			mu.Lock()
			unsubscribeReceived = true
			mu.Unlock()
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.Subscribe(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	sub.Unsubscribe()

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if !unsubscribeReceived {
		t.Error("expected unsubscribe message to be sent")
	}
	mu.Unlock()

	// Verify channels are closed
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Error("expected events channel to be closed")
		}
	default:
		// Channel might be buffered
	}
}

// ============================================================================
// Error Handling Tests
// ============================================================================

func TestSubscriptionClient_ReceiveError(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_123",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.Subscribe(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Send a subscription error
	go func() {
		time.Sleep(50 * time.Millisecond)
		server.SendMessage(map[string]any{
			"type":           "subscription_error",
			"subscriptionId": "sub_123",
			"code":           "NATS_DISCONNECT",
			"message":        "Connection lost",
			"retrying":       true,
		})
	}()

	// Receive error
	select {
	case subErr := <-sub.Errors():
		if subErr.Code != "NATS_DISCONNECT" {
			t.Errorf("expected code %q, got %q", "NATS_DISCONNECT", subErr.Code)
		}

		if subErr.Message != "Connection lost" {
			t.Errorf("expected message %q, got %q", "Connection lost", subErr.Message)
		}

		if !subErr.Retrying {
			t.Error("expected retrying to be true")
		}

	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for error")
	}
}

// ============================================================================
// Client Helper Tests
// ============================================================================

func TestClient_CreateSubscriptionClient(t *testing.T) {
	client := NewClient(ClientConfig{
		ServerURL: DefaultServerURL,
	})

	subClient := client.CreateSubscriptionClient()

	if subClient == nil {
		t.Fatal("expected subscription client")
		return
	}

	if subClient.config.WSURL != DefaultWebSocketURL {
		t.Errorf("expected WSURL %q, got %q", DefaultWebSocketURL, subClient.config.WSURL)
	}
}

func TestClient_CreateSubscriptionClientHTTPS(t *testing.T) {
	client := NewClient(ClientConfig{
		ServerURL: "https://api.example.com",
	})

	subClient := client.CreateSubscriptionClient()

	if subClient.config.WSURL != "wss://api.example.com/ws" {
		t.Errorf("expected WSURL %q, got %q", "wss://api.example.com/ws", subClient.config.WSURL)
	}
}

// ============================================================================
// SubscribeAckable Test Helpers
// ============================================================================

// waitForMessages polls the mock server until it has received at least count
// messages, or the timeout expires. This avoids flaky time.Sleep-based waits.
func waitForMessages(t *testing.T, server *mockWSServer, count int, timeout time.Duration) []json.RawMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs := server.GetMessages()
		if len(msgs) >= count {
			return msgs
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d messages, got %d", count, len(server.GetMessages()))
	return nil
}

// waitForCondition polls until condFn returns true, or the timeout expires.
func waitForCondition(t *testing.T, timeout time.Duration, condFn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condFn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

// ============================================================================
// SubscribeAckable Tests
// ============================================================================

func TestSubscribeAckable_SendsManualAckMode(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_ack_1",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Subscribe with nil opts — SubscribeAckable should force manual ack mode
	sub, err := client.SubscribeAckable(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("SubscribeAckable failed: %v", err)
	}
	defer sub.Unsubscribe()

	// Verify the subscribe message included ackMode=manual
	messages := waitForMessages(t, server, 1, 2*time.Second)

	var sentMsg wsSubscribeRequest
	if err := json.Unmarshal(messages[0], &sentMsg); err != nil {
		t.Fatalf("failed to unmarshal message: %v", err)
	}

	if sentMsg.Type != "subscribe" {
		t.Errorf("expected type %q, got %q", "subscribe", sentMsg.Type)
	}

	if sentMsg.Subscription.Options == nil {
		t.Fatal("expected options to be set")
	}

	if sentMsg.Subscription.Options.AckMode != "manual" {
		t.Errorf("expected ackMode %q, got %q", "manual", sentMsg.Subscription.Options.AckMode)
	}
}

func TestSubscribeAckable_SendsManualAckModeWithExistingOpts(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_ack_2",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Subscribe with existing opts — SubscribeAckable should override ack mode
	sub, err := client.SubscribeAckable(ctx, "system.run.>", &SubscribeOptions{
		Replay:        5,
		ConsumerGroup: "my-group",
		AckMode:       AckModeAuto, // This should be overridden to manual
	})
	if err != nil {
		t.Fatalf("SubscribeAckable failed: %v", err)
	}
	defer sub.Unsubscribe()

	messages := waitForMessages(t, server, 1, 2*time.Second)

	var sentMsg wsSubscribeRequest
	if err := json.Unmarshal(messages[0], &sentMsg); err != nil {
		t.Fatalf("failed to unmarshal message: %v", err)
	}

	if sentMsg.Subscription.Options == nil {
		t.Fatal("expected options to be set")
	}

	if sentMsg.Subscription.Options.AckMode != "manual" {
		t.Errorf("expected ackMode %q, got %q", "manual", sentMsg.Subscription.Options.AckMode)
	}

	if sentMsg.Subscription.Options.Replay != 5 {
		t.Errorf("expected replay %d, got %d", 5, sentMsg.Subscription.Options.Replay)
	}

	if sentMsg.Subscription.Options.ConsumerGroup != "my-group" {
		t.Errorf("expected consumerGroup %q, got %q", "my-group", sentMsg.Subscription.Options.ConsumerGroup)
	}
}

func TestSubscribeAckable_ReceivesEvents(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_ack_3",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.SubscribeAckable(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("SubscribeAckable failed: %v", err)
	}
	defer sub.Unsubscribe()

	// Send an event from server
	go func() {
		time.Sleep(50 * time.Millisecond)
		server.SendMessage(map[string]any{
			"type":           "event",
			"subscriptionId": "sub_ack_3",
			"eventId":        "evt_001",
			"topic":          "system.run.abc123.completed",
			"data":           map[string]any{"id": "abc123", "status": "completed"},
		})
	}()

	select {
	case event := <-sub.Events():
		if event.ID != "evt_001" {
			t.Errorf("expected event ID %q, got %q", "evt_001", event.ID)
		}
		if event.Topic != "system.run.abc123.completed" {
			t.Errorf("expected topic %q, got %q", "system.run.abc123.completed", event.Topic)
		}
		var data map[string]any
		if err := json.Unmarshal(event.Data, &data); err != nil {
			t.Fatalf("failed to unmarshal data: %v", err)
		}
		if data["status"] != "completed" {
			t.Errorf("expected status %q, got %q", "completed", data["status"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestSubscribeAckable_Ack(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_ack_4",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.SubscribeAckable(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("SubscribeAckable failed: %v", err)
	}
	defer sub.Unsubscribe()

	// Send Ack
	if err := sub.Ack("evt_100"); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}

	// First message is the subscribe request, second should be the ack
	messages := waitForMessages(t, server, 2, 2*time.Second)

	var ackMsg wsAckRequest
	if err := json.Unmarshal(messages[1], &ackMsg); err != nil {
		t.Fatalf("failed to unmarshal ack message: %v", err)
	}

	if ackMsg.Type != "ack" {
		t.Errorf("expected type %q, got %q", "ack", ackMsg.Type)
	}

	if ackMsg.AckType != "ack" {
		t.Errorf("expected ackType %q, got %q", "ack", ackMsg.AckType)
	}

	if ackMsg.EventID != "evt_100" {
		t.Errorf("expected eventId %q, got %q", "evt_100", ackMsg.EventID)
	}

	if ackMsg.RedeliverDelay != 0 {
		t.Errorf("expected redeliverDelay 0, got %d", ackMsg.RedeliverDelay)
	}
}

func TestSubscribeAckable_Nak(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_ack_5",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.SubscribeAckable(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("SubscribeAckable failed: %v", err)
	}
	defer sub.Unsubscribe()

	// Send Nak with a 5-second delay
	if err := sub.Nak("evt_200", 5*time.Second); err != nil {
		t.Fatalf("Nak failed: %v", err)
	}

	messages := waitForMessages(t, server, 2, 2*time.Second)

	var nakMsg wsAckRequest
	if err := json.Unmarshal(messages[1], &nakMsg); err != nil {
		t.Fatalf("failed to unmarshal nak message: %v", err)
	}

	if nakMsg.Type != "ack" {
		t.Errorf("expected type %q, got %q", "ack", nakMsg.Type)
	}

	if nakMsg.AckType != "nak" {
		t.Errorf("expected ackType %q, got %q", "nak", nakMsg.AckType)
	}

	if nakMsg.EventID != "evt_200" {
		t.Errorf("expected eventId %q, got %q", "evt_200", nakMsg.EventID)
	}

	if nakMsg.RedeliverDelay != 5000 {
		t.Errorf("expected redeliverDelay 5000, got %d", nakMsg.RedeliverDelay)
	}
}

func TestSubscribeAckable_NakZeroDelay(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_ack_5b",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.SubscribeAckable(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("SubscribeAckable failed: %v", err)
	}
	defer sub.Unsubscribe()

	// Send Nak with zero delay — immediate redelivery
	if err := sub.Nak("evt_201", 0); err != nil {
		t.Fatalf("Nak failed: %v", err)
	}

	messages := waitForMessages(t, server, 2, 2*time.Second)

	var nakMsg wsAckRequest
	if err := json.Unmarshal(messages[1], &nakMsg); err != nil {
		t.Fatalf("failed to unmarshal nak message: %v", err)
	}

	if nakMsg.AckType != "nak" {
		t.Errorf("expected ackType %q, got %q", "nak", nakMsg.AckType)
	}

	if nakMsg.RedeliverDelay != 0 {
		t.Errorf("expected redeliverDelay 0, got %d", nakMsg.RedeliverDelay)
	}
}

func TestSubscribeAckable_Term(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_ack_6",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.SubscribeAckable(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("SubscribeAckable failed: %v", err)
	}
	defer sub.Unsubscribe()

	// Send Term
	if err := sub.Term("evt_300"); err != nil {
		t.Fatalf("Term failed: %v", err)
	}

	messages := waitForMessages(t, server, 2, 2*time.Second)

	var termMsg wsAckRequest
	if err := json.Unmarshal(messages[1], &termMsg); err != nil {
		t.Fatalf("failed to unmarshal term message: %v", err)
	}

	if termMsg.Type != "ack" {
		t.Errorf("expected type %q, got %q", "ack", termMsg.Type)
	}

	if termMsg.AckType != "term" {
		t.Errorf("expected ackType %q, got %q", "term", termMsg.AckType)
	}

	if termMsg.EventID != "evt_300" {
		t.Errorf("expected eventId %q, got %q", "evt_300", termMsg.EventID)
	}

	if termMsg.RedeliverDelay != 0 {
		t.Errorf("expected redeliverDelay 0, got %d", termMsg.RedeliverDelay)
	}
}

func TestSubscribeAckable_Unsubscribe(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	var unsubscribeReceived bool
	var mu sync.Mutex

	server.onMessage = func(data []byte) {
		var msg map[string]any
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}

		switch msg["type"] {
		case "subscribe":
			var subMsg wsSubscribeRequest
			json.Unmarshal(data, &subMsg)
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        subMsg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_ack_7",
					},
				},
			})
		case "unsubscribe":
			mu.Lock()
			unsubscribeReceived = true
			mu.Unlock()
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.SubscribeAckable(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("SubscribeAckable failed: %v", err)
	}

	sub.Unsubscribe()

	waitForCondition(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return unsubscribeReceived
	})

	// Verify events channel is closed
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Error("expected events channel to be closed")
		}
	default:
		// Channel might be buffered
	}
}

func TestSubscribeAckable_AckNakTermSequence(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()

	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Type == "subscribe" {
			server.SendMessage(map[string]any{
				"type": "subscription_result",
				"results": []map[string]any{
					{
						"pattern":        msg.Subscription.Pattern,
						"status":         "ok",
						"subscriptionId": "sub_ack_8",
					},
				},
			})
		}
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL: server.URL(),
	})
	defer client.Close()

	ctx := context.Background()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.SubscribeAckable(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("SubscribeAckable failed: %v", err)
	}
	defer sub.Unsubscribe()

	// Send ack, nak, term in sequence for different event IDs
	if err := sub.Ack("evt_a"); err != nil {
		t.Fatalf("Ack failed: %v", err)
	}
	if err := sub.Nak("evt_b", 10*time.Second); err != nil {
		t.Fatalf("Nak failed: %v", err)
	}
	if err := sub.Term("evt_c"); err != nil {
		t.Fatalf("Term failed: %v", err)
	}

	// messages[0] = subscribe, messages[1] = ack, messages[2] = nak, messages[3] = term
	messages := waitForMessages(t, server, 4, 2*time.Second)

	// Verify ack
	var ackMsg wsAckRequest
	if err := json.Unmarshal(messages[1], &ackMsg); err != nil {
		t.Fatalf("failed to unmarshal ack message: %v", err)
	}
	if ackMsg.AckType != "ack" || ackMsg.EventID != "evt_a" {
		t.Errorf("ack message: got ackType=%q eventId=%q, want ackType=%q eventId=%q",
			ackMsg.AckType, ackMsg.EventID, "ack", "evt_a")
	}

	// Verify nak
	var nakMsg wsAckRequest
	if err := json.Unmarshal(messages[2], &nakMsg); err != nil {
		t.Fatalf("failed to unmarshal nak message: %v", err)
	}
	if nakMsg.AckType != "nak" || nakMsg.EventID != "evt_b" || nakMsg.RedeliverDelay != 10000 {
		t.Errorf("nak message: got ackType=%q eventId=%q delay=%d, want ackType=%q eventId=%q delay=%d",
			nakMsg.AckType, nakMsg.EventID, nakMsg.RedeliverDelay, "nak", "evt_b", 10000)
	}

	// Verify term
	var termMsg wsAckRequest
	if err := json.Unmarshal(messages[3], &termMsg); err != nil {
		t.Fatalf("failed to unmarshal term message: %v", err)
	}
	if termMsg.AckType != "term" || termMsg.EventID != "evt_c" {
		t.Errorf("term message: got ackType=%q eventId=%q, want ackType=%q eventId=%q",
			termMsg.AckType, termMsg.EventID, "term", "evt_c")
	}
}

// ============================================================================
// SubscribeEntityStream Tests
// ============================================================================

func TestSubscriptionClient_SubscribeEntityStream(t *testing.T) {
	t.Run("requires EntityType", func(t *testing.T) {
		server := newMockWSServer()
		defer server.Close()

		client := NewSubscriptionClient(SubscriptionClientConfig{
			WSURL: server.URL(),
		})
		defer client.Close()

		ctx := context.Background()
		if err := client.Connect(ctx); err != nil {
			t.Fatalf("Connect failed: %v", err)
		}

		_, err := client.SubscribeEntityStream(ctx, "order-123", EntitySubscribeOptions{})
		if err == nil {
			t.Fatal("expected error for missing EntityType")
		}
		if !strings.Contains(err.Error(), "EntityType is required") {
			t.Errorf("expected EntityType error, got: %v", err)
		}
	})

	t.Run("constructs correct entity pattern", func(t *testing.T) {
		server := newMockWSServer()
		defer server.Close()

		var receivedPattern string
		var mu sync.Mutex

		server.onMessage = func(data []byte) {
			var msg wsSubscribeRequest
			if err := json.Unmarshal(data, &msg); err != nil {
				return
			}
			if msg.Type == "subscribe" {
				mu.Lock()
				receivedPattern = msg.Subscription.Pattern
				mu.Unlock()

				server.SendMessage(map[string]any{
					"type": "subscription_result",
					"results": []map[string]any{
						{
							"pattern":        msg.Subscription.Pattern,
							"status":         "ok",
							"subscriptionId": "sub_entity_1",
						},
					},
				})
			}
		}

		client := NewSubscriptionClient(SubscriptionClientConfig{
			WSURL: server.URL(),
		})
		defer client.Close()

		ctx := context.Background()
		if err := client.Connect(ctx); err != nil {
			t.Fatalf("Connect failed: %v", err)
		}

		sub, err := client.SubscribeEntityStream(ctx, "order-123", EntitySubscribeOptions{
			EntityType: "order",
		})
		if err != nil {
			t.Fatalf("SubscribeEntityStream failed: %v", err)
		}
		defer sub.Unsubscribe()

		mu.Lock()
		got := receivedPattern
		mu.Unlock()

		expected := "entity:order.order-123.>"
		if got != expected {
			t.Errorf("expected pattern %q, got %q", expected, got)
		}
	})

	t.Run("passes replay option", func(t *testing.T) {
		server := newMockWSServer()
		defer server.Close()

		server.onMessage = func(data []byte) {
			var msg wsSubscribeRequest
			if err := json.Unmarshal(data, &msg); err != nil {
				return
			}
			if msg.Type == "subscribe" {
				server.SendMessage(map[string]any{
					"type": "subscription_result",
					"results": []map[string]any{
						{
							"pattern":        msg.Subscription.Pattern,
							"status":         "ok",
							"subscriptionId": "sub_entity_2",
						},
					},
				})
			}
		}

		client := NewSubscriptionClient(SubscriptionClientConfig{
			WSURL: server.URL(),
		})
		defer client.Close()

		ctx := context.Background()
		if err := client.Connect(ctx); err != nil {
			t.Fatalf("Connect failed: %v", err)
		}

		sub, err := client.SubscribeEntityStream(ctx, "order-123", EntitySubscribeOptions{
			EntityType: "order",
			Replay:     50,
		})
		if err != nil {
			t.Fatalf("SubscribeEntityStream failed: %v", err)
		}
		defer sub.Unsubscribe()

		// Verify the subscribe message included replay option
		messages := waitForMessages(t, server, 1, 2*time.Second)

		var sentMsg wsSubscribeRequest
		if err := json.Unmarshal(messages[0], &sentMsg); err != nil {
			t.Fatalf("failed to unmarshal message: %v", err)
		}

		if sentMsg.Subscription.Options == nil {
			t.Fatal("expected options to be set")
		}

		if sentMsg.Subscription.Options.Replay != 50 {
			t.Errorf("expected replay 50, got %d", sentMsg.Subscription.Options.Replay)
		}
	})

	t.Run("zero replay does not set options replay", func(t *testing.T) {
		server := newMockWSServer()
		defer server.Close()

		server.onMessage = func(data []byte) {
			var msg wsSubscribeRequest
			if err := json.Unmarshal(data, &msg); err != nil {
				return
			}
			if msg.Type == "subscribe" {
				server.SendMessage(map[string]any{
					"type": "subscription_result",
					"results": []map[string]any{
						{
							"pattern":        msg.Subscription.Pattern,
							"status":         "ok",
							"subscriptionId": "sub_entity_3",
						},
					},
				})
			}
		}

		client := NewSubscriptionClient(SubscriptionClientConfig{
			WSURL: server.URL(),
		})
		defer client.Close()

		ctx := context.Background()
		if err := client.Connect(ctx); err != nil {
			t.Fatalf("Connect failed: %v", err)
		}

		sub, err := client.SubscribeEntityStream(ctx, "order-456", EntitySubscribeOptions{
			EntityType: "order",
			Replay:     0,
		})
		if err != nil {
			t.Fatalf("SubscribeEntityStream failed: %v", err)
		}
		defer sub.Unsubscribe()

		// Verify the subscribe message was sent without replay options
		messages := waitForMessages(t, server, 1, 2*time.Second)

		var sentMsg wsSubscribeRequest
		if err := json.Unmarshal(messages[0], &sentMsg); err != nil {
			t.Fatalf("failed to unmarshal message: %v", err)
		}

		// With Replay=0, SubscribeOptions has no non-zero fields, so no options block is sent
		if sentMsg.Subscription.Options != nil && sentMsg.Subscription.Options.Replay != 0 {
			t.Errorf("expected no replay option, got %d", sentMsg.Subscription.Options.Replay)
		}
	})
}
