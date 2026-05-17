package ironflow

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUpcasterRegistry_BasicUpcast(t *testing.T) {
	registry := NewUpcasterRegistry()

	// Register v1→v2 upcaster that adds a "currency" field
	registry.Register("order.created", 1, 2, func(data json.RawMessage) (json.RawMessage, error) {
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		m["currency"] = "USD"
		return json.Marshal(m)
	})

	input := json.RawMessage(`{"orderId":"123","total":50}`)
	result, err := registry.Upcast("order.created", input, 1, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(result, &m); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if m["currency"] != "USD" {
		t.Errorf("expected currency=USD, got %v", m["currency"])
	}
	if m["orderId"] != "123" {
		t.Errorf("expected orderId=123, got %v", m["orderId"])
	}
}

func TestUpcasterRegistry_ChainUpcast(t *testing.T) {
	registry := NewUpcasterRegistry()

	// v1→v2: add currency
	registry.Register("order.created", 1, 2, func(data json.RawMessage) (json.RawMessage, error) {
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		m["currency"] = "USD"
		return json.Marshal(m)
	})

	// v2→v3: add version marker
	registry.Register("order.created", 2, 3, func(data json.RawMessage) (json.RawMessage, error) {
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		m["schemaVersion"] = "v3"
		return json.Marshal(m)
	})

	input := json.RawMessage(`{"orderId":"123"}`)
	result, err := registry.Upcast("order.created", input, 1, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(result, &m); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if m["currency"] != "USD" {
		t.Errorf("expected currency=USD, got %v", m["currency"])
	}
	if m["schemaVersion"] != "v3" {
		t.Errorf("expected schemaVersion=v3, got %v", m["schemaVersion"])
	}
}

func TestUpcasterRegistry_IncompleteChain(t *testing.T) {
	registry := NewUpcasterRegistry()

	// Only v1→v2, no v2→v3
	registry.Register("order.created", 1, 2, func(data json.RawMessage) (json.RawMessage, error) {
		return data, nil
	})

	input := json.RawMessage(`{"orderId":"123"}`)
	_, err := registry.Upcast("order.created", input, 1, 3)
	if err == nil {
		t.Fatal("expected error for incomplete chain, got nil")
	}
}

func TestUpcasterRegistry_NoOpWhenAlreadyLatest(t *testing.T) {
	registry := NewUpcasterRegistry()

	registry.Register("order.created", 1, 2, func(data json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"should":"not be called"}`), nil
	})

	input := json.RawMessage(`{"orderId":"123"}`)
	result, err := registry.Upcast("order.created", input, 2, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(result) != string(input) {
		t.Errorf("expected data unchanged, got %s", result)
	}
}

func TestUpcasterRegistry_UpcastToLatest(t *testing.T) {
	registry := NewUpcasterRegistry()

	registry.Register("order.created", 1, 2, func(data json.RawMessage) (json.RawMessage, error) {
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		m["upgraded"] = true
		return json.Marshal(m)
	})

	input := json.RawMessage(`{"orderId":"123"}`)
	result, err := registry.UpcastToLatest("order.created", input, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(result, &m); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if m["upgraded"] != true {
		t.Errorf("expected upgraded=true, got %v", m["upgraded"])
	}
}

func TestUpcasterRegistry_UpcastToLatest_NoUpcasters(t *testing.T) {
	registry := NewUpcasterRegistry()

	input := json.RawMessage(`{"orderId":"123"}`)
	result, err := registry.UpcastToLatest("unknown.event", input, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(result) != string(input) {
		t.Errorf("expected data unchanged, got %s", result)
	}
}

func TestUpcasterRegistry_LatestVersion(t *testing.T) {
	registry := NewUpcasterRegistry()

	if v := registry.LatestVersion("order.created"); v != 0 {
		t.Errorf("expected 0 for unknown event, got %d", v)
	}

	registry.Register("order.created", 1, 2, func(data json.RawMessage) (json.RawMessage, error) {
		return data, nil
	})

	if v := registry.LatestVersion("order.created"); v != 2 {
		t.Errorf("expected 2, got %d", v)
	}

	registry.Register("order.created", 2, 3, func(data json.RawMessage) (json.RawMessage, error) {
		return data, nil
	})

	if v := registry.LatestVersion("order.created"); v != 3 {
		t.Errorf("expected 3, got %d", v)
	}
}

// TestServeHandler_WithUpcasters tests that the serve handler applies upcasting.
func TestServeHandler_WithUpcasters(t *testing.T) {
	upcasters := NewUpcasterRegistry()
	upcasters.Register("order.created", 1, 2, func(data json.RawMessage) (json.RawMessage, error) {
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		m["currency"] = "USD"
		return json.Marshal(m)
	})

	var receivedData json.RawMessage
	fn := Function{
		Config: FunctionConfig{
			ID:   "test-fn",
			Name: "Test Function",
			Triggers: []Trigger{
				{Event: "order.created"},
			},
		},
		Handler: func(ctx Context) (any, error) {
			receivedData = ctx.Event.RawData
			return map[string]any{"ok": true}, nil
		},
	}

	handler := Serve(ServeConfig{
		Functions:        []Function{fn},
		SkipVerification: true,
		Upcasters:        upcasters,
	})

	// Simulate a push request with v1 event data
	req := PushRequest{
		RunID:      "run-1",
		FunctionID: "test-fn",
		Attempt:    1,
		Event: PushEvent{
			ID:        "evt-1",
			Name:      "order.created",
			Version:   1,
			Data:      json.RawMessage(`{"orderId":"123","total":50}`),
			Timestamp: "2024-01-01T00:00:00Z",
		},
	}

	reqBody, _ := json.Marshal(req)

	httpReq, _ := http.NewRequest("POST", "/api/ironflow", bytes.NewReader(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httpReq)

	if rr.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	// Check that handler received upcasted data
	var m map[string]any
	if err := json.Unmarshal(receivedData, &m); err != nil {
		t.Fatalf("failed to unmarshal received data: %v", err)
	}

	if m["currency"] != "USD" {
		t.Errorf("expected currency=USD in handler, got %v", m["currency"])
	}
	if m["orderId"] != "123" {
		t.Errorf("expected orderId=123 in handler, got %v", m["orderId"])
	}
}
