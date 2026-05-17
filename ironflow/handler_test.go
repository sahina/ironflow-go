package ironflow

import (
	"encoding/json"
	"testing"
)

// ============================================================================
// CreateHandler Tests
// ============================================================================

func TestCreateHandler_MinimalConfig(t *testing.T) {
	type OrderEvent struct {
		OrderID string `json:"orderId"`
	}

	handler := CreateHandler(HandlerConfig[OrderEvent]{
		Event: "order.placed",
		Handler: func(event OrderEvent, ctx *HandlerContext) (any, error) {
			return map[string]any{"processed": true, "orderId": event.OrderID}, nil
		},
	})

	if handler.Config.ID != "order-placed-handler" {
		t.Errorf("expected ID %q, got %q", "order-placed-handler", handler.Config.ID)
	}

	if len(handler.Config.Triggers) != 1 {
		t.Fatalf("expected 1 trigger, got %d", len(handler.Config.Triggers))
	}

	if handler.Config.Triggers[0].Event != "order.placed" {
		t.Errorf("expected event %q, got %q", "order.placed", handler.Config.Triggers[0].Event)
	}
}

func TestCreateHandler_CustomID(t *testing.T) {
	type OrderEvent struct{}

	handler := CreateHandler(HandlerConfig[OrderEvent]{
		Event: "order.placed",
		Handler: func(event OrderEvent, ctx *HandlerContext) (any, error) {
			return nil, nil
		},
		Options: &HandlerOptions{
			ID: "custom-order-handler",
		},
	})

	if handler.Config.ID != "custom-order-handler" {
		t.Errorf("expected ID %q, got %q", "custom-order-handler", handler.Config.ID)
	}
}

func TestCreateHandler_WithFilter(t *testing.T) {
	type OrderEvent struct{}

	handler := CreateHandler(HandlerConfig[OrderEvent]{
		Event: "order.placed",
		Handler: func(event OrderEvent, ctx *HandlerContext) (any, error) {
			return nil, nil
		},
		Options: &HandlerOptions{
			Filter: `data.total > 100`,
		},
	})

	if handler.Config.Triggers[0].Expression != `data.total > 100` {
		t.Errorf("expected filter %q, got %q", `data.total > 100`, handler.Config.Triggers[0].Expression)
	}
}

func TestCreateHandler_AllOptions(t *testing.T) {
	type OrderEvent struct{}

	handler := CreateHandler(HandlerConfig[OrderEvent]{
		Event: "order.placed",
		Handler: func(event OrderEvent, ctx *HandlerContext) (any, error) {
			return nil, nil
		},
		Options: &HandlerOptions{
			ID:     "full-order-handler",
			Name:   "Order Handler",
			Filter: `data.priority == "high"`,
			Retry: &RetryConfig{
				MaxAttempts: 5,
			},
			Concurrency: &ConcurrencyConfig{
				Limit: 10,
				Key:   "event.data.customerId",
			},
			Mode:     PullMode,
			ActorKey: "data.customerId",
		},
	})

	if handler.Config.ID != "full-order-handler" {
		t.Errorf("expected ID %q, got %q", "full-order-handler", handler.Config.ID)
	}

	if handler.Config.Name != "Order Handler" {
		t.Errorf("expected name %q, got %q", "Order Handler", handler.Config.Name)
	}

	if handler.Config.Triggers[0].Expression != `data.priority == "high"` {
		t.Errorf("expected filter expression")
	}

	if handler.Config.Retry == nil || handler.Config.Retry.MaxAttempts != 5 {
		t.Error("expected retry config with MaxAttempts 5")
	}

	if handler.Config.Concurrency == nil || handler.Config.Concurrency.Limit != 10 {
		t.Error("expected concurrency config with limit 10")
	}

	if handler.Config.Mode != PullMode {
		t.Errorf("expected mode %q, got %q", PullMode, handler.Config.Mode)
	}

	if handler.Config.ActorKey != "data.customerId" {
		t.Errorf("expected actorKey %q, got %q", "data.customerId", handler.Config.ActorKey)
	}
}

// ============================================================================
// ID Generation Tests
// ============================================================================

func TestGenerateHandlerID(t *testing.T) {
	tests := []struct {
		event    string
		expected string
	}{
		{"order.placed", "order-placed-handler"},
		{"user.created", "user-created-handler"},
		{"payment.processed", "payment-processed-handler"},
		{"order.*", "order-handler"},
		{"events.>", "events-handler"},
		{"system.run.abc123.step.completed", "system-run-abc123-step-completed-handler"},
	}

	for _, tt := range tests {
		t.Run(tt.event, func(t *testing.T) {
			got := generateHandlerID(tt.event)
			if got != tt.expected {
				t.Errorf("generateHandlerID(%q) = %q, want %q", tt.event, got, tt.expected)
			}
		})
	}
}

// ============================================================================
// Handler Execution Tests
// ============================================================================

func TestCreateHandler_HandlerExecution(t *testing.T) {
	type OrderEvent struct {
		OrderID string  `json:"orderId"`
		Total   float64 `json:"total"`
	}

	var capturedEvent OrderEvent
	var capturedContext *HandlerContext

	handler := CreateHandler(HandlerConfig[OrderEvent]{
		Event: "order.placed",
		Handler: func(event OrderEvent, ctx *HandlerContext) (any, error) {
			capturedEvent = event
			capturedContext = ctx
			return map[string]any{"processed": true, "orderId": event.OrderID}, nil
		},
	})

	// Create mock context
	eventData, _ := json.Marshal(map[string]any{
		"orderId": "order-123",
		"total":   99.99,
	})

	ctx := Context{
		Event: Event{
			ID:             "evt-123",
			Name:           "order.placed",
			RawData:        eventData,
			IdempotencyKey: "key-789",
			Source:         EventSourceAPI,
			Metadata:       map[string]any{"correlationId": "corr-123"},
		},
		Run: RunInfo{
			ID:         "run-001",
			FunctionID: "order-placed-handler",
			Attempt:    1,
		},
		exec: &executionContext{
			runID:          "run-001",
			functionID:     "order-placed-handler",
			attempt:        1,
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		},
	}

	result, err := handler.Handler(ctx)
	if err != nil {
		t.Fatalf("handler execution failed: %v", err)
	}

	// Verify event was passed correctly
	if capturedEvent.OrderID != "order-123" {
		t.Errorf("expected orderId %q, got %q", "order-123", capturedEvent.OrderID)
	}

	if capturedEvent.Total != 99.99 {
		t.Errorf("expected total %v, got %v", 99.99, capturedEvent.Total)
	}

	// Verify context was adapted correctly
	if capturedContext == nil {
		t.Fatal("expected context to be captured")
	}

	if capturedContext.EventMeta.ID != "evt-123" {
		t.Errorf("expected event ID %q, got %q", "evt-123", capturedContext.EventMeta.ID)
	}

	if capturedContext.EventMeta.Name != "order.placed" {
		t.Errorf("expected event name %q, got %q", "order.placed", capturedContext.EventMeta.Name)
	}

	if capturedContext.EventMeta.IdempotencyKey != "key-789" {
		t.Errorf("expected idempotency key %q, got %q", "key-789", capturedContext.EventMeta.IdempotencyKey)
	}

	if capturedContext.EventMeta.Source != "api" {
		t.Errorf("expected source %q, got %q", "api", capturedContext.EventMeta.Source)
	}

	if capturedContext.Run.ID != "run-001" {
		t.Errorf("expected run ID %q, got %q", "run-001", capturedContext.Run.ID)
	}

	if capturedContext.Step == nil {
		t.Error("expected Step client to be present")
	}

	if capturedContext.Logger == nil {
		t.Error("expected Logger to be present")
	}

	// Verify result
	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatal("expected result to be a map")
	}

	if resultMap["processed"] != true {
		t.Error("expected processed to be true")
	}

	if resultMap["orderId"] != "order-123" {
		t.Errorf("expected orderId %q, got %q", "order-123", resultMap["orderId"])
	}
}

// ============================================================================
// StepClient Tests
// ============================================================================

func TestStepClient_Run(t *testing.T) {
	// Create a minimal execution context
	exec := &executionContext{
		runID:          "run-001",
		functionID:     "test-function",
		attempt:        1,
		stepCounters:   make(map[string]int),
		completedSteps: make(map[string]*CompletedStep),
		executedSteps:  make([]*StepResult, 0),
	}

	ctx := Context{exec: exec}
	stepClient := &StepClient{ctx: ctx}

	result, err := stepClient.Run("test-step", func() (any, error) {
		return "step result", nil
	})

	if err != nil {
		t.Fatalf("step.Run failed: %v", err)
	}

	if result != "step result" {
		t.Errorf("expected result %q, got %v", "step result", result)
	}
}

// ============================================================================
// HandlerContext Tests
// ============================================================================

func TestHandlerContext_EventAccess(t *testing.T) {
	type OrderEvent struct {
		OrderID string `json:"orderId"`
	}

	handler := CreateHandler(HandlerConfig[OrderEvent]{
		Event: "order.placed",
		Handler: func(event OrderEvent, ctx *HandlerContext) (any, error) {
			// Access both the typed event and the event in context
			typedEvent := ctx.Event.(OrderEvent)
			if typedEvent.OrderID != event.OrderID {
				return nil, nil
			}
			return map[string]any{"orderId": event.OrderID}, nil
		},
	})

	eventData, _ := json.Marshal(map[string]any{"orderId": "order-456"})

	ctx := Context{
		Event: Event{
			ID:      "evt-456",
			Name:    "order.placed",
			RawData: eventData,
		},
		exec: &executionContext{
			runID:          "run-002",
			functionID:     "order-placed-handler",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		},
	}

	result, err := handler.Handler(ctx)
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}

	resultMap := result.(map[string]any)
	if resultMap["orderId"] != "order-456" {
		t.Errorf("expected orderId %q, got %v", "order-456", resultMap["orderId"])
	}
}
