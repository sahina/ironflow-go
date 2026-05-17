package ironflow

import (
	"testing"
)

// TestInvoke_YieldsWithCorrectType verifies that Invoke() panics with a
// yieldSignal of type "invoke_function" when the step is not memoized.
func TestInvoke_YieldsWithCorrectType(t *testing.T) {
	ctx := Context{
		exec: &executionContext{
			runID:          "run-123",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		},
	}

	var capturedSignal *yieldSignal

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic with yield signal")
			}
			signal, ok := r.(*yieldSignal)
			if !ok {
				t.Fatalf("expected *yieldSignal, got %T", r)
			}
			capturedSignal = signal
		}()

		Invoke[map[string]any](ctx, "charge-card", map[string]any{"amount": 100})
	}()

	if capturedSignal == nil {
		t.Fatal("expected yield signal to be captured")
	}
	if capturedSignal.info.Type != "invoke_function" {
		t.Errorf("expected type 'invoke_function', got '%s'", capturedSignal.info.Type)
	}
	if capturedSignal.info.FunctionID != "charge-card" {
		t.Errorf("expected FunctionID 'charge-card', got '%s'", capturedSignal.info.FunctionID)
	}
	if capturedSignal.info.StepID != "run-123:charge-card:0" {
		t.Errorf("expected StepID 'run-123:charge-card:0', got '%s'", capturedSignal.info.StepID)
	}
	if capturedSignal.info.InvokeTimeoutMs != 30000 {
		t.Errorf("expected default timeout 30000ms, got %d", capturedSignal.info.InvokeTimeoutMs)
	}
}

// TestInvoke_ReturnsMemoizedResult verifies that Invoke() returns the memoized
// result without panicking when the step is already completed.
func TestInvoke_ReturnsMemoizedResult(t *testing.T) {
	ctx := Context{
		exec: &executionContext{
			runID:        "run-123",
			stepCounters: make(map[string]int),
			completedSteps: map[string]*CompletedStep{
				"run-123:charge-card:0": {
					ID:     "run-123:charge-card:0",
					Name:   "charge-card",
					Status: "completed",
					Output: map[string]any{"transactionId": "txn-456", "amount": 100},
				},
			},
			executedSteps: make([]*StepResult, 0),
		},
	}

	result, err := Invoke[map[string]any](ctx, "charge-card", map[string]any{"amount": 100})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["transactionId"] != "txn-456" {
		t.Errorf("expected transactionId 'txn-456', got '%v'", result["transactionId"])
	}
}

// TestInvoke_ReturnsInvokeErrorWhenFailed verifies that Invoke() returns an
// *InvokeError when the step is memoized as failed.
func TestInvoke_ReturnsInvokeErrorWhenFailed(t *testing.T) {
	ctx := Context{
		exec: &executionContext{
			runID:        "run-123",
			stepCounters: make(map[string]int),
			completedSteps: map[string]*CompletedStep{
				"run-123:charge-card:0": {
					ID:     "run-123:charge-card:0",
					Name:   "charge-card",
					Status: "failed",
					Error:  map[string]any{"message": "card declined", "function_id": "charge-card", "child_run_id": "run-child-1"},
				},
			},
			executedSteps: make([]*StepResult, 0),
		},
	}

	_, err := Invoke[map[string]any](ctx, "charge-card", map[string]any{"amount": 100})
	if err == nil {
		t.Fatal("expected error for failed step")
	}

	invokeErr, ok := err.(*InvokeError)
	if !ok {
		t.Fatalf("expected *InvokeError, got %T: %v", err, err)
	}
	if invokeErr.FunctionID != "charge-card" {
		t.Errorf("expected FunctionID 'charge-card', got '%s'", invokeErr.FunctionID)
	}
	if invokeErr.ChildRunID != "run-child-1" {
		t.Errorf("expected ChildRunID 'run-child-1', got '%s'", invokeErr.ChildRunID)
	}
	if invokeErr.Cause != "card declined" {
		t.Errorf("expected Cause 'card declined', got '%s'", invokeErr.Cause)
	}
}

// TestInvoke_ReturnsInvokeErrorWhenTimedOut verifies that Invoke() returns an
// *InvokeError with cause "invoke timed out" when the step is memoized as timed_out.
func TestInvoke_ReturnsInvokeErrorWhenTimedOut(t *testing.T) {
	req := &PushRequest{
		RunID:      "run-123",
		FunctionID: "caller",
		Attempt:    1,
		Event:      PushEvent{ID: "evt-1", Name: "test.event"},
		Steps: []CompletedStep{
			{
				ID:     "run-123:charge-card:0",
				Name:   "charge-card",
				Status: "timed_out",
			},
		},
	}
	ctx := NewContextForTest(req)

	_, err := Invoke[map[string]any](ctx, "charge-card", map[string]any{"amount": 100})
	if err == nil {
		t.Fatal("expected error")
	}
	invokeErr, ok := err.(*InvokeError)
	if !ok {
		t.Fatalf("expected *InvokeError, got %T: %v", err, err)
	}
	if invokeErr.Cause != "invoke timed out" {
		t.Errorf("expected cause 'invoke timed out', got %q", invokeErr.Cause)
	}
}

// TestInvokeAsync_YieldsWithCorrectType verifies that InvokeAsync() panics
// with a yieldSignal of type "invoke_function_async" when not memoized.
func TestInvokeAsync_YieldsWithCorrectType(t *testing.T) {
	ctx := Context{
		exec: &executionContext{
			runID:          "run-123",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		},
	}

	var capturedSignal *yieldSignal

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic with yield signal")
			}
			signal, ok := r.(*yieldSignal)
			if !ok {
				t.Fatalf("expected *yieldSignal, got %T", r)
			}
			capturedSignal = signal
		}()

		InvokeAsync(ctx, "send-notification", map[string]any{"message": "hello"})
	}()

	if capturedSignal == nil {
		t.Fatal("expected yield signal to be captured")
	}
	if capturedSignal.info.Type != ResumeTypeInvokeFunctionAsync {
		t.Errorf("expected type 'invoke_function_async', got '%s'", capturedSignal.info.Type)
	}
	if capturedSignal.info.FunctionID != "send-notification" {
		t.Errorf("expected FunctionID 'send-notification', got '%s'", capturedSignal.info.FunctionID)
	}
	if capturedSignal.info.StepID != "run-123:send-notification:0" {
		t.Errorf("expected StepID 'run-123:send-notification:0', got '%s'", capturedSignal.info.StepID)
	}
}

// TestInvokeAsync_ReturnsInvokeErrorWhenFailed verifies that InvokeAsync() returns
// an *InvokeError when the step is memoized as failed (e.g. function not found).
func TestInvokeAsync_ReturnsInvokeErrorWhenFailed(t *testing.T) {
	ctx := Context{
		exec: &executionContext{
			runID:        "run-123",
			stepCounters: make(map[string]int),
			completedSteps: map[string]*CompletedStep{
				"run-123:send-notification:0": {
					ID:     "run-123:send-notification:0",
					Name:   "send-notification",
					Status: "failed",
					Error:  map[string]any{"message": "target function 'send-notification' not found"},
				},
			},
			executedSteps: make([]*StepResult, 0),
		},
	}

	_, err := InvokeAsync(ctx, "send-notification", map[string]any{"message": "hello"})
	if err == nil {
		t.Fatal("expected error for failed async step")
	}
	invokeErr, ok := err.(*InvokeError)
	if !ok {
		t.Fatalf("expected *InvokeError, got %T: %v", err, err)
	}
	if invokeErr.FunctionID != "send-notification" {
		t.Errorf("expected FunctionID 'send-notification', got '%s'", invokeErr.FunctionID)
	}
	if invokeErr.Cause != "target function 'send-notification' not found" {
		t.Errorf("unexpected Cause: %s", invokeErr.Cause)
	}
}

// TestInvokeAsync_ReturnsMemoizedRunID verifies that InvokeAsync() returns
// the memoized run_id when the step is already completed.
func TestInvokeAsync_ReturnsMemoizedRunID(t *testing.T) {
	ctx := Context{
		exec: &executionContext{
			runID:        "run-123",
			stepCounters: make(map[string]int),
			completedSteps: map[string]*CompletedStep{
				"run-123:send-notification:0": {
					ID:     "run-123:send-notification:0",
					Name:   "send-notification",
					Status: "completed",
					Output: map[string]any{"run_id": "run-child-789"},
				},
			},
			executedSteps: make([]*StepResult, 0),
		},
	}

	result, err := InvokeAsync(ctx, "send-notification", map[string]any{"message": "hello"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RunID != "run-child-789" {
		t.Errorf("expected RunID 'run-child-789', got '%s'", result.RunID)
	}
}
