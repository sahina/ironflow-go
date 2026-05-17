package ironflow

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestExecContext() *executionContext {
	return &executionContext{
		runID:          "run-1",
		functionID:     "fn-1",
		stepCounters:   make(map[string]int),
		completedSteps: make(map[string]*CompletedStep),
		executedSteps:  make([]*StepResult, 0),
	}
}

func TestCompensateRegistration(t *testing.T) {
	exec := newTestExecContext()
	ctx := Context{exec: exec}

	Compensate(ctx, "step-1", func() error { return nil })

	if !exec.hasCompensations() {
		t.Fatal("expected compensations to be registered")
	}
}

func TestCompensateNoCompensationsRegistered(t *testing.T) {
	exec := newTestExecContext()

	if exec.hasCompensations() {
		t.Error("expected no compensations")
	}

	// Should be safe to call with no compensations
	exec.executeCompensations()

	if len(exec.executedSteps) != 0 {
		t.Errorf("expected 0 executed steps, got %d", len(exec.executedSteps))
	}
}

func TestCompensateReverseOrder(t *testing.T) {
	exec := newTestExecContext()

	order := []string{}

	exec.compensations = []compensationEntry{
		{stepName: "step-1", fn: func() error { order = append(order, "comp-1"); return nil }},
		{stepName: "step-2", fn: func() error { order = append(order, "comp-2"); return nil }},
		{stepName: "step-3", fn: func() error { order = append(order, "comp-3"); return nil }},
	}

	exec.executeCompensations()

	expected := []string{"comp-3", "comp-2", "comp-1"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d compensations, got %d", len(expected), len(order))
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("expected order[%d] = %s, got %s", i, v, order[i])
		}
	}
}

func TestCompensateStepRecorded(t *testing.T) {
	exec := newTestExecContext()

	exec.compensations = []compensationEntry{
		{stepName: "step-1", fn: func() error { return nil }},
	}

	exec.executeCompensations()

	if len(exec.executedSteps) != 1 {
		t.Fatalf("expected 1 executed step, got %d", len(exec.executedSteps))
	}

	step := exec.executedSteps[0]
	if step.Type != "compensate" {
		t.Errorf("expected type 'compensate', got '%s'", step.Type)
	}
	if step.Name != "compensate:step-1" {
		t.Errorf("expected name 'compensate:step-1', got '%s'", step.Name)
	}
	if step.CompensationFor != "step-1" {
		t.Errorf("expected compensation_for 'step-1', got '%s'", step.CompensationFor)
	}
	if step.Status != "completed" {
		t.Errorf("expected status 'completed', got '%s'", step.Status)
	}
	if step.StartedAt.IsZero() {
		t.Error("expected StartedAt to be set")
	}
	if step.EndedAt == nil {
		t.Error("expected EndedAt to be set")
	}
}

func TestCompensateMemoization(t *testing.T) {
	called := false
	exec := newTestExecContext()

	// Pre-populate completed step (simulating memoization from previous attempt)
	exec.completedSteps["run-1:compensate:step-1:0"] = &CompletedStep{
		ID:     "run-1:compensate:step-1:0",
		Name:   "compensate:step-1",
		Status: "completed",
	}

	exec.compensations = []compensationEntry{
		{stepName: "step-1", fn: func() error { called = true; return nil }},
	}

	exec.executeCompensations()

	if called {
		t.Error("compensation should have been skipped due to memoization")
	}
}

func TestCompensateFailureContinues(t *testing.T) {
	exec := newTestExecContext()

	order := []string{}

	exec.compensations = []compensationEntry{
		{stepName: "step-1", fn: func() error { order = append(order, "comp-1"); return nil }},
		{stepName: "step-2", fn: func() error { return errors.New("comp-2 failed") }},
		{stepName: "step-3", fn: func() error { order = append(order, "comp-3"); return nil }},
	}

	exec.executeCompensations()

	// step-3 runs first (reverse), then step-2 (fails), then step-1 continues
	expected := []string{"comp-3", "comp-1"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d successful compensations, got %d", len(expected), len(order))
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("expected order[%d] = %s, got %s", i, v, order[i])
		}
	}

	// Should have 3 executed steps (2 succeeded, 1 failed)
	if len(exec.executedSteps) != 3 {
		t.Fatalf("expected 3 executed steps, got %d", len(exec.executedSteps))
	}

	// Find the failed compensation
	var failedStep *StepResult
	for _, s := range exec.executedSteps {
		if s.Name == "compensate:step-2" {
			failedStep = s
			break
		}
	}
	if failedStep == nil {
		t.Fatal("expected to find failed compensation step")
	}
	if failedStep.Status != "failed" {
		t.Errorf("expected failed status, got '%s'", failedStep.Status)
	}
	if failedStep.Error == nil || failedStep.Error.Message != "comp-2 failed" {
		t.Error("expected error message 'comp-2 failed'")
	}
	if failedStep.Error.Retryable {
		t.Error("expected compensation error to be non-retryable")
	}
	if failedStep.CompensationFor != "step-2" {
		t.Errorf("expected compensation_for 'step-2', got '%s'", failedStep.CompensationFor)
	}
}

func TestCompensateDurationRecorded(t *testing.T) {
	exec := newTestExecContext()

	exec.compensations = []compensationEntry{
		{stepName: "step-1", fn: func() error { return nil }},
	}

	exec.executeCompensations()

	if len(exec.executedSteps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(exec.executedSteps))
	}
	s := exec.executedSteps[0]
	if s.EndedAt == nil {
		t.Error("expected EndedAt to be set")
	}
	if s.Duration < 0 {
		t.Errorf("expected non-negative duration, got %v", s.Duration)
	}
}

func TestCompensateStepIDFormat(t *testing.T) {
	exec := newTestExecContext()

	exec.compensations = []compensationEntry{
		{stepName: "charge-payment", fn: func() error { return nil }},
	}

	exec.executeCompensations()

	if len(exec.executedSteps) != 1 {
		t.Fatalf("expected 1 step")
	}
	// Step ID must be: runID:compensate:stepName:counter
	expected := "run-1:compensate:charge-payment:0"
	if exec.executedSteps[0].ID != expected {
		t.Errorf("expected step ID %q, got %q", expected, exec.executedSteps[0].ID)
	}
}

// TestCompensateInBranch verifies CompensateInBranch delegates to the parent context.
func TestCompensateInBranch(t *testing.T) {
	exec := newTestExecContext()
	branch := exec.createBranchContext("parallel-op", 0)

	called := false
	CompensateInBranch(branch, "step-1", func() error {
		called = true
		return nil
	})

	// Compensation should be registered on the parent
	if !exec.hasCompensations() {
		t.Fatal("expected compensation registered on parent context")
	}

	// Executing should invoke our function
	exec.executeCompensations()
	if !called {
		t.Error("expected compensation function to be called")
	}
}

// TestServeHTTP_CompensationOnNonRetryableError verifies that when a function
// registers compensations and fails with a non-retryable error, the compensation
// steps are executed and included in the response.
func TestServeHTTP_CompensationOnNonRetryableError(t *testing.T) {
	compensated := false

	fn := CreateFunction(FunctionConfig{
		ID:       "saga-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		_, _ = Run(ctx, "charge", func() (string, error) {
			return "tx-123", nil
		})
		Compensate(ctx, "charge", func() error {
			compensated = true
			return nil
		})
		return nil, NewError("shipping failed", "SHIPPING_FAILED", false)
	})

	handler := Serve(ServeConfig{
		Functions:        []Function{fn},
		SkipVerification: true,
	})

	body := validPushBody("saga-fn")
	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !compensated {
		t.Error("expected compensation to run on non-retryable error")
	}

	var resp PushResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp.Status != "failed" {
		t.Errorf("expected status 'failed', got %q", resp.Status)
	}

	// Response steps must include the compensation step
	var compStep *StepResult
	for _, s := range resp.Steps {
		if s.Type == "compensate" {
			compStep = s
			break
		}
	}
	if compStep == nil {
		t.Fatal("expected compensation step in response steps")
	}
	if compStep.CompensationFor != "charge" {
		t.Errorf("expected compensation_for 'charge', got %q", compStep.CompensationFor)
	}
	if compStep.Status != "completed" {
		t.Errorf("expected compensation status 'completed', got %q", compStep.Status)
	}
}

// TestServeHTTP_NoCompensationOnRetryableError verifies that compensations are NOT
// executed when the error is retryable (transient failures should be retried, not undone).
func TestServeHTTP_NoCompensationOnRetryableError(t *testing.T) {
	compensated := false

	fn := CreateFunction(FunctionConfig{
		ID:       "retryable-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		_, _ = Run(ctx, "charge", func() (string, error) {
			return "tx-123", nil
		})
		Compensate(ctx, "charge", func() error {
			compensated = true
			return nil
		})
		// Regular error — retryable by default
		return nil, errors.New("temporary network failure")
	})

	handler := Serve(ServeConfig{
		Functions:        []Function{fn},
		SkipVerification: true,
	})

	body := validPushBody("retryable-fn")
	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if compensated {
		t.Error("compensation must NOT run on retryable error")
	}

	var resp PushResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	// No compensation steps should appear in the response
	for _, s := range resp.Steps {
		if s.Type == "compensate" {
			t.Errorf("unexpected compensation step %q in response", s.Name)
		}
	}
}
