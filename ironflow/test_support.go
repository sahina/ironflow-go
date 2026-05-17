package ironflow

import (
	"encoding/json"
	"fmt"
	"time"
)

// TestInterceptor allows test frameworks to intercept step execution.
// This is implemented by ironflowtest.TestClient.
type TestInterceptor interface {
	// RunStep intercepts ironflow.Run calls. Returns (output, error).
	RunStep(name string) (any, error)
	// SleepStep intercepts ironflow.Sleep calls. Returns immediately.
	SleepStep(name string)
	// WaitForEventStep intercepts ironflow.WaitForEvent calls. Returns the event.
	WaitForEventStep(name string, filter EventFilter) (Event, error)
	// InvokeStep intercepts ironflow.Invoke calls. Returns (output, error).
	InvokeStep(functionID string, input any) (any, error)
	// InvokeAsyncStep intercepts ironflow.InvokeAsync calls. Returns (result, error).
	InvokeAsyncStep(functionID string, input any) (InvokeAsyncResult, error)
	// CompensateStep intercepts ironflow.Compensate calls.
	CompensateStep(stepName string, fn func() error)
	// RecordStep records a step execution for TestRun.
	RecordStep(name, stepType string, output any, err error)
}

// NewTestContext creates a Context with a test interceptor.
// Used internally by ironflowtest.NewClient.
func NewTestContext(event Event, runID, functionID string, interceptor TestInterceptor) Context {
	return Context{
		Event: event,
		Run: RunInfo{
			ID:         runID,
			FunctionID: functionID,
			Attempt:    1,
			StartedAt:  time.Now(),
		},
		Secrets: NewSecretsReader(nil),
		exec: &executionContext{
			runID:           runID,
			functionID:      functionID,
			attempt:         1,
			stepCounters:    make(map[string]int),
			completedSteps:  make(map[string]*CompletedStep),
			executedSteps:   make([]*StepResult, 0),
			testInterceptor: interceptor,
		},
	}
}

// testRunStep is the test-mode implementation for Run[T].
func testRunStep[T any](exec *executionContext, name string) (T, error) {
	var zero T
	output, err := exec.testInterceptor.RunStep(name)
	if err != nil {
		exec.testInterceptor.RecordStep(name, "run", nil, err)
		return zero, err
	}
	exec.testInterceptor.RecordStep(name, "run", output, nil)

	// Type assertion with JSON fallback
	if result, ok := output.(T); ok {
		return result, nil
	}
	b, marshalErr := json.Marshal(output)
	if marshalErr != nil {
		return zero, fmt.Errorf("test mock for step %q: marshal error: %w", name, marshalErr)
	}
	var result T
	if unmarshalErr := json.Unmarshal(b, &result); unmarshalErr != nil {
		return zero, fmt.Errorf("test mock for step %q returned incompatible type: %w", name, unmarshalErr)
	}
	return result, nil
}

// testInvokeStep is the test-mode implementation for Invoke[T].
func testInvokeStep[T any](exec *executionContext, functionID string, input any) (T, error) {
	var zero T
	output, err := exec.testInterceptor.InvokeStep(functionID, input)
	if err != nil {
		exec.testInterceptor.RecordStep(functionID, "invoke", nil, err)
		return zero, err
	}
	exec.testInterceptor.RecordStep(functionID, "invoke", output, nil)

	if result, ok := output.(T); ok {
		return result, nil
	}
	b, marshalErr := json.Marshal(output)
	if marshalErr != nil {
		return zero, fmt.Errorf("test mock for invoke %q: marshal error: %w", functionID, marshalErr)
	}
	var result T
	if unmarshalErr := json.Unmarshal(b, &result); unmarshalErr != nil {
		return zero, fmt.Errorf("test mock for invoke %q returned incompatible type: %w", functionID, unmarshalErr)
	}
	return result, nil
}
