package ironflowtest

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sahina/ironflow-go/ironflow"
)

// Config configures the test client.
type Config struct {
	Functions []ironflow.Function
}

// TestClient runs workflow functions in-memory for testing.
type TestClient struct {
	functions   map[string][]ironflow.Function // event -> functions
	stepMocks   map[string]func() (any, error)
	invokeMocks map[string]func(any) (any, error)
	eventQueue  map[string][]any
	mu          sync.Mutex
}

// NewClient creates a new test client.
func NewClient(t *testing.T, config Config) *TestClient {
	tc := &TestClient{
		functions:   make(map[string][]ironflow.Function),
		stepMocks:   make(map[string]func() (any, error)),
		invokeMocks: make(map[string]func(any) (any, error)),
		eventQueue:  make(map[string][]any),
	}

	for _, fn := range config.Functions {
		for _, trigger := range fn.Config.Triggers {
			tc.functions[trigger.Event] = append(tc.functions[trigger.Event], fn)
		}
	}

	t.Cleanup(func() {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		tc.stepMocks = make(map[string]func() (any, error))
		tc.invokeMocks = make(map[string]func(any) (any, error))
		tc.eventQueue = make(map[string][]any)
	})

	return tc
}

// MockStep registers a mock for a step.run() call.
func (tc *TestClient) MockStep(name string, fn func() (any, error)) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.stepMocks[name] = fn
}

// MockInvoke registers a mock for a step.invoke() call.
func (tc *TestClient) MockInvoke(functionID string, fn func(data any) (any, error)) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.invokeMocks[functionID] = fn
}

// SendEvent pre-registers an event for step.waitForEvent().
func (tc *TestClient) SendEvent(eventName string, data any) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.eventQueue[eventName] = append(tc.eventQueue[eventName], data)
}

// Emit triggers a function execution and returns the result.
func (tc *TestClient) Emit(t *testing.T, eventName string, data any) *TestRun {
	t.Helper()

	tc.mu.Lock()
	fns, ok := tc.functions[eventName]

	// Snapshot mocks and event queue for thread safety
	stepMocks := make(map[string]func() (any, error), len(tc.stepMocks))
	maps.Copy(stepMocks, tc.stepMocks)
	invokeMocks := make(map[string]func(any) (any, error), len(tc.invokeMocks))
	maps.Copy(invokeMocks, tc.invokeMocks)
	eventQueue := make(map[string][]any, len(tc.eventQueue))
	for k, v := range tc.eventQueue {
		eventQueue[k] = append([]any(nil), v...)
	}
	tc.mu.Unlock()

	if !ok || len(fns) == 0 {
		t.Fatalf("No function registered for event %q", eventName)
		return nil
	}

	fn := fns[0]

	// Marshal event data
	rawData, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Failed to marshal event data: %v", err)
		return nil
	}

	runID := fmt.Sprintf("test-run-%d", time.Now().UnixNano())

	event := ironflow.Event{
		ID:        fmt.Sprintf("test-evt-%d", time.Now().UnixNano()),
		Name:      eventName,
		Version:   1,
		RawData:   rawData,
		Timestamp: time.Now(),
		Source:    ironflow.EventSourceType("test"),
	}

	// Create interceptor with snapshot copies
	interceptor := &testInterceptor{
		stepMocks:   stepMocks,
		invokeMocks: invokeMocks,
		eventQueue:  eventQueue,
	}

	ctx := ironflow.NewTestContext(event, runID, fn.Config.ID, interceptor)

	// Execute handler
	output, execErr := fn.Handler(ctx)

	if execErr != nil {
		// Run compensations in reverse
		compensationsRan := []string{}
		for _, entry := range slices.Backward(interceptor.compensations) {
			compErr := entry.fn()
			compensationsRan = append(compensationsRan, entry.stepName)
			interceptor.steps = append(interceptor.steps, TestStep{
				Name:  "compensate:" + entry.stepName,
				Type:  "compensate",
				Error: compErr,
			})
		}

		return &TestRun{
			Status:           "failed",
			Steps:            interceptor.steps,
			Error:            execErr,
			CompensationsRan: compensationsRan,
		}
	}

	return &TestRun{
		Status:           "completed",
		Steps:            interceptor.steps,
		Output:           output,
		CompensationsRan: []string{},
	}
}

// testInterceptor implements ironflow.TestInterceptor
type testInterceptor struct {
	stepMocks     map[string]func() (any, error)
	invokeMocks   map[string]func(any) (any, error)
	eventQueue    map[string][]any
	steps         []TestStep
	compensations []compensationEntry
	mu            sync.Mutex
}

type compensationEntry struct {
	stepName string
	fn       func() error
}

func (ti *testInterceptor) RunStep(name string) (any, error) {
	mock, ok := ti.stepMocks[name]
	if !ok {
		return nil, fmt.Errorf(
			"step %q was called but has no mock, use tc.MockStep(%q, fn) to provide one",
			name, name,
		)
	}
	return mock()
}

func (ti *testInterceptor) SleepStep(_ string) {
	// No-op: sleep resolves immediately in tests
}

func (ti *testInterceptor) WaitForEventStep(name string, filter ironflow.EventFilter) (ironflow.Event, error) {
	ti.mu.Lock()
	events, ok := ti.eventQueue[filter.Event]
	if !ok || len(events) == 0 {
		ti.mu.Unlock()
		return ironflow.Event{}, fmt.Errorf(
			"waitForEvent(%q) is waiting for %q but no events were pre-registered, "+
				"use tc.SendEvent(%q, data) before calling tc.Emit()",
			name, filter.Event, filter.Event,
		)
	}
	data := events[0]
	ti.eventQueue[filter.Event] = events[1:]
	ti.mu.Unlock()

	rawData, _ := json.Marshal(data)
	return ironflow.Event{
		ID:        fmt.Sprintf("test-evt-%d", time.Now().UnixNano()),
		Name:      filter.Event,
		Version:   1,
		RawData:   rawData,
		Timestamp: time.Now(),
		Source:    ironflow.EventSourceType("test"),
	}, nil
}

func (ti *testInterceptor) InvokeStep(functionID string, input any) (any, error) {
	mock, ok := ti.invokeMocks[functionID]
	if !ok {
		return nil, fmt.Errorf(
			"invoke(%q) was called but has no mock, use tc.MockInvoke(%q, fn) to provide one",
			functionID, functionID,
		)
	}
	return mock(input)
}

func (ti *testInterceptor) InvokeAsyncStep(functionID string, input any) (ironflow.InvokeAsyncResult, error) {
	mock, ok := ti.invokeMocks[functionID]
	if !ok {
		return ironflow.InvokeAsyncResult{}, fmt.Errorf(
			"invokeAsync(%q) was called but has no mock, use tc.MockInvoke(%q, fn) to provide one",
			functionID, functionID,
		)
	}
	_, err := mock(input)
	if err != nil {
		return ironflow.InvokeAsyncResult{}, err
	}
	return ironflow.InvokeAsyncResult{
		RunID: fmt.Sprintf("test-run-%d", time.Now().UnixNano()),
	}, nil
}

func (ti *testInterceptor) CompensateStep(stepName string, fn func() error) {
	ti.mu.Lock()
	defer ti.mu.Unlock()
	ti.compensations = append(ti.compensations, compensationEntry{
		stepName: stepName,
		fn:       fn,
	})
}

func (ti *testInterceptor) RecordStep(name, stepType string, output any, err error) {
	ti.mu.Lock()
	defer ti.mu.Unlock()
	ti.steps = append(ti.steps, TestStep{
		Name:   name,
		Type:   stepType,
		Output: output,
		Error:  err,
	})
}
