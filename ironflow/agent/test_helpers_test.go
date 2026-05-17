package agent

import (
	"errors"
	"testing"
	"time"

	"github.com/sahina/ironflow-go/ironflow"
)

// fakeInterceptor implements ironflow.TestInterceptor for agent unit
// tests. Test code wires in handlers per step name to capture inputs
// and feed back outputs.
type fakeInterceptor struct {
	t *testing.T

	stepMocks   map[string]func() (any, error)
	invokeMocks map[string]func(input any) (any, error)
	asyncMocks  map[string]func(input any) (ironflow.InvokeAsyncResult, error)
	waitMocks   map[string]func(filter ironflow.EventFilter) (ironflow.Event, error)

	stepCalls   []string
	invokeCalls []string
	asyncCalls  []string
	waitCalls   []string
}

func newFakeInterceptor(t *testing.T) *fakeInterceptor {
	return &fakeInterceptor{
		t:           t,
		stepMocks:   map[string]func() (any, error){},
		invokeMocks: map[string]func(input any) (any, error){},
		asyncMocks:  map[string]func(input any) (ironflow.InvokeAsyncResult, error){},
		waitMocks:   map[string]func(filter ironflow.EventFilter) (ironflow.Event, error){},
	}
}

func (f *fakeInterceptor) RunStep(name string) (any, error) {
	f.stepCalls = append(f.stepCalls, name)
	if mock, ok := f.stepMocks[name]; ok {
		return mock()
	}
	return nil, nil
}

func (f *fakeInterceptor) SleepStep(_ string) {}

func (f *fakeInterceptor) WaitForEventStep(name string, filter ironflow.EventFilter) (ironflow.Event, error) {
	f.waitCalls = append(f.waitCalls, name)
	if mock, ok := f.waitMocks[name]; ok {
		return mock(filter)
	}
	return ironflow.Event{}, nil
}

func (f *fakeInterceptor) InvokeStep(functionID string, input any) (any, error) {
	f.invokeCalls = append(f.invokeCalls, functionID)
	if mock, ok := f.invokeMocks[functionID]; ok {
		return mock(input)
	}
	return nil, errors.New("no invoke mock for " + functionID)
}

func (f *fakeInterceptor) InvokeAsyncStep(functionID string, input any) (ironflow.InvokeAsyncResult, error) {
	f.asyncCalls = append(f.asyncCalls, functionID)
	if mock, ok := f.asyncMocks[functionID]; ok {
		return mock(input)
	}
	return ironflow.InvokeAsyncResult{}, errors.New("no async mock for " + functionID)
}

func (f *fakeInterceptor) CompensateStep(_ string, _ func() error) {}

func (f *fakeInterceptor) RecordStep(_, _ string, _ any, _ error) {}

// newAgentContext builds an agent.Context bound to a fresh runtime.
// The interceptor and config let tests drive helpers without spinning
// up a full ironflow.RegisterFunction / Emit cycle.
func newAgentContext(t *testing.T, cfg AgentConfig, interceptor ironflow.TestInterceptor) (Context, *agentRuntime) {
	t.Helper()

	registry, err := buildRegistry(cfg.Tools)
	if err != nil {
		t.Fatalf("buildRegistry: %v", err)
	}
	maxTurns := cfg.MaxTurns
	if maxTurns <= 0 {
		maxTurns = DefaultMaxTurns
	}

	runtime := &agentRuntime{
		maxTurns:     maxTurns,
		registry:     registry,
		byArgsCache:  make(map[string]any),
		byArgsErr:    make(map[string]error),
		memoryConfig: cfg.Memory,
	}

	event := ironflow.Event{
		ID:        "test-evt",
		Name:      "test.event",
		Version:   1,
		RawData:   []byte(`{}`),
		Timestamp: time.Now(),
		Source:    ironflow.EventSourceType("test"),
	}
	ictx := ironflow.NewTestContext(event, "run-test-1", "fn-test", interceptor)
	return Context{Inner: ictx, runtime: runtime}, runtime
}
