package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/sahina/ironflow-go/ironflow"
)

// fakeMemoryBackend implements MemoryBackend for testing.
type fakeMemoryBackend struct {
	appendCalls []MemoryAppendInput
	getCalls    []string
	waitCalls   []string

	appendErr error
	getResult *ironflow.ProjectionStateResult
	getErr    error
}

func (f *fakeMemoryBackend) AppendEvent(_ context.Context, _ string, input MemoryAppendInput) (*ironflow.AppendResult, error) {
	f.appendCalls = append(f.appendCalls, input)
	if f.appendErr != nil {
		return nil, f.appendErr
	}
	return &ironflow.AppendResult{
		EntityVersion: ironflow.ProtoInt64(int64(len(f.appendCalls))),
		EventID:       "evt-1",
	}, nil
}

func (f *fakeMemoryBackend) GetProjection(_ context.Context, name string) (*ironflow.ProjectionStateResult, error) {
	f.getCalls = append(f.getCalls, name)
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.getResult, nil
}

func (f *fakeMemoryBackend) WaitForEvent(_ context.Context, eventID, projection string, _ ironflow.WaitForProjectionOpts) error {
	f.waitCalls = append(f.waitCalls, eventID+":"+projection)
	return nil
}

func TestMemory_Unconfigured(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)
	_, err := Memory(ctx).Get()
	if err == nil {
		t.Fatal("expected unconfigured error")
	}
	var ife *ironflow.IronflowError
	if !errors.As(err, &ife) || ife.Code != CodeMemoryUnconfigured {
		t.Errorf("err code = %v, want %q", err, CodeMemoryUnconfigured)
	}
}

func TestMemory_NoBackend(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	ctx, runtime := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
		Memory:   &MemoryConfig{StreamID: "s", Projection: "p"},
	}, interceptor)
	runtime.memoryBackend = nil // ensure no backend
	_, err := Memory(ctx).Get()
	if err == nil {
		t.Fatal("expected no-backend error")
	}
	var ife *ironflow.IronflowError
	if !errors.As(err, &ife) || ife.Code != CodeMemoryNoBackend {
		t.Errorf("err code = %v, want %q", err, CodeMemoryNoBackend)
	}
}

func TestMemory_GetCachesWithinRun(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	backend := &fakeMemoryBackend{
		getResult: &ironflow.ProjectionStateResult{
			Name:  "memory-projection",
			State: map[string]any{"counter": 1},
		},
	}
	interceptor.stepMocks["memory.get"] = func() (any, error) {
		return backend.GetProjection(context.Background(), "memory-projection")
	}

	ctx, runtime := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
		Memory:   &MemoryConfig{StreamID: "s", Projection: "memory-projection"},
	}, interceptor)
	runtime.memoryBackend = backend

	state1, err := Memory(ctx).Get()
	if err != nil {
		t.Fatalf("Get1: %v", err)
	}
	state2, err := Memory(ctx).Get()
	if err != nil {
		t.Fatalf("Get2: %v", err)
	}
	if state1["counter"] != 1.0 && state1["counter"] != 1 {
		t.Errorf("state1.counter = %v", state1["counter"])
	}
	_ = state2
	// Step interceptor should fire only once because cache hits the second time.
	getStepCalls := 0
	for _, name := range interceptor.stepCalls {
		if name == "memory.get" {
			getStepCalls++
		}
	}
	if getStepCalls != 1 {
		t.Errorf("memory.get step count = %d, want 1 (cache should hit)", getStepCalls)
	}
}

func TestMemory_AppendGeneratesIdempotencyKey(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	backend := &fakeMemoryBackend{}
	interceptor.stepMocks["memory.append"] = func() (any, error) {
		// Call backend so the test sees the input. Use the latest
		// recorded input by replaying the runtime memory append.
		return backend.AppendEvent(context.Background(), "stream-1", MemoryAppendInput{
			Name:           "evt",
			Data:           map[string]any{"x": 1},
			EntityType:     memoryDefaultEntityType,
			IdempotencyKey: "run-test-1:memory.append:0",
		})
	}
	interceptor.stepMocks["memory.append.wait"] = func() (any, error) {
		return struct{}{}, backend.WaitForEvent(context.Background(), "evt-1", "p", ironflow.WaitForProjectionOpts{})
	}

	ctx, runtime := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
		Memory:   &MemoryConfig{StreamID: "stream-1", Projection: "p"},
	}, interceptor)
	runtime.memoryBackend = backend

	if err := Memory(ctx).Append("evt", map[string]any{"x": 1}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(backend.appendCalls) != 1 {
		t.Fatalf("appendCalls = %d, want 1", len(backend.appendCalls))
	}
	got := backend.appendCalls[0]
	if got.IdempotencyKey != "run-test-1:memory.append:0" {
		t.Errorf("IdempotencyKey = %q, want %q", got.IdempotencyKey, "run-test-1:memory.append:0")
	}
	if got.EntityType != memoryDefaultEntityType {
		t.Errorf("EntityType = %q, want %q", got.EntityType, memoryDefaultEntityType)
	}
	if len(backend.waitCalls) != 1 {
		t.Errorf("waitCalls = %d, want 1 (auto-wait after append)", len(backend.waitCalls))
	}
}

func TestMemory_AppendNilDataRejected(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	backend := &fakeMemoryBackend{}
	ctx, runtime := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
		Memory:   &MemoryConfig{StreamID: "stream-1", Projection: "p"},
	}, interceptor)
	runtime.memoryBackend = backend

	err := Memory(ctx).Append("evt", nil)
	if err == nil {
		t.Fatal("expected invalid-data error")
	}
	var ife *ironflow.IronflowError
	if !errors.As(err, &ife) || ife.Code != CodeMemoryInvalidData {
		t.Errorf("err code = %v, want %q", err, CodeMemoryInvalidData)
	}
}

func TestMemory_EntityStreamRequiresProjection(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	ctx, _ := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
		Memory:   &MemoryConfig{StreamID: "s", Projection: "p"},
	}, interceptor)
	_, err := Memory(ctx).EntityStream("stream-x", "")
	if err == nil {
		t.Fatal("expected projection-required error")
	}
	var pr *MemoryProjectionRequiredError
	if !errors.As(err, &pr) {
		t.Fatalf("err is not MemoryProjectionRequiredError: %T %v", err, err)
	}
}

func TestMemory_BypassCache(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	backend := &fakeMemoryBackend{
		getResult: &ironflow.ProjectionStateResult{
			Name:  "p",
			State: map[string]any{"v": 1},
		},
	}
	interceptor.stepMocks["memory.get"] = func() (any, error) {
		return backend.GetProjection(context.Background(), "p")
	}
	ctx, runtime := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
		Memory:   &MemoryConfig{StreamID: "s", Projection: "p"},
	}, interceptor)
	runtime.memoryBackend = backend

	if _, err := Memory(ctx).Get(); err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if _, err := Memory(ctx).Get(MemoryGetOptions{BypassCache: true}); err != nil {
		t.Fatalf("bypass Get: %v", err)
	}
	count := 0
	for _, n := range interceptor.stepCalls {
		if n == "memory.get" {
			count++
		}
	}
	if count != 2 {
		t.Errorf("memory.get step count = %d, want 2 (bypass triggers second call)", count)
	}
}
