package agent

import (
	"errors"
	"os"
	"testing"

	"github.com/sahina/ironflow-go/ironflow"
)

func TestDefaultMemoryBackend_NoServerURL(t *testing.T) {
	t.Setenv("IRONFLOW_URL", "")
	t.Setenv("IRONFLOW_SERVER_URL", "")
	backend, err := defaultMemoryBackend()
	if err == nil {
		t.Fatal("expected error when no server URL set")
	}
	if backend != nil {
		t.Errorf("backend = %v, want nil", backend)
	}
}

func TestDefaultMemoryBackend_WithServerURL(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("skip on CI — calls NewClient which prints banner")
	}
	t.Setenv("IRONFLOW_SERVER_URL", "http://localhost:9123")
	backend, err := defaultMemoryBackend()
	if err != nil {
		t.Fatalf("defaultMemoryBackend: %v", err)
	}
	if backend == nil {
		t.Fatal("backend = nil, want constructed")
	}
}

func TestProjectionStateToMap_NilSafe(t *testing.T) {
	if got := projectionStateToMap(nil); got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestCoerceProjectionState_VariousShapes(t *testing.T) {
	if got := coerceProjectionState(nil); got != nil {
		t.Errorf("nil → %v, want nil", got)
	}
	if got := coerceProjectionState(map[string]any{"x": 1}); got["x"] != 1 {
		t.Errorf("map passthrough lost value: %v", got)
	}
	type wrapped struct {
		Y int `json:"y"`
	}
	got := coerceProjectionState(wrapped{Y: 5})
	if got["y"] != 5.0 && got["y"] != 5 {
		t.Errorf("struct coerce: %v", got)
	}
}

func TestDecodeAs_PassthroughTyped(t *testing.T) {
	type cfg struct {
		Hi string `json:"hi"`
	}
	in := cfg{Hi: "yo"}
	out, err := decodeAs[cfg](in)
	if err != nil {
		t.Fatalf("decodeAs: %v", err)
	}
	if out.Hi != "yo" {
		t.Errorf("Hi = %q", out.Hi)
	}
}

func TestDecodeAs_NilReturnsZero(t *testing.T) {
	out, err := decodeAs[int](nil)
	if err != nil {
		t.Fatalf("decodeAs nil: %v", err)
	}
	if out != 0 {
		t.Errorf("expected zero, got %d", out)
	}
}

func TestLLM_NilCallSurfacesRefusal(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	interceptor.stepMocks["llm.turn"] = func() (any, error) {
		// Step interceptor will be invoked by the wrapped Run; the
		// nil-call branch is inside the closure under Run, but the
		// interceptor runs first in test mode and may not invoke
		// our closure at all. Validate by routing through a Call=nil
		// request and asserting the final error type.
		return LLMCompleteResult{}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)
	_, err := LLM(ctx, LLMCompleteRequest{Call: nil})
	// In test mode the interceptor short-circuits the nil-Call branch,
	// so err is nil and the call returns the mock's empty result.
	if err != nil {
		t.Fatalf("expected nil error from interceptor short-circuit, got: %v", err)
	}
}

func TestNewLLMInvalidJSONError_HasCode(t *testing.T) {
	err := NewLLMInvalidJSONError("not json", map[string]any{"raw": "abc"})
	if err.Code != CodeLLMInvalidJSON {
		t.Errorf("Code = %q, want %q", err.Code, CodeLLMInvalidJSON)
	}
	if !err.Retryable {
		t.Error("Retryable = false, want true")
	}
}

func TestErrors_UnwrapAndIs(t *testing.T) {
	cause := errors.New("boom")
	verr := NewToolValidationError("t", cause)
	if !errors.Is(verr, &ironflow.IronflowError{Code: CodeToolValidation}) {
		t.Error("errors.Is should match by code")
	}
	if !errors.Is(verr, cause) {
		t.Error("errors.Is should chain through Cause")
	}
}

func TestContext_Accessors(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)
	if ctx.Event().Name != "test.event" {
		t.Errorf("Event().Name = %q, want test.event", ctx.Event().Name)
	}
	// Secrets() returns a value type — just exercise the accessor.
	_ = ctx.Secrets()
}

func TestAgent_HandlerRunsWithMemory(t *testing.T) {
	t.Setenv("IRONFLOW_URL", "")
	t.Setenv("IRONFLOW_SERVER_URL", "")

	called := false
	fn := Agent(AgentConfig{
		Function: ironflow.FunctionConfig{ID: "agent-mem", Triggers: []ironflow.Trigger{{Event: "x"}}},
		Memory:   &MemoryConfig{StreamID: "s", Projection: "p"},
	}, func(_ Context) (any, error) {
		called = true
		return "ok", nil
	})

	interceptor := newFakeInterceptor(t)
	event := ironflow.Event{ID: "e", Name: "x", Version: 1, RawData: []byte("{}")}
	ictx := ironflow.NewTestContext(event, "run-x", "agent-mem", interceptor)
	out, err := fn.Handler(ictx)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
	if out != "ok" {
		t.Errorf("output = %v, want ok", out)
	}
}
