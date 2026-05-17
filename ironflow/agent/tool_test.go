package agent

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sahina/ironflow-go/ironflow"
)

type addInput struct {
	A int `json:"a"`
	B int `json:"b"`
}

type addOutput struct {
	Sum int `json:"sum"`
}

func newAddTool() ToolDefinition[addInput, addOutput] {
	return DefineTool[addInput, addOutput](ToolDefinition[addInput, addOutput]{
		Name: "add",
		Handler: func(in addInput) (addOutput, error) {
			return addOutput{Sum: in.A + in.B}, nil
		},
	})
}

func TestTool_HappyPath(t *testing.T) {
	tool := newAddTool()
	interceptor := newFakeInterceptor(t)
	interceptor.stepMocks["tool.add"] = func() (any, error) {
		return addOutput{Sum: 7}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
	}, interceptor)

	out, err := Tool(ctx, tool, addInput{A: 3, B: 4})
	if err != nil {
		t.Fatalf("Tool: %v", err)
	}
	if out.Sum != 7 {
		t.Errorf("Sum = %d, want 7", out.Sum)
	}
	if got := interceptor.stepCalls; len(got) != 1 || got[0] != "tool.add" {
		t.Errorf("stepCalls = %v, want [tool.add]", got)
	}
}

func TestTool_ValidateFailure(t *testing.T) {
	tool := DefineTool[addInput, addOutput](ToolDefinition[addInput, addOutput]{
		Name: "add-validated",
		Validate: func(in addInput) error {
			if in.A < 0 {
				return errors.New("A must be non-negative")
			}
			return nil
		},
		Handler: func(in addInput) (addOutput, error) {
			return addOutput{Sum: in.A + in.B}, nil
		},
	})
	interceptor := newFakeInterceptor(t)
	ctx, _ := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
	}, interceptor)

	_, err := Tool(ctx, tool, addInput{A: -1, B: 0})
	if err == nil {
		t.Fatal("expected validation error")
	}
	var verr *ToolValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("err is not ToolValidationError: %T %v", err, err)
	}
	if verr.ToolName != "add-validated" {
		t.Errorf("ToolName = %q, want add-validated", verr.ToolName)
	}
	if verr.Code != CodeToolValidation {
		t.Errorf("Code = %q, want %q", verr.Code, CodeToolValidation)
	}
	if len(interceptor.stepCalls) != 0 {
		t.Errorf("expected no step run on validation failure, got %v", interceptor.stepCalls)
	}
}

func TestTool_ByArgsCacheReuses(t *testing.T) {
	tool := DefineTool[addInput, addOutput](ToolDefinition[addInput, addOutput]{
		Name:       "add-byargs",
		Idempotent: IdempotentByArgs,
		Handler:    func(in addInput) (addOutput, error) { return addOutput{Sum: in.A + in.B}, nil },
	})
	interceptor := newFakeInterceptor(t)
	calls := 0
	interceptor.stepMocks["tool.add-byargs.0a8d3aaf3a82d8b1"] = func() (any, error) {
		calls++
		return addOutput{Sum: 5}, nil
	}
	// We don't know the exact 16-char hash here without computing.
	// Override more permissively via inspecting recorded step calls
	// — but the mock matches a step-name passed to RunStep. Compute
	// the hash to set the right key.
	hash, err := hashArgs(addInput{A: 2, B: 3})
	if err != nil {
		t.Fatalf("hashArgs: %v", err)
	}
	interceptor.stepMocks["tool.add-byargs."+hash] = func() (any, error) {
		calls++
		return addOutput{Sum: 5}, nil
	}

	ctx, _ := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
	}, interceptor)

	got1, err := Tool(ctx, tool, addInput{A: 2, B: 3})
	if err != nil {
		t.Fatalf("call1: %v", err)
	}
	got2, err := Tool(ctx, tool, addInput{A: 2, B: 3})
	if err != nil {
		t.Fatalf("call2: %v", err)
	}

	if got1.Sum != 5 || got2.Sum != 5 {
		t.Errorf("expected Sum=5 on both calls, got %d / %d", got1.Sum, got2.Sum)
	}
	if calls != 1 {
		t.Errorf("expected handler invoked once across two calls, got %d", calls)
	}
}

func TestTool_ByArgsConcurrentSameArgs_FoldsToOneInvocation(t *testing.T) {
	tool := DefineTool[addInput, addOutput](ToolDefinition[addInput, addOutput]{
		Name:       "add-byargs-concurrent",
		Idempotent: IdempotentByArgs,
		Handler:    func(in addInput) (addOutput, error) { return addOutput{Sum: in.A + in.B}, nil },
	})
	interceptor := newFakeInterceptor(t)
	hash, err := hashArgs(addInput{A: 7, B: 8})
	if err != nil {
		t.Fatalf("hashArgs: %v", err)
	}

	// Block the first goroutine inside RunStep until the second
	// goroutine has had a chance to call Do — that exercises the
	// in-flight fold rather than the cache fast-path.
	release := make(chan struct{})
	var invocations atomic.Int32
	interceptor.stepMocks["tool.add-byargs-concurrent."+hash] = func() (any, error) {
		invocations.Add(1)
		<-release
		return addOutput{Sum: 15}, nil
	}

	ctx, _ := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
	}, interceptor)

	type result struct {
		out addOutput
		err error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := Tool(ctx, tool, addInput{A: 7, B: 8})
			results <- result{out: out, err: err}
		}()
	}

	// Give both goroutines time to enter singleflight.Do.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	close(results)

	if got := invocations.Load(); got != 1 {
		t.Errorf("step invocations = %d, want 1 (singleflight should fold concurrent same-args)", got)
	}
	for r := range results {
		if r.err != nil {
			t.Errorf("call err: %v", r.err)
		}
		if r.out.Sum != 15 {
			t.Errorf("Sum = %d, want 15", r.out.Sum)
		}
	}
}

func TestTool_ByArgsDifferentArgsRunHandler(t *testing.T) {
	tool := DefineTool[addInput, addOutput](ToolDefinition[addInput, addOutput]{
		Name:       "add-byargs2",
		Idempotent: IdempotentByArgs,
		Handler:    func(in addInput) (addOutput, error) { return addOutput{Sum: in.A + in.B}, nil },
	})
	interceptor := newFakeInterceptor(t)
	hash1, _ := hashArgs(addInput{A: 1, B: 1})
	hash2, _ := hashArgs(addInput{A: 9, B: 9})
	interceptor.stepMocks["tool.add-byargs2."+hash1] = func() (any, error) {
		return addOutput{Sum: 2}, nil
	}
	interceptor.stepMocks["tool.add-byargs2."+hash2] = func() (any, error) {
		return addOutput{Sum: 18}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
	}, interceptor)

	r1, _ := Tool(ctx, tool, addInput{A: 1, B: 1})
	r2, _ := Tool(ctx, tool, addInput{A: 9, B: 9})
	if r1.Sum != 2 || r2.Sum != 18 {
		t.Errorf("got %v / %v, want 2 / 18", r1, r2)
	}
}

func TestTool_TimeoutDefault(t *testing.T) {
	tool := newAddTool()
	if tool.Timeout != 0 {
		t.Fatalf("expected default zero timeout, got %v", tool.Timeout)
	}
	// Custom timeout still passes through:
	custom := DefineTool[addInput, addOutput](ToolDefinition[addInput, addOutput]{
		Name:    "x",
		Timeout: 5 * time.Second,
		Handler: tool.Handler,
	})
	if custom.Timeout != 5*time.Second {
		t.Errorf("custom.Timeout = %v, want 5s", custom.Timeout)
	}
}

func TestToolByName_Dispatches(t *testing.T) {
	tool := newAddTool()
	interceptor := newFakeInterceptor(t)
	interceptor.stepMocks["tool.add"] = func() (any, error) {
		return addOutput{Sum: 9}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
		Tools:    []ToolEntry{tool.Entry()},
	}, interceptor)

	out, err := ToolByName(ctx, "add", map[string]any{"a": 4, "b": 5})
	if err != nil {
		t.Fatalf("ToolByName: %v", err)
	}
	if v, ok := out.(addOutput); !ok || v.Sum != 9 {
		t.Errorf("output = %T %v, want addOutput{9}", out, out)
	}
}

func TestToolByName_NotFound(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	ctx, _ := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
	}, interceptor)
	_, err := ToolByName(ctx, "missing", nil)
	if err == nil {
		t.Fatal("expected ToolNotFoundError")
	}
	var nf *ToolNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("err is not ToolNotFoundError: %T %v", err, err)
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Errorf("error message lacks tool name: %v", err)
	}
}
