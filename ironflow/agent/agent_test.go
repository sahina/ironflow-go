package agent

import (
	"errors"
	"testing"

	"github.com/sahina/ironflow-go/ironflow"
)

func TestAgent_Defaults(t *testing.T) {
	fn := Agent(AgentConfig{
		Function: ironflow.FunctionConfig{
			ID:       "test-agent",
			Triggers: []ironflow.Trigger{{Event: "test.event"}},
		},
	}, func(ctx Context) (any, error) {
		if ctx.Run().FunctionID != "test-agent" {
			t.Errorf("Run().FunctionID = %q, want %q", ctx.Run().FunctionID, "test-agent")
		}
		return "ok", nil
	})

	if fn.Config.ID != "test-agent" {
		t.Errorf("Config.ID = %q, want %q", fn.Config.ID, "test-agent")
	}
	if fn.Config.Mode != ironflow.PushMode {
		t.Errorf("Config.Mode default = %q, want PushMode", fn.Config.Mode)
	}
}

func TestAgent_DuplicateTool_Panics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for duplicate tools")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("recovered non-error: %T %v", r, r)
		}
		var dup *DuplicateToolError
		if !errors.As(err, &dup) {
			t.Fatalf("recovered err is not DuplicateToolError: %T %v", err, err)
		}
		if dup.ToolName != "dup" {
			t.Errorf("DuplicateToolError.ToolName = %q, want %q", dup.ToolName, "dup")
		}
	}()

	t1 := DefineTool[int, int](ToolDefinition[int, int]{
		Name:    "dup",
		Handler: func(i int) (int, error) { return i, nil },
	}).Entry()
	t2 := DefineTool[int, int](ToolDefinition[int, int]{
		Name:    "dup",
		Handler: func(i int) (int, error) { return i + 1, nil },
	}).Entry()

	Agent(AgentConfig{
		Function: ironflow.FunctionConfig{ID: "agent-dup"},
		Tools:    []ToolEntry{t1, t2},
	}, func(_ Context) (any, error) { return nil, nil })
}

func TestContext_TurnZero_Initial(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	ctx, _ := newAgentContext(t, AgentConfig{
		Function: ironflow.FunctionConfig{ID: "a"},
	}, interceptor)
	if ctx.Turn() != 0 {
		t.Errorf("Turn() = %d, want 0", ctx.Turn())
	}
}
