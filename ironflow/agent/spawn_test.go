package agent

import (
	"testing"

	"github.com/sahina/ironflow-go/ironflow"
)

type subInput struct {
	X int `json:"x"`
}

type subOutput struct {
	Y int `json:"y"`
}

func TestSpawn_AwaitTrue_UsesInvoke(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	interceptor.invokeMocks["sub-fn"] = func(input any) (any, error) {
		return subOutput{Y: 42}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)

	res, err := Spawn[subInput, subOutput](ctx, "spawn-step", SpawnOptions[subInput]{
		FunctionID: "sub-fn",
		Input:      subInput{X: 1},
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if res.Output.Y != 42 {
		t.Errorf("Output.Y = %d, want 42", res.Output.Y)
	}
	if res.RunID != "" {
		t.Errorf("RunID = %q, want empty for await=true", res.RunID)
	}
	if len(interceptor.invokeCalls) != 1 || interceptor.invokeCalls[0] != "sub-fn" {
		t.Errorf("invokeCalls = %v, want [sub-fn]", interceptor.invokeCalls)
	}
}

func TestSpawn_AwaitFalse_UsesInvokeAsync(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	// Async path wraps InvokeAsync in ironflow.Run("spawn.<name>", ...).
	// In test mode the interceptor's RunStep short-circuits before the
	// inner closure runs, so mock the step output directly.
	interceptor.stepMocks["spawn.spawn-step"] = func() (any, error) {
		return ironflow.InvokeAsyncResult{RunID: "child-run-1"}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)

	awaitFalse := false
	res, err := Spawn[subInput, subOutput](ctx, "spawn-step", SpawnOptions[subInput]{
		FunctionID: "sub-fn",
		Input:      subInput{X: 1},
		Await:      &awaitFalse,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if res.RunID != "child-run-1" {
		t.Errorf("RunID = %q, want child-run-1", res.RunID)
	}
	if len(interceptor.stepCalls) != 1 || interceptor.stepCalls[0] != "spawn.spawn-step" {
		t.Errorf("stepCalls = %v, want [spawn.spawn-step]", interceptor.stepCalls)
	}
}
