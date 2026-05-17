package ironflowtest_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sahina/ironflow-go/ironflow"
	"github.com/sahina/ironflow-go/ironflow/ironflowtest"
)

// ---- Test fixtures ----

var simpleFunction = ironflow.CreateFunction(
	ironflow.FunctionConfig{
		ID:       "simple",
		Triggers: []ironflow.Trigger{{Event: "test.event"}},
	},
	func(ctx ironflow.Context) (any, error) {
		result, err := ironflow.Run[map[string]any](ctx, "my-step", func() (map[string]any, error) {
			return map[string]any{"value": 42}, nil
		})
		return result, err
	},
)

var multiStepFunction = ironflow.CreateFunction(
	ironflow.FunctionConfig{
		ID:       "multi-step",
		Triggers: []ironflow.Trigger{{Event: "order.placed"}},
	},
	func(ctx ironflow.Context) (any, error) {
		charge, err := ironflow.Run[map[string]any](ctx, "charge-card", func() (map[string]any, error) {
			return nil, nil
		})
		if err != nil {
			return nil, err
		}
		reserve, err := ironflow.Run[map[string]any](ctx, "reserve-inventory", func() (map[string]any, error) {
			return nil, nil
		})
		if err != nil {
			return nil, err
		}
		ship, err := ironflow.Run[map[string]any](ctx, "ship-order", func() (map[string]any, error) {
			return nil, nil
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{"charge": charge, "reserve": reserve, "ship": ship}, nil
	},
)

var sleepFunction = ironflow.CreateFunction(
	ironflow.FunctionConfig{
		ID:       "with-sleep",
		Triggers: []ironflow.Trigger{{Event: "test.sleep"}},
	},
	func(ctx ironflow.Context) (any, error) {
		result, err := ironflow.Run[map[string]any](ctx, "before-sleep", func() (map[string]any, error) {
			return map[string]any{"done": true}, nil
		})
		if err != nil {
			return nil, err
		}
		if err := ironflow.Sleep(ctx, "wait-24h", 24*time.Hour); err != nil {
			return nil, err
		}
		return result, nil
	},
)

var waitEventFunction = ironflow.CreateFunction(
	ironflow.FunctionConfig{
		ID:       "with-wait",
		Triggers: []ironflow.Trigger{{Event: "order.created"}},
	},
	func(ctx ironflow.Context) (any, error) {
		event, err := ironflow.WaitForEvent[map[string]any](ctx, "wait-approval", ironflow.EventFilter{
			Event:   "order.approved",
			Match:   "data.orderId",
			Timeout: 7 * 24 * time.Hour,
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{"approved": true, "eventName": event.Name}, nil
	},
)

var sagaFunction = ironflow.CreateFunction(
	ironflow.FunctionConfig{
		ID:       "saga",
		Triggers: []ironflow.Trigger{{Event: "order.placed"}},
	},
	func(ctx ironflow.Context) (any, error) {
		charge, err := ironflow.Run[map[string]any](ctx, "charge-card", func() (map[string]any, error) {
			return map[string]any{"chargeId": "ch_1"}, nil
		})
		if err != nil {
			return nil, err
		}
		ironflow.Compensate(ctx, "charge-card", func() error { return nil })

		reserve, err := ironflow.Run[map[string]any](ctx, "reserve-inventory", func() (map[string]any, error) {
			return map[string]any{"reservationId": "res_1"}, nil
		})
		if err != nil {
			return nil, err
		}
		ironflow.Compensate(ctx, "reserve-inventory", func() error { return nil })

		_, err = ironflow.Run[map[string]any](ctx, "ship-order", func() (map[string]any, error) {
			return map[string]any{"trackingId": "trk_1"}, nil
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{"charge": charge, "reserve": reserve}, nil
	},
)

var invokeFunction = ironflow.CreateFunction(
	ironflow.FunctionConfig{
		ID:       "with-invoke",
		Triggers: []ironflow.Trigger{{Event: "test.invoke"}},
	},
	func(ctx ironflow.Context) (any, error) {
		result, err := ironflow.Invoke[map[string]any](ctx, "payment-service", map[string]any{"amount": 100})
		return result, err
	},
)

var parallelFunction = ironflow.CreateFunction(
	ironflow.FunctionConfig{
		ID:       "with-parallel",
		Triggers: []ironflow.Trigger{{Event: "test.parallel"}},
	},
	func(ctx ironflow.Context) (any, error) {
		results, err := ironflow.Parallel(ctx, "fetch-all", []func(*ironflow.BranchContext) (map[string]any, error){
			func(b *ironflow.BranchContext) (map[string]any, error) {
				return ironflow.RunWithBranch[map[string]any](b, "fetch-a", func() (map[string]any, error) {
					return map[string]any{"a": 1}, nil
				})
			},
			func(b *ironflow.BranchContext) (map[string]any, error) {
				return ironflow.RunWithBranch[map[string]any](b, "fetch-b", func() (map[string]any, error) {
					return map[string]any{"b": 2}, nil
				})
			},
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{"results": results}, nil
	},
)

// ---- Tests ----

func TestNewClient(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{simpleFunction},
	})
	if tc == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestEmit_HappyPath(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{simpleFunction},
	})
	tc.MockStep("my-step", func() (any, error) {
		return map[string]any{"value": 99}, nil
	})

	run := tc.Emit(t, "test.event", map[string]any{})

	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	if run.StepOutput("my-step") == nil {
		t.Fatal("expected step output")
	}
	if len(run.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(run.Steps))
	}
	if run.Steps[0].Type != "run" {
		t.Fatalf("expected run type, got %s", run.Steps[0].Type)
	}
}

func TestEmit_MultipleSteps(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{multiStepFunction},
	})
	tc.MockStep("charge-card", func() (any, error) {
		return map[string]any{"chargeId": "ch_123"}, nil
	})
	tc.MockStep("reserve-inventory", func() (any, error) {
		return map[string]any{"reservationId": "res_456"}, nil
	})
	tc.MockStep("ship-order", func() (any, error) {
		return map[string]any{"trackingId": "trk_789"}, nil
	})

	run := tc.Emit(t, "order.placed", map[string]any{"orderId": "order-1"})

	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	if len(run.Steps) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(run.Steps))
	}
	if run.StepOutput("charge-card") == nil {
		t.Fatal("expected charge-card output")
	}
}

func TestEmit_UnmockedStep(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{simpleFunction},
	})
	// Don't mock "my-step"

	run := tc.Emit(t, "test.event", map[string]any{})

	if run.Status != "failed" {
		t.Fatalf("expected failed, got %s", run.Status)
	}
	if run.Error == nil {
		t.Fatal("expected error")
	}
}

func TestEmit_Sleep(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{sleepFunction},
	})
	tc.MockStep("before-sleep", func() (any, error) {
		return map[string]any{"done": true}, nil
	})

	run := tc.Emit(t, "test.sleep", map[string]any{})

	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s (err: %v)", run.Status, run.Error)
	}
	if len(run.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(run.Steps))
	}
	if run.Steps[1].Type != "sleep" {
		t.Fatalf("expected sleep step, got %s", run.Steps[1].Type)
	}
}

func TestEmit_WaitForEvent(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{waitEventFunction},
	})
	tc.SendEvent("order.approved", map[string]any{"orderId": "order-1", "approved": true})

	run := tc.Emit(t, "order.created", map[string]any{"orderId": "order-1"})

	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s (err: %v)", run.Status, run.Error)
	}
	output, ok := run.Output.(map[string]any)
	if !ok {
		t.Fatalf("expected map output, got %T", run.Output)
	}
	if output["approved"] != true {
		t.Fatalf("expected approved=true, got %v", output["approved"])
	}
}

func TestEmit_WaitForEvent_NoEvent(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{waitEventFunction},
	})
	// Don't SendEvent

	run := tc.Emit(t, "order.created", map[string]any{})

	if run.Status != "failed" {
		t.Fatalf("expected failed, got %s", run.Status)
	}
	if run.Error == nil {
		t.Fatal("expected error")
	}
}

func TestEmit_Compensations(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{sagaFunction},
	})
	tc.MockStep("charge-card", func() (any, error) {
		return map[string]any{"chargeId": "ch_123"}, nil
	})
	tc.MockStep("reserve-inventory", func() (any, error) {
		return map[string]any{"reservationId": "res_456"}, nil
	})
	tc.MockStep("ship-order", func() (any, error) {
		return nil, fmt.Errorf("shipping unavailable")
	})

	run := tc.Emit(t, "order.placed", map[string]any{})

	if run.Status != "failed" {
		t.Fatalf("expected failed, got %s", run.Status)
	}
	if len(run.CompensationsRan) != 2 {
		t.Fatalf("expected 2 compensations, got %d", len(run.CompensationsRan))
	}
	if run.CompensationsRan[0] != "reserve-inventory" {
		t.Fatalf("expected reserve-inventory first, got %s", run.CompensationsRan[0])
	}
	if run.CompensationsRan[1] != "charge-card" {
		t.Fatalf("expected charge-card second, got %s", run.CompensationsRan[1])
	}
}

func TestEmit_CompensationsNotRunOnSuccess(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{sagaFunction},
	})
	tc.MockStep("charge-card", func() (any, error) {
		return map[string]any{"chargeId": "ch_123"}, nil
	})
	tc.MockStep("reserve-inventory", func() (any, error) {
		return map[string]any{"reservationId": "res_456"}, nil
	})
	tc.MockStep("ship-order", func() (any, error) {
		return map[string]any{"trackingId": "trk_789"}, nil
	})

	run := tc.Emit(t, "order.placed", map[string]any{})

	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s", run.Status)
	}
	if len(run.CompensationsRan) != 0 {
		t.Fatalf("expected 0 compensations, got %d", len(run.CompensationsRan))
	}
}

func TestEmit_Invoke(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{invokeFunction},
	})
	tc.MockInvoke("payment-service", func(data any) (any, error) {
		return map[string]any{"txId": "tx_123"}, nil
	})

	run := tc.Emit(t, "test.invoke", map[string]any{})

	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s (err: %v)", run.Status, run.Error)
	}
	if run.StepOutput("payment-service") == nil {
		t.Fatal("expected invoke step output")
	}
}

func TestEmit_UnmockedInvoke(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{invokeFunction},
	})
	// Don't mock invoke

	run := tc.Emit(t, "test.invoke", map[string]any{})

	if run.Status != "failed" {
		t.Fatalf("expected failed, got %s", run.Status)
	}
	if run.Error == nil {
		t.Fatal("expected error")
	}
}

func TestEmit_Parallel(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{parallelFunction},
	})
	tc.MockStep("fetch-a", func() (any, error) {
		return map[string]any{"a": 1}, nil
	})
	tc.MockStep("fetch-b", func() (any, error) {
		return map[string]any{"b": 2}, nil
	})

	run := tc.Emit(t, "test.parallel", map[string]any{})

	if run.Status != "completed" {
		t.Fatalf("expected completed, got %s (err: %v)", run.Status, run.Error)
	}
	output, ok := run.Output.(map[string]any)
	if !ok {
		t.Fatalf("expected map output, got %T", run.Output)
	}
	results, ok := output["results"].([]map[string]any)
	if !ok {
		t.Fatalf("expected results slice, got %T", output["results"])
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestStepOutput_NotFound(t *testing.T) {
	tc := ironflowtest.NewClient(t, ironflowtest.Config{
		Functions: []ironflow.Function{simpleFunction},
	})
	tc.MockStep("my-step", func() (any, error) {
		return map[string]any{"value": 42}, nil
	})

	run := tc.Emit(t, "test.event", map[string]any{})
	if run.StepOutput("nonexistent") != nil {
		t.Fatal("expected nil for nonexistent step")
	}
}
