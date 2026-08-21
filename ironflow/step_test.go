package ironflow

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestExecutionContext(t *testing.T) {
	createBasicRequest := func() *PushRequest {
		return &PushRequest{
			RunID:      "run_123",
			FunctionID: "test-function",
			Attempt:    1,
			Event: PushEvent{
				ID:        "evt_123",
				Name:      "test.event",
				Data:      json.RawMessage(`{"key":"value"}`),
				Timestamp: time.Now().Format(time.RFC3339),
			},
			Steps: []CompletedStep{},
		}
	}

	t.Run("generates unique step IDs", func(t *testing.T) {
		ctx := &executionContext{
			runID:        "run_123",
			stepCounters: make(map[string]int),
		}

		id1 := ctx.generateStepID("fetch-data")
		id2 := ctx.generateStepID("fetch-data")
		id3 := ctx.generateStepID("process-data")

		if id1 != "run_123:fetch-data:0" {
			t.Errorf("expected 'run_123:fetch-data:0', got '%s'", id1)
		}
		if id2 != "run_123:fetch-data:1" {
			t.Errorf("expected 'run_123:fetch-data:1', got '%s'", id2)
		}
		if id3 != "run_123:process-data:0" {
			t.Errorf("expected 'run_123:process-data:0', got '%s'", id3)
		}
	})

	t.Run("indexes completed steps", func(t *testing.T) {
		req := createBasicRequest()
		req.Steps = []CompletedStep{
			{
				ID:     "run_123:step1:0",
				Name:   "step1",
				Status: "completed",
				Output: map[string]string{"result": "done"},
			},
		}

		ctx := newExecutionContext(req)

		step, ok := ctx.completedSteps["run_123:step1:0"]
		if !ok {
			t.Fatal("expected to find completed step")
		}
		if step.Name != "step1" {
			t.Errorf("expected name 'step1', got '%s'", step.Name)
		}
	})

	t.Run("checks resume context", func(t *testing.T) {
		req := createBasicRequest()
		req.Resume = &ResumeContext{
			StepID: "run_123:wait:0",
			Type:   "wait_for_event",
			Data:   map[string]bool{"received": true},
		}

		ctx := newExecutionContext(req)

		if !ctx.isResumingFrom("run_123:wait:0", "wait_for_event") {
			t.Error("expected isResumingFrom to return true")
		}
		if ctx.isResumingFrom("run_123:wait:0", "sleep") {
			t.Error("expected isResumingFrom to return false for wrong type")
		}
		if ctx.isResumingFrom("run_123:other:0", "wait_for_event") {
			t.Error("expected isResumingFrom to return false for wrong step")
		}
	})
}

func TestRun(t *testing.T) {
	t.Run("executes function and returns result", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		result, err := Run(ctx, "my-step", func() (string, error) {
			return "hello", nil
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "hello" {
			t.Errorf("expected 'hello', got '%s'", result)
		}

		// Check that step was recorded
		if len(ctx.exec.executedSteps) != 1 {
			t.Fatalf("expected 1 executed step, got %d", len(ctx.exec.executedSteps))
		}
		if ctx.exec.executedSteps[0].Name != "my-step" {
			t.Errorf("expected step name 'my-step', got '%s'", ctx.exec.executedSteps[0].Name)
		}
		if ctx.exec.executedSteps[0].Status != "completed" {
			t.Errorf("expected status 'completed', got '%s'", ctx.exec.executedSteps[0].Status)
		}
	})

	t.Run("returns memoized result", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:        "run_123",
				stepCounters: make(map[string]int),
				completedSteps: map[string]*CompletedStep{
					"run_123:my-step:0": {
						ID:     "run_123:my-step:0",
						Name:   "my-step",
						Status: "completed",
						Output: "memoized-value",
					},
				},
				executedSteps: make([]*StepResult, 0),
			},
		}

		callCount := 0
		result, err := Run(ctx, "my-step", func() (string, error) {
			callCount++
			return "new-value", nil
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "memoized-value" {
			t.Errorf("expected 'memoized-value', got '%s'", result)
		}
		if callCount != 0 {
			t.Errorf("function should not have been called, but was called %d times", callCount)
		}
	})

	t.Run("handles function error", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		_, err := Run(ctx, "failing-step", func() (string, error) {
			return "", errors.New("step failed")
		})

		if err == nil {
			t.Fatal("expected error")
		}

		stepErr, ok := err.(*StepError)
		if !ok {
			t.Fatalf("expected StepError, got %T", err)
		}
		if stepErr.StepName != "failing-step" {
			t.Errorf("expected step name 'failing-step', got '%s'", stepErr.StepName)
		}

		// Check that failed step was recorded
		if len(ctx.exec.executedSteps) != 1 {
			t.Fatalf("expected 1 executed step, got %d", len(ctx.exec.executedSteps))
		}
		if ctx.exec.executedSteps[0].Status != "failed" {
			t.Errorf("expected status 'failed', got '%s'", ctx.exec.executedSteps[0].Status)
		}
	})

	t.Run("handles typed results", func(t *testing.T) {
		type Result struct {
			Value int    `json:"value"`
			Name  string `json:"name"`
		}

		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		result, err := Run(ctx, "typed-step", func() (Result, error) {
			return Result{Value: 42, Name: "test"}, nil
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Value != 42 {
			t.Errorf("expected Value 42, got %d", result.Value)
		}
		if result.Name != "test" {
			t.Errorf("expected Name 'test', got '%s'", result.Name)
		}
	})
}

func TestSleep(t *testing.T) {
	t.Run("yields with sleep signal", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic with yield signal")
			}

			signal, ok := r.(*yieldSignal)
			if !ok {
				t.Fatalf("expected yieldSignal, got %T", r)
			}
			if signal.info.Type != "sleep" {
				t.Errorf("expected type 'sleep', got '%s'", signal.info.Type)
			}
		}()

		Sleep(ctx, "wait-1h", 1*time.Hour)
		t.Fatal("expected Sleep to panic")
	})

	t.Run("returns immediately when resuming", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:        "run_123",
				stepCounters: make(map[string]int),
				completedSteps: map[string]*CompletedStep{
					"run_123:wait-1h:0": {
						ID:     "run_123:wait-1h:0",
						Name:   "wait-1h",
						Status: "completed",
					},
				},
				executedSteps: make([]*StepResult, 0),
			},
		}

		// Should not panic - returns immediately due to memoization
		err := Sleep(ctx, "wait-1h", 1*time.Hour)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestWaitForEvent(t *testing.T) {
	t.Run("yields when no memoized result and no resume context", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		filter := EventFilter{
			Event:   "order.approved",
			Match:   "data.orderId",
			Timeout: 7 * 24 * time.Hour,
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic with yield signal")
			}

			signal, ok := r.(*yieldSignal)
			if !ok {
				t.Fatalf("expected yieldSignal, got %T", r)
			}
			if signal.info.Type != "wait_for_event" {
				t.Errorf("expected type 'wait_event', got '%s'", signal.info.Type)
			}
			if signal.info.StepID != "run_123:wait-approval:0" {
				t.Errorf("expected step ID 'run_123:wait-approval:0', got '%s'", signal.info.StepID)
			}
			if signal.info.EventFilter == nil {
				t.Fatal("expected EventFilter to be set")
			}
			if signal.info.EventFilter.Event != "order.approved" {
				t.Errorf("expected filter event 'order.approved', got '%s'", signal.info.EventFilter.Event)
			}
			if signal.info.EventFilter.Match != "data.orderId" {
				t.Errorf("expected filter match 'data.orderId', got '%s'", signal.info.EventFilter.Match)
			}
		}()

		WaitForEvent[any](ctx, "wait-approval", filter)
		t.Fatal("expected WaitForEvent to panic")
	})

	t.Run("applies default timeout when zero", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		filter := EventFilter{
			Event: "order.approved",
			Match: "data.orderId",
			// Timeout intentionally left as zero
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic with yield signal")
			}

			signal, ok := r.(*yieldSignal)
			if !ok {
				t.Fatalf("expected yieldSignal, got %T", r)
			}
			if signal.info.EventFilter.Timeout != 7*24*time.Hour {
				t.Errorf("expected default timeout 7d, got %v", signal.info.EventFilter.Timeout)
			}
		}()

		WaitForEvent[any](ctx, "wait-approval", filter)
		t.Fatal("expected WaitForEvent to panic")
	})

	t.Run("returns memoized result when step already completed", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:        "run_123",
				stepCounters: make(map[string]int),
				completedSteps: map[string]*CompletedStep{
					"run_123:wait-approval:0": {
						ID:     "run_123:wait-approval:0",
						Name:   "wait-approval",
						Status: "completed",
						Output: map[string]any{
							"id":   "evt_456",
							"name": "order.approved",
						},
					},
				},
				executedSteps: make([]*StepResult, 0),
			},
		}

		filter := EventFilter{
			Event:   "order.approved",
			Match:   "data.orderId",
			Timeout: 7 * 24 * time.Hour,
		}

		event, err := WaitForEvent[any](ctx, "wait-approval", filter)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if event.ID != "evt_456" {
			t.Errorf("expected event ID 'evt_456', got '%s'", event.ID)
		}
		if event.Name != "order.approved" {
			t.Errorf("expected event name 'order.approved', got '%s'", event.Name)
		}
	})

	t.Run("resumes with event data from resume context", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
				resumeContext: &ResumeContext{
					StepID: "run_123:wait-approval:0",
					Type:   "wait_for_event",
					Data: map[string]any{
						"id":   "evt_789",
						"name": "order.approved",
					},
				},
			},
		}

		filter := EventFilter{
			Event:   "order.approved",
			Match:   "data.orderId",
			Timeout: 7 * 24 * time.Hour,
		}

		event, err := WaitForEvent[any](ctx, "wait-approval", filter)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if event.ID != "evt_789" {
			t.Errorf("expected event ID 'evt_789', got '%s'", event.ID)
		}
		if event.Name != "order.approved" {
			t.Errorf("expected event name 'order.approved', got '%s'", event.Name)
		}
		if !ctx.exec.resumeHandled {
			t.Error("expected resumeHandled to be true")
		}
	})

	t.Run("resume with nil data returns empty event", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
				resumeContext: &ResumeContext{
					StepID: "run_123:wait-approval:0",
					Type:   "wait_for_event",
					Data:   nil,
				},
			},
		}

		filter := EventFilter{
			Event: "order.approved",
		}

		// When Data is nil, the resume branch is entered but data check fails,
		// so it falls through to the memoization check, then to yield.
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic with yield signal when resume data is nil")
			}

			signal, ok := r.(*yieldSignal)
			if !ok {
				t.Fatalf("expected yieldSignal, got %T", r)
			}
			if signal.info.Type != "wait_for_event" {
				t.Errorf("expected type 'wait_event', got '%s'", signal.info.Type)
			}
		}()

		WaitForEvent[any](ctx, "wait-approval", filter)
		t.Fatal("expected WaitForEvent to panic")
	})

	t.Run("does not resume when step type does not match", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
				resumeContext: &ResumeContext{
					StepID: "run_123:wait-approval:0",
					Type:   "sleep", // wrong type — should not match wait_event
					Data: map[string]any{
						"id":   "evt_789",
						"name": "order.approved",
					},
				},
			},
		}

		filter := EventFilter{
			Event: "order.approved",
		}

		// Should yield because resume type doesn't match
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic with yield signal")
			}

			signal, ok := r.(*yieldSignal)
			if !ok {
				t.Fatalf("expected yieldSignal, got %T", r)
			}
			if signal.info.Type != "wait_for_event" {
				t.Errorf("expected type 'wait_event', got '%s'", signal.info.Type)
			}
		}()

		WaitForEvent[any](ctx, "wait-approval", filter)
		t.Fatal("expected WaitForEvent to panic")
	})
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
		hasError bool
	}{
		{"1h", 1 * time.Hour, false},
		{"30m", 30 * time.Minute, false},
		{"1s", 1 * time.Second, false},
		{"500ms", 500 * time.Millisecond, false},
		{"1d", 24 * time.Hour, false},
		{"7d", 7 * 24 * time.Hour, false},
		{"1w", 7 * 24 * time.Hour, false},
		{"2w", 14 * 24 * time.Hour, false},
		{"invalid", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := ParseDuration(tt.input)

			if tt.hasError {
				if err == nil {
					t.Errorf("expected error for input '%s'", tt.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error for input '%s': %v", tt.input, err)
			}
			if result != tt.expected {
				t.Errorf("expected %v, got %v for input '%s'", tt.expected, result, tt.input)
			}
		})
	}
}

// TestStepIDEscaping covers #1694 item 4: unescaped, a top-level step literally
// named "a:0:b" at index 0 and a step named "b" at index 0 inside parallel "a"
// branch 0 both render as "run_123:a:0:b:0" — one memoization key, one steps
// row, two different steps. Must stay in lockstep with the equivalent test in
// sdk/js/node/src/internal/context.test.ts.
func TestStepIDEscaping(t *testing.T) {
	t.Run("colon in name does not collide with a branch scope", func(t *testing.T) {
		ctx := &executionContext{runID: "run_123", stepCounters: make(map[string]int)}

		topLevel := ctx.generateStepID("a:0:b")
		branch := ctx.createBranchContext("a", 0)
		inBranch := branch.generateStepID("b")

		if topLevel == inBranch {
			t.Fatalf("distinct steps share one id: %q", topLevel)
		}
		if inBranch != "run_123:a:0:b:0" {
			t.Errorf("branch step id: got %q, want %q", inBranch, "run_123:a:0:b:0")
		}
	})

	t.Run("plain names are unchanged", func(t *testing.T) {
		ctx := &executionContext{runID: "run_123", stepCounters: make(map[string]int)}

		if got := ctx.generateStepID("charge-card"); got != "run_123:charge-card:0" {
			t.Errorf("got %q, want %q", got, "run_123:charge-card:0")
		}
	})

	// Upgrade bridge: a run that paused before escaping shipped carries rows keyed
	// by the UNESCAPED name. Without this, the newly escaped id misses the memoized
	// row and the completed step re-executes for real at the deploy boundary.
	t.Run("resumes a legacy unescaped id when that is the memoized row", func(t *testing.T) {
		ctx := &executionContext{
			runID:        "run_123",
			stepCounters: make(map[string]int),
			completedSteps: map[string]*CompletedStep{
				// Written by the pre-escaping SDK.
				"run_123:charge:card:0": {ID: "run_123:charge:card:0", Status: "completed"},
			},
		}
		if got := ctx.generateStepID("charge:card"); got != "run_123:charge:card:0" {
			t.Errorf("resumed step id: got %q, want the legacy id %q", got, "run_123:charge:card:0")
		}
	})

	t.Run("uses the escaped id when no legacy row exists", func(t *testing.T) {
		ctx := &executionContext{
			runID:          "run_123",
			stepCounters:   make(map[string]int),
			completedSteps: map[string]*CompletedStep{},
		}
		if got := ctx.generateStepID("charge:card"); got != `run_123:charge\:card:0` {
			t.Errorf("fresh step id: got %q, want the escaped id", got)
		}
	})

	// The SDK's own namespaces are structure, not user input. Escaping their
	// colon would change the id of every existing publish/compensation step, so
	// the first resume after an upgrade would miss the memoized row and re-run
	// the side effect.
	t.Run("SDK compensate:/publish: prefixes stay literal", func(t *testing.T) {
		ctx := &executionContext{runID: "run_123", stepCounters: make(map[string]int)}

		cases := map[string]string{
			"compensate:charge-card": "run_123:compensate:charge-card:0",
			"publish:orders.created": "run_123:publish:orders.created:0",
			// ...while the leaf after the prefix is still escaped.
			"publish:a:0:b": `run_123:publish:a\:0\:b:0`,
		}
		for name, want := range cases {
			if got := ctx.generateStepID(name); got != want {
				t.Errorf("generateStepID(%q): got %q, want %q", name, got, want)
			}
		}
	})
}
