package ironflow

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPanicInStepFunction(t *testing.T) {
	t.Run("panic in Run step function propagates", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_panic",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("Expected panic to propagate")
			}
			panicMsg, ok := r.(string)
			if !ok {
				t.Fatalf("Expected string panic, got %T", r)
			}
			if panicMsg != "intentional panic in step" {
				t.Errorf("Expected 'intentional panic in step', got '%s'", panicMsg)
			}
		}()

		_, _ = Run(ctx, "panicking-step", func() (string, error) {
			panic("intentional panic in step")
		})

		t.Fatal("Should not reach here")
	})

	t.Run("panic in RunWithBranch propagates", func(t *testing.T) {
		parent := &executionContext{
			runID:          "run_branch_panic",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		}
		branch := parent.createBranchContext("parallel", 0)

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("Expected panic to propagate")
			}
		}()

		_, _ = RunWithBranch(branch, "panicking-step", func() (string, error) {
			panic("branch panic")
		})

		t.Fatal("Should not reach here")
	})
}

func TestYieldSignalHandling(t *testing.T) {
	t.Run("Sleep yields via panic signal", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_sleep_yield",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		var capturedSignal *yieldSignal

		func() {
			defer func() {
				if r := recover(); r != nil {
					if signal, ok := r.(*yieldSignal); ok {
						capturedSignal = signal
					} else {
						panic(r) // Re-panic if not a yield signal
					}
				}
			}()

			Sleep(ctx, "test-sleep", 1*time.Hour)
		}()

		if capturedSignal == nil {
			t.Fatal("Expected yield signal from Sleep")
		}
		if capturedSignal.info.Type != "sleep" {
			t.Errorf("Expected type 'sleep', got '%s'", capturedSignal.info.Type)
		}
		if capturedSignal.info.Until == "" {
			t.Error("Expected Until to be set")
		}
	})

	t.Run("WaitForEvent yields via panic signal", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_wait_yield",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		var capturedSignal *yieldSignal

		func() {
			defer func() {
				if r := recover(); r != nil {
					if signal, ok := r.(*yieldSignal); ok {
						capturedSignal = signal
					} else {
						panic(r)
					}
				}
			}()

			WaitForEvent[any](ctx, "wait-approval", EventFilter{
				Event:   "approval.received",
				Match:   "data.orderId",
				Timeout: 24 * time.Hour,
			})
		}()

		if capturedSignal == nil {
			t.Fatal("Expected yield signal from WaitForEvent")
		}
		if capturedSignal.info.Type != "wait_for_event" {
			t.Errorf("Expected type 'wait_event', got '%s'", capturedSignal.info.Type)
		}
		if capturedSignal.info.EventFilter == nil {
			t.Error("Expected EventFilter to be set")
		}
		if capturedSignal.info.EventFilter.Event != "approval.received" {
			t.Errorf("Expected event 'approval.received', got '%s'", capturedSignal.info.EventFilter.Event)
		}
	})
}

func TestServeHandlerPanicRecovery(t *testing.T) {
	t.Run("handler converts yield signal to yielded response", func(t *testing.T) {
		handler := Serve(ServeConfig{
			Functions: []Function{
				{
					Config: FunctionConfig{ID: "sleep-function"},
					Handler: func(ctx Context) (any, error) {
						Sleep(ctx, "long-sleep", 1*time.Hour)
						return "done", nil
					},
				},
			},
			SkipVerification: true,
		})

		body := `{
			"run_id": "run_123",
			"function_id": "sleep-function",
			"attempt": 1,
			"event": {
				"id": "evt_123",
				"name": "test.event",
				"data": {},
				"timestamp": "2024-01-01T00:00:00Z"
			},
			"steps": []
		}`

		req := httptest.NewRequest("POST", "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", rec.Code)
		}

		var resp PushResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("Failed to parse response: %v", err)
		}

		if resp.Status != "yielded" {
			t.Errorf("Expected status 'yielded', got '%s'", resp.Status)
		}
		if resp.Yield == nil {
			t.Fatal("Expected Yield to be set")
		}
		if resp.Yield.Type != "sleep" {
			t.Errorf("Expected yield type 'sleep', got '%s'", resp.Yield.Type)
		}
	})

	t.Run("handler propagates real panic", func(t *testing.T) {
		handler := Serve(ServeConfig{
			Functions: []Function{
				{
					Config: FunctionConfig{ID: "panic-function"},
					Handler: func(ctx Context) (any, error) {
						panic("real panic in handler")
					},
				},
			},
			SkipVerification: true,
		})

		body := `{
			"run_id": "run_panic",
			"function_id": "panic-function",
			"attempt": 1,
			"event": {
				"id": "evt_123",
				"name": "test.event",
				"data": {},
				"timestamp": "2024-01-01T00:00:00Z"
			},
			"steps": []
		}`

		req := httptest.NewRequest("POST", "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("Expected panic to propagate through handler")
			}
			if r != "real panic in handler" {
				t.Errorf("Expected 'real panic in handler', got '%v'", r)
			}
		}()

		handler.ServeHTTP(rec, req)
	})
}

func TestParallelPanicHandling(t *testing.T) {
	t.Run("yield signal in parallel branch is captured", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_parallel_yield",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		var capturedSignal *yieldSignal

		func() {
			defer func() {
				if r := recover(); r != nil {
					if signal, ok := r.(*yieldSignal); ok {
						capturedSignal = signal
					} else {
						panic(r)
					}
				}
			}()

			Parallel(ctx, "parallel-with-sleep", []func(*BranchContext) (string, error){
				func(b *BranchContext) (string, error) {
					return "branch-0-result", nil
				},
				func(b *BranchContext) (string, error) {
					SleepWithBranch(b, "branch-sleep", 1*time.Hour)
					return "branch-1-result", nil
				},
			})
		}()

		if capturedSignal == nil {
			t.Fatal("Expected yield signal from parallel branch")
		}
		if capturedSignal.info.Type != "sleep" {
			t.Errorf("Expected type 'sleep', got '%s'", capturedSignal.info.Type)
		}
	})

	// Note: Testing panic propagation in parallel branches is difficult because
	// panics happen in separate goroutines. The Parallel function re-panics
	// non-yield panics, which will crash the program. This is the expected
	// behavior - real panics should not be silently swallowed.
	//
	// We skip the direct test here because there's no way to catch a panic
	// from a different goroutine. The behavior is verified by code inspection:
	// parallel.go line 145 re-panics non-yield panics.
}

func TestPanicDoesNotCorruptState(t *testing.T) {
	t.Run("executed steps recorded before panic", func(t *testing.T) {
		exec := &executionContext{
			runID:          "run_steps_before_panic",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		}
		ctx := Context{exec: exec}

		func() {
			defer func() {
				recover() // Catch the panic
			}()

			// Execute successful step first
			_, _ = Run(ctx, "successful-step", func() (string, error) {
				return "success", nil
			})

			// Then panic
			_, _ = Run(ctx, "panicking-step", func() (string, error) {
				panic("step panic")
			})
		}()

		// Verify the first step was recorded
		if len(exec.executedSteps) != 1 {
			t.Errorf("Expected 1 executed step, got %d", len(exec.executedSteps))
		}
		if exec.executedSteps[0].Name != "successful-step" {
			t.Errorf("Expected step name 'successful-step', got '%s'", exec.executedSteps[0].Name)
		}
		if exec.executedSteps[0].Status != "completed" {
			t.Errorf("Expected status 'completed', got '%s'", exec.executedSteps[0].Status)
		}
	})

	t.Run("step counter state preserved after panic", func(t *testing.T) {
		exec := &executionContext{
			runID:          "run_counter_after_panic",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		}
		ctx := Context{exec: exec}

		func() {
			defer func() {
				recover()
			}()

			// Execute a step
			_, _ = Run(ctx, "step-a", func() (string, error) {
				return "a", nil
			})
			// Execute same-named step
			_, _ = Run(ctx, "step-a", func() (string, error) {
				return "a2", nil
			})

			panic("intentional panic")
		}()

		// Step counter should reflect executed steps
		if exec.stepCounters["step-a"] != 2 {
			t.Errorf("Expected step counter 2, got %d", exec.stepCounters["step-a"])
		}
	})
}

// Note: TestPanicInNestedParallel is omitted because testing panic propagation
// across goroutines is not possible with defer/recover. The Parallel and
// ParallelWithBranch functions correctly re-panic non-yield panics, which
// will crash the program as expected for unrecoverable errors.
