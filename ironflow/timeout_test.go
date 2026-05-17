package ironflow

import (
	"errors"
	"testing"
	"time"
)

func TestStepTimeoutError(t *testing.T) {
	t.Run("creates error with step name and timeout", func(t *testing.T) {
		err := NewStepTimeoutError("call-api", 30*time.Second)

		if err.StepName != "call-api" {
			t.Errorf("expected StepName 'call-api', got '%s'", err.StepName)
		}
		if err.Timeout != 30*time.Second {
			t.Errorf("expected Timeout 30s, got %v", err.Timeout)
		}
		if err.Code != "STEP_TIMEOUT" {
			t.Errorf("expected Code 'STEP_TIMEOUT', got '%s'", err.Code)
		}
		if !err.Retryable {
			t.Error("expected Retryable to be true")
		}
		if err.Error() == "" {
			t.Error("expected non-empty error message")
		}
	})

	t.Run("errors.As works with StepTimeoutError", func(t *testing.T) {
		err := NewStepTimeoutError("slow-step", 5*time.Minute)

		var timeoutErr *StepTimeoutError
		if !errors.As(err, &timeoutErr) {
			t.Fatal("errors.As should match StepTimeoutError")
		}
		if timeoutErr.StepName != "slow-step" {
			t.Errorf("expected StepName 'slow-step', got '%s'", timeoutErr.StepName)
		}
	})

	t.Run("IsRetryable returns true for StepTimeoutError", func(t *testing.T) {
		err := NewStepTimeoutError("step-a", time.Second)
		if !IsRetryable(err) {
			t.Error("expected StepTimeoutError to be retryable")
		}
	})
}

func TestWithTimeout(t *testing.T) {
	t.Run("returns result when function completes before timeout", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		result, err := Run(ctx, "fast-step", func() (string, error) {
			return "done", nil
		}, WithTimeout(5*time.Second))

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "done" {
			t.Errorf("expected 'done', got '%s'", result)
		}
	})

	t.Run("returns StepTimeoutError when function exceeds timeout", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		_, err := Run(ctx, "slow-step", func() (string, error) {
			time.Sleep(5 * time.Second)
			return "never", nil
		}, WithTimeout(50*time.Millisecond))

		if err == nil {
			t.Fatal("expected error")
		}

		var timeoutErr *StepTimeoutError
		if !errors.As(err, &timeoutErr) {
			t.Fatalf("expected StepTimeoutError, got %T: %v", err, err)
		}
		if timeoutErr.StepName != "slow-step" {
			t.Errorf("expected step name 'slow-step', got '%s'", timeoutErr.StepName)
		}
		if timeoutErr.Timeout != 50*time.Millisecond {
			t.Errorf("expected timeout 50ms, got %v", timeoutErr.Timeout)
		}
	})

	t.Run("records timed-out step as failed", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		_, _ = Run(ctx, "timeout-step", func() (string, error) {
			time.Sleep(5 * time.Second)
			return "never", nil
		}, WithTimeout(50*time.Millisecond))

		if len(ctx.exec.executedSteps) != 1 {
			t.Fatalf("expected 1 executed step, got %d", len(ctx.exec.executedSteps))
		}
		if ctx.exec.executedSteps[0].Status != "failed" {
			t.Errorf("expected status 'failed', got '%s'", ctx.exec.executedSteps[0].Status)
		}
	})

	t.Run("memoized step returns without applying timeout", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:        "run_123",
				stepCounters: make(map[string]int),
				completedSteps: map[string]*CompletedStep{
					"run_123:cached-step:0": {
						ID:     "run_123:cached-step:0",
						Name:   "cached-step",
						Status: "completed",
						Output: "cached-value",
					},
				},
				executedSteps: make([]*StepResult, 0),
			},
		}

		callCount := 0
		result, err := Run(ctx, "cached-step", func() (string, error) {
			callCount++
			time.Sleep(time.Minute)
			return "never", nil
		}, WithTimeout(1*time.Millisecond))

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "cached-value" {
			t.Errorf("expected 'cached-value', got '%s'", result)
		}
		if callCount != 0 {
			t.Errorf("function should not have been called, called %d times", callCount)
		}
	})

	t.Run("uses function-level stepTimeout as default", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
				stepTimeout:    50 * time.Millisecond,
			},
		}

		_, err := Run(ctx, "slow-step", func() (string, error) {
			time.Sleep(5 * time.Second)
			return "never", nil
		})

		if err == nil {
			t.Fatal("expected error")
		}

		var timeoutErr *StepTimeoutError
		if !errors.As(err, &timeoutErr) {
			t.Fatalf("expected StepTimeoutError, got %T: %v", err, err)
		}
	})

	t.Run("step-level timeout overrides function-level default", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
				stepTimeout:    10 * time.Millisecond,
			},
		}

		// Step-level 5s should override function-level 10ms
		result, err := Run(ctx, "fast-step", func() (string, error) {
			time.Sleep(50 * time.Millisecond)
			return "done", nil
		}, WithTimeout(5*time.Second))

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "done" {
			t.Errorf("expected 'done', got '%s'", result)
		}
	})

	t.Run("propagates function error when it occurs before timeout", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		_, err := Run(ctx, "error-step", func() (string, error) {
			return "", errors.New("step failed")
		}, WithTimeout(5*time.Second))

		if err == nil {
			t.Fatal("expected error")
		}

		// Should be a StepError, not StepTimeoutError
		var stepErr *StepError
		if !errors.As(err, &stepErr) {
			t.Fatalf("expected StepError, got %T: %v", err, err)
		}
		if stepErr.StepName != "error-step" {
			t.Errorf("expected step name 'error-step', got '%s'", stepErr.StepName)
		}
	})
}
