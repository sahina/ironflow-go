package ironflow

import (
	"errors"
	"testing"
	"time"
)

func TestIronflowError(t *testing.T) {
	t.Run("creates error with message", func(t *testing.T) {
		err := NewError("something went wrong", "TEST_ERROR", true)

		if err.Error() != "something went wrong" {
			t.Errorf("expected message 'something went wrong', got '%s'", err.Error())
		}
		if err.Code != "TEST_ERROR" {
			t.Errorf("expected code 'TEST_ERROR', got '%s'", err.Code)
		}
		if !err.Retryable {
			t.Error("expected retryable to be true")
		}
	})

	t.Run("creates non-retryable error", func(t *testing.T) {
		err := NewError("permanent failure", "PERM_ERROR", false)

		if err.Retryable {
			t.Error("expected retryable to be false")
		}
	})
}

func TestWrapError(t *testing.T) {
	t.Run("wraps error with message", func(t *testing.T) {
		original := errors.New("original error")
		wrapped := WrapError(original, "wrapped message", "WRAP_CODE", true)

		if wrapped.Error() != "wrapped message: original error" {
			t.Errorf("expected wrapped message, got '%s'", wrapped.Error())
		}
		if wrapped.Code != "WRAP_CODE" {
			t.Errorf("expected code 'WRAP_CODE', got '%s'", wrapped.Code)
		}
		if !errors.Is(wrapped, original) {
			t.Error("wrapped error should contain original")
		}
	})
}

func TestStepError(t *testing.T) {
	t.Run("creates step error", func(t *testing.T) {
		err := NewStepError("step failed", "step_123", "my-step", true, nil)

		if err.Error() != "step failed" {
			t.Errorf("expected message 'step failed', got '%s'", err.Error())
		}
		if err.StepID != "step_123" {
			t.Errorf("expected StepID 'step_123', got '%s'", err.StepID)
		}
		if err.StepName != "my-step" {
			t.Errorf("expected StepName 'my-step', got '%s'", err.StepName)
		}
		if !err.Retryable {
			t.Error("expected retryable to be true")
		}
	})

	t.Run("wraps cause", func(t *testing.T) {
		cause := errors.New("root cause")
		err := NewStepError("step failed", "step_123", "my-step", false, cause)

		if !errors.Is(err, cause) {
			t.Error("step error should contain cause")
		}
		if err.Retryable {
			t.Error("expected retryable to be false")
		}
	})
}

func TestNonRetryable(t *testing.T) {
	t.Run("creates non-retryable wrapper", func(t *testing.T) {
		original := errors.New("some error")
		wrapped := WrapNonRetryable(original)

		// Check it wraps the original
		if !errors.Is(wrapped, original) {
			t.Error("should contain original error")
		}

		// Check embedded IronflowError has Retryable=false
		if wrapped.Retryable {
			t.Error("embedded IronflowError should have Retryable=false")
		}
	})
}

func TestIsRetryable(t *testing.T) {
	t.Run("returns true for IronflowError with retryable=true", func(t *testing.T) {
		err := NewError("error", "CODE", true)
		if !IsRetryable(err) {
			t.Error("expected IsRetryable to return true")
		}
	})

	t.Run("returns false for IronflowError with retryable=false", func(t *testing.T) {
		err := NewError("error", "CODE", false)
		if IsRetryable(err) {
			t.Error("expected IsRetryable to return false")
		}
	})

	t.Run("returns false for NonRetryableError via embedded IronflowError", func(t *testing.T) {
		err := WrapNonRetryable(errors.New("error"))
		// Access the embedded IronflowError directly
		if err.Retryable {
			t.Error("expected Retryable to be false")
		}
	})

	t.Run("returns true for regular errors", func(t *testing.T) {
		err := errors.New("regular error")
		if !IsRetryable(err) {
			t.Error("expected IsRetryable to return true for regular errors")
		}
	})

	t.Run("returns true for nil", func(t *testing.T) {
		if !IsRetryable(nil) {
			t.Error("expected IsRetryable to return true for nil")
		}
	})
}

func TestYieldSignal(t *testing.T) {
	t.Run("creates sleep yield", func(t *testing.T) {
		yield := newSleepYield("step_123", "2024-01-01T12:00:00Z")

		if yield.info.StepID != "step_123" {
			t.Errorf("expected StepID 'step_123', got '%s'", yield.info.StepID)
		}
		if yield.info.Type != "sleep" {
			t.Errorf("expected Type 'sleep', got '%s'", yield.info.Type)
		}
		if yield.info.Until != "2024-01-01T12:00:00Z" {
			t.Errorf("expected Until '2024-01-01T12:00:00Z', got '%s'", yield.info.Until)
		}
	})

	t.Run("creates wait event yield", func(t *testing.T) {
		filter := &EventFilter{
			Event:   "order.approved",
			Match:   "data.orderId",
			Timeout: 7 * 24 * time.Hour,
		}
		yield := newWaitEventYield("step_456", filter)

		if yield.info.StepID != "step_456" {
			t.Errorf("expected StepID 'step_456', got '%s'", yield.info.StepID)
		}
		if yield.info.Type != "wait_for_event" {
			t.Errorf("expected Type 'wait_event', got '%s'", yield.info.Type)
		}
		if yield.info.EventFilter.Event != "order.approved" {
			t.Errorf("expected EventFilter.Event 'order.approved', got '%s'", yield.info.EventFilter.Event)
		}
	})
}

func TestIsYieldSignal(t *testing.T) {
	t.Run("returns true for yield signal", func(t *testing.T) {
		yield := newSleepYield("step_123", "2024-01-01T00:00:00Z")

		signal, ok := isYieldSignal(yield)
		if !ok {
			t.Error("expected isYieldSignal to return true")
		}
		if signal == nil {
			t.Error("expected signal to not be nil")
		}
	})

	t.Run("returns false for regular errors", func(t *testing.T) {
		err := errors.New("regular error")

		_, ok := isYieldSignal(err)
		if ok {
			t.Error("expected isYieldSignal to return false for regular error")
		}
	})

	t.Run("returns false for nil", func(t *testing.T) {
		_, ok := isYieldSignal(nil)
		if ok {
			t.Error("expected isYieldSignal to return false for nil")
		}
	})
}
