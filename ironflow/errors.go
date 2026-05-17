package ironflow

import (
	"errors"
	"fmt"
	"time"
)

// Common error types
var (
	// ErrFunctionNotFound is returned when a function is not found.
	ErrFunctionNotFound = errors.New("function not found")

	// ErrRunNotFound is returned when a run is not found.
	ErrRunNotFound = errors.New("run not found")

	// ErrInvalidSignature is returned when webhook signature verification fails.
	ErrInvalidSignature = errors.New("invalid signature")

	// ErrSignatureExpired is returned when the signature timestamp is too old.
	ErrSignatureExpired = errors.New("signature expired")

	// ErrMissingSignature is returned when the signature header is missing.
	ErrMissingSignature = errors.New("missing signature")

	// ErrTimeout is returned when an operation times out.
	ErrTimeout = errors.New("timeout")

	// ErrValidation is returned when validation fails.
	ErrValidation = errors.New("validation error")

	// ErrUnauthorized is returned when the API key is missing or invalid (HTTP 401).
	ErrUnauthorized = errors.New("unauthorized")

	// ErrEnterpriseLicenseRequired is returned when an enterprise license is needed (HTTP 402).
	ErrEnterpriseLicenseRequired = errors.New("enterprise license required")

	// ErrForbidden is returned when the caller lacks permission (HTTP 403).
	ErrForbidden = errors.New("forbidden")
)

// IronflowError is the base error type for all Ironflow errors.
type IronflowError struct {
	// Message is the error message.
	Message string

	// Code is the error code for categorization.
	Code string

	// Retryable indicates if the operation can be retried.
	Retryable bool

	// RetryAfter is the duration to wait before retrying (from Retry-After header).
	RetryAfter time.Duration

	// Details contains additional error details.
	Details map[string]any

	// Cause is the underlying error.
	Cause error
}

// Error implements the error interface.
func (e *IronflowError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

// Unwrap returns the underlying error.
func (e *IronflowError) Unwrap() error {
	return e.Cause
}

// Is implements error matching for errors.Is.
func (e *IronflowError) Is(target error) bool {
	if t, ok := target.(*IronflowError); ok {
		return e.Code == t.Code
	}
	return false
}

// NewError creates a new IronflowError.
func NewError(message, code string, retryable bool) *IronflowError {
	return &IronflowError{
		Message:   message,
		Code:      code,
		Retryable: retryable,
	}
}

// WrapError wraps an error with additional context.
func WrapError(err error, message, code string, retryable bool) *IronflowError {
	return &IronflowError{
		Message:   message,
		Code:      code,
		Retryable: retryable,
		Cause:     err,
	}
}

// StepError is returned when step execution fails.
type StepError struct {
	*IronflowError
	StepID   string
	StepName string
}

// NewStepError creates a new StepError.
func NewStepError(message, stepID, stepName string, retryable bool, cause error) *StepError {
	return &StepError{
		IronflowError: &IronflowError{
			Message:   message,
			Code:      "STEP_ERROR",
			Retryable: retryable,
			Cause:     cause,
			Details: map[string]any{
				"stepId":   stepID,
				"stepName": stepName,
			},
		},
		StepID:   stepID,
		StepName: stepName,
	}
}

// StepTimeoutError is returned when a step exceeds its configured timeout.
type StepTimeoutError struct {
	*IronflowError
	StepName string
	Timeout  time.Duration
}

// NewStepTimeoutError creates a new StepTimeoutError.
func NewStepTimeoutError(stepName string, timeout time.Duration) *StepTimeoutError {
	return &StepTimeoutError{
		IronflowError: &IronflowError{
			Message:   fmt.Sprintf("step %q timed out after %s", stepName, timeout),
			Code:      "STEP_TIMEOUT",
			Retryable: true,
		},
		StepName: stepName,
		Timeout:  timeout,
	}
}

// NonRetryableError wraps an error to mark it as non-retryable.
type NonRetryableError struct {
	*IronflowError
}

// NewNonRetryableError creates a new non-retryable error.
func NewNonRetryableError(message string) *NonRetryableError {
	return &NonRetryableError{
		IronflowError: &IronflowError{
			Message:   message,
			Code:      "NON_RETRYABLE",
			Retryable: false,
		},
	}
}

// WrapNonRetryable wraps an existing error as non-retryable.
func WrapNonRetryable(err error) *NonRetryableError {
	return &NonRetryableError{
		IronflowError: &IronflowError{
			Message:   err.Error(),
			Code:      "NON_RETRYABLE",
			Retryable: false,
			Cause:     err,
		},
	}
}

// yieldSignal is an internal signal to yield execution.
// It is not a real error but uses the error interface for control flow.
type yieldSignal struct {
	info *YieldInfo
}

func (y *yieldSignal) Error() string {
	return "yield signal"
}

func newSleepYield(stepID, until string) *yieldSignal {
	return &yieldSignal{
		info: &YieldInfo{
			StepID: stepID,
			Type:   "sleep",
			Until:  until,
		},
	}
}

func newWaitEventYield(stepID string, filter *EventFilter) *yieldSignal {
	// Convert the Timeout duration to a string for JSON serialization
	if filter.Timeout > 0 && filter.TimeoutStr == "" {
		filter.TimeoutStr = formatDuration(filter.Timeout)
	}
	return &yieldSignal{
		info: &YieldInfo{
			StepID:      stepID,
			Type:        "wait_for_event",
			EventFilter: filter,
		},
	}
}

// formatDuration converts a time.Duration to a compact string (e.g., "30s", "5m", "1h").
func formatDuration(d time.Duration) string {
	if d >= time.Hour && d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	if d >= time.Minute && d%time.Minute == 0 {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	return fmt.Sprintf("%ds", int(d.Seconds()))
}

// IsRetryable checks if an error is retryable.
func IsRetryable(err error) bool {
	var ironflowErr *IronflowError
	if errors.As(err, &ironflowErr) {
		return ironflowErr.Retryable
	}
	// Default to retryable for unknown errors
	return true
}

func newInvokeYield(info *YieldInfo) *yieldSignal {
	return &yieldSignal{info: info}
}

// IsYieldSignal checks if an error is a yield signal.
func isYieldSignal(err error) (*yieldSignal, bool) {
	var signal *yieldSignal
	if errors.As(err, &signal) {
		return signal, true
	}
	return nil, false
}
