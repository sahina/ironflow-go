package ironflow

import (
	"errors"
	"testing"

	"connectrpc.com/connect"
)

// The write throttle's two limits are opposite kinds of failure, and #1972
// step 10 had them classified backwards: the disk cap answered CodeUnavailable
// (retryable, so a full disk retried forever) and the rate cap answered
// CodeResourceExhausted (not retryable, so the one transient case hard-failed).
//
// Pinned here rather than only in internal/server, because the defect was the
// PAIRING of a server code with an SDK table — a test on either side alone
// passes while the pair is wrong.
func TestThrottleRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		code      connect.Code
		retryable bool
		why       string
	}{
		{"rate cap", connect.CodeResourceExhausted, true, "shaped by the limiter; the next second has budget, and Retry-After says when"},
		{"disk cap", connect.CodeFailedPrecondition, false, "permanent until an operator frees space; retrying cannot move it"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := connectError(connect.NewError(tc.code, errors.New("boom")))
			var ifErr *IronflowError
			if !errors.As(err, &ifErr) {
				t.Fatalf("connectError returned %T, want *IronflowError", err)
			}
			if ifErr.Retryable != tc.retryable {
				t.Errorf("Retryable = %v, want %v — %s", ifErr.Retryable, tc.retryable, tc.why)
			}
		})
	}
}

// The Retry-After the rate cap sets must survive into the SDK error, or a
// retryable classification just means "hammer it immediately".
func TestRateCapCarriesRetryAfter(t *testing.T) {
	ce := connect.NewError(connect.CodeResourceExhausted, errors.New("write rate limit exceeded"))
	ce.Meta().Set("Retry-After", "1")
	var ifErr *IronflowError
	if !errors.As(connectError(ce), &ifErr) {
		t.Fatal("want *IronflowError")
	}
	if ifErr.RetryAfter == 0 {
		t.Error("RetryAfter = 0; the limiter's Retry-After header was dropped")
	}
}
