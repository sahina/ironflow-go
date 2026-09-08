package ironflow

import (
	"errors"
	"strings"

	"connectrpc.com/connect"
)

// connectError converts a ConnectRPC failure into the *IronflowError every
// other method on this client returns (#1972 step 10).
//
// Why this exists rather than returning the connect.Error directly: the SDK's
// error contract is the struct, not the transport. `IsRetryable` reads
// IronflowError.Retryable and **defaults to true for a type it does not
// recognize**, so handing back a bare *connect.Error tells callers to retry a
// rejected query forever. `errors.Is(err, ErrUnauthorized)` and the other
// sentinels go quiet the same way. A method that moves from REST to Connect
// must not change what a caller matches on.
//
// #1972 step 10 removes 35 duplicate REST routes, each move re-posing this
// question for its consumer, so the mapping lives here once.
//
// Retryable mirrors what restRequest did — `resp.StatusCode >= 500` — read
// through Connect's own code-to-status table, so the same failure classifies
// the same way on either transport.
func connectError(err error) error {
	if err == nil {
		return nil
	}

	var ce *connect.Error
	if !errors.As(err, &ce) {
		// A transport or context failure that never reached the server. It has
		// no Connect code to read, and it is exactly the class REST called
		// retryable.
		return WrapError(err, "request failed", "REQUEST_FAILED", true)
	}

	code := connect.CodeOf(ce)
	ifErr := NewError(
		strings.ToUpper(code.String())+": "+ce.Message(),
		strings.ToUpper(code.String()),
		isRetryableConnectCode(code),
	)
	ifErr.Details = map[string]any{"connect_code": code.String()}
	ifErr.Cause = ce
	ifErr.RetryAfter = parseRetryAfter(ce.Meta().Get("Retry-After"))

	// The sentinels REST attached by status, attached by code instead. The
	// Connect code is the contract and the HTTP status it serializes to is not
	// (ADR 0079 section 3) — CodeAborted and CodeAlreadyExists both serialize
	// to 409 and want opposite things, so reading the code is what keeps
	// ErrContended distinct from ErrConflict here.
	switch code {
	case connect.CodeUnauthenticated:
		ifErr.Cause = ErrUnauthorized
		ifErr.Message += " — " + AuthHelp
	case connect.CodePermissionDenied:
		ifErr.Cause = ErrForbidden
		ifErr.Message += " — " + AuthHelp
	case connect.CodeAborted:
		// Two opposite outcomes share this code, so the code alone is not the
		// answer here: InjectStepOutput answers aborted BOTH when the CAS lost
		// and nothing was written and when the step write landed but the run
		// moved under it. Reading only the code told a caller "nothing was
		// applied" about a durable write (#2093). The server names which one in
		// Ironflow-Error-Reason; an aborted with no reason is a pre-#2093
		// server or a non-inject RPC, and contention is the right default for
		// both -- ErrContended is what every other producer of this code means.
		if ce.Meta().Get(ErrorReasonHeader) == ReasonInjectionUnverified {
			ifErr.Cause = ErrInjectionUnverified
		} else {
			ifErr.Cause = ErrContended
		}
	case connect.CodeAlreadyExists:
		// A deduplicated request: wait, do not reissue.
		ifErr.Cause = ErrConflict
	default:
		// Every other code keeps the *connect.Error as its Cause, set above.
		// No sentinel exists for them, and inventing one per code would give
		// callers something to match on that the REST path never offered.
	}

	return ifErr
}

// isRetryableConnectCode reports whether re-sending the identical request can
// succeed. That is narrower than "the server failed": `Retryable` drives
// requestWith, which re-sends the same marshaled body unchanged.
//
// The 5xx codes qualify because the request never took effect. So does
// CodeResourceExhausted, which is what the write-rate limiter answers — a
// shaped request that the very next second has budget for, and the one code
// that carries a Retry-After the caller should honor (`connectError` parses it
// into IronflowError.RetryAfter).
//
// CodeAborted is deliberately absent despite reading as transient: it is a lost
// CAS race, so the caller must RE-READ before reissuing, not resend the stale
// body. That is what the ErrContended sentinel above is for.
//
// CodeFailedPrecondition is absent for the opposite reason — it is the disk-cap
// wall, permanent until an operator frees space. It used to be answered as
// CodeUnavailable, which landed in the retryable set below and turned a full
// disk into an infinite retry loop (#1972 step 10).
func isRetryableConnectCode(code connect.Code) bool {
	switch code {
	case connect.CodeUnknown, // 500
		connect.CodeDeadlineExceeded,  // 504
		connect.CodeUnimplemented,     // 501
		connect.CodeInternal,          // 500
		connect.CodeUnavailable,       // 503
		connect.CodeDataLoss,          // 500
		connect.CodeResourceExhausted: // 429 — rate limited, honor Retry-After
		return true
	default:
		return false
	}
}
