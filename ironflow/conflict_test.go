package ironflow

// #1963: a 409 has to be distinguishable, and never retryable.
//
// ResumeRun is the reason. When an identical resume is already in flight inside
// the stream's dedupe window the server leaves the run exactly as it was found
// and answers 409, telling the caller to wait. This SDK is Connect-only, so
// before #1963 it received that as a 500 -- and its status table marks anything
// >= 500 Retryable, so the one error whose entire purpose is "do not retry"
// asked callers to retry.
//
// Two properties, both of which a caller depends on: errors.Is finds
// ErrConflict, and Retryable is false.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const dedupeMessage = "a resume for this run is already in flight; wait for it to land before retrying"

func TestResumeRun_ConflictIsTypedAndNotRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ironflow.v1.IronflowService/ResumeRun" {
			t.Errorf("unexpected path %q; ResumeRun is on the Connect RPC", r.URL.Path)
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"already_exists","message":"` + dedupeMessage + `"}`))
	}))
	defer server.Close()

	client := newAuthTestClient(server)
	_, err := client.ResumeRun(context.Background(), "run-1", "")

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrConflict) {
		t.Errorf("expected errors.Is(err, ErrConflict), got: %v", err)
	}

	var ifErr *IronflowError
	if !errors.As(err, &ifErr) {
		t.Fatalf("expected *IronflowError, got %T", err)
	}
	// The retry flag is the half that was actually wrong before #1963.
	if ifErr.Retryable {
		t.Error("409 must not be Retryable -- retrying is the thing this error reports against")
	}
	// The server puts the actionable sentence on the sentinel so both
	// transports carry it; a caller that surfaces the message gets the advice.
	if !strings.Contains(ifErr.Message, "wait for it to land") {
		t.Errorf("message %q dropped the actionable advice", ifErr.Message)
	}
}

// TestConflict_IsDistinctFromOtherStatuses guards the obvious way to "fix" a
// failing ErrConflict assertion: making every error match it.
func TestConflict_IsDistinctFromOtherStatuses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
	}{
		{"not found", http.StatusNotFound},
		{"precondition failed", http.StatusPreconditionFailed},
		{"internal", http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"code":"x","message":"nope"}`))
			}))
			defer server.Close()

			client := newAuthTestClient(server)
			_, err := client.ResumeRun(context.Background(), "run-1", "")
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if errors.Is(err, ErrConflict) {
				t.Errorf("status %d must not map to ErrConflict", tc.status)
			}
		})
	}
}

// #2074: 409 stopped meaning one thing.
//
// Connect serializes both CodeAlreadyExists and CodeAborted to 409, and the two
// want opposite things from the caller -- wait vs retry. The status alone
// cannot pick between them, so the discriminator is the Connect code in the
// error body, which executeRequest already unmarshals.
//
// The third row is the one that keeps this honest: a REST-shaped 409 carries no
// Connect code at all, and it has to keep landing on ErrConflict.
//
// The sentinel is the whole discrimination. Neither is Retryable -- see
// TestContended_IsNotReissued below for why -- so a test asserting only on the
// flag would pass on both rows and prove nothing.
func TestConflict_DiscriminatesOnConnectCode(t *testing.T) {
	for _, tc := range []struct {
		name         string
		body         string
		wantSentinel error
	}{
		{
			name:         "aborted is contention",
			body:         `{"code":"aborted","message":"resume run contended after retry"}`,
			wantSentinel: ErrContended,
		},
		{
			name:         "already_exists is dedupe",
			body:         `{"code":"already_exists","message":"` + dedupeMessage + `"}`,
			wantSentinel: ErrConflict,
		},
		{
			name:         "no code stays a conflict",
			body:         `{"error":"conflict"}`,
			wantSentinel: ErrConflict,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			client := newAuthTestClient(server)
			_, err := client.ResumeRun(context.Background(), "run-1", "")
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, tc.wantSentinel) {
				t.Errorf("expected errors.Is(err, %v), got: %v", tc.wantSentinel, err)
			}
			// A tripwire, not the primary check: today the two sentinels are
			// independent errors.New values and only one is ever assigned, so
			// this cannot fire. It exists because #2055 wraps the engine-side
			// ErrContended in ErrInvalidStateTransition, and the same instinct
			// applied here -- wrapping one sentinel in the other -- would make
			// a caller's `errors.Is(err, ErrConflict)` catch contention again,
			// which is the exact regression #2074 is about.
			other := ErrConflict
			if tc.wantSentinel == ErrConflict {
				other = ErrContended
			}
			if errors.Is(err, other) {
				t.Errorf("must not also match %v", other)
			}
			// Read the flag off the error rather than through IsRetryable,
			// which defaults to true for anything that is not an
			// *IronflowError -- so the helper would pass here for the wrong
			// reason if the 4xx path ever stopped returning one.
			var ifErr *IronflowError
			if !errors.As(err, &ifErr) {
				t.Fatalf("expected *IronflowError, got %T", err)
			}
			if ifErr.Retryable {
				t.Error("no 409 is blindly Retryable -- see TestContended_IsNotReissued")
			}
		})
	}
}

// TestContended_IsNotReissued is the guard on the tempting half of #2074.
//
// The issue says contention means "retry", and marking the error Retryable
// looks like the way to say so. It is not: Retryable is what requestWith reads
// to re-send the identical marshaled body, while gRPC defines Aborted as
// "retry at a HIGHER level" -- restart the read-modify-write. Where the caller
// supplied the version (entity-stream append, the webhook mutators) a blind
// reissue sends the same stale version and fails identically, three times, on
// the default retry config; where the server read it (UpdateFunction,
// UpdateFunctionStatus, RollbackFunction, CancelRun) the reissue could land
// instead, re-applying a write the caller never re-read.
//
// Counting requests is the only assertion that catches this. errors.Is and
// IsRetryable both still pass with the reissue in place.
//
// Deliberately NOT newAuthTestClient: that helper pins MaxAttempts to 1, so the
// retry loop never runs and this test would pass no matter what Retryable said.
// The default config is the one shipped callers get.
func TestContended_IsNotReissued(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"aborted","message":"version conflict"}`))
	}))
	defer server.Close()

	client := NewClient(ClientConfig{ServerURL: server.URL})
	if client.retryConfig == nil || client.retryConfig.MaxAttempts < 2 {
		t.Fatalf("this test is vacuous unless the default config retries: %+v", client.retryConfig)
	}

	if _, err := client.ResumeRun(context.Background(), "run-1", ""); err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("server saw %d requests, want 1 -- a contended write must not be reissued with the same body", got)
	}
}
