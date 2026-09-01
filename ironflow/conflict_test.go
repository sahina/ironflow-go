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
