package ironflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
)

// TestAborted_DiscriminatesOnReasonHeader is #2074's question one level down.
//
// #2074 split 409 by Connect code because CodeAlreadyExists and CodeAborted
// want opposite things. CodeAborted then stopped meaning one thing too:
// InjectStepOutput answers it when the CAS lost and NOTHING was written, and
// also when the step write LANDED but the run moved under it (#2073). Both
// arrive as aborted/409, and this SDK collapsed both onto ErrContended, whose
// doc promises "nothing was applied" -- a lie for the second one, and the
// dangerous direction: it invites a reissue that overwrites a durable write the
// caller has not read.
//
// The server names which one in Ironflow-Error-Reason. The absent-header row is
// the one that keeps this honest: every other producer of aborted means
// contention, and a pre-#2093 server sends no header at all, so the default
// must stay ErrContended.
func TestAborted_DiscriminatesOnReasonHeader(t *testing.T) {
	for _, tc := range []struct {
		name         string
		reason       string
		wantSentinel error
		otherwise    error
	}{
		{
			name:         "injection_unverified is a durable write",
			reason:       ReasonInjectionUnverified,
			wantSentinel: ErrInjectionUnverified,
			otherwise:    ErrContended,
		},
		{
			name:         "contended is a lost race",
			reason:       ReasonContended,
			wantSentinel: ErrContended,
			otherwise:    ErrInjectionUnverified,
		},
		{
			name:         "no reason header stays contention",
			reason:       "",
			wantSentinel: ErrContended,
			otherwise:    ErrInjectionUnverified,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.reason != "" {
					w.Header().Set(ErrorReasonHeader, tc.reason)
				}
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"code":"aborted","message":"inject step output"}`))
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
			// The two sentinels must partition, not overlap. A caller that
			// checks ErrContended first and reissues would re-apply a landed
			// write if the unverified case also matched it.
			if errors.Is(err, tc.otherwise) {
				t.Errorf("must not also match %v -- the two make opposite promises about "+
					"durable state", tc.otherwise)
			}
			// Neither is blindly Retryable: one needs a re-read before the
			// reissue, the other needs a human to look at the step.
			var ifErr *IronflowError
			if !errors.As(err, &ifErr) {
				t.Fatalf("expected *IronflowError, got %T", err)
			}
			if ifErr.Retryable {
				t.Error("no aborted is blindly Retryable -- see TestContended_IsNotReissued")
			}
		})
	}
}

// TestConnectError_AbortedDiscriminatesOnReasonHeader is the same split on the
// OTHER transport arm. connectError maps a *connect.Error and reads the reason
// from ce.Meta(); client.go's executeRequest maps a JSON body and reads it from
// resp.Header. Two code paths, one wire contract -- and the ConnectRPC one is
// what every migrated RPC actually takes, so testing only the JSON arm would
// pin the half fewer callers reach.
func TestConnectError_AbortedDiscriminatesOnReasonHeader(t *testing.T) {
	for _, tc := range []struct {
		name         string
		reason       string
		wantSentinel error
		otherwise    error
	}{
		{"injection_unverified", ReasonInjectionUnverified, ErrInjectionUnverified, ErrContended},
		{"contended", ReasonContended, ErrContended, ErrInjectionUnverified},
		{"absent header", "", ErrContended, ErrInjectionUnverified},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ce := connect.NewError(connect.CodeAborted, errors.New("inject step output"))
			if tc.reason != "" {
				ce.Meta().Set(ErrorReasonHeader, tc.reason)
			}
			err := connectError(ce)
			if !errors.Is(err, tc.wantSentinel) {
				t.Errorf("expected errors.Is(err, %v), got: %v", tc.wantSentinel, err)
			}
			if errors.Is(err, tc.otherwise) {
				t.Errorf("must not also match %v", tc.otherwise)
			}
		})
	}
}
