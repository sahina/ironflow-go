package ironflow

import (
	"context"
	"testing"
	"time"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

// recvFenceMsg reads one outgoing worker message or fails the test.
func recvFenceMsg(t *testing.T, ch <-chan *ironflowv1.WorkerMessage) *ironflowv1.WorkerMessage {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(time.Second):
		t.Fatal("no message received")
		return nil
	}
}

// TestStreamJobReporter_EchoesFence verifies the streaming worker copies the
// execution fence (execution_seq + lease_token) it received on the JobAssignment
// onto every mutating message it sends back (#1206, ADR 0037, chunk 3e SDK echo).
// Without this echo the engine's ingress fence guard has nothing to validate.
func TestStreamJobReporter_EchoesFence(t *testing.T) {
	const (
		wantSeq   = int64(7)
		wantToken = "lease-tok"
	)
	r := &streamJobReporter{logger: NewNoopLogger(), executionSeq: wantSeq, leaseToken: wantToken}

	assertFence := func(t *testing.T, seq int64, token string) {
		t.Helper()
		if seq != wantSeq {
			t.Errorf("execution_seq = %d, want %d", seq, wantSeq)
		}
		if token != wantToken {
			t.Errorf("lease_token = %q, want %q", token, wantToken)
		}
	}

	t.Run("JobCompleted", func(t *testing.T) {
		outCh := make(chan *ironflowv1.WorkerMessage, 1)
		r.outCh = outCh
		if err := r.ReportCompleted(context.Background(), "job-1", map[string]any{"ok": true}, nil); err != nil {
			t.Fatal(err)
		}
		jc := recvFenceMsg(t, outCh).GetPayload().(*ironflowv1.WorkerMessage_JobCompleted).JobCompleted
		assertFence(t, jc.GetExecutionSeq(), jc.GetLeaseToken())
	})

	t.Run("JobFailed", func(t *testing.T) {
		outCh := make(chan *ironflowv1.WorkerMessage, 1)
		r.outCh = outCh
		if err := r.ReportFailed(context.Background(), "job-1", &PushError{Message: "x"}, nil); err != nil {
			t.Fatal(err)
		}
		jf := recvFenceMsg(t, outCh).GetPayload().(*ironflowv1.WorkerMessage_JobFailed).JobFailed
		assertFence(t, jf.GetExecutionSeq(), jf.GetLeaseToken())
	})

	t.Run("StepYielded_sleep", func(t *testing.T) {
		outCh := make(chan *ironflowv1.WorkerMessage, 1)
		r.outCh = outCh
		yield := &YieldInfo{Type: "sleep", StepID: "s1", Until: time.Now().Add(time.Hour).Format(time.RFC3339)}
		if err := r.ReportYielded(context.Background(), "job-1", yield); err != nil {
			t.Fatal(err)
		}
		sy := recvFenceMsg(t, outCh).GetPayload().(*ironflowv1.WorkerMessage_StepYielded).StepYielded
		assertFence(t, sy.GetExecutionSeq(), sy.GetLeaseToken())
	})
}

// TestProtoToJobAssignment_CarriesFence confirms the inbound fence on a
// JobAssignment survives the proto->SDK conversion so the reporter can echo it.
func TestProtoToJobAssignment_CarriesFence(t *testing.T) {
	pa := &ironflowv1.JobAssignment{
		JobId:        "job-1",
		RunId:        "run-1",
		FunctionId:   "fn-1",
		ExecutionSeq: 9,
		LeaseToken:   "tok-xyz",
	}
	job, err := protoToJobAssignment(pa)
	if err != nil {
		t.Fatalf("protoToJobAssignment: %v", err)
	}
	if job.ExecutionSeq != 9 {
		t.Errorf("ExecutionSeq = %d, want 9", job.ExecutionSeq)
	}
	if job.LeaseToken != "tok-xyz" {
		t.Errorf("LeaseToken = %q, want tok-xyz", job.LeaseToken)
	}
}
