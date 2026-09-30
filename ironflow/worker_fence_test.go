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
		if err := r.ReportCompleted(context.Background(), "job-1", map[string]any{"ok": true}, nil, 0); err != nil {
			t.Fatal(err)
		}
		jc := recvFenceMsg(t, outCh).GetPayload().(*ironflowv1.WorkerMessage_JobCompleted).JobCompleted
		assertFence(t, jc.GetExecutionSeq(), jc.GetLeaseToken())
	})

	t.Run("JobFailed", func(t *testing.T) {
		outCh := make(chan *ironflowv1.WorkerMessage, 1)
		r.outCh = outCh
		if err := r.ReportFailed(context.Background(), "job-1", &PushError{Message: "x"}, nil, 0); err != nil {
			t.Fatal(err)
		}
		jf := recvFenceMsg(t, outCh).GetPayload().(*ironflowv1.WorkerMessage_JobFailed).JobFailed
		assertFence(t, jf.GetExecutionSeq(), jf.GetLeaseToken())
	})

	t.Run("StepYielded_sleep", func(t *testing.T) {
		outCh := make(chan *ironflowv1.WorkerMessage, 1)
		r.outCh = outCh
		yield := &YieldInfo{Type: "sleep", StepID: "s1", Until: time.Now().Add(time.Hour).Format(time.RFC3339)}
		if err := r.ReportYielded(context.Background(), "job-1", yield, nil, 0); err != nil {
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

// Step rows only exist if Step* carry the job ID (the engine's run ID) and the
// fence; a bare streamStepReporter left them empty (#2413).
func TestStreamingJobStepMessagesCarryJobAndFence(t *testing.T) {
	fn := CreateFunction(FunctionConfig{ID: "fn"}, func(ctx Context) (any, error) {
		return Run(ctx, "a", func() (string, error) { return "ok", nil })
	})
	exec := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
	outCh := make(chan *ironflowv1.WorkerMessage, 16)
	rep := &streamJobReporter{outCh: outCh, logger: NewNoopLogger(), executionSeq: 7, leaseToken: "tok"}
	job := &jobAssignment{JobID: "job-1", RunID: "run-1", FunctionID: "fn", Attempt: 1}
	if err := exec.execute(context.Background(), job, rep); err != nil {
		t.Fatal(err)
	}
	close(outCh)
	var started, completed bool
	for msg := range outCh {
		switch p := msg.GetPayload().(type) {
		case *ironflowv1.WorkerMessage_StepStarted:
			started = p.StepStarted.GetJobId() == "job-1" && p.StepStarted.GetExecutionSeq() == 7 && p.StepStarted.GetLeaseToken() == "tok"
		case *ironflowv1.WorkerMessage_StepCompleted:
			completed = p.StepCompleted.GetJobId() == "job-1" && p.StepCompleted.GetExecutionSeq() == 7 && p.StepCompleted.GetLeaseToken() == "tok"
		}
	}
	if !started || !completed {
		t.Errorf("StepStarted/StepCompleted missing job ID or fence: started=%v completed=%v", started, completed)
	}
}

// A parallel branch step (RunWithBranch) reports like Run, and a terminal
// failure carries its compensation steps on JobFailed, since the streaming
// worker has no checkpointer to hand them over (#2413).
func TestStreamingBranchStepsAndCompensationsReported(t *testing.T) {
	fn := CreateFunction(FunctionConfig{ID: "fn"}, func(ctx Context) (any, error) {
		if _, err := Run(ctx, "a", func() (string, error) { return "ok", nil }); err != nil {
			return nil, err
		}
		Compensate(ctx, "a", func() error { return nil })
		if _, err := Parallel(ctx, "fan", []func(*BranchContext) (string, error){
			func(b *BranchContext) (string, error) {
				return RunWithBranch(b, "leaf", func() (string, error) { return "x", nil })
			},
		}); err != nil {
			return nil, err
		}
		return nil, NewNonRetryableError("stop")
	})
	exec := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
	outCh := make(chan *ironflowv1.WorkerMessage, 32)
	rep := &streamJobReporter{outCh: outCh, logger: NewNoopLogger()}
	if err := exec.execute(context.Background(), &jobAssignment{JobID: "job-1", RunID: "run-1", FunctionID: "fn", Attempt: 1}, rep); err != nil {
		t.Fatal(err)
	}
	close(outCh)
	completed := map[string]bool{}
	var compensations []string
	for msg := range outCh {
		switch p := msg.GetPayload().(type) {
		case *ironflowv1.WorkerMessage_StepCompleted:
			completed[p.StepCompleted.GetStepId()] = true
		case *ironflowv1.WorkerMessage_JobFailed:
			for _, s := range p.JobFailed.GetSteps() {
				compensations = append(compensations, s.GetId())
			}
		}
	}
	if !completed["run-1:fan:0:leaf:0"] {
		t.Errorf("branch step not reported: %v", completed)
	}
	if len(compensations) != 1 || compensations[0] != "run-1:compensate:a:0" {
		t.Errorf("JobFailed steps = %v, want only run-1:compensate:a:0", compensations)
	}
}
