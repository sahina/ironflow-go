package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type checkpointTestReporter struct {
	mu         sync.Mutex
	completed  []completedStep
	updates    []checkpointTestUpdate
	checkpoint chan struct{}
}

type checkpointTestUpdate struct {
	status string
	steps  []*StepResult
	offset int
}

func (r *checkpointTestReporter) ReportCompleted(context.Context, string, any, []*StepResult, int) error {
	return nil
}
func (r *checkpointTestReporter) ReportFailed(context.Context, string, *PushError, []*StepResult, int) error {
	return nil
}
func (r *checkpointTestReporter) ReportYielded(context.Context, string, *YieldInfo, []*StepResult, int) error {
	return nil
}
func (r *checkpointTestReporter) ReportProgress(_ context.Context, _ string, steps []*StepResult, offset int) error {
	r.mu.Lock()
	r.updates = append(r.updates, checkpointTestUpdate{status: "progress", steps: steps, offset: offset})
	for _, step := range steps {
		if step.Status == "completed" {
			r.completed = append(r.completed, completedStep{StepID: step.ID, Name: step.Name, Output: step.Output})
		}
	}
	r.mu.Unlock()
	select {
	case r.checkpoint <- struct{}{}:
	default:
	}
	return nil
}
func (r *checkpointTestReporter) ReportCompletedAt(_ context.Context, _ string, _ any, steps []*StepResult, offset int) error {
	r.record("completed", steps, offset)
	return nil
}
func (r *checkpointTestReporter) ReportFailedAt(_ context.Context, _ string, _ *PushError, steps []*StepResult, offset int) error {
	r.record("failed", steps, offset)
	return nil
}
func (r *checkpointTestReporter) ReportYieldedAt(_ context.Context, _ string, _ *YieldInfo, steps []*StepResult, offset int) error {
	r.record("yielded", steps, offset)
	return nil
}
func (r *checkpointTestReporter) record(status string, steps []*StepResult, offset int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updates = append(r.updates, checkpointTestUpdate{status: status, steps: steps, offset: offset})
}

func TestJobExecutorCheckpointReplaysMemoizedSteps(t *testing.T) {
	reporter := &checkpointTestReporter{checkpoint: make(chan struct{}, 1)}
	var effects int
	fn := CreateFunction(FunctionConfig{ID: "checkpoint"}, func(ctx Context) (any, error) {
		_, err := Run(ctx, "charge", func() (string, error) {
			effects++
			return "charged", nil
		})
		if err != nil {
			return nil, err
		}
		if ctx.Run.Attempt == 1 {
			select {
			case <-reporter.checkpoint:
			case <-time.After(time.Second):
				return nil, errors.New("checkpoint timeout")
			}
			return nil, errors.New("retry")
		}
		_, err = Run(ctx, "ship", func() (string, error) {
			effects++
			return "shipped", nil
		})
		return nil, err
	})
	executor := &jobExecutor{
		functions: map[string]Function{fn.Config.ID: fn},
		logger:    NewNoopLogger(), checkpointInterval: 5 * time.Millisecond,
	}

	job := &jobAssignment{JobID: "run", RunID: "run", FunctionID: "checkpoint", Attempt: 1, StepSequenceBase: 7}
	if err := executor.execute(context.Background(), job, reporter); err != nil {
		t.Fatalf("first execution: %v", err)
	}

	reporter.mu.Lock()
	job.Attempt = 2
	job.StepSequenceBase = 8 // server sequence base advances past the checkpoint
	job.CompletedSteps = append([]completedStep(nil), reporter.completed...)
	reporter.mu.Unlock()
	if err := executor.execute(context.Background(), job, reporter); err != nil {
		t.Fatalf("restarted execution: %v", err)
	}
	if effects != 2 {
		t.Fatalf("side effects = %d, want 2 (charge once, ship once)", effects)
	}

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.updates) < 3 {
		t.Fatalf("updates = %d, want checkpoint and two terminal reports", len(reporter.updates))
	}
	if got := reporter.updates[0]; got.status != "progress" || got.offset != 7 || len(got.steps) != 1 {
		t.Fatalf("checkpoint = %#v, want progress at offset 7 with one step", got)
	}
	if got := reporter.updates[1]; got.status != "failed" || got.offset != 8 || len(got.steps) != 0 {
		t.Fatalf("first terminal update = %#v, want failed at offset 8 with empty tail", got)
	}
	last := reporter.updates[len(reporter.updates)-1]
	if last.status != "completed" || last.offset != 8 || len(last.steps) != 1 || last.steps[0].Name != "ship" {
		t.Fatalf("restart terminal update = %#v, want completed at offset 8 with ship only", last)
	}
}

func TestHTTPJobReporterIncludesCheckpointAndTerminalOffsets(t *testing.T) {
	var updates []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode report: %v", err)
		}
		updates = append(updates, body)
	}))
	defer srv.Close()

	reporter := &httpJobReporter{worker: &Worker{
		workerID: "worker", config: WorkerConfig{ServerURL: srv.URL},
		httpClient: srv.Client(), logger: NewNoopLogger(),
	}}
	ctx := context.Background()
	step := &StepResult{ID: "step-1", Name: "charge", Status: "completed"}
	if err := reporter.ReportProgress(ctx, "run", []*StepResult{step}, 4); err != nil {
		t.Fatal(err)
	}
	if err := reporter.ReportCompletedAt(ctx, "run", nil, nil, 5); err != nil {
		t.Fatal(err)
	}
	if err := reporter.ReportFailedAt(ctx, "run", &PushError{Message: "failed"}, []*StepResult{step}, 6); err != nil {
		t.Fatal(err)
	}
	if err := reporter.ReportYieldedAt(ctx, "run", &YieldInfo{Type: "sleep"}, []*StepResult{step}, 7); err != nil {
		t.Fatal(err)
	}

	want := []struct {
		status string
		offset float64
		steps  bool
	}{{"progress", 4, true}, {"completed", 5, true}, {"failed", 6, true}, {"yielded", 7, true}}
	if len(updates) != len(want) {
		t.Fatalf("got %d updates, want %d", len(updates), len(want))
	}
	for i, expected := range want {
		got := updates[i]
		if got["status"] != expected.status || got["step_offset"] != expected.offset {
			t.Errorf("update %d = %#v, want status %q and offset %v", i, got, expected.status, expected.offset)
		}
		_, hasSteps := got["steps"]
		if hasSteps != expected.steps {
			t.Errorf("update %d has steps=%v, want %v", i, hasSteps, expected.steps)
		}
	}
}
