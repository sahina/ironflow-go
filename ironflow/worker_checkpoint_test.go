package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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

// Cancellation must reach step bodies, not just their outbound HTTP requests.
func TestPollingWorkerAbandonsStaleCheckpoint(t *testing.T) {
	for _, branch := range []bool{false, true} {
		t.Run(fmt.Sprintf("branch=%v", branch), func(t *testing.T) {
			progress := make(chan struct{}, 1)
			release := make(chan struct{})
			defer close(release)
			var effects, reports, compensations atomic.Int32
			var heartbeatJobs atomic.Int32
			heartbeatJobs.Store(1)
			w := newTestWorker("http://localhost")
			w.executor.checkpointInterval = time.Millisecond
			w.httpClient.Transport = &mockRoundTripper{roundTripFunc: func(req *http.Request) (*http.Response, error) {
				var body struct {
					Status string `json:"status"`
					Jobs   []any  `json:"jobs"`
				}
				_ = json.NewDecoder(req.Body).Decode(&body)
				if strings.HasSuffix(req.URL.Path, "/heartbeat") {
					heartbeatJobs.Store(int32(len(body.Jobs)))
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
				}
				if body.Status == "progress" {
					select {
					case progress <- struct{}{}:
					default:
					}
					return &http.Response{StatusCode: 409, Body: io.NopCloser(strings.NewReader(`{"error":"STALE_EXECUTION"}`))}, nil
				}
				reports.Add(1)
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			}}
			w.executor.functions["fn"] = CreateFunction(FunctionConfig{ID: "fn"}, func(ctx Context) (any, error) {
				_, _ = Run(ctx, "first", func() (int, error) { return 1, nil })
				Compensate(ctx, "first", func() error { compensations.Add(1); return nil })
				<-release
				step := func() (int, error) { effects.Add(1); return 2, nil }
				if branch {
					return Parallel(ctx, "parallel", []func(*BranchContext) (int, error){func(b *BranchContext) (int, error) {
						return RunWithBranch(b, "next", step)
					}})
				}
				return Run(ctx, "next", step)
			})
			w.processJob(context.Background(), &jobAssignment{JobID: "run", RunID: "run", FunctionID: "fn"})
			select {
			case <-progress:
			case <-time.After(time.Second):
				t.Fatal("no checkpoint")
			}
			deadline := time.Now().Add(time.Second)
			for heartbeatJobs.Load() != 0 && time.Now().Before(deadline) {
				w.sendHeartbeat(context.Background())
				time.Sleep(time.Millisecond)
			}
			if heartbeatJobs.Load() != 0 {
				t.Fatal("stale execution remained in heartbeat")
			}
			// Release only after cancellation. The next step body must never run.
			release <- struct{}{}
			deadline = time.Now().Add(time.Second)
			for w.jobCount.Load() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if w.jobCount.Load() != 0 {
				t.Fatal("job did not settle")
			}
			if effects.Load() != 0 || reports.Load() != 0 || compensations.Load() != 0 {
				t.Fatalf("effects=%d reports=%d compensations=%d", effects.Load(), reports.Load(), compensations.Load())
			}
		})
	}
}

func TestPollingWorkerCheckpointFinishRace(t *testing.T) {
	for _, tc := range []struct {
		status      int
		body        string
		outcome     string
		wantReports int32
	}{
		{409, `{"error":"STALE_EXECUTION"}`, "completed", 0},
		{409, `{"error":"STALE_EXECUTION"}`, "failed", 0},
		{409, `{"error":"STALE_EXECUTION"}`, "yielded", 0},
		{409, `{"error":"RUN_NOT_RUNNING"}`, "completed", 1},
		{409, `not json`, "completed", 1},
		{500, `{"error":"STALE_EXECUTION"}`, "completed", 1},
	} {
		t.Run(fmt.Sprintf("%d/%s/%s", tc.status, tc.body, tc.outcome), func(t *testing.T) {
			checkpoint := make(chan struct{})
			checkpointStarted := sync.OnceFunc(func() { close(checkpoint) })
			release := make(chan struct{})
			releaseResponse := sync.OnceFunc(func() { close(release) })
			defer releaseResponse()
			handlerDone := make(chan struct{})
			var reports atomic.Int32
			w := newTestWorker("http://localhost")
			w.executor.checkpointInterval = time.Millisecond
			w.httpClient.Transport = &mockRoundTripper{roundTripFunc: func(req *http.Request) (*http.Response, error) {
				var body struct {
					Status string `json:"status"`
				}
				_ = json.NewDecoder(req.Body).Decode(&body)
				if body.Status == "progress" {
					checkpointStarted()
					<-release
					return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
				}
				reports.Add(1)
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			}}
			w.executor.functions["fn"] = CreateFunction(FunctionConfig{ID: "fn"}, func(ctx Context) (any, error) {
				_, _ = Run(ctx, "first", func() (int, error) { return 1, nil })
				<-checkpoint
				defer close(handlerDone)
				switch tc.outcome {
				case "failed":
					return nil, NewNonRetryableError("failure")
				case "yielded":
					return nil, Sleep(ctx, "sleep", time.Hour)
				default:
					return "done", nil
				}
			})
			w.processJob(context.Background(), &jobAssignment{JobID: "run", RunID: "run", FunctionID: "fn"})
			select {
			case <-handlerDone:
			case <-time.After(time.Second):
				t.Fatal("handler did not finish")
			}
			releaseResponse()
			deadline := time.Now().Add(time.Second)
			for w.jobCount.Load() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if w.jobCount.Load() != 0 {
				t.Fatal("job did not settle")
			}
			if reports.Load() != tc.wantReports {
				t.Fatalf("reports=%d, want %d", reports.Load(), tc.wantReports)
			}
		})
	}
}

func TestPollingExecutorDoesNotStartCanceledHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var ran bool
	fn := CreateFunction(FunctionConfig{ID: "fn"}, func(Context) (any, error) { ran = true; return nil, nil })
	executor := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
	if err := executor.execute(ctx, &jobAssignment{FunctionID: "fn"}, &checkpointTestReporter{}); err != nil {
		t.Fatal(err)
	}
	if ran {
		t.Fatal("canceled job started the handler")
	}
}

func TestCompensationsStopOnJobCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var effects int
	exec := &executionContext{ctx: ctx, stepCounters: make(map[string]int), compensations: []compensationEntry{
		{stepName: "later", fn: func() error { effects++; return nil }},
		{stepName: "in-flight", fn: func() error { cancel(); return nil }},
	}}
	exec.executeCompensations()
	if effects != 0 {
		t.Fatal("started another compensation after cancellation")
	}
}

func TestHTTPJobReporterStaleFinalUpdate(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		stale  bool
	}{
		{409, `{"error":"STALE_EXECUTION"}`, true},
		{409, `{"error":"RUN_NOT_RUNNING"}`, false},
		{409, `not json`, false},
		{500, `{"error":"STALE_EXECUTION"}`, false},
	} {
		t.Run(fmt.Sprintf("%d/%s", tc.status, tc.body), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w := newTestWorker("http://localhost")
			w.httpClient.Transport = &mockRoundTripper{roundTripFunc: func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			}}
			reporter := &httpJobReporter{worker: w, cancel: cancel}
			err := reporter.ReportCompletedAt(ctx, "run", "done", nil, 0)
			if tc.stale {
				if err != nil || ctx.Err() != context.Canceled {
					t.Fatalf("stale final update: err=%v cancellation=%v", err, ctx.Err())
				}
			} else if err == nil || ctx.Err() != nil {
				t.Fatalf("other rejection: err=%v cancellation=%v", err, ctx.Err())
			}
		})
	}
}
