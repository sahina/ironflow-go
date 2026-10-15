package ironflow

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// A result JSON cannot encode is reported as a failed job, not dropped while
// the lease expires into a full re-run (#2873).
func TestPullWorker_UnencodableResultFailsJob(t *testing.T) {
	var mu sync.Mutex
	var puts []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		puts = append(puts, r.Method+" "+r.URL.Path+" "+string(b))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	w := newTestWorker(server.URL)
	fn := Function{
		Config: FunctionConfig{ID: "fn"},
		Handler: func(ctx Context) (any, error) {
			return struct{ F float64 }{math.NaN()}, nil
		},
	}
	exec := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
	var job jobAssignment
	if err := json.Unmarshal([]byte(`{"job_id":"job-1","run_id":"run-1","function_id":"fn","attempt":1,"event":{"id":"e1","name":"e","data":{}}}`), &job); err != nil {
		t.Fatal(err)
	}
	rep := &httpJobReporter{worker: w}
	_ = exec.execute(context.Background(), &job, rep)
	mu.Lock()
	defer mu.Unlock()
	for _, p := range puts {
		if strings.Contains(p, `"status":"failed"`) && strings.Contains(p, `"SERIALIZATION_ERROR"`) && strings.Contains(p, `"retryable":false`) {
			return
		}
	}
	t.Fatalf("no failed PUT arrived; got %d requests", len(puts))
}

// A step output JSON cannot encode, followed by a yield, fails the job rather
// than yielding: the resumed run would otherwise replay a stored null.
func TestPullWorker_UnencodableStepBeforeYieldFailsJob(t *testing.T) {
	rep := &recordingReporter{}
	fn := Function{Config: FunctionConfig{ID: "fn"}, Handler: func(ctx Context) (any, error) {
		if _, err := Run(ctx, "bad", func() (float64, error) { return math.Inf(1), nil }); err != nil {
			return nil, err
		}
		return nil, Sleep(ctx, "nap", time.Hour)
	}}
	exec := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
	job := jobAssignment{JobID: "job-1", RunID: "run-1", FunctionID: "fn", Attempt: 1}
	if err := exec.execute(context.Background(), &job, rep); err != nil {
		t.Fatal(err)
	}
	if rep.yielded != nil || rep.failed == nil || rep.failed.Code != "SERIALIZATION_ERROR" || rep.failed.Retryable {
		t.Fatalf("yielded=%v failed=%+v, want a non-retryable SERIALIZATION_ERROR failure", rep.yielded, rep.failed)
	}
}

// The serialization failure runs compensations, like any terminal failure.
func TestPullWorker_UnencodableResultRunsCompensations(t *testing.T) {
	compensated := false
	fn := Function{Config: FunctionConfig{ID: "fn"}, Handler: func(ctx Context) (any, error) {
		if _, err := Run(ctx, "charge", func() (int, error) { return 1, nil }); err != nil {
			return nil, err
		}
		Compensate(ctx, "charge", func() error { compensated = true; return nil })
		return math.NaN(), nil
	}}
	exec := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
	job := jobAssignment{JobID: "job-1", RunID: "run-1", FunctionID: "fn", Attempt: 1}
	rep := &recordingReporter{}
	if err := exec.execute(context.Background(), &job, rep); err != nil {
		t.Fatal(err)
	}
	if !compensated || rep.failed == nil || rep.failed.Code != "SERIALIZATION_ERROR" {
		t.Fatalf("compensated=%v failed=%+v", compensated, rep.failed)
	}
}
