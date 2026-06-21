package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Chunk 2 (#1206 T9): REST worker SDK fence-echo. The worker polls with
// ?available=N, parses the capacity batched response, acknowledges fenced
// assignments before executing, and echoes the execution fence on every update.

func newTestWorker(serverURL string) *Worker {
	fn := CreateFunction(FunctionConfig{ID: "fn", Triggers: []Trigger{{Event: "e"}}},
		func(ctx Context) (any, error) { return nil, nil })
	return NewWorker(WorkerConfig{ServerURL: serverURL, Functions: []Function{fn}, Logger: NewNoopLogger(), MaxConcurrentJobs: 4})
}

func TestWorker_RequestJobs_BatchWithFence(t *testing.T) {
	var gotAvailable string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAvailable = r.URL.Query().Get("available")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jobs":[
			{"job_id":"r0","run_id":"r0","function_id":"fn","execution_seq":7,"lease_token":"tok-0"},
			{"job_id":"r1","run_id":"r1","function_id":"fn","execution_seq":9,"lease_token":"tok-1"}
		]}`))
	}))
	defer server.Close()

	jobs, err := newTestWorker(server.URL).requestJobs(context.Background(), 3)
	if err != nil {
		t.Fatalf("requestJobs: %v", err)
	}
	if gotAvailable != "3" {
		t.Fatalf("available query = %q, want 3", gotAvailable)
	}
	if len(jobs) != 2 {
		t.Fatalf("got %d jobs, want 2", len(jobs))
	}
	if jobs[0].JobID != "r0" || jobs[0].ExecutionSeq != 7 || jobs[0].LeaseToken != "tok-0" {
		t.Fatalf("job 0 fence not decoded: %+v", jobs[0])
	}
	if jobs[1].ExecutionSeq != 9 || jobs[1].LeaseToken != "tok-1" {
		t.Fatalf("job 1 fence not decoded: %+v", jobs[1])
	}
}

func TestWorker_RequestJobs_LegacySingleNoFence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job_id":"j1","run_id":"j1","function_id":"fn"}`))
	}))
	defer server.Close()

	jobs, err := newTestWorker(server.URL).requestJobs(context.Background(), 1)
	if err != nil {
		t.Fatalf("requestJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1 (legacy single)", len(jobs))
	}
	if jobs[0].LeaseToken != "" || jobs[0].ExecutionSeq != 0 {
		t.Fatalf("legacy job must carry no fence: %+v", jobs[0])
	}
}

func TestWorker_AckJob_SendsFence(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	w := newTestWorker(server.URL)
	job := &jobAssignment{JobID: "run-x", RunID: "run-x", ExecutionSeq: 42, LeaseToken: "tok-x"}
	if err := w.ackJob(context.Background(), job); err != nil {
		t.Fatalf("ackJob: %v", err)
	}
	if gotPath != "/api/v1/workers/"+w.workerID+"/jobs/run-x/ack" {
		t.Fatalf("ack path = %q", gotPath)
	}
	if gotBody["run_id"] != "run-x" || gotBody["lease_token"] != "tok-x" {
		t.Fatalf("ack body fence wrong: %+v", gotBody)
	}
	if seq, _ := strconv.Atoi(string(mustJSONNumber(t, gotBody["execution_seq"]))); seq != 42 {
		t.Fatalf("ack execution_seq = %v, want 42", gotBody["execution_seq"])
	}
}

func TestWorker_AckJob_StaleReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"STALE_EXECUTION","message":"recovered"}`))
	}))
	defer server.Close()

	w := newTestWorker(server.URL)
	job := &jobAssignment{JobID: "r", RunID: "r", ExecutionSeq: 1, LeaseToken: "tok"}
	if err := w.ackJob(context.Background(), job); err == nil {
		t.Fatal("expected error on 409 stale ack")
	}
}

func TestWorker_ReportCompleted_EchoesFenceWhenCapacity(t *testing.T) {
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	w := newTestWorker(server.URL)
	rep := &httpJobReporter{worker: w, executionSeq: 11, leaseToken: "tok-c"}
	if err := rep.ReportCompleted(context.Background(), "job-1", map[string]string{"ok": "1"}, nil); err != nil {
		t.Fatalf("ReportCompleted: %v", err)
	}
	if gotBody["status"] != "completed" {
		t.Fatalf("status = %v, want completed", gotBody["status"])
	}
	if gotBody["lease_token"] != "tok-c" {
		t.Fatalf("completion did not echo lease_token: %+v", gotBody)
	}
	if string(mustJSONNumber(t, gotBody["execution_seq"])) != "11" {
		t.Fatalf("completion execution_seq = %v, want 11", gotBody["execution_seq"])
	}
}

func TestWorker_ReportCompleted_NoFenceForLegacy(t *testing.T) {
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	w := newTestWorker(server.URL)
	rep := &httpJobReporter{worker: w} // no fence (legacy)
	if err := rep.ReportCompleted(context.Background(), "job-1", nil, nil); err != nil {
		t.Fatalf("ReportCompleted: %v", err)
	}
	if _, ok := gotBody["lease_token"]; ok {
		t.Fatalf("legacy completion must NOT carry a fence: %+v", gotBody)
	}
	if _, ok := gotBody["execution_seq"]; ok {
		t.Fatalf("legacy completion must NOT carry execution_seq: %+v", gotBody)
	}
}

// TestWorker_ProcessJob_AcksThenExecutes drives the real processJob path: a fenced
// assignment is acked before the handler runs, and the completion echoes the fence.
func TestWorker_ProcessJob_AcksThenExecutes(t *testing.T) {
	var mu sync.Mutex
	acked := false
	var completeBody map[string]any
	done := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPut && len(r.URL.Path) > 4 && r.URL.Path[len(r.URL.Path)-4:] == "/ack":
			acked = true
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut: // terminal update
			_ = json.NewDecoder(r.Body).Decode(&completeBody)
			w.WriteHeader(http.StatusOK)
			close(done)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	executed := make(chan struct{}, 1)
	fn := CreateFunction(FunctionConfig{ID: "fn", Triggers: []Trigger{{Event: "e"}}},
		func(ctx Context) (any, error) { executed <- struct{}{}; return map[string]string{"r": "ok"}, nil })
	w := NewWorker(WorkerConfig{ServerURL: server.URL, Functions: []Function{fn}, Logger: NewNoopLogger(), MaxConcurrentJobs: 4})

	job := &jobAssignment{
		JobID: "run-1", RunID: "run-1", FunctionID: "fn", ExecutionSeq: 5, LeaseToken: "tok-1",
		Event: jobEvent{ID: "e1", Name: "e", Data: json.RawMessage(`{}`)},
	}
	w.processJob(context.Background(), job)

	<-executed
	select {
	case <-done:
	case <-context.Background().Done():
	}
	mu.Lock()
	defer mu.Unlock()
	if !acked {
		t.Fatal("fenced job was not acked before completion")
	}
	if completeBody["lease_token"] != "tok-1" {
		t.Fatalf("completion did not echo the fence: %+v", completeBody)
	}
}

// TestWorker_ProcessJob_DropsOnStaleAck: a stale (409) ack means the segment was
// recovered/superseded — the worker must NOT execute the handler and must NOT send
// any terminal update; the slot is freed.
func TestWorker_ProcessJob_DropsOnStaleAck(t *testing.T) {
	var mu sync.Mutex
	terminalSent := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/ack"):
			w.WriteHeader(http.StatusConflict) // stale fence
			_, _ = w.Write([]byte(`{"error":"STALE_EXECUTION"}`))
		case r.Method == http.MethodPut: // terminal update — must NOT happen
			mu.Lock()
			terminalSent = true
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	executed := make(chan struct{}, 1)
	fn := CreateFunction(FunctionConfig{ID: "fn", Triggers: []Trigger{{Event: "e"}}},
		func(_ Context) (any, error) { executed <- struct{}{}; return nil, nil })
	w := NewWorker(WorkerConfig{ServerURL: server.URL, Functions: []Function{fn}, Logger: NewNoopLogger(), MaxConcurrentJobs: 4})

	job := &jobAssignment{
		JobID: "r", RunID: "r", FunctionID: "fn", ExecutionSeq: 1, LeaseToken: "tok",
		Event: jobEvent{ID: "e1", Name: "e", Data: json.RawMessage(`{}`)},
	}
	w.processJob(context.Background(), job)

	select {
	case <-executed:
		t.Fatal("handler ran despite a stale ack — the job must be dropped")
	case <-time.After(500 * time.Millisecond):
	}
	mu.Lock()
	defer mu.Unlock()
	if terminalSent {
		t.Fatal("a terminal update was sent for a stale-acked job — it must be dropped silently")
	}
	if w.jobCount.Load() != 0 {
		t.Fatalf("jobCount = %d after a dropped job, want 0 (slot must be freed)", w.jobCount.Load())
	}
}

func mustJSONNumber(t *testing.T, v any) json.Number {
	t.Helper()
	switch n := v.(type) {
	case json.Number:
		return n
	case float64:
		return json.Number(strconv.FormatFloat(n, 'f', -1, 64))
	default:
		t.Fatalf("value %v (%T) is not a JSON number", v, v)
		return ""
	}
}
