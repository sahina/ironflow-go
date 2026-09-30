package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPullWorker_YieldReportsCompletedStepTail(t *testing.T) {
	for _, tc := range []struct {
		name  string
		yield func(Context)
	}{
		{"sleep", func(ctx Context) { _ = Sleep(ctx, "pause", time.Second) }},
		{"wait_for_event", func(ctx Context) {
			_, _ = WaitForEvent[any](ctx, "pause", EventFilter{Event: "resume"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var report map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					_, _ = w.Write([]byte(`{"jobs":[{"job_id":"run-1","run_id":"run-1","function_id":"fn","attempt":1,"step_sequence_base":7,"execution_seq":2,"lease_token":"token","event":{"id":"event-1","name":"start","version":1,"data":{},"timestamp":"2026-01-01T00:00:00Z"}}]}`))
					return
				}
				if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
					t.Errorf("decode yield report: %v", err)
				}
			}))
			t.Cleanup(server.Close)

			fn := CreateFunction(FunctionConfig{ID: "fn"}, func(ctx Context) (any, error) {
				if _, err := Run(ctx, "saved", func() (string, error) { return "value", nil }); err != nil {
					return nil, err
				}
				tc.yield(ctx)
				return nil, nil
			})
			worker := NewWorker(WorkerConfig{ServerURL: server.URL, Functions: []Function{fn}, Logger: NewNoopLogger()})
			jobs, err := worker.requestJobs(context.Background(), 1)
			if err != nil || len(jobs) != 1 {
				t.Fatalf("poll: jobs=%d, err=%v", len(jobs), err)
			}
			reporter := &httpJobReporter{worker: worker, executionSeq: jobs[0].ExecutionSeq, leaseToken: jobs[0].LeaseToken}
			if err := worker.executor.execute(context.Background(), jobs[0], reporter); err != nil {
				t.Fatalf("execute: %v", err)
			}
			if report["status"] != "yielded" || report["step_offset"] != float64(7) || report["execution_seq"] != float64(2) || report["lease_token"] != "token" {
				t.Fatalf("yield report metadata = %v", report)
			}
			yield, ok := report["yield"].(map[string]any)
			if !ok || yield["type"] != tc.name {
				t.Fatalf("yield = %v, want %s", report["yield"], tc.name)
			}
			steps, ok := report["steps"].([]any)
			if !ok || len(steps) != 1 {
				t.Fatalf("steps = %v, want one completed step", report["steps"])
			}
			step, ok := steps[0].(map[string]any)
			if !ok || step["id"] != "run-1:saved:0" || step["status"] != "completed" || step["output"] != "value" {
				t.Fatalf("step = %v, want completed run-1:saved:0", steps[0])
			}
		})
	}
}

func TestPullWorker_TerminalReportsCarryStepOffset(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status string
	}{
		{"completed", nil, "completed"},
		{"failed", NewNonRetryableError("boom"), "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var report map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					_, _ = w.Write([]byte(`{"jobs":[{"job_id":"run-1","run_id":"run-1","function_id":"fn","attempt":1,"step_sequence_base":7,"event":{"id":"event-1","name":"start","version":1,"data":{},"timestamp":"2026-01-01T00:00:00Z"}}]}`))
					return
				}
				if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
					t.Errorf("decode report: %v", err)
				}
			}))
			t.Cleanup(server.Close)

			fn := CreateFunction(FunctionConfig{ID: "fn"}, func(ctx Context) (any, error) {
				if _, err := Run(ctx, "saved", func() (string, error) { return "value", nil }); err != nil {
					return nil, err
				}
				return nil, tc.err
			})
			worker := NewWorker(WorkerConfig{ServerURL: server.URL, Functions: []Function{fn}, Logger: NewNoopLogger()})
			jobs, err := worker.requestJobs(context.Background(), 1)
			if err != nil || len(jobs) != 1 {
				t.Fatalf("poll: jobs=%d, err=%v", len(jobs), err)
			}
			if err := worker.executor.execute(context.Background(), jobs[0], &httpJobReporter{worker: worker}); err != nil {
				t.Fatalf("execute: %v", err)
			}
			if report["status"] != tc.status || report["step_offset"] != float64(7) {
				t.Fatalf("report = %v, want status %s at step_offset 7", report, tc.status)
			}
		})
	}
}
