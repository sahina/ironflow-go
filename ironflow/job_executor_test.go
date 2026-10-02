package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// A failed invoke row on poll makes the handler raise the child's error
// instead of yielding the invoke again, which would start a second child (#2385).
func TestJobExecutorRaisesFailedInvokeFromPoll(t *testing.T) {
	var got error
	fn := Function{
		Config: FunctionConfig{ID: "fn"},
		Handler: func(ctx Context) (any, error) {
			_, err := Invoke[any](ctx, "child", nil)
			got = err
			return nil, err
		},
	}
	exec := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
	var raw jobAssignment
	if err := json.Unmarshal([]byte(`{
		"job_id": "job-1", "run_id": "run-1", "function_id": "fn", "attempt": 1,
		"event": {"id": "e1", "name": "e", "data": {}},
		"completed_steps": [{"step_id": "run-1:child:0", "name": "run-1:child:0", "output": null,
			"status": "failed", "error": {"cause": "boom", "child_run_id": "r2"}}]
	}`), &raw); err != nil {
		t.Fatal(err)
	}
	rep := &recordingReporter{}
	if err := exec.execute(context.Background(), &raw, rep); err != nil {
		t.Fatalf("execute: %v", err)
	}
	var invErr *InvokeError
	if !errors.As(got, &invErr) || invErr.ChildRunID != "r2" {
		t.Fatalf("handler error = %v, want *InvokeError for child run r2", got)
	}
	if rep.failed == nil {
		t.Fatal("job was not reported failed")
	}
}

// runJobForEvent executes one pull job and returns the Event the handler saw.
func runJobForEvent(t *testing.T, eventJSON string) (Event, *recordingReporter) {
	t.Helper()
	var got Event
	fn := Function{
		Config: FunctionConfig{ID: "fn"},
		Handler: func(ctx Context) (any, error) {
			got = ctx.Event
			return nil, nil
		},
	}
	exec := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
	var raw jobAssignment
	if err := json.Unmarshal([]byte(`{
		"job_id": "job-1", "run_id": "run-1", "function_id": "fn", "attempt": 1,
		"event": `+eventJSON+`
	}`), &raw); err != nil {
		t.Fatal(err)
	}
	rep := &recordingReporter{}
	if err := exec.execute(context.Background(), &raw, rep); err != nil {
		t.Fatalf("execute: %v", err)
	}
	return got, rep
}

// A pull handler must see the same event fields as a push handler (#2448).
func TestJobExecutorPassesIdempotencyKeyAndSource(t *testing.T) {
	got, _ := runJobForEvent(t,
		`{"id": "e1", "name": "e", "data": {}, "idempotency_key": "idem-1", "source": "webhook"}`)
	if got.IdempotencyKey != "idem-1" {
		t.Errorf("IdempotencyKey = %q, want idem-1", got.IdempotencyKey)
	}
	if got.Source != EventSourceType("webhook") {
		t.Errorf("Source = %q, want webhook", got.Source)
	}
}

// The REST poll response has no idempotency_key, and an older engine sends no
// source. Absent keys must leave the fields empty and must not fail the job.
func TestJobExecutorToleratesMissingIdempotencyKeyAndSource(t *testing.T) {
	got, rep := runJobForEvent(t, `{"id": "e1", "name": "e", "data": {}}`)
	if rep.failed != nil {
		t.Fatalf("job reported failed: %+v", rep.failed)
	}
	if got.ID != "e1" {
		t.Fatalf("handler did not run; event ID = %q", got.ID)
	}
	if got.IdempotencyKey != "" || got.Source != "" {
		t.Errorf("IdempotencyKey = %q, Source = %q; want both empty", got.IdempotencyKey, got.Source)
	}
}

// A run's outbound calls reuse the environment the worker polls with (#2471).
func TestJobExecutorRunInfoEnvironment(t *testing.T) {
	for _, tc := range []struct{ name, config, envVar, want string }{
		{"config", "staging", "qa", "staging"},
		{"env var", "", "qa", "qa"},
		{"none", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(EnvEnvironment, tc.envVar)
			got := "unset"
			fn := Function{
				Config: FunctionConfig{ID: "fn"},
				Handler: func(ctx Context) (any, error) {
					got = ctx.Run.Environment
					return nil, nil
				},
			}
			exec := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger(), environment: tc.config}
			var raw jobAssignment
			if err := json.Unmarshal([]byte(`{"job_id":"job-1","run_id":"run-1","function_id":"fn","attempt":1,
				"event":{"id":"e1","name":"e","data":{}}}`), &raw); err != nil {
				t.Fatal(err)
			}
			if err := exec.execute(context.Background(), &raw, &recordingReporter{}); err != nil {
				t.Fatalf("execute: %v", err)
			}
			if got != tc.want {
				t.Errorf("Run.Environment = %q, want %q", got, tc.want)
			}
		})
	}
}
