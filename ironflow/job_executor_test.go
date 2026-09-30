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
