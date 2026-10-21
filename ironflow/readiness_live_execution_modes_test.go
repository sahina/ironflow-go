package ironflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

// TestLiveReadinessExecutionModes covers the execution-modes group (#3003).
func TestLiveReadinessExecutionModes(t *testing.T) {
	client, ctx := liveReadiness(t, &struct{}{})
	suffix := time.Now().UnixNano()

	// The engine calls the handler Serve returns, signs nothing (the server has no
	// signing key) and resumes after the sleep by calling it again, so a step that
	// runs once across both calls shows the memoized resume.
	t.Run("exec-push-serve", func(t *testing.T) {
		id := fmt.Sprintf("readiness-push-go-%d", suffix)
		var pushes, a, b atomic.Int32
		fn := CreateFunction(FunctionConfig{
			ID: id, Triggers: []Trigger{{Event: "readiness.push." + id}}, Mode: PushMode,
		}, func(c Context) (any, error) {
			pushes.Add(1)
			x, err := Run(c, "a", func() (int, error) { a.Add(1); return 1, nil })
			if err != nil {
				return nil, err
			}
			if err := Sleep(c, "nap", time.Second); err != nil {
				return nil, err
			}
			y, err := Run(c, "b", func() (int, error) { b.Add(1); return 2, nil })
			return map[string]any{"a": x, "b": y}, err
		})
		srv := httptest.NewServer(Serve(ServeConfig{Functions: []Function{fn}}))
		t.Cleanup(srv.Close)
		fn.Config.EndpointURL = srv.URL
		if err := client.RegisterFunction(ctx, fn); err != nil {
			t.Fatalf("RegisterFunction: %v", err)
		}
		t.Cleanup(func() { _ = client.DeleteFunction(ctx, id) })

		// Push-protocol.md: any method but POST gets 405.
		resp, err := http.Get(srv.URL) // #nosec G107 -- a local test server
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("GET status = %d, want 405", resp.StatusCode)
		}

		emitted, err := client.Emit(ctx, "readiness.push."+id, map[string]any{})
		if err != nil || len(emitted.RunIDs) != 1 {
			t.Fatalf("Emit = %+v, %v, want one run", emitted, err)
		}
		deadline := time.Now().Add(25 * time.Second)
		var run *WorkflowRun
		for {
			if run, err = client.GetRun(ctx, emitted.RunIDs[0]); err != nil {
				t.Fatalf("GetRun: %v", err)
			}
			if run.Status == RunStatusCompleted || run.Status == RunStatusFailed || time.Now().After(deadline) {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if run.Status != RunStatusCompleted || !reflect.DeepEqual(run.Output, map[string]any{"a": float64(1), "b": float64(2)}) {
			t.Fatalf("run = %+v, want completed with a=1, b=2", run)
		}
		if pushes.Load() < 2 || a.Load() != 1 || b.Load() != 1 {
			t.Fatalf("pushes = %d, step a ran %d, step b ran %d, want 2+ pushes and each step once", pushes.Load(), a.Load(), b.Load())
		}
	})

	t.Run("exec-workers-list", func(t *testing.T) {
		workerID := fmt.Sprintf("readiness-worker-go-%d", suffix)
		body, _ := json.Marshal(map[string]any{
			"hostname": "readiness-host", "function_ids": []string{"readiness-worker-fn-" + workerID},
			"max_concurrent_jobs": 3, "labels": map[string]string{"pool": "readiness"},
			"version": map[string]string{"sdk": "go", "runtime": "go-test"},
		})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, os.Getenv("IRONFLOW_TEST_SERVER")+"/api/v1/workers/"+workerID+"/register", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+os.Getenv("IRONFLOW_TEST_API_KEY"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("register worker = %v, %v", resp, err)
		}
		_ = resp.Body.Close()

		workers, err := client.ListWorkers(ctx)
		if err != nil {
			t.Fatalf("ListWorkers: %v", err)
		}
		for _, w := range workers {
			if w.ID != workerID {
				continue
			}
			if w.Version != (WorkerVersion{SDK: "go", Runtime: "go-test"}) {
				t.Fatalf("worker version = %+v, want {go go-test}", w.Version)
			}
			if w.Hostname != "readiness-host" || !reflect.DeepEqual(w.FunctionIDs, []string{"readiness-worker-fn-" + workerID}) ||
				w.MaxConcurrent != 3 || !reflect.DeepEqual(w.Labels, map[string]string{"pool": "readiness"}) ||
				w.ActiveJobs != 0 || w.Transport != "rest-polling" {
				t.Fatalf("worker = %+v, want the registered fields on rest-polling with no jobs", w)
			}
			for _, ts := range []string{w.RegisteredAt, w.LastHeartbeat} {
				if _, err := time.Parse(time.RFC3339, ts); err != nil {
					t.Fatalf("worker time %q: %v", ts, err)
				}
			}
			return
		}
		t.Fatalf("ListWorkers has no %s among %d workers", workerID, len(workers))
	})
}
