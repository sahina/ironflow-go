package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerHeaders_Environment(t *testing.T) {
	cases := []struct {
		name      string
		config    string
		envVar    string
		want      string
		wantUnset bool
	}{
		{name: "config value", config: "staging", want: "staging"},
		{name: "env var fallback", envVar: "qa", want: "qa"},
		{name: "config wins over env var", config: "staging", envVar: "qa", want: "staging"},
		{name: "neither set sends no header", wantUnset: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(EnvEnvironment, tc.envVar)
			cfg := WorkerConfig{ServerURL: "http://example.com", Logger: NewNoopLogger(), Environment: tc.config}

			for kind, headers := range map[string]map[string]string{
				"polling":   NewWorker(cfg).getHeaders(),
				"streaming": NewStreamingWorker(cfg).getHeaders(),
			} {
				got, present := headers[HeaderEnvironment]
				if tc.wantUnset {
					if present {
						t.Errorf("%s: header must be absent, got %q", kind, got)
					}
					continue
				}
				if got != tc.want {
					t.Errorf("%s: %s = %q, want %q", kind, HeaderEnvironment, got, tc.want)
				}
			}
		})
	}
}

func TestWorkerConfig_CheckpointIntervalReachesExecutor(t *testing.T) {
	cfg := WorkerConfig{ServerURL: "http://example.com", Logger: NewNoopLogger(), CheckpointInterval: 5 * time.Second}
	if got := NewWorker(cfg).executor.checkpointInterval; got != 5*time.Second {
		t.Errorf("polling executor checkpointInterval = %v, want 5s", got)
	}
}

func TestDrainTimeoutDefaults(t *testing.T) {
	for name, configured := range map[string]time.Duration{"zero": 0, "negative": -time.Second} {
		t.Run(name, func(t *testing.T) {
			if got := drainTimeout(configured); got != workerDrainTimeout {
				t.Errorf("drainTimeout(%v) = %v, want the default %v", configured, got, workerDrainTimeout)
			}
		})
	}
	if got := drainTimeout(2 * time.Minute); got != 2*time.Minute {
		t.Errorf("drainTimeout(2m) = %v, want 2m", got)
	}
}

func TestWorker_Drain_UsesConfiguredTimeout(t *testing.T) {
	w := NewWorker(WorkerConfig{ServerURL: "http://example.com", Logger: NewNoopLogger(), DrainTimeout: 10 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	var cancelled atomic.Bool
	w.activeJobs.Store("job-1", &activeJob{cancel: func() { cancelled.Store(true); cancel() }})
	w.jobCount.Store(1)

	start := time.Now()
	w.Drain()

	// The default is 30s; a drain this fast proves the configured value was used.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Drain took %v; the configured 10ms timeout was not used", elapsed)
	}
	if !cancelled.Load() || ctx.Err() == nil {
		t.Fatal("deadline did not cancel the active job")
	}
}

// A durable Publish made inside a job must reach the worker's environment.
// Without the header it lands in env_default when the API key is not bound to
// an environment.
func TestJobExecutor_PublishCarriesEnvironment(t *testing.T) {
	cases := []struct {
		name      string
		config    string
		envVar    string
		want      string
		wantUnset bool
	}{
		{name: "config value", config: "staging", want: "staging"},
		{name: "env var fallback", envVar: "qa", want: "qa"},
		{name: "config wins over env var", config: "staging", envVar: "qa", want: "staging"},
		{name: "neither set sends no header", wantUnset: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(EnvEnvironment, tc.envVar)
			var got []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Values(HeaderEnvironment)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"eventId": "evt_abc", "sequence": "1"})
			}))
			defer srv.Close()

			fn := Function{
				Config: FunctionConfig{ID: "fn"},
				Handler: func(ctx Context) (any, error) {
					return nil, Publish(ctx, "order.processed", map[string]any{"id": 1})
				},
			}
			exec := &jobExecutor{
				functions:   map[string]Function{"fn": fn},
				logger:      NewNoopLogger(),
				serverURL:   srv.URL,
				environment: tc.config,
			}
			var job jobAssignment
			if err := json.Unmarshal([]byte(`{
				"job_id": "job-1", "run_id": "run-1", "function_id": "fn", "attempt": 1,
				"event": {"id": "e1", "name": "e", "data": {}}
			}`), &job); err != nil {
				t.Fatal(err)
			}
			rep := &recordingReporter{}
			if err := exec.execute(context.Background(), &job, rep); err != nil {
				t.Fatalf("execute: %v", err)
			}
			if rep.failed != nil {
				t.Fatalf("job reported failed: %+v", rep.failed)
			}
			if tc.wantUnset {
				if len(got) != 0 {
					t.Fatalf("header must be absent, got %v", got)
				}
				return
			}
			if len(got) != 1 || got[0] != tc.want {
				t.Fatalf("%s = %v, want [%s]", HeaderEnvironment, got, tc.want)
			}
		})
	}
}

// Both worker types must hand their environment to the executor, or the
// header above never has a value to carry.
func TestWorkerConfig_EnvironmentReachesExecutor(t *testing.T) {
	cfg := WorkerConfig{ServerURL: "http://example.com", Logger: NewNoopLogger(), Environment: "staging"}
	if got := NewWorker(cfg).executor.environment; got != "staging" {
		t.Errorf("polling executor environment = %q, want staging", got)
	}
	if got := NewStreamingWorker(cfg).executor.environment; got != "staging" {
		t.Errorf("streaming executor environment = %q, want staging", got)
	}
}
