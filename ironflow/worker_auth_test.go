package ironflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// #1673: an unauthenticated worker used to log a bare 401 and reconnect every
// 5s forever. Run must stop on the first 401 and return an error that names the
// env var and the bootstrap key file.
func TestWorkerRun_StopsOn401(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "ingest-corpus",
		Triggers: []Trigger{{Event: "corpus.uploaded"}},
	}, func(ctx Context) (any, error) { return nil, nil })

	w := NewWorker(WorkerConfig{
		ServerURL:      srv.URL,
		Functions:      []Function{fn},
		Logger:         NewNoopLogger(),
		ReconnectDelay: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := w.Run(ctx)
	if err == nil {
		t.Fatal("expected Run to return an error, got nil")
	}
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
	if !strings.Contains(err.Error(), "IRONFLOW_API_KEY") {
		t.Errorf("expected the error to name IRONFLOW_API_KEY, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), ".ironflow_bootstrap_key.json") {
		t.Errorf("expected the error to name the bootstrap key file, got %q", err.Error())
	}
	// One attempt, not a reconnect loop.
	if got := calls.Load(); got != 1 {
		t.Errorf("expected 1 request, got %d — the worker is still retrying", got)
	}
}

// A 403 stops the worker the same way: the key is valid but cannot register.
func TestWorkerRun_StopsOn403(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	fn := CreateFunction(FunctionConfig{ID: "fn"}, func(ctx Context) (any, error) { return nil, nil })
	w := NewWorker(WorkerConfig{
		ServerURL:      srv.URL,
		Functions:      []Function{fn},
		Logger:         NewNoopLogger(),
		ReconnectDelay: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := w.Run(ctx)
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected ErrForbidden, got %v", err)
	}
}

// The projection runner has the same loop: a 401 must stop it, not poll forever.
func TestProjectionRunnerStart_StopsOn401(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	proj := CreateProjection(ProjectionConfig{
		Name:   "order-totals",
		Events: []string{"order.created"},
		Mode:   ProjectionModeManaged,
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			return state, nil
		},
	})

	r := NewProjectionRunner(proj, srv.URL, nil, NewNoopLogger())
	defer r.Stop()

	err := r.Start()
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized from Start, got %v", err)
	}
	if !strings.Contains(err.Error(), "IRONFLOW_API_KEY") {
		t.Errorf("expected the error to name IRONFLOW_API_KEY, got %q", err.Error())
	}
}

// The streaming worker registers functions over plain HTTP before opening the
// stream, so it hits the same 401 and must stop the same way. Needs an h2c
// server — the streaming worker's HTTP client is HTTP/2-only.
func TestStreamingWorkerRun_StopsOn401(t *testing.T) {
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	})
	srv := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	srv.Start()
	defer srv.Close()

	fn := CreateFunction(FunctionConfig{ID: "fn"}, func(ctx Context) (any, error) { return nil, nil })
	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      srv.URL,
		Functions:      []Function{fn},
		Logger:         NewNoopLogger(),
		ReconnectDelay: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := w.Run(ctx)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("expected 1 request, got %d — the streaming worker is still retrying", got)
	}
}

// Key revoked mid-run: registration succeeds, then the job poll 401s. The
// worker must stop rather than poll a 401 every 5s (#1673).
func TestWorkerRun_StopsOn401DuringPoll(t *testing.T) {
	var polls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/ironflow.v1.IronflowService/RegisterFunction", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("/api/v1/workers/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/jobs") {
			polls.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	fn := CreateFunction(FunctionConfig{ID: "fn"}, func(ctx Context) (any, error) { return nil, nil })
	w := NewWorker(WorkerConfig{
		ServerURL:      srv.URL,
		Functions:      []Function{fn},
		Logger:         NewNoopLogger(),
		ReconnectDelay: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := w.Run(ctx)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
	if got := polls.Load(); got != 1 {
		t.Errorf("expected 1 job poll, got %d — the worker is still polling", got)
	}
}
