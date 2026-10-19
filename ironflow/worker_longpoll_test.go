package ironflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// ADR 0124: the REST worker asks the server to hold an idle poll and polls
// again at once only when the server says it held it.

// pollServer answers every poll with 204 and the given Ironflow-Poll-Wait
// header ("" omits it), counting polls and recording the last wait param.
func pollServer(t *testing.T, header string, onPoll func(n int32)) (*httptest.Server, *atomic.Int32, *atomic.Value) {
	t.Helper()
	var n atomic.Int32
	var wait atomic.Value
	wait.Store("")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wait.Store(r.URL.RawQuery)
		if header != "" {
			w.Header().Set("Ironflow-Poll-Wait", header)
		}
		w.WriteHeader(http.StatusNoContent)
		if onPoll != nil {
			onPoll(n.Add(1))
		} else {
			n.Add(1)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &n, &wait
}

func connectedWorker(url string, pollWait time.Duration) *Worker {
	w := newTestWorker(url)
	w.config.PollWait = pollWait
	w.state.Store(int32(stateConnected))
	return w
}

func TestWorker_Poll_HeldHeaderPollsAgainAtOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv, n, _ := pollServer(t, "20", func(n int32) {
		if n == 3 {
			cancel()
		}
	})

	start := time.Now()
	err := connectedWorker(srv.URL, DefaultWorkerPollWait).pollForJobs(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pollForJobs = %v, want context.Canceled", err)
	}
	if n.Load() < 3 || time.Since(start) > 900*time.Millisecond {
		t.Fatalf("%d polls in %v, want 3 with no idle wait", n.Load(), time.Since(start))
	}
}

func TestWorker_Poll_NotHeldKeepsIdleWait(t *testing.T) {
	for name, header := range map[string]string{"absent": "", "zero": "0"} {
		t.Run(name, func(t *testing.T) {
			srv, n, _ := pollServer(t, header, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()

			_ = connectedWorker(srv.URL, DefaultWorkerPollWait).pollForJobs(ctx)
			if n.Load() != 1 {
				t.Fatalf("%d polls in 300ms, want 1 (1s idle wait kept)", n.Load())
			}
		})
	}
}

func TestWorker_Poll_WaitParam(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pollWait time.Duration
		want     string
	}{
		{"default", 0, "available=1&wait=20"},
		{"capped", 40 * time.Second, "available=1&wait=25"},
		{"custom", 7 * time.Second, "available=1&wait=7"},
		{"disabled", -1, "available=1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, got := pollServer(t, "", nil)
			w := NewWorker(WorkerConfig{ServerURL: srv.URL, Logger: NewNoopLogger(), PollWait: tc.pollWait})
			if _, _, err := w.requestJobs(context.Background(), 1); err != nil {
				t.Fatal(err)
			}
			if got.Load() != tc.want {
				t.Fatalf("query = %q, want %q", got.Load(), tc.want)
			}
		})
	}
}

func TestWorker_Poll_CancelEndsHeldPollQuietly(t *testing.T) {
	held := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(held)
		<-r.Context().Done()
	}))
	defer srv.Close()
	logger := &captureLogger{}
	w := connectedWorker(srv.URL, DefaultWorkerPollWait)
	w.logger = logger

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.pollForJobs(ctx) }()
	<-held
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pollForJobs = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("held poll not cancelled promptly")
	}
	if got := logger.warnings(); len(got) != 0 {
		t.Fatalf("cancellation logged: %v", got)
	}
}
