package ironflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A worker the server forgot (restart, another node) registers again instead
// of polling a 404 forever (#2873). The fake server forgets the worker, so
// poll and heartbeat 404 until it registers again.
func TestWorker_ReregistersWhenServerForgetsIt(t *testing.T) {
	var registers, polls, polls404, hb404 atomic.Int32
	var registered atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case strings.HasSuffix(p, "/register"):
			registers.Add(1)
			registered.Store(true)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case strings.HasSuffix(p, "/heartbeat"):
			if !registered.Load() {
				hb404.Add(1)
				http.Error(w, `{"error":"worker not registered"}`, http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case strings.HasSuffix(p, "/jobs"):
			n := polls.Add(1)
			if n == 3 {
				registered.Store(false) // "server restart"
			}
			if !registered.Load() {
				polls404.Add(1)
				http.Error(w, `{"error":"worker not registered"}`, http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default: // function registration etc.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()

	fn := CreateFunction(FunctionConfig{ID: "f", Triggers: []Trigger{{Event: "e"}}},
		func(ctx Context) (any, error) { return nil, nil })
	w := NewWorker(WorkerConfig{
		ServerURL:         srv.URL,
		Functions:         []Function{fn},
		HeartbeatInterval: 500 * time.Millisecond,
		Logger:            NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && registers.Load() < 2 {
		time.Sleep(100 * time.Millisecond)
	}
	got := registers.Load()
	t.Logf("registers=%d polls=%d polls404=%d heartbeat404=%d", got, polls.Load(), polls404.Load(), hb404.Load())
	cancel()
	w.Stop()
	<-done
	if got < 2 {
		t.Fatalf("worker did not re-register within 15s of server forgetting it: registers=%d", got)
	}
}
