package ironflow

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func TestWorkersRegistrationErrors(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, status := range []int{400, 422, 429, 503} {
			for _, retryable := range []bool{false, true} {
				t.Run(fmt.Sprintf("streaming=%v/status=%d/retryable=%v", streaming, status, retryable), func(t *testing.T) {
					var calls atomic.Int32
					var worker interface {
						Run(context.Context) error
						Stop()
					}
					srv := httptest.NewUnstartedServer(h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if !strings.HasSuffix(r.URL.Path, "/RegisterFunction") {
							worker.Stop()
							_, _ = w.Write([]byte(`{}`))
							return
						}
						n := calls.Add(1)
						if n == 1 {
							w.WriteHeader(status)
							_, _ = fmt.Fprintf(w, `{"message":"invalid config","retryable":%v}`, retryable)
						} else if !retryable {
							// Bound the old reconnect loop without depending on a timeout.
							w.WriteHeader(http.StatusUnauthorized)
						} else {
							_, _ = w.Write([]byte(`{}`))
						}
					}), &http2.Server{}))
					srv.Start()
					defer srv.Close()
					fn := CreateFunction(FunctionConfig{ID: "bad"}, func(Context) (any, error) { return nil, nil })
					cfg := WorkerConfig{ServerURL: srv.URL, Functions: []Function{fn}, Logger: NewNoopLogger(), ReconnectDelay: time.Millisecond}
					if streaming {
						worker = NewStreamingWorker(cfg)
					} else {
						worker = NewWorker(cfg)
					}
					defer worker.Stop()
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					err := worker.Run(ctx)
					if retryable {
						if err != nil || calls.Load() != 2 {
							t.Fatalf("want recovery after 2 registrations, got calls=%d error=%v", calls.Load(), err)
						}
					} else {
						if err == nil || !strings.Contains(err.Error(), "bad") || !strings.Contains(err.Error(), "invalid config") || IsRetryable(err) || calls.Load() != 1 {
							t.Fatalf("want one terminal registration with function and reason, got calls=%d error=%v", calls.Load(), err)
						}
					}
				})
			}
		}
	}
}

func TestStreamingWorkerReregistrationStopsOnTerminalError(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewUnstartedServer(h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !strings.HasSuffix(r.URL.Path, "/RegisterFunction") {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"code":"unavailable","message":"disconnected"}`))
			return
		}
		switch calls.Add(1) {
		case 1:
			_, _ = w.Write([]byte(`{}`))
		case 2:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"message":"invalid config"}`))
		default:
			w.WriteHeader(http.StatusUnauthorized)
		}
	}), &http2.Server{}))
	srv.Start()
	defer srv.Close()
	fn := CreateFunction(FunctionConfig{ID: "fn"}, func(Context) (any, error) { return nil, nil })
	worker := NewStreamingWorker(WorkerConfig{ServerURL: srv.URL, Functions: []Function{fn}, Logger: NewNoopLogger(), ReconnectDelay: time.Millisecond})
	defer worker.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := worker.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "fn") || !strings.Contains(err.Error(), "invalid config") || calls.Load() != 2 {
		t.Fatalf("want terminal failure on second registration, got calls=%d error=%v", calls.Load(), err)
	}
}
