package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

// Finishing execution 1 must not remove execution 2 from the heartbeat, capacity,
// or cancellation registry, even though both assignments have the same job ID.
func TestWorker_ReassignedExecutionHeartbeat(t *testing.T) {
	for _, transport := range []string{"rest", "streaming"} {
		t.Run(transport, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
			started := make(chan int, 4)
			fn := testFn("fn", func(c Context) (any, error) {
				var data struct {
					N int `json:"n"`
				}
				if err := c.Event.Data(&data); err != nil {
					return nil, err
				}
				n := data.N
				started <- n
				select {
				case <-gates[n-1]:
				case <-ctx.Done():
				}
				return nil, nil
			})
			heartbeats := make(chan []string, 8)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/heartbeat") {
					var body struct {
						Jobs []struct {
							JobID string `json:"job_id"`
						} `json:"jobs"`
					}
					_ = json.NewDecoder(r.Body).Decode(&body)
					ids := []string{}
					for _, j := range body.Jobs {
						ids = append(ids, j.JobID)
					}
					heartbeats <- ids
				} else if !strings.HasSuffix(r.URL.Path, "/ack") {
					var body struct {
						ExecutionSeq int64 `json:"execution_seq"`
					}
					_ = json.NewDecoder(r.Body).Decode(&body)
					if body.ExecutionSeq == 1 {
						w.WriteHeader(http.StatusConflict)
						_, _ = w.Write([]byte(`{"error":"STALE_EXECUTION"}`))
					}
				}
			}))
			defer server.Close()
			config := WorkerConfig{ServerURL: server.URL, Functions: []Function{fn}, MaxConcurrentJobs: 2, Logger: NewNoopLogger()}
			var assign func(int)
			var count func() int32
			var heartbeat func()
			var stop func()
			assertNoNack := func() {}
			if transport == "rest" {
				w := NewWorker(config)
				assign = func(n int) {
					w.processJob(ctx, &jobAssignment{JobID: "r", RunID: "r", FunctionID: "fn", ExecutionSeq: int64(n), LeaseToken: "tok", Event: jobEvent{ID: "e", Name: "test.event", Data: json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))}})
				}
				count = w.jobCount.Load
				heartbeat = func() { w.sendHeartbeat(ctx) }
				stop = w.Stop
			} else {
				h := &mockWorkerHandler{onConnect: func(_ context.Context, s *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
					m, err := s.Receive()
					if err != nil {
						return err
					}
					ids := []string{}
					for _, j := range m.GetHeartbeat().GetJobs() {
						ids = append(ids, j.JobId)
					}
					heartbeats <- ids
					return nil
				}}
				streamServer := startMockServer(t, h)
				w := NewStreamingWorker(config)
				assign = func(n int) {
					j := makeJobAssignment("r", "r", "fn", map[string]any{"n": n})
					j.ExecutionSeq = int64(n)
					j.LeaseToken = "tok"
					w.handleJobAssignment(ctx, j)
				}
				count = w.jobCount.Load
				assertNoNack = func() {
					t.Helper()
					for len(w.outCh) > 0 {
						if (<-w.outCh).GetJobNack() != nil {
							t.Fatal("exact duplicate was nacked")
						}
					}
				}
				heartbeat = func() {
					client := ironflowv1connect.NewWorkerServiceClient(newH2CClient(streamServer.URL), streamServer.URL)
					stream := client.Connect(ctx)
					if err := w.sendHeartbeat(stream); err != nil {
						t.Fatal(err)
					}
					_ = stream.CloseRequest()
					_, _ = stream.Receive()
					_ = stream.CloseResponse()
				}
				stop = w.Stop
			}
			defer stop()
			waitCount := func(want int32) {
				t.Helper()
				for count() != want {
					select {
					case <-ctx.Done():
						t.Fatalf("count=%d, want %d", count(), want)
					case <-time.After(time.Millisecond):
					}
				}
			}
			waitStarted := func(want int) {
				t.Helper()
				select {
				case got := <-started:
					if got != want {
						t.Fatalf("started %d, want %d", got, want)
					}
				case <-ctx.Done():
					t.Fatal("handler did not start")
				}
			}
			assertHeartbeat := func(want int) {
				t.Helper()
				heartbeat()
				select {
				case ids := <-heartbeats:
					if len(ids) != want {
						t.Fatalf("heartbeat jobs=%v, want %d active executions", ids, want)
					}
					for _, id := range ids {
						if id != "r" {
							t.Fatal(ids)
						}
					}
				case <-ctx.Done():
					t.Fatal("no heartbeat")
				}
			}
			assign(1)
			waitStarted(1)
			assign(2)
			waitStarted(2)
			assign(2)
			assertNoNack()
			if count() != 2 {
				t.Fatalf("count=%d after duplicate at capacity, want 2", count())
			}
			close(gates[0])
			waitCount(1)
			assertHeartbeat(1)
			assign(2) // Exact redelivery must not execute again or consume another slot.
			if count() != 1 {
				t.Fatalf("count=%d after duplicate, want 1", count())
			}
			close(gates[1])
			waitCount(0)
			assertHeartbeat(0)
		})
	}
}

func TestStreamingWorker_CancelReassignedExecutions(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := make(chan struct{}, 3)
	results := make(chan error, 3)
	sideEffects := make(chan bool, 3)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	fn := testFn("fn", func(c Context) (any, error) {
		var data struct {
			Cancelled bool `json:"cancelled"`
		}
		if err := c.Event.Data(&data); err != nil {
			return nil, err
		}
		started <- struct{}{}
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		_, err := Run(c, "after-cancel", func() (bool, error) {
			sideEffects <- data.Cancelled
			return true, nil
		})
		if data.Cancelled && !errors.Is(err, context.Canceled) {
			results <- fmt.Errorf("cancelled execution started another SDK step: %v", err)
		} else if !data.Cancelled && err != nil {
			results <- fmt.Errorf("unrelated execution was cancelled: %w", err)
		} else {
			results <- nil
		}
		return nil, err
	})
	w := NewStreamingWorker(WorkerConfig{Functions: []Function{fn}, MaxConcurrentJobs: 3, Logger: NewNoopLogger()})
	defer w.Stop()
	defer release()
	for seq := int64(1); seq <= 2; seq++ {
		job := makeJobAssignment("r", "r", "fn", map[string]any{"cancelled": true})
		job.ExecutionSeq = seq
		w.handleJobAssignment(ctx, job)
	}
	w.handleJobAssignment(ctx, makeJobAssignment("other", "other", "fn", map[string]any{"cancelled": false}))
	for range 3 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("handler did not start")
		}
	}
	w.handleCancelJob(&ironflowv1.CancelJob{JobId: "r", Reason: "test"})
	if w.jobCount.Load() != 3 {
		t.Fatal("cancellation released slots before the handlers exited")
	}
	release()
	for range 3 {
		select {
		case err := <-results:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Fatal("handler did not finish")
		}
	}
	for w.jobCount.Load() != 0 {
		select {
		case <-ctx.Done():
			t.Fatal("finished handlers retained slots")
		case <-time.After(time.Millisecond):
		}
	}
	if len(sideEffects) != 1 || <-sideEffects {
		t.Fatal("only the unrelated execution may start another SDK step")
	}
	for len(w.outCh) > 0 {
		msg := <-w.outCh
		if msg.GetJobCompleted().GetJobId() == "r" || msg.GetJobFailed().GetJobId() == "r" {
			t.Fatal("cancelled execution queued a terminal result")
		}
	}
}
