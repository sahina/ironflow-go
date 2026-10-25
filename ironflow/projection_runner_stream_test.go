package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type streamFunc func(context.Context, *connect.Request[ironflowv1.StreamProjectionEventsRequest], *connect.ServerStream[ironflowv1.ProjectionEvent]) error

type fakeProjectionStream struct {
	ironflowv1connect.UnimplementedProjectionServiceHandler
	stream streamFunc
}

func (f fakeProjectionStream) StreamProjectionEvents(ctx context.Context, req *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
	return f.stream(ctx, req, s)
}

// newProjectionStreamServer serves StreamProjectionEvents over Connect (when
// stream is set) and every other path with the JSON fake unary, behind leaseRPCs.
func newProjectionStreamServer(t *testing.T, stream streamFunc, unary http.HandlerFunc) *httptest.Server {
	t.Helper()
	return newProjectionServer(t, http.NewServeMux(), stream, leaseRPCs(unary))
}

// leaseRPCs grants the lease at once and never fences it, and passes every
// other path to next. projection_runner_lease_test.go covers the lease.
func leaseRPCs(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch strings.TrimPrefix(r.URL.Path, projectionServicePath) {
		case "AcquireProjectionLease":
			_, _ = w.Write([]byte(`{"granted":true,"lease":{"token":"sdk:1","epoch":"1"},"ttl":"30s"}`))
		case "RenewProjectionLease":
			_, _ = w.Write([]byte(`{"ttl":"30s"}`))
		case "ReleaseProjectionLease":
			_, _ = w.Write([]byte(`{}`))
		default:
			next(w, r)
		}
	}
}

// newProjectionServer adds the stream and the unary fake to mux.
func newProjectionServer(t *testing.T, mux *http.ServeMux, stream streamFunc, unary http.HandlerFunc) *httptest.Server {
	t.Helper()
	if stream != nil {
		_, h := ironflowv1connect.NewProjectionServiceHandler(fakeProjectionStream{stream: stream})
		mux.Handle(ironflowv1connect.ProjectionServiceStreamProjectionEventsProcedure, h)
	}
	mux.Handle("/", unary)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func streamEvent(id string, seq int64, partition string) *ironflowv1.ProjectionEvent {
	evt := &ironflowv1.ProjectionEvent{Id: id, Name: "inc", Seq: seq}
	if partition != "" {
		evt.Metadata, _ = structpb.NewStruct(map[string]any{"__partition": partition})
	}
	return evt
}

func counterProjection(batchSize int) Projection {
	return CreateProjection(ProjectionConfig{
		Name: "counter", Events: []string{"inc"}, BatchSize: batchSize,
		InitialState: func() map[string]any { return map[string]any{"count": 0.0} },
		Handler: func(state map[string]any, _ ProjectionEvent, _ ProjectionContext) (map[string]any, error) {
			return map[string]any{"count": state["count"].(float64) + 1}, nil
		},
	})
}

func TestProjectionRunner_StreamManagedPartitionsResumeAcrossBatches(t *testing.T) {
	ts := time.Date(2026, 10, 12, 14, 0, 0, 123000000, time.UTC)
	states := map[string]map[string]any{
		"a": {"count": 40.0}, "b": {"count": 90.0}, "__global__": {"count": 10.0},
	}
	var (
		mu        sync.Mutex
		saveOrder []string
		lastTime  = map[string]any{}
		streamReq *ironflowv1.StreamProjectionEventsRequest
		streamHdr string
	)
	srv := newProjectionStreamServer(t, func(ctx context.Context, req *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
		mu.Lock()
		streamReq, streamHdr = req.Msg, req.Header().Get("X-Test")
		mu.Unlock()
		a3 := streamEvent("a3", 4, "a")
		a3.Timestamp = timestamppb.New(ts)
		frames := []*ironflowv1.ProjectionEvent{
			// Batch 1 (full at 3): a, a, b.
			streamEvent("a1", 1, "a"), streamEvent("a2", 2, "a"),
			{Kind: ironflowv1.ProjectionEventKind_PROJECTION_EVENT_KIND_HEARTBEAT},
			streamEvent("b1", 3, "b"),
			// Batch 2 (flushed by the timer): a resumes from its saved state.
			a3, streamEvent("g1", 5, ""),
		}
		for _, f := range frames {
			if err := s.Send(f); err != nil {
				return err
			}
		}
		<-ctx.Done()
		return nil
	}, func(w http.ResponseWriter, req *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(req.Body).Decode(&body)
		mu.Lock()
		defer mu.Unlock()
		switch strings.TrimPrefix(req.URL.Path, projectionServicePath) {
		case "RegisterProjection":
		case "GetProjection":
			pk, _ := body["partition"].(string)
			if pk == "" {
				pk = "__global__"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"state": states[pk]})
		case "SaveProjectionState":
			pk := body["partitionKey"].(string)
			states[pk] = body["state"].(map[string]any)
			saveOrder = append(saveOrder, pk)
			lastTime[pk] = body["lastEventTime"]
		default:
			http.NotFound(w, req)
		}
	})

	runner := NewProjectionRunner(counterProjection(3), srv.URL, map[string]string{"X-Test": "stream"}, nil)
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	defer runner.Stop()
	waitFor(t, "four saves", func() bool { mu.Lock(); defer mu.Unlock(); return len(saveOrder) == 4 })
	runner.Stop()

	mu.Lock()
	defer mu.Unlock()
	if streamReq.Name != "counter" || streamReq.BatchSize != 3 || !streamReq.AcceptHeartbeats || streamHdr != "stream" {
		t.Errorf("stream request = %v (X-Test %q)", streamReq, streamHdr)
	}
	if want := []string{"a", "b", "a", "__global__"}; !reflect.DeepEqual(saveOrder, want) {
		t.Errorf("save order = %v, want %v", saveOrder, want)
	}
	want := map[string]map[string]any{
		"a": {"count": 43.0}, "b": {"count": 91.0}, "__global__": {"count": 11.0},
	}
	if !reflect.DeepEqual(states, want) {
		t.Errorf("saved states = %v, want %v", states, want)
	}
	if got := lastTime["a"]; got != "2026-10-12T14:00:00.123Z" {
		t.Errorf("lastEventTime for a = %v, want the frame timestamp", got)
	}
	if got := lastTime["b"]; got != nil {
		t.Errorf("lastEventTime for b = %v, want it left out", got)
	}
}

func TestProjectionRunner_StreamFailedSaveStopsBatch(t *testing.T) {
	var (
		mu        sync.Mutex
		attempted []string
	)
	closed := make(chan struct{})
	var streams atomic.Int32
	srv := newProjectionStreamServer(t, func(ctx context.Context, _ *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
		if streams.Add(1) > 1 {
			<-ctx.Done()
			return nil
		}
		for _, f := range []*ironflowv1.ProjectionEvent{
			streamEvent("a1", 1, "a"), streamEvent("b1", 2, "b"), streamEvent("a2", 3, "a"),
		} {
			if err := s.Send(f); err != nil {
				return err
			}
		}
		<-ctx.Done() // the runner drops the stream after the failed save
		close(closed)
		return nil
	}, func(w http.ResponseWriter, req *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(req.Body).Decode(&body)
		switch strings.TrimPrefix(req.URL.Path, projectionServicePath) {
		case "RegisterProjection", "GetProjection":
			_, _ = w.Write([]byte(`{}`))
		case "SaveProjectionState":
			pk := body["partitionKey"].(string)
			mu.Lock()
			attempted = append(attempted, pk)
			mu.Unlock()
			if pk == "b" {
				http.Error(w, "boom", http.StatusInternalServerError)
			}
		default:
			http.NotFound(w, req)
		}
	})

	runner := NewProjectionRunner(counterProjection(3), srv.URL, nil, nil)
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	defer runner.Stop()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("runner kept the stream open after a failed save")
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"a", "b"}; !reflect.DeepEqual(attempted, want) {
		t.Errorf("save attempts = %v, want %v (stop at the first failed save)", attempted, want)
	}
}

func TestProjectionRunner_StreamUnsupportedFallsBackToPoll(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stream streamFunc
	}{
		{"http 404", nil},
		{"unimplemented", func(context.Context, *connect.Request[ironflowv1.StreamProjectionEventsRequest], *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
			return connect.NewError(connect.CodeUnimplemented, errors.New("no stream"))
		}},
		{"not found", func(context.Context, *connect.Request[ironflowv1.StreamProjectionEventsRequest], *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
			return connect.NewError(connect.CodeNotFound, errors.New("no projection"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var polls atomic.Int32
			srv := newProjectionStreamServer(t, tc.stream, func(w http.ResponseWriter, req *http.Request) {
				switch strings.TrimPrefix(req.URL.Path, projectionServicePath) {
				case "RegisterProjection":
				case "PollProjectionEvents":
					polls.Add(1)
					_, _ = w.Write([]byte(`{}`))
				default:
					http.NotFound(w, req)
				}
			})
			runner := NewProjectionRunner(counterProjection(0), srv.URL, nil, nil)
			if err := runner.Start(); err != nil {
				t.Fatal(err)
			}
			defer runner.Stop()
			waitFor(t, "a poll", func() bool { return polls.Load() > 0 })
		})
	}
}

func TestProjectionRunner_StreamAuthFailureStopsRunner(t *testing.T) {
	for _, tc := range []struct {
		code connect.Code
		want error
	}{
		{connect.CodeUnauthenticated, ErrUnauthorized},
		{connect.CodePermissionDenied, ErrForbidden},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			var streams, polls atomic.Int32
			srv := newProjectionStreamServer(t, func(context.Context, *connect.Request[ironflowv1.StreamProjectionEventsRequest], *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
				streams.Add(1)
				return connect.NewError(tc.code, errors.New("denied"))
			}, func(w http.ResponseWriter, req *http.Request) {
				if strings.HasSuffix(req.URL.Path, "/PollProjectionEvents") {
					polls.Add(1)
				}
			})
			runner := NewProjectionRunner(counterProjection(0), srv.URL, nil, nil)
			done := make(chan error, 1)
			go func() { done <- runner.streamLoop() }()
			select {
			case err := <-done:
				if !errors.Is(err, tc.want) {
					t.Errorf("streamLoop = %v, want %v", err, tc.want)
				}
			case <-time.After(3 * time.Second):
				runner.Stop()
				t.Fatal("streamLoop kept running after an auth failure")
			}
			if streams.Load() != 1 || polls.Load() != 0 {
				t.Errorf("streams = %d, polls = %d; want 1 and 0", streams.Load(), polls.Load())
			}
		})
	}
}

func TestProjectionRunner_StopClosesStream(t *testing.T) {
	opened, closed := make(chan struct{}), make(chan struct{})
	srv := newProjectionStreamServer(t, func(ctx context.Context, _ *connect.Request[ironflowv1.StreamProjectionEventsRequest], _ *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
		close(opened)
		<-ctx.Done()
		close(closed)
		return nil
	}, func(http.ResponseWriter, *http.Request) {})
	runner := NewProjectionRunner(counterProjection(0), srv.URL, nil, nil)
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-opened:
	case <-time.After(3 * time.Second):
		t.Fatal("runner never opened the stream")
	}
	runner.Stop()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop left the stream open")
	}
}

// attemptLogger records the "attempt" of each stream failure, counts errors
// and keeps the Warn messages.
type attemptLogger struct {
	mu       sync.Mutex
	attempts []any
	errors   int
	warns    []string
	errMsgs  []string
}

func (l *attemptLogger) Debug(string, ...any) {}
func (l *attemptLogger) Warn(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg)
}
func (l *attemptLogger) Info(msg string, args ...any) {
	l.record(msg, args)
}
func (l *attemptLogger) Error(msg string, args ...any) {
	l.mu.Lock()
	l.errors++
	l.errMsgs = append(l.errMsgs, msg)
	l.mu.Unlock()
	l.record(msg, args)
}
func (l *attemptLogger) record(msg string, args []any) {
	if msg != "Projection stream failed, reconnecting" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == "attempt" {
			l.attempts = append(l.attempts, args[i+1])
		}
	}
}

func TestProjectionRunner_StreamFrameResetsReconnectBackoff(t *testing.T) {
	base, maxWait := projectionReconnectBase, projectionReconnectMax
	projectionReconnectBase, projectionReconnectMax = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { projectionReconnectBase, projectionReconnectMax = base, maxWait })

	var calls atomic.Int32
	srv := newProjectionStreamServer(t, func(_ context.Context, _ *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
		switch calls.Add(1) {
		case 1, 2:
			return connect.NewError(connect.CodeUnavailable, errors.New("down"))
		case 3:
			// The link worked: a frame arrives, then the stream drops.
			if err := s.Send(&ironflowv1.ProjectionEvent{Kind: ironflowv1.ProjectionEventKind_PROJECTION_EVENT_KIND_HEARTBEAT}); err != nil {
				return err
			}
			return connect.NewError(connect.CodeUnavailable, errors.New("dropped"))
		default:
			return connect.NewError(connect.CodeUnimplemented, errors.New("end the test"))
		}
	}, func(http.ResponseWriter, *http.Request) {})

	log := &attemptLogger{}
	runner := NewProjectionRunner(counterProjection(0), srv.URL, nil, log)
	if err := runner.streamLoop(); !errors.Is(err, errStreamingUnsupported) {
		t.Fatalf("streamLoop = %v, want errStreamingUnsupported", err)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if want := []any{1, 2, 1}; !reflect.DeepEqual(log.attempts, want) {
		t.Errorf("failure attempts = %v, want %v (a received frame resets the count)", log.attempts, want)
	}
	if log.errors != 0 {
		t.Errorf("logged %d errors, want 0: the drop after a working link must not escalate", log.errors)
	}
}

func TestProjectionRunner_StreamFailingBatchEscalates(t *testing.T) {
	base, maxWait := projectionReconnectBase, projectionReconnectMax
	projectionReconnectBase, projectionReconnectMax = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { projectionReconnectBase, projectionReconnectMax = base, maxWait })

	var opens atomic.Int32
	srv := newProjectionStreamServer(t, func(ctx context.Context, _ *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
		if opens.Add(1) > 4 {
			return connect.NewError(connect.CodeUnimplemented, errors.New("end the test"))
		}
		// The link works (a heartbeat arrives), but the handler rejects the event every time.
		for _, f := range []*ironflowv1.ProjectionEvent{
			{Kind: ironflowv1.ProjectionEventKind_PROJECTION_EVENT_KIND_HEARTBEAT}, streamEvent("e1", 1, ""),
		} {
			if err := s.Send(f); err != nil {
				return err
			}
		}
		<-ctx.Done()
		return nil
	}, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) })

	proj := counterProjection(0)
	proj.Config.Handler = func(map[string]any, ProjectionEvent, ProjectionContext) (map[string]any, error) {
		return nil, errors.New("boom")
	}
	log := &attemptLogger{}
	runner := NewProjectionRunner(proj, srv.URL, nil, log)
	if err := runner.streamLoop(); !errors.Is(err, errStreamingUnsupported) {
		t.Fatalf("streamLoop = %v, want errStreamingUnsupported", err)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if want := []any{1, 2, 3, 4}; !reflect.DeepEqual(log.attempts, want) {
		t.Errorf("failure attempts = %v, want %v (a failed batch must count)", log.attempts, want)
	}
	if log.errors != 2 {
		t.Errorf("logged %d errors, want 2 (attempts 3 and 4)", log.errors)
	}
}

// A batch that succeeds resets the count, so rare failures between good
// batches on a stream that stays up do not escalate.
func TestProjectionRunner_StreamSuccessfulBatchResetsBackoff(t *testing.T) {
	base, maxWait := projectionReconnectBase, projectionReconnectMax
	projectionReconnectBase, projectionReconnectMax = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { projectionReconnectBase, projectionReconnectMax = base, maxWait })

	var opens atomic.Int32
	srv := newProjectionStreamServer(t, func(ctx context.Context, _ *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
		if opens.Add(1) > 4 {
			return connect.NewError(connect.CodeUnimplemented, errors.New("end the test"))
		}
		for _, f := range []*ironflowv1.ProjectionEvent{streamEvent("e1", 1, ""), streamEvent("e2", 2, "")} {
			if err := s.Send(f); err != nil {
				return err
			}
		}
		<-ctx.Done()
		return nil
	}, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) })

	proj := counterProjection(1)
	proj.Config.Handler = func(state map[string]any, e ProjectionEvent, _ ProjectionContext) (map[string]any, error) {
		if e.ID == "e2" {
			return nil, errors.New("boom")
		}
		return state, nil
	}
	log := &attemptLogger{}
	runner := NewProjectionRunner(proj, srv.URL, nil, log)
	if err := runner.streamLoop(); !errors.Is(err, errStreamingUnsupported) {
		t.Fatalf("streamLoop = %v, want errStreamingUnsupported", err)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if want := []any{1, 1, 1, 1}; !reflect.DeepEqual(log.attempts, want) {
		t.Errorf("failure attempts = %v, want %v (the e1 batch succeeded each time)", log.attempts, want)
	}
}

func externalProjection(batchSize int, handler ProjectionHandler) Projection {
	return CreateProjection(ProjectionConfig{
		Name: "mailer", Events: []string{"inc"}, Mode: ProjectionModeExternal, BatchSize: batchSize, Handler: handler,
	})
}

func TestProjectionRunner_StopFinishesAcceptedBatch(t *testing.T) {
	var acks atomic.Int32
	srv := newProjectionStreamServer(t, func(ctx context.Context, _ *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
		if err := s.Send(streamEvent("e1", 1, "")); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	}, func(_ http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/AckProjectionEvents") {
			acks.Add(1)
		}
	})
	entered, release := make(chan struct{}), make(chan struct{})
	runner := NewProjectionRunner(externalProjection(1, func(map[string]any, ProjectionEvent, ProjectionContext) (map[string]any, error) {
		close(entered)
		<-release
		return nil, nil
	}), srv.URL, nil, nil)
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("handler never ran")
	}
	stopped := make(chan struct{})
	go func() { runner.Stop(); close(stopped) }()
	<-runner.ctx.Done() // Stop has begun before the handler returns
	close(release)
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not return")
	}
	if acks.Load() != 1 {
		t.Errorf("acks = %d, want 1: Stop must let the accepted batch finish", acks.Load())
	}
}

func TestProjectionRunner_StopFlushesPendingFrames(t *testing.T) {
	interval := projectionFlushInterval
	projectionFlushInterval = time.Hour // only Stop can flush
	t.Cleanup(func() { projectionFlushInterval = interval })

	sent := make(chan struct{})
	var ackSeq atomic.Int64
	srv := newProjectionStreamServer(t, func(ctx context.Context, _ *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
		for _, f := range []*ironflowv1.ProjectionEvent{streamEvent("e1", 1, ""), streamEvent("e2", 2, "")} {
			if err := s.Send(f); err != nil {
				return err
			}
		}
		close(sent)
		<-ctx.Done()
		return nil
	}, func(_ http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/AckProjectionEvents") {
			var body struct{ LastEventSeq int64 }
			_ = json.NewDecoder(req.Body).Decode(&body)
			ackSeq.Store(body.LastEventSeq)
		}
	})
	var handled atomic.Int32
	runner := NewProjectionRunner(externalProjection(100, func(map[string]any, ProjectionEvent, ProjectionContext) (map[string]any, error) {
		handled.Add(1)
		return nil, nil
	}), srv.URL, nil, nil)
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sent:
	case <-time.After(3 * time.Second):
		t.Fatal("stream never opened")
	}
	time.Sleep(200 * time.Millisecond) // let the runner take both frames
	runner.Stop()
	if handled.Load() != 2 || ackSeq.Load() != 2 {
		t.Errorf("handled = %d, acked seq = %d; want 2 and 2: Stop must flush the pending frames", handled.Load(), ackSeq.Load())
	}
}

func TestProjectionRunner_StopIsBounded(t *testing.T) {
	timeout := projectionStopTimeout
	projectionStopTimeout = 100 * time.Millisecond
	t.Cleanup(func() { projectionStopTimeout = timeout })

	srv := newProjectionStreamServer(t, func(ctx context.Context, _ *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
		if err := s.Send(streamEvent("e1", 1, "")); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	}, func(http.ResponseWriter, *http.Request) {})
	entered, release := make(chan struct{}), make(chan struct{})
	log := &attemptLogger{}
	runner := NewProjectionRunner(externalProjection(1, func(map[string]any, ProjectionEvent, ProjectionContext) (map[string]any, error) {
		close(entered)
		<-release // a handler cannot be cancelled
		return nil, nil
	}), srv.URL, nil, log)
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { close(release); <-runner.done })
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("handler never ran")
	}
	stopped := make(chan struct{})
	go func() { runner.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop waited past its limit for a handler that does not return")
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if n := len(slices.DeleteFunc(slices.Clone(log.warns), func(w string) bool { return !strings.Contains(w, "did not stop") })); n != 1 {
		t.Errorf("warns = %q, want one 'did not stop' warning", log.warns)
	}
}

// Both workers stop their runners in parallel: two blocked handlers cost one
// Stop timeout, not two (#2822).
func TestWorkers_StopProjectionRunnersInParallel(t *testing.T) {
	timeout := projectionStopTimeout
	projectionStopTimeout = 300 * time.Millisecond
	t.Cleanup(func() { projectionStopTimeout = timeout })

	srv := newProjectionStreamServer(t, func(ctx context.Context, _ *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
		if err := s.Send(streamEvent("e1", 1, "")); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	}, func(http.ResponseWriter, *http.Request) {})
	startBlocked := func(t *testing.T) []*ProjectionRunner {
		release := make(chan struct{})
		var runners []*ProjectionRunner
		for range 2 {
			entered := make(chan struct{})
			runner := NewProjectionRunner(externalProjection(1, func(map[string]any, ProjectionEvent, ProjectionContext) (map[string]any, error) {
				close(entered)
				<-release
				return nil, nil
			}), srv.URL, nil, nil)
			if err := runner.Start(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("handler never ran")
			}
			runners = append(runners, runner)
		}
		t.Cleanup(func() {
			close(release)
			for _, r := range runners {
				<-r.done
			}
		})
		return runners
	}
	for name, stop := range map[string]func([]*ProjectionRunner){
		"Worker":          func(rs []*ProjectionRunner) { (&Worker{projectionRunners: rs}).stopProjectionRunners() },
		"StreamingWorker": func(rs []*ProjectionRunner) { (&StreamingWorker{projectionRunners: rs}).stopProjectionRunners() },
	} {
		t.Run(name, func(t *testing.T) {
			runners := startBlocked(t)
			start := time.Now()
			stop(runners)
			if elapsed := time.Since(start); elapsed >= 2*projectionStopTimeout {
				t.Errorf("stop took %v, want under %v: the runners stopped one at a time", elapsed, 2*projectionStopTimeout)
			}
		})
	}
}

func TestProjectionRunner_Start_RetriesRegistration(t *testing.T) {
	base, maxWait := projectionReconnectBase, projectionReconnectMax
	projectionReconnectBase, projectionReconnectMax = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { projectionReconnectBase, projectionReconnectMax = base, maxWait })

	var registers, polls atomic.Int32
	srv := newProjectionStreamServer(t, nil, func(w http.ResponseWriter, req *http.Request) {
		switch strings.TrimPrefix(req.URL.Path, projectionServicePath) {
		case "RegisterProjection":
			if registers.Add(1) <= 2 {
				http.Error(w, "boom", http.StatusInternalServerError)
			}
		case "PollProjectionEvents":
			polls.Add(1)
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, req)
		}
	})
	runner := NewProjectionRunner(counterProjection(0), srv.URL, nil, nil)
	if err := runner.Start(); err != nil {
		t.Fatalf("Start = %v, want nil: the runner retries registration", err)
	}
	defer runner.Stop()
	waitFor(t, "a poll after registration", func() bool { return polls.Load() > 0 })
	if registers.Load() != 3 {
		t.Errorf("registers = %d, want 3 (two failures, then success)", registers.Load())
	}
}

// A config error does not fix itself: the runner stops instead of retrying.
func TestProjectionRunner_Start_StopsOnInvalidArgument(t *testing.T) {
	var registers atomic.Int32
	srv := newProjectionStreamServer(t, nil, func(w http.ResponseWriter, req *http.Request) {
		registers.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":"invalid_argument","message":"events is required"}`))
	})
	runner := NewProjectionRunner(counterProjection(0), srv.URL, nil, nil)
	defer runner.Stop()
	if err := runner.Start(); !errors.Is(err, errProjectionInvalidArgument) {
		t.Fatalf("Start = %v, want errProjectionInvalidArgument", err)
	}
	if registers.Load() != 1 {
		t.Errorf("registers = %d, want 1", registers.Load())
	}
}

// An InvalidArgument on a register retry stops the runner too.
func TestProjectionRunner_RegisterRetryStopsOnInvalidArgument(t *testing.T) {
	base, maxWait := projectionReconnectBase, projectionReconnectMax
	projectionReconnectBase, projectionReconnectMax = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { projectionReconnectBase, projectionReconnectMax = base, maxWait })

	var registers atomic.Int32
	srv := newProjectionStreamServer(t, nil, func(w http.ResponseWriter, req *http.Request) {
		if registers.Add(1) == 1 {
			http.Error(w, "boom", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":"invalid_argument","message":"events is required"}`))
	})
	log := &attemptLogger{}
	runner := NewProjectionRunner(counterProjection(0), srv.URL, nil, log)
	defer runner.Stop()
	if err := runner.Start(); err != nil {
		t.Fatalf("Start = %v, want nil: a 503 is retried", err)
	}
	select {
	case <-runner.done:
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop on InvalidArgument")
	}
	if registers.Load() != 2 {
		t.Errorf("registers = %d, want 2", registers.Load())
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.errors != 1 {
		t.Errorf("logged %d errors, want 1 (the stop)", log.errors)
	}
}

// Register failures log at Warn, then at Error from the 3rd, as the stream does.
func TestProjectionRunner_RegisterRetryEscalates(t *testing.T) {
	base, maxWait := projectionReconnectBase, projectionReconnectMax
	projectionReconnectBase, projectionReconnectMax = time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { projectionReconnectBase, projectionReconnectMax = base, maxWait })

	var registers atomic.Int32
	srv := newProjectionStreamServer(t, nil, func(w http.ResponseWriter, req *http.Request) {
		if !strings.HasSuffix(req.URL.Path, "RegisterProjection") {
			http.NotFound(w, req)
		} else if registers.Add(1) <= 4 {
			http.Error(w, "boom", http.StatusServiceUnavailable)
		} else {
			_, _ = w.Write([]byte(`{}`))
		}
	})
	log := &attemptLogger{}
	runner := NewProjectionRunner(counterProjection(0), srv.URL, nil, log)
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a successful register", func() bool { return registers.Load() > 4 })
	runner.Stop()
	log.mu.Lock()
	defer log.mu.Unlock()
	notRegister := func(m string) bool { return !strings.Contains(m, "register failed") }
	warns := slices.DeleteFunc(slices.Clone(log.warns), notRegister)
	errs := slices.DeleteFunc(slices.Clone(log.errMsgs), notRegister)
	if len(warns) != 2 || len(errs) != 2 {
		t.Errorf("register warns = %d, errors = %q; want 2 warns (attempts 1-2) and 2 errors (attempts 3-4)", len(warns), errs)
	}
}
