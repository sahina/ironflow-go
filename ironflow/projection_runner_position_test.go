package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

// The stream conformance of ADR 0164: the runner opens each stream at its
// delivery position, raises it to a heartbeat mark only after the frames
// before the mark commit, and keeps it where it was when a batch fails.
func TestProjectionRunner_StreamPositionRisesOnlyAfterCommit(t *testing.T) {
	var (
		mu        sync.Mutex
		positions []int64
		saves     int
	)
	srv := newProjectionStreamServer(t, func(ctx context.Context, req *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
		mu.Lock()
		positions = append(positions, req.Msg.GetPosition())
		open := len(positions)
		mu.Unlock()
		frames := []*ironflowv1.ProjectionEvent{
			streamEvent("e1", 1, ""),
			{Kind: ironflowv1.ProjectionEventKind_PROJECTION_EVENT_KIND_HEARTBEAT, Seq: 7},
		}
		if open == 2 {
			frames = []*ironflowv1.ProjectionEvent{streamEvent("e8", 8, "")}
		}
		for _, f := range frames {
			if err := s.Send(f); err != nil {
				return err
			}
		}
		if open == 1 {
			return nil // a clean end: the runner reconnects
		}
		<-ctx.Done()
		return nil
	}, func(w http.ResponseWriter, req *http.Request) {
		switch strings.TrimPrefix(req.URL.Path, projectionServicePath) {
		case "RegisterProjection":
		case "GetProjection":
			_ = json.NewEncoder(w).Encode(map[string]any{})
		case "SaveProjectionState":
			mu.Lock()
			saves++
			failed := saves == 2 // the save of e8
			mu.Unlock()
			if failed {
				http.Error(w, `{"code":"unavailable"}`, http.StatusServiceUnavailable)
			}
		default:
			http.NotFound(w, req)
		}
	})

	runner := NewProjectionRunner(counterProjection(1), srv.URL, nil, nil)
	shortenReconnect(t)
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	defer runner.Stop()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(positions)
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(positions) < 3 {
		t.Fatalf("streams opened at %v, want 3 opens", positions)
	}
	// Open 1 starts at 0, commits e1 and then the mark 7. Open 2 fails the save
	// of e8, so open 3 starts at 7 again.
	if positions[0] != 0 || positions[1] != 7 || positions[2] != 7 {
		t.Fatalf("streams opened at %v, want [0 7 7 ...]", positions)
	}
}

func shortenReconnect(t *testing.T) {
	t.Helper()
	base := projectionReconnectBase
	projectionReconnectBase = 10 * time.Millisecond
	t.Cleanup(func() { projectionReconnectBase = base })
}

// The poll conformance of ADR 0164: the runner sends its position, raises it
// to the response watermark after the batch commits (an empty page too), keeps
// it after a failed batch, and starts at 0 on a new lease.
func TestProjectionRunner_PollPositionRisesOnlyAfterCommit(t *testing.T) {
	var (
		mu        sync.Mutex
		positions []string
		polls     int
	)
	server := httptest.NewServer(leaseRPCs(func(w http.ResponseWriter, req *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(req.Body).Decode(&body)
		mu.Lock()
		defer mu.Unlock()
		switch strings.TrimPrefix(req.URL.Path, projectionServicePath) {
		case "RegisterProjection":
		case "PollProjectionEvents":
			polls++
			positions = append(positions, body["position"].(string))
			switch polls {
			case 1: // a page of other partitions' events: nothing to apply
				_, _ = w.Write([]byte(`{"events":[],"lastEventSeq":"40","position":"0"}`))
			case 2, 3:
				_, _ = w.Write([]byte(`{"events":[{"id":"e41","seq":"41"}],"lastEventSeq":"45","position":"40"}`))
			default:
				_, _ = w.Write([]byte(`{"events":[],"lastEventSeq":"45","position":"45"}`))
			}
		case "AckProjectionEvents":
			if polls == 2 {
				http.Error(w, `{"code":"unavailable"}`, http.StatusServiceUnavailable)
			}
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()

	proj := CreateProjection(ProjectionConfig{
		Name: "orders", Events: []string{"inc"}, Mode: ProjectionModeExternal,
		Handler: func(map[string]any, ProjectionEvent, ProjectionContext) (map[string]any, error) { return nil, nil },
	})
	runner := NewProjectionRunner(proj, server.URL, nil, nil)
	runner.lease = &projectionLease{Token: "sdk:1", Epoch: 1}
	for range 4 {
		_, _ = runner.poll()
	}
	mu.Lock()
	got := append([]string(nil), positions...)
	mu.Unlock()
	if want := []string{"0", "40", "40", "45"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("polls sent positions %v, want %v", got, want)
	}

	runner.lease = nil
	ln := httptest.NewServer(leaseRPCs(http.NotFound))
	defer ln.Close()
	runner.baseURL = ln.URL
	if _, err := runner.acquireLease(); err != nil {
		t.Fatal(err)
	}
	if runner.pos != 0 {
		t.Fatalf("a new lease kept position %d", runner.pos)
	}
}

// A new runner against a server older than ADR 0164 stops with SERVER_TOO_OLD
// and applies nothing, on both transports.
func TestProjectionRunner_ServerTooOldForPosition(t *testing.T) {
	applied := 0
	proj := CreateProjection(ProjectionConfig{
		Name: "orders", Events: []string{"inc"}, Mode: ProjectionModeExternal,
		Handler: func(map[string]any, ProjectionEvent, ProjectionContext) (map[string]any, error) {
			applied++
			return nil, nil
		},
	})

	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"events":[{"id":"e1","seq":"1"}],"lastEventSeq":"1"}`))
	}))
	defer old.Close()
	runner := NewProjectionRunner(proj, old.URL, nil, nil)
	runner.lease = &projectionLease{Token: "sdk:1", Epoch: 1}
	_, err := runner.poll()
	var ie *IronflowError
	if !errors.As(err, &ie) || ie.Code != ErrCodeServerTooOld {
		t.Fatalf("poll error = %v, want SERVER_TOO_OLD", err)
	}

	mux := http.NewServeMux()
	_, h := ironflowv1connect.NewProjectionServiceHandler(oldStreamServer{})
	mux.Handle(ironflowv1connect.ProjectionServiceStreamProjectionEventsProcedure, h)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	runner = NewProjectionRunner(proj, srv.URL, nil, nil)
	client := ironflowv1connect.NewProjectionServiceClient(http.DefaultClient, srv.URL, connect.WithProtoJSON())
	_, _, err = runner.readStream(context.Background(), client)
	if !errors.As(err, &ie) || ie.Code != ErrCodeServerTooOld {
		t.Fatalf("stream error = %v, want SERVER_TOO_OLD", err)
	}
	if !isServerTooOld(err) {
		t.Fatal("streamLoop would reconnect on this error")
	}
	if applied != 0 {
		t.Fatalf("applied %d events from an old server", applied)
	}
}

// oldStreamServer is a server older than ADR 0164: no OPEN frame.
type oldStreamServer struct {
	ironflowv1connect.UnimplementedProjectionServiceHandler
}

func (oldStreamServer) StreamProjectionEvents(_ context.Context, _ *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
	return s.Send(streamEvent("e1", 1, ""))
}

func TestProjectionRunner_ServerTooOldOnEmptyStream(t *testing.T) {
	_, h := ironflowv1connect.NewProjectionServiceHandler(emptyStreamServer{})
	srv := httptest.NewServer(h)
	defer srv.Close()
	runner := NewProjectionRunner(counterProjection(1), srv.URL, nil, nil)
	client := ironflowv1connect.NewProjectionServiceClient(http.DefaultClient, srv.URL, connect.WithProtoJSON())
	_, _, err := runner.readStream(context.Background(), client)
	var ie *IronflowError
	if !errors.As(err, &ie) || ie.Code != ErrCodeServerTooOld {
		t.Fatalf("stream error = %v, want SERVER_TOO_OLD", err)
	}
}

type emptyStreamServer struct {
	ironflowv1connect.UnimplementedProjectionServiceHandler
}

func (emptyStreamServer) StreamProjectionEvents(context.Context, *connect.Request[ironflowv1.StreamProjectionEventsRequest], *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
	return nil
}

func TestProjectionRunner_StopsOnFailedPrecondition(t *testing.T) {
	refused := `{"code":"failed_precondition","message":"projection \"orders\" is managed and its delivery floor, seq 1000, is above the PUBSUB stream's last seq 3"}`
	for _, transport := range []string{"stream", "poll"} {
		t.Run(transport, func(t *testing.T) {
			var mu sync.Mutex
			reads := 0
			read := func() {
				mu.Lock()
				reads++
				mu.Unlock()
			}
			stream := func(context.Context, *connect.Request[ironflowv1.StreamProjectionEventsRequest], *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
				read()
				return connect.NewError(connect.CodeFailedPrecondition, errors.New("projection is managed and its stream is behind"))
			}
			if transport == "poll" {
				stream = nil
			}
			srv := newProjectionStreamServer(t, stream, func(w http.ResponseWriter, req *http.Request) {
				switch strings.TrimPrefix(req.URL.Path, projectionServicePath) {
				case "RegisterProjection":
				case "PollProjectionEvents":
					read()
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(refused))
				default:
					http.NotFound(w, req)
				}
			})
			runner := NewProjectionRunner(counterProjection(1), srv.URL, nil, nil)
			shortenReconnect(t)
			if err := runner.Start(); err != nil {
				t.Fatal(err)
			}
			defer runner.Stop()
			select {
			case <-runner.done:
			case <-time.After(10 * time.Second):
				mu.Lock()
				defer mu.Unlock()
				t.Fatalf("runner still running after %d refused reads", reads)
			}
			mu.Lock()
			defer mu.Unlock()
			if reads != 1 {
				t.Fatalf("runner read %d times, want 1", reads)
			}
		})
	}
}

func TestProjectionRunner_StopsOnRefusedStateLoad(t *testing.T) {
	refused := `{"code":"failed_precondition","message":"its last rebuild did not finish"}`
	for _, tc := range []struct{ name, transport, partition string }{
		{"stream global", "stream", ""},
		{"stream partition", "stream", "a"},
		{"poll partition", "poll", "a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var loads, saves atomic.Int64
			stream := func(ctx context.Context, _ *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
				if err := s.Send(streamEvent("e1", 1, tc.partition)); err != nil {
					return err
				}
				<-ctx.Done()
				return nil
			}
			if tc.transport == "poll" {
				stream = nil
			}
			srv := newProjectionStreamServer(t, stream, func(w http.ResponseWriter, req *http.Request) {
				switch strings.TrimPrefix(req.URL.Path, projectionServicePath) {
				case "RegisterProjection":
				case "GetProjection":
					loads.Add(1)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(refused))
				case "PollProjectionEvents":
					_ = json.NewEncoder(w).Encode(map[string]any{
						"events":       []map[string]any{{"id": "e1", "name": "inc", "seq": "1", "metadata": map[string]any{"__partition": tc.partition}}},
						"lastEventSeq": "1",
					})
				case "SaveProjectionState":
					saves.Add(1)
				default:
					http.NotFound(w, req)
				}
			})
			runner := NewProjectionRunner(counterProjection(1), srv.URL, nil, nil)
			shortenReconnect(t)
			if err := runner.Start(); err != nil {
				t.Fatal(err)
			}
			defer runner.Stop()
			select {
			case <-runner.done:
			case <-time.After(10 * time.Second):
				t.Fatalf("runner still running after %d refused loads", loads.Load())
			}
			if loads.Load() == 0 || saves.Load() != 0 {
				t.Fatalf("loads=%d saves=%d, want at least one load and no save", loads.Load(), saves.Load())
			}
		})
	}
}
