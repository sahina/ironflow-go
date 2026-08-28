package ironflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type resumePubSubServer struct {
	ironflowv1connect.UnimplementedPubSubServiceHandler

	mu       sync.Mutex
	requests []*ironflowv1.SubscribeRequest
}

type capturePubSubServer struct {
	ironflowv1connect.UnimplementedPubSubServiceHandler
	request chan *ironflowv1.SubscribeRequest
}

func (s capturePubSubServer) Subscribe(
	_ context.Context,
	req *connect.Request[ironflowv1.SubscribeRequest],
	_ *connect.ServerStream[ironflowv1.SubscriptionEvent],
) error {
	s.request <- req.Msg
	return nil
}

func TestGrpcSubscriptionClient_SubscribeWithZeroResumeCursor(t *testing.T) {
	requests := make(chan *ironflowv1.SubscribeRequest, 1)
	path, handler := ironflowv1connect.NewPubSubServiceHandler(capturePubSubServer{request: requests})
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	server.Start()
	defer server.Close()

	client := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{
		ServerURL: server.URL,
		Logger:    NewNoopLogger(),
	})
	defer client.Close()
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	zero := uint64(0)
	if _, err := client.Subscribe(context.Background(), "orders.*", &SubscribeOptions{
		StartAfterSequence: &zero,
	}); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	select {
	case request := <-requests:
		if request.Options.StartAfterSequence == nil || *request.Options.StartAfterSequence != 0 {
			t.Fatalf("start_after_sequence = %v, want explicit 0", request.Options.StartAfterSequence)
		}
		if !request.Options.IncludeMetadata {
			t.Fatal("cursor subscriptions must request sequence metadata")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscribe request")
	}
}

func (s *resumePubSubServer) Subscribe(
	ctx context.Context,
	req *connect.Request[ironflowv1.SubscribeRequest],
	stream *connect.ServerStream[ironflowv1.SubscriptionEvent],
) error {
	s.mu.Lock()
	s.requests = append(s.requests, req.Msg)
	call := len(s.requests)
	s.mu.Unlock()

	sequence := uint64(405)
	if call > 1 {
		sequence = 406
	}
	if err := stream.Send(&ironflowv1.SubscriptionEvent{
		SubscriptionId: "sub-1",
		EventId:        "event",
		Topic:          "orders.created",
		Sequence:       sequence,
		Metadata:       &ironflowv1.EventMetadata{},
	}); err != nil {
		return err
	}
	if call == 1 {
		return connect.NewError(connect.CodeUnavailable, errors.New("transport dropped"))
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *resumePubSubServer) capturedRequests() []*ironflowv1.SubscribeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*ironflowv1.SubscribeRequest(nil), s.requests...)
}

func TestGrpcSubscriptionClient_ReconnectAdvancesResumeCursor(t *testing.T) {
	service := &resumePubSubServer{}
	path, handler := ironflowv1connect.NewPubSubServiceHandler(service)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	server.Start()
	defer server.Close()

	client := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{
		ServerURL:         server.URL,
		ReconnectDelay:    time.Millisecond,
		MaxReconnectDelay: time.Millisecond,
		ReconnectBackoff:  1,
		Logger:            NewNoopLogger(),
	})
	defer client.Close()
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cursor := uint64(400)
	sub, err := client.Subscribe(context.Background(), "orders.*", &SubscribeOptions{
		StartAfterSequence: &cursor,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	for _, want := range []uint64{405, 406} {
		select {
		case event := <-sub.Events():
			if event.Meta == nil || event.Meta.Sequence != want {
				t.Fatalf("event sequence = %v, want %d", event.Meta, want)
			}
		case err := <-sub.Errors():
			t.Fatalf("subscription error before sequence %d: %v", want, err)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for sequence %d", want)
		}
	}

	waitForCondition(t, time.Second, func() bool {
		return len(service.capturedRequests()) >= 2
	})
	requests := service.capturedRequests()
	if requests[0].Options.StartAfterSequence == nil || *requests[0].Options.StartAfterSequence != 400 {
		t.Fatalf("initial cursor = %v, want 400", requests[0].Options.StartAfterSequence)
	}
	if requests[1].Options.StartAfterSequence == nil || *requests[1].Options.StartAfterSequence != 405 {
		t.Fatalf("reconnect cursor = %v, want 405", requests[1].Options.StartAfterSequence)
	}
	if requests[1].Options.Replay != 0 {
		t.Fatalf("reconnect replay = %d, want 0", requests[1].Options.Replay)
	}
}

func TestGrpcSubscriptionClient_UnpositionedStreamKeepsEndingOnFailure(t *testing.T) {
	service := &resumePubSubServer{}
	path, handler := ironflowv1connect.NewPubSubServiceHandler(service)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	server.Start()
	defer server.Close()

	client := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{
		ServerURL:         server.URL,
		AutoReconnect:     true,
		ReconnectDelay:    time.Millisecond,
		MaxReconnectDelay: time.Millisecond,
		ReconnectBackoff:  1,
		Logger:            NewNoopLogger(),
	})
	defer client.Close()
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	sub, err := client.Subscribe(context.Background(), "orders.*", nil)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	select {
	case <-sub.Events():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for initial event")
	}
	select {
	case <-sub.Errors():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for terminal stream error")
	}
	time.Sleep(10 * time.Millisecond)
	if got := len(service.capturedRequests()); got != 1 {
		t.Fatalf("unpositioned stream attempts = %d, want 1", got)
	}
}

type rejectingPubSubServer struct {
	ironflowv1connect.UnimplementedPubSubServiceHandler
}

type preAckReplayRejectingServer struct {
	ironflowv1connect.UnimplementedPubSubServiceHandler
	mu       sync.Mutex
	requests []*ironflowv1.SubscribeRequest
}

func (s *preAckReplayRejectingServer) Subscribe(
	_ context.Context,
	req *connect.Request[ironflowv1.SubscribeRequest],
	_ *connect.ServerStream[ironflowv1.SubscriptionEvent],
) error {
	s.mu.Lock()
	s.requests = append(s.requests, req.Msg)
	call := len(s.requests)
	s.mu.Unlock()
	if call == 1 {
		return connect.NewError(connect.CodeUnavailable, errors.New("transport dropped"))
	}
	return connect.NewError(
		connect.CodeInvalidArgument,
		errors.New("replay and start_after_sequence are mutually exclusive"),
	)
}

func (s *preAckReplayRejectingServer) capturedRequests() []*ironflowv1.SubscribeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*ironflowv1.SubscribeRequest(nil), s.requests...)
}

func (s rejectingPubSubServer) Subscribe(
	_ context.Context,
	req *connect.Request[ironflowv1.SubscribeRequest],
	_ *connect.ServerStream[ironflowv1.SubscriptionEvent],
) error {
	options := req.Msg.GetOptions()
	message := "missing cursor incompatibility"
	if options.StartAfterSequence == nil {
		message = "start_after_sequence was not sent"
	} else if options.Replay > 0 {
		message = "replay and start_after_sequence are mutually exclusive"
	} else if options.ConsumerGroup != "" {
		message = "start_after_sequence cannot be combined with consumer_group"
	}
	return connect.NewError(connect.CodeInvalidArgument, errors.New(message))
}

func TestGrpcSubscriptionClient_ResumeCursorRejections(t *testing.T) {
	tests := []struct {
		name    string
		options func(*uint64) *SubscribeOptions
		message string
	}{
		{
			name: "replay",
			options: func(cursor *uint64) *SubscribeOptions {
				return &SubscribeOptions{Replay: 3, StartAfterSequence: cursor}
			},
			message: "replay and start_after_sequence are mutually exclusive",
		},
		{
			name: "consumer group",
			options: func(cursor *uint64) *SubscribeOptions {
				return &SubscribeOptions{ConsumerGroup: "processors", StartAfterSequence: cursor}
			},
			message: "start_after_sequence cannot be combined with consumer_group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, handler := ironflowv1connect.NewPubSubServiceHandler(rejectingPubSubServer{})
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			server := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
			server.Start()
			defer server.Close()

			client := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{
				ServerURL: server.URL,
				Logger:    NewNoopLogger(),
			})
			defer client.Close()
			if err := client.Connect(context.Background()); err != nil {
				t.Fatalf("Connect failed: %v", err)
			}

			cursor := uint64(400)
			sub, err := client.Subscribe(context.Background(), "orders.*", tt.options(&cursor))
			if err != nil {
				t.Fatalf("Subscribe failed: %v", err)
			}
			select {
			case subErr := <-sub.Errors():
				if subErr.Code != "INVALID_ARGUMENT" || !strings.Contains(subErr.Message, tt.message) {
					t.Fatalf("subscription error = %#v, want INVALID_ARGUMENT containing %q", subErr, tt.message)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for rejection")
			}
		})
	}
}

func TestGrpcSubscriptionClient_PreservesReplayUntilCursorRequestAccepted(t *testing.T) {
	service := &preAckReplayRejectingServer{}
	path, handler := ironflowv1connect.NewPubSubServiceHandler(service)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	server.Start()
	defer server.Close()

	client := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{
		ServerURL:         server.URL,
		ReconnectDelay:    time.Millisecond,
		MaxReconnectDelay: time.Millisecond,
		ReconnectBackoff:  1,
		Logger:            NewNoopLogger(),
	})
	defer client.Close()
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	cursor := uint64(400)
	sub, err := client.Subscribe(context.Background(), "orders.*", &SubscribeOptions{
		Replay:             25,
		StartAfterSequence: &cursor,
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	select {
	case subErr := <-sub.Errors():
		if subErr.Code != "INVALID_ARGUMENT" {
			t.Fatalf("subscription error = %#v, want INVALID_ARGUMENT", subErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for rejection")
	}

	requests := service.capturedRequests()
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	if requests[1].GetOptions().GetReplay() != 25 {
		t.Fatalf("retry replay = %d, want 25 before acceptance", requests[1].GetOptions().GetReplay())
	}
	if requests[1].GetOptions().StartAfterSequence == nil ||
		requests[1].GetOptions().GetStartAfterSequence() != 400 {
		t.Fatalf("retry cursor = %v, want 400", requests[1].GetOptions().StartAfterSequence)
	}
}
