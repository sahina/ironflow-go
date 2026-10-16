package ironflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

// ackEventServer sends one consumer-group event, then holds the stream open,
// and records every AckEvent call with its headers.
type ackEventServer struct {
	ironflowv1connect.UnimplementedPubSubServiceHandler
	acks          chan *connect.Request[ironflowv1.AckEventRequest]
	streamHeaders chan http.Header
}

func newAckEventServer() *ackEventServer {
	return &ackEventServer{
		acks:          make(chan *connect.Request[ironflowv1.AckEventRequest], 10),
		streamHeaders: make(chan http.Header, 1),
	}
}

func (s *ackEventServer) Subscribe(
	ctx context.Context,
	req *connect.Request[ironflowv1.SubscribeRequest],
	stream *connect.ServerStream[ironflowv1.SubscriptionEvent],
) error {
	select {
	case s.streamHeaders <- req.Header().Clone():
	default:
	}
	if err := stream.Send(&ironflowv1.SubscriptionEvent{
		SubscriptionId: "consumer-abc",
		EventId:        "evt-1",
		Topic:          "orders.created",
	}); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *ackEventServer) AckEvent(
	_ context.Context,
	req *connect.Request[ironflowv1.AckEventRequest],
) (*connect.Response[ironflowv1.AckEventResponse], error) {
	s.acks <- req
	return connect.NewResponse(&ironflowv1.AckEventResponse{}), nil
}

func startAckEventServer(t *testing.T, svc ironflowv1connect.PubSubServiceHandler) string {
	t.Helper()
	path, handler := ironflowv1connect.NewPubSubServiceHandler(svc)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	server.Start()
	t.Cleanup(server.Close)
	return server.URL
}

func TestGrpcAckableSubscription_AcksThroughAckEvent(t *testing.T) {
	svc := newAckEventServer()
	client := newGrpcSubscriptionClient(GrpcSubscriptionClientConfig{
		ServerURL: startAckEventServer(t, svc),
		Logger:    NewNoopLogger(),
	}, "ifkey_test")
	defer client.Close()
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	sub, err := client.SubscribeAckable(context.Background(), "topic:orders", &SubscribeOptions{
		ConsumerGroup: "workers",
		Namespace:     "default",
	})
	if err != nil {
		t.Fatalf("SubscribeAckable: %v", err)
	}
	select {
	case <-sub.Events():
	case <-time.After(5 * time.Second):
		t.Fatal("no event arrived")
	}

	if err := sub.Ack("evt-1"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := sub.Nak("evt-1", 2*time.Second); err != nil {
		t.Fatalf("Nak: %v", err)
	}
	if err := sub.Term("evt-1"); err != nil {
		t.Fatalf("Term: %v", err)
	}

	want := []struct {
		ackType ironflowv1.AckType
		delayMs int32
	}{
		{ironflowv1.AckType_ACK_TYPE_ACK, 0},
		{ironflowv1.AckType_ACK_TYPE_NAK, 2000},
		{ironflowv1.AckType_ACK_TYPE_TERM, 0},
	}
	for i, w := range want {
		var req *connect.Request[ironflowv1.AckEventRequest]
		select {
		case req = <-svc.acks:
		case <-time.After(5 * time.Second):
			t.Fatalf("ack %d never reached AckEvent", i)
		}
		m := req.Msg
		if m.GroupName != "workers" || m.Namespace != "default" || m.ConsumerId != "consumer-abc" || m.EventId != "evt-1" {
			t.Fatalf("ack %d = %+v, want group workers, namespace default, consumer consumer-abc, event evt-1", i, m)
		}
		if m.AckType != w.ackType || m.RedeliverDelayMs != w.delayMs {
			t.Fatalf("ack %d type/delay = %v/%d, want %v/%d", i, m.AckType, m.RedeliverDelayMs, w.ackType, w.delayMs)
		}
		if got := req.Header().Get("Authorization"); got != "Bearer ifkey_test" {
			t.Fatalf("ack %d Authorization = %q, want bearer key", i, got)
		}
	}
}

func TestGrpcJoinSubscription_AcksAndUnsubscribesWithoutPanic(t *testing.T) {
	svc := newAckEventServer()
	client := newGrpcSubscriptionClient(GrpcSubscriptionClientConfig{
		ServerURL: startAckEventServer(t, svc),
		Logger:    NewNoopLogger(),
	}, "ifkey_test")
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	grpcSub, err := client.SubscribeAckable(context.Background(), "topic:orders", &SubscribeOptions{
		ConsumerGroup: "workers",
	})
	if err != nil {
		t.Fatalf("SubscribeAckable: %v", err)
	}
	sub := grpcJoinSubscription(client, grpcSub)

	select {
	case <-sub.Events():
	case <-time.After(5 * time.Second):
		t.Fatal("no event arrived")
	}
	if got := sub.CurrentID(); got != sub.ID {
		t.Fatalf("CurrentID = %q, want %q", got, sub.ID)
	}
	if err := sub.Ack("evt-1"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	select {
	case req := <-svc.acks:
		if req.Msg.ConsumerId != "consumer-abc" || req.Msg.GroupName != "workers" {
			t.Fatalf("ack = %+v", req.Msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ack never reached AckEvent")
	}

	sub.Unsubscribe()
	sub.Unsubscribe() // a second call must be a no-op, not a double close
	if client.IsConnected() {
		t.Fatal("Unsubscribe must close the grpc client it owns")
	}
	// After unsubscribe an ack may fail on the server, but it must not panic.
	_ = sub.Ack("evt-1")
}

func TestGrpcSubscriptionClient_StreamCarriesBearerKey(t *testing.T) {
	svc := newAckEventServer()
	client := newGrpcSubscriptionClient(GrpcSubscriptionClientConfig{
		ServerURL: startAckEventServer(t, svc),
		Logger:    NewNoopLogger(),
	}, "ifkey_stream")
	defer client.Close()
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := client.Subscribe(context.Background(), "topic:orders", nil); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	select {
	case h := <-svc.streamHeaders:
		if got := h.Get("Authorization"); got != "Bearer ifkey_stream" {
			t.Fatalf("stream Authorization = %q, want bearer key", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream never opened")
	}
}

func TestGrpcSubscriptionClient_ConnectionCallbackFiresOnce(t *testing.T) {
	client := newGrpcSubscriptionClient(GrpcSubscriptionClientConfig{
		ServerURL: "http://127.0.0.1:0",
		Logger:    NewNoopLogger(),
	}, "")
	defer client.Close()
	var calls []bool
	client.SetConnectionCallback(func(connected bool) { calls = append(calls, connected) })

	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("second Connect: %v", err)
	}
	if len(calls) != 1 || !calls[0] {
		t.Fatalf("callback calls = %v, want exactly [true]", calls)
	}
}

func TestNextSubscriptionReconnectDelay(t *testing.T) {
	cases := []struct {
		name             string
		current, maximum time.Duration
		backoff          float64
		want             time.Duration
	}{
		{"grows by backoff", time.Second, 30 * time.Second, 1.5, 1500 * time.Millisecond},
		{"caps at maximum", 25 * time.Second, 30 * time.Second, 2, 30 * time.Second},
		{"stays at maximum", 30 * time.Second, 30 * time.Second, 1.5, 30 * time.Second},
		{"backoff of 1 still grows", time.Second, 30 * time.Second, 1, time.Second + time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextSubscriptionReconnectDelay(tc.current, tc.maximum, tc.backoff); got != tc.want {
				t.Fatalf("next(%v, %v, %v) = %v, want %v", tc.current, tc.maximum, tc.backoff, got, tc.want)
			}
		})
	}
}
