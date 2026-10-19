package ironflow

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type (
	bidiStream = connect.BidiStream[ironflowv1.SubscribeBidirectionalRequest, ironflowv1.SubscribeBidirectionalResponse]
	bidiResp   = ironflowv1.SubscribeBidirectionalResponse
)

// fakeBidi serves PubSubService.SubscribeBidirectional with a script per stream.
// n is the 1-based count of streams opened so far.
type fakeBidi struct {
	ironflowv1connect.UnimplementedPubSubServiceHandler
	script  func(ctx context.Context, n int, st *bidiStream) error
	streams atomic.Int32
}

func (f *fakeBidi) SubscribeBidirectional(ctx context.Context, st *bidiStream) error {
	return f.script(ctx, int(f.streams.Add(1)), st)
}

func startFakeBidi(t *testing.T, script func(ctx context.Context, n int, st *bidiStream) error) (*Client, *fakeBidi) {
	t.Helper()
	old := groupStreamInitialDelay
	groupStreamInitialDelay = 20 * time.Millisecond
	t.Cleanup(func() { groupStreamInitialDelay = old })

	f := &fakeBidi{script: script}
	path, h := ironflowv1connect.NewPubSubServiceHandler(f)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	srv := httptest.NewUnstartedServer(mux)
	srv.Config.Protocols = new(http.Protocols)
	srv.Config.Protocols.SetHTTP1(true)
	srv.Config.Protocols.SetUnencryptedHTTP2(true)
	srv.Start()
	t.Cleanup(srv.Close)
	return NewClient(ClientConfig{ServerURL: srv.URL, APIKey: "ifkey_test", Logger: NewNoopLogger()}), f
}

// joinAndAnnounce reads the join frame and answers joined.
func joinAndAnnounce(t *testing.T, st *bidiStream, subID string, mode ironflowv1.AckMode) error {
	t.Helper()
	req, err := st.Receive()
	if err != nil {
		return err
	}
	if req.GetJoin() == nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("first frame must be join"))
	}
	return st.Send(&bidiResp{Payload: &ironflowv1.SubscribeBidirectionalResponse_Joined{
		Joined: &ironflowv1.ConsumerGroupJoined{SubscriptionId: subID, GroupName: req.GetJoin().GetGroupName(), Pattern: "topic:orders", AckMode: mode},
	}})
}

func sendEvent(st *bidiStream, subID, eventID string) error {
	return st.Send(&bidiResp{Payload: &ironflowv1.SubscribeBidirectionalResponse_Event{
		Event: &ironflowv1.SubscriptionEvent{SubscriptionId: subID, EventId: eventID, Topic: "orders"},
	}})
}

func sendResult(st *bidiStream, ackID uint64, eventID string, code ironflowv1.AckResultCode, msg string) error {
	return st.Send(&bidiResp{Payload: &ironflowv1.SubscribeBidirectionalResponse_AckResult{
		AckResult: &ironflowv1.AckResult{AckId: ackID, EventId: eventID, Code: code, Message: msg},
	}})
}

func joinGroup(t *testing.T, c *Client, ctx context.Context) *AckableSubscription {
	t.Helper()
	sub, err := c.JoinConsumerGroup(ctx, "workers", WithJoinTransport("grpc"))
	if err != nil {
		t.Fatalf("JoinConsumerGroup: %v", err)
	}
	t.Cleanup(sub.Unsubscribe)
	return sub
}

func nextEvent(t *testing.T, sub *AckableSubscription) *SubscriptionEvent {
	t.Helper()
	select {
	case ev, ok := <-sub.Events():
		if !ok {
			t.Fatal("events channel closed")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("no event arrived")
	}
	return nil
}

// serveOneEvent joins MANUAL, sends one event, and hands every later frame to onAck.
func serveOneEvent(t *testing.T, onAck func(st *bidiStream, ack *ironflowv1.SubscriptionAck) error) func(context.Context, int, *bidiStream) error {
	return func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		if err := sendEvent(st, "member-1", "evt-1"); err != nil {
			return err
		}
		for {
			req, err := st.Receive()
			if err != nil {
				return nil
			}
			if err := onAck(st, req.GetAck()); err != nil {
				return err
			}
		}
	}
}

func TestGroupStream_AckWaitsForResult(t *testing.T) {
	c, _ := startFakeBidi(t, serveOneEvent(t, func(st *bidiStream, ack *ironflowv1.SubscriptionAck) error {
		time.Sleep(50 * time.Millisecond)
		return sendResult(st, ack.GetAckId(), ack.GetEventId(), ironflowv1.AckResultCode_ACK_RESULT_CODE_OK, "")
	}))
	sub := joinGroup(t, c, context.Background())
	ev := nextEvent(t, sub)

	start := time.Now()
	if err := sub.Ack(ev.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if d := time.Since(start); d < 50*time.Millisecond {
		t.Fatalf("Ack returned after %v, before the result", d)
	}
}

func TestGroupStream_AckErrorCarriesCode(t *testing.T) {
	c, _ := startFakeBidi(t, serveOneEvent(t, func(st *bidiStream, ack *ironflowv1.SubscriptionAck) error {
		return sendResult(st, ack.GetAckId(), ack.GetEventId(), ironflowv1.AckResultCode_ACK_RESULT_CODE_INTERNAL, "nats down")
	}))
	sub := joinGroup(t, c, context.Background())
	ev := nextEvent(t, sub)

	err := sub.Ack(ev.ID)
	var ackErr *AckError
	if !errors.As(err, &ackErr) {
		t.Fatalf("Ack error = %v, want *AckError", err)
	}
	if ackErr.Code != "internal" || ackErr.Message != "nats down" || ackErr.EventID != "evt-1" {
		t.Fatalf("AckError = %+v", ackErr)
	}
}

func TestGroupStream_ResultsCorrelateByAckID(t *testing.T) {
	var first *ironflowv1.SubscriptionAck
	saw1 := make(chan struct{})
	c, _ := startFakeBidi(t, serveOneEvent(t, func(st *bidiStream, ack *ironflowv1.SubscriptionAck) error {
		if first == nil {
			first = ack
			close(saw1)
			return nil
		}
		// Answer the second ack first, then the first: the same event ID throughout.
		if err := sendResult(st, ack.GetAckId(), ack.GetEventId(), ironflowv1.AckResultCode_ACK_RESULT_CODE_INTERNAL, "nak failed"); err != nil {
			return err
		}
		return sendResult(st, first.GetAckId(), first.GetEventId(), ironflowv1.AckResultCode_ACK_RESULT_CODE_OK, "")
	}))
	sub := joinGroup(t, c, context.Background())
	ev := nextEvent(t, sub)

	ackDone := make(chan error, 1)
	go func() { ackDone <- sub.Ack(ev.ID) }()
	<-saw1
	nakErr := sub.Nak(ev.ID, time.Second)

	var ackErr *AckError
	if !errors.As(nakErr, &ackErr) || ackErr.Message != "nak failed" {
		t.Fatalf("Nak error = %v, want its own INTERNAL result", nakErr)
	}
	select {
	case err := <-ackDone:
		if err != nil {
			t.Fatalf("Ack = %v, want nil: it must not take the Nak's result", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Ack never returned")
	}
}

func TestGroupStream_PendingAcksFailOnStreamEnd(t *testing.T) {
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if n > 1 {
			<-ctx.Done()
			return nil
		}
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		if err := sendEvent(st, "member-1", "evt-1"); err != nil {
			return err
		}
		if _, err := st.Receive(); err != nil { // the ack, never answered
			return err
		}
		return nil // end the stream
	})
	sub := joinGroup(t, c, context.Background())
	ev := nextEvent(t, sub)

	done := make(chan error, 1)
	go func() { done <- sub.Ack(ev.ID) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrStaleAck) {
			t.Fatalf("Ack = %v, want an error wrapping ErrStaleAck: the stream ended without a result", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Ack still pending 1s after the stream ended")
	}
}

// An ack whose Send fails because the stream ended, before the reader marks
// the conn ended, must still be a stale ack.
func TestGroupStream_AckSendFailureOnEndedStreamIsStale(t *testing.T) {
	sawEvent := make(chan struct{})
	endStream := make(chan struct{})
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if n > 1 {
			<-ctx.Done()
			return nil
		}
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		if err := sendEvent(st, "member-1", "evt-1"); err != nil {
			return err
		}
		close(sawEvent)
		<-endStream
		return nil // end the stream
	})
	sub := joinGroup(t, c, context.Background())
	ev := nextEvent(t, sub)
	<-sawEvent
	sc := sub.acker.(*groupStream).current()

	// Park the ack between its ended check and its Send.
	sc.sendMu.Lock()
	done := make(chan error, 1)
	go func() { done <- sc.settle(ev.ID, ironflowv1.AckType_ACK_TYPE_ACK, 0) }()
	waitUntil(t, "ack registered", func() bool {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		return len(sc.pending) == 1
	})
	close(endStream)
	waitUntil(t, "stream marked ended", func() bool {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		return sc.ended
	})
	sc.sendMu.Unlock()

	select {
	case err := <-done:
		if !errors.Is(err, ErrStaleAck) {
			t.Fatalf("settle = %v, want an error wrapping ErrStaleAck", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("settle did not return")
	}
}

func TestGroupStream_StaleAckAfterReconnect(t *testing.T) {
	gotEvent := make(chan struct{})
	acksOnStream2 := make(chan *ironflowv1.SubscriptionAck, 4)
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if n == 1 {
			if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
				return err
			}
			if err := sendEvent(st, "member-1", "evt-1"); err != nil {
				return err
			}
			<-gotEvent
			return connect.NewError(connect.CodeUnavailable, errors.New("dropped"))
		}
		if err := joinAndAnnounce(t, st, "member-2", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		for {
			req, err := st.Receive()
			if err != nil {
				return nil
			}
			if req.GetAck() != nil {
				acksOnStream2 <- req.GetAck()
			}
		}
	})
	sub := joinGroup(t, c, context.Background())
	ev := nextEvent(t, sub)
	close(gotEvent)

	deadline := time.Now().Add(5 * time.Second)
	for sub.CurrentID() != "member-2" {
		if time.Now().After(deadline) {
			t.Fatal("never rejoined as member-2")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := sub.Ack(ev.ID); !errors.Is(err, ErrStaleAck) {
		t.Fatalf("Ack = %v, want ErrStaleAck", err)
	}
	select {
	case ack := <-acksOnStream2:
		t.Fatalf("stale ack reached the new stream: %+v", ack)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestGroupStream_NotFoundStopsWithoutRetry(t *testing.T) {
	c, f := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if _, err := st.Receive(); err != nil {
			return err
		}
		return connect.NewError(connect.CodeNotFound, errors.New("no such group"))
	})
	_, err := c.JoinConsumerGroup(context.Background(), "workers", WithJoinTransport("grpc"))
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("JoinConsumerGroup error = %v, want NotFound", err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := f.streams.Load(); got != 1 {
		t.Fatalf("server saw %d joins, want exactly 1", got)
	}
}

func TestGroupStream_AutoGroupTracksNothing(t *testing.T) {
	const total = 1000
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_AUTO); err != nil {
			return err
		}
		for i := 0; i < total; i++ {
			if err := sendEvent(st, "member-1", fmt.Sprintf("evt-%d", i)); err != nil {
				return err
			}
		}
		for {
			if _, err := st.Receive(); err != nil {
				return nil
			}
		}
	})
	sub := joinGroup(t, c, context.Background())
	for i := 0; i < total; i++ {
		nextEvent(t, sub)
	}
	g := sub.acker.(*groupStream)
	if n := g.current().deliveredCount(); n != 0 {
		t.Fatalf("AUTO group tracked %d delivered IDs, want 0", n)
	}
}

func TestGroupStream_UnsubscribeHalfCloses(t *testing.T) {
	sawEOF := make(chan error, 1)
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		_, err := st.Receive()
		sawEOF <- err
		return nil
	})
	sub := joinGroup(t, c, context.Background())
	sub.Unsubscribe()
	select {
	case err := <-sawEOF:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("server Receive = %v, want io.EOF (a half-close)", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server never saw the half-close")
	}
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Fatal("unexpected event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("events channel not closed after Unsubscribe")
	}
}

func TestGroupStream_CtxCancelTearsDownStream(t *testing.T) {
	handlerDone := make(chan struct{})
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		defer close(handlerDone)
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		if err := sendEvent(st, "member-1", "evt-1"); err != nil {
			return err
		}
		for { // idle once the ack is answered: ends only on a half-close or a reset
			req, err := st.Receive()
			if err != nil {
				return err
			}
			if ack := req.GetAck(); ack != nil {
				if err := sendResult(st, ack.GetAckId(), ack.GetEventId(), ironflowv1.AckResultCode_ACK_RESULT_CODE_OK, ""); err != nil {
					return err
				}
			}
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	sub := joinGroup(t, c, ctx)
	// A full ack round trip proves the request body is flushed and idle. A cancel
	// before that is seen by the transport and would pass without the teardown.
	if err := sub.Ack(nextEvent(t, sub).ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	cancel()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("server stream still open 1s after the join ctx was cancelled")
	}
	select {
	case e := <-sub.Errors():
		if e == nil || e.Code != "CANCELED" {
			t.Fatalf("Errors() = %+v, want CANCELED", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the ctx end did not reach Errors()")
	}
}

type countingLogger struct {
	Logger
	warns atomic.Int32
}

func (l *countingLogger) Warn(string, ...any) { l.warns.Add(1) }

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestGroupStream_NotFoundOnRejoinIsTerminal(t *testing.T) {
	c, f := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if n == 1 {
			if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
				return err
			}
			return connect.NewError(connect.CodeUnavailable, errors.New("dropped"))
		}
		if _, err := st.Receive(); err != nil {
			return err
		}
		return connect.NewError(connect.CodeNotFound, errors.New("group deleted"))
	})
	sub := joinGroup(t, c, context.Background())

	select {
	case e := <-sub.Errors():
		if e.Code != "NOT_FOUND" {
			t.Fatalf("Errors() code = %q, want NOT_FOUND", e.Code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no terminal error reached Errors()")
	}
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Fatal("unexpected event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Events() not closed after the terminal error")
	}
	time.Sleep(150 * time.Millisecond) // a retry would show within several 20 ms delays
	if got := f.streams.Load(); got != 2 {
		t.Fatalf("server saw %d join attempts, want exactly 2", got)
	}
	if err := sub.Ack("evt-1"); !errors.Is(err, ErrSubscriptionClosed) || errors.Is(err, ErrStaleAck) {
		t.Fatalf("Ack after a terminal error = %v, want ErrSubscriptionClosed", err)
	}
}

// A rejoin that lands on an older node (a rolling upgrade) names the minimum
// server version, as the first join does.
func TestGroupStream_UnimplementedOnRejoinNamesMinVersion(t *testing.T) {
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if n == 1 {
			if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
				return err
			}
			return connect.NewError(connect.CodeUnavailable, errors.New("dropped"))
		}
		if _, err := st.Receive(); err != nil {
			return err
		}
		return connect.NewError(connect.CodeUnimplemented, errors.New("no such method"))
	})
	sub := joinGroup(t, c, context.Background())

	select {
	case e := <-sub.Errors():
		if !strings.Contains(e.Message, "0.48.0") {
			t.Fatalf("Errors() message = %q, want it to name server 0.48.0", e.Message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no terminal error reached Errors()")
	}
}

func TestGroupStream_AckAfterUnsubscribeIsClosed(t *testing.T) {
	c, _ := startFakeBidi(t, serveOneEvent(t, func(*bidiStream, *ironflowv1.SubscriptionAck) error { return nil }))
	sub := joinGroup(t, c, context.Background())
	ev := nextEvent(t, sub)
	sub.Unsubscribe()
	for name, err := range map[string]error{
		"Ack":  sub.Ack(ev.ID),
		"Nak":  sub.Nak(ev.ID, time.Second),
		"Term": sub.Term(ev.ID),
	} {
		if !errors.Is(err, ErrSubscriptionClosed) || errors.Is(err, ErrStaleAck) {
			t.Fatalf("%s after Unsubscribe = %v, want ErrSubscriptionClosed", name, err)
		}
	}
}

// A MANUAL consumer that acks from its Events loop must get its result even
// when more events are queued than the Events channel holds.
func TestGroupStream_ManualAckFromEventsLoopDoesNotDeadlock(t *testing.T) {
	const total = 250 // the Events channel holds 100
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		for i := 0; i < total; i++ {
			if err := sendEvent(st, "member-1", fmt.Sprintf("evt-%d", i)); err != nil {
				return err
			}
		}
		for {
			req, err := st.Receive()
			if err != nil {
				return nil
			}
			if ack := req.GetAck(); ack != nil {
				if err := sendResult(st, ack.GetAckId(), ack.GetEventId(), ironflowv1.AckResultCode_ACK_RESULT_CODE_OK, ""); err != nil {
					return err
				}
			}
		}
	})
	sub := joinGroup(t, c, context.Background())
	ev := nextEvent(t, sub) // the rest stay unread

	done := make(chan error, 1)
	go func() { done <- sub.Ack(ev.ID) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Ack blocked: the reader stalled on a full Events channel")
	}
}

func TestGroupStream_AutoBackpressuresAndAckIsNoop(t *testing.T) {
	const total = 200
	var sent atomic.Int32
	frames := make(chan *ironflowv1.SubscribeBidirectionalRequest, 16)
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_AUTO); err != nil {
			return err
		}
		go func() {
			for {
				req, err := st.Receive()
				if err != nil {
					return
				}
				select {
				case frames <- req:
				default:
				}
			}
		}()
		// Random bytes: the stream gzips, and a repeated byte would shrink to nothing.
		raw := make([]byte, 192<<10)
		_, _ = rand.Read(raw)
		data, _ := structpb.NewStruct(map[string]any{"p": base64.StdEncoding.EncodeToString(raw)})
		for i := 0; i < total; i++ {
			if err := st.Send(&bidiResp{Payload: &ironflowv1.SubscribeBidirectionalResponse_Event{
				Event: &ironflowv1.SubscriptionEvent{SubscriptionId: "member-1", EventId: fmt.Sprintf("evt-%d", i), Data: data},
			}}); err != nil {
				return nil
			}
			sent.Add(1)
		}
		<-ctx.Done()
		return nil
	})
	log := &countingLogger{Logger: NewNoopLogger()}
	c.logger = log
	sub := joinGroup(t, c, context.Background())

	// Nobody reads Events(): HTTP/2 flow control must stop the server well short of total.
	var last int32 = -1
	waitUntil(t, "the server to block on flow control", func() bool {
		cur := sent.Load()
		stable := cur == last
		last = cur
		time.Sleep(100 * time.Millisecond)
		return stable
	})
	if got := sent.Load(); got >= total {
		t.Fatalf("server sent all %d events to a client that read none: no backpressure", got)
	}

	if err := sub.Ack("evt-0"); err != nil {
		t.Fatalf("Ack on an AUTO group = %v, want nil", err)
	}
	if err := sub.Nak("evt-0", time.Second); err != nil {
		t.Fatalf("Nak on an AUTO group = %v, want nil", err)
	}
	select {
	case req := <-frames:
		t.Fatalf("an AUTO ack reached the server: %+v", req)
	case <-time.After(100 * time.Millisecond):
	}
	if got := log.warns.Load(); got != 1 {
		t.Fatalf("logged %d warnings, want exactly 1 per subscription", got)
	}

	for i := 0; i < total; i++ { // nothing was lost
		if ev := nextEvent(t, sub); ev.ID != fmt.Sprintf("evt-%d", i) {
			t.Fatalf("event %d = %s", i, ev.ID)
		}
	}
}

func TestGroupStream_UnsubscribeDuringReconnectLeavesNoMember(t *testing.T) {
	joinSeen := make(chan struct{})
	gate := make(chan struct{})
	stream2End := make(chan error, 1)
	c, f := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		switch n {
		case 1:
			if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
				return err
			}
			return connect.NewError(connect.CodeUnavailable, errors.New("dropped"))
		case 2:
			if _, err := st.Receive(); err != nil {
				return err
			}
			close(joinSeen)
			<-gate // the client is mid-reconnect, waiting for joined
			if err := st.Send(&bidiResp{Payload: &ironflowv1.SubscribeBidirectionalResponse_Joined{
				Joined: &ironflowv1.ConsumerGroupJoined{SubscriptionId: "member-2", Pattern: "topic:orders", AckMode: ironflowv1.AckMode_ACK_MODE_MANUAL},
			}}); err != nil {
				stream2End <- err
				return nil
			}
			_, err := st.Receive() // a live member would block here
			stream2End <- err
			return nil
		}
		<-ctx.Done()
		return nil
	})
	sub := joinGroup(t, c, context.Background())
	<-joinSeen
	sub.Unsubscribe()
	close(gate)
	select {
	case err := <-stream2End:
		if err == nil {
			t.Fatal("stream 2 ended without an error from the client")
		}
	case <-time.After(time.Second):
		t.Fatal("the member that joined during Unsubscribe is still live")
	}
	time.Sleep(100 * time.Millisecond)
	if got := f.streams.Load(); got != 2 {
		t.Fatalf("server saw %d streams, want 2: Unsubscribe must stop the reconnect", got)
	}
}

// A server that accepts the join and hangs up must see the delay grow: only a
// frame past joined resets it.
func TestGroupStream_ReconnectDelayGrowsWithoutAcceptedFrames(t *testing.T) {
	var mu sync.Mutex
	var starts []time.Time
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		return nil // a clean end
	})
	groupStreamInitialDelay = 50 * time.Millisecond // restored by startFakeBidi's cleanup
	joinGroup(t, c, context.Background())
	waitUntil(t, "four streams", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(starts) >= 4 // starts, not f.streams: the fake counts a stream before it appends
	})

	mu.Lock()
	defer mu.Unlock()
	// Waits are 50, 75, 112 ms; timers never fire early, so these are floors.
	if g := starts[2].Sub(starts[1]); g < 70*time.Millisecond {
		t.Fatalf("second reconnect gap = %v, want >= 70ms (delay grew to 75ms)", g)
	}
	if g := starts[3].Sub(starts[2]); g < 105*time.Millisecond {
		t.Fatalf("third reconnect gap = %v, want >= 105ms (delay grew to 112ms)", g)
	}
}

func TestGroupStream_ReconnectDropsQueuedEventsOfEndedStream(t *testing.T) {
	const total = 250 // 100 fill Events(), one waits in the pump, the rest queue
	drop := make(chan struct{})
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if n == 1 {
			if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
				return err
			}
			for i := 0; i < total; i++ {
				if err := sendEvent(st, "member-1", fmt.Sprintf("evt-%d", i)); err != nil {
					return err
				}
			}
			<-drop
			return connect.NewError(connect.CodeUnavailable, errors.New("dropped"))
		}
		if err := joinAndAnnounce(t, st, "member-2", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		if err := sendEvent(st, "member-2", "fresh"); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	})
	sub := joinGroup(t, c, context.Background())
	g := sub.acker.(*groupStream)
	waitUntil(t, "the queue to fill", func() bool {
		g.qmu.Lock()
		defer g.qmu.Unlock()
		return len(g.queue) >= total-101
	})
	close(drop)
	waitUntil(t, "the rejoin", func() bool { return sub.CurrentID() == "member-2" })

	for {
		ev := nextEvent(t, sub)
		if ev.ID == "fresh" {
			return
		}
		var i int
		if _, err := fmt.Sscanf(ev.ID, "evt-%d", &i); err != nil || i >= 100 {
			t.Fatalf("got %s: a queued event of the ended stream was delivered", ev.ID)
		}
	}
}

func shorten(t *testing.T, v *time.Duration, d time.Duration) {
	t.Helper()
	old := *v
	*v = d
	t.Cleanup(func() { *v = old })
}

func TestSubscriptionEventFromProto_DataValue(t *testing.T) {
	for _, tc := range []struct {
		name string
		v    *structpb.Value
		want string
	}{
		{"string", structpb.NewStringValue("hello"), `"hello"`},
		{"list", structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewNumberValue(1), structpb.NewNumberValue(2)}}), `[1,2]`},
		{"number", structpb.NewNumberValue(42), `42`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := subscriptionEventFromProto(&ironflowv1.SubscriptionEvent{EventId: "e", DataValue: tc.v})
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.ReplaceAll(string(ev.Data), " ", ""); got != tc.want {
				t.Fatalf("Data = %s, want %s", ev.Data, tc.want)
			}
		})
	}
}

func TestNakDelayMs_RefusesOutOfRange(t *testing.T) {
	if ms, err := nakDelayMs("e", 1500*time.Millisecond); err != nil || ms != 1500 {
		t.Fatalf("nakDelayMs(1.5s) = %d, %v", ms, err)
	}
	for _, d := range []time.Duration{-time.Second, 30 * 24 * time.Hour, (1<<32)*time.Millisecond + time.Second} {
		_, err := nakDelayMs("e", d)
		var ackErr *AckError
		if !errors.As(err, &ackErr) || ackErr.Code != "invalid_argument" {
			t.Fatalf("nakDelayMs(%s) error = %v, want invalid_argument", d, err)
		}
	}
}

// A NAK's copy is settled even when the redelivery goes to another member, and
// two copies delivered on one stream each take an ack.
func TestGroupStream_DeliveredCountsCopies(t *testing.T) {
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		for _, id := range []string{"evt-1", "evt-2", "evt-2"} {
			if err := sendEvent(st, "member-1", id); err != nil {
				return err
			}
		}
		for {
			req, err := st.Receive()
			if err != nil {
				return nil
			}
			ack := req.GetAck()
			if err := sendResult(st, ack.GetAckId(), ack.GetEventId(), ironflowv1.AckResultCode_ACK_RESULT_CODE_OK, ""); err != nil {
				return err
			}
		}
	})
	sub := joinGroup(t, c, context.Background())
	g := sub.acker.(*groupStream)
	for range 3 {
		nextEvent(t, sub)
	}
	if err := sub.Nak("evt-1", 0); err != nil {
		t.Fatalf("Nak: %v", err)
	}
	for i := range 2 {
		if err := sub.Ack("evt-2"); err != nil {
			t.Fatalf("Ack of copy %d: %v", i+1, err)
		}
	}
	if n := g.current().deliveredCount(); n != 0 {
		t.Fatalf("delivered set holds %d IDs after every copy settled, want 0", n)
	}
	if err := sub.Ack("evt-2"); !errors.Is(err, ErrStaleAck) {
		t.Fatalf("third Ack = %v, want ErrStaleAck", err)
	}
}

func TestGroupStream_DeliveredPrunesOldEntries(t *testing.T) {
	shorten(t, &deliveryMaxAge, 100*time.Millisecond)
	next := make(chan struct{})
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		for _, id := range []string{"evt-1", "evt-2"} {
			if err := sendEvent(st, "member-1", id); err != nil {
				return err
			}
		}
		<-next
		if err := sendEvent(st, "member-1", "evt-3"); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	})
	sub := joinGroup(t, c, context.Background())
	g := sub.acker.(*groupStream)
	nextEvent(t, sub)
	nextEvent(t, sub)
	time.Sleep(150 * time.Millisecond)
	close(next)
	nextEvent(t, sub)
	if n := g.current().deliveredCount(); n != 1 {
		t.Fatalf("delivered set holds %d IDs, want 1 (evt-3; the old two pruned)", n)
	}
}

func TestGroupStream_IdleStreamReconnects(t *testing.T) {
	shorten(t, &groupStreamIdleTimeout, 200*time.Millisecond)
	c, f := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, fmt.Sprintf("member-%d", n), ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		<-ctx.Done() // a half-open connection: no frame, no end
		return nil
	})
	sub := joinGroup(t, c, context.Background())
	waitUntil(t, "a reconnect after the idle timeout", func() bool { return f.streams.Load() >= 2 })
	select {
	case e := <-sub.Errors():
		t.Fatalf("idle reconnect reported %+v", e)
	default:
	}
}

// A server that stops reading fills the HTTP/2 window, and Send blocks. The ack
// returns at ackTimeout and the stream reconnects.
func TestGroupStream_BlockedAckSendTimesOut(t *testing.T) {
	shorten(t, &ackTimeout, 300*time.Millisecond)
	bigID := strings.Repeat("e", 8<<20) // more than any HTTP/2 window
	c, f := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		if n == 1 {
			if err := sendEvent(st, "member-1", bigID); err != nil {
				return err
			}
		}
		<-ctx.Done() // never reads the ack
		return nil
	})
	sub := joinGroup(t, c, context.Background())
	nextEvent(t, sub)
	done := make(chan error, 1)
	go func() { done <- sub.Ack(bigID) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "not sent within") {
			t.Fatalf("Ack = %v, want a send timeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Ack still blocked in Send")
	}
	waitUntil(t, "a reconnect after the blocked send", func() bool { return f.streams.Load() >= 2 })
}
