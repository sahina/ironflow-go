package ironflow

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

func TestIssue3888ShortHTTP2BodyEnds(t *testing.T) {
	shorten(t, &groupStreamInitialDelay, 20*time.Millisecond)
	frame, err := proto.Marshal(joinedFrame())
	if err != nil {
		t.Fatal(err)
	}
	body := envelope(frame)
	var attempts atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("protocol = %s, want HTTP/2", r.Proto)
		}
		attempts.Add(1)
		var prefix [5]byte
		if _, err := io.ReadFull(r.Body, prefix[:]); err != nil {
			return
		}
		if _, err := io.CopyN(io.Discard, r.Body, int64(binary.BigEndian.Uint32(prefix[1:]))); err != nil {
			return
		}
		w.Header().Set("Content-Type", "application/connect+proto")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)+1))
		_, _ = w.Write(body)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	setDefaultTransport(t, nonTransportTrusting(srv))
	c := NewClient(ClientConfig{ServerURL: srv.URL, APIKey: "ifkey_test", Logger: NewNoopLogger()})
	sub := joinGroup(t, c, context.Background())
	select {
	case err := <-sub.Errors():
		if err == nil {
			t.Fatal("Errors() closed without the short-body error")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("short HTTP/2 body did not end the subscription; attempts = %d", attempts.Load())
	}
	if got := attempts.Load(); got != 4 {
		t.Fatalf("attempts = %d, want 4", got)
	}
}

func TestSubscriptions_AmbiguousBodyBudget(t *testing.T) {
	for _, group := range []bool{true, false} {
		for _, tc := range []struct {
			name  string
			steps []string
			want  int32
		}{
			{"joined", []string{"short"}, 4},
			{"initial open", []string{"empty"}, 4},
			{"initial recovery", []string{"empty", "short", "short", "short"}, 4},
			{"heartbeat", []string{"heartbeat"}, 4},
			{"almost healthy", []string{"short", "short", "healthy", "short"}, 4},
			{"slow reopen", []string{"short", "slowempty", "slowempty", "slowempty"}, 4},
			{"reopen", []string{"short", "empty", "empty", "empty"}, 4},
			{"one event", []string{"event"}, 4},
			{"other failure", []string{"short", "unavailable", "short", "short", "short"}, 5},
			{"healthy reset", []string{"short", "short", "healthy", "short", "short", "short"}, 6},
			{"clean end", []string{"short", "clean", "short", "short", "short"}, 5},
		} {
			if !group && (tc.name == "clean end" || tc.name == "heartbeat") {
				continue
			}
			t.Run(fmt.Sprintf("group=%v/%s", group, tc.name), func(t *testing.T) {
				shorten(t, &groupStreamInitialDelay, time.Millisecond)
				var clock atomic.Int64
				oldNow := subscriptionRetryNow
				subscriptionRetryNow = func() time.Time { return time.Unix(1, clock.Load()) }
				t.Cleanup(func() { subscriptionRetryNow = oldNow })
				release := make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				t.Cleanup(unblock)
				var attempts atomic.Int32
				srv := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					n := attempts.Add(1)
					step := tc.steps[min(int(n)-1, len(tc.steps)-1)]
					if r.ProtoMajor != 2 {
						t.Errorf("protocol = %s", r.Proto)
					}
					if group {
						readJoinBody(r)
					}
					if step == "unavailable" {
						http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
						return
					}
					body := subscriptionTestBody(t, group, n, step != "short" && step != "heartbeat")
					if step == "heartbeat" {
						b, err := proto.Marshal(&bidiResp{Payload: &ironflowv1.SubscribeBidirectionalResponse_Heartbeat{Heartbeat: &ironflowv1.StreamHeartbeat{}}})
						if err != nil {
							t.Error(err)
						}
						body = append(body, envelope(b)...)
					}
					if step == "empty" || step == "slowempty" {
						if step == "slowempty" {
							clock.Add(int64(time.Minute))
						}
						body = nil
					}
					if group {
						w.Header().Set("Content-Type", "application/connect+proto")
					} else {
						w.Header().Set("Content-Type", cursorClientContentType)
					}
					if step == "clean" {
						body = append(body, []byte{2, 0, 0, 0, 2, '{', '}'}...)
					} else {
						w.Header().Set("Content-Length", strconv.Itoa(len(body)+1))
					}
					_, _ = w.Write(body)
					if step == "healthy" {
						w.(http.Flusher).Flush()
						select {
						case <-release:
						case <-r.Context().Done():
						}
					}
				}))
				t.Cleanup(srv.Close)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				events, errs, initialErr := startBudgetSubscription(t, ctx, srv.URL, group, NewNoopLogger())
				if initialErr != nil {
					if !group || tc.name != "initial open" {
						t.Fatal(initialErr)
					}
					if !errors.Is(initialErr, io.ErrUnexpectedEOF) {
						t.Fatalf("cause lost: %v", initialErr)
					}
					assertBudgetMessage(t, initialErr.Error())
				} else {
					var terminal *SubscriptionError
					for terminal == nil {
						select {
						case ev, ok := <-events:
							if !ok {
								events = nil
								continue
							}
							if (tc.name == "healthy reset" || tc.name == "almost healthy") && ev.ID == "event-3" {
								elapsed := 60 * time.Second
								if tc.name == "almost healthy" {
									elapsed = 59 * time.Second
								}
								clock.Store(int64(elapsed))
								unblock()
							}
						case e := <-errs:
							if e == nil || e.Retrying {
								t.Fatalf("terminal error = %+v", e)
							}
							terminal = e
						case <-ctx.Done():
							t.Fatal("subscription did not report exhaustion")
						}
					}
					assertBudgetMessage(t, terminal.Message)
				}
				if got := attempts.Load(); got != tc.want {
					t.Fatalf("attempts = %d, want %d", got, tc.want)
				}
			})
		}
	}
}

func TestSubscriptions_BackpressureDoesNotResetAmbiguousBodyBudget(t *testing.T) {
	for _, group := range []bool{true, false} {
		t.Run(fmt.Sprintf("group=%v", group), func(t *testing.T) {
			shorten(t, &groupStreamInitialDelay, time.Millisecond)
			var clock atomic.Int64
			var eventBuffer atomic.Value
			eventBuffer.Store((<-chan *SubscriptionEvent)(nil))
			deliveryBlocked := make(chan struct{})
			var blockedOnce sync.Once
			oldNow := subscriptionRetryNow
			subscriptionRetryNow = func() time.Time {
				now := time.Unix(1, clock.Load())
				buffer := eventBuffer.Load().(<-chan *SubscriptionEvent)
				if buffer != nil && len(buffer) == cap(buffer) {
					blockedOnce.Do(func() { close(deliveryBlocked) })
				}
				return now
			}
			t.Cleanup(func() { subscriptionRetryNow = oldNow })
			var attempts atomic.Int32
			consumerReady := make(chan struct{})
			fourthEnded := make(chan struct{})
			fifth := make(chan struct{})
			srv := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := attempts.Add(1)
				if n == 5 {
					close(fifth)
					<-r.Context().Done()
					return
				}
				if group {
					readJoinBody(r)
					w.Header().Set("Content-Type", "application/connect+proto")
				} else {
					w.Header().Set("Content-Type", cursorClientContentType)
				}
				body := subscriptionTestBody(t, group, n, true)
				if n == 4 {
					select {
					case <-consumerReady:
					case <-r.Context().Done():
						return
					}
					event := body
					if group {
						event = body[len(subscriptionTestBody(t, true, n, false)):]
					}
					for range 100 {
						body = append(body, event...)
					}
				}
				w.Header().Set("Content-Length", strconv.Itoa(len(body)+1))
				_, _ = w.Write(body)
				if n == 4 {
					close(fourthEnded)
				}
			}))
			t.Cleanup(srv.Close)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			events, errs, err := startBudgetSubscription(t, ctx, srv.URL, group, NewNoopLogger())
			if err != nil {
				t.Fatal(err)
			}
			eventBuffer.Store(events)
			close(consumerReady)
			select {
			case <-fourthEnded:
			case <-ctx.Done():
				t.Fatal("fourth response never completed")
			}
			select {
			case <-deliveryBlocked:
			case <-ctx.Done():
				t.Fatal("event delivery never blocked")
			}
			clock.Store(int64(time.Minute))
			for {
				select {
				case _, ok := <-events:
					if !ok {
						events = nil
					}
				case <-fifth:
					t.Fatal("consumer backpressure reset the budget: made attempt 5 instead of ending on attempt 4")
				case e := <-errs:
					if e == nil || e.Retrying {
						t.Fatalf("terminal error = %+v", e)
					}
					assertBudgetMessage(t, e.Message)
					if got := attempts.Load(); got != 4 {
						t.Fatalf("attempts = %d, want 4", got)
					}
					return
				case <-ctx.Done():
					t.Fatal("subscription did not report exhaustion")
				}
			}
		})
	}
}

func readJoinBody(r *http.Request) {
	var prefix [5]byte
	if _, err := io.ReadFull(r.Body, prefix[:]); err == nil {
		_, _ = io.CopyN(io.Discard, r.Body, int64(binary.BigEndian.Uint32(prefix[1:])))
	}
}

func subscriptionTestBody(t *testing.T, group bool, n int32, event bool) []byte {
	t.Helper()
	ev := &ironflowv1.SubscriptionEvent{SubscriptionId: "member", EventId: fmt.Sprintf("event-%d", n), Topic: "orders", Sequence: uint64(n), Metadata: &ironflowv1.EventMetadata{}}
	if !group {
		b, err := protojson.Marshal(ev)
		if err != nil {
			t.Error(err)
		}
		return envelope(b)
	}
	joined := joinedFrame()
	joined.GetJoined().AckMode = ironflowv1.AckMode_ACK_MODE_AUTO
	b, err := proto.Marshal(joined)
	if err != nil {
		t.Error(err)
	}
	body := envelope(b)
	if event {
		b, err = proto.Marshal(&bidiResp{Payload: &ironflowv1.SubscribeBidirectionalResponse_Event{Event: ev}})
		if err != nil {
			t.Error(err)
		}
		body = append(body, envelope(b)...)
	}
	return body
}

func startBudgetSubscription(t *testing.T, ctx context.Context, url string, group bool, logger Logger) (<-chan *SubscriptionEvent, <-chan *SubscriptionError, error) {
	t.Helper()
	if group {
		c := NewClient(ClientConfig{ServerURL: url, APIKey: "ifkey_test", Logger: logger})
		sub, err := c.JoinConsumerGroup(ctx, "workers", WithJoinTransport("grpc"))
		if err != nil {
			return nil, nil, err
		}
		t.Cleanup(sub.Unsubscribe)
		return sub.Events(), sub.Errors(), nil
	}
	c := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{ServerURL: url, ReconnectDelay: time.Millisecond, Logger: logger})
	t.Cleanup(c.Close)
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	cursor := uint64(0)
	sub, err := c.Subscribe(ctx, "orders", &SubscribeOptions{StartAfterSequence: &cursor})
	if err != nil {
		return nil, nil, err
	}
	t.Cleanup(sub.Unsubscribe)
	return sub.Events(), sub.Errors(), nil
}

func assertBudgetMessage(t *testing.T, msg string) {
	t.Helper()
	if !strings.Contains(msg, "retry budget exhausted after 3 retries") || !strings.Contains(msg, "unexpected EOF") {
		t.Fatalf("exhaustion message lost budget or cause: %s", msg)
	}
}

func TestSubscriptions_AmbiguousBodyRecoveryAndCancel(t *testing.T) {
	for _, group := range []bool{true, false} {
		for _, cancelBackoff := range []bool{true, false} {
			t.Run(fmt.Sprintf("group=%v/cancelBackoff=%v", group, cancelBackoff), func(t *testing.T) {
				shorten(t, &groupStreamInitialDelay, 20*time.Millisecond)
				var attempts atomic.Int32
				backoff := make(chan struct{})
				logger := &budgetTestLogger{Logger: NewNoopLogger(), backoff: backoff}
				srv := newH2CTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					n := attempts.Add(1)
					if group {
						readJoinBody(r)
						w.Header().Set("Content-Type", "application/connect+proto")
					} else {
						w.Header().Set("Content-Type", cursorClientContentType)
					}
					body := subscriptionTestBody(t, group, n, true)
					if n == 1 {
						w.Header().Set("Content-Length", strconv.Itoa(len(body)+1))
					}
					_, _ = w.Write(body)
					w.(http.Flusher).Flush()
					if n == 1 {
						return
					}
					<-r.Context().Done()
				}))
				t.Cleanup(srv.Close)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if cancelBackoff {
					shorten(t, &groupStreamInitialDelay, time.Hour)
				}
				var events <-chan *SubscriptionEvent
				var errs <-chan *SubscriptionError
				if !group && cancelBackoff {
					c := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{ServerURL: srv.URL, ReconnectDelay: time.Hour, Logger: logger})
					t.Cleanup(c.Close)
					if err := c.Connect(ctx); err != nil {
						t.Fatal(err)
					}
					cursor := uint64(0)
					sub, err := c.Subscribe(ctx, "orders", &SubscribeOptions{StartAfterSequence: &cursor})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(sub.Unsubscribe)
					events, errs = sub.Events(), sub.Errors()
				} else {
					var err error
					events, errs, err = startBudgetSubscription(t, ctx, srv.URL, group, logger)
					if err != nil {
						t.Fatal(err)
					}
				}
				if cancelBackoff {
					<-backoff
					cancel()
				} else {
				recovery:
					for {
						select {
						case ev := <-events:
							if ev != nil && ev.ID == "event-2" {
								break recovery
							}
						case e := <-errs:
							t.Fatalf("recovery failed: %+v", e)
						case <-ctx.Done():
							t.Fatal("no recovered event")
						}
					}
					cancel()
				}
				select {
				case e := <-errs:
					if e == nil || e.Code != "canceled" {
						t.Fatalf("cancellation = %+v", e)
					}
				case <-time.After(time.Second):
					t.Fatal("cancellation did not finish")
				}
				want := int32(2)
				if cancelBackoff {
					want = 1
				}
				if got := attempts.Load(); got != want {
					t.Fatalf("attempts = %d, want %d", got, want)
				}
			})
		}
	}
}

func TestSubscriptionRetryExhaustionSurvivesConnectWrapping(t *testing.T) {
	for _, code := range []connect.Code{connect.CodeInvalidArgument, connect.CodeUnavailable, connect.CodeInternal, connect.CodeUnknown} {
		t.Run(code.String(), func(t *testing.T) {
			budget := newSubscriptionRetryBudget()
			cause := connect.NewError(code, &lostConnectionError{io.ErrUnexpectedEOF})
			for i := 0; i < 3; i++ {
				if err := budget.ended(cause); !isRetryableSubscriptionError(context.Background(), err) {
					t.Fatalf("retry %d refused: %v", i+1, err)
				}
			}
			err := connect.NewError(code, budget.ended(cause))
			if isRetryableSubscriptionError(context.Background(), err) {
				t.Fatal("Connect wrapper bypassed exhaustion")
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("cause lost: %v", err)
			}
		})
	}
}

type budgetTestLogger struct {
	Logger
	backoff chan struct{}
	once    sync.Once
}

func (l *budgetTestLogger) Warn(_ string, _ ...any) {
	l.once.Do(func() { close(l.backoff) })
}
