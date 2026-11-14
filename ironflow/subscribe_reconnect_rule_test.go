package ironflow

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

// connect-go does not export its flagEnvelopeCompressed.
const envelopeCompressedFlag = 1

func corruptGzipEnvelope(t *testing.T, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(body); err != nil {
		t.Fatalf("gzip: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip: %v", err)
	}
	b := buf.Bytes()
	b[len(b)-8] ^= 0xff
	out := envelope(b)
	out[0] = envelopeCompressedFlag
	return out
}

func truncatedEnvelope() []byte {
	return append(envelope(make([]byte, 10))[:5], 1, 2, 3)
}

func frameServer(t *testing.T, path, contentType string, hold bool, frames ...[]byte) (string, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Connect-Content-Encoding", "gzip")
		for _, f := range frames {
			_, _ = w.Write(f)
		}
		w.(http.Flusher).Flush()
		if hold {
			<-r.Context().Done()
		}
	})
	srv := newH2CTestServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL, &calls
}

func joinedEnvelope(t *testing.T) []byte {
	t.Helper()
	b, err := proto.Marshal(joinedFrame())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return envelope(b)
}

func TestGroupStream_FrameThatArrivedButCannotDecodeEnds(t *testing.T) {
	shorten(t, &groupStreamInitialDelay, 20*time.Millisecond)
	for name, tc := range map[string]struct {
		frame []byte
		hold  bool
	}{
		"bad gzip checksum":        {frame: corruptGzipEnvelope(t, []byte{0x0a, 0x00}), hold: true},
		"truncated by a clean end": {frame: truncatedEnvelope()},
	} {
		t.Run(name, func(t *testing.T) {
			url, calls := frameServer(t, "/ironflow.v1.PubSubService/SubscribeBidirectional", "application/connect+proto",
				tc.hold, joinedEnvelope(t), tc.frame)
			c := NewClient(ClientConfig{ServerURL: url, APIKey: "ifkey_test", Logger: NewNoopLogger()})
			sub := joinGroup(t, c, context.Background())
			if e := waitForEnd(t, sub); e == nil || e.Code != "invalid_argument" {
				t.Fatalf("Errors() = %+v, want invalid_argument", e)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("streams = %d, want 1: a frame that arrived must not be retried", got)
			}
		})
	}
}

func TestGrpcSubscriptionClient_CursorEndsOnFrameThatCannotDecode(t *testing.T) {
	event := []byte(`{"subscriptionId":"sub-1","eventId":"e","topic":"orders","sequence":"401","metadata":{}}`)
	for name, tc := range map[string]struct {
		frame []byte
		hold  bool
	}{
		"bad gzip checksum":        {frame: corruptGzipEnvelope(t, event), hold: true},
		"truncated by a clean end": {frame: truncatedEnvelope()},
	} {
		t.Run(name, func(t *testing.T) {
			url, calls := frameServer(t, "/ironflow.v1.PubSubService/Subscribe", cursorClientContentType,
				tc.hold, tc.frame)
			client := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{ServerURL: url, ReconnectDelay: 10 * time.Millisecond, Logger: NewNoopLogger()})
			t.Cleanup(client.Close)
			if err := client.Connect(context.Background()); err != nil {
				t.Fatalf("Connect: %v", err)
			}
			cursor := uint64(400)
			sub, err := client.Subscribe(context.Background(), "orders.*", &SubscribeOptions{StartAfterSequence: &cursor})
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			select {
			case e := <-sub.Errors():
				if e == nil || e.Code != "invalid_argument" {
					t.Fatalf("Errors() = %+v, want invalid_argument", e)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the frame did not end the subscription")
			}
			time.Sleep(100 * time.Millisecond)
			if got := calls.Load(); got != 1 {
				t.Fatalf("attempts = %d, want 1", got)
			}
		})
	}
}

func TestGroupStream_RejoinToSilentHostRedials(t *testing.T) {
	shorten(t, &groupStreamIdleTimeout, 300*time.Millisecond)
	c, proxy := proxiedFakeBidi(t, oneEventPerStream(t))
	sub := joinGroup(t, c, context.Background())
	eventOrEnd(t, sub)

	proxy.silent.Store(true)
	proxy.rst()
	waitUntil(t, "a second dial to the silent host", func() bool { return proxy.silents.Load() >= 2 })
	proxy.silent.Store(false)

	eventOrEnd(t, sub)
	select {
	case e := <-sub.Errors():
		t.Fatalf("a silent host reached Errors(): %+v", e)
	default:
	}
}

func TestGroupStream_JoinToSilentHostFails(t *testing.T) {
	shorten(t, &groupStreamIdleTimeout, 300*time.Millisecond)
	proxy := startCutProxy(t, "127.0.0.1:1")
	proxy.silent.Store(true)
	c := NewClient(ClientConfig{ServerURL: proxy.url(), APIKey: "ifkey_test", Logger: NewNoopLogger()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.JoinConsumerGroup(ctx, "workers", WithJoinTransport("grpc"))

	if connect.CodeOf(err) != connect.CodeUnavailable || ctx.Err() != nil {
		t.Fatalf("JoinConsumerGroup = %v, want unavailable before the caller's deadline", err)
	}
}

func TestGrpcSubscriptionClient_CursorReportsDeadlinePassedDuringBackoff(t *testing.T) {
	service := &holdingCursorServer{}
	_, proxy := proxiedCursorClient(t, service)
	client := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{ServerURL: proxy.url(), ReconnectDelay: 10 * time.Second, Logger: NewNoopLogger()})
	t.Cleanup(client.Close)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cursor := uint64(400)
	sub, err := client.Subscribe(ctx, "orders.*", &SubscribeOptions{StartAfterSequence: &cursor})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cursorEvent(t, sub)

	proxy.rst()

	select {
	case e, ok := <-sub.Errors():
		if !ok || e.Code != "deadline_exceeded" {
			t.Fatalf("Errors() = %+v (open %v), want deadline_exceeded", e, ok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the deadline did not reach Errors()")
	}
}

type warnLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *warnLog) Debug(string, ...any) {}
func (l *warnLog) Info(string, ...any)  {}
func (l *warnLog) Error(string, ...any) {}
func (l *warnLog) Warn(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprint(append([]any{msg}, args...)...))
}

func (l *warnLog) count(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

func TestGroupStream_LogsEachFailedAttempt(t *testing.T) {
	direct, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if n == 2 || n == 3 {
			return connect.NewError(connect.CodeUnavailable, fmt.Errorf("still down %d", n))
		}
		return oneEventPerStream(t)(ctx, n, st)
	})
	proxy := startCutProxy(t, strings.TrimPrefix(direct.serverURL, "http://"))
	log := &warnLog{}
	c := NewClient(ClientConfig{ServerURL: proxy.url(), APIKey: "ifkey_test", Logger: log})
	sub := joinGroup(t, c, context.Background())
	eventOrEnd(t, sub)

	proxy.rst()
	eventOrEnd(t, sub)

	if n := log.count("still down"); n != 2 {
		t.Fatalf("warnings naming the failed rejoins = %d, want 2: %q", n, log.lines)
	}
	if n := log.count("reconnect"); n < 3 {
		t.Fatalf("reconnect warnings = %d, want one per failed attempt (3): %q", n, log.lines)
	}
}

func TestGrpcSubscriptionClient_CursorLogsEachFailedAttempt(t *testing.T) {
	service := &holdingCursorServer{}
	_, proxy := proxiedCursorClient(t, service)
	log := &warnLog{}
	client := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{ServerURL: proxy.url(), ReconnectDelay: 10 * time.Millisecond, Logger: log})
	t.Cleanup(client.Close)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	cursor := uint64(400)
	sub, err := client.Subscribe(context.Background(), "orders.*", &SubscribeOptions{StartAfterSequence: &cursor})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cursorEvent(t, sub)

	proxy.rst()
	cursorEvent(t, sub)

	if n := log.count("reconnect"); n != 1 {
		t.Fatalf("reconnect warnings = %d, want 1: %q", n, log.lines)
	}
}
