package ironflow

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type cutProxy struct {
	ln     net.Listener
	target string

	silent  atomic.Bool
	silents atomic.Int32

	mu    sync.Mutex
	pairs []proxiedConn
	held  []net.Conn
}

type proxiedConn struct{ client, upstream *net.TCPConn }

func startCutProxy(t *testing.T, target string) *cutProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &cutProxy{ln: ln, target: target}
	t.Cleanup(func() {
		_ = ln.Close()
		p.cut(func(c proxiedConn) { _ = c.client.Close() })
		p.mu.Lock()
		for _, c := range p.held {
			_ = c.Close()
		}
		p.mu.Unlock()
	})
	go p.accept()
	return p
}

func (p *cutProxy) url() string { return "http://" + p.ln.Addr().String() }

func (p *cutProxy) accept() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		if p.silent.Load() {
			p.silents.Add(1)
			p.mu.Lock()
			p.held = append(p.held, client)
			p.mu.Unlock()
			continue
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		pair := proxiedConn{client: client.(*net.TCPConn), upstream: upstream.(*net.TCPConn)}
		p.mu.Lock()
		p.pairs = append(p.pairs, pair)
		p.mu.Unlock()
		go func() { _, _ = io.Copy(pair.upstream, pair.client) }()
		go func() { _, _ = io.Copy(pair.client, pair.upstream) }()
	}
}

func (p *cutProxy) cut(how func(proxiedConn)) {
	p.mu.Lock()
	pairs := p.pairs
	p.pairs = nil
	p.mu.Unlock()
	for _, c := range pairs {
		how(c)
		_ = c.upstream.Close()
	}
}

func (p *cutProxy) rst() {
	p.cut(func(c proxiedConn) {
		// SO_LINGER 0 makes Close send RST.
		_ = c.client.SetLinger(0)
		_ = c.client.Close()
	})
}

func (p *cutProxy) fin() {
	p.cut(func(c proxiedConn) { _ = c.client.CloseWrite() })
}

func proxiedFakeBidi(t *testing.T, script func(ctx context.Context, n int, st *bidiStream) error) (*Client, *cutProxy) {
	t.Helper()
	direct, _ := startFakeBidi(t, script)
	target := strings.TrimPrefix(direct.serverURL, "http://")
	proxy := startCutProxy(t, target)
	return NewClient(ClientConfig{ServerURL: proxy.url(), APIKey: "ifkey_test", Logger: NewNoopLogger()}), proxy
}

func eventOrEnd(t *testing.T, sub *AckableSubscription) *SubscriptionEvent {
	t.Helper()
	select {
	case ev, ok := <-sub.Events():
		if ok {
			return ev
		}
		select {
		case e := <-sub.Errors():
			t.Fatalf("the subscription ended: %+v", e)
		default:
		}
		t.Fatal("the subscription ended with no error")
	case <-time.After(10 * time.Second):
		t.Fatal("no event arrived")
	}
	return nil
}

func oneEventPerStream(t *testing.T) func(context.Context, int, *bidiStream) error {
	return func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, fmt.Sprintf("member-%d", n), ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		if err := sendEvent(st, fmt.Sprintf("member-%d", n), fmt.Sprintf("evt-%d", n)); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	}
}

func TestGroupStream_ResumesAfterLostConnection(t *testing.T) {
	for name, cut := range map[string]func(*cutProxy){
		"rst": (*cutProxy).rst,
		"fin": (*cutProxy).fin,
	} {
		t.Run(name, func(t *testing.T) {
			c, proxy := proxiedFakeBidi(t, oneEventPerStream(t))
			sub := joinGroup(t, c, context.Background())
			if ev := eventOrEnd(t, sub); ev.ID != "evt-1" {
				t.Fatalf("first event = %s", ev.ID)
			}

			cut(proxy)

			if ev := eventOrEnd(t, sub); ev.ID != "evt-2" {
				t.Fatalf("event after the cut = %s", ev.ID)
			}
			if err := sub.Ack("evt-1"); !errors.Is(err, ErrStaleAck) {
				t.Fatalf("ack of an event from the lost stream = %v, want ErrStaleAck", err)
			}
		})
	}
}

const groupCutHelperEnv = "IRONFLOW_TEST_GROUP_CUT_HELPER_ADDR"

func TestGroupStreamCutHelperServer(t *testing.T) {
	addr := os.Getenv(groupCutHelperEnv)
	if addr == "" {
		t.Skip("runs only as a subprocess of TestGroupStream_ResumesAfterServerKilled")
	}
	generation := os.Getenv(groupCutHelperEnv + "_GEN")
	f := &fakeBidi{script: func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, "member-"+generation, ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		if err := sendEvent(st, "member-"+generation, "evt-gen-"+generation); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	}}
	path, h := ironflowv1connect.NewPubSubServiceHandler(f)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	srv := &http.Server{Addr: addr, Handler: mux, Protocols: new(http.Protocols)}
	srv.Protocols.SetHTTP1(true)
	srv.Protocols.SetUnencryptedHTTP2(true)
	_ = srv.ListenAndServe()
}

func startCutHelper(t *testing.T, addr string, generation int) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestGroupStreamCutHelperServer$")
	cmd.Env = append(os.Environ(), groupCutHelperEnv+"="+addr, groupCutHelperEnv+"_GEN="+strconv.Itoa(generation))
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	waitUntilWithin(t, 10*time.Second, "the helper server to listen", func() bool {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	})
	return cmd
}

func TestGroupStream_ResumesAfterServerKilled(t *testing.T) {
	shorten(t, &groupStreamInitialDelay, 20*time.Millisecond)
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))

	first := startCutHelper(t, addr, 1)
	c := NewClient(ClientConfig{ServerURL: "http://" + addr, APIKey: "ifkey_test", Logger: NewNoopLogger()})
	sub := joinGroup(t, c, context.Background())
	if ev := eventOrEnd(t, sub); ev.ID != "evt-gen-1" {
		t.Fatalf("first event = %s", ev.ID)
	}

	if err := first.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_ = first.Wait()
	startCutHelper(t, addr, 2)

	if ev := eventOrEnd(t, sub); ev.ID != "evt-gen-2" {
		t.Fatalf("event after the restart = %s", ev.ID)
	}
}

// rawH2Server speaks HTTP/2 frames by hand, because a Go handler cannot send
// RST_STREAM(CANCEL).
func rawH2Server(t *testing.T, contentType string, marshal func(proto.Message) ([]byte, error), stream func(n int) (msgs []proto.Message, reset bool)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var streams atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				if _, err := io.ReadFull(conn, make([]byte, len(http2.ClientPreface))); err != nil {
					return
				}
				fr := http2.NewFramer(conn, conn)
				_ = fr.WriteSettings()
				for {
					f, err := fr.ReadFrame()
					if err != nil {
						return
					}
					switch f := f.(type) {
					case *http2.SettingsFrame:
						if !f.IsAck() {
							_ = fr.WriteSettingsAck()
						}
					case *http2.HeadersFrame:
						var block bytes.Buffer
						enc := hpack.NewEncoder(&block)
						_ = enc.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
						_ = enc.WriteField(hpack.HeaderField{Name: "content-type", Value: contentType})
						_ = fr.WriteHeaders(http2.HeadersFrameParam{StreamID: f.StreamID, BlockFragment: block.Bytes(), EndHeaders: true})
						msgs, reset := stream(int(streams.Add(1)))
						for _, m := range msgs {
							b, err := marshal(m)
							if err != nil {
								t.Errorf("marshal: %v", err)
								return
							}
							_ = fr.WriteData(f.StreamID, false, envelope(b))
						}
						if reset {
							_ = fr.WriteRSTStream(f.StreamID, http2.ErrCodeCancel)
						}
					}
				}
			}()
		}
	}()
	return "http://" + ln.Addr().String()
}

func envelope(body []byte) []byte {
	out := make([]byte, 5+len(body))
	binary.BigEndian.PutUint32(out[1:5], uint32(len(body)))
	copy(out[5:], body)
	return out
}

func joinedFrame() *bidiResp {
	return &bidiResp{Payload: &ironflowv1.SubscribeBidirectionalResponse_Joined{
		Joined: &ironflowv1.ConsumerGroupJoined{SubscriptionId: "member", AckMode: ironflowv1.AckMode_ACK_MODE_MANUAL},
	}}
}

func TestGroupStream_ResumesAfterPeerResetsStreamWithCancel(t *testing.T) {
	shorten(t, &groupStreamInitialDelay, 20*time.Millisecond)
	url := rawH2Server(t, "application/connect+proto", proto.Marshal, func(n int) ([]proto.Message, bool) {
		if n == 1 {
			return []proto.Message{joinedFrame()}, true
		}
		return []proto.Message{joinedFrame(), &bidiResp{Payload: &ironflowv1.SubscribeBidirectionalResponse_Event{
			Event: &ironflowv1.SubscriptionEvent{SubscriptionId: "member", EventId: "evt-2", Topic: "orders"},
		}}}, false
	})
	c := NewClient(ClientConfig{ServerURL: url, APIKey: "ifkey_test", Logger: NewNoopLogger()})
	sub := joinGroup(t, c, context.Background())
	if ev := eventOrEnd(t, sub); ev.ID != "evt-2" {
		t.Fatalf("event after the reset = %s", ev.ID)
	}
}

func TestGroupStream_ServerCanceledReconnects(t *testing.T) {
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, fmt.Sprintf("member-%d", n), ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		if err := sendEvent(st, fmt.Sprintf("member-%d", n), fmt.Sprintf("evt-%d", n)); err != nil {
			return err
		}
		if n == 1 {
			return connect.NewError(connect.CodeCanceled, errors.New("context canceled"))
		}
		<-ctx.Done()
		return nil
	})
	sub := joinGroup(t, c, context.Background())
	// evt-1 may never surface: the SDK drops a queued event of an ended stream.
	for eventOrEnd(t, sub).ID != "evt-2" {
	}
}

func waitForEnd(t *testing.T, sub *AckableSubscription) *SubscriptionError {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case _, ok := <-sub.Events():
			if !ok {
				select {
				case e := <-sub.Errors():
					return e
				default:
					return nil
				}
			}
		case <-deadline:
			t.Fatal("the subscription did not end")
			return nil
		}
	}
}

func TestGroupStream_ServerRefusalAfterLostConnectionEnds(t *testing.T) {
	for _, code := range []connect.Code{
		connect.CodeInvalidArgument,
		connect.CodeNotFound,
		connect.CodeUnauthenticated,
		connect.CodePermissionDenied,
		connect.CodeFailedPrecondition,
	} {
		t.Run(code.String(), func(t *testing.T) {
			var streams atomic.Int32
			c, proxy := proxiedFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
				streams.Store(int32(n))
				if n > 1 {
					return connect.NewError(code, errors.New("no"))
				}
				if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
					return err
				}
				if err := sendEvent(st, "member-1", "evt-1"); err != nil {
					return err
				}
				<-ctx.Done()
				return nil
			})
			sub := joinGroup(t, c, context.Background())
			eventOrEnd(t, sub)

			proxy.rst()

			e := waitForEnd(t, sub)
			if e == nil || e.Code != code.String() {
				t.Fatalf("Errors() = %+v, want %s", e, code)
			}
			if got := streams.Load(); got != 2 {
				t.Fatalf("streams = %d, want 2: the refusal must end the retries", got)
			}
		})
	}
}

func TestGroupStream_ServerInvalidArgumentMidStreamEnds(t *testing.T) {
	c, _ := startFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
		if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
			return err
		}
		return connect.NewError(connect.CodeInvalidArgument, errors.New("join is allowed once"))
	})
	sub := joinGroup(t, c, context.Background())
	if e := waitForEnd(t, sub); e == nil || e.Code != "invalid_argument" {
		t.Fatalf("Errors() = %+v, want invalid_argument", e)
	}
}

func TestGroupStream_HTTPRefusalOnRejoinEnds(t *testing.T) {
	for status, want := range map[int]string{
		http.StatusUnauthorized: "unauthenticated",
		http.StatusForbidden:    "permission_denied",
		http.StatusNotFound:     "unimplemented",
	} {
		t.Run(want, func(t *testing.T) {
			var (
				refused atomic.Bool
				hits    atomic.Int32
			)
			direct, _ := startFakeBidi(t, oneEventPerStream(t))
			target, err := url.Parse(direct.serverURL)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			upstream := httputil.NewSingleHostReverseProxy(target)
			upstream.Transport = newH2CClient(direct.serverURL).Transport
			upstream.FlushInterval = -1
			front := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if refused.Load() {
					hits.Add(1)
					http.Error(w, "refused by the gateway", status)
					return
				}
				upstream.ServeHTTP(w, r)
			}))
			front.Config.Protocols = new(http.Protocols)
			front.Config.Protocols.SetHTTP1(true)
			front.Config.Protocols.SetUnencryptedHTTP2(true)
			front.Start()
			t.Cleanup(front.Close)
			proxy := startCutProxy(t, strings.TrimPrefix(front.URL, "http://"))

			c := NewClient(ClientConfig{ServerURL: proxy.url(), APIKey: "ifkey_test", Logger: NewNoopLogger()})
			sub := joinGroup(t, c, context.Background())
			eventOrEnd(t, sub)

			refused.Store(true)
			proxy.rst()

			if e := waitForEnd(t, sub); e == nil || e.Code != want {
				t.Fatalf("Errors() = %+v, want %s", e, want)
			}
			if got := hits.Load(); got != 1 {
				t.Fatalf("refused rejoins = %d, want 1", got)
			}
		})
	}
}

func TestGroupStream_UndecodableFrameEnds(t *testing.T) {
	var streams atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/ironflow.v1.PubSubService/SubscribeBidirectional", func(w http.ResponseWriter, r *http.Request) {
		streams.Add(1)
		joined, err := proto.Marshal(joinedFrame())
		if err != nil {
			t.Errorf("marshal: %v", err)
			return
		}
		w.Header().Set("Content-Type", "application/connect+proto")
		_, _ = w.Write(envelope(joined))
		_, _ = w.Write(envelope([]byte{0xff}))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	shorten(t, &groupStreamInitialDelay, 20*time.Millisecond)
	srv := newH2CTestServer(mux)
	t.Cleanup(srv.Close)

	c := NewClient(ClientConfig{ServerURL: srv.URL, APIKey: "ifkey_test", Logger: NewNoopLogger()})
	sub := joinGroup(t, c, context.Background())
	if e := waitForEnd(t, sub); e == nil || e.Code != "invalid_argument" {
		t.Fatalf("Errors() = %+v, want invalid_argument", e)
	}
	if got := streams.Load(); got != 1 {
		t.Fatalf("streams = %d, want 1", got)
	}
}

func TestGroupStream_StoppedWorkerStaysStopped(t *testing.T) {
	stop := map[string]struct {
		stop func(*AckableSubscription, context.CancelFunc)
		want string
	}{
		"unsubscribe": {stop: func(s *AckableSubscription, _ context.CancelFunc) { s.Unsubscribe() }},
		"ctx cancel":  {stop: func(_ *AckableSubscription, cancel context.CancelFunc) { cancel() }, want: "canceled"},
	}
	for name, tt := range stop {
		for _, afterCut := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/after cut %v", name, afterCut), func(t *testing.T) {
				var streams atomic.Int32
				c, proxy := proxiedFakeBidi(t, func(ctx context.Context, n int, st *bidiStream) error {
					streams.Store(int32(n))
					if n > 1 {
						return connect.NewError(connect.CodeUnavailable, errors.New("still down"))
					}
					if err := joinAndAnnounce(t, st, "member-1", ironflowv1.AckMode_ACK_MODE_MANUAL); err != nil {
						return err
					}
					if err := sendEvent(st, "member-1", "evt-1"); err != nil {
						return err
					}
					<-ctx.Done()
					return nil
				})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				sub := joinGroup(t, c, ctx)
				eventOrEnd(t, sub)
				if afterCut {
					proxy.rst()
					waitUntil(t, "a rejoin attempt", func() bool { return streams.Load() >= 2 })
				}

				tt.stop(sub, cancel)

				e := waitForEnd(t, sub)
				switch {
				case tt.want == "" && e != nil:
					t.Fatalf("Errors() = %+v, want none", e)
				case tt.want != "" && (e == nil || e.Code != tt.want):
					t.Fatalf("Errors() = %+v, want %s", e, tt.want)
				}
				before := streams.Load()
				time.Sleep(150 * time.Millisecond)
				if got := streams.Load(); got != before {
					t.Fatalf("streams grew from %d to %d after the stop", before, got)
				}
			})
		}
	}
}

type holdingCursorServer struct {
	ironflowv1connect.UnimplementedPubSubServiceHandler

	refuseAfter int

	mu      sync.Mutex
	cursors []uint64
}

func (s *holdingCursorServer) Subscribe(
	ctx context.Context,
	req *connect.Request[ironflowv1.SubscribeRequest],
	stream *connect.ServerStream[ironflowv1.SubscriptionEvent],
) error {
	cursor := req.Msg.GetOptions().GetStartAfterSequence()
	s.mu.Lock()
	s.cursors = append(s.cursors, cursor)
	call := len(s.cursors)
	s.mu.Unlock()
	if s.refuseAfter > 0 && call > s.refuseAfter {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("the cursor is out of range"))
	}
	if err := stream.Send(&ironflowv1.SubscriptionEvent{
		SubscriptionId: "sub-1", EventId: "event", Topic: "orders.created",
		Sequence: cursor + 1, Metadata: &ironflowv1.EventMetadata{},
	}); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *holdingCursorServer) seen() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.cursors...)
}

func proxiedCursorClient(t *testing.T, s *holdingCursorServer) (*GrpcSubscriptionClient, *cutProxy) {
	t.Helper()
	path, handler := ironflowv1connect.NewPubSubServiceHandler(s)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := newH2CTestServer(mux)
	t.Cleanup(server.Close)
	proxy := startCutProxy(t, strings.TrimPrefix(server.URL, "http://"))
	client := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{
		ServerURL:      proxy.url(),
		ReconnectDelay: 10 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})
	t.Cleanup(client.Close)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return client, proxy
}

func cursorEvent(t *testing.T, sub *GrpcSubscription) uint64 {
	t.Helper()
	select {
	case ev, ok := <-sub.Events():
		if !ok {
			t.Fatalf("the subscription ended: %+v", <-sub.Errors())
		}
		return ev.Meta.Sequence
	case <-time.After(10 * time.Second):
		t.Fatal("no event arrived")
	}
	return 0
}

// cursorClientContentType is what GrpcSubscriptionClient speaks: it sets WithProtoJSON.
const cursorClientContentType = "application/connect+json"

func TestGrpcSubscriptionClient_CursorResumesAfterLostConnection(t *testing.T) {
	for name, cut := range map[string]func(*cutProxy){
		"rst": (*cutProxy).rst,
		"fin": (*cutProxy).fin,
	} {
		t.Run(name, func(t *testing.T) {
			service := &holdingCursorServer{}
			client, proxy := proxiedCursorClient(t, service)
			cursor := uint64(400)
			sub, err := client.Subscribe(context.Background(), "orders.*", &SubscribeOptions{StartAfterSequence: &cursor})
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			if got := cursorEvent(t, sub); got != 401 {
				t.Fatalf("first sequence = %d", got)
			}

			cut(proxy)

			if got := cursorEvent(t, sub); got != 402 {
				t.Fatalf("sequence after the cut = %d", got)
			}
			if got := service.seen(); len(got) != 2 || got[1] != 401 {
				t.Fatalf("cursors = %v, want the reconnect to resume after 401", got)
			}
		})
	}
}

func TestGrpcSubscriptionClient_CursorResumesAfterPeerResetsStreamWithCancel(t *testing.T) {
	event := func(seq uint64) *ironflowv1.SubscriptionEvent {
		return &ironflowv1.SubscriptionEvent{SubscriptionId: "sub-1", EventId: "event", Topic: "orders", Sequence: seq, Metadata: &ironflowv1.EventMetadata{}}
	}
	url := rawH2Server(t, cursorClientContentType, protojson.Marshal, func(n int) ([]proto.Message, bool) {
		return []proto.Message{event(uint64(400 + n))}, n == 1
	})
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
	cursorEvent(t, sub)
	if got := cursorEvent(t, sub); got != 402 {
		t.Fatalf("sequence after the reset = %d", got)
	}
}

func TestGrpcSubscriptionClient_CursorEndsOnServerRefusalAfterLostConnection(t *testing.T) {
	service := &holdingCursorServer{refuseAfter: 1}
	client, proxy := proxiedCursorClient(t, service)
	cursor := uint64(400)
	sub, err := client.Subscribe(context.Background(), "orders.*", &SubscribeOptions{StartAfterSequence: &cursor})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cursorEvent(t, sub)

	proxy.rst()

	select {
	case e := <-sub.Errors():
		if e == nil || e.Code != "invalid_argument" {
			t.Fatalf("Errors() = %+v, want invalid_argument", e)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the refusal did not reach Errors()")
	}
	time.Sleep(100 * time.Millisecond)
	if got := service.seen(); len(got) != 2 {
		t.Fatalf("attempts = %d, want 2", len(got))
	}
}

func TestGrpcSubscriptionClient_CursorEndsOnUndecodableFrame(t *testing.T) {
	var streams atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/ironflow.v1.PubSubService/Subscribe", func(w http.ResponseWriter, r *http.Request) {
		streams.Add(1)
		w.Header().Set("Content-Type", cursorClientContentType)
		_, _ = w.Write(envelope([]byte("{not json")))
	})
	srv := newH2CTestServer(mux)
	t.Cleanup(srv.Close)
	client := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{ServerURL: srv.URL, ReconnectDelay: 10 * time.Millisecond, Logger: NewNoopLogger()})
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
	case <-time.After(10 * time.Second):
		t.Fatal("the frame did not end the subscription")
	}
	if got := streams.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestGrpcSubscriptionClient_CursorStopsWhenCallerStops(t *testing.T) {
	t.Run("ctx cancel reports canceled", func(t *testing.T) {
		service := &holdingCursorServer{}
		client, _ := proxiedCursorClient(t, service)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cursor := uint64(400)
		sub, err := client.Subscribe(ctx, "orders.*", &SubscribeOptions{StartAfterSequence: &cursor})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		cursorEvent(t, sub)

		cancel()

		select {
		case e := <-sub.Errors():
			if e == nil || e.Code != "canceled" {
				t.Fatalf("Errors() = %+v, want canceled", e)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the cancel did not reach Errors()")
		}
		time.Sleep(100 * time.Millisecond)
		if got := service.seen(); len(got) != 1 {
			t.Fatalf("attempts = %d, want 1: the cancel must not reconnect", len(got))
		}
	})
	t.Run("unsubscribe", func(t *testing.T) {
		service := &holdingCursorServer{}
		client, _ := proxiedCursorClient(t, service)
		cursor := uint64(400)
		sub, err := client.Subscribe(context.Background(), "orders.*", &SubscribeOptions{StartAfterSequence: &cursor})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		cursorEvent(t, sub)

		sub.Unsubscribe()

		time.Sleep(100 * time.Millisecond)
		if got := service.seen(); len(got) != 1 {
			t.Fatalf("attempts = %d, want 1: Unsubscribe must not reconnect", len(got))
		}
	})
}

func TestGrpcSubscriptionClient_UnpositionedStreamEndsOnLostConnection(t *testing.T) {
	service := &holdingCursorServer{}
	client, proxy := proxiedCursorClient(t, service)
	sub, err := client.Subscribe(context.Background(), "orders.*", nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	select {
	case <-sub.Events():
	case <-time.After(5 * time.Second):
		t.Fatal("no event arrived")
	}

	proxy.rst()

	select {
	case <-sub.Errors():
	case <-time.After(5 * time.Second):
		t.Fatal("the cut did not reach Errors()")
	}
	time.Sleep(100 * time.Millisecond)
	if got := service.seen(); len(got) != 1 {
		t.Fatalf("attempts = %d, want 1", len(got))
	}
}
