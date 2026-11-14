package ironflow

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func setDefaultTransport(t *testing.T, rt http.RoundTripper) {
	t.Helper()
	old := http.DefaultTransport
	http.DefaultTransport = rt
	t.Cleanup(func() { http.DefaultTransport = old })
}

func nonTransportTrusting(srv *httptest.Server) http.RoundTripper {
	inner := srv.Client().Transport
	return roundTripperFunc(func(r *http.Request) (*http.Response, error) { return inner.RoundTrip(r) })
}

func TestGroupStream_UsesProgramTransportOverTLS(t *testing.T) {
	f := &fakeBidi{script: oneEventPerStream(t)}
	path, h := ironflowv1connect.NewPubSubServiceHandler(f)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	setDefaultTransport(t, nonTransportTrusting(srv))

	c := NewClient(ClientConfig{ServerURL: srv.URL, APIKey: "ifkey_test", Logger: NewNoopLogger()})
	sub := joinGroup(t, c, context.Background())
	if ev := eventOrEnd(t, sub); ev.ID != "evt-1" {
		t.Fatalf("first event = %s", ev.ID)
	}
}

func countingProxy(t *testing.T) (*url.URL, *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var n atomic.Int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n.Add(1)
			_ = conn.Close()
		}
	}()
	return &url.URL{Scheme: "http", Host: ln.Addr().String()}, &n
}

func TestStreamClients_DialCleartextServerDirectly(t *testing.T) {
	t.Run("group stream", func(t *testing.T) {
		c, _ := startFakeBidi(t, oneEventPerStream(t))
		proxyURL, proxied := countingProxy(t)
		setDefaultTransport(t, &http.Transport{Proxy: http.ProxyURL(proxyURL)})

		sub := joinGroup(t, c, context.Background())
		if ev := eventOrEnd(t, sub); ev.ID != "evt-1" {
			t.Fatalf("first event = %s", ev.ID)
		}
		if got := proxied.Load(); got != 0 {
			t.Fatalf("proxy connections = %d, want 0", got)
		}
	})
	t.Run("cursor subscription", func(t *testing.T) {
		service := &holdingCursorServer{}
		path, handler := ironflowv1connect.NewPubSubServiceHandler(service)
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		srv := newH2CTestServer(mux)
		t.Cleanup(srv.Close)
		proxyURL, proxied := countingProxy(t)
		setDefaultTransport(t, &http.Transport{Proxy: http.ProxyURL(proxyURL)})

		sub := subscribeAtCursor(t, srv.URL)
		if got := cursorEvent(t, sub); got != 401 {
			t.Fatalf("first sequence = %d", got)
		}
		if got := proxied.Load(); got != 0 {
			t.Fatalf("proxy connections = %d, want 0", got)
		}
	})
}

func subscribeAtCursor(t *testing.T, serverURL string) *GrpcSubscription {
	t.Helper()
	client := NewGrpcSubscriptionClient(GrpcSubscriptionClientConfig{ServerURL: serverURL, ReconnectDelay: 10 * time.Millisecond, Logger: NewNoopLogger()})
	t.Cleanup(client.Close)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	cursor := uint64(400)
	sub, err := client.Subscribe(context.Background(), "orders.*", &SubscribeOptions{StartAfterSequence: &cursor})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	return sub
}

func startTrustedHTTP1TLSServer(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	setDefaultTransport(t, nonTransportTrusting(srv))
	return srv
}

func TestGrpcSubscriptionClient_CursorResumesAfterLostConnectionOverHTTP1(t *testing.T) {
	for name, cut := range map[string]func(*cutProxy){
		"rst": (*cutProxy).rst,
		"fin": (*cutProxy).fin,
	} {
		t.Run(name, func(t *testing.T) {
			service := &holdingCursorServer{}
			path, handler := ironflowv1connect.NewPubSubServiceHandler(service)
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			srv := startTrustedHTTP1TLSServer(t, mux)
			proxy := startCutProxy(t, strings.TrimPrefix(srv.URL, "https://"))

			sub := subscribeAtCursor(t, "https://"+proxy.ln.Addr().String())
			if got := cursorEvent(t, sub); got != 401 {
				t.Fatalf("first sequence = %d", got)
			}

			cut(proxy)

			if got := cursorEvent(t, sub); got != 402 {
				t.Fatalf("sequence after the cut = %d", got)
			}
		})
	}
}

func TestGrpcSubscriptionClient_CursorEndsOnMalformedHTTP1Chunk(t *testing.T) {
	frame, err := protojson.Marshal(&ironflowv1.SubscriptionEvent{
		SubscriptionId: "sub-1", EventId: "event", Topic: "orders.created",
		Sequence: 401, Metadata: &ironflowv1.EventMetadata{},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := envelope(frame)
	var streams atomic.Int32
	srv := startTrustedHTTP1TLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		streams.Add(1)
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		writeRaw(rw, "HTTP/1.1 200 OK\r\nContent-Type: "+cursorClientContentType+"\r\nTransfer-Encoding: chunked\r\n\r\n")
		writeRaw(rw, fmt.Sprintf("%x\r\n%s\r\n", len(body), body))
		writeRaw(rw, "Z\r\n")
		// Keep the connection open so the bad chunk, not an EOF, ends the read.
		_, _ = io.Copy(io.Discard, rw)
	}))

	sub := subscribeAtCursor(t, srv.URL)
	if got := cursorEvent(t, sub); got != 401 {
		t.Fatalf("first sequence = %d", got)
	}
	select {
	case e := <-sub.Errors():
		if e == nil {
			t.Fatal("Errors() closed with no error")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("the malformed chunk did not end the subscription; attempts = %d", streams.Load())
	}
	if got := streams.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func writeRaw(rw *bufio.ReadWriter, s string) {
	_, _ = rw.WriteString(s)
	_ = rw.Flush()
}
