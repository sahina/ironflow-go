package ironflow

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// socksThatHangs accepts a SOCKS5 CONNECT and then never answers the upgrade.
func socksThatHangs(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				buf := make([]byte, 262)
				if _, err := io.ReadFull(c, buf[:2]); err != nil {
					return
				}
				if _, err := io.ReadFull(c, buf[:buf[1]]); err != nil {
					return
				}
				_, _ = c.Write([]byte{5, 0})
				if _, err := io.ReadFull(c, buf[:4]); err != nil {
					return
				}
				rest := 6 // IPv4 address and port
				if buf[3] == 3 {
					if _, err := io.ReadFull(c, buf[:1]); err != nil {
						return
					}
					rest = int(buf[0]) + 2
				}
				if _, err := io.ReadFull(c, buf[:rest]); err != nil {
					return
				}
				_, _ = c.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0})
				_, _ = io.Copy(io.Discard, c)
			}()
		}
	}()
	return l.Addr().String()
}

// A dial through SOCKS5 keeps its time limit: the proxy dial clears the
// connection's deadline.
func TestDialWS_SOCKS5KeepsTheHandshakeTimeout(t *testing.T) {
	addr := socksThatHangs(t)
	old := *websocket.DefaultDialer
	t.Cleanup(func() { *websocket.DefaultDialer = old })
	websocket.DefaultDialer.Proxy = func(*http.Request) (*url.URL, error) { return &url.URL{Scheme: "socks5", Host: addr}, nil }
	dial := func(ctx context.Context) error {
		done := make(chan error, 1)
		go func() {
			conn, _, err := dialWS(ctx, "ws://ironflow.test/ws", nil) //nolint:bodyclose // websocket upgrade response is not an HTTP body
			if err == nil {
				_ = conn.Close()
			}
			done <- err
		}()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("the dial is still waiting")
			return nil
		}
	}
	t.Run("caller deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()
		if err := dial(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("dial = %v, want the deadline", err)
		}
	})
	t.Run("caller cancels", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(300*time.Millisecond, cancel)
		if err := dial(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("dial = %v, want the cancellation", err)
		}
	})
	t.Run("handshake timeout", func(t *testing.T) {
		websocket.DefaultDialer.HandshakeTimeout = 300 * time.Millisecond
		if err := dial(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("dial = %v, want the deadline", err)
		}
	})
}
