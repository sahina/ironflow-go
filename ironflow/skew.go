package ironflow

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"

	"connectrpc.com/connect"
	"github.com/gorilla/websocket"
)

// headerSDK names the SDK and its version on every request (ADR 0125, version
// skew). The server reads no value from it; it is for logs and for support.
const headerSDK = "X-Ironflow-SDK"

// minServerVersionSubscribeBidirectional is the first server that implements
// PubSubService.SubscribeBidirectional (ADR 0123). The Node and Python SDKs
// carry the same value; scripts/skew/check-min-versions.sh keeps them equal.
const minServerVersionSubscribeBidirectional = "0.48.0"

// minServerVersionProjectionLease is the first server with the projection
// runner lease RPCs (ADR 0120). The runner needs them; the other SDKs carry the
// same value and scripts/skew/check-min-versions.sh keeps them equal.
const minServerVersionProjectionLease = "0.44.0"

// minServerVersionProjectionPosition is the first server that honors the
// projection delivery position (ADR 0164). The other SDKs carry the same value
// and scripts/skew/check-min-versions.sh keeps them equal.
const minServerVersionProjectionPosition = "0.55.0"

// isServerTooOld reports an error that a retry against the same server cannot fix.
func isServerTooOld(err error) bool {
	var ie *IronflowError
	return errors.As(err, &ie) && ie.Code == ErrCodeServerTooOld
}

// ErrCodeServerTooOld is the IronflowError code for a call the server is too
// old to serve.
const ErrCodeServerTooOld = "SERVER_TOO_OLD"

func sdkHeaderValue() string { return "go/" + SDKVersion }

// sdkTransport sets X-Ironflow-SDK unless the request already has one. A nil
// inner is http.DefaultTransport, read at call time so tests can swap it.
type sdkTransport struct{ inner http.RoundTripper }

func (t sdkTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	inner := t.inner
	if inner == nil {
		inner = http.DefaultTransport
	}
	if r.Header.Get(headerSDK) != "" {
		return inner.RoundTrip(r)
	}
	r = r.Clone(r.Context())
	r.Header.Set(headerSDK, sdkHeaderValue())
	return inner.RoundTrip(r)
}

// CloseIdleConnections forwards to the wrapped transport; http.Client reaches it
// only through this method, and the h2c stream clients depend on it.
func (t sdkTransport) CloseIdleConnections() {
	inner := t.inner
	if inner == nil {
		inner = http.DefaultTransport
	}
	if c, ok := inner.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

// withSDKHeader returns a shallow copy of c that sends X-Ironflow-SDK. The
// caller's client is never changed.
func withSDKHeader(c *http.Client) *http.Client {
	cp := *c
	if _, ok := cp.Transport.(sdkTransport); !ok {
		cp.Transport = sdkTransport{inner: cp.Transport}
	}
	return &cp
}

// sdkDefaultClient replaces http.DefaultClient inside the SDK.
var sdkDefaultClient = withSDKHeader(&http.Client{})

// dialWS dials a WebSocket with X-Ironflow-SDK added to header.
func dialWS(ctx context.Context, url string, header http.Header) (*websocket.Conn, *http.Response, error) {
	if header == nil {
		header = http.Header{}
	}
	header.Set(headerSDK, sdkHeaderValue())
	return dialBounded(ctx, websocket.DefaultDialer, url, header)
}

// dialBounded dials with d and closes the connection when the dial's time runs
// out. A SOCKS5 proxy dial clears the deadline the dialer set on it, so a server
// that never answers the upgrade would otherwise hold the dial open.
func dialBounded(ctx context.Context, base *websocket.Dialer, url string, header http.Header) (*websocket.Conn, *http.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if base.HandshakeTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, base.HandshakeTimeout)
		defer cancel()
	}
	d := *base
	dial := d.NetDialContext
	switch {
	case dial != nil:
	case d.NetDial != nil:
		dial = func(_ context.Context, network, addr string) (net.Conn, error) { return d.NetDial(network, addr) }
	default:
		dial = (&net.Dialer{}).DialContext
	}
	var stop func() bool
	bound := func(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
		return func(dialCtx context.Context, network, addr string) (net.Conn, error) {
			c, err := dial(dialCtx, network, addr)
			if err == nil {
				stop = context.AfterFunc(ctx, func() { _ = c.Close() })
			}
			return c, err
		}
	}
	d.NetDialContext = bound(dial)
	if d.NetDialTLSContext != nil {
		d.NetDialTLSContext = bound(d.NetDialTLSContext)
	}
	conn, resp, err := d.DialContext(ctx, url, header)
	if stop != nil && !stop() {
		// The close can win over the dialer's own deadline: name the cause.
		if err != nil {
			return nil, resp, fmt.Errorf("%w: %w", context.Cause(ctx), err)
		}
		_ = conn.Close()
		return nil, resp, context.Cause(ctx)
	}
	return conn, resp, err
}

// serverTooOld turns a Connect Unimplemented from SubscribeBidirectional into an error that names
// the minimum server version. Any other error passes through unchanged.
func serverTooOld(err error) error {
	if err == nil || connect.CodeOf(err) != connect.CodeUnimplemented {
		return err
	}
	return newServerTooOld("SubscribeBidirectional", minServerVersionSubscribeBidirectional, "upgrade the server or use the WebSocket transport", err)
}

// newServerTooOld is the SERVER_TOO_OLD error for rpc. It is not retryable.
func newServerTooOld(rpc, minVersion, remedy string, cause error) *IronflowError {
	e := NewError(fmt.Sprintf("%s needs Ironflow server >= %s; %s", rpc, minVersion, remedy), ErrCodeServerTooOld, false)
	e.Cause = cause
	return e
}
