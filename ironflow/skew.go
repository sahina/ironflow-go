package ironflow

import (
	"context"
	"fmt"
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
	return websocket.DefaultDialer.DialContext(ctx, url, header)
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
