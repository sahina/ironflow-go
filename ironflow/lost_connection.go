package ironflow

import (
	"errors"
	"io"
	"net"
	"net/http"
)

// lostConnectionError marks a response-body read that the transport failed: a
// reset, a FIN, a killed server, or an ambiguous short body. connect-go v1.21
// reports a failed read of a frame prefix, where an idle stream waits, as a client-side
// invalid_argument ("incomplete envelope"), the same code it gives a frame that
// arrived and cannot decode. The code alone cannot tell the two apart.
type lostConnectionError struct{ err error }

func (e *lostConnectionError) Error() string { return e.err.Error() }
func (e *lostConnectionError) Unwrap() error { return e.err }

func isLostConnection(err error) bool {
	var lost *lostConnectionError
	return errors.As(err, &lost)
}

func withLostConnectionMark(c *http.Client) *http.Client {
	c.Transport = lostConnectionTransport{inner: c.Transport}
	return c
}

type lostConnectionTransport struct{ inner http.RoundTripper }

func (t lostConnectionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.inner.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	resp.Body = lostConnectionBody{resp.Body}
	return resp, nil
}

// CloseIdleConnections forwards: http.Client reaches the transport only through it.
func (t lostConnectionTransport) CloseIdleConnections() {
	if c, ok := t.inner.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

type lostConnectionBody struct{ io.ReadCloser }

// Read marks only a socket error or a stream the transport saw end early. A
// malformed response, such as a bad HTTP/1.1 chunk length, would fail again.
func (b lostConnectionBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	var netErr net.Error
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &netErr) {
		err = &lostConnectionError{err}
	}
	return n, err
}
