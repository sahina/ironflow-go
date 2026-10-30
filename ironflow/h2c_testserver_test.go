package ironflow

import (
	"net/http"
	"net/http/httptest"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// newH2CTestServer starts a cleartext HTTP/2 (prior knowledge) server. x/net's h2c
// hijacks the connection, so httptest.Server.Close does not wait for the streaming
// worker's 30s drain; net/http's own h2c would make it wait.
func newH2CTestServer(h http.Handler) *httptest.Server {
	return httptest.NewServer(h2c.NewHandler(h, &http2.Server{})) //nolint:staticcheck // see above
}
