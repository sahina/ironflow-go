package ironflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
)

func retrySignalClient(url string, maxDelay time.Duration) *Client {
	return &Client{
		serverURL:  url,
		httpClient: &http.Client{},
		retryConfig: &ClientRetryConfig{
			MaxAttempts:          3,
			InitialDelay:         time.Millisecond,
			MaxDelay:             maxDelay,
			BackoffMultiplier:    2.0,
			ConnectionRetryDelay: time.Millisecond,
		},
		logger: NewNoopLogger(),
	}
}

func restErrorFor(t *testing.T, status int, header, body string) *IronflowError {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if header != "" {
			w.Header().Set("Retry-After", header)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	c := retrySignalClient(srv.URL, time.Millisecond)
	c.retryConfig = nil
	err := c.request(context.Background(), "GET", "/x", nil, nil)
	ife, ok := err.(*IronflowError)
	if !ok {
		t.Fatalf("want *IronflowError, got %T", err)
	}
	return ife
}

func TestRESTRetryableFollowsServerFlag(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"flag true wins on 409", 409, `{"error":"x","retryable":true}`, true},
		{"flag false wins on 503", 503, `{"error":"x","retryable":false}`, false},
		{"no flag 503 retryable", 503, `{"error":"x"}`, true},
		{"no flag 501 not retryable", 501, `{"error":"x"}`, false},
		{"no flag 409 not retryable", 409, `{"error":"x"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := restErrorFor(t, tc.status, "", tc.body).Retryable; got != tc.want {
				t.Errorf("Retryable = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRESTRetryAfterBodyAndHeader(t *testing.T) {
	if got := restErrorFor(t, 503, "", `{"error":"x","retry_after":2}`).RetryAfter; got != 2*time.Second {
		t.Errorf("body retry_after: RetryAfter = %v, want 2s", got)
	}
	if got := restErrorFor(t, 503, "5", `{"error":"x","retry_after":2}`).RetryAfter; got != 5*time.Second {
		t.Errorf("header must win: RetryAfter = %v, want 5s", got)
	}
}

func TestKVErrorFollowsServerFlag(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"flag true on 409", 409, `{"error":"x","retryable":true,"retry_after":2}`, true},
		{"flag false on 503", 503, `{"error":"x","retryable":false}`, false},
		{"no flag 501", 501, `{"error":"x"}`, false},
		{"no flag 503", 503, `{"error":"x"}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer cleanup()
			_, err := client.KV().Bucket("b").Get(context.Background(), "k")
			ife, ok := err.(*IronflowError)
			if !ok {
				t.Fatalf("want *IronflowError, got %T", err)
			}
			if ife.Retryable != tc.want {
				t.Errorf("Retryable = %v, want %v", ife.Retryable, tc.want)
			}
		})
	}
}

func TestWriteRetryOnlyWhenRateLimited(t *testing.T) {
	for _, tc := range []struct {
		name   string
		method string
		status int
		want   int32
	}{
		{"POST 503 sent once", "POST", 503, 1},
		{"PATCH 503 sent once", "PATCH", 503, 1},
		{"POST 429 resent", "POST", 429, 3},
		{"GET 503 resent", "GET", 503, 3},
		{"PUT 503 resent", "PUT", 503, 3},
		{"DELETE 503 resent", "DELETE", 503, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"error":"x","retryable":true}`))
			}))
			defer srv.Close()
			err := retrySignalClient(srv.URL, 5*time.Millisecond).request(context.Background(), tc.method, "/x", nil, nil)
			if err == nil {
				t.Fatal("want error")
			}
			if ife, ok := err.(*IronflowError); !ok || !ife.Retryable {
				t.Errorf("caller error must stay retryable, got %#v", err)
			}
			if got := hits.Load(); got != tc.want {
				t.Errorf("server hits = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestWriteNotResentAfterNetworkFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()
	var retries atomic.Int32
	c := retrySignalClient(url, 5*time.Millisecond)
	c.retryConfig.OnRetry = func(RetryEvent) { retries.Add(1) }
	_ = c.request(context.Background(), "POST", "/x", nil, nil)
	if retries.Load() != 0 {
		t.Errorf("POST resent %d times after a network failure, want 0", retries.Load())
	}
	_ = c.request(context.Background(), "GET", "/x", nil, nil)
	if retries.Load() == 0 {
		t.Error("GET must still retry a network failure")
	}
}

func TestRetryAfterClampedToMaxDelay(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	var maxDelay time.Duration
	c := retrySignalClient(srv.URL, 20*time.Millisecond)
	c.retryConfig.OnRetry = func(e RetryEvent) { maxDelay = max(maxDelay, e.Delay) }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_ = c.request(ctx, "GET", "/x", nil, nil)
	if hits.Load() != 3 {
		t.Errorf("hits = %d, want 3", hits.Load())
	}
	if maxDelay > 20*time.Millisecond {
		t.Errorf("delay = %v, want <= MaxDelay 20ms", maxDelay)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("took %v; Retry-After was not clamped", time.Since(start))
	}
}

func TestConnectUnimplementedNotRetryable(t *testing.T) {
	if isRetryableConnectCode(connect.CodeUnimplemented) {
		t.Error("CodeUnimplemented must not be retryable")
	}
}

func TestConnectWriteRetryOnlyWhenRateLimited(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write bool
		code  connect.Code
		want  int32
	}{
		{"write unavailable once", true, connect.CodeUnavailable, 1},
		{"write resource_exhausted resent", true, connect.CodeResourceExhausted, 3},
		{"read unavailable resent", false, connect.CodeUnavailable, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			c := retrySignalClient("http://unused", 5*time.Millisecond)
			_ = c.withRetry(context.Background(), tc.write, func() error {
				calls.Add(1)
				return connectError(connect.NewError(tc.code, nil))
			})
			if got := calls.Load(); got != tc.want {
				t.Errorf("calls = %d, want %d", got, tc.want)
			}
		})
	}
}

// Many reads are Connect RPCs sent as POST. They change nothing, so a 5xx is
// resent; a write RPC is not.
func TestReadRPCOverPOSTRetries5xx(t *testing.T) {
	for path, want := range map[string]int32{
		"/ironflow.v1.IronflowService/GetRun":  3,
		"/ironflow.v1.IronflowService/Trigger": 1,
	} {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"busy"}`))
		}))
		c := retrySignalClient(srv.URL, time.Millisecond)
		_ = c.request(context.Background(), "POST", path, map[string]any{}, nil)
		srv.Close()
		if got := hits.Load(); got != want {
			t.Errorf("%s: %d hits, want %d", path, got, want)
		}
	}
}

// Without a server flag (an older server), the fallback matches the shared
// rule: 408 and 429 retry, 501 and 507 do not.
func TestRESTFallbackMatchesSharedRule(t *testing.T) {
	for status, want := range map[int]bool{408: true, 429: true, 500: true, 501: false, 503: true, 507: false, 404: false} {
		if got, _ := restRetrySignal(status, http.Header{}, nil); got != want {
			t.Errorf("status %d: retryable %v, want %v", status, got, want)
		}
	}
}
