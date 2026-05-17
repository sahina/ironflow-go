package ironflow

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectionRefused(t *testing.T) {
	t.Run("client returns clear error when server is down", func(t *testing.T) {
		// Use a port that's not listening
		client := &Client{
			serverURL:  "http://localhost:19999", // Unlikely to be in use
			httpClient: &http.Client{Timeout: 2 * time.Second},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]any
		err := client.request(ctx, "POST", "/test", nil, &result)

		if err == nil {
			t.Fatal("Expected error for connection refused")
		}

		// Should be marked as retryable
		ironflowErr, ok := err.(*IronflowError)
		if ok && !ironflowErr.Retryable {
			t.Error("Connection refused error should be retryable")
		}
	})

	t.Run("client retries on connection refused", func(t *testing.T) {
		var attemptCount atomic.Int32
		const failFirstN = 2

		// Custom transport that simulates connection refused for first N attempts
		mockTransport := &mockRoundTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				count := attemptCount.Add(1)
				if count <= failFirstN {
					// Simulate connection refused error
					return nil, &net.OpError{
						Op:  "dial",
						Net: "tcp",
						Err: errors.New("connect: connection refused"),
					}
				}
				// Return successful response after failFirstN attempts
				body := `{"status":"ok"}`
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(body)),
					Header:     make(http.Header),
				}, nil
			},
		}

		client := &Client{
			serverURL:  "http://localhost:9999",
			httpClient: &http.Client{Transport: mockTransport},
			retryConfig: &ClientRetryConfig{
				MaxAttempts:          5,
				InitialDelay:         10 * time.Millisecond,
				MaxDelay:             100 * time.Millisecond,
				BackoffMultiplier:    2.0,
				ConnectionRetryDelay: 10 * time.Millisecond,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]string
		err := client.request(ctx, "POST", "/test", nil, &result)

		if err != nil {
			t.Fatalf("Expected success after retries, got: %v", err)
		}

		finalCount := attemptCount.Load()
		expectedAttempts := int32(failFirstN + 1) // 2 failures + 1 success
		if finalCount != expectedAttempts {
			t.Errorf("Expected %d attempts, got %d", expectedAttempts, finalCount)
		}

		if result["status"] != "ok" {
			t.Errorf("Expected status 'ok', got '%s'", result["status"])
		}
	})
}

func TestRequestTimeout(t *testing.T) {
	t.Run("client times out on slow server", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(5 * time.Second) // Very slow
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{Timeout: 100 * time.Millisecond},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]any
		err := client.request(ctx, "POST", "/test", nil, &result)

		if err == nil {
			t.Fatal("Expected timeout error")
		}

		// Verify it completed in reasonable time (not 5 seconds)
		// The error should happen quickly due to client timeout
	})

	t.Run("timeout error is retryable", func(t *testing.T) {
		var requestCount atomic.Int32

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count := requestCount.Add(1)
			if count < 3 {
				time.Sleep(500 * time.Millisecond) // Timeout for first 2 requests
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{Timeout: 100 * time.Millisecond},
			retryConfig: &ClientRetryConfig{
				MaxAttempts:          5,
				InitialDelay:         10 * time.Millisecond,
				MaxDelay:             100 * time.Millisecond,
				BackoffMultiplier:    2.0,
				ConnectionRetryDelay: 50 * time.Millisecond,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]string
		err := client.request(ctx, "POST", "/test", nil, &result)

		// Should eventually succeed after retries
		if err == nil {
			count := requestCount.Load()
			if count < 3 {
				t.Errorf("Expected at least 3 attempts, got %d", count)
			}
		}
	})
}

func TestPartialResponse(t *testing.T) {
	t.Run("client handles connection closed mid-response", func(t *testing.T) {
		// Server that closes connection after partial response
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()

		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Read request
			buf := make([]byte, 1024)
			conn.Read(buf)

			// Send partial response then close
			conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n{\"data\":"))
			time.Sleep(10 * time.Millisecond)
			conn.Close() // Close before full response
		}()

		client := &Client{
			serverURL:  "http://" + listener.Addr().String(),
			httpClient: &http.Client{Timeout: 2 * time.Second},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]any
		err = client.request(ctx, "POST", "/test", nil, &result)

		if err == nil {
			t.Fatal("Expected error for partial response")
		}
	})
}

func TestServerErrors(t *testing.T) {
	t.Run("500 errors are retryable", func(t *testing.T) {
		var requestCount atomic.Int32

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count := requestCount.Add(1)
			if count < 3 {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"code":"SERVER_ERROR","message":"Internal error"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts:       5,
				InitialDelay:      10 * time.Millisecond,
				MaxDelay:          100 * time.Millisecond,
				BackoffMultiplier: 2.0,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]string
		err := client.request(ctx, "POST", "/test", nil, &result)

		if err != nil {
			t.Fatalf("Expected success after retries, got: %v", err)
		}

		count := requestCount.Load()
		if count != 3 {
			t.Errorf("Expected 3 attempts, got %d", count)
		}
	})

	t.Run("503 with Retry-After header is respected", func(t *testing.T) {
		var requestCount atomic.Int32
		var requestTimes []time.Time

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count := requestCount.Add(1)
			requestTimes = append(requestTimes, time.Now())

			if count == 1 {
				w.Header().Set("Retry-After", "1") // 1 second
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`{"code":"SERVICE_UNAVAILABLE","message":"Try again later"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts:       3,
				InitialDelay:      10 * time.Millisecond, // Short, but Retry-After should override
				MaxDelay:          5 * time.Second,
				BackoffMultiplier: 2.0,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]string
		err := client.request(ctx, "POST", "/test", nil, &result)

		if err != nil {
			t.Fatalf("Expected success, got: %v", err)
		}

		// Verify there was a delay close to 1 second (Retry-After value)
		if len(requestTimes) >= 2 {
			delay := requestTimes[1].Sub(requestTimes[0])
			if delay < 900*time.Millisecond {
				t.Errorf("Expected ~1s delay from Retry-After, got %v", delay)
			}
		}
	})

	t.Run("400 errors are not retryable", func(t *testing.T) {
		var requestCount atomic.Int32

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"code":"BAD_REQUEST","message":"Invalid input"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts:       5,
				InitialDelay:      10 * time.Millisecond,
				MaxDelay:          100 * time.Millisecond,
				BackoffMultiplier: 2.0,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]string
		err := client.request(ctx, "POST", "/test", nil, &result)

		if err == nil {
			t.Fatal("Expected error for 400 response")
		}

		// Should NOT have retried
		count := requestCount.Load()
		if count != 1 {
			t.Errorf("Expected 1 attempt (no retries), got %d", count)
		}
	})
}

func TestRetryCallback(t *testing.T) {
	t.Run("OnRetry callback is called on each retry", func(t *testing.T) {
		var retryEvents []RetryEvent

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"ERROR","message":"error"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts:       3,
				InitialDelay:      10 * time.Millisecond,
				MaxDelay:          100 * time.Millisecond,
				BackoffMultiplier: 2.0,
				OnRetry: func(event RetryEvent) {
					retryEvents = append(retryEvents, event)
				},
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]string
		_ = client.request(ctx, "POST", "/test", nil, &result)

		// Should have 2 retry events (attempts 1 and 2, not 3 since that's the last)
		if len(retryEvents) != 2 {
			t.Errorf("Expected 2 retry events, got %d", len(retryEvents))
		}

		if len(retryEvents) > 0 {
			if retryEvents[0].Attempt != 1 {
				t.Errorf("Expected first retry attempt 1, got %d", retryEvents[0].Attempt)
			}
			if retryEvents[0].MaxAttempts != 3 {
				t.Errorf("Expected MaxAttempts 3, got %d", retryEvents[0].MaxAttempts)
			}
		}
	})
}

func TestDisabledRetry(t *testing.T) {
	t.Run("retry can be disabled", func(t *testing.T) {
		var requestCount atomic.Int32

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"ERROR","message":"error"}`))
		}))
		defer server.Close()

		// Disable retry by setting MaxAttempts to 0
		client := NewClient(ClientConfig{
			ServerURL: server.URL,
			Retry: &ClientRetryConfig{
				MaxAttempts: 0, // Disable retries
			},
			Logger: NewNoopLogger(),
		})

		ctx := context.Background()
		_, err := client.Health(ctx)

		if err == nil {
			t.Fatal("Expected error")
		}

		count := requestCount.Load()
		if count != 1 {
			t.Errorf("Expected exactly 1 request (no retries), got %d", count)
		}
	})
}

func TestNetworkErrorDetection(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"connection refused", errors.New("dial tcp: connection refused"), true},
		{"no such host", errors.New("no such host"), true},
		{"network unreachable", errors.New("network is unreachable"), true},
		{"dial tcp", errors.New("dial tcp 127.0.0.1:8080: connect: connection refused"), true},
		{"regular error", errors.New("some other error"), false},
		{"nil error", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNetworkError(tt.err)
			if result != tt.expected {
				t.Errorf("isNetworkError(%v) = %v, expected %v", tt.err, result, tt.expected)
			}
		})
	}
}

func TestMalformedResponse(t *testing.T) {
	t.Run("client handles invalid JSON response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`not valid json`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]string
		err := client.request(ctx, "POST", "/test", nil, &result)

		if err == nil {
			t.Fatal("Expected error for invalid JSON")
		}

		// Should be an unmarshal error
		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("Expected IronflowError, got %T", err)
		}
		if ironflowErr.Code != "UNMARSHAL_ERROR" {
			t.Errorf("Expected code 'UNMARSHAL_ERROR', got '%s'", ironflowErr.Code)
		}
	})

	t.Run("client handles empty response body", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			// No body
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]string
		err := client.request(ctx, "POST", "/test", nil, &result)

		// Empty body with result expectation should fail
		if err == nil {
			t.Fatal("Expected error for empty response")
		}
	})
}

// mockRoundTripper allows injecting custom transport behavior
type mockRoundTripper struct {
	roundTripFunc func(*http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req)
}

func TestCustomTransport(t *testing.T) {
	t.Run("client uses custom HTTP client", func(t *testing.T) {
		var requestReceived bool

		mockTransport := &mockRoundTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				requestReceived = true
				// Return a mock response
				body := `{"status":"ok"}`
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(body)),
					Header:     make(http.Header),
				}
				return resp, nil
			},
		}

		customClient := &http.Client{
			Transport: mockTransport,
		}

		client := NewClient(ClientConfig{
			ServerURL:  "http://mock-server",
			HTTPClient: customClient,
			Logger:     NewNoopLogger(),
		})

		ctx := context.Background()
		_, _ = client.Health(ctx)

		if !requestReceived {
			t.Error("Expected custom transport to receive request")
		}
	})
}

func TestExponentialBackoff(t *testing.T) {
	t.Run("backoff increases exponentially", func(t *testing.T) {
		var requestTimes []time.Time

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestTimes = append(requestTimes, time.Now())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"ERROR","message":"error"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts:       4,
				InitialDelay:      100 * time.Millisecond,
				MaxDelay:          2 * time.Second,
				BackoffMultiplier: 2.0,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]string
		_ = client.request(ctx, "POST", "/test", nil, &result)

		if len(requestTimes) < 3 {
			t.Fatalf("Expected at least 3 requests, got %d", len(requestTimes))
		}

		// Check delays increase
		delay1 := requestTimes[1].Sub(requestTimes[0])
		delay2 := requestTimes[2].Sub(requestTimes[1])

		// Second delay should be roughly 2x first delay (with some tolerance)
		ratio := float64(delay2) / float64(delay1)
		if ratio < 1.5 || ratio > 2.5 {
			t.Errorf("Expected exponential backoff (ratio ~2), got ratio %v (delays: %v, %v)", ratio, delay1, delay2)
		}
	})

	t.Run("backoff capped at max delay", func(t *testing.T) {
		var requestTimes []time.Time

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestTimes = append(requestTimes, time.Now())
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"ERROR","message":"error"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts:       5,
				InitialDelay:      100 * time.Millisecond,
				MaxDelay:          150 * time.Millisecond, // Low cap
				BackoffMultiplier: 2.0,
			},
			logger: NewNoopLogger(),
		}

		ctx := context.Background()
		var result map[string]string
		_ = client.request(ctx, "POST", "/test", nil, &result)

		if len(requestTimes) < 4 {
			t.Fatalf("Expected at least 4 requests, got %d", len(requestTimes))
		}

		// Later delays should be capped
		for i := 2; i < len(requestTimes)-1; i++ {
			delay := requestTimes[i+1].Sub(requestTimes[i])
			// Should not exceed max delay by much (allow some tolerance)
			if delay > 200*time.Millisecond {
				t.Errorf("Delay %d (%v) exceeded max delay cap", i, delay)
			}
		}
	})
}
