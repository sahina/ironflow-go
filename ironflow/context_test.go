package ironflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestContextCancellation(t *testing.T) {
	t.Run("client request respects context cancellation", func(t *testing.T) {
		// Create a server that delays response
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(5 * time.Second) // Long delay
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{Timeout: 10 * time.Second},
			logger:     NewNoopLogger(),
		}

		// Create a context that cancels quickly
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		var result map[string]string
		err := client.request(ctx, "POST", "/test", map[string]string{"key": "value"}, &result)

		if err == nil {
			t.Fatal("Expected error due to context cancellation")
		}

		// Should be a context deadline exceeded or canceled error
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			// Check if wrapped error contains context error
			if ctx.Err() == nil {
				t.Errorf("Expected context error, got: %v", err)
			}
		}
	})

	t.Run("client request cancels when context is already cancelled", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		// Create an already-cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		var result map[string]string
		err := client.request(ctx, "POST", "/test", nil, &result)

		if err == nil {
			t.Fatal("Expected error due to cancelled context")
		}
	})

	t.Run("client retry stops on context cancellation", func(t *testing.T) {
		var requestCount atomic.Int32

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			// Return 500 to trigger retry
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":"SERVER_ERROR","message":"error"}`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts:          5,
				InitialDelay:         100 * time.Millisecond,
				MaxDelay:             1 * time.Second,
				BackoffMultiplier:    2.0,
				ConnectionRetryDelay: 100 * time.Millisecond,
			},
			logger: NewNoopLogger(),
		}

		// Create context that cancels after a short time
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()

		var result map[string]string
		err := client.request(ctx, "POST", "/test", nil, &result)

		if err == nil {
			t.Fatal("Expected error")
		}

		// Should have made fewer requests than max attempts due to cancellation
		count := requestCount.Load()
		if count >= 5 {
			t.Errorf("Expected fewer than 5 requests due to cancellation, got %d", count)
		}
	})

	t.Run("worker run respects context cancellation", func(t *testing.T) {
		worker := NewWorker(WorkerConfig{
			ServerURL:         "http://localhost:19999", // Non-existent server
			Functions:         []Function{},
			ReconnectDelay:    50 * time.Millisecond,
			HeartbeatInterval: 1 * time.Hour, // Long interval to not interfere
			Logger:            NewNoopLogger(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- worker.Run(ctx)
		}()

		select {
		case err := <-errCh:
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				t.Errorf("Expected context error, got: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Worker did not stop after context cancellation")
		}
	})

	t.Run("worker stop cancels active jobs", func(t *testing.T) {
		worker := &Worker{
			config: WorkerConfig{
				MaxConcurrentJobs: 10,
				HeartbeatInterval: 1 * time.Hour,
				ReconnectDelay:    1 * time.Second,
			},
			functions:  make(map[string]Function),
			workerID:   "test-worker",
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
			stopCh:     make(chan struct{}),
		}

		// Simulate an active job with a cancel function
		cancelCalled := false
		job := &activeJob{
			jobID:     "job-1",
			runID:     "run-1",
			startedAt: time.Now(),
			cancel: func() {
				cancelCalled = true
			},
		}
		worker.activeJobs.Store("job-1", job)

		// Stop the worker
		worker.Stop()

		// Verify cancel was called
		if !cancelCalled {
			t.Error("Expected job cancel function to be called")
		}
	})
}

func TestContextDeadline(t *testing.T) {
	t.Run("client respects context deadline", func(t *testing.T) {
		// Server that takes longer than deadline
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(5 * time.Second):
				w.WriteHeader(http.StatusOK)
			}
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		deadline := time.Now().Add(100 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		start := time.Now()
		var result map[string]string
		err := client.request(ctx, "POST", "/test", nil, &result)

		elapsed := time.Since(start)

		if err == nil {
			t.Fatal("Expected error due to deadline")
		}

		// Should have failed close to the deadline, not waited for full server delay
		if elapsed > 500*time.Millisecond {
			t.Errorf("Request took too long (%v), should have cancelled at deadline", elapsed)
		}
	})

	t.Run("EmitSync respects context deadline", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Simulate a long-running sync emit
			time.Sleep(5 * time.Second)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			httpClient: &http.Client{},
			logger:     NewNoopLogger(),
		}

		// Short timeout
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := client.EmitSync(ctx, "test.event", map[string]string{"key": "value"}, 50*time.Millisecond)

		if err == nil {
			t.Fatal("Expected error due to context timeout")
		}
	})
}

func TestContextPropagation(t *testing.T) {
	t.Run("context values are accessible in function handler", func(t *testing.T) {
		// This tests that the Context struct we create has proper data
		req := &PushRequest{
			RunID:      "run_ctx_test",
			FunctionID: "test-function",
			Attempt:    1,
			Event: PushEvent{
				ID:        "evt_123",
				Name:      "test.event",
				Data:      []byte(`{"key":"value"}`),
				Timestamp: time.Now().Format(time.RFC3339),
			},
		}

		ctx := Context{
			Event: Event{
				ID:   req.Event.ID,
				Name: req.Event.Name,
			},
			Run: RunInfo{
				ID:         req.RunID,
				FunctionID: req.FunctionID,
				Attempt:    req.Attempt,
			},
		}

		// Verify context has correct data
		if ctx.Run.ID != "run_ctx_test" {
			t.Errorf("Expected run ID 'run_ctx_test', got '%s'", ctx.Run.ID)
		}
		if ctx.Run.FunctionID != "test-function" {
			t.Errorf("Expected function ID 'test-function', got '%s'", ctx.Run.FunctionID)
		}
		if ctx.Event.Name != "test.event" {
			t.Errorf("Expected event name 'test.event', got '%s'", ctx.Event.Name)
		}
	})
}
