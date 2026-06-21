package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// NewWorker defaults
// ============================================================================

func TestNewWorker_Defaults(t *testing.T) {
	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	w := NewWorker(WorkerConfig{
		ServerURL: "http://example.com",
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	if w.config.MaxConcurrentJobs != DefaultWorkerMaxConcurrentJobs {
		t.Errorf("expected MaxConcurrentJobs=%d, got %d",
			DefaultWorkerMaxConcurrentJobs, w.config.MaxConcurrentJobs)
	}
	if w.config.HeartbeatInterval != DefaultWorkerHeartbeatInterval {
		t.Errorf("expected HeartbeatInterval=%v, got %v",
			DefaultWorkerHeartbeatInterval, w.config.HeartbeatInterval)
	}
	if w.config.ReconnectDelay != DefaultWorkerReconnectDelay {
		t.Errorf("expected ReconnectDelay=%v, got %v",
			DefaultWorkerReconnectDelay, w.config.ReconnectDelay)
	}
	if w.workerID == "" {
		t.Error("expected workerID to be generated")
	}
	if !strings.HasPrefix(w.workerID, "worker-") {
		t.Errorf("expected workerID to start with 'worker-', got %q", w.workerID)
	}
	if w.httpClient == nil {
		t.Error("expected httpClient to be initialized")
	}
	if w.state.Load() != int32(stateIdle) {
		t.Errorf("expected initial state to be stateIdle (0), got %d", w.state.Load())
	}
}

func TestNewWorker_CustomConfig(t *testing.T) {
	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	w := NewWorker(WorkerConfig{
		ServerURL:         "http://custom:8080",
		Functions:         []Function{fn},
		MaxConcurrentJobs: 5,
		HeartbeatInterval: 10 * time.Second,
		ReconnectDelay:    2 * time.Second,
		Labels:            map[string]string{"gpu": "a100"},
		Logger:            NewNoopLogger(),
	})

	if w.config.MaxConcurrentJobs != 5 {
		t.Errorf("expected MaxConcurrentJobs=5, got %d", w.config.MaxConcurrentJobs)
	}
	if w.config.HeartbeatInterval != 10*time.Second {
		t.Errorf("expected HeartbeatInterval=10s, got %v", w.config.HeartbeatInterval)
	}
	if w.config.ReconnectDelay != 2*time.Second {
		t.Errorf("expected ReconnectDelay=2s, got %v", w.config.ReconnectDelay)
	}
	if w.config.ServerURL != "http://custom:8080" {
		t.Errorf("expected ServerURL='http://custom:8080', got %q", w.config.ServerURL)
	}
	if w.config.Labels["gpu"] != "a100" {
		t.Errorf("expected label gpu=a100, got %v", w.config.Labels)
	}
}

// ============================================================================
// NewWorker builds function map
// ============================================================================

func TestNewWorker_BuildsFunctionMap(t *testing.T) {
	fn1 := CreateFunction(FunctionConfig{
		ID:       "fn-alpha",
		Triggers: []Trigger{{Event: "alpha.event"}},
	}, func(ctx Context) (any, error) {
		return "alpha", nil
	})

	fn2 := CreateFunction(FunctionConfig{
		ID:       "fn-beta",
		Triggers: []Trigger{{Event: "beta.event"}},
	}, func(ctx Context) (any, error) {
		return "beta", nil
	})

	w := NewWorker(WorkerConfig{
		ServerURL: "http://example.com",
		Functions: []Function{fn1, fn2},
		Logger:    NewNoopLogger(),
	})

	if len(w.functions) != 2 {
		t.Fatalf("expected 2 functions in map, got %d", len(w.functions))
	}

	if _, ok := w.functions["fn-alpha"]; !ok {
		t.Error("expected fn-alpha in function map")
	}
	if _, ok := w.functions["fn-beta"]; !ok {
		t.Error("expected fn-beta in function map")
	}
}

// ============================================================================
// Duplicate function IDs warn but don't panic
// ============================================================================

func TestNewWorker_DuplicateFunctionWarning(t *testing.T) {
	fn1 := CreateFunction(FunctionConfig{
		ID:       "dup-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return "first", nil
	})
	fn2 := CreateFunction(FunctionConfig{
		ID:       "dup-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return "second", nil
	})

	// Should not panic — just warn. The later function overwrites.
	w := NewWorker(WorkerConfig{
		ServerURL: "http://example.com",
		Functions: []Function{fn1, fn2},
		Logger:    NewNoopLogger(),
	})

	if len(w.functions) != 1 {
		t.Fatalf("expected 1 function in map (deduped), got %d", len(w.functions))
	}
	if _, ok := w.functions["dup-fn"]; !ok {
		t.Error("expected dup-fn in function map")
	}
}

// ============================================================================
// Stop() transitions state
// ============================================================================

func TestWorker_Stop(t *testing.T) {
	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	w := NewWorker(WorkerConfig{
		ServerURL: "http://example.com",
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	w.Stop()

	if w.state.Load() != int32(stateStopped) {
		t.Errorf("expected state to be stateStopped (%d), got %d", stateStopped, w.state.Load())
	}

	// Calling Stop() again should not panic (double close protection)
	w.Stop()
}

func TestWorker_Stop_CancelsActiveJobs(t *testing.T) {
	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	w := NewWorker(WorkerConfig{
		ServerURL: "http://example.com",
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	// Simulate an active job
	_, cancel := context.WithCancel(context.Background())
	var cancelledByStop atomic.Bool

	wrappedCancel := func() {
		cancelledByStop.Store(true)
		cancel()
	}

	w.activeJobs.Store("job-1", &activeJob{
		jobID:     "job-1",
		runID:     "run-1",
		startedAt: time.Now(),
		cancel:    wrappedCancel,
	})

	w.Stop()

	if !cancelledByStop.Load() {
		t.Error("expected Stop() to cancel active jobs")
	}
}

// ============================================================================
// Drain() waits for active jobs then stops
// ============================================================================

func TestWorker_Drain_WithNoJobs(t *testing.T) {
	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	w := NewWorker(WorkerConfig{
		ServerURL: "http://example.com",
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	// Set to connected state so Drain does something meaningful
	w.state.Store(int32(stateConnected))

	w.Drain()

	if w.state.Load() != int32(stateStopped) {
		t.Errorf("expected state to be stateStopped (%d), got %d", stateStopped, w.state.Load())
	}
}

func TestWorker_Drain_AlreadyStopped(t *testing.T) {
	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	w := NewWorker(WorkerConfig{
		ServerURL: "http://example.com",
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	w.state.Store(int32(stateStopped))
	// Should return immediately without hanging
	w.Drain()
}

// ============================================================================
// Worker registration
// ============================================================================

func TestWorker_Registration(t *testing.T) {
	var receivedBody map[string]any
	var receivedMethod string
	var receivedPath string
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		receivedMethod = r.Method
		receivedPath = r.URL.Path

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
		}
		_ = json.Unmarshal(body, &receivedBody)

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "process-video",
		Triggers: []Trigger{{Event: "video.uploaded"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL:         server.URL,
		Functions:         []Function{fn},
		MaxConcurrentJobs: 4,
		Labels:            map[string]string{"gpu": "nvidia"},
		Logger:            NewNoopLogger(),
	})

	err := worker.registerWorker(context.Background())
	if err != nil {
		t.Fatalf("registerWorker failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// The post() method uses PUT (not POST) per the implementation
	if receivedMethod != "PUT" {
		t.Errorf("expected method PUT, got %s", receivedMethod)
	}

	expectedPath := "/api/v1/workers/" + worker.workerID + "/register"
	if receivedPath != expectedPath {
		t.Errorf("expected path %q, got %q", expectedPath, receivedPath)
	}

	if receivedBody["worker_id"] != worker.workerID {
		t.Errorf("expected worker_id=%q, got %v", worker.workerID, receivedBody["worker_id"])
	}

	maxJobs, ok := receivedBody["max_concurrent_jobs"].(float64)
	if !ok || int(maxJobs) != 4 {
		t.Errorf("expected max_concurrent_jobs=4, got %v", receivedBody["max_concurrent_jobs"])
	}

	labels, ok := receivedBody["labels"].(map[string]any)
	if !ok {
		t.Fatalf("expected labels to be a map, got %T", receivedBody["labels"])
	}
	if labels["gpu"] != "nvidia" {
		t.Errorf("expected label gpu=nvidia, got %v", labels["gpu"])
	}

	functionIDs, ok := receivedBody["function_ids"].([]any)
	if !ok {
		t.Fatalf("expected function_ids to be an array, got %T", receivedBody["function_ids"])
	}
	if len(functionIDs) != 1 || functionIDs[0] != "process-video" {
		t.Errorf("expected function_ids=[process-video], got %v", functionIDs)
	}

	version, ok := receivedBody["version"].(map[string]any)
	if !ok {
		t.Fatalf("expected version to be a map, got %T", receivedBody["version"])
	}
	if version["runtime"] != "go" {
		t.Errorf("expected version.runtime=go, got %v", version["runtime"])
	}
	if version["sdk"] != SDKVersion {
		t.Errorf("expected version.sdk=%q, got %v", SDKVersion, version["sdk"])
	}
	if version["sdk"] == "0.1.0" {
		t.Errorf("version.sdk is hardcoded to the stale 0.1.0 literal (issue #461)")
	}
}

// ============================================================================
// Worker polls for jobs and executes them
// ============================================================================

func TestWorker_PollAndExecuteJob(t *testing.T) {
	var mu sync.Mutex
	var jobCompletedPath string
	var jobCompletedBody map[string]any
	jobRequests := 0

	fn := CreateFunction(FunctionConfig{
		ID:       "my-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return map[string]string{"result": "done"}, nil
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.Contains(path, "/RegisterFunction"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"created": true})
		case strings.HasSuffix(path, "/register"):
			// Registration endpoint
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/heartbeat"):
			// Heartbeat endpoint
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/jobs") && r.Method == "GET":
			// Job request endpoint
			jobRequests++
			if jobRequests == 1 {
				// Return a job on first request
				job := jobAssignment{
					JobID:      "job-001",
					RunID:      "run-001",
					FunctionID: "my-fn",
					Attempt:    1,
					Event: jobEvent{
						ID:        "evt-001",
						Name:      "test.event",
						Version:   1,
						Data:      json.RawMessage(`{"key":"value"}`),
						Timestamp: "2024-01-01T00:00:00Z",
					},
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(job)
			} else {
				// No more jobs
				w.WriteHeader(http.StatusNoContent)
			}

		case r.Method == "PUT" && strings.Contains(path, "/jobs/"):
			// Job completed/failed endpoint
			jobCompletedPath = path
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &jobCompletedBody)
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	worker := NewWorker(WorkerConfig{
		ServerURL:         server.URL,
		Functions:         []Function{fn},
		MaxConcurrentJobs: 1,
		HeartbeatInterval: 1 * time.Hour, // long so it doesn't fire during test
		ReconnectDelay:    100 * time.Millisecond,
		Logger:            NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short time to let the worker process the job
	go func() {
		// Wait until job is processed
		for i := 0; i < 50; i++ {
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			done := jobCompletedBody != nil
			mu.Unlock()
			if done {
				break
			}
		}
		// Give a bit more time for cleanup
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_ = worker.Run(ctx)

	mu.Lock()
	defer mu.Unlock()

	expectedPath := "/api/v1/workers/" + worker.workerID + "/jobs/job-001"
	if jobCompletedPath != expectedPath {
		t.Errorf("expected completed path %q, got %q", expectedPath, jobCompletedPath)
	}

	if jobCompletedBody == nil {
		t.Fatal("expected job completed body to be sent")
	}

	if jobCompletedBody["status"] != "completed" {
		t.Errorf("expected status=completed, got %v", jobCompletedBody["status"])
	}

	output, ok := jobCompletedBody["output"].(map[string]any)
	if !ok {
		t.Fatalf("expected output to be a map, got %T", jobCompletedBody["output"])
	}
	if output["result"] != "done" {
		t.Errorf("expected output.result=done, got %v", output["result"])
	}
}

// ============================================================================
// Worker poll delivers event metadata to handler
// ============================================================================

func TestWorker_PollDeliversMetadataToHandler(t *testing.T) {
	var mu sync.Mutex
	var receivedMetadata map[string]any
	var handlerCalled bool
	jobRequests := 0

	fn := CreateFunction(FunctionConfig{
		ID:       "meta-fn",
		Triggers: []Trigger{{Event: "order.placed"}},
	}, func(ctx Context) (any, error) {
		mu.Lock()
		receivedMetadata = ctx.Event.Metadata
		handlerCalled = true
		mu.Unlock()
		return map[string]string{"ok": "true"}, nil
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		path := r.URL.Path
		switch {
		case strings.Contains(path, "/RegisterFunction"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"created": true})
		case strings.HasSuffix(path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(path, "/heartbeat"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(path, "/jobs") && r.Method == "GET":
			jobRequests++
			if jobRequests == 1 {
				job := jobAssignment{
					JobID:      "job-meta-001",
					RunID:      "run-meta-001",
					FunctionID: "meta-fn",
					Attempt:    1,
					Event: jobEvent{
						ID:        "evt-meta",
						Name:      "order.placed",
						Version:   1,
						Data:      json.RawMessage(`{"orderId":"o-1"}`),
						Timestamp: "2024-01-01T00:00:00Z",
						Metadata:  json.RawMessage(`{"causationId":"cmd-001","correlationId":"corr-xyz","tenantId":"tenant-42"}`),
					},
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(job)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		case r.Method == "PUT" && strings.Contains(path, "/jobs/"):
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	worker := NewWorker(WorkerConfig{
		ServerURL:         server.URL,
		Functions:         []Function{fn},
		MaxConcurrentJobs: 1,
		HeartbeatInterval: 1 * time.Hour,
		ReconnectDelay:    100 * time.Millisecond,
		Logger:            NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for i := 0; i < 50; i++ {
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			done := handlerCalled
			mu.Unlock()
			if done {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_ = worker.Run(ctx)

	mu.Lock()
	defer mu.Unlock()

	if !handlerCalled {
		t.Fatal("expected handler to be called")
	}
	if receivedMetadata == nil {
		t.Fatal("expected handler to receive metadata")
	}
	if receivedMetadata["causationId"] != "cmd-001" {
		t.Errorf("expected causationId=cmd-001, got %v", receivedMetadata["causationId"])
	}
	if receivedMetadata["correlationId"] != "corr-xyz" {
		t.Errorf("expected correlationId=corr-xyz, got %v", receivedMetadata["correlationId"])
	}
	if receivedMetadata["tenantId"] != "tenant-42" {
		t.Errorf("expected tenantId=tenant-42, got %v", receivedMetadata["tenantId"])
	}
}

// ============================================================================
// Worker sends job failed
// ============================================================================

func TestWorker_SendsJobFailed(t *testing.T) {
	var mu sync.Mutex
	var jobResultBody map[string]any
	jobRequests := 0

	fn := CreateFunction(FunctionConfig{
		ID:       "failing-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, NewError("something went wrong", "CUSTOM_ERROR", false)
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.Contains(path, "/RegisterFunction"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"created": true})

		case strings.HasSuffix(path, "/heartbeat"):
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/jobs") && r.Method == "GET":
			jobRequests++
			if jobRequests == 1 {
				job := jobAssignment{
					JobID:      "job-fail-001",
					RunID:      "run-fail-001",
					FunctionID: "failing-fn",
					Attempt:    1,
					Event: jobEvent{
						ID:        "evt-001",
						Name:      "test.event",
						Version:   1,
						Data:      json.RawMessage(`{}`),
						Timestamp: "2024-01-01T00:00:00Z",
					},
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(job)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}

		case r.Method == "PUT" && strings.Contains(path, "/jobs/"):
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &jobResultBody)
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	worker := NewWorker(WorkerConfig{
		ServerURL:         server.URL,
		Functions:         []Function{fn},
		MaxConcurrentJobs: 1,
		HeartbeatInterval: 1 * time.Hour,
		ReconnectDelay:    100 * time.Millisecond,
		Logger:            NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for i := 0; i < 50; i++ {
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			done := jobResultBody != nil
			mu.Unlock()
			if done {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_ = worker.Run(ctx)

	mu.Lock()
	defer mu.Unlock()

	if jobResultBody == nil {
		t.Fatal("expected job result body to be sent")
	}

	if jobResultBody["status"] != "failed" {
		t.Errorf("expected status=failed, got %v", jobResultBody["status"])
	}

	errObj, ok := jobResultBody["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected error to be a map, got %T", jobResultBody["error"])
	}
	if errObj["message"] != "something went wrong" {
		t.Errorf("expected error message 'something went wrong', got %v", errObj["message"])
	}
	// NonRetryableError is not retryable
	if errObj["retryable"] != false {
		t.Errorf("expected retryable=false, got %v", errObj["retryable"])
	}
}

// ============================================================================
// Worker sends job failed when function is not found
// ============================================================================

func TestWorker_SendsJobFailed_FunctionNotFound(t *testing.T) {
	var mu sync.Mutex
	var jobResultBody map[string]any
	jobRequests := 0

	fn := CreateFunction(FunctionConfig{
		ID:       "existing-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.Contains(path, "/RegisterFunction"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"created": true})

		case strings.HasSuffix(path, "/heartbeat"):
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/jobs") && r.Method == "GET":
			jobRequests++
			if jobRequests == 1 {
				// Return a job for a function the worker doesn't have
				job := jobAssignment{
					JobID:      "job-nf-001",
					RunID:      "run-nf-001",
					FunctionID: "non-existent-fn",
					Attempt:    1,
					Event: jobEvent{
						ID:        "evt-001",
						Name:      "test.event",
						Version:   1,
						Data:      json.RawMessage(`{}`),
						Timestamp: "2024-01-01T00:00:00Z",
					},
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(job)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}

		case r.Method == "PUT" && strings.Contains(path, "/jobs/"):
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &jobResultBody)
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	worker := NewWorker(WorkerConfig{
		ServerURL:         server.URL,
		Functions:         []Function{fn},
		MaxConcurrentJobs: 1,
		HeartbeatInterval: 1 * time.Hour,
		ReconnectDelay:    100 * time.Millisecond,
		Logger:            NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for i := 0; i < 50; i++ {
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			done := jobResultBody != nil
			mu.Unlock()
			if done {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_ = worker.Run(ctx)

	mu.Lock()
	defer mu.Unlock()

	if jobResultBody == nil {
		t.Fatal("expected job result body to be sent")
	}

	if jobResultBody["status"] != "failed" {
		t.Errorf("expected status=failed, got %v", jobResultBody["status"])
	}

	errObj, ok := jobResultBody["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected error to be a map, got %T", jobResultBody["error"])
	}
	if !strings.Contains(errObj["message"].(string), "function not found") {
		t.Errorf("expected error message to contain 'function not found', got %v", errObj["message"])
	}
	if errObj["code"] != "FUNCTION_NOT_FOUND" {
		t.Errorf("expected error code 'FUNCTION_NOT_FOUND', got %v", errObj["code"])
	}
	if errObj["retryable"] != false {
		t.Errorf("expected retryable=false, got %v", errObj["retryable"])
	}
}

// ============================================================================
// Worker heartbeat
// ============================================================================

func TestWorker_Heartbeat(t *testing.T) {
	var mu sync.Mutex
	heartbeatCount := 0
	var lastHeartbeatBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.Contains(path, "/RegisterFunction"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"created": true})

		case strings.HasSuffix(path, "/heartbeat"):
			heartbeatCount++
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &lastHeartbeatBody)
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/jobs") && r.Method == "GET":
			// No jobs available
			w.WriteHeader(http.StatusNoContent)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL:         server.URL,
		Functions:         []Function{fn},
		MaxConcurrentJobs: 2,
		HeartbeatInterval: 100 * time.Millisecond, // fast for testing
		ReconnectDelay:    100 * time.Millisecond,
		Logger:            NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())

	// Let it run long enough for at least 2 heartbeats
	go func() {
		time.Sleep(350 * time.Millisecond)
		cancel()
	}()

	_ = worker.Run(ctx)

	mu.Lock()
	defer mu.Unlock()

	if heartbeatCount < 2 {
		t.Errorf("expected at least 2 heartbeats, got %d", heartbeatCount)
	}

	if lastHeartbeatBody == nil {
		t.Fatal("expected heartbeat body")
	}

	if lastHeartbeatBody["worker_id"] != worker.workerID {
		t.Errorf("expected worker_id=%q in heartbeat, got %v",
			worker.workerID, lastHeartbeatBody["worker_id"])
	}

	activeJobs, ok := lastHeartbeatBody["active_jobs"].(float64)
	if !ok {
		t.Fatalf("expected active_jobs to be a number, got %T", lastHeartbeatBody["active_jobs"])
	}
	if int(activeJobs) != 0 {
		t.Errorf("expected 0 active_jobs, got %d", int(activeJobs))
	}
}

// ============================================================================
// Run blocks until context cancelled
// ============================================================================

func TestWorker_Run_BlocksUntilCancelled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.Contains(path, "/RegisterFunction"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"created": true})
		case strings.HasSuffix(path, "/heartbeat"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(path, "/jobs"):
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL:         server.URL,
		Functions:         []Function{fn},
		HeartbeatInterval: 1 * time.Hour,
		Logger:            NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- worker.Run(ctx)
	}()

	// Verify it is still running
	select {
	case <-done:
		t.Fatal("Run returned before cancel was called")
	case <-time.After(100 * time.Millisecond):
		// Good -- still blocking
	}

	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// ============================================================================
// Run returns error if already running
// ============================================================================

func TestWorker_Run_AlreadyRunning(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.Contains(path, "/RegisterFunction"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"created": true})
		case strings.HasSuffix(path, "/heartbeat"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(path, "/jobs"):
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL:         server.URL,
		Functions:         []Function{fn},
		HeartbeatInterval: 1 * time.Hour,
		Logger:            NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the worker in a goroutine
	go func() {
		_ = worker.Run(ctx)
	}()

	// Wait for it to transition past idle
	time.Sleep(100 * time.Millisecond)

	// Try to run again — should fail
	err := worker.Run(context.Background())
	if err == nil {
		t.Fatal("expected error when calling Run on an already-running worker")
	}

	var ironflowErr *IronflowError
	if ok := err.(*IronflowError); ok != nil {
		ironflowErr = ok
	}

	if ironflowErr == nil {
		t.Fatalf("expected IronflowError, got %T: %v", err, err)
		return
	}
	if ironflowErr.Code != "WORKER_ALREADY_RUNNING" {
		t.Errorf("expected code WORKER_ALREADY_RUNNING, got %q", ironflowErr.Code)
	}
}

// ============================================================================
// Run stops via Stop()
// ============================================================================

func TestWorker_Run_StoppedViaStop(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.Contains(path, "/RegisterFunction"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"created": true})
		case strings.HasSuffix(path, "/heartbeat"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(path, "/jobs"):
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL:         server.URL,
		Functions:         []Function{fn},
		HeartbeatInterval: 1 * time.Hour,
		Logger:            NewNoopLogger(),
	})

	done := make(chan error, 1)

	go func() {
		done <- worker.Run(context.Background())
	}()

	// Wait for worker to be connected
	time.Sleep(200 * time.Millisecond)

	worker.Stop()

	select {
	case err := <-done:
		// Run should return nil when stopped via Stop()
		if err != nil {
			t.Errorf("expected nil error from Stop(), got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after Stop()")
	}
}

// ============================================================================
// requestJob returns nil when no jobs (204)
// ============================================================================

func TestWorker_RequestJob_NoContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	jobs, err := worker.requestJobs(context.Background(), 1)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("expected no jobs for 204 response, got %d", len(jobs))
	}
}

// ============================================================================
// requestJob parses job assignment
// ============================================================================

func TestWorker_RequestJob_ReturnsJob(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		job := jobAssignment{
			JobID:      "job-123",
			RunID:      "run-456",
			FunctionID: "my-fn",
			Attempt:    2,
			Event: jobEvent{
				ID:        "evt-789",
				Name:      "order.placed",
				Version:   1,
				Data:      json.RawMessage(`{"orderId":"abc"}`),
				Timestamp: "2024-06-15T10:30:00Z",
			},
			CompletedSteps: []completedStep{
				{StepID: "step-1", Name: "validate", Output: "ok"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(job)
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "my-fn",
		Triggers: []Trigger{{Event: "order.placed"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	jobs, err := worker.requestJobs(context.Background(), 1)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job (legacy single-object), got %d", len(jobs))
	}
	job := jobs[0]

	if job.JobID != "job-123" {
		t.Errorf("expected JobID=job-123, got %q", job.JobID)
	}
	if job.RunID != "run-456" {
		t.Errorf("expected RunID=run-456, got %q", job.RunID)
	}
	if job.FunctionID != "my-fn" {
		t.Errorf("expected FunctionID=my-fn, got %q", job.FunctionID)
	}
	if job.Attempt != 2 {
		t.Errorf("expected Attempt=2, got %d", job.Attempt)
	}
	if job.Event.Name != "order.placed" {
		t.Errorf("expected Event.Name=order.placed, got %q", job.Event.Name)
	}
	if len(job.CompletedSteps) != 1 {
		t.Fatalf("expected 1 completed step, got %d", len(job.CompletedSteps))
	}
	if job.CompletedSteps[0].StepID != "step-1" {
		t.Errorf("expected completed step ID=step-1, got %q", job.CompletedSteps[0].StepID)
	}
}

// ============================================================================
// requestJob returns error on unexpected status
// ============================================================================

func TestWorker_RequestJob_ErrorOnBadStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	jobs, err := worker.requestJobs(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
	if jobs != nil {
		t.Errorf("expected nil jobs on error, got %+v", jobs)
	}
	if !strings.Contains(err.Error(), "unexpected status: 500") {
		t.Errorf("expected 'unexpected status: 500' in error, got %q", err.Error())
	}
}

// ============================================================================
// sendJobCompleted sends correct body
// ============================================================================

func TestWorker_SendJobCompleted(t *testing.T) {
	var receivedBody map[string]any
	var receivedPath string
	var receivedMethod string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		receivedPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	steps := []*StepResult{
		{ID: "step-1", Name: "fetch", Type: "run", Status: "completed"},
	}
	output := map[string]string{"result": "success"}

	reporter := &httpJobReporter{worker: worker}
	err := reporter.ReportCompleted(context.Background(), "job-abc", output, steps)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if receivedMethod != "PUT" {
		t.Errorf("expected method PUT, got %s", receivedMethod)
	}

	expectedPath := "/api/v1/workers/" + worker.workerID + "/jobs/job-abc"
	if receivedPath != expectedPath {
		t.Errorf("expected path %q, got %q", expectedPath, receivedPath)
	}

	if receivedBody["status"] != "completed" {
		t.Errorf("expected status=completed, got %v", receivedBody["status"])
	}

	outputMap, ok := receivedBody["output"].(map[string]any)
	if !ok {
		t.Fatalf("expected output to be a map, got %T", receivedBody["output"])
	}
	if outputMap["result"] != "success" {
		t.Errorf("expected output.result=success, got %v", outputMap["result"])
	}

	stepsArr, ok := receivedBody["steps"].([]any)
	if !ok {
		t.Fatalf("expected steps to be an array, got %T", receivedBody["steps"])
	}
	if len(stepsArr) != 1 {
		t.Errorf("expected 1 step, got %d", len(stepsArr))
	}
}

// ============================================================================
// sendJobFailed sends correct body
// ============================================================================

func TestWorker_SendJobFailed(t *testing.T) {
	var receivedBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	pushErr := &PushError{
		Message:   "timeout exceeded",
		Code:      "TIMEOUT",
		Retryable: true,
	}

	reporter := &httpJobReporter{worker: worker}
	err := reporter.ReportFailed(context.Background(), "job-xyz", pushErr, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if receivedBody["status"] != "failed" {
		t.Errorf("expected status=failed, got %v", receivedBody["status"])
	}

	errObj, ok := receivedBody["error"].(map[string]any)
	if !ok {
		t.Fatalf("expected error to be a map, got %T", receivedBody["error"])
	}
	if errObj["message"] != "timeout exceeded" {
		t.Errorf("expected message 'timeout exceeded', got %v", errObj["message"])
	}
	if errObj["code"] != "TIMEOUT" {
		t.Errorf("expected code 'TIMEOUT', got %v", errObj["code"])
	}
	if errObj["retryable"] != true {
		t.Errorf("expected retryable=true, got %v", errObj["retryable"])
	}
}

// ============================================================================
// sendJobYielded sends correct body
// ============================================================================

func TestWorker_SendJobYielded(t *testing.T) {
	var receivedBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	yieldInfo := &YieldInfo{
		StepID: "step-sleep-1",
		Type:   "sleep",
		Until:  "2024-12-01T00:00:00Z",
	}

	reporter := &httpJobReporter{worker: worker}
	err := reporter.ReportYielded(context.Background(), "job-yield-1", yieldInfo)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if receivedBody["status"] != "yielded" {
		t.Errorf("expected status=yielded, got %v", receivedBody["status"])
	}

	yieldObj, ok := receivedBody["yield"].(map[string]any)
	if !ok {
		t.Fatalf("expected yield to be a map, got %T", receivedBody["yield"])
	}
	if yieldObj["step_id"] != "step-sleep-1" {
		t.Errorf("expected step_id=step-sleep-1, got %v", yieldObj["step_id"])
	}
	if yieldObj["type"] != "sleep" {
		t.Errorf("expected type=sleep, got %v", yieldObj["type"])
	}
	if yieldObj["until"] != "2024-12-01T00:00:00Z" {
		t.Errorf("expected until=2024-12-01T00:00:00Z, got %v", yieldObj["until"])
	}
}

// ============================================================================
// Worker reconnects on registration failure
// ============================================================================

func TestWorker_ReconnectsOnRegistrationFailure(t *testing.T) {
	var mu sync.Mutex
	registerAttempts := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.Contains(path, "/RegisterFunction"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"created": true})
		case strings.HasSuffix(path, "/register"):
			registerAttempts++
			if registerAttempts < 3 {
				// Fail first 2 registrations
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`server starting up`))
			} else {
				w.WriteHeader(http.StatusOK)
			}

		case strings.HasSuffix(path, "/heartbeat"):
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/jobs"):
			w.WriteHeader(http.StatusNoContent)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL:         server.URL,
		Functions:         []Function{fn},
		HeartbeatInterval: 1 * time.Hour,
		ReconnectDelay:    50 * time.Millisecond, // fast reconnect for testing
		Logger:            NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		// Wait enough time for 2 failed + 1 successful registration
		time.Sleep(500 * time.Millisecond)
		cancel()
	}()

	_ = worker.Run(ctx)

	mu.Lock()
	defer mu.Unlock()

	if registerAttempts < 3 {
		t.Errorf("expected at least 3 register attempts, got %d", registerAttempts)
	}
}

// ============================================================================
// Worker handles completed steps from job assignment (memoization)
// ============================================================================

func TestWorker_ExecuteJob_WithCompletedSteps(t *testing.T) {
	var mu sync.Mutex
	var jobResultBody map[string]any
	jobRequests := 0

	fn := CreateFunction(FunctionConfig{
		ID:       "step-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		// This function uses the event data
		return map[string]string{"processed": "true"}, nil
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.Contains(path, "/RegisterFunction"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"created": true})

		case strings.HasSuffix(path, "/heartbeat"):
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/jobs") && r.Method == "GET":
			jobRequests++
			if jobRequests == 1 {
				job := jobAssignment{
					JobID:      "job-resume-001",
					RunID:      "run-resume-001",
					FunctionID: "step-fn",
					Attempt:    2,
					Event: jobEvent{
						ID:        "evt-001",
						Name:      "test.event",
						Version:   1,
						Data:      json.RawMessage(`{"key":"value"}`),
						Timestamp: "2024-01-01T00:00:00Z",
					},
					CompletedSteps: []completedStep{
						{StepID: "step-1", Name: "validate", Output: "valid"},
						{StepID: "step-2", Name: "enrich", Output: map[string]any{"enriched": true}},
					},
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(job)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}

		case r.Method == "PUT" && strings.Contains(path, "/jobs/"):
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &jobResultBody)
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	worker := NewWorker(WorkerConfig{
		ServerURL:         server.URL,
		Functions:         []Function{fn},
		MaxConcurrentJobs: 1,
		HeartbeatInterval: 1 * time.Hour,
		ReconnectDelay:    100 * time.Millisecond,
		Logger:            NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for i := 0; i < 50; i++ {
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			done := jobResultBody != nil
			mu.Unlock()
			if done {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_ = worker.Run(ctx)

	mu.Lock()
	defer mu.Unlock()

	if jobResultBody == nil {
		t.Fatal("expected job result body to be sent")
	}

	if jobResultBody["status"] != "completed" {
		t.Errorf("expected status=completed, got %v", jobResultBody["status"])
	}
}

// ============================================================================
// post() returns error on 4xx/5xx
// ============================================================================

func TestWorker_Post_ErrorOnBadStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer server.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "test-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	worker := NewWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	err := worker.httpPut(context.Background(), "/test", map[string]string{"key": "val"}, nil)
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
	if !strings.Contains(err.Error(), "request failed") {
		t.Errorf("expected 'request failed' in error, got %q", err.Error())
	}
}

// ============================================================================
// generateWorkerID produces unique IDs
// ============================================================================

func TestGenerateWorkerID(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := generateWorkerID()
		if !strings.HasPrefix(id, "worker-") {
			t.Errorf("expected worker- prefix, got %q", id)
		}
		if ids[id] {
			t.Errorf("duplicate workerID generated: %q", id)
		}
		ids[id] = true
	}
}

// ============================================================================
// getHostname returns a value
// ============================================================================

func TestGetHostname(t *testing.T) {
	hostname := getHostname()
	if hostname == "" {
		t.Error("expected non-empty hostname")
	}
}

// ============================================================================
// Worker OnError callback
// ============================================================================

// makeOnErrorTestServer creates a test HTTP server that handles worker
// registration, heartbeats, and serves a single job on the first poll then
// returns 204. The PUT result is stored in the returned map. The returned
// mutex guards all server state and can be shared with OnError callbacks.
func makeOnErrorTestServer(t *testing.T, firstJob jobAssignment) (*httptest.Server, *sync.Mutex, *map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var body map[string]any
	jobRequests := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/register"):
			w.WriteHeader(http.StatusOK)
		case strings.Contains(path, "/RegisterFunction"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"created": true})
		case strings.HasSuffix(path, "/heartbeat"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(path, "/jobs") && r.Method == "GET":
			jobRequests++
			if jobRequests == 1 {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(firstJob)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		case r.Method == "PUT" && strings.Contains(path, "/jobs/"):
			b, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(b, &body)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &mu, &body
}

// runOnErrorWorker creates a Worker, runs it until the first job result is
// recorded (or timeout), then cancels. mu and jobResultBody must come from
// makeOnErrorTestServer.
func runOnErrorWorker(t *testing.T, serverURL string, fn Function, onError func(error, ErrorContext), mu *sync.Mutex, jobResultBody *map[string]any) {
	t.Helper()
	worker := NewWorker(WorkerConfig{
		ServerURL:         serverURL,
		Functions:         []Function{fn},
		MaxConcurrentJobs: 1,
		HeartbeatInterval: 1 * time.Hour,
		ReconnectDelay:    100 * time.Millisecond,
		Logger:            NewNoopLogger(),
		OnError:           onError,
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for i := 0; i < 50; i++ {
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			done := *jobResultBody != nil
			mu.Unlock()
			if done {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_ = worker.Run(ctx)
}

func TestWorker_OnError_CalledOnHandlerFailure(t *testing.T) {
	var capturedErr error
	var capturedCtx ErrorContext
	onErrorCalled := false

	fn := CreateFunction(FunctionConfig{
		ID:       "failing-fn",
		Triggers: []Trigger{{Event: "order.placed"}},
	}, func(ctx Context) (any, error) {
		return nil, fmt.Errorf("handler exploded")
	})

	server, mu, jobResultBody := makeOnErrorTestServer(t, jobAssignment{
		JobID: "job-err-001", RunID: "run-err-001", FunctionID: "failing-fn", Attempt: 3,
		Event: jobEvent{ID: "evt-001", Name: "order.placed", Version: 1, Data: json.RawMessage(`{}`), Timestamp: "2024-01-01T00:00:00Z"},
	})

	runOnErrorWorker(t, server.URL, fn, func(err error, ctx ErrorContext) {
		mu.Lock()
		defer mu.Unlock()
		onErrorCalled = true
		capturedErr = err
		capturedCtx = ctx
	}, mu, jobResultBody)

	mu.Lock()
	defer mu.Unlock()

	if !onErrorCalled {
		t.Fatal("expected OnError to be called")
	}
	if capturedErr == nil || capturedErr.Error() != "handler exploded" {
		t.Errorf("expected error 'handler exploded', got %v", capturedErr)
	}
	if capturedCtx.FunctionID != "failing-fn" {
		t.Errorf("expected FunctionID='failing-fn', got %q", capturedCtx.FunctionID)
	}
	if capturedCtx.JobID != "job-err-001" {
		t.Errorf("expected JobID='job-err-001', got %q", capturedCtx.JobID)
	}
	if capturedCtx.RunID != "run-err-001" {
		t.Errorf("expected RunID='run-err-001', got %q", capturedCtx.RunID)
	}
	if capturedCtx.Attempt != 3 {
		t.Errorf("expected Attempt=3, got %d", capturedCtx.Attempt)
	}
	if capturedCtx.EventName != "order.placed" {
		t.Errorf("expected EventName='order.placed', got %q", capturedCtx.EventName)
	}
}

func TestWorker_OnError_CalledOnFunctionNotFound(t *testing.T) {
	var capturedErr error
	var capturedCtx ErrorContext
	onErrorCalled := false

	fn := CreateFunction(FunctionConfig{
		ID:       "existing-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, nil
	})

	server, mu, jobResultBody := makeOnErrorTestServer(t, jobAssignment{
		JobID: "job-notfound-001", RunID: "run-notfound-001", FunctionID: "nonexistent-fn", Attempt: 1,
		Event: jobEvent{ID: "evt-001", Name: "test.event", Version: 1, Data: json.RawMessage(`{}`), Timestamp: "2024-01-01T00:00:00Z"},
	})

	runOnErrorWorker(t, server.URL, fn, func(err error, ctx ErrorContext) {
		mu.Lock()
		defer mu.Unlock()
		onErrorCalled = true
		capturedErr = err
		capturedCtx = ctx
	}, mu, jobResultBody)

	mu.Lock()
	defer mu.Unlock()

	if !onErrorCalled {
		t.Fatal("expected OnError to be called for function not found")
	}
	if capturedErr == nil || !strings.Contains(capturedErr.Error(), "function not found") {
		t.Errorf("expected 'function not found' error, got %v", capturedErr)
	}
	if capturedCtx.FunctionID != "nonexistent-fn" {
		t.Errorf("expected FunctionID='nonexistent-fn', got %q", capturedCtx.FunctionID)
	}
}

func TestWorker_OnError_NotCalledOnSuccess(t *testing.T) {
	onErrorCalled := false

	fn := CreateFunction(FunctionConfig{
		ID:       "success-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return map[string]string{"ok": "true"}, nil
	})

	server, mu, jobResultBody := makeOnErrorTestServer(t, jobAssignment{
		JobID: "job-ok-001", RunID: "run-ok-001", FunctionID: "success-fn", Attempt: 1,
		Event: jobEvent{ID: "evt-001", Name: "test.event", Version: 1, Data: json.RawMessage(`{}`), Timestamp: "2024-01-01T00:00:00Z"},
	})

	runOnErrorWorker(t, server.URL, fn, func(err error, ctx ErrorContext) {
		mu.Lock()
		defer mu.Unlock()
		onErrorCalled = true
	}, mu, jobResultBody)

	mu.Lock()
	defer mu.Unlock()

	if onErrorCalled {
		t.Error("expected OnError NOT to be called on success")
	}
	if (*jobResultBody)["status"] != "completed" {
		t.Errorf("expected status=completed, got %v", (*jobResultBody)["status"])
	}
}

func TestWorker_OnError_NotCalledOnYield(t *testing.T) {
	onErrorCalled := false

	fn := CreateFunction(FunctionConfig{
		ID:       "yielding-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		// Sleep triggers a yield signal (panic with *yieldSignal)
		Sleep(ctx, "wait", 1*time.Second)
		return nil, nil
	})

	server, mu, jobResultBody := makeOnErrorTestServer(t, jobAssignment{
		JobID: "job-yield-001", RunID: "run-yield-001", FunctionID: "yielding-fn", Attempt: 1,
		Event: jobEvent{ID: "evt-001", Name: "test.event", Version: 1, Data: json.RawMessage(`{}`), Timestamp: "2024-01-01T00:00:00Z"},
	})

	runOnErrorWorker(t, server.URL, fn, func(err error, ctx ErrorContext) {
		mu.Lock()
		defer mu.Unlock()
		onErrorCalled = true
	}, mu, jobResultBody)

	mu.Lock()
	defer mu.Unlock()

	if onErrorCalled {
		t.Error("expected OnError NOT to be called on yield")
	}
	if *jobResultBody == nil {
		t.Fatal("expected job result body to be sent")
	}
	if (*jobResultBody)["status"] != "yielded" {
		t.Errorf("expected status=yielded, got %v", (*jobResultBody)["status"])
	}
}

func TestWorker_OnError_NilDoesNotPanic(t *testing.T) {
	fn := CreateFunction(FunctionConfig{
		ID:       "failing-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, fmt.Errorf("kaboom")
	})

	server, mu, jobResultBody := makeOnErrorTestServer(t, jobAssignment{
		JobID: "job-nil-001", RunID: "run-nil-001", FunctionID: "failing-fn", Attempt: 1,
		Event: jobEvent{ID: "evt-001", Name: "test.event", Version: 1, Data: json.RawMessage(`{}`), Timestamp: "2024-01-01T00:00:00Z"},
	})

	// No OnError set — should not panic
	runOnErrorWorker(t, server.URL, fn, nil, mu, jobResultBody)

	mu.Lock()
	defer mu.Unlock()

	if *jobResultBody == nil {
		t.Fatal("expected job result body to be sent")
	}
	if (*jobResultBody)["status"] != "failed" {
		t.Errorf("expected status=failed, got %v", (*jobResultBody)["status"])
	}
}

func TestWorker_OnError_PanicIsRecovered(t *testing.T) {
	fn := CreateFunction(FunctionConfig{
		ID:       "failing-fn",
		Triggers: []Trigger{{Event: "test.event"}},
	}, func(ctx Context) (any, error) {
		return nil, fmt.Errorf("handler error")
	})

	server, mu, jobResultBody := makeOnErrorTestServer(t, jobAssignment{
		JobID: "job-panic-001", RunID: "run-panic-001", FunctionID: "failing-fn", Attempt: 1,
		Event: jobEvent{ID: "evt-001", Name: "test.event", Version: 1, Data: json.RawMessage(`{}`), Timestamp: "2024-01-01T00:00:00Z"},
	})

	// OnError panics — should be recovered, not crash the worker
	runOnErrorWorker(t, server.URL, fn, func(err error, ctx ErrorContext) {
		panic("callback panic!")
	}, mu, jobResultBody)

	mu.Lock()
	defer mu.Unlock()

	if *jobResultBody == nil {
		t.Fatal("expected job result body to be sent despite OnError panic")
	}
	if (*jobResultBody)["status"] != "failed" {
		t.Errorf("expected status=failed, got %v", (*jobResultBody)["status"])
	}
}
