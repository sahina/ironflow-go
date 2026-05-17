package ironflow

import (
	"encoding/json"
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
// Registration sends correct payload
// ============================================================================

func TestProjectionRunner_Register(t *testing.T) {
	var mu sync.Mutex
	var receivedBody map[string]any
	var receivedPath string
	var receivedContentType string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		receivedPath = r.URL.Path
		receivedContentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"registered"}`))
	}))
	defer server.Close()

	proj := CreateProjection(ProjectionConfig{
		Name:         "order-totals",
		Events:       []string{"order.created", "order.updated"},
		Mode:         ProjectionModeManaged,
		PartitionKey: "$.data.customerId",
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			return state, nil
		},
		InitialState: func() map[string]any { return map[string]any{"total": 0} },
	})

	runner := NewProjectionRunner(proj, server.URL, map[string]string{
		"Authorization": "Bearer test-key",
	}, NewNoopLogger())
	defer runner.Stop()

	err := runner.register()
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if receivedPath != "/ironflow.v1.ProjectionService/RegisterProjection" {
		t.Errorf("expected path /ironflow.v1.ProjectionService/RegisterProjection, got %q", receivedPath)
	}
	if receivedContentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", receivedContentType)
	}
	if receivedBody["name"] != "order-totals" {
		t.Errorf("expected name=order-totals, got %v", receivedBody["name"])
	}
	if receivedBody["mode"] != "managed" {
		t.Errorf("expected mode=managed, got %v", receivedBody["mode"])
	}
	if receivedBody["partitionKey"] != "$.data.customerId" {
		t.Errorf("expected partitionKey=$.data.customerId, got %v", receivedBody["partitionKey"])
	}

	events, ok := receivedBody["events"].([]any)
	if !ok || len(events) != 2 {
		t.Fatalf("expected 2 events, got %v", receivedBody["events"])
	}
	if events[0] != "order.created" || events[1] != "order.updated" {
		t.Errorf("expected events [order.created, order.updated], got %v", events)
	}

	version, ok := receivedBody["version"].(float64)
	if !ok || int(version) != 1 {
		t.Errorf("expected version=1, got %v", receivedBody["version"])
	}
}

func TestProjectionRunner_Register_SetsCustomHeaders(t *testing.T) {
	var mu sync.Mutex
	var receivedHeaders http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedHeaders = r.Header.Clone()
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	proj := CreateProjection(ProjectionConfig{
		Name:   "test",
		Events: []string{"test.event"},
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			return nil, nil
		},
	})

	runner := NewProjectionRunner(proj, server.URL, map[string]string{
		"Authorization": "Bearer my-token",
		"X-Custom":      "value",
	}, NewNoopLogger())
	defer runner.Stop()

	_ = runner.register()

	mu.Lock()
	defer mu.Unlock()

	if receivedHeaders.Get("Authorization") != "Bearer my-token" {
		t.Errorf("expected Authorization header, got %q", receivedHeaders.Get("Authorization"))
	}
	if receivedHeaders.Get("X-Custom") != "value" {
		t.Errorf("expected X-Custom header, got %q", receivedHeaders.Get("X-Custom"))
	}
}

func TestProjectionRunner_Register_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`internal error`))
	}))
	defer server.Close()

	proj := CreateProjection(ProjectionConfig{
		Name:   "test",
		Events: []string{"test.event"},
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			return nil, nil
		},
	})

	runner := NewProjectionRunner(proj, server.URL, nil, NewNoopLogger())
	defer runner.Stop()

	err := runner.register()
	if err == nil {
		t.Fatal("expected error on 500 response")
	}
	if !strings.Contains(err.Error(), "HTTP 500") {
		t.Errorf("expected 'HTTP 500' in error, got %q", err.Error())
	}
}

// ============================================================================
// Managed mode: polls, runs handler, saves state
// ============================================================================

func TestProjectionRunner_ManagedMode_PollAndSaveState(t *testing.T) {
	var mu sync.Mutex
	var registerCalled bool
	var saveStateCalled bool
	var saveStateBody map[string]any
	pollCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/RegisterProjection"):
			registerCalled = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"registered"}`))

		case strings.HasSuffix(path, "/PollProjectionEvents"):
			pollCount++
			w.Header().Set("Content-Type", "application/json")
			if pollCount == 1 {
				// Return events on first poll
				_ = json.NewEncoder(w).Encode(map[string]any{
					"events": []map[string]any{
						{
							"id":        "evt-1",
							"name":      "order.created",
							"data":      map[string]any{"amount": 100.0},
							"seq":       1,
							"timestamp": "2024-01-01T00:00:00Z",
							"source":    "api",
						},
						{
							"id":        "evt-2",
							"name":      "order.created",
							"data":      map[string]any{"amount": 50.0},
							"seq":       2,
							"timestamp": "2024-01-01T01:00:00Z",
							"source":    "api",
						},
					},
					"currentState": map[string]any{"total": 0.0, "count": 0.0},
				})
			} else {
				// No more events
				_ = json.NewEncoder(w).Encode(map[string]any{
					"events": []any{},
				})
			}

		case strings.HasSuffix(path, "/SaveProjectionState"):
			saveStateCalled = true
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &saveStateBody)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	var handlerCalls int
	proj := CreateProjection(ProjectionConfig{
		Name:   "order-totals",
		Events: []string{"order.created"},
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			handlerCalls++
			total, _ := state["total"].(float64)
			count, _ := state["count"].(float64)
			amount, _ := event.Data["amount"].(float64)
			return map[string]any{
				"total": total + amount,
				"count": count + 1,
			}, nil
		},
		InitialState: func() map[string]any {
			return map[string]any{"total": 0.0, "count": 0.0}
		},
	})

	runner := NewProjectionRunner(proj, server.URL, nil, NewNoopLogger())

	// Start and let it process one batch
	if err := runner.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for processing
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := saveStateCalled
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	runner.Stop()
	// Give a moment for goroutine to exit
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if !registerCalled {
		t.Error("expected RegisterProjection to be called")
	}
	if !saveStateCalled {
		t.Fatal("expected SaveProjectionState to be called")
	}
	if handlerCalls != 2 {
		t.Errorf("expected handler called 2 times, got %d", handlerCalls)
	}

	// Check saved state
	if saveStateBody["name"] != "order-totals" {
		t.Errorf("expected name=order-totals, got %v", saveStateBody["name"])
	}
	if saveStateBody["lastEventId"] != "evt-2" {
		t.Errorf("expected lastEventId=evt-2, got %v", saveStateBody["lastEventId"])
	}

	lastSeq, ok := saveStateBody["lastEventSeq"].(float64)
	if !ok || int(lastSeq) != 2 {
		t.Errorf("expected lastEventSeq=2, got %v", saveStateBody["lastEventSeq"])
	}

	state, ok := saveStateBody["state"].(map[string]any)
	if !ok {
		t.Fatalf("expected state to be a map, got %T", saveStateBody["state"])
	}
	if state["total"] != 150.0 {
		t.Errorf("expected state.total=150, got %v", state["total"])
	}
	if state["count"] != 2.0 {
		t.Errorf("expected state.count=2, got %v", state["count"])
	}
}

func TestProjectionRunner_ManagedMode_UsesInitialStateWhenServerReturnsNil(t *testing.T) {
	var mu sync.Mutex
	var saveStateBody map[string]any
	pollCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/RegisterProjection"):
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/PollProjectionEvents"):
			pollCount++
			w.Header().Set("Content-Type", "application/json")
			if pollCount == 1 {
				// Return events without currentState (nil)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"events": []map[string]any{
						{
							"id":   "evt-1",
							"name": "test.event",
							"data": map[string]any{"value": 10.0},
							"seq":  1,
						},
					},
				})
			} else {
				_ = json.NewEncoder(w).Encode(map[string]any{"events": []any{}})
			}

		case strings.HasSuffix(path, "/SaveProjectionState"):
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &saveStateBody)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	proj := CreateProjection(ProjectionConfig{
		Name:   "test-initial",
		Events: []string{"test.event"},
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			sum, _ := state["sum"].(float64)
			val, _ := event.Data["value"].(float64)
			return map[string]any{"sum": sum + val}, nil
		},
		InitialState: func() map[string]any {
			return map[string]any{"sum": 0.0}
		},
	})

	runner := NewProjectionRunner(proj, server.URL, nil, NewNoopLogger())
	if err := runner.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := saveStateBody != nil
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	runner.Stop()
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if saveStateBody == nil {
		t.Fatal("expected SaveProjectionState to be called")
	}

	state, ok := saveStateBody["state"].(map[string]any)
	if !ok {
		t.Fatalf("expected state to be a map, got %T", saveStateBody["state"])
	}
	// InitialState sum=0 + event value=10 = 10
	if state["sum"] != 10.0 {
		t.Errorf("expected state.sum=10, got %v", state["sum"])
	}
}

// ============================================================================
// External mode: polls, runs handler, acks
// ============================================================================

func TestProjectionRunner_ExternalMode_PollAndAck(t *testing.T) {
	var mu sync.Mutex
	var ackCalled bool
	var ackBody map[string]any
	pollCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/RegisterProjection"):
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/PollProjectionEvents"):
			pollCount++
			w.Header().Set("Content-Type", "application/json")
			if pollCount == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"events": []map[string]any{
						{
							"id":        "evt-a",
							"name":      "order.completed",
							"data":      map[string]any{"email": "user@test.com"},
							"seq":       10,
							"timestamp": "2024-06-01T12:00:00Z",
						},
						{
							"id":        "evt-b",
							"name":      "order.completed",
							"data":      map[string]any{"email": "admin@test.com"},
							"seq":       11,
							"timestamp": "2024-06-01T12:05:00Z",
						},
					},
				})
			} else {
				_ = json.NewEncoder(w).Encode(map[string]any{"events": []any{}})
			}

		case strings.HasSuffix(path, "/AckProjectionEvents"):
			ackCalled = true
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &ackBody)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	var handlerCalls atomic.Int32
	var processedEvents []string
	var eventsMu sync.Mutex

	proj := CreateProjection(ProjectionConfig{
		Name:   "email-notifier",
		Events: []string{"order.completed"},
		Mode:   ProjectionModeExternal,
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			handlerCalls.Add(1)
			eventsMu.Lock()
			processedEvents = append(processedEvents, event.Data["email"].(string))
			eventsMu.Unlock()
			return nil, nil
		},
	})

	runner := NewProjectionRunner(proj, server.URL, nil, NewNoopLogger())
	if err := runner.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := ackCalled
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	runner.Stop()
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if !ackCalled {
		t.Fatal("expected AckProjectionEvents to be called")
	}
	if int(handlerCalls.Load()) != 2 {
		t.Errorf("expected handler called 2 times, got %d", handlerCalls.Load())
	}

	eventsMu.Lock()
	defer eventsMu.Unlock()
	if len(processedEvents) != 2 {
		t.Fatalf("expected 2 processed events, got %d", len(processedEvents))
	}
	if processedEvents[0] != "user@test.com" || processedEvents[1] != "admin@test.com" {
		t.Errorf("unexpected processed events: %v", processedEvents)
	}

	if ackBody["name"] != "email-notifier" {
		t.Errorf("expected ack name=email-notifier, got %v", ackBody["name"])
	}
	if ackBody["lastEventId"] != "evt-b" {
		t.Errorf("expected lastEventId=evt-b, got %v", ackBody["lastEventId"])
	}
	lastSeq, ok := ackBody["lastEventSeq"].(float64)
	if !ok || int(lastSeq) != 11 {
		t.Errorf("expected lastEventSeq=11, got %v", ackBody["lastEventSeq"])
	}
}

// ============================================================================
// Empty poll — no save/ack
// ============================================================================

func TestProjectionRunner_EmptyPoll_NoSaveOrAck(t *testing.T) {
	var mu sync.Mutex
	var saveStateCalled bool
	var ackCalled bool
	pollCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/RegisterProjection"):
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/PollProjectionEvents"):
			pollCount++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"events": []any{},
			})

		case strings.HasSuffix(path, "/SaveProjectionState"):
			saveStateCalled = true
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/AckProjectionEvents"):
			ackCalled = true
			w.WriteHeader(http.StatusOK)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	proj := CreateProjection(ProjectionConfig{
		Name:   "test-empty",
		Events: []string{"test.event"},
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			return state, nil
		},
		InitialState: func() map[string]any { return map[string]any{} },
	})

	runner := NewProjectionRunner(proj, server.URL, nil, NewNoopLogger())
	if err := runner.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Let the runner poll a few times
	time.Sleep(300 * time.Millisecond)
	runner.Stop()
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if pollCount < 1 {
		t.Error("expected at least 1 poll")
	}
	if saveStateCalled {
		t.Error("expected SaveProjectionState to NOT be called on empty poll")
	}
	if ackCalled {
		t.Error("expected AckProjectionEvents to NOT be called on empty poll")
	}
}

// ============================================================================
// Stop() terminates the poll loop
// ============================================================================

func TestProjectionRunner_Stop_TerminatesPollLoop(t *testing.T) {
	var mu sync.Mutex
	pollCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/RegisterProjection"):
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/PollProjectionEvents"):
			pollCount++
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"events": []any{}})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	proj := CreateProjection(ProjectionConfig{
		Name:   "stop-test",
		Events: []string{"test.event"},
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			return nil, nil
		},
	})

	runner := NewProjectionRunner(proj, server.URL, nil, NewNoopLogger())
	if err := runner.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Let it poll once
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	initialPollCount := pollCount
	mu.Unlock()

	runner.Stop()

	// Wait and verify poll count doesn't increase
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	finalPollCount := pollCount
	mu.Unlock()

	if initialPollCount < 1 {
		t.Error("expected at least 1 poll before stop")
	}
	// After stop, poll count should not have grown significantly
	// (may have had one more in-flight poll)
	if finalPollCount > initialPollCount+1 {
		t.Errorf("expected poll loop to stop, but polls went from %d to %d",
			initialPollCount, finalPollCount)
	}
}

// ============================================================================
// Start() fails if registration fails
// ============================================================================

func TestProjectionRunner_Start_FailsOnRegistrationError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`server error`))
	}))
	defer server.Close()

	proj := CreateProjection(ProjectionConfig{
		Name:   "fail-register",
		Events: []string{"test.event"},
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			return nil, nil
		},
	})

	runner := NewProjectionRunner(proj, server.URL, nil, NewNoopLogger())
	defer runner.Stop()

	err := runner.Start()
	if err == nil {
		t.Fatal("expected error when registration fails")
	}
	if !strings.Contains(err.Error(), "register projection") {
		t.Errorf("expected 'register projection' in error, got %q", err.Error())
	}
}

// ============================================================================
// Handler context is populated correctly
// ============================================================================

func TestProjectionRunner_HandlerContextPopulated(t *testing.T) {
	var mu sync.Mutex
	var capturedCtx ProjectionContext
	var capturedEvent ProjectionEvent
	var saveStateCalled bool
	pollCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/RegisterProjection"):
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/PollProjectionEvents"):
			pollCount++
			w.Header().Set("Content-Type", "application/json")
			if pollCount == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"events": []map[string]any{
						{
							"id":        "evt-ctx-1",
							"name":      "user.created",
							"data":      map[string]any{"userId": "u123"},
							"seq":       42,
							"timestamp": "2024-03-15T10:30:00Z",
							"source":    "webhook",
							"metadata":  map[string]any{"traceId": "abc"},
						},
					},
					"currentState": map[string]any{},
				})
			} else {
				_ = json.NewEncoder(w).Encode(map[string]any{"events": []any{}})
			}

		case strings.HasSuffix(path, "/SaveProjectionState"):
			saveStateCalled = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	proj := CreateProjection(ProjectionConfig{
		Name:   "context-test",
		Events: []string{"user.created"},
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			capturedCtx = ctx
			capturedEvent = event
			return state, nil
		},
		InitialState: func() map[string]any { return map[string]any{} },
	})

	runner := NewProjectionRunner(proj, server.URL, nil, NewNoopLogger())
	if err := runner.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := saveStateCalled
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	runner.Stop()
	time.Sleep(50 * time.Millisecond)

	// Verify ProjectionContext
	if capturedCtx.Event.ID != "evt-ctx-1" {
		t.Errorf("expected ctx.Event.ID=evt-ctx-1, got %q", capturedCtx.Event.ID)
	}
	if capturedCtx.Event.Name != "user.created" {
		t.Errorf("expected ctx.Event.Name=user.created, got %q", capturedCtx.Event.Name)
	}
	if capturedCtx.Event.Seq != 42 {
		t.Errorf("expected ctx.Event.Seq=42, got %d", capturedCtx.Event.Seq)
	}
	if capturedCtx.Event.Timestamp != "2024-03-15T10:30:00Z" {
		t.Errorf("expected ctx.Event.Timestamp=2024-03-15T10:30:00Z, got %q", capturedCtx.Event.Timestamp)
	}
	if capturedCtx.Projection.Name != "context-test" {
		t.Errorf("expected ctx.Projection.Name=context-test, got %q", capturedCtx.Projection.Name)
	}
	if capturedCtx.Projection.Version != 1 {
		t.Errorf("expected ctx.Projection.Version=1, got %d", capturedCtx.Projection.Version)
	}

	// Verify ProjectionEvent
	if capturedEvent.ID != "evt-ctx-1" {
		t.Errorf("expected event.ID=evt-ctx-1, got %q", capturedEvent.ID)
	}
	if capturedEvent.Source != "webhook" {
		t.Errorf("expected event.Source=webhook, got %q", capturedEvent.Source)
	}
	if capturedEvent.Data["userId"] != "u123" {
		t.Errorf("expected event.Data.userId=u123, got %v", capturedEvent.Data["userId"])
	}
	if capturedEvent.Metadata["traceId"] != "abc" {
		t.Errorf("expected event.Metadata.traceId=abc, got %v", capturedEvent.Metadata["traceId"])
	}
}

// ============================================================================
// Poll sends correct batch size
// ============================================================================

func TestProjectionRunner_PollSendsBatchSize(t *testing.T) {
	var mu sync.Mutex
	var pollBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/RegisterProjection"):
			w.WriteHeader(http.StatusOK)

		case strings.HasSuffix(path, "/PollProjectionEvents"):
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &pollBody)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"events": []any{}})

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	proj := CreateProjection(ProjectionConfig{
		Name:      "batch-test",
		Events:    []string{"test.event"},
		BatchSize: 50,
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			return nil, nil
		},
	})

	runner := NewProjectionRunner(proj, server.URL, nil, NewNoopLogger())
	if err := runner.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for first poll
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := pollBody != nil
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	runner.Stop()
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if pollBody == nil {
		t.Fatal("expected poll to be called")
	}
	if pollBody["name"] != "batch-test" {
		t.Errorf("expected name=batch-test, got %v", pollBody["name"])
	}

	batchSize, ok := pollBody["batchSize"].(float64)
	if !ok || int(batchSize) != 50 {
		t.Errorf("expected batchSize=50, got %v", pollBody["batchSize"])
	}
}

// ============================================================================
// Register omits partitionKey when empty
// ============================================================================

func TestProjectionRunner_Register_OmitsPartitionKeyWhenEmpty(t *testing.T) {
	var mu sync.Mutex
	var receivedBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	proj := CreateProjection(ProjectionConfig{
		Name:   "no-partition",
		Events: []string{"test.event"},
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			return nil, nil
		},
	})

	runner := NewProjectionRunner(proj, server.URL, nil, NewNoopLogger())
	defer runner.Stop()

	_ = runner.register()

	mu.Lock()
	defer mu.Unlock()

	if _, exists := receivedBody["partitionKey"]; exists {
		t.Error("expected partitionKey to be omitted when empty")
	}
}

// ============================================================================
// NewProjectionRunner defaults
// ============================================================================

func TestNewProjectionRunner_Defaults(t *testing.T) {
	proj := CreateProjection(ProjectionConfig{
		Name:   "test",
		Events: []string{"test.event"},
		Handler: func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error) {
			return nil, nil
		},
	})

	// Nil logger should default to noop
	runner := NewProjectionRunner(proj, "http://localhost:9123", nil, nil)
	defer runner.Stop()

	if runner.logger == nil {
		t.Error("expected logger to be set")
	}
	if runner.httpClient == nil {
		t.Error("expected httpClient to be initialized")
	}
	if runner.baseURL != "http://localhost:9123" {
		t.Errorf("expected baseURL=http://localhost:9123, got %q", runner.baseURL)
	}
}
