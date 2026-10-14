package ironflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

func TestProjectionRunner_ManagedPartitionsResumeAcrossBatches(t *testing.T) {
	states := map[string]map[string]any{
		"a": {"count": 40.0}, "b": {"count": 90.0}, "__global__": {"count": 10.0},
	}
	loads := map[string]int{}
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if body["name"] != "counter" || req.Header.Get("X-Test") != "partition" {
			t.Errorf("missing projection name or headers: %v", body)
		}
		switch strings.TrimPrefix(req.URL.Path, projectionServicePath) {
		case "PollProjectionEvents":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"currentState": states["__global__"],
				"events": []map[string]any{
					{"id": "a1", "seq": 1, "metadata": map[string]any{"__partition": "a"}},
					{"id": "a2", "seq": 2, "metadata": map[string]any{"__partition": "a"}},
					{"id": "b1", "seq": 3, "metadata": map[string]any{"__partition": "b"}},
					{"id": "g1", "seq": 4},
				},
			})
		case "GetProjection":
			pk, _ := body["partition"].(string)
			loads[pk]++
			_ = json.NewEncoder(w).Encode(map[string]any{"state": states[pk]})
		case "SaveProjectionState":
			states[body["partitionKey"].(string)] = body["state"].(map[string]any)
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()
	proj := CreateProjection(ProjectionConfig{
		Name: "counter", Events: []string{"inc"},
		InitialState: func() map[string]any { return map[string]any{"count": 0.0} },
		Handler: func(state map[string]any, _ ProjectionEvent, _ ProjectionContext) (map[string]any, error) {
			return map[string]any{"count": state["count"].(float64) + 1}, nil
		},
	})
	for range 2 {
		runner := NewProjectionRunner(proj, server.URL, map[string]string{"X-Test": "partition"}, nil)
		if n, err := runner.poll(); err != nil || n != 4 {
			t.Fatalf("poll = (%d, %v), want (4, nil)", n, err)
		}
	}
	want := map[string]map[string]any{
		"a": {"count": 44.0}, "b": {"count": 92.0}, "__global__": {"count": 12.0},
	}
	if !reflect.DeepEqual(states, want) {
		t.Fatalf("saved states = %v, want %v", states, want)
	}
	if !reflect.DeepEqual(loads, map[string]int{"a": 2, "b": 2}) {
		t.Errorf("partition loads = %v, want two per non-global partition", loads)
	}
}

func TestProjectionRunner_ManagedPartitionMissingAndEmptyState(t *testing.T) {
	for _, tc := range []struct {
		name, response string
		global         bool
		want           float64
	}{
		{"missing", `{}`, false, 11},
		{"empty", `{"state":{}}`, false, 11},
		{"global missing", `{}`, true, 11},
		{"global empty", `{"state":{}}`, true, 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var saved map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if strings.HasSuffix(req.URL.Path, "/GetProjection") {
					_, _ = w.Write([]byte(tc.response))
					return
				}
				_ = json.NewDecoder(req.Body).Decode(&saved)
			}))
			defer server.Close()
			proj := CreateProjection(ProjectionConfig{
				Name: "counter", Events: []string{"inc"},
				InitialState: func() map[string]any { return map[string]any{"count": 10.0} },
				Handler: func(state map[string]any, _ ProjectionEvent, _ ProjectionContext) (map[string]any, error) {
					count, _ := state["count"].(float64)
					return map[string]any{"count": count + 1}, nil
				},
			})
			runner := NewProjectionRunner(proj, server.URL, nil, nil)
			resp := pollResponse{Events: []pollEvent{{ID: "e1", Metadata: map[string]any{"__partition": "a"}}}}
			if tc.global {
				// The global state arrives as currentState in the poll response.
				var r struct {
					State map[string]any `json:"state"`
				}
				_ = json.Unmarshal([]byte(tc.response), &r)
				resp = pollResponse{Events: []pollEvent{{ID: "e1"}}, CurrentState: r.State}
			}
			_, err := runner.processManaged(proj.Config, resp)
			if err != nil {
				t.Fatal(err)
			}
			if got := saved["state"].(map[string]any)["count"]; got != tc.want {
				t.Errorf("saved count = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestProjectionRunner_ManagedSavesRunsInSeqOrder(t *testing.T) {
	for _, tc := range []struct {
		name   string
		failAt int // 1-based save that fails; 0 = none
		want   []string
	}{
		{"all saved", 0, []string{"a@1=1", "b@2=1", "a@3=2"}},
		// The cursor stops at seq 1, so b@2 and a@3 are redelivered.
		{"second save fails", 2, []string{"a@1=1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var saves []string
			attempts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if strings.HasSuffix(req.URL.Path, "/GetProjection") {
					_, _ = w.Write([]byte(`{}`))
					return
				}
				attempts++
				if attempts == tc.failAt {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				var body map[string]any
				_ = json.NewDecoder(req.Body).Decode(&body)
				saves = append(saves, fmt.Sprintf("%s@%v=%v", body["partitionKey"], body["lastEventSeq"], body["state"].(map[string]any)["count"]))
			}))
			defer server.Close()
			proj := CreateProjection(ProjectionConfig{
				Name: "counter", Events: []string{"inc"},
				InitialState: func() map[string]any { return map[string]any{"count": 0.0} },
				Handler: func(state map[string]any, _ ProjectionEvent, _ ProjectionContext) (map[string]any, error) {
					return map[string]any{"count": state["count"].(float64) + 1}, nil
				},
			})
			runner := NewProjectionRunner(proj, server.URL, nil, nil)
			// Delivered out of order on purpose: the runner sorts by seq.
			_, err := runner.processManaged(proj.Config, pollResponse{Events: []pollEvent{
				{ID: "a3", Seq: 3, Metadata: map[string]any{"__partition": "a"}},
				{ID: "a1", Seq: 1, Metadata: map[string]any{"__partition": "a"}},
				{ID: "b2", Seq: 2, Metadata: map[string]any{"__partition": "b"}},
			}})
			if (err != nil) != (tc.failAt != 0) {
				t.Fatalf("err = %v, failAt = %d", err, tc.failAt)
			}
			if !reflect.DeepEqual(saves, tc.want) {
				t.Errorf("saves = %v, want %v", saves, tc.want)
			}
		})
	}
}

func TestProjectionRunner_ManagedPartitionNonObjectStateIsNotOverwritten(t *testing.T) {
	saved := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasSuffix(req.URL.Path, "/GetProjection") {
			_, _ = w.Write([]byte(`{"stateValue":[1,2]}`))
			return
		}
		saved++
	}))
	defer server.Close()
	proj := CreateProjection(ProjectionConfig{
		Name: "counter", Events: []string{"inc"},
		InitialState: func() map[string]any { return map[string]any{} },
		Handler: func(state map[string]any, _ ProjectionEvent, _ ProjectionContext) (map[string]any, error) {
			return state, nil
		},
	})
	runner := NewProjectionRunner(proj, server.URL, nil, nil)
	_, err := runner.processManaged(proj.Config, pollResponse{Events: []pollEvent{{ID: "e1", Metadata: map[string]any{"__partition": "a"}}}})
	if err == nil || !strings.Contains(err.Error(), "not a JSON object") {
		t.Fatalf("err = %v, want non-object state error", err)
	}
	if saved != 0 {
		t.Errorf("saved %d times over a non-object state", saved)
	}
}

func TestProjectionRunner_ManagedGlobalNonObjectStateIsNotOverwritten(t *testing.T) {
	for _, tc := range []struct {
		name    string
		run     func(*ProjectionRunner) error
		wantErr bool
	}{
		{"poll", func(r *ProjectionRunner) error { _, err := r.poll(); return err }, true},
		{"stream", func(r *ProjectionRunner) error { return r.processBatch([]pollEvent{{ID: "e1", Seq: 1}}) }, true},
		// The global state is not used, so a partition batch goes on.
		{"stream, partition only", func(r *ProjectionRunner) error {
			return r.processBatch([]pollEvent{{ID: "e1", Seq: 1, Metadata: map[string]any{"__partition": "a"}}})
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var saved atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				var body map[string]any
				_ = json.NewDecoder(req.Body).Decode(&body)
				switch strings.TrimPrefix(req.URL.Path, projectionServicePath) {
				case "PollProjectionEvents":
					_, _ = w.Write([]byte(`{"events":[{"id":"e1","seq":"1"}],"currentStateValue":5}`))
				case "GetProjection":
					if body["partition"] == "a" {
						_, _ = w.Write([]byte(`{}`))
					} else {
						_, _ = w.Write([]byte(`{"stateValue":5}`))
					}
				case "SaveProjectionState":
					saved.Add(1)
				}
			}))
			defer server.Close()
			runner := NewProjectionRunner(counterProjection(0), server.URL, nil, nil)
			err := tc.run(runner)
			if !tc.wantErr {
				if err != nil || saved.Load() != 1 {
					t.Fatalf("err = %v, saved = %d; want nil and 1", err, saved.Load())
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "not a JSON object") {
				t.Fatalf("err = %v, want non-object state error", err)
			}
			if saved.Load() != 0 {
				t.Errorf("saved %d times over a non-object global state", saved.Load())
			}
		})
	}
}

func TestProjectionRunner_EventDataValue(t *testing.T) {
	scalar, _ := structpb.NewValue("hello")
	object, _ := structpb.NewValue(map[string]any{"a": 1.0})
	var polled pollEvent
	if err := json.Unmarshal([]byte(`{"id":"e3","dataValue":[1,2]}`), &polled); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		evt       ProjectionEvent
		wantData  map[string]any
		wantValue any
	}{
		{"stream scalar", toProjectionEvent(toPollEvent(&ironflowv1.ProjectionEvent{Id: "e1", DataValue: scalar})), nil, "hello"},
		{"stream object", toProjectionEvent(toPollEvent(&ironflowv1.ProjectionEvent{Id: "e2", DataValue: object})), map[string]any{"a": 1.0}, nil},
		{"poll array", toProjectionEvent(polled), nil, []any{1.0, 2.0}},
	} {
		if !reflect.DeepEqual(tc.evt.Data, tc.wantData) || !reflect.DeepEqual(tc.evt.DataValue, tc.wantValue) {
			t.Errorf("%s: Data = %v, DataValue = %v; want %v and %v", tc.name, tc.evt.Data, tc.evt.DataValue, tc.wantData, tc.wantValue)
		}
	}
}

func TestProjectionRunner_ManagedPartitionLoadFailureDoesNotSave(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusServiceUnavailable, http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var handled, saved atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if strings.HasSuffix(req.URL.Path, "/GetProjection") {
					var body map[string]any
					_ = json.NewDecoder(req.Body).Decode(&body)
					if body["partition"] == "b" {
						w.WriteHeader(status)
					} else {
						_, _ = w.Write([]byte(`{}`))
					}
					return
				}
				saved.Add(1)
			}))
			defer server.Close()
			proj := CreateProjection(ProjectionConfig{
				Name: "counter", Events: []string{"inc"},
				Handler: func(state map[string]any, _ ProjectionEvent, _ ProjectionContext) (map[string]any, error) {
					handled.Add(1)
					return state, nil
				},
			})
			runner := NewProjectionRunner(proj, server.URL, nil, nil)
			// Partition a loads fine; b fails. No partition may be saved, or
			// the server cursor would skip b's unsaved events.
			_, err := runner.processManaged(proj.Config, pollResponse{Events: []pollEvent{
				{ID: "e1", Seq: 1, Metadata: map[string]any{"__partition": "a"}},
				{ID: "e2", Seq: 2, Metadata: map[string]any{"__partition": "b"}},
			}})
			if err == nil {
				t.Fatal("partition load failure must fail the batch")
			}
			if handled.Load() != 0 || saved.Load() != 0 {
				t.Errorf("handled=%d saved=%d after failed load", handled.Load(), saved.Load())
			}
		})
	}
}

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

// Other register errors are retried (TestProjectionRunner_Start_RetriesRegistration).
func TestProjectionRunner_Start_FailsOnAuthRegistrationError(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var registers atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				registers.Add(1)
				w.WriteHeader(status)
			}))
			defer server.Close()

			runner := NewProjectionRunner(counterProjection(0), server.URL, nil, nil)
			defer runner.Stop()
			err := runner.Start()
			if err == nil || !strings.Contains(err.Error(), "register projection") {
				t.Fatalf("Start = %v, want a register projection error", err)
			}
			if !errors.Is(err, ErrUnauthorized) && !errors.Is(err, ErrForbidden) {
				t.Errorf("Start = %v, want ErrUnauthorized or ErrForbidden", err)
			}
			if registers.Load() != 1 {
				t.Errorf("registers = %d, want 1: an auth error is not retried", registers.Load())
			}
		})
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
