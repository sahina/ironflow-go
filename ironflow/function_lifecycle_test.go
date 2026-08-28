package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFunctionLifecycleMethods(t *testing.T) {
	var calls []struct {
		path string
		body map[string]any
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request for %s: %v", r.URL.Path, err)
		}
		calls = append(calls, struct {
			path string
			body map[string]any
		}{r.URL.Path, body})

		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/ironflow.v1.IronflowService/GetFunction":
			_, _ = w.Write([]byte(`{"id":"fn-1","status":"FUNCTION_STATUS_ACTIVE","preferredMode":"EXECUTION_MODE_PULL"}`))
		case "/ironflow.v1.IronflowService/UpdateFunctionStatus":
			_, _ = w.Write([]byte(`{"id":"fn-1","status":"FUNCTION_STATUS_PAUSED"}`))
		case "/ironflow.v1.IronflowService/DeleteFunction":
			_, _ = w.Write([]byte(`{}`))
		case "/ironflow.v1.IronflowService/ListFunctionHistory":
			_, _ = w.Write([]byte(`{"entries":[{"eventId":"evt-1","entityVersion":"12","functionId":"fn-1","changeType":"update","functionSnapshot":{"id":"fn-1","status":"FUNCTION_STATUS_ACTIVE"}}],"hasMore":true}`))
		case "/ironflow.v1.IronflowService/GetFunctionAtVersion":
			_, _ = w.Write([]byte(`{"entry":{"eventId":"evt-2","entityVersion":"9","functionId":"fn-1","changeType":"update"}}`))
		case "/ironflow.v1.IronflowService/RollbackFunction":
			_, _ = w.Write([]byte(`{"function":{"id":"fn-1","status":"FUNCTION_STATUS_ACTIVE"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := &Client{
		serverURL:   server.URL,
		httpClient:  server.Client(),
		retryConfig: &ClientRetryConfig{MaxAttempts: 1},
		logger:      NewNoopLogger(),
	}
	ctx := context.Background()

	fn, err := client.GetFunction(ctx, "fn-1")
	if err != nil {
		t.Fatal(err)
	}
	if fn.Status != FunctionStatusActive || fn.PreferredMode != PullMode {
		t.Fatalf("GetFunction normalization = status %q, mode %q", fn.Status, fn.PreferredMode)
	}

	fn, err = client.UpdateFunctionStatus(ctx, "fn-1", FunctionStatusPaused)
	if err != nil {
		t.Fatal(err)
	}
	if fn.Status != FunctionStatusPaused {
		t.Fatalf("UpdateFunctionStatus status = %q", fn.Status)
	}
	if err := client.DeleteFunction(ctx, "fn-1"); err != nil {
		t.Fatal(err)
	}

	history, err := client.ListFunctionHistory(ctx, "fn-1", ListFunctionHistoryOptions{Limit: 20, FromVersion: 13})
	if err != nil {
		t.Fatal(err)
	}
	if !history.HasMore || len(history.Entries) != 1 || history.Entries[0].EntityVersion.Int64() != 12 {
		t.Fatalf("ListFunctionHistory result = %#v", history)
	}
	if history.Entries[0].FunctionSnapshot == nil || history.Entries[0].FunctionSnapshot.Status != FunctionStatusActive {
		t.Fatalf("history snapshot was not normalized: %#v", history.Entries[0].FunctionSnapshot)
	}

	entry, err := client.GetFunctionAtVersion(ctx, "fn-1", 9)
	if err != nil {
		t.Fatal(err)
	}
	if entry.EntityVersion.Int64() != 9 {
		t.Fatalf("GetFunctionAtVersion entity version = %d", entry.EntityVersion.Int64())
	}

	fn, err = client.RollbackFunction(ctx, "fn-1", 9, "bad deploy")
	if err != nil {
		t.Fatal(err)
	}
	if fn.Status != FunctionStatusActive {
		t.Fatalf("RollbackFunction status = %q", fn.Status)
	}

	wantPaths := []string{
		"/ironflow.v1.IronflowService/GetFunction",
		"/ironflow.v1.IronflowService/UpdateFunctionStatus",
		"/ironflow.v1.IronflowService/DeleteFunction",
		"/ironflow.v1.IronflowService/ListFunctionHistory",
		"/ironflow.v1.IronflowService/GetFunctionAtVersion",
		"/ironflow.v1.IronflowService/RollbackFunction",
	}
	if len(calls) != len(wantPaths) {
		t.Fatalf("calls = %d, want %d", len(calls), len(wantPaths))
	}
	for i, want := range wantPaths {
		if calls[i].path != want {
			t.Errorf("call %d path = %q, want %q", i, calls[i].path, want)
		}
	}
	if calls[1].body["status"] != "FUNCTION_STATUS_PAUSED" {
		t.Errorf("status request = %#v", calls[1].body)
	}
	if calls[3].body["limit"] != float64(20) || calls[3].body["fromVersion"] != float64(13) {
		t.Errorf("history request = %#v", calls[3].body)
	}
	if calls[5].body["changeReason"] != "bad deploy" {
		t.Errorf("rollback request = %#v", calls[5].body)
	}
}
