package ironflow

import (
	"context"
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

// livePushServer serves fn over the push protocol and returns its URL. The
// engine posts to it, so a function registered with this URL runs for real.
func livePushServer(t *testing.T, fn Function) string {
	t.Helper()
	srv := httptest.NewServer(Serve(ServeConfig{Functions: []Function{fn}}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestLiveReadinessFunctions covers the functions group (#3003) on one push
// function this test registers, serves and deletes. The server has no signing
// key, so the handler accepts unsigned requests.
func TestLiveReadinessFunctions(t *testing.T) {
	client, ctx := liveReadiness(t, &struct{}{})
	id := fmt.Sprintf("readiness-fn-go-%d", time.Now().UnixNano())
	fn := CreateFunction(FunctionConfig{
		ID: id, Name: "Readiness functions (Go)", Description: "first",
		Triggers:    []Trigger{{Event: "readiness.functions." + id}},
		Retry:       &RetryConfig{MaxAttempts: 4, InitialDelay: 2 * time.Second, BackoffFactor: 3, MaxDelay: time.Minute},
		Timeout:     45 * time.Second,
		Concurrency: &ConcurrencyConfig{Limit: 2, Key: "who"},
		Mode:        PushMode,
		Metadata:    map[string]any{"team": "readiness"},
	}, func(c Context) (any, error) {
		var in map[string]any
		if err := c.Event.Data(&in); err != nil {
			return nil, err
		}
		return map[string]any{"echoed": in}, nil
	})
	fn.Config.EndpointURL = livePushServer(t, fn)

	t.Run("function-register and function-get", func(t *testing.T) {
		if err := client.RegisterFunction(ctx, fn); err != nil {
			t.Fatalf("RegisterFunction: %v", err)
		}
		got, err := client.GetFunction(ctx, id)
		if err != nil {
			t.Fatalf("GetFunction: %v", err)
		}
		if got.ID != id || got.Name != "Readiness functions (Go)" || got.Description != "first" ||
			got.Status != FunctionStatusActive || got.Version != 1 || got.TimeoutMs != 45000 ||
			got.PreferredMode != PushMode || got.EndpointURL != fn.Config.EndpointURL ||
			got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
			t.Fatalf("GetFunction = %+v, want the registered fields at version 1, active, push", got)
		}
		if len(got.Triggers) != 1 || got.Triggers[0].Event != "readiness.functions."+id {
			t.Fatalf("triggers = %+v", got.Triggers)
		}
		if want := (&RegisteredFunctionRetryConfig{MaxAttempts: 4, InitialDelayMs: 2000, BackoffFactor: 3, MaxDelayMs: 60000}); !reflect.DeepEqual(got.Retry, want) {
			t.Fatalf("retry = %+v, want %+v", got.Retry, want)
		}
		if want := (&RegisteredFunctionConcurrencyConfig{Limit: 2, Key: "who"}); !reflect.DeepEqual(got.Concurrency, want) {
			t.Fatalf("concurrency = %+v, want %+v", got.Concurrency, want)
		}
		if !reflect.DeepEqual(got.Metadata, map[string]any{"team": "readiness"}) {
			t.Fatalf("metadata = %v", got.Metadata)
		}

		// A second registration updates in place: version 2, new description.
		fn2 := fn
		fn2.Config.Description = "second"
		if err := client.RegisterFunction(ctx, fn2); err != nil {
			t.Fatalf("re-register: %v", err)
		}
		if got, err = client.GetFunction(ctx, id); err != nil || got.Version != 2 || got.Description != "second" {
			t.Fatalf("after re-register = %+v, %v, want version 2 \"second\"", got, err)
		}
	})

	t.Run("function-list", func(t *testing.T) {
		all, err := client.ListFunctions(ctx)
		if err != nil {
			t.Fatalf("ListFunctions: %v", err)
		}
		for _, f := range all {
			if f.ID == id {
				if f.Name != "Readiness functions (Go)" || f.Status != "active" || f.PreferredMode != "push" || f.CreatedAt == "" || f.UpdatedAt == "" {
					t.Fatalf("listed = %+v, want name, active, push and both times", f)
				}
				return
			}
		}
		t.Fatalf("ListFunctions has no %s among %d functions", id, len(all))
	})

	t.Run("function-invoke", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		res, err := client.InvokeSync(ctx, id, map[string]any{"who": "José"}, 30*time.Second)
		if err != nil {
			t.Fatalf("InvokeSync: %v", err)
		}
		want := map[string]any{"echoed": map[string]any{"who": "José"}}
		if res.RunID == "" || res.FunctionID != id || res.Status != RunStatusCompleted || !reflect.DeepEqual(res.Output, want) {
			t.Fatalf("InvokeSync = %+v (error %+v), want a completed run of %s with %v", res, res.Error, id, want)
		}
		run, err := client.GetRun(ctx, res.RunID)
		if err != nil || run.FunctionID != id || !reflect.DeepEqual(run.Input, map[string]any{"who": "José"}) {
			t.Fatalf("GetRun = %+v, %v, want the invoke input on %s", run, err, id)
		}
		if _, err := client.InvokeSync(ctx, id+"-missing", nil, 5*time.Second); !hasErrorCode(err, "not_found") {
			t.Fatalf("InvokeSync of an unknown function = %v, want not_found", err)
		}
	})

	t.Run("function-status", func(t *testing.T) {
		before, err := client.GetFunction(ctx, id)
		if err != nil {
			t.Fatalf("GetFunction: %v", err)
		}
		paused, err := client.UpdateFunctionStatus(ctx, id, FunctionStatusPaused)
		if err != nil || paused.Status != FunctionStatusPaused || paused.Version != before.Version+1 {
			t.Fatalf("pause = %+v, %v, want paused at version %d", paused, err, before.Version+1)
		}
		if got, err := client.GetFunction(ctx, id); err != nil || got.Status != FunctionStatusPaused {
			t.Fatalf("GetFunction after pause = %+v, %v", got, err)
		}
		// A paused function starts no run for its trigger event.
		if res, err := client.Emit(ctx, "readiness.functions."+id, map[string]any{}); err != nil || len(res.RunIDs) != 0 {
			t.Fatalf("Emit while paused = %+v, %v, want no run", res, err)
		}
		active, err := client.UpdateFunctionStatus(ctx, id, FunctionStatusActive)
		if err != nil || active.Status != FunctionStatusActive || active.Version != before.Version+2 {
			t.Fatalf("activate = %+v, %v, want active at version %d", active, err, before.Version+2)
		}
		if res, err := client.Emit(ctx, "readiness.functions."+id, map[string]any{}); err != nil || len(res.RunIDs) != 1 {
			t.Fatalf("Emit after activate = %+v, %v, want one run", res, err)
		}
	})

	t.Run("function-history", func(t *testing.T) {
		hist, err := client.ListFunctionHistory(ctx, id, ListFunctionHistoryOptions{Limit: 100})
		if err != nil {
			t.Fatalf("ListFunctionHistory: %v", err)
		}
		// Newest first: activate, pause, re-register, register.
		wantTypes := []FunctionChangeType{FunctionChangeStatusChange, FunctionChangeStatusChange, FunctionChangeUpdate, FunctionChangeCreated}
		wantStatus := []FunctionStatus{FunctionStatusActive, FunctionStatusPaused, FunctionStatusActive, FunctionStatusActive}
		if len(hist.Entries) != 4 || hist.HasMore {
			t.Fatalf("history = %d entries (more %v), want 4", len(hist.Entries), hist.HasMore)
		}
		for i, e := range hist.Entries {
			if e.ChangeType != wantTypes[i] || e.FunctionID != id || e.FunctionSnapshot == nil || e.FunctionSnapshot.Status != wantStatus[i] || e.RecordedAt.IsZero() || e.EventID == "" {
				t.Fatalf("entry %d = %+v, want %s with status %s", i, e, wantTypes[i], wantStatus[i])
			}
			if i > 0 && e.EntityVersion != hist.Entries[i-1].EntityVersion-1 {
				t.Fatalf("entry %d version %d does not follow %d", i, e.EntityVersion, hist.Entries[i-1].EntityVersion)
			}
		}
		if d := hist.Entries[0].FunctionSnapshot.Description; d != "second" {
			t.Fatalf("newest snapshot description = %q, want second", d)
		}
		if d := hist.Entries[3].FunctionSnapshot.Description; d != "first" {
			t.Fatalf("oldest snapshot description = %q, want first", d)
		}

		page1, err := client.ListFunctionHistory(ctx, id, ListFunctionHistoryOptions{Limit: 2})
		if err != nil || len(page1.Entries) != 2 || !page1.HasMore {
			t.Fatalf("page 1 = %+v, %v, want 2 entries and more", page1, err)
		}
		page2, err := client.ListFunctionHistory(ctx, id, ListFunctionHistoryOptions{Limit: 2, FromVersion: int64(page1.Entries[1].EntityVersion)})
		if err != nil || len(page2.Entries) != 2 || page2.HasMore || page2.Entries[0].EventID != hist.Entries[2].EventID {
			t.Fatalf("page 2 = %+v, %v, want the two older entries and no more", page2, err)
		}
	})

	t.Run("function-rollback", func(t *testing.T) {
		if _, err := client.UpdateFunctionStatus(ctx, id, FunctionStatusPaused); err != nil {
			t.Fatalf("pause: %v", err)
		}
		before, err := client.GetFunction(ctx, id)
		if err != nil {
			t.Fatalf("GetFunction: %v", err)
		}
		hist, err := client.ListFunctionHistory(ctx, id, ListFunctionHistoryOptions{Limit: 100})
		if err != nil || len(hist.Entries) == 0 {
			t.Fatalf("ListFunctionHistory = %+v, %v", hist, err)
		}
		created := hist.Entries[len(hist.Entries)-1]
		if created.ChangeType != FunctionChangeCreated {
			t.Fatalf("oldest entry = %+v, want created", created)
		}

		back, err := client.RollbackFunction(ctx, id, int64(created.EntityVersion), "readiness rollback")
		if err != nil {
			t.Fatalf("RollbackFunction: %v", err)
		}
		if back.Description != "first" || back.Status != FunctionStatusActive || back.Version != before.Version+1 {
			t.Fatalf("rolled back = %+v, want description first, active, version %d", back, before.Version+1)
		}
		if got, err := client.GetFunction(ctx, id); err != nil || got.Description != "first" || got.Status != FunctionStatusActive {
			t.Fatalf("GetFunction after rollback = %+v, %v", got, err)
		}
		hist, err = client.ListFunctionHistory(ctx, id, ListFunctionHistoryOptions{Limit: 1})
		if err != nil || len(hist.Entries) != 1 {
			t.Fatalf("ListFunctionHistory = %+v, %v", hist, err)
		}
		if e := hist.Entries[0]; e.ChangeType != FunctionChangeRollback || e.ChangeReason != "readiness rollback" || e.FunctionSnapshot.Description != "first" {
			t.Fatalf("newest entry = %+v, want the rollback with its reason", e)
		}
		if _, err := client.RollbackFunction(ctx, id, 9999, ""); !hasErrorCode(err, "not_found") {
			t.Fatalf("rollback to a missing version = %v, want not_found", err)
		}
	})

	t.Run("function-delete", func(t *testing.T) {
		if err := client.DeleteFunction(ctx, id); err != nil {
			t.Fatalf("DeleteFunction: %v", err)
		}
		if _, err := client.GetFunction(ctx, id); !hasErrorCode(err, "not_found") {
			t.Fatalf("GetFunction after delete = %v, want not_found", err)
		}
		all, err := client.ListFunctions(ctx)
		if err != nil {
			t.Fatalf("ListFunctions: %v", err)
		}
		for _, f := range all {
			if f.ID == id {
				t.Fatalf("ListFunctions still has %s", id)
			}
		}
		// The history outlives the function and ends with the delete.
		hist, err := client.ListFunctionHistory(ctx, id, ListFunctionHistoryOptions{Limit: 1})
		if err != nil || len(hist.Entries) != 1 || hist.Entries[0].ChangeType != FunctionChangeDelete {
			t.Fatalf("history after delete = %+v, %v, want the delete entry", hist, err)
		}
		if err := client.DeleteFunction(ctx, id); !hasErrorCode(err, "not_found") {
			t.Fatalf("second delete = %v, want not_found", err)
		}
	})
}
