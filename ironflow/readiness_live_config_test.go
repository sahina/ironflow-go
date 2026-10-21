package ironflow

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"
)

// TestLiveReadinessConfig covers the config group (#3003). The writes are set-up
// for the reads; config-write is proven by TestLiveWatch.
func TestLiveReadinessConfig(t *testing.T) {
	var seed struct{}
	client, ctx := liveReadiness(t, &seed)
	cfg := client.Config()

	t.Run("config-read", func(t *testing.T) {
		name := fmt.Sprintf("gocfg%d", time.Now().UnixNano())
		// The non-ASCII value proves the data survives as UTF-8.
		data := map[string]any{"a": 1.0, "s": "José", "nested": map[string]any{"k": []any{1.0, 2.0}}}
		set, err := cfg.Set(ctx, name, data)
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
		defer func() { _ = cfg.Delete(ctx, name) }()

		got, err := cfg.Get(ctx, name)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != name || !reflect.DeepEqual(got.Data, data) || got.Revision != set.Revision || got.UpdatedAt == "" {
			t.Fatalf("Get = %+v, want %s with %v at revision %d", got, name, data, set.Revision)
		}

		// List gives the name, revision and time, not the data.
		find := func() *ConfigEntry {
			t.Helper()
			entries, err := cfg.List(ctx)
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if i := slices.IndexFunc(entries, func(e ConfigEntry) bool { return e.Name == name }); i >= 0 {
				return &entries[i]
			}
			return nil
		}
		if e := find(); e == nil || e.Revision != set.Revision || e.UpdatedAt == "" {
			t.Fatalf("List entry = %+v, want %s at revision %d", e, name, set.Revision)
		}

		patched, err := cfg.Patch(ctx, name, map[string]any{"b": true})
		if err != nil || patched.Revision <= set.Revision {
			t.Fatalf("Patch = %+v, %v, want a revision after %d", patched, err, set.Revision)
		}
		got, err = cfg.Get(ctx, name)
		want := map[string]any{"a": 1.0, "s": "José", "nested": map[string]any{"k": []any{1.0, 2.0}}, "b": true}
		if err != nil || !reflect.DeepEqual(got.Data, want) || got.Revision != patched.Revision {
			t.Fatalf("Get after patch = %+v, %v, want %v at revision %d", got, err, want, patched.Revision)
		}
		if e := find(); e == nil || e.Revision != patched.Revision {
			t.Fatalf("List entry after patch = %+v, want revision %d", e, patched.Revision)
		}

		if err := cfg.Delete(ctx, name); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if _, err := cfg.Get(ctx, name); !hasErrorCode(err, "not_found") {
			t.Fatalf("Get after delete = %v, want not_found", err)
		}
		if e := find(); e != nil {
			t.Fatalf("List after delete still has %+v", e)
		}
	})
}
