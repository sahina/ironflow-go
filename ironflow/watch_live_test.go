package ironflow

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// idleGap outlasts any read deadline a watcher might poll with, so a watcher that
// stops reading after an idle second fails here (#2949).
const idleGap = 1500 * time.Millisecond

func next[T any](t *testing.T, ch <-chan T, errs <-chan error, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case err := <-errs:
		t.Fatalf("%s: watch error: %v", what, err)
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: no event in 10s", what)
	}
	var zero T
	return zero
}

// TestLiveWatch proves KV and config watch against a real engine (#2949).
// It skips unless IRONFLOW_TEST_SERVER is set; `make test-watch-live` sets it.
func TestLiveWatch(t *testing.T) {
	serverURL := os.Getenv("IRONFLOW_TEST_SERVER")
	if serverURL == "" {
		t.Skip("IRONFLOW_TEST_SERVER not set; run `make test-watch-live`")
	}
	client := NewClient(ClientConfig{ServerURL: serverURL, APIKey: os.Getenv("IRONFLOW_TEST_API_KEY")})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	t.Run("kv-watch replay, put, delete", func(t *testing.T) {
		name := "gowatch" + suffix
		if _, err := client.KV().CreateBucket(ctx, BucketConfig{Name: name}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		bucket := client.KV().Bucket(name)
		preRev, err := bucket.Put(ctx, "pre", []byte(`{"v":0}`))
		if err != nil {
			t.Fatalf("Put pre: %v", err)
		}
		events, errs := make(chan KVWatchEvent, 16), make(chan error, 1)
		w, err := bucket.Watch(ctx, KVWatchCallbacks{OnUpdate: func(e KVWatchEvent) { events <- e }, OnError: func(e error) { errs <- e }})
		if err != nil {
			t.Fatalf("Watch: %v", err)
		}
		defer w.Stop()

		pre := next(t, events, errs, "replay")
		if pre.Key != "pre" || pre.Operation != "put" || string(pre.Value) != `{"v":0}` || pre.Revision != preRev || pre.Bucket != name {
			t.Fatalf("replay = %+v, want pre put {\"v\":0} rev %d", pre, preRev)
		}

		time.Sleep(idleGap)
		value := []byte(`{"v":1,"s":"é"}`)
		putRev, err := bucket.Put(ctx, "user.a", value)
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		put := next(t, events, errs, "put")
		if put.Key != "user.a" || put.Operation != "put" || !reflect.DeepEqual(put.Value, value) || put.Revision != putRev {
			t.Fatalf("put = %+v, want user.a put %s rev %d", put, value, putRev)
		}

		if err := bucket.Delete(ctx, "user.a"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		del := next(t, events, errs, "delete")
		if del.Key != "user.a" || del.Operation != "delete" || del.Value != nil || del.Revision <= putRev {
			t.Fatalf("delete = %+v, want user.a delete, nil value, rev > %d", del, putRev)
		}
	})

	t.Run("kv-watch key filter", func(t *testing.T) {
		name := "gowatchf" + suffix
		if _, err := client.KV().CreateBucket(ctx, BucketConfig{Name: name}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		bucket := client.KV().Bucket(name)
		events, errs := make(chan KVWatchEvent, 16), make(chan error, 1)
		w, err := bucket.Watch(ctx, KVWatchCallbacks{OnUpdate: func(e KVWatchEvent) { events <- e }, OnError: func(e error) { errs <- e }}, WithWatchKey("user.*"))
		if err != nil {
			t.Fatalf("Watch: %v", err)
		}
		defer w.Stop()
		time.Sleep(500 * time.Millisecond)
		if _, err := bucket.Put(ctx, "other.x", []byte(`1`)); err != nil {
			t.Fatalf("Put other: %v", err)
		}
		if _, err := bucket.Put(ctx, "user.b", []byte(`2`)); err != nil {
			t.Fatalf("Put user: %v", err)
		}
		if got := next(t, events, errs, "filtered"); got.Key != "user.b" || string(got.Value) != "2" {
			t.Fatalf("filtered = %+v, want user.b only", got)
		}
	})

	t.Run("kv-watch missing bucket", func(t *testing.T) {
		errs := make(chan error, 1)
		w, err := client.KV().Bucket("gonosuch"+suffix).Watch(ctx, KVWatchCallbacks{OnUpdate: func(KVWatchEvent) {}, OnError: func(e error) { errs <- e }})
		if err != nil {
			t.Fatalf("Watch: %v", err)
		}
		defer w.Stop()
		select {
		case err := <-errs:
			if !strings.Contains(err.Error(), "bucket not found") {
				t.Fatalf("error = %v, want bucket not found", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no error for a missing bucket")
		}
	})

	t.Run("config-watch replay, set, patch, delete", func(t *testing.T) {
		name := "gowatchcfg" + suffix
		pre, err := client.Config().Set(ctx, name, map[string]any{"a": 1.0})
		if err != nil {
			t.Fatalf("Set pre: %v", err)
		}
		events, errs := make(chan ConfigWatchEvent, 16), make(chan error, 1)
		w, err := client.Config().Watch(ctx, name, ConfigWatchCallbacks{OnUpdate: func(e ConfigWatchEvent) { events <- e }, OnError: func(e error) { errs <- e }})
		if err != nil {
			t.Fatalf("Watch: %v", err)
		}
		defer w.Stop()

		first := next(t, events, errs, "replay")
		if first.Name != name || !reflect.DeepEqual(first.Data, map[string]any{"a": 1.0}) || first.Revision != pre.Revision {
			t.Fatalf("replay = %+v, want a=1 rev %d", first, pre.Revision)
		}
		if ts, err := time.Parse(time.RFC3339Nano, first.UpdatedAt); err != nil || !strings.HasSuffix(first.UpdatedAt, "Z") || time.Since(ts) > time.Minute {
			t.Fatalf("updatedAt = %q, want a recent UTC RFC 3339 time", first.UpdatedAt)
		}

		time.Sleep(idleGap)
		set, err := client.Config().Set(ctx, name, map[string]any{"a": 2.0, "s": "é"})
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
		if got := next(t, events, errs, "set"); !reflect.DeepEqual(got.Data, map[string]any{"a": 2.0, "s": "é"}) || got.Revision != set.Revision {
			t.Fatalf("set = %+v, want a=2 s=é rev %d", got, set.Revision)
		}
		if _, err := client.Config().Patch(ctx, name, map[string]any{"b": true}); err != nil {
			t.Fatalf("Patch: %v", err)
		}
		if got := next(t, events, errs, "patch"); !reflect.DeepEqual(got.Data, map[string]any{"a": 2.0, "s": "é", "b": true}) {
			t.Fatalf("patch = %+v, want the merged document", got)
		}
		if err := client.Config().Delete(ctx, name); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if got := next(t, events, errs, "delete"); got.Data != nil {
			t.Fatalf("delete = %+v, want nil data", got)
		}
	})
}
