package ironflow

import (
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// recordEnvServer answers every request with `{}` and records each path's
// X-Ironflow-Environment value ("<absent>" when the header is missing).
func recordEnvServer(t *testing.T) (*httptest.Server, func() map[string]string) {
	t.Helper()
	var mu sync.Mutex
	seen := map[string]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v, ok := r.Header[http.CanonicalHeaderKey(HeaderEnvironment)]
		mu.Lock()
		if ok {
			seen[r.URL.Path] = v[0]
		} else {
			seen[r.URL.Path] = "<absent>"
		}
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	return srv, func() map[string]string {
		mu.Lock()
		defer mu.Unlock()
		return maps.Clone(seen)
	}
}

func TestClientEnvironment_SetsHeaderOnRESTAndRPC(t *testing.T) {
	srv, seen := recordEnvServer(t)
	c := NewClient(ClientConfig{ServerURL: srv.URL, Environment: "staging", Logger: NewNoopLogger()})
	ctx := context.Background()

	_, _ = c.Emit(ctx, "order.placed", map[string]any{"id": 1})      // executeRequest
	_, _ = c.ListWorkers(ctx)                                        // hand-rolled request
	_ = c.PatchStep(ctx, "step_1", map[string]any{"a": 1}, "reason") // ConnectRPC interceptor

	got := seen()
	for _, path := range []string{
		"/ironflow.v1.PubSubService/Emit",
		"/api/v1/workers",
		"/ironflow.v1.IronflowService/PatchStep",
	} {
		if got[path] != "staging" {
			t.Errorf("%s: X-Ironflow-Environment = %q, want staging", path, got[path])
		}
	}
}

// Webhooks, projection delete, KV and config build their requests outside
// executeRequest. Secret routes keep their own "current" value.
func TestClientEnvironment_HandBuiltRequests(t *testing.T) {
	srv, seen := recordEnvServer(t)
	c := NewClient(ClientConfig{ServerURL: srv.URL, Environment: "staging", Logger: NewNoopLogger()})
	ctx := context.Background()

	_, _ = c.Webhooks().ListSources(ctx)
	_ = c.Projections().Delete(ctx, "p1")
	_, _ = c.Config().Get(ctx, "app")
	_, _ = c.KV().Bucket("b").Get(ctx, "k")
	_, _ = c.KV().Bucket("b").Put(ctx, "k2", []byte("v"))
	_, _ = c.Secrets().Get(ctx, "s1")
	// The server does not upgrade; the handshake headers still arrive.
	_, _ = c.KV().Bucket("b").Watch(ctx, KVWatchCallbacks{})
	_, _ = c.Config().Watch(ctx, "app", ConfigWatchCallbacks{})

	got := seen()
	for path, want := range map[string]string{
		"/ironflow.v1.WebhookService/ListWebhookSources": "staging",
		"/api/v1/projections/p1":                         "staging",
		"/api/v1/config/app":                             "staging",
		"/api/v1/kv/buckets/b/keys/k":                    "staging",
		"/api/v1/kv/buckets/b/keys/k2":                   "staging",
		"/api/v1/kv/buckets/b/watch":                     "staging",
		"/api/v1/config/app/watch":                       "staging",
		"/api/v1/secrets/s1":                             "current",
	} {
		if got[path] != want {
			t.Errorf("%s: X-Ironflow-Environment = %q, want %q", path, got[path], want)
		}
	}
}

// The projection wait stream uses its own h2c client, so it needs an h2c server.
func TestClientEnvironment_ProjectionWaitStream(t *testing.T) {
	got := make(chan string, 1)
	srv := httptest.NewUnstartedServer(h2c.NewHandler(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case got <- r.Header.Get(HeaderEnvironment):
		default:
		}
	}), &http2.Server{}))
	srv.Start()
	t.Cleanup(srv.Close)

	c := NewClient(ClientConfig{ServerURL: srv.URL, Environment: "staging", Logger: NewNoopLogger()})
	if _, cancel, err := c.WaitForProjectionStream(context.Background(), "p1", WaitForProjectionOpts{}); err == nil {
		defer cancel()
	}
	select {
	case v := <-got:
		if v != "staging" {
			t.Errorf("X-Ironflow-Environment = %q, want staging", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the wait stream request did not reach the server")
	}
}

func TestClientEnvironment_UnsetSendsNoHeaderEvenWithIronflowEnv(t *testing.T) {
	t.Setenv(EnvEnvironment, "qa")
	srv, seen := recordEnvServer(t)
	c := NewClient(ClientConfig{ServerURL: srv.URL, Logger: NewNoopLogger()})
	ctx := context.Background()

	_, _ = c.Emit(ctx, "order.placed", map[string]any{"id": 1})
	_, _ = c.ListWorkers(ctx)
	_ = c.PatchStep(ctx, "step_1", map[string]any{"a": 1}, "reason")

	got := seen()
	if len(got) != 3 {
		t.Fatalf("saw %d paths, want 3: %v", len(got), got)
	}
	for path, v := range got {
		if v != "<absent>" {
			t.Errorf("%s: X-Ironflow-Environment = %q, want no header", path, v)
		}
	}
}
