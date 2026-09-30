package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type dedupTestResult struct {
	OrderID string `json:"order_id"`
	Version int    `json:"version"`
}

// kvEntryJSON returns a JSON-encoded KVEntry where Value is base64-encoded
// (Go's json.Marshal encodes []byte as base64 automatically).
func kvEntryJSON(t *testing.T, key string, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("kvEntryJSON marshal: %v", err)
	}
	entry := struct {
		Key       string    `json:"key"`
		Value     []byte    `json:"value"`
		Revision  uint64    `json:"revision"`
		Operation string    `json:"operation"`
		CreatedAt time.Time `json:"created_at"`
	}{Key: key, Value: b, Revision: 1, Operation: "put", CreatedAt: time.Now()}
	out, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("kvEntryJSON encode: %v", err)
	}
	return out
}

func errorJSON(msg string) []byte {
	b, _ := json.Marshal(map[string]string{"error": msg})
	return b
}

func bucketInfoJSON() []byte {
	b, _ := json.Marshal(map[string]any{"name": "test-bucket", "values": 0, "bytes": 0, "history": 1})
	return b
}

// setupDedupServer creates a mock server with configurable per-route responses.
// Routes: "bucket" (POST /api/v1/kv/buckets), "create", "put", "get", "delete".
type routeConfig struct {
	status int
	body   []byte
}

func setupDedupServer(t *testing.T, routes map[string]routeConfig) (*Client, func()) {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var cfg routeConfig
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/kv/buckets"):
			cfg = routes["bucket"]
		case r.Method == http.MethodDelete:
			cfg = routes["delete"]
		case r.Method == http.MethodGet:
			cfg = routes["get"]
		case r.Method == http.MethodPut && r.Header.Get("If-None-Match") == "*":
			cfg = routes["create"]
		case r.Method == http.MethodPut:
			cfg = routes["put"]
		}
		if cfg.body == nil {
			cfg.body = []byte("{}")
		}
		if cfg.status == 0 {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(cfg.status)
		w.Write(cfg.body)
	})
	return setupMockKVServer(t, handler)
}

// ============================================================================
// TryClaim
// ============================================================================

func TestCommandDedup_TryClaim_Winner(t *testing.T) {
	client, cleanup := setupDedupServer(t, map[string]routeConfig{
		"bucket": {201, bucketInfoJSON()},
		"create": {201, []byte(`{"revision":1}`)},
	})
	defer cleanup()

	d, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("NewCommandDedup: %v", err)
	}

	prior, err := d.TryClaim(context.Background(), "cmd-1", dedupTestResult{OrderID: "ord-1", Version: 0})
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	if prior != nil {
		t.Errorf("expected nil (winner), got %+v", prior)
	}
}

func TestCommandDedup_TryClaim_Loser(t *testing.T) {
	stored := dedupTestResult{OrderID: "ord-1", Version: 3}
	client, cleanup := setupDedupServer(t, map[string]routeConfig{
		"bucket": {201, bucketInfoJSON()},
		"create": {412, errorJSON("key already exists")},
		"get":    {200, kvEntryJSON(t, "cmd-1", stored)},
	})
	defer cleanup()

	d, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("NewCommandDedup: %v", err)
	}

	prior, err := d.TryClaim(context.Background(), "cmd-1", dedupTestResult{OrderID: "other"})
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	if prior == nil {
		t.Fatal("expected prior entry, got nil")
	}
	if prior.OrderID != stored.OrderID || prior.Version != stored.Version {
		t.Errorf("got %+v, want %+v", *prior, stored)
	}
}

func TestCommandDedup_TryClaim_CorruptEntry_FailsClosed(t *testing.T) {
	client, cleanup := setupDedupServer(t, map[string]routeConfig{
		"bucket": {201, bucketInfoJSON()},
		"create": {412, errorJSON("key already exists")},
		"get":    {200, []byte(`{"key":"cmd-1","value":"bm90LXZhbGlkLWpzb24=","revision":1,"operation":"put","created_at":"2026-01-01T00:00:00Z"}`)},
		// value is base64("not-valid-json") — corrupt entry
	})
	defer cleanup()

	d, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("NewCommandDedup: %v", err)
	}

	_, err = d.TryClaim(context.Background(), "cmd-1", dedupTestResult{})
	if err == nil {
		t.Fatal("expected error for corrupt entry, got nil (fail-closed)")
	}
}

// claimRaceServer answers the create-only PUT with createStatus[n] (n = 0-based call count,
// last entry repeats) and every GET with getStatus / getBody.
func claimRaceServer(t *testing.T, createStatus []int, getStatus int, getBody []byte) (*Client, *atomic.Int32, func()) {
	t.Helper()
	var creates atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/kv/buckets"):
			w.WriteHeader(201)
			w.Write(bucketInfoJSON())
		case r.Method == http.MethodPut && r.Header.Get("If-None-Match") == "*":
			n := int(creates.Add(1)) - 1
			if n >= len(createStatus) {
				n = len(createStatus) - 1
			}
			w.WriteHeader(createStatus[n])
			w.Write([]byte(`{"revision":1}`))
		case r.Method == http.MethodGet:
			w.WriteHeader(getStatus)
			w.Write(getBody)
		default:
			w.WriteHeader(500)
		}
	})
	client, cleanup := setupMockKVServer(t, handler)
	return client, &creates, cleanup
}

func newRaceDedup(t *testing.T, client *Client) *CommandDedup[dedupTestResult] {
	t.Helper()
	d, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("NewCommandDedup: %v", err)
	}
	return d
}

func TestCommandDedup_TryClaim_LoserMissingEntry_ClaimsAgain(t *testing.T) {
	// 412, the winner releases (GET 404), then the second create wins.
	client, creates, cleanup := claimRaceServer(t, []int{412, 201}, 404, errorJSON("key not found"))
	defer cleanup()

	prior, err := newRaceDedup(t, client).TryClaim(context.Background(), "cmd-1", dedupTestResult{})
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	if prior != nil {
		t.Errorf("expected nil (winner), got %+v", prior)
	}
	if n := creates.Load(); n != 2 {
		t.Errorf("expected 2 create writes, got %d", n)
	}
}

func TestCommandDedup_TryClaim_LostRaceExhausted_Errors(t *testing.T) {
	client, creates, cleanup := claimRaceServer(t, []int{412}, 404, errorJSON("key not found"))
	defer cleanup()

	prior, err := newRaceDedup(t, client).TryClaim(context.Background(), "cmd-1", dedupTestResult{})
	if prior != nil {
		t.Errorf("expected nil prior, got %+v", prior)
	}
	var ife *IronflowError
	if !errors.As(err, &ife) || ife.Code != "COMMAND_DEDUP_RACE" {
		t.Fatalf("expected COMMAND_DEDUP_RACE error, got %v", err)
	}
	if n := creates.Load(); n != 3 {
		t.Errorf("expected exactly 3 create writes, got %d", n)
	}
}

func TestCommandDedup_TryClaim_ReadBackErrorDoesNotLoop(t *testing.T) {
	client, creates, cleanup := claimRaceServer(t, []int{412}, 403, errorJSON("forbidden"))
	defer cleanup()

	_, err := newRaceDedup(t, client).TryClaim(context.Background(), "cmd-1", dedupTestResult{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if n := creates.Load(); n != 1 {
		t.Errorf("expected 1 create write, got %d", n)
	}
}

func TestCommandDedup_TryClaim_IsOwner(t *testing.T) {
	// Version carries the caller's attempt token; OrderID "claimed" vs "done" is the status.
	owns := func(p dedupTestResult) bool { return p.Version == 7 && p.OrderID == "claimed" }
	tests := []struct {
		name      string
		stored    dedupTestResult
		wantPrior bool
	}{
		{"own orphan is reclaimed", dedupTestResult{OrderID: "claimed", Version: 7}, false},
		{"other caller's claim is a duplicate", dedupTestResult{OrderID: "claimed", Version: 8}, true},
		{"own finalized result is not replayed", dedupTestResult{OrderID: "done", Version: 7}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, _, cleanup := claimRaceServer(t, []int{412}, 200, kvEntryJSON(t, "cmd-1", tc.stored))
			defer cleanup()

			prior, err := newRaceDedup(t, client).TryClaim(context.Background(), "cmd-1",
				dedupTestResult{OrderID: "claimed", Version: 7}, TryClaimOptions[dedupTestResult]{IsOwner: owns})
			if err != nil {
				t.Fatalf("TryClaim: %v", err)
			}
			if (prior != nil) != tc.wantPrior {
				t.Errorf("prior = %+v, wantPrior %v", prior, tc.wantPrior)
			}
		})
	}
}

func TestCommandDedup_TryClaim_IsOwnerNotCalledOnWin(t *testing.T) {
	client, _, cleanup := claimRaceServer(t, []int{201}, 200, nil)
	defer cleanup()

	boom := func(dedupTestResult) bool {
		t.Error("IsOwner must not run on the winner path")
		return false
	}
	prior, err := newRaceDedup(t, client).TryClaim(context.Background(), "cmd-1", dedupTestResult{},
		TryClaimOptions[dedupTestResult]{IsOwner: boom})
	if err != nil || prior != nil {
		t.Fatalf("expected (nil, nil), got (%+v, %v)", prior, err)
	}
}

func TestCommandDedup_TryClaim_Error(t *testing.T) {
	client, cleanup := setupDedupServer(t, map[string]routeConfig{
		"bucket": {201, bucketInfoJSON()},
		"create": {500, errorJSON("server error")},
	})
	defer cleanup()

	d, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("NewCommandDedup: %v", err)
	}

	_, err = d.TryClaim(context.Background(), "cmd-1", dedupTestResult{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ============================================================================
// Finalize
// ============================================================================

func TestCommandDedup_Finalize_OK(t *testing.T) {
	client, cleanup := setupDedupServer(t, map[string]routeConfig{
		"bucket": {201, bucketInfoJSON()},
		"put":    {200, []byte(`{"revision":2}`)},
	})
	defer cleanup()

	d, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("NewCommandDedup: %v", err)
	}

	if err := d.Finalize(context.Background(), "cmd-1", dedupTestResult{OrderID: "ord-1", Version: 1}); err != nil {
		t.Errorf("Finalize: %v", err)
	}
}

func TestCommandDedup_Finalize_Error(t *testing.T) {
	client, cleanup := setupDedupServer(t, map[string]routeConfig{
		"bucket": {201, bucketInfoJSON()},
		"put":    {500, errorJSON("server error")},
	})
	defer cleanup()

	d, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("NewCommandDedup: %v", err)
	}

	if err := d.Finalize(context.Background(), "cmd-1", dedupTestResult{}); err == nil {
		t.Error("expected error, got nil")
	}
}

// ============================================================================
// Release
// ============================================================================

func TestCommandDedup_Release_OK(t *testing.T) {
	client, cleanup := setupDedupServer(t, map[string]routeConfig{
		"bucket": {201, bucketInfoJSON()},
		"delete": {204, nil},
	})
	defer cleanup()

	d, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("NewCommandDedup: %v", err)
	}

	if err := d.Release(context.Background(), "cmd-1"); err != nil {
		t.Errorf("Release: %v", err)
	}
}

func TestCommandDedup_Release_NotFound(t *testing.T) {
	client, cleanup := setupDedupServer(t, map[string]routeConfig{
		"bucket": {201, bucketInfoJSON()},
		"delete": {404, errorJSON("key not found")},
	})
	defer cleanup()

	d, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("NewCommandDedup: %v", err)
	}

	if err := d.Release(context.Background(), "cmd-1"); err != nil {
		t.Errorf("Release on 404 should be swallowed, got: %v", err)
	}
}

func TestCommandDedup_Release_Error(t *testing.T) {
	client, cleanup := setupDedupServer(t, map[string]routeConfig{
		"bucket": {201, bucketInfoJSON()},
		"delete": {500, errorJSON("server error")},
	})
	defer cleanup()

	d, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("NewCommandDedup: %v", err)
	}

	if err := d.Release(context.Background(), "cmd-1"); err == nil {
		t.Error("expected error, got nil")
	}
}

// ============================================================================
// ensureBucket
// ============================================================================

func TestCommandDedup_EnsureBucket_Cached(t *testing.T) {
	var bucketCalls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/kv/buckets") {
			bucketCalls.Add(1)
			w.WriteHeader(201)
			w.Write(bucketInfoJSON())
			return
		}
		// create endpoint — return 201 for both TryClaim calls
		w.WriteHeader(201)
		w.Write([]byte(`{"revision":1}`))
	})
	client, cleanup := setupMockKVServer(t, handler)
	defer cleanup()

	d, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("NewCommandDedup: %v", err)
	}
	_, _ = d.TryClaim(context.Background(), "cmd-1", dedupTestResult{})
	_, _ = d.TryClaim(context.Background(), "cmd-2", dedupTestResult{})

	if n := bucketCalls.Load(); n != 1 {
		t.Errorf("expected createBucket called once, got %d", n)
	}
}

func TestCommandDedup_EnsureBucket_AlreadyExists(t *testing.T) {
	client, cleanup := setupDedupServer(t, map[string]routeConfig{
		"bucket": {409, errorJSON("bucket already exists")},
	})
	defer cleanup()

	_, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Errorf("409 on createBucket should succeed, got: %v", err)
	}
}

func TestCommandDedup_EnsureBucket_RetryAfterFailure(t *testing.T) {
	// First NewCommandDedup call fails (503). Second instance retries and succeeds.
	// With sync.Once, retries happen by creating a new instance.
	var callCount atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/kv/buckets") {
			n := callCount.Add(1)
			if n == 1 {
				w.WriteHeader(503)
				w.Write(errorJSON("unavailable"))
				return
			}
			w.WriteHeader(201)
			w.Write(bucketInfoJSON())
			return
		}
		w.WriteHeader(201)
		w.Write([]byte(`{"revision":1}`))
	})
	client, cleanup := setupMockKVServer(t, handler)
	defer cleanup()

	_, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err == nil {
		t.Fatal("expected error on first call, got nil")
	}

	// New instance — each instance has its own sync.Once so this retries independently.
	d2, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("second NewCommandDedup should succeed: %v", err)
	}
	prior, err := d2.TryClaim(context.Background(), "cmd-1", dedupTestResult{})
	if err != nil {
		t.Fatalf("TryClaim after retry: %v", err)
	}
	if prior != nil {
		t.Errorf("expected nil (winner), got %+v", prior)
	}
}

// ============================================================================
// Concurrent TryClaim — race safety
// ============================================================================

func TestCommandDedup_TryClaim_ConcurrentGoroutines(t *testing.T) {
	// Simulate N goroutines racing to claim the same commandId.
	// The first create succeeds (201); all others get 412 and read the winner's entry.
	const N = 10
	stored := dedupTestResult{OrderID: "ord-1", Version: 5}

	var createCalls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/kv/buckets"):
			w.WriteHeader(201)
			w.Write(bucketInfoJSON())
		case r.Method == http.MethodPut && r.Header.Get("If-None-Match") == "*":
			n := createCalls.Add(1)
			if n == 1 {
				w.WriteHeader(201)
				w.Write([]byte(`{"revision":1}`))
			} else {
				w.WriteHeader(412)
				w.Write(errorJSON("key already exists"))
			}
		case r.Method == http.MethodGet:
			w.WriteHeader(200)
			w.Write(kvEntryJSON(t, "cmd-1", stored))
		default:
			w.WriteHeader(500)
		}
	})
	client, cleanup := setupMockKVServer(t, handler)
	defer cleanup()

	d, err := NewCommandDedup[dedupTestResult](context.Background(), client.KV(), "test-bucket", CommandDedupOptions{})
	if err != nil {
		t.Fatalf("NewCommandDedup: %v", err)
	}

	type result struct {
		prior *dedupTestResult
		err   error
	}
	results := make(chan result, N)
	for range N {
		go func() {
			prior, err := d.TryClaim(context.Background(), "cmd-1", dedupTestResult{OrderID: "goroutine"})
			results <- result{prior, err}
		}()
	}

	var winners, losers int
	for range N {
		r := <-results
		if r.err != nil {
			t.Errorf("TryClaim error: %v", r.err)
			continue
		}
		if r.prior == nil {
			winners++
		} else {
			losers++
			if r.prior.OrderID != stored.OrderID || r.prior.Version != stored.Version {
				t.Errorf("loser got wrong prior: %+v", *r.prior)
			}
		}
	}

	if winners != 1 {
		t.Errorf("expected exactly 1 winner, got %d", winners)
	}
	if losers != N-1 {
		t.Errorf("expected %d losers, got %d", N-1, losers)
	}
}
