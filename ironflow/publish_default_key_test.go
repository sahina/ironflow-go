package ironflow

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// keyRecorder is a publish endpoint that records each request's idempotencyKey.
func keyRecorder(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var keys []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		k, _ := body["idempotencyKey"].(string)
		mu.Lock()
		keys = append(keys, k)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"eventId": "e", "sequence": "1"})
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), keys...) }
}

func newRootCtx(srvURL string) Context {
	exec := &executionContext{
		runID:          "r1",
		stepCounters:   make(map[string]int),
		completedSteps: make(map[string]*CompletedStep),
		logger:         &captureLogger{},
		serverURL:      srvURL,
	}
	return Context{exec: exec}
}

func assertKeyShape(t *testing.T, k string) {
	t.Helper()
	if !strings.HasPrefix(k, "step-") || len(k) > 255 || strings.ContainsAny(k, " \t\r\n\x00") {
		t.Fatalf("bad default key %q (len %d)", k, len(k))
	}
}

func TestPublishDefaultKeyStableOnReplay(t *testing.T) {
	srv, keys := keyRecorder(t)
	for range 2 { // a fresh invocation of the same run: counters restart, nothing memoized
		if err := Publish(newRootCtx(srv.URL), "t", map[string]any{"a": 1}); err != nil {
			t.Fatal(err)
		}
	}
	k := keys()
	if len(k) != 2 || k[0] != k[1] {
		t.Fatalf("keys = %v, want two equal", k)
	}
	assertKeyShape(t, k[0])

	b1, _ := newBranch(t, nil)
	b2, _ := newBranch(t, nil)
	b1.parent.serverURL, b2.parent.serverURL = srv.URL, srv.URL
	_ = PublishWithBranch(b1, "t", 1)
	_ = PublishWithBranch(b2, "t", 1)
	k = keys()
	if k[2] != k[3] {
		t.Fatalf("branch replay keys differ: %v", k[2:])
	}
}

func TestPublishDefaultKeyDiffersPerCall(t *testing.T) {
	srv, keys := keyRecorder(t)
	ctx := newRootCtx(srv.URL)
	_ = Publish(ctx, "t", 1)
	_ = Publish(ctx, "t", 1)
	k := keys()
	if len(k) != 2 || k[0] == k[1] {
		t.Fatalf("keys = %v, want two distinct", k)
	}
}

func TestPublishExplicitKeyWins(t *testing.T) {
	srv, keys := keyRecorder(t)
	_ = Publish(newRootCtx(srv.URL), "t", 1, WithPublishIdempotencyKey("mine"))
	b, exec := newBranch(t, nil)
	exec.serverURL = srv.URL
	_ = PublishWithBranch(b, "t", 1, WithPublishIdempotencyKey("mine2"))
	if k := keys(); len(k) != 2 || k[0] != "mine" || k[1] != "mine2" {
		t.Fatalf("keys = %v", k)
	}
}

func TestPublishDefaultKeyLongTopicAndSpacedBranch(t *testing.T) {
	srv, keys := keyRecorder(t)
	topic := strings.Repeat("x", 249)
	if err := Publish(newRootCtx(srv.URL), topic, 1); err != nil {
		t.Fatal(err)
	}
	b, exec := newBranch(t, nil)
	exec.serverURL = srv.URL
	spaced := exec.createBranchContext("my branch name", 0)
	if err := PublishWithBranch(spaced, topic, 1); err != nil {
		t.Fatal(err)
	}
	for _, k := range keys() {
		assertKeyShape(t, k)
	}
	_ = b
}

func TestPublishDoesNotShiftLaterStepIDs(t *testing.T) {
	srv, _ := keyRecorder(t)
	ctx := newRootCtx(srv.URL)
	_ = Publish(ctx, "t", 1)
	if _, err := Run(ctx, "after", func() (int, error) { return 1, nil }); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, s := range ctx.exec.executedSteps {
		got = append(got, s.ID)
	}
	want := []string{"r1:publish:t:0", "r1:after:0"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("step IDs = %v, want %v", got, want)
	}
}

func TestRootPublishKeyCoversPayload(t *testing.T) {
	srv, keys := keyRecorder(t)
	// Same step ID ("r1:publish:t:0") in two fresh invocations, different data:
	// the flipped-index case from Parallel must not collide.
	_ = Publish(newRootCtx(srv.URL), "t", map[string]any{"branch": "A"})
	_ = Publish(newRootCtx(srv.URL), "t", map[string]any{"branch": "B"})
	if k := keys(); len(k) != 2 || k[0] == k[1] {
		t.Fatalf("keys = %v, want distinct", k)
	}
}
