package ironflow

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestLiveReadinessCluster covers health-ready, capabilities-discovery and
// server-info (#3003). The server-info route (IronflowService/Info, behind
// `ironflow server info`) has no SDK method, so it is read over raw HTTP.
func TestLiveReadinessCluster(t *testing.T) {
	var seed struct{}
	client, ctx := liveReadiness(t, &seed)
	server := os.Getenv("IRONFLOW_TEST_SERVER")

	get := func(path string, out any) int {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server+path, nil) // no credential: these routes are public
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(resp.Body)
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("GET %s: not JSON: %s", path, raw)
		}
		return resp.StatusCode
	}

	caps, err := client.GetCapabilities(ctx)
	if err != nil {
		t.Fatalf("GetCapabilities: %v", err)
	}

	t.Run("capabilities-discovery", func(t *testing.T) {
		if caps.Version == "" || !slices.Contains(caps.Transports, "websocket") {
			t.Errorf("GetCapabilities = %+v, want a version and the websocket transport", caps)
		}
		for _, f := range []string{"replay", "wildcard-patterns"} {
			if !slices.Contains(caps.Features, f) {
				t.Errorf("features %v miss %q", caps.Features, f)
			}
		}
		// The SDK type drops the other fields, so read them off the wire. The
		// route is public: no key is sent.
		var wire struct {
			AuthRequired        bool  `json:"auth_required"`
			FilesMaxObjectBytes int64 `json:"files_max_object_bytes"`
			Version             string
		}
		if code := get("/api/v1/capabilities", &wire); code != http.StatusOK || !wire.AuthRequired || wire.FilesMaxObjectBytes <= 0 || wire.Version != caps.Version {
			t.Errorf("GET /api/v1/capabilities = %d %+v, want 200, auth_required, a files cap and version %q", code, wire, caps.Version)
		}
	})

	t.Run("health-ready", func(t *testing.T) {
		// The Connect Health RPC the SDK calls.
		if status, err := client.Health(ctx); err != nil || status != "healthy" {
			t.Errorf("Health = %q, %v, want healthy", status, err)
		}
		var health struct{ Status, Timestamp, Version string }
		if code := get("/health", &health); code != http.StatusOK || health.Status != "healthy" || health.Version != caps.Version {
			t.Errorf("GET /health = %d %+v, want 200 healthy at version %q", code, health, caps.Version)
		}
		if ts, err := time.Parse(time.RFC3339, health.Timestamp); err != nil || time.Since(ts) > time.Minute || time.Until(ts) > time.Minute {
			t.Errorf("/health timestamp %q is not now (%v)", health.Timestamp, err)
		}
		if ready, err := client.Ready(ctx); err != nil || ready.Status != "ready" {
			t.Errorf("Ready = %+v, %v, want ready", ready, err)
		}
	})

	t.Run("server-info", func(t *testing.T) {
		code, body := identityPost(t, server+"/ironflow.v1.IronflowService/Info", os.Getenv("IRONFLOW_TEST_API_KEY"), map[string]any{})
		var info struct {
			Version          string
			GoVersion        string
			StartedAt        time.Time
			FunctionCount    int32
			ConnectedWorkers *int32
		}
		if code != http.StatusOK || json.Unmarshal(body, &info) != nil {
			t.Fatalf("Info = %d %s", code, body)
		}
		if info.Version != caps.Version || !strings.HasPrefix(info.GoVersion, "go") {
			t.Errorf("Info = %s, want version %q and a Go version", body, caps.Version)
		}
		if info.StartedAt.IsZero() || info.StartedAt.After(time.Now()) || time.Since(info.StartedAt) > 6*time.Hour {
			t.Errorf("startedAt = %v, want a recent past time", info.StartedAt)
		}
		// The seed command runs to the end, registering every group's functions
		// in this environment, before any suite starts, so the order of the
		// groups cannot matter. Connect JSON omits a zero, so >= 1 also proves
		// the field is present.
		if fns, err := client.ListFunctions(ctx); err != nil || info.FunctionCount < 1 || int(info.FunctionCount) < len(fns) {
			t.Errorf("functionCount = %d, ListFunctions = %d, %v, want a count that covers the listing", info.FunctionCount, len(fns), err)
		}
		// Not pinned to 0: the handler returns a stub 0 until it counts
		// connections. Connect JSON omits a zero, so the field may be absent; when
		// the server sends it, it must be a non-negative integer (the decode above
		// rejects a fraction).
		if w := info.ConnectedWorkers; w != nil && *w < 0 {
			t.Errorf("connectedWorkers = %d, want 0 or more", *w)
		}
		if code, _ := identityPost(t, server+"/ironflow.v1.IronflowService/Info", "", map[string]any{}); code != http.StatusUnauthorized {
			t.Errorf("Info without a key = %d, want 401", code)
		}
	})
}
