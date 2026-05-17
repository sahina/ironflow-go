package agent

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sahina/ironflow-go/ironflow"
)

const (
	testCallbackURL = "http://localhost:3000/api/ironflow/ironflow/agent-tools/dispatch"
	testAPIKey      = "ifkey_test_register"
	testHMACSecret  = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	testSchema      = `{"type":"object","properties":{"q":{"type":"string"}}}`
)

func TestExposeMcp_RejectsEmptyTools(t *testing.T) {
	_, err := ExposeMcp(ExposeMcpConfig{Name: "x", Version: "1", CallbackURL: testCallbackURL})
	if err == nil {
		t.Fatal("expected error on empty tools")
	}
	var ife *ironflow.IronflowError
	if !errors.As(err, &ife) || ife.Code != CodeMcpNoTools {
		t.Errorf("err code = %v, want %q", err, CodeMcpNoTools)
	}
}

func TestExposeMcp_RejectsMissingCallbackURL(t *testing.T) {
	_, err := ExposeMcp(ExposeMcpConfig{
		Name:    "x",
		Version: "1",
		Tools:   []McpToolDef{{Name: "t", InputSchemaJSON: testSchema, Handler: noopHandler}},
	})
	var ife *ironflow.IronflowError
	if !errors.As(err, &ife) || ife.Code != CodeMcpMissingCallbackURL {
		t.Errorf("err code = %v, want %q", err, CodeMcpMissingCallbackURL)
	}
}

func TestExposeMcp_RejectsMissingServerURL(t *testing.T) {
	t.Setenv("IRONFLOW_URL", "")
	t.Setenv("IRONFLOW_SERVER_URL", "")
	_, err := ExposeMcp(ExposeMcpConfig{
		Name:        "x",
		Version:     "1",
		CallbackURL: testCallbackURL,
		APIKey:      testAPIKey,
		Tools:       []McpToolDef{{Name: "t", InputSchemaJSON: testSchema, Handler: noopHandler}},
	})
	var ife *ironflow.IronflowError
	if !errors.As(err, &ife) || ife.Code != CodeMcpMissingServerURL {
		t.Errorf("err code = %v, want %q", err, CodeMcpMissingServerURL)
	}
}

func TestExposeMcp_RejectsMissingAPIKey(t *testing.T) {
	t.Setenv("IRONFLOW_API_KEY", "")
	_, err := ExposeMcp(ExposeMcpConfig{
		Name:        "x",
		Version:     "1",
		CallbackURL: testCallbackURL,
		ServerURL:   "http://localhost:9123",
		Tools:       []McpToolDef{{Name: "t", InputSchemaJSON: testSchema, Handler: noopHandler}},
	})
	var ife *ironflow.IronflowError
	if !errors.As(err, &ife) || ife.Code != CodeMcpMissingAPIKey {
		t.Errorf("err code = %v, want %q", err, CodeMcpMissingAPIKey)
	}
}

func TestExposeMcp_RejectsDuplicateTools(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()
	_, err := ExposeMcp(ExposeMcpConfig{
		Name:        "demo",
		Version:     "1",
		CallbackURL: testCallbackURL,
		ServerURL:   "http://localhost:9123",
		APIKey:      testAPIKey,
		Tools: []McpToolDef{
			{Name: "x", InputSchemaJSON: testSchema, Handler: noopHandler},
			{Name: "x", InputSchemaJSON: testSchema, Handler: noopHandler},
		},
	})
	var ife *ironflow.IronflowError
	if !errors.As(err, &ife) || ife.Code != CodeMcpDuplicateTool {
		t.Errorf("err code = %v, want %q", err, CodeMcpDuplicateTool)
	}
}

func TestExposeMcp_RejectsMissingSchema(t *testing.T) {
	_, err := ExposeMcp(ExposeMcpConfig{
		Name:        "demo",
		Version:     "1",
		CallbackURL: testCallbackURL,
		ServerURL:   "http://localhost:9123",
		APIKey:      testAPIKey,
		Tools:       []McpToolDef{{Name: "t", Handler: noopHandler}},
	})
	var ife *ironflow.IronflowError
	if !errors.As(err, &ife) || ife.Code != CodeMcpMissingSchema {
		t.Errorf("err code = %v, want %q", err, CodeMcpMissingSchema)
	}
}

func TestExposeMcp_RegisterToolWireShape(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.URL.Path != registerToolPath {
			t.Errorf("path = %s, want %s", r.URL.Path, registerToolPath)
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+testAPIKey {
			t.Errorf("authorization = %q, want Bearer %s", got, testAPIKey)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content-type = %q, want application/json", got)
		}
		var body registerToolRequestJSON
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode register body: %v", err)
		}
		if body.AgentName != "docproc" {
			t.Errorf("agentName = %q, want docproc", body.AgentName)
		}
		if body.CallbackURL != testCallbackURL {
			t.Errorf("callbackUrl mismatch")
		}
		if len(body.Tools) != 1 {
			t.Fatalf("tools len = %d, want 1", len(body.Tools))
		}
		if body.Tools[0].Name != "search" {
			t.Errorf("tool name = %q, want search", body.Tools[0].Name)
		}
		if !contains(body.Tools[0].RequiredScopes, "search") {
			t.Errorf("required scopes missing 'search': %v", body.Tools[0].RequiredScopes)
		}
		if !strings.Contains(body.Tools[0].InputSchemaJSON, "object") {
			t.Errorf("inputSchemaJson missing 'object': %q", body.Tools[0].InputSchemaJSON)
		}
		writeRegisterResponse(w, testHMACSecret, []string{"docproc.search"})
	}))
	defer server.Close()

	handle, err := ExposeMcp(ExposeMcpConfig{
		Name:        "docproc",
		Version:     "0.1.0",
		CallbackURL: testCallbackURL,
		ServerURL:   server.URL,
		APIKey:      testAPIKey,
		Tools: []McpToolDef{{
			Name:            "search",
			Description:     "Search the corpus",
			InputSchemaJSON: testSchema,
			Scopes:          []string{"search"},
			Handler:         func(input any) (any, error) { return map[string]any{"hits": 3}, nil },
		}},
	})
	if err != nil {
		t.Fatalf("ExposeMcp: %v", err)
	}
	if handle.Status != "active" {
		t.Errorf("status = %q, want active", handle.Status)
	}
	if handle.ToolCount != 1 {
		t.Errorf("toolCount = %d, want 1", handle.ToolCount)
	}
	if !equalSlices(handle.ToolNames, []string{"docproc.search"}) {
		t.Errorf("toolNames = %v, want [docproc.search]", handle.ToolNames)
	}

	entry, ok := lookupLocal("docproc.search")
	if !ok {
		t.Fatal("expected entry in local registry")
	}
	if entry.HMACSecret != testHMACSecret {
		t.Errorf("hmac mismatch")
	}
	if entry.AgentName != "docproc" {
		t.Errorf("agentName = %q, want docproc", entry.AgentName)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("server hits = %d, want 1", got)
	}
}

func TestExposeMcp_FallsBackToEnv(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeRegisterResponse(w, testHMACSecret, []string{"x.y"})
	}))
	defer server.Close()

	t.Setenv("IRONFLOW_URL", server.URL)
	t.Setenv("IRONFLOW_API_KEY", testAPIKey)
	handle, err := ExposeMcp(ExposeMcpConfig{
		Name:        "x",
		Version:     "0.1.0",
		CallbackURL: testCallbackURL,
		Tools:       []McpToolDef{{Name: "y", InputSchemaJSON: testSchema, Handler: noopHandler}},
	})
	if err != nil {
		t.Fatalf("ExposeMcp: %v", err)
	}
	if handle.Status != "active" {
		t.Errorf("status = %q, want active", handle.Status)
	}
}

func TestExposeMcp_TransportError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"permission_denied","message":"missing agent:tools:register"}`))
	}))
	defer server.Close()

	_, err := ExposeMcp(ExposeMcpConfig{
		Name:        "demo",
		Version:     "0.1.0",
		CallbackURL: testCallbackURL,
		ServerURL:   server.URL,
		APIKey:      testAPIKey,
		Tools:       []McpToolDef{{Name: "y", InputSchemaJSON: testSchema, Handler: noopHandler}},
	})
	var ife *ironflow.IronflowError
	if !errors.As(err, &ife) || ife.Code != CodeMcpTransportError {
		t.Fatalf("err = %v, want code %q", err, CodeMcpTransportError)
	}
	if !strings.Contains(err.Error(), "HTTP 403") {
		t.Errorf("err message missing HTTP 403: %v", err)
	}
}

func TestExposeMcp_InvalidResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	_, err := ExposeMcp(ExposeMcpConfig{
		Name:        "demo",
		Version:     "0.1.0",
		CallbackURL: testCallbackURL,
		ServerURL:   server.URL,
		APIKey:      testAPIKey,
		Tools:       []McpToolDef{{Name: "y", InputSchemaJSON: testSchema, Handler: noopHandler}},
	})
	var ife *ironflow.IronflowError
	if !errors.As(err, &ife) || ife.Code != CodeMcpInvalidResponse {
		t.Fatalf("err = %v, want code %q", err, CodeMcpInvalidResponse)
	}
}

func TestExposeMcp_UnregisterClearsOnTransportFailure(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case registerToolPath:
			writeRegisterResponse(w, testHMACSecret, []string{"demo.alpha"})
		case unregisterToolPath:
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"code":"unavailable","message":"down"}`))
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	handle, err := ExposeMcp(ExposeMcpConfig{
		Name:        "demo",
		Version:     "0.1.0",
		CallbackURL: testCallbackURL,
		ServerURL:   server.URL,
		APIKey:      testAPIKey,
		Tools:       []McpToolDef{{Name: "alpha", InputSchemaJSON: testSchema, Handler: noopHandler}},
	})
	if err != nil {
		t.Fatalf("ExposeMcp: %v", err)
	}
	if _, ok := lookupLocal("demo.alpha"); !ok {
		t.Fatal("entry missing before unregister")
	}

	if uErr := handle.Unregister(); uErr == nil {
		t.Fatal("expected unregister error")
	}
	// Local registry must be cleared even when the server-side call fails.
	if _, ok := lookupLocal("demo.alpha"); ok {
		t.Fatal("entry still present after unregister")
	}
}

func TestExposeMcp_UnregisterIsIdempotent(t *testing.T) {
	clearLocalForTests()
	defer clearLocalForTests()

	var registerHits, unregisterHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case registerToolPath:
			registerHits.Add(1)
			writeRegisterResponse(w, testHMACSecret, []string{"demo.beta"})
		case unregisterToolPath:
			unregisterHits.Add(1)
			_, _ = w.Write([]byte(`{"removedCount":1}`))
		}
	}))
	defer server.Close()

	handle, err := ExposeMcp(ExposeMcpConfig{
		Name:        "demo",
		Version:     "0.1.0",
		CallbackURL: testCallbackURL,
		ServerURL:   server.URL,
		APIKey:      testAPIKey,
		Tools:       []McpToolDef{{Name: "beta", InputSchemaJSON: testSchema, Handler: noopHandler}},
	})
	if err != nil {
		t.Fatalf("ExposeMcp: %v", err)
	}

	if uErr := handle.Unregister(); uErr != nil {
		t.Fatalf("first unregister: %v", uErr)
	}
	if uErr := handle.Unregister(); uErr != nil {
		t.Fatalf("second unregister: %v", uErr)
	}
	if got := registerHits.Load(); got != 1 {
		t.Errorf("register hits = %d, want 1", got)
	}
	if got := unregisterHits.Load(); got != 1 {
		t.Errorf("unregister hits = %d, want 1 (second call must be no-op)", got)
	}
}

// helpers ---------------------------------------------------------------

func noopHandler(_ any) (any, error) { return map[string]any{}, nil }

func writeRegisterResponse(w http.ResponseWriter, secret string, toolNames []string) {
	w.Header().Set("Content-Type", "application/json")
	body, _ := json.Marshal(registerToolResponseJSON{HMACSecret: secret, RegisteredToolNames: toolNames})
	_, _ = w.Write(body)
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
