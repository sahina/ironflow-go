package ironflow

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestLiveIdentityAuthSessions covers auth-dashboard-session (#3003): login,
// validate and desktop-session. No SDK wraps these routes (the dashboard and
// Ironflow Desktop call them), so the test speaks raw HTTP.
func TestLiveIdentityAuthSessions(t *testing.T) {
	tn, ctx, suffix := liveIdentityTenant(t, "sessions")
	admin := tn.client()

	call := func(method, path, bearer string, env string, body any) (int, map[string]any) {
		t.Helper()
		var rd io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rd = bytes.NewReader(b)
		}
		req, _ := http.NewRequestWithContext(ctx, method, tn.server+path, rd)
		req.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		if env != "" {
			req.Header.Set("X-Ironflow-Environment", env)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(resp.Body)
		var out map[string]any
		_ = json.Unmarshal(raw, &out)
		return resp.StatusCode, out
	}
	login := func(email, password string) (int, string) {
		code, out := call(http.MethodPost, "/api/v1/auth/login", "", "", map[string]string{"email": email, "password": password})
		tok, _ := out["token"].(string)
		return code, tok
	}
	validate := func(token string) (int, map[string]any) {
		return call(http.MethodGet, "/api/v1/auth/validate", token, "", nil)
	}

	adminEmail, viewerEmail := "go-sess-admin-"+suffix+"@example.com", "go-sess-viewer-"+suffix+"@example.com"
	adminUser, err := admin.Users().Create(ctx, CreateUserInput{Email: adminEmail, Password: "admin-pass-123", Roles: []string{"admin"}})
	if err != nil {
		t.Fatalf("Create admin user: %v", err)
	}
	viewerUser, err := admin.Users().Create(ctx, CreateUserInput{Email: viewerEmail, Password: "viewer-pass-123", Roles: []string{"viewer"}})
	if err != nil {
		t.Fatalf("Create viewer user: %v", err)
	}

	var adminToken, viewerToken string
	t.Run("login", func(t *testing.T) {
		code, tok := login(adminEmail, "admin-pass-123")
		if code != http.StatusOK || strings.Count(tok, ".") != 2 {
			t.Fatalf("login = %d %q, want 200 and a JWT", code, tok)
		}
		adminToken = tok
		_, viewerToken = login(viewerEmail, "viewer-pass-123")
		if code, tok := login(adminEmail, "wrong"); code != http.StatusUnauthorized || tok != "" {
			t.Errorf("a wrong password = %d %q, want 401 and no token", code, tok)
		}
		if code, _ := login("nobody-"+suffix+"@example.com", "x"); code != http.StatusUnauthorized {
			t.Errorf("an unknown email = %d, want 401", code)
		}
		if code, _ := call(http.MethodPost, "/api/v1/auth/login", "", "", "not an object"); code != http.StatusBadRequest {
			t.Errorf("a malformed body = %d, want 400", code)
		}
	})

	t.Run("validate", func(t *testing.T) {
		code, out := validate(adminToken)
		if code != http.StatusOK || out["valid"] != true || out["user_id"] != adminUser.ID || out["org_id"] != tn.orgID {
			t.Fatalf("validate admin = %d %v", code, out)
		}
		want := map[string]bool{"files:read": true, "files:write": true, "files:manage": true}
		if got, _ := out["actions"].(map[string]any); len(got) != 3 || got["files:read"] != want["files:read"] || got["files:write"] != want["files:write"] || got["files:manage"] != want["files:manage"] {
			t.Errorf("admin actions = %v, want %v", out["actions"], want)
		}
		// A viewer reads files and nothing more; the map follows the user's roles.
		code, out = validate(viewerToken)
		if got, _ := out["actions"].(map[string]any); code != http.StatusOK || out["user_id"] != viewerUser.ID ||
			got["files:read"] != true || got["files:write"] != false || got["files:manage"] != false {
			t.Errorf("validate viewer = %d %v, want read only", code, out)
		}
		// The cookie carries the token the same way the bearer does.
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, tn.server+"/api/v1/auth/validate", nil)
		req.AddCookie(&http.Cookie{Name: "ironflow_dashboard_token", Value: adminToken})
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("validate with the cookie = %d, want 200", resp.StatusCode)
		}
		if code, _ := validate(""); code != http.StatusUnauthorized {
			t.Errorf("validate without a token = %d, want 401", code)
		}
		if code, _ := validate("not.a.jwt"); code != http.StatusUnauthorized {
			t.Errorf("validate with a bad token = %d, want 401", code)
		}
		// The account is checked on every call: a deleted user is logged out.
		if err := admin.Users().Delete(ctx, viewerUser.ID); err != nil {
			t.Fatalf("Delete viewer: %v", err)
		}
		if code, _ := validate(viewerToken); code != http.StatusUnauthorized {
			t.Errorf("validate after the user was deleted = %d, want 401", code)
		}
	})

	t.Run("desktop-session", func(t *testing.T) {
		key, err := admin.CreateAPIKey(ctx, CreateAPIKeyInput{Name: "desktop-" + suffix})
		if err != nil {
			t.Fatalf("CreateAPIKey: %v", err)
		}
		code, out := call(http.MethodPost, "/api/v1/auth/desktop-session", key.Key, "", nil)
		session, _ := out["token"].(string)
		if code != http.StatusOK || strings.Count(session, ".") != 2 {
			t.Fatalf("desktop-session = %d %v, want 200 and a JWT", code, out)
		}
		// The JWT carries the key's org, roles and environment.
		if code, out := validate(session); code != http.StatusOK || out["valid"] != true || out["org_id"] != tn.orgID {
			t.Errorf("validate the session = %d %v", code, out)
		}
		if code, _ := call(http.MethodGet, "/api/v1/roles", session, "", nil); code != http.StatusOK {
			t.Errorf("GET /roles with the session = %d, want 200 in the key's environment", code)
		}
		// The environment boundary of the key holds for the session.
		if code, _ := call(http.MethodGet, "/api/v1/roles", session, "env_default", nil); code != http.StatusForbidden {
			t.Errorf("GET /roles with the session in another environment = %d, want 403", code)
		}
		// "current" means the session's own environment (#3067). The tenant's
		// environment is not env_default, so a 403 here would mean it fell back.
		if code, _ := call(http.MethodGet, "/api/v1/roles", session, "current", nil); code != http.StatusOK {
			t.Errorf("GET /roles with the session and environment current = %d, want 200", code)
		}
		if code, _ := call(http.MethodGet, "/api/v1/roles?env=current", session, "", nil); code != http.StatusOK {
			t.Errorf("GET /roles?env=current with the session = %d, want 200", code)
		}
		// Any other unknown name still fails closed.
		if code, _ := call(http.MethodGet, "/api/v1/roles", session, "no-such-environment", nil); code != http.StatusForbidden {
			t.Errorf("GET /roles with the session and an unknown name = %d, want 403", code)
		}
		// A session is minted from an API key, not exchanged from another token.
		if code, _ := call(http.MethodPost, "/api/v1/auth/desktop-session", adminToken, tn.envID, nil); code != http.StatusUnauthorized {
			t.Errorf("desktop-session with a dashboard JWT = %d, want 401", code)
		}
		if code, _ := call(http.MethodPost, "/api/v1/auth/desktop-session", "", "", nil); code != http.StatusUnauthorized {
			t.Errorf("desktop-session without a credential = %d, want 401", code)
		}
		// The session stops when its source key is deleted.
		if err := admin.DeleteAPIKey(ctx, key.ID); err != nil {
			t.Fatalf("DeleteAPIKey: %v", err)
		}
		if code, _ := validate(session); code != http.StatusUnauthorized {
			t.Errorf("validate the session after its key was deleted = %d, want 401", code)
		}
	})
}
