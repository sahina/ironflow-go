package ironflow

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// identityPost sends a JSON POST with an optional bearer and returns the
// status and body. The login and Connect routes have no Go SDK helper.
func identityPost(t *testing.T, url, bearer string, body any) (int, []byte) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

// TestLiveIdentity proves the identity and tenancy rows (#2945) on a live
// server. It skips unless IRONFLOW_TEST_PLATFORM_TOKEN is set;
// `make test-identity-live` sets it.
func TestLiveIdentity(t *testing.T) {
	platformToken := os.Getenv("IRONFLOW_TEST_PLATFORM_TOKEN")
	if platformToken == "" {
		t.Skip("IRONFLOW_TEST_PLATFORM_TOKEN not set; run `make test-identity-live`")
	}
	server := os.Getenv("IRONFLOW_TEST_SERVER")
	operator := NewClient(ClientConfig{ServerURL: server, APIKey: os.Getenv("IRONFLOW_TEST_API_KEY")})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	// tenant-provision: the bootstrap org's admin provisions; the new key works.
	orgName := "Go Zürich " + suffix
	tenant, err := operator.Tenants().Provision(ctx, ProvisionTenantInput{OrgName: orgName})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if tenant.Org.Name != orgName || tenant.Environment.Name != "production" ||
		!strings.HasPrefix(tenant.APIKey.Key, "ifkey_") || len(tenant.APIKey.Roles) != 1 || tenant.APIKey.Roles[0] != "admin" {
		t.Fatalf("Provision result = %+v", tenant)
	}
	orgID, envID := tenant.Org.ID, tenant.Environment.ID
	tenantKey := tenant.APIKey.Key
	tenantClient := func() *Client { return NewClient(ClientConfig{ServerURL: server, APIKey: tenantKey}) }
	if got, err := tenantClient().GetOrganization(ctx, orgID); err != nil || got.Name != orgName {
		t.Fatalf("the provisioned key reads its org: %+v, %v", got, err)
	}

	t.Run("tenant-provision and tenant-list are the bootstrap org's only", func(t *testing.T) {
		// The Users and Tenants helpers return the server message, not ErrForbidden.
		if _, err := tenantClient().Tenants().Provision(ctx, ProvisionTenantInput{OrgName: "Escalate " + suffix}); err == nil || err.Error() != "insufficient permissions" {
			t.Errorf("a tenant admin provisions: err = %v, want 403", err)
		}
		if _, err := tenantClient().Tenants().List(ctx); err == nil || err.Error() != "insufficient permissions" {
			t.Errorf("a tenant admin lists tenants: err = %v, want 403", err)
		}
	})

	t.Run("tenant-list", func(t *testing.T) {
		tenants, err := operator.Tenants().List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		for _, tn := range tenants {
			if tn.ID == orgID {
				if tn.Name != orgName || tn.EnvCount != 1 || tn.KeyCount != 1 || tn.CreatedAt == "" {
					t.Errorf("tenant row = %+v, want %q with 1 env and 1 key", tn, orgName)
				}
				return
			}
		}
		t.Fatalf("tenant %s not in %+v", orgID, tenants)
	})

	t.Run("env-rotate-apikey", func(t *testing.T) {
		code, body := identityPost(t, server+"/ironflow.v1.EnvironmentService/RotateAPIKey", tenantKey, map[string]string{"id": envID})
		var rotated struct{ ID, Name, APIKey string }
		if code != http.StatusOK || json.Unmarshal(body, &rotated) != nil {
			t.Fatalf("RotateAPIKey = %d %s", code, body)
		}
		if rotated.ID != envID || rotated.Name != "production" || rotated.APIKey == tenantKey || !strings.HasPrefix(rotated.APIKey, "ifkey_") {
			t.Fatalf("RotateAPIKey = %+v", rotated)
		}
		if _, err := tenantClient().GetOrganization(ctx, orgID); !errors.Is(err, ErrUnauthorized) {
			t.Errorf("the old key after rotation: err = %v, want 401", err)
		}
		tenantKey = rotated.APIKey
		if _, err := tenantClient().GetOrganization(ctx, orgID); err != nil {
			t.Fatalf("the new key: %v", err)
		}

		// With two keys in an environment and the caller holding neither, the RPC cannot tell which to rotate.
		project := strings.TrimPrefix(orgID, "org_")
		staging, err := tenantClient().Projects().CreateEnvironment(ctx, CreateEnvironmentInput{Name: "staging", ProjectID: "proj_" + project + "_default"})
		if err != nil {
			t.Fatalf("CreateEnvironment: %v", err)
		}
		for _, name := range []string{"k1", "k2"} {
			if _, err := tenantClient().CreateAPIKey(ctx, CreateAPIKeyInput{Name: name, EnvID: staging.ID}); err != nil {
				t.Fatalf("CreateAPIKey: %v", err)
			}
		}
		code, body = identityPost(t, server+"/ironflow.v1.EnvironmentService/RotateAPIKey", tenantKey, map[string]string{"id": staging.ID})
		if code != http.StatusBadRequest || !strings.Contains(string(body), "failed_precondition") {
			t.Errorf("two keys: %d %s, want failed_precondition", code, body)
		}
	})

	t.Run("org-export", func(t *testing.T) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server+"/api/v1/export", nil)
		req.Header.Set("Authorization", "Bearer "+tenantKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("export: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/x-ndjson" {
			t.Fatalf("export = %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
		}
		raw, _ := io.ReadAll(resp.Body)
		var lines []map[string]any
		sc := bufio.NewScanner(bytes.NewReader(raw))
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var line map[string]any
			if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
				t.Fatalf("not NDJSON: %q", sc.Text())
			}
			lines = append(lines, line)
		}
		if len(lines) < 3 || lines[0]["type"] != "manifest" || lines[0]["org_id"] != orgID {
			t.Fatalf("manifest = %v", lines)
		}
		footer := lines[len(lines)-1]
		if footer["type"] != "footer" {
			t.Fatalf("the stream did not end with a footer: %v", footer)
		}
		// body_sha256 covers every byte before the footer line.
		body := raw[:bytes.LastIndexByte(bytes.TrimRight(raw, "\n"), '\n')+1]
		if sum := sha256.Sum256(body); hex.EncodeToString(sum[:]) != footer["body_sha256"] {
			t.Errorf("body_sha256 = %v, want %x", footer["body_sha256"], sum)
		}
		counts := map[string]float64{}
		envs := map[string]bool{}
		for _, l := range lines[1 : len(lines)-1] {
			if l["type"] != "record" {
				continue
			}
			entity, _ := l["entity"].(string)
			counts[entity+"s"]++
			data, _ := l["data"].(map[string]any)
			switch entity {
			case "org":
				if data["id"] != orgID || data["name"] != orgName {
					t.Errorf("org record = %v", data)
				}
			case "environment":
				envs[data["id"].(string)] = true
			}
		}
		if !envs[envID] {
			t.Errorf("environment records %v miss %s", envs, envID)
		}
		want, _ := footer["counts"].(map[string]any)
		if want["org"] != float64(1) || counts["orgs"] != 1 || want["environments"] != counts["environments"] || want["projects"] != counts["projects"] || want["audit_events"] != counts["audit_events"] {
			t.Errorf("footer counts %v, records %v", want, counts)
		}
	})

	t.Run("user-crud", func(t *testing.T) {
		users := tenantClient().Users()
		email := "go-user-" + suffix + "@example.com"
		u, err := users.Create(ctx, CreateUserInput{Email: email, Name: "Zoë Go", Password: "first-pass-123", Roles: []string{"viewer"}})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if u.OrgID != orgID || u.Email != email || u.Name != "Zoë Go" || len(u.Roles) != 1 || u.Roles[0] != "viewer" {
			t.Fatalf("Create = %+v", u)
		}
		list, err := users.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		found := false
		for _, x := range list {
			found = found || x.ID == u.ID
		}
		if !found {
			t.Errorf("List %+v misses %s", list, u.ID)
		}
		name := "Zoë Renamed"
		if got, err := users.Update(ctx, u.ID, UpdateUserInput{Name: &name, Roles: []string{"developer"}}); err != nil || got.Name != name || len(got.Roles) != 1 || got.Roles[0] != "developer" {
			t.Fatalf("Update = %+v, %v", got, err)
		}
		if got, err := users.Get(ctx, u.ID); err != nil || got.Name != name || got.Roles[0] != "developer" {
			t.Fatalf("Get after Update = %+v, %v", got, err)
		}
		if err := users.Delete(ctx, u.ID); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if _, err := users.Get(ctx, u.ID); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Errorf("Get after Delete: err = %v, want not found", err)
		}
	})

	t.Run("user-password", func(t *testing.T) {
		email := "go-pw-" + suffix + "@example.com"
		u, err := tenantClient().Users().Create(ctx, CreateUserInput{Email: email, Password: "old-pass-123", Roles: []string{"developer"}})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		login := func(pw string) (int, string) {
			code, body := identityPost(t, server+"/api/v1/auth/login", "", map[string]string{"email": email, "password": pw})
			var out struct{ Token string }
			_ = json.Unmarshal(body, &out)
			return code, out.Token
		}
		code, session := login("old-pass-123")
		if code != http.StatusOK || session == "" {
			t.Fatalf("login = %d", code)
		}
		// A session defaults to env_default, which belongs to org_default, so name the user's environment.
		self := NewClient(ClientConfig{ServerURL: server, APIKey: session, Environment: envID})
		if err := self.Users().ChangePassword(ctx, u.ID, ChangePasswordInput{CurrentPassword: "wrong", NewPassword: "x-pass-789"}); err == nil || err.Error() != "incorrect current password" {
			t.Errorf("wrong current password: err = %v, want 401", err)
		}
		if err := self.Users().ChangePassword(ctx, u.ID, ChangePasswordInput{CurrentPassword: "old-pass-123", NewPassword: "new-pass-456"}); err != nil {
			t.Fatalf("a developer changes their own password: %v", err)
		}
		if code, _ := login("old-pass-123"); code != http.StatusUnauthorized {
			t.Errorf("old password after change = %d, want 401", code)
		}
		if code, _ := login("new-pass-456"); code != http.StatusOK {
			t.Errorf("new password after change = %d, want 200", code)
		}
	})

	t.Run("org-crud", func(t *testing.T) {
		platform := NewClient(ClientConfig{ServerURL: server, APIKey: platformToken})
		created, err := platform.CreateOrganization(ctx, CreateOrgInput{Name: "Go Plattform Ünïcode " + suffix})
		if err != nil {
			t.Fatalf("CreateOrganization with a platform credential: %v", err)
		}
		if !strings.HasPrefix(created.ID, "org_") || created.Name != "Go Plattform Ünïcode "+suffix || created.CreatedAt.IsZero() {
			t.Fatalf("CreateOrganization = %+v", created)
		}
		if _, err := tenantClient().CreateOrganization(ctx, CreateOrgInput{Name: "Escalate"}); !errors.Is(err, ErrForbidden) {
			t.Errorf("a tenant creates an org: err = %v, want 403", err)
		}

		self := tenantClient()
		orgs, err := self.ListOrganizations(ctx)
		if err != nil || len(orgs) != 1 || orgs[0].ID != orgID {
			t.Fatalf("a tenant lists only its own org: %+v, %v", orgs, err)
		}
		renamed := "Go Zürich renamed " + suffix
		if got, err := self.UpdateOrganization(ctx, orgID, UpdateOrgInput{Name: renamed}); err != nil || got.Name != renamed || !got.UpdatedAt.After(got.CreatedAt) {
			t.Fatalf("UpdateOrganization = %+v, %v", got, err)
		}
		if got, err := self.GetOrganization(ctx, orgID); err != nil || got.Name != renamed {
			t.Fatalf("GetOrganization after update = %+v, %v", got, err)
		}
		if err := self.DeleteOrganization(ctx, orgID); err != nil {
			t.Fatalf("DeleteOrganization: %v", err)
		}
		// Deleting the org deletes its keys.
		if _, err := self.GetOrganization(ctx, orgID); !errors.Is(err, ErrUnauthorized) {
			t.Errorf("the key after its org is deleted: err = %v, want 401", err)
		}
	})
}
