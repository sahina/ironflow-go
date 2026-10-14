package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newAuthTestClient(server *httptest.Server) *Client {
	return &Client{
		serverURL:  server.URL,
		httpClient: &http.Client{},
		retryConfig: &ClientRetryConfig{
			MaxAttempts: 1,
		},
		logger: NewNoopLogger(),
	}
}

// ============================================================================
// API Key Tests
// ============================================================================

func TestCreateAPIKey(t *testing.T) {
	t.Run("sends correct request and parses response", func(t *testing.T) {
		var receivedMethod, receivedPath string
		var receivedBody map[string]any

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &receivedBody)

			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"ak_123","name":"test-key","key_prefix":"ifk_ab","key":"ifk_abc123","role_ids":["role_admin"],"created_at":"2024-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.CreateAPIKey(context.Background(), CreateAPIKeyInput{
			Name:    "test-key",
			RoleIDs: []string{"role_admin"},
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "POST" {
			t.Errorf("expected POST, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/apikeys" {
			t.Errorf("expected /api/v1/apikeys, got %s", receivedPath)
		}
		if receivedBody["name"] != "test-key" {
			t.Errorf("expected name 'test-key', got %v", receivedBody["name"])
		}
		if result.ID != "ak_123" {
			t.Errorf("expected ID 'ak_123', got %s", result.ID)
		}
		if result.Key != "ifk_abc123" {
			t.Errorf("expected Key 'ifk_abc123', got %s", result.Key)
		}
		if len(result.RoleIDs) != 1 || result.RoleIDs[0] != "role_admin" {
			t.Errorf("expected RoleIDs ['role_admin'], got %v", result.RoleIDs)
		}
	})
}

func TestListAPIKeys(t *testing.T) {
	t.Run("sends GET and parses array response", func(t *testing.T) {
		var receivedMethod, receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":"ak_1","name":"key-1","key_prefix":"ifk_aa","created_at":"2024-01-01T00:00:00Z"},{"id":"ak_2","name":"key-2","key_prefix":"ifk_bb","created_at":"2024-01-01T00:00:00Z"}]`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.ListAPIKeys(context.Background())

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "GET" {
			t.Errorf("expected GET, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/apikeys" {
			t.Errorf("expected /api/v1/apikeys, got %s", receivedPath)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 keys, got %d", len(result))
		}
		if result[0].ID != "ak_1" {
			t.Errorf("expected first key ID 'ak_1', got %s", result[0].ID)
		}
	})
}

func TestGetAPIKeyByID(t *testing.T) {
	t.Run("sends GET with ID in path", func(t *testing.T) {
		var receivedMethod, receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"ak_123","name":"my-key","key_prefix":"ifk_ab","created_at":"2024-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.GetAPIKey(context.Background(), "ak_123")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "GET" {
			t.Errorf("expected GET, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/apikeys/ak_123" {
			t.Errorf("expected /api/v1/apikeys/ak_123, got %s", receivedPath)
		}
		if result.Name != "my-key" {
			t.Errorf("expected name 'my-key', got %s", result.Name)
		}
	})
}

func TestDeleteAPIKey(t *testing.T) {
	t.Run("sends DELETE with ID in path", func(t *testing.T) {
		var receivedMethod, receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		err := client.DeleteAPIKey(context.Background(), "ak_123")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "DELETE" {
			t.Errorf("expected DELETE, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/apikeys/ak_123" {
			t.Errorf("expected /api/v1/apikeys/ak_123, got %s", receivedPath)
		}
	})
}

func TestRotateAPIKey(t *testing.T) {
	t.Run("sends POST to rotate endpoint", func(t *testing.T) {
		var receivedMethod, receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"ak_new","name":"my-key","key_prefix":"ifk_cd","key":"ifk_newkey","created_at":"2024-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.RotateAPIKey(context.Background(), "ak_old")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "POST" {
			t.Errorf("expected POST, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/apikeys/ak_old/rotate" {
			t.Errorf("expected /api/v1/apikeys/ak_old/rotate, got %s", receivedPath)
		}
		if result.ID != "ak_new" {
			t.Errorf("expected ID 'ak_new', got %s", result.ID)
		}
		if result.Key != "ifk_newkey" {
			t.Errorf("expected Key 'ifk_newkey', got %s", result.Key)
		}
	})
}

// ============================================================================
// Organization Tests
// ============================================================================

func TestCreateOrganization(t *testing.T) {
	t.Run("sends correct request and parses response", func(t *testing.T) {
		var receivedMethod, receivedPath string
		var receivedBody map[string]any

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &receivedBody)

			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"org_abc","name":"My Org","created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.CreateOrganization(context.Background(), CreateOrgInput{Name: "My Org"})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "POST" {
			t.Errorf("expected POST, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/orgs" {
			t.Errorf("expected /api/v1/orgs, got %s", receivedPath)
		}
		if receivedBody["name"] != "My Org" {
			t.Errorf("expected name 'My Org', got %v", receivedBody["name"])
		}
		if result.ID != "org_abc" {
			t.Errorf("expected ID 'org_abc', got %s", result.ID)
		}
		if result.Name != "My Org" {
			t.Errorf("expected name 'My Org', got %s", result.Name)
		}
	})
}

func TestListOrganizations(t *testing.T) {
	t.Run("sends GET and parses array response", func(t *testing.T) {
		var receivedMethod string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":"org_1","name":"Org 1","created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}]`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.ListOrganizations(context.Background())

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "GET" {
			t.Errorf("expected GET, got %s", receivedMethod)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 org, got %d", len(result))
		}
		if result[0].ID != "org_1" {
			t.Errorf("expected ID 'org_1', got %s", result[0].ID)
		}
	})
}

func TestGetOrganization(t *testing.T) {
	t.Run("sends GET with ID in path", func(t *testing.T) {
		var receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"org_abc","name":"My Org","created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.GetOrganization(context.Background(), "org_abc")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedPath != "/api/v1/orgs/org_abc" {
			t.Errorf("expected /api/v1/orgs/org_abc, got %s", receivedPath)
		}
		if result.Name != "My Org" {
			t.Errorf("expected name 'My Org', got %s", result.Name)
		}
	})
}

func TestUpdateOrganization(t *testing.T) {
	t.Run("sends PATCH with body", func(t *testing.T) {
		var receivedMethod, receivedPath string
		var receivedBody map[string]any

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &receivedBody)

			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"org_abc","name":"Updated Org","created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-02T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.UpdateOrganization(context.Background(), "org_abc", UpdateOrgInput{Name: "Updated Org"})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "PATCH" {
			t.Errorf("expected PATCH, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/orgs/org_abc" {
			t.Errorf("expected /api/v1/orgs/org_abc, got %s", receivedPath)
		}
		if receivedBody["name"] != "Updated Org" {
			t.Errorf("expected name 'Updated Org', got %v", receivedBody["name"])
		}
		if result.Name != "Updated Org" {
			t.Errorf("expected name 'Updated Org', got %s", result.Name)
		}
	})
}

func TestDeleteOrganization(t *testing.T) {
	t.Run("sends DELETE with ID in path", func(t *testing.T) {
		var receivedMethod, receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		err := client.DeleteOrganization(context.Background(), "org_abc")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "DELETE" {
			t.Errorf("expected DELETE, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/orgs/org_abc" {
			t.Errorf("expected /api/v1/orgs/org_abc, got %s", receivedPath)
		}
	})
}

// ============================================================================
// Role Tests
// ============================================================================

func TestCreateRole(t *testing.T) {
	t.Run("sends correct request and parses response", func(t *testing.T) {
		var receivedMethod, receivedPath string
		var receivedBody map[string]any

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &receivedBody)

			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"role_abc","org_id":"org_default","name":"deployer","is_default":false,"created_at":"2024-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.CreateRole(context.Background(), CreateRoleInput{Name: "deployer"})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "POST" {
			t.Errorf("expected POST, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/roles" {
			t.Errorf("expected /api/v1/roles, got %s", receivedPath)
		}
		if receivedBody["name"] != "deployer" {
			t.Errorf("expected name 'deployer', got %v", receivedBody["name"])
		}
		if result.ID != "role_abc" {
			t.Errorf("expected ID 'role_abc', got %s", result.ID)
		}
		if result.OrgID != "org_default" {
			t.Errorf("expected OrgID 'org_default', got %s", result.OrgID)
		}
	})
}

func TestListRoles(t *testing.T) {
	t.Run("sends GET and parses array response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":"role_admin","org_id":"org_default","name":"admin","is_default":true,"created_at":"2024-01-01T00:00:00Z"}]`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.ListRoles(context.Background())

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 role, got %d", len(result))
		}
		if result[0].Name != "admin" {
			t.Errorf("expected name 'admin', got %s", result[0].Name)
		}
		if !result[0].IsDefault {
			t.Errorf("expected IsDefault true")
		}
	})
}

func TestGetRole(t *testing.T) {
	t.Run("sends GET with ID in path", func(t *testing.T) {
		var receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"role_abc","org_id":"org_default","name":"deployer","is_default":false,"created_at":"2024-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.GetRole(context.Background(), "role_abc")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedPath != "/api/v1/roles/role_abc" {
			t.Errorf("expected /api/v1/roles/role_abc, got %s", receivedPath)
		}
		if result.Name != "deployer" {
			t.Errorf("expected name 'deployer', got %s", result.Name)
		}
	})
}

func TestUpdateRole(t *testing.T) {
	t.Run("sends PATCH with body", func(t *testing.T) {
		var receivedMethod, receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"role_abc","org_id":"org_default","name":"new-name","is_default":false,"created_at":"2024-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.UpdateRole(context.Background(), "role_abc", UpdateRoleInput{Name: "new-name"})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "PATCH" {
			t.Errorf("expected PATCH, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/roles/role_abc" {
			t.Errorf("expected /api/v1/roles/role_abc, got %s", receivedPath)
		}
		if result.Name != "new-name" {
			t.Errorf("expected name 'new-name', got %s", result.Name)
		}
	})
}

func TestDeleteRole(t *testing.T) {
	t.Run("sends DELETE with ID in path", func(t *testing.T) {
		var receivedMethod, receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		err := client.DeleteRole(context.Background(), "role_abc")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "DELETE" {
			t.Errorf("expected DELETE, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/roles/role_abc" {
			t.Errorf("expected /api/v1/roles/role_abc, got %s", receivedPath)
		}
	})
}

// ============================================================================
// Error Handling Tests
// ============================================================================

func TestAuthMethodsErrorHandling(t *testing.T) {
	t.Run("returns error on 404", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"NOT_FOUND","message":"API key not found"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		_, err := client.GetAPIKey(context.Background(), "nonexistent")

		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "NOT_FOUND") {
			t.Errorf("expected NOT_FOUND in error, got: %s", err.Error())
		}
	})

	t.Run("returns error on 403", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":"FORBIDDEN","message":"access denied"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		_, err := client.ListRoles(context.Background())

		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "FORBIDDEN") {
			t.Errorf("expected FORBIDDEN in error, got: %s", err.Error())
		}
	})

	t.Run("sends authorization header when API key is set", func(t *testing.T) {
		var receivedAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedAuth = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}))
		defer server.Close()

		client := &Client{
			serverURL:  server.URL,
			apiKey:     "ifk_test_key",
			httpClient: &http.Client{},
			retryConfig: &ClientRetryConfig{
				MaxAttempts: 1,
			},
			logger: NewNoopLogger(),
		}

		_, _ = client.ListRoles(context.Background())

		if receivedAuth != "Bearer ifk_test_key" {
			t.Errorf("expected 'Bearer ifk_test_key', got %q", receivedAuth)
		}
	})

	t.Run("GET requests have no Content-Type header", func(t *testing.T) {
		var receivedContentType string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedContentType = r.Header.Get("Content-Type")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		_, _ = client.ListAPIKeys(context.Background())

		if receivedContentType != "" {
			t.Errorf("expected no Content-Type for GET, got %q", receivedContentType)
		}
	})

	t.Run("POST requests have Content-Type application/json", func(t *testing.T) {
		var receivedContentType string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedContentType = r.Header.Get("Content-Type")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"org_1","name":"test","created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		_, _ = client.CreateOrganization(context.Background(), CreateOrgInput{Name: "test"})

		if receivedContentType != "application/json" {
			t.Errorf("expected Content-Type 'application/json' for POST, got %q", receivedContentType)
		}
	})
}

func TestAuthErrorIs_Unauthorized(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"UNAUTHORIZED","message":"invalid api key"}`))
	}))
	defer server.Close()

	client := newAuthTestClient(server)
	_, err := client.ListAPIKeys(context.Background())

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrUnauthorized) {
		t.Errorf("expected errors.Is(err, ErrUnauthorized), got: %v", err)
	}
}

func TestAuthErrorIs_EnterpriseLicenseRequired(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = w.Write([]byte(`{"code":"ENTERPRISE_REQUIRED","message":"enterprise license required"}`))
	}))
	defer server.Close()

	client := newAuthTestClient(server)
	_, err := client.ListOrganizations(context.Background())

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrEnterpriseLicenseRequired) {
		t.Errorf("expected errors.Is(err, ErrEnterpriseLicenseRequired), got: %v", err)
	}
}

func TestAuthErrorIs_Forbidden(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"FORBIDDEN","message":"access denied"}`))
	}))
	defer server.Close()

	client := newAuthTestClient(server)
	_, err := client.ListRoles(context.Background())

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrForbidden) {
		t.Errorf("expected errors.Is(err, ErrForbidden), got: %v", err)
	}
}

func TestPolicySet_K01_K02_GoSDK(t *testing.T) {
	var gotMethod, gotPath, gotIfMatch string
	var gotBody map[string]any
	status := 200
	hits := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		gotMethod, gotPath, gotIfMatch = r.Method, r.URL.Path, r.Header.Get("If-Match")
		gotBody = nil
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"version":2,"modules":{"a.rego":"package p\n"},"mode":"monitor","enforcement_active":true,"versions":[{"version":2,"mode":"monitor","created_at":"x"}],"decision":"deny","reasons_omitted":3}`))
	}))
	defer ts.Close()
	c := newAuthTestClient(ts)
	ctx := context.Background()

	if ps, err := c.GetPolicySet(ctx); err != nil || gotMethod != "GET" || gotPath != "/api/v1/policy-set" || ps.Version != 2 || ps.EnforcementActive == nil || !*ps.EnforcementActive {
		t.Fatalf("get: %v %s %s %+v", err, gotMethod, gotPath, ps)
	}
	if _, err := c.PutPolicySet(ctx, PolicySetInput{Modules: map[string]string{"a.rego": "package p\n"}, Mode: "enforce"}, 1); err != nil || gotMethod != "PUT" || gotIfMatch != "1" || gotBody["mode"] != "enforce" {
		t.Fatalf("put: %v %s %q %v", err, gotMethod, gotIfMatch, gotBody)
	}
	if err := c.ValidatePolicySet(ctx, PolicySetInput{Modules: map[string]string{}}); err != nil || gotPath != "/api/v1/policy-set/validate" {
		t.Fatalf("validate: %v %s", err, gotPath)
	}
	if dr, err := c.DryRunPolicySet(ctx, PolicySetDryRunInput{Input: map[string]any{"request": map[string]any{}, "subject": map[string]any{}}}); err != nil || gotPath != "/api/v1/policy-set/dry-run" || dr.ReasonsOmitted != 3 {
		t.Fatalf("dry-run: %v %s %+v", err, gotPath, dr)
	}
	if vs, err := c.ListPolicySetVersions(ctx); err != nil || gotPath != "/api/v1/policy-set/versions" || len(vs) != 1 {
		t.Fatalf("versions: %v %s %v", err, gotPath, vs)
	}
	if _, err := c.GetPolicySetVersion(ctx, 1); err != nil || gotPath != "/api/v1/policy-set/versions/1" {
		t.Fatalf("version: %v %s", err, gotPath)
	}
	if _, err := c.RollbackPolicySet(ctx, 1, 2); err != nil || gotPath != "/api/v1/policy-set/versions/1/rollback" || gotIfMatch != "2" {
		t.Fatalf("rollback: %v %s %q", err, gotPath, gotIfMatch)
	}

	for _, st := range []int{412, 428} { // K02
		status = st
		_, err := c.PutPolicySet(ctx, PolicySetInput{}, 0)
		want := ErrPreconditionFailed
		if st == 428 {
			want = ErrPreconditionRequired
		}
		if !errors.Is(err, want) {
			t.Fatalf("status %d: err = %v", st, err)
		}
		if IsVersionConflict(err) != (st == 412) {
			t.Fatalf("IsVersionConflict(%d) wrong", st)
		}
	}
}

// The write committed: a resend of the same PUT would fail with 412, so a 5xx is never retried.
func TestPolicySet_NotPropagatedSurfacesItsCodeAndIsNotResent(t *testing.T) {
	hits := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"saved as version 3 but not yet active on all nodes","code":"policy_set_not_propagated","retryable":false}`))
	}))
	defer ts.Close()
	c := newAuthTestClient(ts)
	c.retryConfig = &ClientRetryConfig{MaxAttempts: 3}

	_, err := c.PutPolicySet(context.Background(), PolicySetInput{}, 2)
	var ife *IronflowError
	if !errors.As(err, &ife) || ife.Code != PolicySetErrNotPropagated || !strings.Contains(ife.Message, "version 3") || hits != 1 || IsRetryable(err) {
		t.Fatalf("err = %v, code = %v, hits = %d", err, ife, hits)
	}
}

// The strings are the server's wire contract (internal/auth/policyset/errors.go and internal/server/policy_set_*.go).
func TestPolicySet_ErrorCodeConstantsMatchServer(t *testing.T) {
	for got, want := range map[string]string{
		PolicySetErrCompileError:      "policy_set_compile_error",
		PolicySetErrV0Syntax:          "policy_set_v0_syntax",
		PolicySetErrReservedNamespace: "policy_set_reserved_namespace",
		PolicySetErrBuiltinForbidden:  "policy_set_builtin_forbidden",
		PolicySetErrSchemaError:       "policy_set_schema_error",
		PolicySetErrEntrypointType:    "policy_set_entrypoint_type",
		PolicySetErrBadFileName:       "policy_set_bad_file_name",
		PolicySetErrTooLarge:          "policy_set_too_large",
		PolicySetErrCompileTimeout:    "policy_set_compile_timeout",
		PolicySetErrBadMode:           "policy_set_bad_mode",
		PolicySetErrBadDecision:       "policy_set_bad_decision",
		PolicySetErrSprintfFormat:     "policy_set_sprintf_format",
		PolicySetErrBadRequest:        "policy_set_bad_request",
		PolicySetErrForbidden:         "policy_set_forbidden",
		PolicySetErrNotFound:          "policy_set_not_found",
		PolicySetErrVersionConflict:   "policy_set_version_conflict",
		PolicySetErrIfMatchRequired:   "policy_set_precondition_required",
		PolicySetErrSelfLockout:       "policy_set_self_lockout",
		PolicySetErrRateLimited:       "policy_set_rate_limited",
		PolicySetErrBusy:              "policy_set_busy",
		PolicySetErrNotPropagated:     "policy_set_not_propagated",
	} {
		if got != want {
			t.Errorf("constant = %q, want %q", got, want)
		}
	}
}
