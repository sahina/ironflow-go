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

func TestAssignPolicyToRole(t *testing.T) {
	t.Run("sends POST with policy_id in body", func(t *testing.T) {
		var receivedMethod, receivedPath string
		var receivedBody map[string]any

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &receivedBody)

			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		err := client.AssignPolicyToRole(context.Background(), "role_abc", "pol_xyz")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "POST" {
			t.Errorf("expected POST, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/roles/role_abc/policies" {
			t.Errorf("expected /api/v1/roles/role_abc/policies, got %s", receivedPath)
		}
		if receivedBody["policy_id"] != "pol_xyz" {
			t.Errorf("expected policy_id 'pol_xyz', got %v", receivedBody["policy_id"])
		}
	})
}

func TestRemovePolicyFromRole(t *testing.T) {
	t.Run("sends DELETE with role and policy IDs in path", func(t *testing.T) {
		var receivedMethod, receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		err := client.RemovePolicyFromRole(context.Background(), "role_abc", "pol_xyz")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "DELETE" {
			t.Errorf("expected DELETE, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/roles/role_abc/policies/pol_xyz" {
			t.Errorf("expected /api/v1/roles/role_abc/policies/pol_xyz, got %s", receivedPath)
		}
	})
}

// ============================================================================
// Policy Tests
// ============================================================================

func TestCreatePolicy(t *testing.T) {
	t.Run("sends correct request and parses response", func(t *testing.T) {
		var receivedMethod, receivedPath string
		var receivedBody map[string]any

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &receivedBody)

			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"pol_abc","org_id":"org_default","name":"allow-emit","effect":"deny","actions":"emit","resources":"irn:*","created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.CreatePolicy(context.Background(), CreatePolicyInput{
			Name:      "allow-emit",
			Effect:    "deny",
			Actions:   "emit",
			Resources: "irn:*",
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "POST" {
			t.Errorf("expected POST, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/policies" {
			t.Errorf("expected /api/v1/policies, got %s", receivedPath)
		}
		if receivedBody["name"] != "allow-emit" {
			t.Errorf("expected name 'allow-emit', got %v", receivedBody["name"])
		}
		if receivedBody["effect"] != "deny" {
			t.Errorf("expected effect 'deny', got %v", receivedBody["effect"])
		}
		if result.ID != "pol_abc" {
			t.Errorf("expected ID 'pol_abc', got %s", result.ID)
		}
		if result.Effect != "deny" {
			t.Errorf("expected effect 'deny', got %s", result.Effect)
		}
	})
}

func TestListPolicies(t *testing.T) {
	t.Run("sends GET and parses array response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":"pol_1","org_id":"org_default","name":"pol-1","effect":"deny","actions":"*","resources":"irn:*","created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}]`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.ListPolicies(context.Background())

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 policy, got %d", len(result))
		}
		if result[0].Name != "pol-1" {
			t.Errorf("expected name 'pol-1', got %s", result[0].Name)
		}
	})
}

func TestGetPolicy(t *testing.T) {
	t.Run("sends GET with ID in path", func(t *testing.T) {
		var receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"pol_abc","org_id":"org_default","name":"my-policy","effect":"deny","actions":"delete","resources":"irn:*","condition":"request.time.getHours() < 9","created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-01T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.GetPolicy(context.Background(), "pol_abc")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedPath != "/api/v1/policies/pol_abc" {
			t.Errorf("expected /api/v1/policies/pol_abc, got %s", receivedPath)
		}
		if result.Effect != "deny" {
			t.Errorf("expected effect 'deny', got %s", result.Effect)
		}
		if result.Condition != "request.time.getHours() < 9" {
			t.Errorf("expected condition, got %s", result.Condition)
		}
	})
}

func TestUpdatePolicy(t *testing.T) {
	t.Run("sends PATCH with body", func(t *testing.T) {
		var receivedMethod, receivedPath string
		var receivedBody map[string]any

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &receivedBody)

			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"pol_abc","org_id":"org_default","name":"updated-policy","effect":"deny","actions":"*","resources":"irn:*","created_at":"2024-01-01T00:00:00Z","updated_at":"2024-01-02T00:00:00Z"}`))
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		result, err := client.UpdatePolicy(context.Background(), "pol_abc", UpdatePolicyInput{
			Name:   "updated-policy",
			Effect: "deny",
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "PATCH" {
			t.Errorf("expected PATCH, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/policies/pol_abc" {
			t.Errorf("expected /api/v1/policies/pol_abc, got %s", receivedPath)
		}
		if receivedBody["name"] != "updated-policy" {
			t.Errorf("expected name 'updated-policy', got %v", receivedBody["name"])
		}
		if result.Name != "updated-policy" {
			t.Errorf("expected name 'updated-policy', got %s", result.Name)
		}
	})
}

func TestDeletePolicy(t *testing.T) {
	t.Run("sends DELETE with ID in path", func(t *testing.T) {
		var receivedMethod, receivedPath string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		client := newAuthTestClient(server)
		err := client.DeletePolicy(context.Background(), "pol_abc")

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedMethod != "DELETE" {
			t.Errorf("expected DELETE, got %s", receivedMethod)
		}
		if receivedPath != "/api/v1/policies/pol_abc" {
			t.Errorf("expected /api/v1/policies/pol_abc, got %s", receivedPath)
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

		_, _ = client.ListPolicies(context.Background())

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
