package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// setupMockUserServer creates a mock HTTP server and returns a configured Client.
func setupMockUserServer(t *testing.T, handler http.Handler) (*Client, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	client := &Client{
		serverURL:  server.URL,
		httpClient: server.Client(),
		retryConfig: &ClientRetryConfig{
			MaxAttempts: 1,
		},
		logger: NewNoopLogger(),
	}
	return client, server.Close
}

// ============================================================================
// UserClient.Create
// ============================================================================

func TestUserClient_Create(t *testing.T) {
	t.Run("creates user on success", func(t *testing.T) {
		client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/users" {
				t.Errorf("expected path /api/v1/users, got %s", r.URL.Path)
			}
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if body["email"] != "alice@example.com" {
				t.Errorf("expected email 'alice@example.com', got %v", body["email"])
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]any{
				"id":         "user-1",
				"org_id":     "org_default",
				"email":      "alice@example.com",
				"name":       "Alice",
				"roles":      []string{"admin"},
				"created_at": "2026-03-28T00:00:00Z",
				"updated_at": "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		user, err := client.Users().Create(context.Background(), CreateUserInput{
			Email:    "alice@example.com",
			Name:     "Alice",
			Password: "secret123",
			Roles:    []string{"admin"},
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if user.ID != "user-1" {
			t.Errorf("expected id 'user-1', got %s", user.ID)
		}
		if user.Email != "alice@example.com" {
			t.Errorf("expected email 'alice@example.com', got %s", user.Email)
		}
		if len(user.Roles) != 1 || user.Roles[0] != "admin" {
			t.Errorf("expected roles ['admin'], got %v", user.Roles)
		}
	})

	t.Run("returns error on conflict", func(t *testing.T) {
		client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]any{"error": "user with this email already exists"})
		}))
		defer cleanup()

		_, err := client.Users().Create(context.Background(), CreateUserInput{
			Email:    "existing@example.com",
			Password: "secret",
		})
		if err == nil {
			t.Fatal("expected error for 409, got nil")
		}
	})
}

func TestUserClient_ChangePassword(t *testing.T) {
	client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/api/v1/users/user-1/password" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var body ChangePasswordInput
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.CurrentPassword != "old" || body.NewPassword != "new" {
			t.Fatalf("body = %#v", body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer cleanup()

	if err := client.Users().ChangePassword(context.Background(), "user-1", ChangePasswordInput{CurrentPassword: "old", NewPassword: "new"}); err != nil {
		t.Fatal(err)
	}
}

func TestTenantClient_Provision(t *testing.T) {
	client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/tenants/provision" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"org":         map[string]any{"id": "org-acme", "name": "Acme"},
			"environment": map[string]any{"id": "env-prod", "name": "production"},
			"api_key":     map[string]any{"key": "ifkey_secret", "roles": []string{"admin"}},
		})
	}))
	defer cleanup()

	result, err := client.Tenants().Provision(context.Background(), ProvisionTenantInput{OrgName: "Acme"})
	if err != nil || result.APIKey.Key != "ifkey_secret" {
		t.Fatalf("Provision = %#v, %v", result, err)
	}
}

// ============================================================================
// UserClient.List
// ============================================================================

func TestUserClient_List(t *testing.T) {
	t.Run("returns list of users", func(t *testing.T) {
		client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/users" {
				t.Errorf("expected path /api/v1/users, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode([]map[string]any{
				{
					"id":    "user-1",
					"email": "alice@example.com",
					"name":  "Alice",
				},
				{
					"id":    "user-2",
					"email": "bob@example.com",
					"name":  "Bob",
				},
			})
		}))
		defer cleanup()

		users, err := client.Users().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(users) != 2 {
			t.Fatalf("expected 2 users, got %d", len(users))
		}
		if users[0].Email != "alice@example.com" {
			t.Errorf("expected first email 'alice@example.com', got %s", users[0].Email)
		}
	})

	t.Run("returns empty slice not nil when no users", func(t *testing.T) {
		client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode([]map[string]any{})
		}))
		defer cleanup()

		users, err := client.Users().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if users == nil {
			t.Error("expected empty slice, got nil")
		}
	})
}

// ============================================================================
// UserClient.Get
// ============================================================================

func TestUserClient_Get(t *testing.T) {
	t.Run("returns user on success", func(t *testing.T) {
		client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/users/user-1" {
				t.Errorf("expected path /api/v1/users/user-1, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"id":    "user-1",
				"email": "alice@example.com",
			})
		}))
		defer cleanup()

		user, err := client.Users().Get(context.Background(), "user-1")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if user.ID != "user-1" {
			t.Errorf("expected id 'user-1', got %s", user.ID)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "user not found"})
		}))
		defer cleanup()

		_, err := client.Users().Get(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// UserClient.Update
// ============================================================================

func TestUserClient_Update(t *testing.T) {
	t.Run("updates user on success", func(t *testing.T) {
		var receivedBody map[string]any
		client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "PATCH" {
				t.Errorf("expected method PATCH, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/users/user-1" {
				t.Errorf("expected path /api/v1/users/user-1, got %s", r.URL.Path)
			}
			json.NewDecoder(r.Body).Decode(&receivedBody)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"id":    "user-1",
				"email": "alice@example.com",
				"name":  "Alice Smith",
			})
		}))
		defer cleanup()

		name := "Alice Smith"
		user, err := client.Users().Update(context.Background(), "user-1", UpdateUserInput{
			Name: &name,
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if user.Name != "Alice Smith" {
			t.Errorf("expected name 'Alice Smith', got %s", user.Name)
		}
		if receivedBody["name"] != "Alice Smith" {
			t.Errorf("expected name 'Alice Smith' in body, got %v", receivedBody["name"])
		}
	})
}

// ============================================================================
// UserClient.Delete
// ============================================================================

func TestUserClient_Delete(t *testing.T) {
	t.Run("deletes user successfully", func(t *testing.T) {
		client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "DELETE" {
				t.Errorf("expected method DELETE, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/users/user-1" {
				t.Errorf("expected path /api/v1/users/user-1, got %s", r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		err := client.Users().Delete(context.Background(), "user-1")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "user not found"})
		}))
		defer cleanup()

		err := client.Users().Delete(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// TenantClient.List
// ============================================================================

func TestTenantClient_List(t *testing.T) {
	t.Run("returns list of tenants", func(t *testing.T) {
		client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/tenants" {
				t.Errorf("expected path /api/v1/tenants, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode([]map[string]any{
				{
					"id":         "org_acme",
					"name":       "Acme Corp",
					"env_count":  2,
					"key_count":  3,
					"created_at": "2026-01-01",
				},
			})
		}))
		defer cleanup()

		tenants, err := client.Tenants().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(tenants) != 1 {
			t.Fatalf("expected 1 tenant, got %d", len(tenants))
		}
		if tenants[0].Name != "Acme Corp" {
			t.Errorf("expected name 'Acme Corp', got %s", tenants[0].Name)
		}
		if tenants[0].EnvCount != 2 {
			t.Errorf("expected env_count 2, got %d", tenants[0].EnvCount)
		}
		if tenants[0].KeyCount != 3 {
			t.Errorf("expected key_count 3, got %d", tenants[0].KeyCount)
		}
	})

	t.Run("returns enterprise required error on 402", func(t *testing.T) {
		client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusPaymentRequired)
			json.NewEncoder(w).Encode(map[string]any{"error": "enterprise license required"})
		}))
		defer cleanup()

		_, err := client.Tenants().List(context.Background())
		if err == nil {
			t.Fatal("expected error for 402, got nil")
		}
	})

	t.Run("returns empty slice not nil when no tenants", func(t *testing.T) {
		client, cleanup := setupMockUserServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode([]map[string]any{})
		}))
		defer cleanup()

		tenants, err := client.Tenants().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if tenants == nil {
			t.Error("expected empty slice, got nil")
		}
	})
}

// ============================================================================
// Type Tests
// ============================================================================

func TestUserTenantTypes(t *testing.T) {
	t.Run("User JSON marshaling", func(t *testing.T) {
		user := User{
			ID:    "user-1",
			OrgID: "org_default",
			Email: "alice@example.com",
			Name:  "Alice",
			Roles: []string{"admin", "viewer"},
		}

		data, err := json.Marshal(user)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded User
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if decoded.ID != "user-1" {
			t.Errorf("expected id 'user-1', got %s", decoded.ID)
		}
		if len(decoded.Roles) != 2 {
			t.Errorf("expected 2 roles, got %d", len(decoded.Roles))
		}
	})

	t.Run("Tenant JSON marshaling", func(t *testing.T) {
		tenant := Tenant{
			ID:       "org_acme",
			Name:     "Acme Corp",
			EnvCount: 3,
			KeyCount: 5,
		}

		data, err := json.Marshal(tenant)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded Tenant
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if decoded.Name != "Acme Corp" {
			t.Errorf("expected name 'Acme Corp', got %s", decoded.Name)
		}
		if decoded.EnvCount != 3 {
			t.Errorf("expected env_count 3, got %d", decoded.EnvCount)
		}
	})
}

func TestWebhookSourceDeliveryTypes(t *testing.T) {
	t.Run("WebhookSource JSON marshaling", func(t *testing.T) {
		source := WebhookSource{
			ID:          "stripe",
			EventPrefix: "stripe.",
			SourceType:  "api",
		}

		data, err := json.Marshal(source)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded WebhookSource
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if decoded.ID != "stripe" {
			t.Errorf("expected id 'stripe', got %s", decoded.ID)
		}
		if decoded.EventPrefix != "stripe." {
			t.Errorf("expected event_prefix 'stripe.', got %s", decoded.EventPrefix)
		}
	})

	t.Run("WebhookDelivery JSON marshaling", func(t *testing.T) {
		delivery := WebhookDelivery{
			ID:       "del-1",
			SourceID: "stripe",
			Status:   "delivered",
		}

		data, err := json.Marshal(delivery)
		if err != nil {
			t.Fatalf("failed to marshal: %v", err)
		}

		var decoded WebhookDelivery
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if decoded.Status != "delivered" {
			t.Errorf("expected status 'delivered', got %s", decoded.Status)
		}
	})
}
