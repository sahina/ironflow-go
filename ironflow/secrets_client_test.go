package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// setupMockSecretsServer creates a mock HTTP server and returns a configured Client.
func setupMockSecretsServer(t *testing.T, handler http.Handler) (*Client, func()) {
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
// SecretsClient.Get
// ============================================================================

func TestSecretsClient_Get(t *testing.T) {
	t.Run("returns secret on success", func(t *testing.T) {
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/secrets/db-password" {
				t.Errorf("expected path /api/v1/secrets/db-password, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"name":       "db-password",
				"value":      "super-secret",
				"created_at": "2026-03-28T00:00:00Z",
				"updated_at": "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		secret, err := client.Secrets().Get(context.Background(), "db-password")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if secret.Name != "db-password" {
			t.Errorf("expected name 'db-password', got %s", secret.Name)
		}
		if secret.Value != "super-secret" {
			t.Errorf("expected value 'super-secret', got %s", secret.Value)
		}
		if secret.CreatedAt != "2026-03-28T00:00:00Z" {
			t.Errorf("expected createdAt '2026-03-28T00:00:00Z', got %s", secret.CreatedAt)
		}
		if secret.UpdatedAt != "2026-03-28T00:00:00Z" {
			t.Errorf("expected updatedAt '2026-03-28T00:00:00Z', got %s", secret.UpdatedAt)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "secret not found"})
		}))
		defer cleanup()

		_, err := client.Secrets().Get(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})

	t.Run("handles secret name with special characters", func(t *testing.T) {
		// Go's HTTP server decodes percent-encoding before the handler sees r.URL.Path.
		var receivedPath string
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Secret{Name: "my secret"})
		}))
		defer cleanup()

		_, _ = client.Secrets().Get(context.Background(), "my secret")
		// Go's HTTP server decodes %20 back to space in r.URL.Path
		if receivedPath != "/api/v1/secrets/my secret" {
			t.Errorf("expected path '/api/v1/secrets/my secret', got %s", receivedPath)
		}
	})
}

// ============================================================================
// SecretsClient.Set
// ============================================================================

func TestSecretsClient_Set(t *testing.T) {
	t.Run("creates secret on success", func(t *testing.T) {
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/secrets" {
				t.Errorf("expected path /api/v1/secrets, got %s", r.URL.Path)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode request body: %v", err)
			}
			if body["name"] != "api-key" {
				t.Errorf("expected body name 'api-key', got %s", body["name"])
			}
			if body["value"] != "my-api-key-value" {
				t.Errorf("expected body value 'my-api-key-value', got %s", body["value"])
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]any{
				"name":       "api-key",
				"value":      "my-api-key-value",
				"created_at": "2026-03-28T00:00:00Z",
				"updated_at": "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		secret, err := client.Secrets().Set(context.Background(), "api-key", "my-api-key-value")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if secret.Name != "api-key" {
			t.Errorf("expected name 'api-key', got %s", secret.Name)
		}
		if secret.CreatedAt != "2026-03-28T00:00:00Z" {
			t.Errorf("expected createdAt '2026-03-28T00:00:00Z', got %s", secret.CreatedAt)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
		}))
		defer cleanup()

		_, err := client.Secrets().Set(context.Background(), "api-key", "value")
		if err == nil {
			t.Fatal("expected error for 500, got nil")
		}
	})
}

// ============================================================================
// SecretsClient.Update
// ============================================================================

func TestSecretsClient_Update(t *testing.T) {
	t.Run("updates secret on success", func(t *testing.T) {
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "PUT" {
				t.Errorf("expected method PUT, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/secrets/db-password" {
				t.Errorf("expected path /api/v1/secrets/db-password, got %s", r.URL.Path)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode request body: %v", err)
			}
			if body["value"] != "new-password" {
				t.Errorf("expected body value 'new-password', got %s", body["value"])
			}
			// Ensure name is NOT in body for PUT
			if _, ok := body["name"]; ok {
				t.Error("expected body to not contain 'name' for PUT")
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"name":       "db-password",
				"value":      "new-password",
				"created_at": "2026-03-28T00:00:00Z",
				"updated_at": "2026-03-28T01:00:00Z",
			})
		}))
		defer cleanup()

		secret, err := client.Secrets().Update(context.Background(), "db-password", "new-password")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if secret.Name != "db-password" {
			t.Errorf("expected name 'db-password', got %s", secret.Name)
		}
		if secret.UpdatedAt != "2026-03-28T01:00:00Z" {
			t.Errorf("expected updatedAt '2026-03-28T01:00:00Z', got %s", secret.UpdatedAt)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "secret not found"})
		}))
		defer cleanup()

		_, err := client.Secrets().Update(context.Background(), "nonexistent", "value")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// SecretsClient.List
// ============================================================================

func TestSecretsClient_List(t *testing.T) {
	t.Run("returns list of secrets without values", func(t *testing.T) {
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/secrets" {
				t.Errorf("expected path /api/v1/secrets, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"secrets": []map[string]any{
					{
						"name":       "db-password",
						"created_at": "2026-03-28T00:00:00Z",
						"updated_at": "2026-03-28T00:00:00Z",
					},
					{
						"name":       "api-key",
						"created_at": "2026-03-27T00:00:00Z",
						"updated_at": "2026-03-27T00:00:00Z",
					},
				},
			})
		}))
		defer cleanup()

		secrets, err := client.Secrets().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(secrets) != 2 {
			t.Fatalf("expected 2 secrets, got %d", len(secrets))
		}
		if secrets[0].Name != "db-password" {
			t.Errorf("expected first name 'db-password', got %s", secrets[0].Name)
		}
		if secrets[1].Name != "api-key" {
			t.Errorf("expected second name 'api-key', got %s", secrets[1].Name)
		}
		if secrets[0].CreatedAt != "2026-03-28T00:00:00Z" {
			t.Errorf("expected first createdAt '2026-03-28T00:00:00Z', got %s", secrets[0].CreatedAt)
		}
	})

	t.Run("returns empty slice not nil when list is empty", func(t *testing.T) {
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"secrets": nil,
			})
		}))
		defer cleanup()

		secrets, err := client.Secrets().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if secrets == nil {
			t.Error("expected empty slice, got nil")
		}
		if len(secrets) != 0 {
			t.Errorf("expected 0 secrets, got %d", len(secrets))
		}
	})

	t.Run("handles plain array response format", func(t *testing.T) {
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode([]map[string]any{
				{
					"name":       "plain-secret",
					"created_at": "2026-03-28T00:00:00Z",
					"updated_at": "2026-03-28T00:00:00Z",
				},
			})
		}))
		defer cleanup()

		secrets, err := client.Secrets().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(secrets) != 1 {
			t.Fatalf("expected 1 secret, got %d", len(secrets))
		}
		if secrets[0].Name != "plain-secret" {
			t.Errorf("expected name 'plain-secret', got %s", secrets[0].Name)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
		}))
		defer cleanup()

		_, err := client.Secrets().List(context.Background())
		if err == nil {
			t.Fatal("expected error for 500, got nil")
		}
	})
}

// ============================================================================
// SecretsClient.Delete
// ============================================================================

func TestSecretsClient_Delete(t *testing.T) {
	t.Run("deletes secret successfully", func(t *testing.T) {
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "DELETE" {
				t.Errorf("expected method DELETE, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/secrets/db-password" {
				t.Errorf("expected path /api/v1/secrets/db-password, got %s", r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		err := client.Secrets().Delete(context.Background(), "db-password")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockSecretsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "secret not found"})
		}))
		defer cleanup()

		err := client.Secrets().Delete(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}
