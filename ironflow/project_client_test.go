package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// setupMockProjectServer creates a mock HTTP server and returns a configured Client.
func setupMockProjectServer(t *testing.T, handler http.Handler) (*Client, func()) {
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
// ProjectClient.List
// ============================================================================

func TestProjectClient_List(t *testing.T) {
	t.Run("returns list of projects", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projects" {
				t.Errorf("expected path /api/v1/projects, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"projects": []map[string]any{
					{
						"id":          "proj_abc123",
						"name":        "my-service",
						"description": "My main service",
						"org_id":      "org_default",
						"created_at":  "2026-03-28T00:00:00Z",
						"updated_at":  "2026-03-28T00:00:00Z",
					},
					{
						"id":          "proj_def456",
						"name":        "analytics",
						"description": "",
						"org_id":      "org_default",
						"created_at":  "2026-03-27T00:00:00Z",
						"updated_at":  "2026-03-27T00:00:00Z",
					},
				},
			})
		}))
		defer cleanup()

		projects, err := client.Projects().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(projects) != 2 {
			t.Fatalf("expected 2 projects, got %d", len(projects))
		}
		if projects[0].ID != "proj_abc123" {
			t.Errorf("expected first ID 'proj_abc123', got %s", projects[0].ID)
		}
		if projects[0].Name != "my-service" {
			t.Errorf("expected first name 'my-service', got %s", projects[0].Name)
		}
		if projects[0].OrgID != "org_default" {
			t.Errorf("expected first orgId 'org_default', got %s", projects[0].OrgID)
		}
		if projects[1].Name != "analytics" {
			t.Errorf("expected second name 'analytics', got %s", projects[1].Name)
		}
	})

	t.Run("returns empty slice not nil when list is empty", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"projects": nil,
			})
		}))
		defer cleanup()

		projects, err := client.Projects().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if projects == nil {
			t.Error("expected empty slice, got nil")
		}
		if len(projects) != 0 {
			t.Errorf("expected 0 projects, got %d", len(projects))
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
		}))
		defer cleanup()

		_, err := client.Projects().List(context.Background())
		if err == nil {
			t.Fatal("expected error for 500, got nil")
		}
	})
}

// ============================================================================
// ProjectClient.Create
// ============================================================================

func TestProjectClient_Create(t *testing.T) {
	t.Run("creates project on success", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projects" {
				t.Errorf("expected path /api/v1/projects, got %s", r.URL.Path)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode request body: %v", err)
			}
			if body["name"] != "new-project" {
				t.Errorf("expected body name 'new-project', got %s", body["name"])
			}
			if body["description"] != "A new project" {
				t.Errorf("expected body description 'A new project', got %s", body["description"])
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]any{
				"id":          "proj_new123",
				"name":        "new-project",
				"description": "A new project",
				"org_id":      "org_default",
				"created_at":  "2026-03-28T00:00:00Z",
				"updated_at":  "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		project, err := client.Projects().Create(context.Background(), CreateProjectInput{
			Name:        "new-project",
			Description: "A new project",
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if project.ID != "proj_new123" {
			t.Errorf("expected ID 'proj_new123', got %s", project.ID)
		}
		if project.Name != "new-project" {
			t.Errorf("expected name 'new-project', got %s", project.Name)
		}
		if project.Description != "A new project" {
			t.Errorf("expected description 'A new project', got %s", project.Description)
		}
		if project.CreatedAt != "2026-03-28T00:00:00Z" {
			t.Errorf("expected createdAt '2026-03-28T00:00:00Z', got %s", project.CreatedAt)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
		}))
		defer cleanup()

		_, err := client.Projects().Create(context.Background(), CreateProjectInput{Name: "fail"})
		if err == nil {
			t.Fatal("expected error for 500, got nil")
		}
	})
}

// ============================================================================
// ProjectClient.Update
// ============================================================================

func TestProjectClient_Update(t *testing.T) {
	t.Run("updates project on success", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "PUT" {
				t.Errorf("expected method PUT, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projects/proj_abc123" {
				t.Errorf("expected path /api/v1/projects/proj_abc123, got %s", r.URL.Path)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode request body: %v", err)
			}
			if body["name"] != "renamed-project" {
				t.Errorf("expected body name 'renamed-project', got %s", body["name"])
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"id":          "proj_abc123",
				"name":        "renamed-project",
				"description": "My main service",
				"org_id":      "org_default",
				"created_at":  "2026-03-28T00:00:00Z",
				"updated_at":  "2026-03-28T01:00:00Z",
			})
		}))
		defer cleanup()

		project, err := client.Projects().Update(context.Background(), "proj_abc123", UpdateProjectInput{
			Name: "renamed-project",
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if project.Name != "renamed-project" {
			t.Errorf("expected name 'renamed-project', got %s", project.Name)
		}
		if project.UpdatedAt != "2026-03-28T01:00:00Z" {
			t.Errorf("expected updatedAt '2026-03-28T01:00:00Z', got %s", project.UpdatedAt)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "project not found"})
		}))
		defer cleanup()

		_, err := client.Projects().Update(context.Background(), "proj_nonexistent", UpdateProjectInput{Name: "x"})
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// ProjectClient.Delete
// ============================================================================

func TestProjectClient_Delete(t *testing.T) {
	t.Run("deletes project successfully", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "DELETE" {
				t.Errorf("expected method DELETE, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projects/proj_abc123" {
				t.Errorf("expected path /api/v1/projects/proj_abc123, got %s", r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		err := client.Projects().Delete(context.Background(), "proj_abc123")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "project not found"})
		}))
		defer cleanup()

		err := client.Projects().Delete(context.Background(), "proj_nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// ProjectClient.ListEnvironments
// ============================================================================

func TestProjectClient_ListEnvironments(t *testing.T) {
	t.Run("returns list of environments", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/environments" {
				t.Errorf("expected path /api/v1/environments, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"environments": []map[string]any{
					{
						"id":         "env_prod",
						"name":       "production",
						"project_id": "proj_abc123",
						"created_at": "2026-03-28T00:00:00Z",
						"updated_at": "2026-03-28T00:00:00Z",
					},
					{
						"id":         "env_staging",
						"name":       "staging",
						"project_id": "proj_abc123",
						"created_at": "2026-03-27T00:00:00Z",
						"updated_at": "2026-03-27T00:00:00Z",
					},
				},
			})
		}))
		defer cleanup()

		envs, err := client.Projects().ListEnvironments(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(envs) != 2 {
			t.Fatalf("expected 2 environments, got %d", len(envs))
		}
		if envs[0].ID != "env_prod" {
			t.Errorf("expected first ID 'env_prod', got %s", envs[0].ID)
		}
		if envs[0].Name != "production" {
			t.Errorf("expected first name 'production', got %s", envs[0].Name)
		}
		if envs[0].ProjectID != "proj_abc123" {
			t.Errorf("expected first projectId 'proj_abc123', got %s", envs[0].ProjectID)
		}
		if envs[1].Name != "staging" {
			t.Errorf("expected second name 'staging', got %s", envs[1].Name)
		}
	})

	t.Run("returns empty slice not nil when list is empty", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"environments": nil,
			})
		}))
		defer cleanup()

		envs, err := client.Projects().ListEnvironments(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if envs == nil {
			t.Error("expected empty slice, got nil")
		}
		if len(envs) != 0 {
			t.Errorf("expected 0 environments, got %d", len(envs))
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
		}))
		defer cleanup()

		_, err := client.Projects().ListEnvironments(context.Background())
		if err == nil {
			t.Fatal("expected error for 500, got nil")
		}
	})
}

// ============================================================================
// ProjectClient.CreateEnvironment
// ============================================================================

func TestProjectClient_CreateEnvironment(t *testing.T) {
	t.Run("creates environment on success", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/environments" {
				t.Errorf("expected path /api/v1/environments, got %s", r.URL.Path)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode request body: %v", err)
			}
			if body["name"] != "production" {
				t.Errorf("expected body name 'production', got %s", body["name"])
			}
			if body["project_id"] != "proj_abc123" {
				t.Errorf("expected body projectId 'proj_abc123', got %s", body["project_id"])
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]any{
				"id":         "env_prod",
				"name":       "production",
				"project_id": "proj_abc123",
				"created_at": "2026-03-28T00:00:00Z",
				"updated_at": "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		env, err := client.Projects().CreateEnvironment(context.Background(), CreateEnvironmentInput{
			Name:      "production",
			ProjectID: "proj_abc123",
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if env.ID != "env_prod" {
			t.Errorf("expected ID 'env_prod', got %s", env.ID)
		}
		if env.Name != "production" {
			t.Errorf("expected name 'production', got %s", env.Name)
		}
		if env.ProjectID != "proj_abc123" {
			t.Errorf("expected projectId 'proj_abc123', got %s", env.ProjectID)
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
		}))
		defer cleanup()

		_, err := client.Projects().CreateEnvironment(context.Background(), CreateEnvironmentInput{Name: "fail", ProjectID: "proj_x"})
		if err == nil {
			t.Fatal("expected error for 500, got nil")
		}
	})
}

// ============================================================================
// ProjectClient.UpdateEnvironment
// ============================================================================

func TestProjectClient_UpdateEnvironment(t *testing.T) {
	t.Run("updates environment on success", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "PUT" {
				t.Errorf("expected method PUT, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/environments/env_prod" {
				t.Errorf("expected path /api/v1/environments/env_prod, got %s", r.URL.Path)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode request body: %v", err)
			}
			if body["name"] != "prod-renamed" {
				t.Errorf("expected body name 'prod-renamed', got %s", body["name"])
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"id":         "env_prod",
				"name":       "prod-renamed",
				"project_id": "proj_abc123",
				"created_at": "2026-03-28T00:00:00Z",
				"updated_at": "2026-03-28T02:00:00Z",
			})
		}))
		defer cleanup()

		env, err := client.Projects().UpdateEnvironment(context.Background(), "env_prod", UpdateEnvironmentInput{
			Name: "prod-renamed",
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if env.Name != "prod-renamed" {
			t.Errorf("expected name 'prod-renamed', got %s", env.Name)
		}
		if env.UpdatedAt != "2026-03-28T02:00:00Z" {
			t.Errorf("expected updatedAt '2026-03-28T02:00:00Z', got %s", env.UpdatedAt)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "environment not found"})
		}))
		defer cleanup()

		_, err := client.Projects().UpdateEnvironment(context.Background(), "env_nonexistent", UpdateEnvironmentInput{Name: "x"})
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// ProjectClient.DeleteEnvironment
// ============================================================================

func TestProjectClient_DeleteEnvironment(t *testing.T) {
	t.Run("deletes environment successfully", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "DELETE" {
				t.Errorf("expected method DELETE, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/environments/env_prod" {
				t.Errorf("expected path /api/v1/environments/env_prod, got %s", r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		err := client.Projects().DeleteEnvironment(context.Background(), "env_prod")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "environment not found"})
		}))
		defer cleanup()

		err := client.Projects().DeleteEnvironment(context.Background(), "env_nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}
