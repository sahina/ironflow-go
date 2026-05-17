package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// setupMockSchemaServer creates a mock HTTP server and returns a configured Client.
func setupMockSchemaServer(t *testing.T, handler http.Handler) (*Client, func()) {
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
// SchemaClient.Register
// ============================================================================

func TestSchemaClient_Register(t *testing.T) {
	t.Run("registers schema successfully", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/events/schemas" {
				t.Errorf("expected path /api/v1/events/schemas, got %s", r.URL.Path)
			}
			// Decode the wire request format (schema_json as string).
			var body struct {
				EventName  string `json:"event_name"`
				Version    int    `json:"version"`
				SchemaJSON string `json:"schema_json"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode request body: %v", err)
			}
			if body.EventName != "order.placed" {
				t.Errorf("expected event_name 'order.placed', got %s", body.EventName)
			}
			if body.Version != 1 {
				t.Errorf("expected version 1, got %d", body.Version)
			}
			if body.SchemaJSON == "" {
				t.Error("expected non-empty schema_json")
			}
			// Real server returns {"status": "created"} with HTTP 201.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(map[string]any{"status": "created"})
		}))
		defer cleanup()

		schema, err := client.Schemas().Register(context.Background(), RegisterSchemaInput{
			Name:    "order.placed",
			Version: 1,
			Schema:  map[string]any{"type": "object"},
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if schema.Name != "order.placed" {
			t.Errorf("expected name 'order.placed', got %s", schema.Name)
		}
		if schema.Version != 1 {
			t.Errorf("expected version 1, got %d", schema.Version)
		}
		if schema.Schema == nil {
			t.Error("expected non-nil schema")
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
		}))
		defer cleanup()

		_, err := client.Schemas().Register(context.Background(), RegisterSchemaInput{
			Name:    "order.placed",
			Version: 1,
			Schema:  map[string]any{},
		})
		if err == nil {
			t.Fatal("expected error for 500, got nil")
		}
	})
}

// ============================================================================
// SchemaClient.List
// ============================================================================

func TestSchemaClient_List(t *testing.T) {
	t.Run("returns list of schemas", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/events/schemas" {
				t.Errorf("expected path /api/v1/events/schemas, got %s", r.URL.Path)
			}
			// Real server returns store.EventSchema objects with snake_case fields.
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"schemas": []map[string]any{
					{
						"event_name":  "order.placed",
						"version":     1,
						"schema_json": `{"type":"object"}`,
						"created_at":  "2026-03-28T00:00:00Z",
					},
					{
						"event_name":  "order.placed",
						"version":     2,
						"schema_json": `{"type":"object"}`,
						"created_at":  "2026-03-29T00:00:00Z",
					},
				},
			})
		}))
		defer cleanup()

		schemas, err := client.Schemas().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(schemas) != 2 {
			t.Fatalf("expected 2 schemas, got %d", len(schemas))
		}
		if schemas[0].Name != "order.placed" {
			t.Errorf("expected first schema name 'order.placed', got %s", schemas[0].Name)
		}
		if schemas[1].Version != 2 {
			t.Errorf("expected second schema version 2, got %d", schemas[1].Version)
		}
	})

	t.Run("returns empty slice when list is nil", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"schemas": nil,
			})
		}))
		defer cleanup()

		schemas, err := client.Schemas().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if schemas == nil {
			t.Error("expected empty slice, got nil")
		}
		if len(schemas) != 0 {
			t.Errorf("expected 0 schemas, got %d", len(schemas))
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
		}))
		defer cleanup()

		_, err := client.Schemas().List(context.Background())
		if err == nil {
			t.Fatal("expected error for 500, got nil")
		}
	})
}

// ============================================================================
// SchemaClient.Get
// ============================================================================

func TestSchemaClient_Get(t *testing.T) {
	t.Run("returns schema by name", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/events/schemas/order.placed" {
				t.Errorf("expected path /api/v1/events/schemas/order.placed, got %s", r.URL.Path)
			}
			// Real server returns store.EventSchema with snake_case fields.
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"event_name":  "order.placed",
				"version":     2,
				"schema_json": `{"type":"object"}`,
				"created_at":  "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		schema, err := client.Schemas().Get(context.Background(), "order.placed")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if schema.Name != "order.placed" {
			t.Errorf("expected name 'order.placed', got %s", schema.Name)
		}
		if schema.Version != 2 {
			t.Errorf("expected version 2, got %d", schema.Version)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "schema not found"})
		}))
		defer cleanup()

		_, err := client.Schemas().Get(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// SchemaClient.GetVersion
// ============================================================================

func TestSchemaClient_GetVersion(t *testing.T) {
	t.Run("returns specific schema version", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/events/schemas/order.placed/1" {
				t.Errorf("expected path /api/v1/events/schemas/order.placed/1, got %s", r.URL.Path)
			}
			// Real server returns store.EventSchema with snake_case fields.
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"event_name":  "order.placed",
				"version":     1,
				"schema_json": `{"type":"object"}`,
				"created_at":  "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		schema, err := client.Schemas().GetVersion(context.Background(), "order.placed", 1)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if schema.Version != 1 {
			t.Errorf("expected version 1, got %d", schema.Version)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "schema version not found"})
		}))
		defer cleanup()

		_, err := client.Schemas().GetVersion(context.Background(), "order.placed", 999)
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// SchemaClient.Delete
// ============================================================================

func TestSchemaClient_Delete(t *testing.T) {
	t.Run("deletes schema version successfully", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "DELETE" {
				t.Errorf("expected method DELETE, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/events/schemas/order.placed/1" {
				t.Errorf("expected path /api/v1/events/schemas/order.placed/1, got %s", r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		err := client.Schemas().Delete(context.Background(), "order.placed", 1)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "schema not found"})
		}))
		defer cleanup()

		err := client.Schemas().Delete(context.Background(), "nonexistent", 1)
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// SchemaClient.TestUpcast
// ============================================================================

func TestSchemaClient_TestUpcast(t *testing.T) {
	t.Run("returns upcast result on success", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/events/upcast" {
				t.Errorf("expected path /api/v1/events/upcast, got %s", r.URL.Path)
			}
			var body TestUpcastInput
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode request body: %v", err)
			}
			if body.EventName != "order.placed" {
				t.Errorf("expected eventName 'order.placed', got %s", body.EventName)
			}
			if body.FromVersion != 1 {
				t.Errorf("expected fromVersion 1, got %d", body.FromVersion)
			}
			if body.ToVersion != 2 {
				t.Errorf("expected toVersion 2, got %d", body.ToVersion)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data":    map[string]any{"orderId": "123", "totalV2": 99.99},
			})
		}))
		defer cleanup()

		result, err := client.Schemas().TestUpcast(context.Background(), TestUpcastInput{
			EventName:   "order.placed",
			FromVersion: 1,
			ToVersion:   2,
			Data:        map[string]any{"orderId": "123", "total": 99.99},
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if !result.Success {
			t.Error("expected success to be true")
		}
	})

	t.Run("returns failure result with error message", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"success": false,
				"error":   "no upcaster registered for version 1 → 3",
			})
		}))
		defer cleanup()

		result, err := client.Schemas().TestUpcast(context.Background(), TestUpcastInput{
			EventName:   "order.placed",
			FromVersion: 1,
			ToVersion:   3,
			Data:        map[string]any{},
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if result.Success {
			t.Error("expected success to be false")
		}
		if result.Error == "" {
			t.Error("expected non-empty error message")
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		client, cleanup := setupMockSchemaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
		}))
		defer cleanup()

		_, err := client.Schemas().TestUpcast(context.Background(), TestUpcastInput{
			EventName:   "order.placed",
			FromVersion: 1,
			ToVersion:   2,
			Data:        map[string]any{},
		})
		if err == nil {
			t.Fatal("expected error for 500, got nil")
		}
	})
}
