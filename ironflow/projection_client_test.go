package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// setupMockProjectionServer creates a mock HTTP server and returns a configured Client.
func setupMockProjectionServer(t *testing.T, handler http.Handler) (*Client, func()) {
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
// ProjectionClient.Get
// ============================================================================

func TestProjectionClient_Get(t *testing.T) {
	// Wire shape mirrors `internal/server/server.go:2531` ProjectionResponse:
	// embedded ProjectionRegistry (envelope-level fields) + nested `state`
	// ProjectionState row carrying user state under `.state.state`.
	t.Run("peels wire envelope into flat ProjectionStateResult", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projections/order-totals" {
				t.Errorf("expected path /api/v1/projections/order-totals, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"name":           "order-totals",
				"version":        float64(10),
				"mode":           "managed",
				"last_event_seq": float64(42),
				"updated_at":     "2026-03-28T00:00:00Z",
				"state": map[string]any{
					"projection_name": "order-totals",
					"environment_id":  "env_default",
					"partition_key":   "__global__",
					"state":           map[string]any{"total": float64(42)},
					"last_event_id":   "evt-42",
					"last_event_seq":  float64(42),
					"last_event_time": "2026-03-28T00:00:00Z",
					"version":         float64(10),
					"updated_at":      "2026-03-28T00:00:00Z",
				},
			})
		}))
		defer cleanup()

		state, err := client.Projections().Get(context.Background(), "order-totals")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if state.Name != "order-totals" {
			t.Errorf("expected name 'order-totals', got %s", state.Name)
		}
		if state.Partition != "__global__" {
			t.Errorf("expected partition '__global__', got %s", state.Partition)
		}
		userState, ok := state.State.(map[string]any)
		if !ok {
			t.Fatalf("expected map state, got %T", state.State)
		}
		if userState["total"] != float64(42) {
			t.Errorf("expected state total 42, got %v", userState["total"])
		}
		if state.LastEventID != "evt-42" {
			t.Errorf("expected lastEventId 'evt-42', got %s", state.LastEventID)
		}
		if state.LastEventSeq != 42 {
			t.Errorf("expected lastEventSeq 42, got %d", state.LastEventSeq)
		}
		if state.LastEventTime == nil {
			t.Fatal("expected lastEventTime, got nil")
		}
		if state.Version != 10 {
			t.Errorf("expected version 10, got %d", state.Version)
		}
		if state.Mode != "managed" {
			t.Errorf("expected mode 'managed', got %s", state.Mode)
		}
		if state.UpdatedAt.IsZero() {
			t.Error("expected non-zero updatedAt")
		}
	})

	t.Run("returns empty state when server omits inner state row", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"name":           "fresh",
				"version":        float64(1),
				"mode":           "managed",
				"last_event_seq": float64(0),
				"updated_at":     "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		state, err := client.Projections().Get(context.Background(), "fresh")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if state.Partition != "__global__" {
			t.Errorf("expected partition '__global__', got %s", state.Partition)
		}
		if m, ok := state.State.(map[string]any); !ok || len(m) != 0 {
			t.Errorf("expected empty state map, got %v", state.State)
		}
		if state.LastEventTime != nil {
			t.Errorf("expected nil lastEventTime, got %v", state.LastEventTime)
		}
		if state.LastEventSeq != 0 {
			t.Errorf("expected lastEventSeq 0, got %d", state.LastEventSeq)
		}
	})

	t.Run("threads partition option and echoes requested key when no state row", func(t *testing.T) {
		var receivedQuery string
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedQuery = r.URL.RawQuery
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"name":           "by-customer",
				"version":        float64(1),
				"mode":           "managed",
				"last_event_seq": float64(0),
				"updated_at":     "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		state, err := client.Projections().Get(context.Background(), "by-customer", WithPartition("customer-99"))
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if receivedQuery != "partition=customer-99" {
			t.Errorf("expected query 'partition=customer-99', got %s", receivedQuery)
		}
		if state.Partition != "customer-99" {
			t.Errorf("expected partition echo 'customer-99', got %s", state.Partition)
		}
	})

	t.Run("returns drift error when inner state.state field is missing", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"name":           "drifted",
				"version":        float64(1),
				"mode":           "managed",
				"last_event_seq": float64(0),
				"updated_at":     "2026-03-28T00:00:00Z",
				"state": map[string]any{
					"projection_name": "drifted",
					"partition_key":   "__global__",
				},
			})
		}))
		defer cleanup()

		_, err := client.Projections().Get(context.Background(), "drifted")
		if err == nil {
			t.Fatal("expected drift error, got nil")
		}
		if !strings.Contains(err.Error(), "envelope drift") {
			t.Errorf("expected envelope drift error, got: %v", err)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "projection not found"})
		}))
		defer cleanup()

		_, err := client.Projections().Get(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})

	t.Run("handles projection name with special characters", func(t *testing.T) {
		var receivedPath string
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"name":           "order totals",
				"version":        float64(1),
				"mode":           "managed",
				"last_event_seq": float64(0),
				"updated_at":     "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		_, _ = client.Projections().Get(context.Background(), "order totals")
		if receivedPath != "/api/v1/projections/order totals" {
			t.Errorf("expected path '/api/v1/projections/order totals', got %s", receivedPath)
		}
	})
}

// ============================================================================
// ProjectionClient.List
// ============================================================================

func TestProjectionClient_List(t *testing.T) {
	t.Run("returns list of projection statuses", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projections" {
				t.Errorf("expected path /api/v1/projections, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"projections": []map[string]any{
					{
						"name":         "order-totals",
						"status":       "running",
						"eventCount":   float64(100),
						"lastEventAt":  "2026-03-28T00:00:00Z",
						"errorCount":   float64(0),
						"consumerName": "proj-order-totals",
					},
					{
						"name":         "user-summary",
						"status":       "paused",
						"eventCount":   float64(50),
						"lastEventAt":  "2026-03-27T00:00:00Z",
						"errorCount":   float64(2),
						"lastError":    "connection timeout",
						"consumerName": "proj-user-summary",
					},
				},
			})
		}))
		defer cleanup()

		statuses, err := client.Projections().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(statuses) != 2 {
			t.Fatalf("expected 2 statuses, got %d", len(statuses))
		}
		if statuses[0].Name != "order-totals" {
			t.Errorf("expected first name 'order-totals', got %s", statuses[0].Name)
		}
		if statuses[0].Status != "running" {
			t.Errorf("expected first status 'running', got %s", statuses[0].Status)
		}
		if statuses[1].Name != "user-summary" {
			t.Errorf("expected second name 'user-summary', got %s", statuses[1].Name)
		}
		if statuses[1].LastError != "connection timeout" {
			t.Errorf("expected second lastError 'connection timeout', got %s", statuses[1].LastError)
		}
	})

	t.Run("returns empty slice not nil when list is empty", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"projections": nil,
			})
		}))
		defer cleanup()

		statuses, err := client.Projections().List(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if statuses == nil {
			t.Error("expected empty slice, got nil")
		}
		if len(statuses) != 0 {
			t.Errorf("expected 0 statuses, got %d", len(statuses))
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
		}))
		defer cleanup()

		_, err := client.Projections().List(context.Background())
		if err == nil {
			t.Fatal("expected error for 500, got nil")
		}
	})
}

// ============================================================================
// ProjectionClient.GetStatus
// ============================================================================

func TestProjectionClient_GetStatus(t *testing.T) {
	t.Run("returns projection status on success", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projections/order-totals/status" {
				t.Errorf("expected path /api/v1/projections/order-totals/status, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(ProjectionStatusInfo{
				Name:         "order-totals",
				Status:       "running",
				EventCount:   200,
				ConsumerName: "proj-order-totals",
			})
		}))
		defer cleanup()

		status, err := client.Projections().GetStatus(context.Background(), "order-totals")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if status.Name != "order-totals" {
			t.Errorf("expected name 'order-totals', got %s", status.Name)
		}
		if status.Status != "running" {
			t.Errorf("expected status 'running', got %s", status.Status)
		}
		if status.EventCount != 200 {
			t.Errorf("expected eventCount 200, got %d", status.EventCount)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "projection not found"})
		}))
		defer cleanup()

		_, err := client.Projections().GetStatus(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// ProjectionClient.Rebuild
// ============================================================================

func TestProjectionClient_Rebuild(t *testing.T) {
	t.Run("triggers rebuild and returns job", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projections/order-totals/rebuild" {
				t.Errorf("expected path /api/v1/projections/order-totals/rebuild, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(RebuildJob{
				Name:      "order-totals",
				Status:    "running",
				Progress:  0,
				StartedAt: "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		job, err := client.Projections().Rebuild(context.Background(), "order-totals")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if job.Name != "order-totals" {
			t.Errorf("expected name 'order-totals', got %s", job.Name)
		}
		if job.Status != "running" {
			t.Errorf("expected status 'running', got %s", job.Status)
		}
		if job.StartedAt != "2026-03-28T00:00:00Z" {
			t.Errorf("expected startedAt '2026-03-28T00:00:00Z', got %s", job.StartedAt)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "projection not found"})
		}))
		defer cleanup()

		_, err := client.Projections().Rebuild(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// ProjectionClient.GetRebuildJob
// ============================================================================

func TestProjectionClient_GetRebuildJob(t *testing.T) {
	t.Run("returns rebuild job status", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Errorf("expected method GET, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projections/order-totals/rebuild" {
				t.Errorf("expected path /api/v1/projections/order-totals/rebuild, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(RebuildJob{
				Name:      "order-totals",
				Status:    "completed",
				Progress:  100,
				StartedAt: "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		job, err := client.Projections().GetRebuildJob(context.Background(), "order-totals")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if job.Status != "completed" {
			t.Errorf("expected status 'completed', got %s", job.Status)
		}
		if job.Progress != 100 {
			t.Errorf("expected progress 100, got %d", job.Progress)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "no rebuild job found"})
		}))
		defer cleanup()

		_, err := client.Projections().GetRebuildJob(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// ProjectionClient.Delete
// ============================================================================

func TestProjectionClient_Delete(t *testing.T) {
	t.Run("deletes projection successfully", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "DELETE" {
				t.Errorf("expected method DELETE, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projections/order-totals" {
				t.Errorf("expected path /api/v1/projections/order-totals, got %s", r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		err := client.Projections().Delete(context.Background(), "order-totals")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "projection not found"})
		}))
		defer cleanup()

		err := client.Projections().Delete(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// ProjectionClient.Pause
// ============================================================================

func TestProjectionClient_Pause(t *testing.T) {
	t.Run("pauses projection successfully", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projections/order-totals/pause" {
				t.Errorf("expected path /api/v1/projections/order-totals/pause, got %s", r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		err := client.Projections().Pause(context.Background(), "order-totals")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "projection not found"})
		}))
		defer cleanup()

		err := client.Projections().Pause(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// ProjectionClient.Resume
// ============================================================================

func TestProjectionClient_Resume(t *testing.T) {
	t.Run("resumes projection successfully", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projections/order-totals/resume" {
				t.Errorf("expected path /api/v1/projections/order-totals/resume, got %s", r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		err := client.Projections().Resume(context.Background(), "order-totals")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "projection not found"})
		}))
		defer cleanup()

		err := client.Projections().Resume(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// ProjectionClient.ExecuteSQL
// ============================================================================

func TestProjectionClient_ExecuteSQL(t *testing.T) {
	t.Run("executes SQL query and returns results", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/sql" {
				t.Errorf("expected path /api/v1/sql, got %s", r.URL.Path)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode request body: %v", err)
			}
			if body["query"] != "SELECT * FROM orders LIMIT 10" {
				t.Errorf("expected query 'SELECT * FROM orders LIMIT 10', got %s", body["query"])
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"columns": []string{"id", "status", "total"},
				"rows": []map[string]any{
					{"id": "ord_1", "status": "completed", "total": float64(99)},
					{"id": "ord_2", "status": "pending", "total": float64(42)},
				},
				"count": float64(2),
			})
		}))
		defer cleanup()

		result, err := client.Projections().ExecuteSQL(context.Background(), "SELECT * FROM orders LIMIT 10")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(result.Columns) != 3 {
			t.Fatalf("expected 3 columns, got %d", len(result.Columns))
		}
		if result.Columns[0] != "id" {
			t.Errorf("expected first column 'id', got %s", result.Columns[0])
		}
		if result.Columns[1] != "status" {
			t.Errorf("expected second column 'status', got %s", result.Columns[1])
		}
		if result.Columns[2] != "total" {
			t.Errorf("expected third column 'total', got %s", result.Columns[2])
		}
		if len(result.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(result.Rows))
		}
		if result.Rows[0]["id"] != "ord_1" {
			t.Errorf("expected first row id 'ord_1', got %v", result.Rows[0]["id"])
		}
		if result.Rows[0]["status"] != "completed" {
			t.Errorf("expected first row status 'completed', got %v", result.Rows[0]["status"])
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "endpoint not found"})
		}))
		defer cleanup()

		_, err := client.Projections().ExecuteSQL(context.Background(), "SELECT 1")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
		}))
		defer cleanup()

		_, err := client.Projections().ExecuteSQL(context.Background(), "SELECT 1")
		if err == nil {
			t.Fatal("expected error for 500, got nil")
		}
	})
}

// ============================================================================
// ProjectionClient.CancelRebuild
// ============================================================================

func TestProjectionClient_CancelRebuild(t *testing.T) {
	t.Run("cancels rebuild successfully", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projections/order-totals/cancel" {
				t.Errorf("expected path /api/v1/projections/order-totals/cancel, got %s", r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		err := client.Projections().CancelRebuild(context.Background(), "order-totals")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "projection not found"})
		}))
		defer cleanup()

		err := client.Projections().CancelRebuild(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}
