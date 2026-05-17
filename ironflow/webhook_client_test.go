package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// setupMockWebhookServer creates a mock HTTP server and returns a configured Client.
func setupMockWebhookServer(t *testing.T, handler http.Handler) (*Client, func()) {
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
// WebhookManagementClient.CreateSource
// ============================================================================

func TestWebhookManagementClient_CreateSource(t *testing.T) {
	t.Run("creates source successfully", func(t *testing.T) {
		var receivedBody map[string]any
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/ironflow.v1.WebhookService/CreateWebhookSource" {
				t.Errorf("expected path /ironflow.v1.WebhookService/CreateWebhookSource, got %s", r.URL.Path)
			}
			json.NewDecoder(r.Body).Decode(&receivedBody)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"id":           "stripe",
				"event_prefix": "stripe.",
				"source_type":  "api",
				"created_at":   "2026-03-28T00:00:00Z",
				"updated_at":   "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		source, err := client.Webhooks().CreateSource(context.Background(), CreateWebhookSourceInput{
			ID:              "stripe",
			EventPrefix:     "stripe.",
			VerifyHeader:    "X-Stripe-Signature",
			VerifyAlgorithm: "sha256",
			VerifySecret:    "whsec_test123",
			Metadata:        map[string]any{"env": "production"},
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if source.ID != "stripe" {
			t.Errorf("expected id 'stripe', got %s", source.ID)
		}
		if source.EventPrefix != "stripe." {
			t.Errorf("expected event_prefix 'stripe.', got %s", source.EventPrefix)
		}
		if source.SourceType != "api" {
			t.Errorf("expected source_type 'api', got %s", source.SourceType)
		}
		// Verify request body fields
		if receivedBody["id"] != "stripe" {
			t.Errorf("expected id 'stripe' in body, got %v", receivedBody["id"])
		}
		if receivedBody["event_prefix"] != "stripe." {
			t.Errorf("expected event_prefix 'stripe.' in body, got %v", receivedBody["event_prefix"])
		}
		if receivedBody["verify_header"] != "X-Stripe-Signature" {
			t.Errorf("expected verify_header in body, got %v", receivedBody["verify_header"])
		}
		if receivedBody["verify_secret"] != "whsec_test123" {
			t.Errorf("expected verify_secret in body, got %v", receivedBody["verify_secret"])
		}
	})

	t.Run("returns error on conflict", func(t *testing.T) {
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]any{"error": "webhook source already exists"})
		}))
		defer cleanup()

		_, err := client.Webhooks().CreateSource(context.Background(), CreateWebhookSourceInput{
			ID:          "stripe",
			EventPrefix: "stripe.",
		})
		if err == nil {
			t.Fatal("expected error for 409, got nil")
		}
	})
}

// ============================================================================
// WebhookManagementClient.ListSources
// ============================================================================

func TestWebhookManagementClient_ListSources(t *testing.T) {
	t.Run("returns sources on success", func(t *testing.T) {
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/ironflow.v1.WebhookService/ListWebhookSources" {
				t.Errorf("expected path /ironflow.v1.WebhookService/ListWebhookSources, got %s", r.URL.Path)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"sources": []map[string]any{
					{
						"id":           "stripe",
						"event_prefix": "stripe.",
						"source_type":  "api",
						"created_at":   "2026-03-28T00:00:00Z",
						"updated_at":   "2026-03-28T00:00:00Z",
					},
				},
			})
		}))
		defer cleanup()

		sources, err := client.Webhooks().ListSources(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(sources) != 1 {
			t.Fatalf("expected 1 source, got %d", len(sources))
		}
		if sources[0].ID != "stripe" {
			t.Errorf("expected id 'stripe', got %s", sources[0].ID)
		}
		if sources[0].EventPrefix != "stripe." {
			t.Errorf("expected event_prefix 'stripe.', got %s", sources[0].EventPrefix)
		}
	})

	t.Run("returns empty slice when no sources exist", func(t *testing.T) {
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"sources": nil})
		}))
		defer cleanup()

		sources, err := client.Webhooks().ListSources(context.Background())
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if sources == nil {
			t.Error("expected empty slice, got nil")
		}
		if len(sources) != 0 {
			t.Errorf("expected 0 sources, got %d", len(sources))
		}
	})

	t.Run("returns error on server error", func(t *testing.T) {
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]any{"error": "internal server error"})
		}))
		defer cleanup()

		_, err := client.Webhooks().ListSources(context.Background())
		if err == nil {
			t.Fatal("expected error for 500, got nil")
		}
	})
}

// ============================================================================
// WebhookManagementClient.DeleteSource
// ============================================================================

func TestWebhookManagementClient_DeleteSource(t *testing.T) {
	t.Run("deletes source successfully", func(t *testing.T) {
		var receivedBody map[string]any
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				t.Errorf("expected method POST, got %s", r.Method)
			}
			if r.URL.Path != "/ironflow.v1.WebhookService/DeleteWebhookSource" {
				t.Errorf("expected path /ironflow.v1.WebhookService/DeleteWebhookSource, got %s", r.URL.Path)
			}
			json.NewDecoder(r.Body).Decode(&receivedBody)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]any{})
		}))
		defer cleanup()

		err := client.Webhooks().DeleteSource(context.Background(), "stripe")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if receivedBody["id"] != "stripe" {
			t.Errorf("expected id 'stripe' in body, got %v", receivedBody["id"])
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "not found"})
		}))
		defer cleanup()

		err := client.Webhooks().DeleteSource(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// WebhookManagementClient.ListDeliveries
// ============================================================================

func TestWebhookManagementClient_ListDeliveries(t *testing.T) {
	t.Run("returns deliveries with total count", func(t *testing.T) {
		var receivedBody map[string]any
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/ironflow.v1.WebhookService/ListWebhookDeliveries" {
				t.Errorf("expected path /ironflow.v1.WebhookService/ListWebhookDeliveries, got %s", r.URL.Path)
			}
			json.NewDecoder(r.Body).Decode(&receivedBody)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"deliveries": []map[string]any{
					{
						"id":        "del-1",
						"source_id": "stripe",
						"status":    "delivered",
						"event_id":  "evt-123",
					},
					{
						"id":        "del-2",
						"source_id": "stripe",
						"status":    "failed",
						"error":     "timeout",
					},
				},
				"total_count": 2,
			})
		}))
		defer cleanup()

		opts := ListWebhookDeliveriesOpts{
			SourceID: "stripe",
			Status:   "delivered",
			Limit:    10,
		}
		deliveries, total, err := client.Webhooks().ListDeliveries(context.Background(), opts)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if len(deliveries) != 2 {
			t.Fatalf("expected 2 deliveries, got %d", len(deliveries))
		}
		if total != 2 {
			t.Errorf("expected total_count 2, got %d", total)
		}
		if deliveries[0].ID != "del-1" {
			t.Errorf("expected first delivery id 'del-1', got %s", deliveries[0].ID)
		}
		if deliveries[0].Status != "delivered" {
			t.Errorf("expected first delivery status 'delivered', got %s", deliveries[0].Status)
		}
		if deliveries[1].Error != "timeout" {
			t.Errorf("expected second delivery error 'timeout', got %s", deliveries[1].Error)
		}
		// Check request body includes filters
		if receivedBody["source_id"] != "stripe" {
			t.Errorf("expected source_id 'stripe' in body, got %v", receivedBody["source_id"])
		}
	})

	t.Run("returns empty slice when no deliveries", func(t *testing.T) {
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"deliveries":  nil,
				"total_count": 0,
			})
		}))
		defer cleanup()

		deliveries, total, err := client.Webhooks().ListDeliveries(context.Background(), ListWebhookDeliveriesOpts{})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if deliveries == nil {
			t.Error("expected empty slice, got nil")
		}
		if len(deliveries) != 0 {
			t.Errorf("expected 0 deliveries, got %d", len(deliveries))
		}
		if total != 0 {
			t.Errorf("expected total_count 0, got %d", total)
		}
	})
}
