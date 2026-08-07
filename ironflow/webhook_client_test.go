package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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
			Name:            "stripe",
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
		if receivedBody["name"] != "stripe" {
			t.Errorf("expected name 'stripe' in body, got %v", receivedBody["name"])
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
			Name:        "stripe",
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
// WebhookManagementClient.GetSource
// ============================================================================

func TestWebhookManagementClient_GetSource(t *testing.T) {
	t.Run("returns source successfully", func(t *testing.T) {
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/ironflow.v1.WebhookService/GetWebhookSource" {
				t.Errorf("expected path /ironflow.v1.WebhookService/GetWebhookSource, got %s", r.URL.Path)
			}
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if body["id"] != "wh_stripe" {
				t.Errorf("expected id 'wh_stripe' in body, got %v", body["id"])
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"id":           "wh_stripe",
				"name":         "stripe",
				"event_prefix": "stripe.",
				"source_type":  "api",
			})
		}))
		defer cleanup()

		source, err := client.Webhooks().GetSource(context.Background(), "wh_stripe")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if source.ID != "wh_stripe" {
			t.Errorf("expected id 'wh_stripe', got %s", source.ID)
		}
		if source.Name != "stripe" {
			t.Errorf("expected name 'stripe', got %s", source.Name)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "not found"})
		}))
		defer cleanup()

		_, err := client.Webhooks().GetSource(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// WebhookManagementClient.UpdateSource
// ============================================================================

func TestWebhookManagementClient_UpdateSource(t *testing.T) {
	t.Run("updates source successfully", func(t *testing.T) {
		var receivedBody map[string]any
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/ironflow.v1.WebhookService/UpdateWebhookSource" {
				t.Errorf("expected path /ironflow.v1.WebhookService/UpdateWebhookSource, got %s", r.URL.Path)
			}
			json.NewDecoder(r.Body).Decode(&receivedBody)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"id":                "wh_x",
				"name":              "renamed",
				"event_prefix":      "stripe.",
				"verify_header":     "X-New",
				"verify_algorithm":  "hmac-sha1",
				"verify_secret_set": true,
				"source_type":       "api",
			})
		}))
		defer cleanup()

		source, err := client.Webhooks().UpdateSource(context.Background(), UpdateWebhookSourceInput{
			ID:              "wh_x",
			Name:            "renamed",
			VerifyHeader:    "X-New",
			VerifyAlgorithm: "hmac-sha1",
			Metadata:        map[string]any{"env": "prod"},
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if source.Name != "renamed" {
			t.Errorf("expected name 'renamed', got %s", source.Name)
		}
		if !source.VerifySecretSet {
			t.Error("expected verify_secret_set true")
		}
		if receivedBody["id"] != "wh_x" {
			t.Errorf("expected id 'wh_x' in body, got %v", receivedBody["id"])
		}
		if receivedBody["verify_header"] != "X-New" {
			t.Errorf("expected verify_header 'X-New' in body, got %v", receivedBody["verify_header"])
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "not found"})
		}))
		defer cleanup()

		_, err := client.Webhooks().UpdateSource(context.Background(), UpdateWebhookSourceInput{
			ID:   "nonexistent",
			Name: "x",
		})
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// WebhookManagementClient.RotateSecret
// ============================================================================

func TestWebhookManagementClient_RotateSecret(t *testing.T) {
	t.Run("rotates secret successfully", func(t *testing.T) {
		var receivedBody map[string]any
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/ironflow.v1.WebhookService/RotateWebhookSecret" {
				t.Errorf("expected path /ironflow.v1.WebhookService/RotateWebhookSecret, got %s", r.URL.Path)
			}
			json.NewDecoder(r.Body).Decode(&receivedBody)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"id":                "wh_x",
				"name":              "rot",
				"verify_secret_set": true,
				"source_type":       "api",
			})
		}))
		defer cleanup()

		source, err := client.Webhooks().RotateSecret(context.Background(), RotateWebhookSecretInput{
			ID:           "wh_x",
			VerifySecret: "whsec_new",
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if !source.VerifySecretSet {
			t.Error("expected verify_secret_set true after rotate")
		}
		if receivedBody["id"] != "wh_x" {
			t.Errorf("expected id in body, got %v", receivedBody["id"])
		}
		if receivedBody["verify_secret"] != "whsec_new" {
			t.Errorf("expected verify_secret in body, got %v", receivedBody["verify_secret"])
		}
		if _, ok := receivedBody["grace_seconds"]; ok {
			t.Errorf("did not expect grace_seconds when GracePeriod is nil, got %v", receivedBody["grace_seconds"])
		}
	})

	t.Run("sends grace_seconds when GracePeriod set", func(t *testing.T) {
		var receivedBody map[string]any
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewDecoder(r.Body).Decode(&receivedBody)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"id": "wh_x", "verify_secret_set": true})
		}))
		defer cleanup()

		grace := 60 * time.Minute
		_, err := client.Webhooks().RotateSecret(context.Background(), RotateWebhookSecretInput{
			ID:           "wh_x",
			VerifySecret: "whsec_new",
			GracePeriod:  &grace,
		})
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if got := receivedBody["grace_seconds"]; got != float64(3600) {
			t.Errorf("expected grace_seconds=3600, got %v", got)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "not found"})
		}))
		defer cleanup()

		_, err := client.Webhooks().RotateSecret(context.Background(), RotateWebhookSecretInput{
			ID:           "nonexistent",
			VerifySecret: "x",
		})
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

// ============================================================================
// WebhookManagementClient.RotateIngestToken
// ============================================================================

func TestWebhookManagementClient_RotateIngestToken(t *testing.T) {
	t.Run("returns the raw token, which is unrecoverable afterwards", func(t *testing.T) {
		var receivedBody map[string]any
		var receivedPath string
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			json.NewDecoder(r.Body).Decode(&receivedBody)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"id":                  "whs_1",
				"event_prefix":        "stripe.",
				"ingest_token":        "ifwh_rawsecretvalue",
				"ingest_token_prefix": "ifwh_raws",
				"created_at":          "2026-03-28T00:00:00Z",
				"updated_at":          "2026-03-28T00:00:00Z",
			})
		}))
		defer cleanup()

		source, err := client.Webhooks().RotateIngestToken(context.Background(),
			RotateWebhookIngestTokenInput{ID: "whs_1"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if receivedPath != "/ironflow.v1.WebhookService/RotateWebhookIngestToken" {
			t.Errorf("path = %s", receivedPath)
		}
		if receivedBody["id"] != "whs_1" {
			t.Errorf("id = %v, want whs_1", receivedBody["id"])
		}
		// The whole point of the call: this field exists only in this response.
		if source.IngestToken != "ifwh_rawsecretvalue" {
			t.Errorf("IngestToken = %q, want the raw token", source.IngestToken)
		}
		if source.IngestTokenPrefix != "ifwh_raws" {
			t.Errorf("IngestTokenPrefix = %q", source.IngestTokenPrefix)
		}
	})

	t.Run("omits expected_updated_at when unset", func(t *testing.T) {
		var receivedBody map[string]any
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewDecoder(r.Body).Decode(&receivedBody)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"id": "whs_1"})
		}))
		defer cleanup()

		if _, err := client.Webhooks().RotateIngestToken(context.Background(),
			RotateWebhookIngestTokenInput{ID: "whs_1"}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Sending a zero timestamp would make every rotate look stale.
		if _, ok := receivedBody["expected_updated_at"]; ok {
			t.Error("expected_updated_at must be absent when the caller did not set it")
		}
	})

	t.Run("sends expected_updated_at as RFC3339 UTC", func(t *testing.T) {
		var receivedBody map[string]any
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewDecoder(r.Body).Decode(&receivedBody)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"id": "whs_1"})
		}))
		defer cleanup()

		at := time.Date(2026, 3, 28, 12, 0, 0, 0, time.FixedZone("EST", -5*3600))
		if _, err := client.Webhooks().RotateIngestToken(context.Background(),
			RotateWebhookIngestTokenInput{ID: "whs_1", ExpectedUpdatedAt: &at}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := receivedBody["expected_updated_at"]; got != "2026-03-28T17:00:00Z" {
			t.Errorf("expected_updated_at = %v, want the UTC form", got)
		}
	})

	t.Run("propagates a server error", func(t *testing.T) {
		client, cleanup := setupMockWebhookServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"code":"not_found","message":"webhook source not found"}`))
		}))
		defer cleanup()

		if _, err := client.Webhooks().RotateIngestToken(context.Background(),
			RotateWebhookIngestTokenInput{ID: "whs_missing"}); err == nil {
			t.Fatal("expected an error")
		}
	})
}
