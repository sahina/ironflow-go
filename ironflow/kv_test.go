package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// setupMockKVServer creates a mock HTTP server and returns a configured Client.
func setupMockKVServer(t *testing.T, handler http.Handler) (*Client, func()) {
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
// KVClient bucket operations
// ============================================================================

func TestKVCreateBucket(t *testing.T) {
	t.Run("success returns BucketInfo", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string
		var receivedBody map[string]any

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("failed to read request body: %v", err)
			}
			if err := json.Unmarshal(body, &receivedBody); err != nil {
				t.Errorf("failed to unmarshal request body: %v", err)
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(BucketInfo{
				Name:        "my-bucket",
				Description: "test bucket",
				TTLSeconds:  3600,
				Values:      0,
				Bytes:       0,
				History:     5,
				CreatedAt:   time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
			})
		}))
		defer cleanup()

		ctx := context.Background()
		info, err := client.KV().CreateBucket(ctx, BucketConfig{
			Name:        "my-bucket",
			Description: "test bucket",
			TTL:         time.Hour,
			History:     5,
		})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "POST" {
			t.Errorf("expected method POST, got %s", receivedMethod)
		}

		if receivedPath != "/api/v1/kv/buckets" {
			t.Errorf("expected path /api/v1/kv/buckets, got %s", receivedPath)
		}

		if receivedBody["name"] != "my-bucket" {
			t.Errorf("expected body name 'my-bucket', got %v", receivedBody["name"])
		}

		if receivedBody["description"] != "test bucket" {
			t.Errorf("expected body description 'test bucket', got %v", receivedBody["description"])
		}

		if info.Name != "my-bucket" {
			t.Errorf("expected info.Name 'my-bucket', got %s", info.Name)
		}

		if info.Description != "test bucket" {
			t.Errorf("expected info.Description 'test bucket', got %s", info.Description)
		}

		if info.TTLSeconds != 3600 {
			t.Errorf("expected info.TTLSeconds 3600, got %d", info.TTLSeconds)
		}

		if info.History != 5 {
			t.Errorf("expected info.History 5, got %d", info.History)
		}
	})

	t.Run("server error returns IronflowError", func(t *testing.T) {
		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "internal server error"})
		}))
		defer cleanup()

		ctx := context.Background()
		_, err := client.KV().CreateBucket(ctx, BucketConfig{Name: "fail-bucket"})

		if err == nil {
			t.Fatal("expected error for 500 response")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected *IronflowError, got %T", err)
		}

		if !ironflowErr.Retryable {
			t.Error("expected 500 error to be retryable")
		}
	})
}

func TestKVDeleteBucket(t *testing.T) {
	t.Run("success returns no error", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		ctx := context.Background()
		err := client.KV().DeleteBucket(ctx, "my-bucket")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "DELETE" {
			t.Errorf("expected method DELETE, got %s", receivedMethod)
		}

		if receivedPath != "/api/v1/kv/buckets/my-bucket" {
			t.Errorf("expected path /api/v1/kv/buckets/my-bucket, got %s", receivedPath)
		}
	})

	t.Run("404 returns error", func(t *testing.T) {
		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "bucket not found"})
		}))
		defer cleanup()

		ctx := context.Background()
		err := client.KV().DeleteBucket(ctx, "nonexistent")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected *IronflowError, got %T", err)
		}

		if ironflowErr.Retryable {
			t.Error("expected 404 error to NOT be retryable")
		}
	})
}

func TestKVListBuckets(t *testing.T) {
	t.Run("returns list of buckets", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]any{
				"buckets": []BucketInfo{
					{Name: "bucket-a", Values: 10, Bytes: 1024, History: 1},
					{Name: "bucket-b", Values: 20, Bytes: 2048, History: 3},
				},
				"count": 2,
			})
		}))
		defer cleanup()

		ctx := context.Background()
		buckets, err := client.KV().ListBuckets(ctx)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "GET" {
			t.Errorf("expected method GET, got %s", receivedMethod)
		}

		if receivedPath != "/api/v1/kv/buckets" {
			t.Errorf("expected path /api/v1/kv/buckets, got %s", receivedPath)
		}

		if len(buckets) != 2 {
			t.Fatalf("expected 2 buckets, got %d", len(buckets))
		}

		if buckets[0].Name != "bucket-a" {
			t.Errorf("expected first bucket name 'bucket-a', got %s", buckets[0].Name)
		}

		if buckets[1].Name != "bucket-b" {
			t.Errorf("expected second bucket name 'bucket-b', got %s", buckets[1].Name)
		}

		if buckets[0].Values != 10 {
			t.Errorf("expected first bucket values 10, got %d", buckets[0].Values)
		}
	})
}

func TestKVGetBucketInfo(t *testing.T) {
	t.Run("success returns BucketInfo", func(t *testing.T) {
		var receivedPath string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(BucketInfo{
				Name:       "my-bucket",
				Values:     42,
				Bytes:      8192,
				History:    10,
				TTLSeconds: 7200,
			})
		}))
		defer cleanup()

		ctx := context.Background()
		info, err := client.KV().GetBucketInfo(ctx, "my-bucket")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedPath != "/api/v1/kv/buckets/my-bucket" {
			t.Errorf("expected path /api/v1/kv/buckets/my-bucket, got %s", receivedPath)
		}

		if info.Name != "my-bucket" {
			t.Errorf("expected name 'my-bucket', got %s", info.Name)
		}

		if info.Values != 42 {
			t.Errorf("expected values 42, got %d", info.Values)
		}

		if info.TTLSeconds != 7200 {
			t.Errorf("expected ttl_seconds 7200, got %d", info.TTLSeconds)
		}
	})

	t.Run("404 returns error", func(t *testing.T) {
		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "bucket not found"})
		}))
		defer cleanup()

		ctx := context.Background()
		_, err := client.KV().GetBucketInfo(ctx, "missing")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected *IronflowError, got %T", err)
		}

		if ironflowErr.Retryable {
			t.Error("expected 404 error to NOT be retryable")
		}
	})
}

func TestKVCreateBucketWithConfig(t *testing.T) {
	t.Run("sends all config fields in request body", func(t *testing.T) {
		var receivedBody map[string]any

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("failed to read request body: %v", err)
			}
			if err := json.Unmarshal(body, &receivedBody); err != nil {
				t.Errorf("failed to unmarshal request body: %v", err)
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(BucketInfo{Name: "configured-bucket"})
		}))
		defer cleanup()

		ctx := context.Background()
		_, err := client.KV().CreateBucket(ctx, BucketConfig{
			Name:         "configured-bucket",
			Description:  "fully configured",
			TTL:          2 * time.Hour,
			MaxValueSize: 1024,
			MaxBytes:     1048576,
			History:      10,
		})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["name"] != "configured-bucket" {
			t.Errorf("expected name 'configured-bucket', got %v", receivedBody["name"])
		}

		if receivedBody["description"] != "fully configured" {
			t.Errorf("expected description 'fully configured', got %v", receivedBody["description"])
		}

		// TTL is sent as ttl_seconds (int64 of seconds)
		ttlSeconds, ok := receivedBody["ttl_seconds"].(float64)
		if !ok {
			t.Fatalf("expected ttl_seconds to be a number, got %T", receivedBody["ttl_seconds"])
		}
		if ttlSeconds != 7200 {
			t.Errorf("expected ttl_seconds 7200, got %v", ttlSeconds)
		}

		maxValueSize, ok := receivedBody["max_value_size"].(float64)
		if !ok {
			t.Fatalf("expected max_value_size to be a number, got %T", receivedBody["max_value_size"])
		}
		if maxValueSize != 1024 {
			t.Errorf("expected max_value_size 1024, got %v", maxValueSize)
		}

		maxBytes, ok := receivedBody["max_bytes"].(float64)
		if !ok {
			t.Fatalf("expected max_bytes to be a number, got %T", receivedBody["max_bytes"])
		}
		if maxBytes != 1048576 {
			t.Errorf("expected max_bytes 1048576, got %v", maxBytes)
		}

		history, ok := receivedBody["history"].(float64)
		if !ok {
			t.Fatalf("expected history to be a number, got %T", receivedBody["history"])
		}
		if history != 10 {
			t.Errorf("expected history 10, got %v", history)
		}
	})

	t.Run("omits zero-value optional fields", func(t *testing.T) {
		var receivedBody map[string]any

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("failed to read request body: %v", err)
			}
			if err := json.Unmarshal(body, &receivedBody); err != nil {
				t.Errorf("failed to unmarshal request body: %v", err)
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(BucketInfo{Name: "minimal-bucket"})
		}))
		defer cleanup()

		ctx := context.Background()
		_, err := client.KV().CreateBucket(ctx, BucketConfig{
			Name: "minimal-bucket",
		})

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedBody["name"] != "minimal-bucket" {
			t.Errorf("expected name 'minimal-bucket', got %v", receivedBody["name"])
		}

		// These optional fields should not be present when zero-valued
		if _, exists := receivedBody["description"]; exists {
			t.Errorf("expected description to be absent, got %v", receivedBody["description"])
		}
		if _, exists := receivedBody["ttl_seconds"]; exists {
			t.Errorf("expected ttl_seconds to be absent, got %v", receivedBody["ttl_seconds"])
		}
		if _, exists := receivedBody["max_value_size"]; exists {
			t.Errorf("expected max_value_size to be absent, got %v", receivedBody["max_value_size"])
		}
		if _, exists := receivedBody["max_bytes"]; exists {
			t.Errorf("expected max_bytes to be absent, got %v", receivedBody["max_bytes"])
		}
		if _, exists := receivedBody["history"]; exists {
			t.Errorf("expected history to be absent, got %v", receivedBody["history"])
		}
	})
}

// ============================================================================
// KVBucket key operations
// ============================================================================

func TestKVBucketGet(t *testing.T) {
	t.Run("success returns KVEntry", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(KVEntry{
				Key:       "my-key",
				Value:     []byte("hello world"),
				Revision:  3,
				CreatedAt: "2026-01-15T10:00:00Z",
				Operation: "put",
			})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		entry, err := bucket.Get(ctx, "my-key")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "GET" {
			t.Errorf("expected method GET, got %s", receivedMethod)
		}

		if receivedPath != "/api/v1/kv/buckets/test-bucket/keys/my-key" {
			t.Errorf("expected path /api/v1/kv/buckets/test-bucket/keys/my-key, got %s", receivedPath)
		}

		if entry.Key != "my-key" {
			t.Errorf("expected key 'my-key', got %s", entry.Key)
		}

		if entry.Revision != 3 {
			t.Errorf("expected revision 3, got %d", entry.Revision)
		}

		if entry.Operation != "put" {
			t.Errorf("expected operation 'put', got %s", entry.Operation)
		}
	})

	t.Run("404 returns error for key not found", func(t *testing.T) {
		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "key not found"})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		_, err := bucket.Get(ctx, "missing-key")

		if err == nil {
			t.Fatal("expected error for 404 response")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected *IronflowError, got %T", err)
		}

		if ironflowErr.Retryable {
			t.Error("expected 404 error to NOT be retryable")
		}
	})
}

func TestKVBucketPut(t *testing.T) {
	t.Run("success returns revision", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string
		var receivedContentType string
		var receivedBody []byte

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			receivedContentType = r.Header.Get("Content-Type")

			var err error
			receivedBody, err = io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("failed to read request body: %v", err)
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]uint64{"revision": 1})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		revision, err := bucket.Put(ctx, "my-key", []byte("hello world"))

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "PUT" {
			t.Errorf("expected method PUT, got %s", receivedMethod)
		}

		if receivedPath != "/api/v1/kv/buckets/test-bucket/keys/my-key" {
			t.Errorf("expected path /api/v1/kv/buckets/test-bucket/keys/my-key, got %s", receivedPath)
		}

		if receivedContentType != "application/octet-stream" {
			t.Errorf("expected Content-Type 'application/octet-stream', got %s", receivedContentType)
		}

		if string(receivedBody) != "hello world" {
			t.Errorf("expected body 'hello world', got %s", string(receivedBody))
		}

		if revision != 1 {
			t.Errorf("expected revision 1, got %d", revision)
		}
	})

	t.Run("does not set If-None-Match or If-Match headers", func(t *testing.T) {
		var receivedIfNoneMatch string
		var receivedIfMatch string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedIfNoneMatch = r.Header.Get("If-None-Match")
			receivedIfMatch = r.Header.Get("If-Match")

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]uint64{"revision": 1})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		_, err := bucket.Put(ctx, "my-key", []byte("data"))

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedIfNoneMatch != "" {
			t.Errorf("expected no If-None-Match header, got %s", receivedIfNoneMatch)
		}

		if receivedIfMatch != "" {
			t.Errorf("expected no If-Match header, got %s", receivedIfMatch)
		}
	})
}

func TestKVBucketCreate(t *testing.T) {
	t.Run("sends If-None-Match header", func(t *testing.T) {
		var receivedIfNoneMatch string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedIfNoneMatch = r.Header.Get("If-None-Match")

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]uint64{"revision": 1})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		revision, err := bucket.Create(ctx, "new-key", []byte("value"))

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedIfNoneMatch != "*" {
			t.Errorf("expected If-None-Match header '*', got '%s'", receivedIfNoneMatch)
		}

		if revision != 1 {
			t.Errorf("expected revision 1, got %d", revision)
		}
	})

	t.Run("412 conflict when key exists", func(t *testing.T) {
		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusPreconditionFailed)
			json.NewEncoder(w).Encode(map[string]string{"error": "key already exists"})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		_, err := bucket.Create(ctx, "existing-key", []byte("value"))

		if err == nil {
			t.Fatal("expected error for 412 response")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected *IronflowError, got %T", err)
		}

		if ironflowErr.Code != "HTTP_412" {
			t.Errorf("expected code 'HTTP_412', got '%s'", ironflowErr.Code)
		}

		if ironflowErr.Retryable {
			t.Error("expected 412 error to NOT be retryable")
		}
	})
}

func TestKVBucketUpdate(t *testing.T) {
	t.Run("sends If-Match header with revision", func(t *testing.T) {
		var receivedIfMatch string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedIfMatch = r.Header.Get("If-Match")

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]uint64{"revision": 6})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		revision, err := bucket.Update(ctx, "my-key", []byte("updated-value"), 5)

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedIfMatch != "5" {
			t.Errorf("expected If-Match header '5', got '%s'", receivedIfMatch)
		}

		if revision != 6 {
			t.Errorf("expected revision 6, got %d", revision)
		}
	})

	t.Run("412 on revision mismatch", func(t *testing.T) {
		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusPreconditionFailed)
			json.NewEncoder(w).Encode(map[string]string{"error": "revision mismatch"})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		_, err := bucket.Update(ctx, "my-key", []byte("value"), 3)

		if err == nil {
			t.Fatal("expected error for 412 response")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected *IronflowError, got %T", err)
		}

		if ironflowErr.Code != "HTTP_412" {
			t.Errorf("expected code 'HTTP_412', got '%s'", ironflowErr.Code)
		}
	})
}

func TestKVBucketDelete(t *testing.T) {
	t.Run("success uses DELETE method", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		err := bucket.Delete(ctx, "my-key")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "DELETE" {
			t.Errorf("expected method DELETE, got %s", receivedMethod)
		}

		if receivedPath != "/api/v1/kv/buckets/test-bucket/keys/my-key" {
			t.Errorf("expected path /api/v1/kv/buckets/test-bucket/keys/my-key, got %s", receivedPath)
		}
	})
}

func TestKVBucketPurge(t *testing.T) {
	t.Run("sends purge=true query parameter", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string
		var receivedQuery string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path
			receivedQuery = r.URL.RawQuery
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		err := bucket.Purge(ctx, "my-key")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "DELETE" {
			t.Errorf("expected method DELETE, got %s", receivedMethod)
		}

		if receivedPath != "/api/v1/kv/buckets/test-bucket/keys/my-key" {
			t.Errorf("expected path /api/v1/kv/buckets/test-bucket/keys/my-key, got %s", receivedPath)
		}

		if receivedQuery != "purge=true" {
			t.Errorf("expected query 'purge=true', got '%s'", receivedQuery)
		}
	})
}

func TestKVBucketListKeys(t *testing.T) {
	t.Run("returns list of keys", func(t *testing.T) {
		var receivedMethod string
		var receivedPath string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedMethod = r.Method
			receivedPath = r.URL.Path

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]any{
				"keys":  []string{"key-a", "key-b"},
				"count": 2,
			})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		keys, err := bucket.ListKeys(ctx, "")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedMethod != "GET" {
			t.Errorf("expected method GET, got %s", receivedMethod)
		}

		if receivedPath != "/api/v1/kv/buckets/test-bucket/keys" {
			t.Errorf("expected path /api/v1/kv/buckets/test-bucket/keys, got %s", receivedPath)
		}

		if len(keys) != 2 {
			t.Fatalf("expected 2 keys, got %d", len(keys))
		}

		if keys[0] != "key-a" {
			t.Errorf("expected first key 'key-a', got %s", keys[0])
		}

		if keys[1] != "key-b" {
			t.Errorf("expected second key 'key-b', got %s", keys[1])
		}
	})

	t.Run("includes filter parameter in URL", func(t *testing.T) {
		var receivedRawQuery string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedRawQuery = r.URL.RawQuery

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]any{
				"keys":  []string{"user.alice", "user.bob"},
				"count": 2,
			})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		keys, err := bucket.ListKeys(ctx, "user.*")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if !strings.Contains(receivedRawQuery, "filter=") {
			t.Errorf("expected query to contain 'filter=', got '%s'", receivedRawQuery)
		}

		// The filter value is URL-encoded, so user.* becomes user.%2A or user.*
		// depending on encoding. Check the decoded query parameter.
		if !strings.Contains(receivedRawQuery, "filter=user.") {
			t.Errorf("expected query to contain 'filter=user.', got '%s'", receivedRawQuery)
		}

		if len(keys) != 2 {
			t.Fatalf("expected 2 keys, got %d", len(keys))
		}
	})

	t.Run("no filter parameter when filter is empty", func(t *testing.T) {
		var receivedRawQuery string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedRawQuery = r.URL.RawQuery

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]any{
				"keys":  []string{},
				"count": 0,
			})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		_, err := bucket.ListKeys(ctx, "")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		if receivedRawQuery != "" {
			t.Errorf("expected no query parameters, got '%s'", receivedRawQuery)
		}
	})
}

func TestKVBucketURLEncoding(t *testing.T) {
	t.Run("bucket names with special characters are URL-encoded", func(t *testing.T) {
		var receivedRequestURI string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// RequestURI preserves the original encoded form sent by the client.
			receivedRequestURI = r.RequestURI

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(BucketInfo{Name: "my bucket/special"})
		}))
		defer cleanup()

		ctx := context.Background()

		// GetBucketInfo uses url.PathEscape on the bucket name
		_, err := client.KV().GetBucketInfo(ctx, "my bucket/special")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		// url.PathEscape encodes spaces as %20 and slashes as %2F
		expectedURI := "/api/v1/kv/buckets/my%20bucket%2Fspecial"
		if receivedRequestURI != expectedURI {
			t.Errorf("expected request URI %s, got %s", expectedURI, receivedRequestURI)
		}
	})

	t.Run("bucket names are URL-encoded in Bucket key operations", func(t *testing.T) {
		var receivedRequestURI string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// RequestURI preserves the original encoded form sent by the client.
			receivedRequestURI = r.RequestURI

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(KVEntry{
				Key:      "test-key",
				Value:    []byte("value"),
				Revision: 1,
			})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("my bucket")
		_, err := bucket.Get(ctx, "test-key")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		// Bucket name should be escaped: "my bucket" -> "my%20bucket"
		expectedURI := "/api/v1/kv/buckets/my%20bucket/keys/test-key"
		if receivedRequestURI != expectedURI {
			t.Errorf("expected request URI %s, got %s", expectedURI, receivedRequestURI)
		}
	})

	t.Run("keys with special characters are URL-encoded", func(t *testing.T) {
		// Keys are now encoded with url.PathEscape so that special characters
		// (spaces, slashes, etc.) are safely embedded in the URL path.
		var receivedRequestURI string

		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Use RequestURI to see the raw (unescaped) path as sent by the client.
			receivedRequestURI = r.RequestURI

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(KVEntry{
				Key:      "my/key",
				Value:    []byte("value"),
				Revision: 1,
			})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		_, err := bucket.Get(ctx, "my/key")

		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		// The key "my/key" is encoded as "my%2Fkey" by url.PathEscape.
		expectedURI := "/api/v1/kv/buckets/test-bucket/keys/my%2Fkey"
		if receivedRequestURI != expectedURI {
			t.Errorf("expected encoded key URI %s, got %s", expectedURI, receivedRequestURI)
		}
	})
}

func TestKVErrorRetryable(t *testing.T) {
	statusCases := []struct {
		name       string
		statusCode int
		retryable  bool
	}{
		{"400 Bad Request is not retryable", 400, false},
		{"401 Unauthorized is not retryable", 401, false},
		{"403 Forbidden is not retryable", 403, false},
		{"404 Not Found is not retryable", 404, false},
		{"409 Conflict is not retryable", 409, false},
		{"412 Precondition Failed is not retryable", 412, false},
		{"422 Unprocessable Entity is not retryable", 422, false},
		{"429 Too Many Requests is not retryable", 429, false},
		{"500 Internal Server Error is retryable", 500, true},
		{"502 Bad Gateway is retryable", 502, true},
		{"503 Service Unavailable is retryable", 503, true},
		{"504 Gateway Timeout is retryable", 504, true},
	}

	for _, tc := range statusCases {
		t.Run(tc.name, func(t *testing.T) {
			client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.statusCode)
				json.NewEncoder(w).Encode(map[string]string{
					"error": fmt.Sprintf("error %d", tc.statusCode),
				})
			}))
			defer cleanup()

			ctx := context.Background()
			// Test using a bucket operation (Get) that goes through restRequest
			bucket := client.KV().Bucket("test-bucket")
			_, err := bucket.Get(ctx, "test-key")

			if err == nil {
				t.Fatal("expected error")
			}

			ironflowErr, ok := err.(*IronflowError)
			if !ok {
				t.Fatalf("expected *IronflowError, got %T", err)
			}

			if ironflowErr.Retryable != tc.retryable {
				t.Errorf("expected Retryable=%v for status %d, got %v", tc.retryable, tc.statusCode, ironflowErr.Retryable)
			}
		})
	}

	t.Run("put operation 5xx errors are retryable", func(t *testing.T) {
		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "internal error"})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		_, err := bucket.Put(ctx, "key", []byte("value"))

		if err == nil {
			t.Fatal("expected error")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected *IronflowError, got %T", err)
		}

		if !ironflowErr.Retryable {
			t.Error("expected 500 error from Put to be retryable")
		}
	})

	t.Run("put operation 4xx errors are not retryable", func(t *testing.T) {
		client, cleanup := setupMockKVServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "bad request"})
		}))
		defer cleanup()

		ctx := context.Background()
		bucket := client.KV().Bucket("test-bucket")
		_, err := bucket.Put(ctx, "key", []byte("value"))

		if err == nil {
			t.Fatal("expected error")
		}

		ironflowErr, ok := err.(*IronflowError)
		if !ok {
			t.Fatalf("expected *IronflowError, got %T", err)
		}

		if ironflowErr.Retryable {
			t.Error("expected 400 error from Put to NOT be retryable")
		}
	})
}

// ============================================================================
// KVBucket.Watch tests
// ============================================================================

// wsUpgrader is a permissive WebSocket upgrader for tests.
var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// setupMockWatchServer creates a test server that upgrades to WebSocket and
// calls handleFn with the connection. Returns the configured Client and
// a cleanup function.
func setupMockWatchServer(t *testing.T, bucket string, handleFn func(conn *websocket.Conn)) (*Client, func()) {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/kv/buckets/"+bucket+"/watch", func(w http.ResponseWriter, r *http.Request) {
		conn, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("websocket upgrade failed: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		handleFn(conn)
	})

	server := httptest.NewServer(mux)
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

// TestKVWatch_BasicUpdate verifies that an update event is delivered to the
// OnUpdate callback.
func TestKVWatch_BasicUpdate(t *testing.T) {
	const bucketName = "watch-bucket"

	client, cleanup := setupMockWatchServer(t, bucketName, func(conn *websocket.Conn) {
		// Send one kv_update message then close.
		msg := map[string]any{
			"type":      "kv_update",
			"key":       "hello",
			"value":     "d29ybGQ=", // base64("world")
			"revision":  float64(7),
			"operation": "put",
			"bucket":    bucketName,
		}
		if err := conn.WriteJSON(msg); err != nil {
			t.Errorf("server write error: %v", err)
		}
		// Wait a bit so the client can read the message before we close.
		time.Sleep(200 * time.Millisecond)
		_ = conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	})
	defer cleanup()

	received := make(chan KVWatchEvent, 1)
	closed := make(chan struct{})

	ctx := context.Background()
	bucket := client.KV().Bucket(bucketName)
	watcher, err := bucket.Watch(ctx, KVWatchCallbacks{
		OnUpdate: func(e KVWatchEvent) {
			received <- e
		},
		OnClose: func() {
			close(closed)
		},
	})
	if err != nil {
		t.Fatalf("Watch returned error: %v", err)
	}
	defer watcher.Stop()

	select {
	case event := <-received:
		if event.Type != "kv_update" {
			t.Errorf("expected type kv_update, got %q", event.Type)
		}
		if event.Key != "hello" {
			t.Errorf("expected key hello, got %q", event.Key)
		}
		if event.Revision != 7 {
			t.Errorf("expected revision 7, got %d", event.Revision)
		}
		if event.Operation != "put" {
			t.Errorf("expected operation put, got %q", event.Operation)
		}
		if event.Bucket != bucketName {
			t.Errorf("expected bucket %q, got %q", bucketName, event.Bucket)
		}
		if string(event.Value) != "world" {
			t.Errorf("expected decoded value %q, got %q", "world", string(event.Value))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}

	// Wait for OnClose to be called.
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnClose")
	}
}

// TestKVWatch_Stop verifies that calling Stop() shuts down the goroutine cleanly.
func TestKVWatch_Stop(t *testing.T) {
	const bucketName = "stop-bucket"

	// Server stays open until the client closes.
	serverGotClose := make(chan struct{})
	client, cleanup := setupMockWatchServer(t, bucketName, func(conn *websocket.Conn) {
		defer close(serverGotClose)
		// Drain messages until client disconnects.
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	})
	defer cleanup()

	ctx := context.Background()
	bucket := client.KV().Bucket(bucketName)
	watcher, err := bucket.Watch(ctx, KVWatchCallbacks{})
	if err != nil {
		t.Fatalf("Watch returned error: %v", err)
	}

	// Stop should return promptly.
	done := make(chan struct{})
	go func() {
		watcher.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Good — Stop returned.
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() did not return within 3s")
	}

	// Server should have seen the close.
	select {
	case <-serverGotClose:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not observe client close within 3s")
	}
}

// TestKVWatch_ErrorMessage verifies that an error message from the server
// triggers the OnError callback.
func TestKVWatch_ErrorMessage(t *testing.T) {
	const bucketName = "err-bucket"

	client, cleanup := setupMockWatchServer(t, bucketName, func(conn *websocket.Conn) {
		_ = conn.WriteJSON(map[string]string{
			"type":    "error",
			"message": "bucket not found",
		})
		time.Sleep(200 * time.Millisecond)
	})
	defer cleanup()

	errCh := make(chan error, 1)

	ctx := context.Background()
	bucket := client.KV().Bucket(bucketName)
	watcher, err := bucket.Watch(ctx, KVWatchCallbacks{
		OnError: func(e error) {
			errCh <- e
		},
	})
	if err != nil {
		t.Fatalf("Watch returned error: %v", err)
	}
	defer watcher.Stop()

	select {
	case watchErr := <-errCh:
		if !strings.Contains(watchErr.Error(), "bucket not found") {
			t.Errorf("expected 'bucket not found' in error, got: %v", watchErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnError")
	}
}

// TestKVWatch_KeyFilter verifies that the ?key= query parameter is included in
// the WebSocket URL when WithWatchKey is used.
func TestKVWatch_KeyFilter(t *testing.T) {
	const bucketName = "filter-bucket"
	const watchKey = "my.special.key"

	receivedKey := make(chan string, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/kv/buckets/"+bucketName+"/watch", func(w http.ResponseWriter, r *http.Request) {
		receivedKey <- r.URL.Query().Get("key")
		conn, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("websocket upgrade failed: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		// Wait for client to close.
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := &Client{
		serverURL:  server.URL,
		httpClient: server.Client(),
		retryConfig: &ClientRetryConfig{
			MaxAttempts: 1,
		},
		logger: NewNoopLogger(),
	}

	ctx := context.Background()
	bucket := client.KV().Bucket(bucketName)
	watcher, err := bucket.Watch(ctx, KVWatchCallbacks{}, WithWatchKey(watchKey))
	if err != nil {
		t.Fatalf("Watch returned error: %v", err)
	}
	defer watcher.Stop()

	select {
	case key := <-receivedKey:
		if key != watchKey {
			t.Errorf("expected server to receive key=%q, got %q", watchKey, key)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for server to receive request")
	}
}

// TestKVWatch_StopIdempotent verifies calling Stop() twice does not panic.
func TestKVWatch_StopIdempotent(t *testing.T) {
	const bucketName = "idempotent-bucket"

	client, cleanup := setupMockWatchServer(t, bucketName, func(conn *websocket.Conn) {
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	})
	defer cleanup()

	ctx := context.Background()
	bucket := client.KV().Bucket(bucketName)
	watcher, err := bucket.Watch(ctx, KVWatchCallbacks{})
	if err != nil {
		t.Fatalf("Watch returned error: %v", err)
	}

	watcher.Stop()
	// Second Stop must not panic.
	watcher.Stop()
}
