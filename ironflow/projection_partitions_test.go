package ironflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListProjectionPartitions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RequestURI() != "/api/v1/projections/orders/partitions?limit=25&q=cust" {
			t.Errorf("URI = %q", r.URL.RequestURI())
		}
		_, _ = w.Write([]byte(`{"partitions":["cust-1","cust-2"],"returned":2}`))
	}))
	defer server.Close()
	client := &Client{serverURL: server.URL, httpClient: server.Client(), retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}

	result, err := client.ListProjectionPartitions(context.Background(), "orders", ListProjectionPartitionsOptions{Query: "cust", Limit: 25})
	if err != nil || result.Returned != 2 || len(result.Partitions) != 2 {
		t.Fatalf("ListProjectionPartitions = %#v, %v", result, err)
	}
}
