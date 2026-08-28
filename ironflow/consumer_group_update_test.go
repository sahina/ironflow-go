package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUpdateConsumerGroup(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ironflow.v1.PubSubService/UpdateConsumerGroup" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(`{"id":"cg-1","namespace":"default","name":"orders","pattern":"order.*","ackMode":"ACK_MODE_MANUAL","backpressure":"BACKPRESSURE_MODE_BUFFER","maxInflight":0,"status":"CONSUMER_GROUP_STATUS_PAUSED","createdAt":"2026-08-27T12:00:00Z","updatedAt":"2026-08-27T12:00:00Z"}`))
	}))
	defer server.Close()
	client := &Client{serverURL: server.URL, httpClient: server.Client(), retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}
	zero := 0
	paused := ConsumerGroupStatusPaused

	group, err := client.UpdateConsumerGroup(context.Background(), "orders", UpdateConsumerGroupInput{
		MaxInflight: &zero,
		Status:      &paused,
	})
	if err != nil || group.Status != ConsumerGroupStatusPaused || group.MaxInflight != 0 {
		t.Fatalf("UpdateConsumerGroup = %#v, %v", group, err)
	}
	requestGroup := body["group"].(map[string]any)
	if requestGroup["max_inflight"] != float64(0) || requestGroup["status"] != "CONSUMER_GROUP_STATUS_PAUSED" {
		t.Fatalf("group request = %#v", requestGroup)
	}
	mask := body["update_mask"].(map[string]any)["paths"].([]any)
	if len(mask) != 2 || mask[0] != "max_inflight" || mask[1] != "status" {
		t.Fatalf("update mask = %#v", mask)
	}
}

func TestUpdateConsumerGroupRejectsEmptyInput(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	client := &Client{serverURL: server.URL, httpClient: server.Client(), retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}

	_, err := client.UpdateConsumerGroup(context.Background(), "orders", UpdateConsumerGroupInput{})
	if err == nil || err.Error() != "consumer group update requires at least one field" {
		t.Fatalf("UpdateConsumerGroup error = %v", err)
	}
	if called {
		t.Fatal("UpdateConsumerGroup sent an HTTP request for empty input")
	}
}

func TestListConsumerGroupsFollowsPagination(t *testing.T) {
	requestBodies := make([]map[string]any, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		requestBodies = append(requestBodies, body)
		if len(requestBodies) == 1 {
			_, _ = w.Write([]byte(`{"groups":[{"id":"cg-1","namespace":"default","name":"orders","pattern":"order.*"}],"nextCursor":"cg-page-2"}`))
			return
		}
		_, _ = w.Write([]byte(`{"groups":[{"id":"cg-2","namespace":"default","name":"payments","pattern":"payment.*"}]}`))
	}))
	defer server.Close()
	client := &Client{serverURL: server.URL, httpClient: server.Client(), retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}

	groups, err := client.ListConsumerGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 || groups[0].Name != "orders" || groups[1].Name != "payments" {
		t.Fatalf("ListConsumerGroups = %#v", groups)
	}
	if len(requestBodies) != 2 {
		t.Fatalf("request count = %d", len(requestBodies))
	}
	if _, ok := requestBodies[0]["cursor"]; ok {
		t.Fatalf("first request unexpectedly contains cursor: %#v", requestBodies[0])
	}
	if requestBodies[1]["cursor"] != "cg-page-2" {
		t.Fatalf("second request cursor = %#v", requestBodies[1]["cursor"])
	}
}
