package ironflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestEventReadMethods(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.RequestURI())
		switch r.URL.Path {
		case "/api/v1/events":
			_, _ = w.Write([]byte(`{"events":[{"id":"evt-1","name":"order.placed","timestamp":"2026-08-27T12:00:00.123Z","data":{"id":"1"},"source":"sdk","processed":true,"created_at":"2026-08-27T12:00:00.123Z"}],"count":1,"limit":25,"next_cursor":"next","has_next":true,"has_prev":false,"approx_total":10}`))
		case "/api/v1/events/names":
			_, _ = w.Write([]byte(`{"names":[{"name":"order.placed","count":8}],"scanned":8,"truncated":false,"scan_cap":10000}`))
		case "/api/v1/events/evt%2F1", "/api/v1/events/evt/1":
			_, _ = w.Write([]byte(`{"id":"evt/1","name":"order.placed","timestamp":"2026-08-27T12:00:00.123Z","source":"sdk","processed":true,"created_at":"2026-08-27T12:00:00.123Z"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := &Client{serverURL: server.URL, httpClient: server.Client(), retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}
	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	page, err := client.ListEvents(context.Background(), ListEventsOptions{
		Names: []string{"order.placed", "order.shipped"}, Sources: []string{"sdk"}, Since: &since, Limit: 25, Cursor: "cur",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].ID != "evt-1" || !page.HasNext || page.NextCursor != "next" {
		t.Fatalf("ListEvents = %#v", page)
	}
	event, err := client.GetEvent(context.Background(), "evt/1")
	if err != nil || event.ID != "evt/1" {
		t.Fatalf("GetEvent = %#v, %v", event, err)
	}
	names, err := client.ListEventNames(context.Background(), ListEventNamesOptions{Sources: []string{"sdk"}, Since: &since})
	if err != nil || len(names.Names) != 1 || names.ScanCap != 10000 {
		t.Fatalf("ListEventNames = %#v, %v", names, err)
	}
	if len(paths) != 3 {
		t.Fatalf("paths = %#v", paths)
	}
	if paths[0] != "/api/v1/events?cursor=cur&limit=25&names=order.placed%2Corder.shipped&since=2026-08-01T00%3A00%3A00Z&source=sdk" {
		t.Errorf("ListEvents path = %q", paths[0])
	}
	if paths[2] != "/api/v1/events/names?since=2026-08-01T00%3A00%3A00Z&source=sdk" {
		t.Errorf("ListEventNames path = %q", paths[2])
	}
}
