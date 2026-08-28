package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListAgentTools(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/ironflow.v1.AgentToolsService/ListTools" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"tools": []map[string]any{{
				"qualified_name": "docs.search", "description": "Search docs",
				"input_schema_json": "{}", "required_scopes": []string{"docs:read"},
			}},
			"next_cursor": "next",
		})
	}))
	defer server.Close()

	result, err := newAuthTestClient(server).ListAgentTools(context.Background(), "cursor")
	if err != nil || len(result.Tools) != 1 || result.Tools[0].QualifiedName != "docs.search" || result.NextCursor != "next" {
		t.Fatalf("ListAgentTools = %#v, %v", result, err)
	}
}

func TestListAuditEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/audit" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.URL.Query().Get("run_id") != "run-1" || r.URL.Query().Get("limit") != "5" {
			t.Fatalf("query = %s", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"events": []map[string]any{{
				"id": "audit-1", "run_id": "run-1", "function_id": "fn-1",
				"event_type": "run.completed", "payload": map[string]any{}, "created_at": "now",
			}},
			"total_count": 1,
			"next_cursor": "next",
		})
	}))
	defer server.Close()

	result, err := newAuthTestClient(server).ListAuditEvents(context.Background(), ListAuditEventsOpts{RunID: "run-1", Limit: 5})
	if err != nil || len(result.Events) != 1 || result.Events[0].RunID != "run-1" || result.NextCursor != "next" {
		t.Fatalf("ListAuditEvents = %#v, %v", result, err)
	}
}
