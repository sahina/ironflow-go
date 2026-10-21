// An external test package: agent imports ironflow, so a test in package
// ironflow cannot import agent without a cycle.
package ironflow_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sahina/ironflow-go/ironflow"
	"github.com/sahina/ironflow-go/ironflow/agent"
)

// TestLiveReadinessAgentTools covers the agent-tools group (#3003). Tool
// registration is proven by `make test-conformance`.
func TestLiveReadinessAgentTools(t *testing.T) {
	if os.Getenv("IRONFLOW_READINESS_SEED") == "" {
		t.Skip("IRONFLOW_READINESS_SEED not set; run `make test-readiness-live`")
	}
	serverURL, apiKey := os.Getenv("IRONFLOW_TEST_SERVER"), os.Getenv("IRONFLOW_TEST_API_KEY")
	client := ironflow.NewClient(ironflow.ClientConfig{ServerURL: serverURL, APIKey: apiKey})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The server checks a callback URL's scheme and, for an IP literal, its range.
	// A hostname passes, and nothing here is dispatched.
	name := fmt.Sprintf("gosdk%d", time.Now().UnixNano())
	const schema = `{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`
	handle, err := agent.ExposeMcp(agent.ExposeMcpConfig{
		Name: name, Version: "1.0.0", ServerURL: serverURL, APIKey: apiKey,
		CallbackURL: "https://tools.example.test/api/ironflow" + agent.DispatchPath,
		Tools: []agent.McpToolDef{
			{Name: "lookup", Description: "Look up an order", InputSchemaJSON: schema},
			{Name: "refund", Description: "Refund an order", InputSchemaJSON: schema},
		},
	})
	if err != nil {
		t.Fatalf("ExposeMcp: %v", err)
	}
	defer func() { _ = handle.Unregister() }()

	// The registry is shared with the other SDKs' tools, so look only at this agent's.
	mine := func() map[string]ironflow.VisibleAgentTool {
		t.Helper()
		list, err := client.ListAgentTools(ctx, "")
		if err != nil {
			t.Fatalf("ListAgentTools: %v", err)
		}
		out := map[string]ironflow.VisibleAgentTool{}
		for _, tool := range list.Tools {
			if strings.HasPrefix(tool.QualifiedName, name+".") {
				out[tool.QualifiedName] = tool
			}
		}
		return out
	}

	t.Run("agent-tool-list", func(t *testing.T) {
		got := mine()
		if len(got) != 2 {
			t.Fatalf("ListAgentTools has %v for %s, want lookup and refund", got, name)
		}
		lookup := got[name+".lookup"]
		if lookup.Description != "Look up an order" || !agentToolSchemaEqual(lookup.InputSchemaJSON, schema) || len(lookup.RequiredScopes) != 0 {
			t.Fatalf("lookup = %+v, want its description and input schema, with no scopes", lookup)
		}
		if _, ok := got[name+".refund"]; !ok {
			t.Fatalf("ListAgentTools has %v, want %s.refund", got, name)
		}
	})

	t.Run("agent-tool-unregister", func(t *testing.T) {
		if err := handle.Unregister(); err != nil {
			t.Fatalf("Unregister: %v", err)
		}
		if got := mine(); len(got) != 0 {
			t.Fatalf("ListAgentTools still has %v after Unregister", got)
		}
	})
}

func agentToolSchemaEqual(a, b string) bool {
	var x, y any
	return json.Unmarshal([]byte(a), &x) == nil && json.Unmarshal([]byte(b), &y) == nil && reflect.DeepEqual(x, y)
}
