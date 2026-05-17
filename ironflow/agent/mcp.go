// Package agent — exposeMcp() runtime path.
//
// Boot-time flow (mirrors @ironflow/node/agent S6):
//
//  1. Validate caller config (tools non-empty, callbackUrl + serverUrl +
//     apiKey present, no duplicate tool names, every tool carries a
//     JSON Schema).
//  2. POST `/ironflow.v1.AgentToolsService/RegisterTool` with
//     {agentName, callbackUrl, tools[]}.
//  3. Stash the returned HMAC secret + def closures in the local
//     registry so the dispatch handler (dispatch.go) can validate
//     inbound calls.
//
// Unregister flow:
//
//  1. Mark handle so the second call is a no-op.
//  2. Drop local registry entries (always — even on transport error)
//     so handler closures stop receiving dispatches.
//  3. POST `/ironflow.v1.AgentToolsService/UnregisterTool`.
//
// Authoritative authorization stays server-side. McpToolDef.Scopes are
// advisory hints to MCP clients; the server enforces api_key.tool_scopes
// superset against ToolDef.required_scopes.
//
// Mount pattern. Unlike the JS SDK (where serve() embeds the dispatch
// route automatically), Go callers wire the dispatch handler onto their
// own mux because agent imports ironflow, not the other way around. Use
// a subtree pattern (trailing slash) for Serve so prefixed paths match,
// and http.StripPrefix on DispatchHandler so it sees DispatchPath
// verbatim (DispatchHandler enforces exact-equality routing):
//
//	mux := http.NewServeMux()
//	mux.Handle("/api/ironflow/", ironflow.Serve(cfg))
//	mux.Handle("/api/ironflow"+agent.DispatchPath,
//	    http.StripPrefix("/api/ironflow", agent.DispatchHandler()))
//	// CallbackURL must end with the dispatch path:
//	agent.ExposeMcp(agent.ExposeMcpConfig{
//	    CallbackURL: "https://app.example.com/api/ironflow"+agent.DispatchPath,
//	    ...
//	})

package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sahina/ironflow-go/ironflow"
)

const (
	registerToolPath   = "/ironflow.v1.AgentToolsService/RegisterTool"
	unregisterToolPath = "/ironflow.v1.AgentToolsService/UnregisterTool"
)

// httpDoer is the subset of *http.Client used by ExposeMcp + Unregister
// so tests can inject a stub Doer.
type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// httpClientTimeout bounds Register/Unregister POSTs so a hung Ironflow
// server cannot block the SDK boot path indefinitely.
const httpClientTimeout = 30 * time.Second

// invalidResponsePreviewBytes caps how much of an unparseable response
// body we include in the error message — keeps multi-MB server bodies
// out of caller logs.
const invalidResponsePreviewBytes = 256

// maxResponseBodyBytes caps the RegisterTool/UnregisterTool response
// body size. Server is trusted but a misconfigured proxy could stream
// arbitrarily large bodies; cap turns it into an InvalidResponse error.
const maxResponseBodyBytes = 1 << 20

var (
	httpClientOnce sync.Once
	httpClient     httpDoer
)

func getHTTPClient() httpDoer {
	httpClientOnce.Do(func() {
		httpClient = &http.Client{Timeout: httpClientTimeout}
	})
	return httpClient
}

// ExposeMcp registers the supplied MCP tool definitions with an
// Ironflow server. On success the returned handle exposes the
// qualified tool names + an Unregister() method.
//
// Returns an *IronflowError with one of:
//
//	AGENT_MCP_NO_TOOLS, AGENT_MCP_MISSING_CALLBACK_URL,
//	AGENT_MCP_MISSING_SERVER_URL, AGENT_MCP_MISSING_API_KEY,
//	AGENT_MCP_DUPLICATE_TOOL, AGENT_MCP_MISSING_SCHEMA,
//	AGENT_MCP_TRANSPORT_ERROR, AGENT_MCP_INVALID_RESPONSE.
func ExposeMcp(cfg ExposeMcpConfig) (*ExposeMcpHandle, error) {
	if len(cfg.Tools) == 0 {
		return nil, &ironflow.IronflowError{
			Message:   "ExposeMcp requires at least one tool",
			Code:      CodeMcpNoTools,
			Retryable: false,
		}
	}
	if cfg.CallbackURL == "" {
		return nil, &ironflow.IronflowError{
			Message:   "ExposeMcp requires CallbackURL pointing at your Serve() mount",
			Code:      CodeMcpMissingCallbackURL,
			Retryable: false,
		}
	}

	serverURL := cfg.ServerURL
	if serverURL == "" {
		serverURL = os.Getenv("IRONFLOW_URL")
	}
	if serverURL == "" {
		serverURL = os.Getenv("IRONFLOW_SERVER_URL")
	}
	if serverURL == "" {
		return nil, &ironflow.IronflowError{
			Message:   "ExposeMcp requires ServerURL (or IRONFLOW_URL / IRONFLOW_SERVER_URL env)",
			Code:      CodeMcpMissingServerURL,
			Retryable: false,
		}
	}

	apiKey := cfg.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("IRONFLOW_API_KEY")
	}
	if apiKey == "" {
		return nil, &ironflow.IronflowError{
			Message:   "ExposeMcp requires APIKey (or IRONFLOW_API_KEY env) with the agent:tools:register action",
			Code:      CodeMcpMissingAPIKey,
			Retryable: false,
		}
	}

	seen := make(map[string]struct{}, len(cfg.Tools))
	toolPayload := make([]toolDefJSON, 0, len(cfg.Tools))
	for _, def := range cfg.Tools {
		if _, dup := seen[def.Name]; dup {
			return nil, &ironflow.IronflowError{
				Message:   fmt.Sprintf("ExposeMcp received duplicate tool name %q", def.Name),
				Code:      CodeMcpDuplicateTool,
				Retryable: false,
				Details:   map[string]any{"toolName": def.Name},
			}
		}
		seen[def.Name] = struct{}{}
		if def.InputSchemaJSON == "" {
			return nil, &ironflow.IronflowError{
				Message:   fmt.Sprintf("ExposeMcp tool %q missing InputSchemaJSON", def.Name),
				Code:      CodeMcpMissingSchema,
				Retryable: false,
				Details:   map[string]any{"toolName": def.Name},
			}
		}
		toolPayload = append(toolPayload, toToolDefJSON(def))
	}

	body := registerToolRequestJSON{
		AgentName:   cfg.Name,
		CallbackURL: cfg.CallbackURL,
		Tools:       toolPayload,
	}

	respBody, err := postJSON(serverURL, registerToolPath, apiKey, body)
	if err != nil {
		return nil, err
	}
	var decoded registerToolResponseJSON
	if err := decodeJSON(respBody, &decoded); err != nil {
		return nil, err
	}
	if decoded.HMACSecret == "" || len(decoded.RegisteredToolNames) == 0 {
		return nil, &ironflow.IronflowError{
			Message:   "RegisterTool response missing hmacSecret or registeredToolNames",
			Code:      CodeMcpInvalidResponse,
			Retryable: false,
		}
	}

	for _, def := range cfg.Tools {
		qualifiedName := cfg.Name + "." + def.Name
		registerLocal(RegisteredTool{
			AgentName:     cfg.Name,
			QualifiedName: qualifiedName,
			HMACSecret:    decoded.HMACSecret,
			Def:           def,
		})
	}

	return &ExposeMcpHandle{
		Name:      cfg.Name,
		ToolCount: len(decoded.RegisteredToolNames),
		Status:    "active",
		ToolNames: decoded.RegisteredToolNames,
		agentName: cfg.Name,
		serverURL: serverURL,
		apiKey:    apiKey,
	}, nil
}

// Unregister removes every tool registered under this handle's agent
// name. The local registry is cleared even when the server-side call
// fails so handler closures stop receiving dispatches. The second call
// is a no-op (idempotent).
func (h *ExposeMcpHandle) Unregister() error {
	if h == nil {
		return nil
	}
	h.unregMu.Lock()
	if h.unregistered {
		h.unregMu.Unlock()
		return nil
	}
	h.unregistered = true
	h.unregMu.Unlock()

	unregisterLocal(h.agentName)

	if _, err := postJSON(h.serverURL, unregisterToolPath, h.apiKey, unregisterToolRequestJSON{
		AgentName: h.agentName,
	}); err != nil {
		return &ironflow.IronflowError{
			Message:   "unregister failed: " + err.Error(),
			Code:      CodeMcpUnregisterFailed,
			Retryable: false,
			Cause:     err,
		}
	}
	return nil
}

type toolDefJSON struct {
	Name            string   `json:"name"`
	Description     string   `json:"description"`
	InputSchemaJSON string   `json:"inputSchemaJson"`
	RequiredScopes  []string `json:"requiredScopes"`
	TimeoutMS       uint32   `json:"timeoutMs"`
}

func toToolDefJSON(def McpToolDef) toolDefJSON {
	scopes := def.Scopes
	if scopes == nil {
		scopes = []string{}
	}
	return toolDefJSON{
		Name:            def.Name,
		Description:     def.Description,
		InputSchemaJSON: def.InputSchemaJSON,
		RequiredScopes:  scopes,
		TimeoutMS:       def.TimeoutMS,
	}
}

type registerToolRequestJSON struct {
	AgentName   string        `json:"agentName"`
	CallbackURL string        `json:"callbackUrl"`
	Tools       []toolDefJSON `json:"tools"`
}

type registerToolResponseJSON struct {
	HMACSecret          string   `json:"hmacSecret"`
	RegisteredToolNames []string `json:"registeredToolNames"`
}

type unregisterToolRequestJSON struct {
	AgentName string `json:"agentName"`
}

// postJSON marshals body, POSTs to {serverURL}{path}, reads + closes
// the response, and returns the bytes. 4xx/5xx are mapped to a
// transport-level *IronflowError; the response body is closed before
// returning in every branch.
func postJSON(serverURL, path, apiKey string, body any) ([]byte, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, &ironflow.IronflowError{
			Message:   "failed to marshal request: " + err.Error(),
			Code:      CodeMcpTransportError,
			Retryable: false,
			Cause:     err,
		}
	}
	url := strings.TrimRight(serverURL, "/") + path
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return nil, &ironflow.IronflowError{
			Message:   "failed to build request: " + err.Error(),
			Code:      CodeMcpTransportError,
			Retryable: false,
			Cause:     err,
		}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := getHTTPClient().Do(req)
	if err != nil {
		return nil, &ironflow.IronflowError{
			Message:   fmt.Sprintf("POST %s failed: %v", path, err),
			Code:      CodeMcpTransportError,
			Retryable: false,
			Cause:     err,
		}
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	if readErr != nil {
		return nil, &ironflow.IronflowError{
			Message:   "read response: " + readErr.Error(),
			Code:      CodeMcpInvalidResponse,
			Retryable: false,
			Cause:     readErr,
		}
	}

	if resp.StatusCode >= 400 {
		msg := fmt.Sprintf("POST %s failed: HTTP %d", path, resp.StatusCode)
		if len(respBody) > 0 {
			msg += " — " + string(respBody)
		}
		return nil, &ironflow.IronflowError{
			Message:   msg,
			Code:      CodeMcpTransportError,
			Retryable: false,
		}
	}
	return respBody, nil
}

// decodeJSON unmarshals respBody into `into`. An empty body is a
// no-op — callers verify required fields after unmarshal so a zero
// struct surfaces as CodeMcpInvalidResponse downstream (UnregisterTool
// callers also tolerate empty bodies).
func decodeJSON(respBody []byte, into any) error {
	if len(respBody) == 0 {
		return nil
	}
	if err := json.Unmarshal(respBody, into); err != nil {
		preview := string(respBody)
		if len(preview) > invalidResponsePreviewBytes {
			preview = preview[:invalidResponsePreviewBytes]
		}
		return &ironflow.IronflowError{
			Message:   "response body is not valid JSON: " + preview,
			Code:      CodeMcpInvalidResponse,
			Retryable: false,
			Cause:     err,
		}
	}
	return nil
}
