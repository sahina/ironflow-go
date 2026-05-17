package agent

import (
	"github.com/sahina/ironflow-go/ironflow"
)

// Error codes mirror @ironflow/node/agent so cross-SDK consumers can
// match them identically. These are stable error-classification
// constants, not credentials.
//
//nolint:gosec // G101 false positive: error codes, not secrets.
const (
	CodeMaxTurnsExceeded         = "AGENT_MAX_TURNS_EXCEEDED"
	CodeToolValidation           = "AGENT_TOOL_VALIDATION"
	CodeDuplicateTool            = "AGENT_DUPLICATE_TOOL"
	CodeToolNotFound             = "AGENT_TOOL_NOT_FOUND"
	CodeMemoryUnconfigured       = "AGENT_MEMORY_UNCONFIGURED"
	CodeMemoryNoBackend          = "AGENT_MEMORY_NO_BACKEND"
	CodeMemoryProjectionRequired = "AGENT_MEMORY_PROJECTION_REQUIRED"
	CodeMemoryInvalidData        = "AGENT_MEMORY_INVALID_DATA"
	CodeMemoryNotImplemented     = "AGENT_MEMORY_NOT_IMPLEMENTED"
	CodeMcpNoTools               = "AGENT_MCP_NO_TOOLS"
	CodeMcpMissingCallbackURL    = "AGENT_MCP_MISSING_CALLBACK_URL"
	CodeMcpMissingServerURL      = "AGENT_MCP_MISSING_SERVER_URL"
	CodeMcpMissingAPIKey         = "AGENT_MCP_MISSING_API_KEY"
	CodeMcpDuplicateTool         = "AGENT_MCP_DUPLICATE_TOOL"
	CodeMcpMissingSchema         = "AGENT_MCP_MISSING_SCHEMA"
	CodeMcpTransportError        = "AGENT_MCP_TRANSPORT_ERROR"
	CodeMcpInvalidResponse       = "AGENT_MCP_INVALID_RESPONSE"
	CodeMcpUnregisterFailed      = "AGENT_MCP_UNREGISTER_FAILED"
	CodeLLMRefusal               = "LLM_REFUSAL"
	CodeLLMInvalidJSON           = "LLM_INVALID_JSON"
	CodeLLMMaxTokens             = "LLM_MAX_TOKENS"
)

// MaxTurnsExceededError is returned when an agent exceeds its configured
// turn budget. Default budget is 20; configurable via AgentConfig.MaxTurns.
type MaxTurnsExceededError struct {
	*ironflow.IronflowError
	MaxTurns int
}

// NewMaxTurnsExceededError constructs a MaxTurnsExceededError.
func NewMaxTurnsExceededError(maxTurns int) *MaxTurnsExceededError {
	return &MaxTurnsExceededError{
		IronflowError: &ironflow.IronflowError{
			Message:   "agent exceeded maxTurns",
			Code:      CodeMaxTurnsExceeded,
			Retryable: false,
			Details:   map[string]any{"maxTurns": maxTurns},
		},
		MaxTurns: maxTurns,
	}
}

// LLMRefusalError signals the provider refused the request (safety,
// content filter, policy).
type LLMRefusalError struct {
	*ironflow.IronflowError
}

// NewLLMRefusalError constructs an LLMRefusalError.
func NewLLMRefusalError(message string, details map[string]any) *LLMRefusalError {
	return &LLMRefusalError{
		IronflowError: &ironflow.IronflowError{
			Message:   message,
			Code:      CodeLLMRefusal,
			Retryable: false,
			Details:   details,
		},
	}
}

// LLMInvalidJSONError signals the provider returned content that failed
// JSON parsing when JSON was required. Caller-raised inside the
// provider closure.
type LLMInvalidJSONError struct {
	*ironflow.IronflowError
}

// NewLLMInvalidJSONError constructs an LLMInvalidJSONError.
func NewLLMInvalidJSONError(message string, details map[string]any) *LLMInvalidJSONError {
	return &LLMInvalidJSONError{
		IronflowError: &ironflow.IronflowError{
			Message:   message,
			Code:      CodeLLMInvalidJSON,
			Retryable: true,
			Details:   details,
		},
	}
}

// LLMMaxTokensError signals the provider truncated the response by
// hitting max_tokens.
type LLMMaxTokensError struct {
	*ironflow.IronflowError
}

// NewLLMMaxTokensError constructs an LLMMaxTokensError.
func NewLLMMaxTokensError(message string, details map[string]any) *LLMMaxTokensError {
	return &LLMMaxTokensError{
		IronflowError: &ironflow.IronflowError{
			Message:   message,
			Code:      CodeLLMMaxTokens,
			Retryable: false,
			Details:   details,
		},
	}
}

// ToolValidationError is raised when ToolDefinition.Validate returns
// non-nil. Distinct from generic validation so callers can branch.
type ToolValidationError struct {
	*ironflow.IronflowError
	ToolName string
}

// NewToolValidationError constructs a ToolValidationError.
func NewToolValidationError(toolName string, cause error) *ToolValidationError {
	return &ToolValidationError{
		IronflowError: &ironflow.IronflowError{
			Message:   "tool " + quote(toolName) + " input validation failed",
			Code:      CodeToolValidation,
			Retryable: false,
			Details:   map[string]any{"toolName": toolName},
			Cause:     cause,
		},
		ToolName: toolName,
	}
}

// DuplicateToolError is raised when AgentConfig.Tools contains two or
// more definitions sharing the same name. Silent overwrite would let
// LLM-driven dispatch route to an unintended handler — fail loudly at
// agent construction.
type DuplicateToolError struct {
	*ironflow.IronflowError
	ToolName string
}

// NewDuplicateToolError constructs a DuplicateToolError.
func NewDuplicateToolError(toolName string) *DuplicateToolError {
	return &DuplicateToolError{
		IronflowError: &ironflow.IronflowError{
			Message:   "duplicate tool " + quote(toolName) + " registered on AgentConfig.Tools",
			Code:      CodeDuplicateTool,
			Retryable: false,
			Details:   map[string]any{"toolName": toolName},
		},
		ToolName: toolName,
	}
}

// ToolNotFoundError is raised when ToolByName is called with a name not
// registered on AgentConfig.Tools.
type ToolNotFoundError struct {
	*ironflow.IronflowError
	ToolName string
}

// NewToolNotFoundError constructs a ToolNotFoundError.
func NewToolNotFoundError(toolName string) *ToolNotFoundError {
	return &ToolNotFoundError{
		IronflowError: &ironflow.IronflowError{
			Message:   "tool " + quote(toolName) + " not registered on AgentConfig.Tools — register it or call by reference",
			Code:      CodeToolNotFound,
			Retryable: false,
			Details:   map[string]any{"toolName": toolName},
		},
		ToolName: toolName,
	}
}

// quote returns a double-quoted string for inclusion in error messages.
func quote(s string) string { return `"` + s + `"` }

// MemoryProjectionRequiredError is raised when Memory.EntityStream is
// called without a projection. Per architecture decision: raw event
// replay is not exposed through the agent memory API.
type MemoryProjectionRequiredError struct {
	*ironflow.IronflowError
	StreamID string
}

// NewMemoryProjectionRequiredError constructs a MemoryProjectionRequiredError.
func NewMemoryProjectionRequiredError(streamID string) *MemoryProjectionRequiredError {
	return &MemoryProjectionRequiredError{
		IronflowError: &ironflow.IronflowError{
			Message:   "memory.EntityStream requires a projection — raw replay is not exposed via the agent API",
			Code:      CodeMemoryProjectionRequired,
			Retryable: false,
			Details:   map[string]any{"streamId": streamID},
		},
		StreamID: streamID,
	}
}
