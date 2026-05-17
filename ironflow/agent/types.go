package agent

import (
	"sync"
	"time"

	"github.com/sahina/ironflow-go/ironflow"
)

// ============================================================================
// Tool — definition + invocation
// ============================================================================

// ToolIdempotency selects the idempotency strategy for a tool.
//
//   - IdempotentByCall: each Tool() invocation memoizes by call site
//     (default). Matches existing ironflow.Run semantics — same call,
//     same memoized result.
//   - IdempotentByArgs: memoize by SHA-256 hash of stable-JSON args.
//     Subsequent calls with the same input return the cached result,
//     even from different call sites.
type ToolIdempotency string

const (
	IdempotentByCall ToolIdempotency = "byCall"
	IdempotentByArgs ToolIdempotency = "byArgs"
)

// ToolDefinition is produced by DefineTool. Definitions are typically
// registered on AgentConfig.Tools so the agent can dispatch
// LLM-requested tool calls by name. They are also callable directly via
// Tool(ctx, def, input) for type-safe invocation.
type ToolDefinition[I any, O any] struct {
	// Name is the unique tool identifier (used for LLM dispatch).
	Name string

	// Description is an optional human-readable description (surfaced
	// to LLMs and MCP).
	Description string

	// Validate is an optional caller-supplied input validator. Returning
	// a non-nil error raises ToolValidationError. Go has no
	// Zod-equivalent runtime validator — callers wire whatever they
	// want (struct tags, custom logic, codegen).
	Validate func(input I) error

	// Idempotent selects the idempotency strategy. Defaults to
	// IdempotentByCall when zero.
	Idempotent ToolIdempotency

	// Timeout overrides the default 60s tool timeout. Zero falls back
	// to the default.
	Timeout time.Duration

	// Handler is the tool implementation.
	Handler func(input I) (O, error)
}

// toolEntry erases the I/O generics so a heterogeneous tool registry can
// store DefineTool results. ToolByName uses it to dispatch by name; the
// caller of ToolByName must accept the any-typed output.
type toolEntry struct {
	name        string
	description string
	idempotent  ToolIdempotency
	timeout     time.Duration

	// invoke runs the underlying typed handler. Implementations bind
	// generics at DefineTool time and accept any.
	invoke func(ctx ironflow.Context, runtime *agentRuntime, args any) (any, error)
}

// ============================================================================
// LLM
// ============================================================================

// LLMCompleteRequest is a provider-agnostic completion request. The
// LLM() wrapper does not own a provider router — callers pass a Call
// closure that does the actual API call. The wrapper memoizes the
// closure's resolved value as a step.
type LLMCompleteRequest struct {
	// Messages is the conversation history. Provider-shape-agnostic.
	Messages []LLMMessage

	// Tools optionally lists tool definitions for function-calling
	// providers.
	Tools []LLMToolHint

	// Call is the provider call closure. Must return a normalized
	// LLMCompleteResult. The wrapper memoizes the closure's resolved
	// value as a step. The closure is responsible for the actual
	// provider call and for mapping provider-specific shapes onto
	// LLMCompleteResult.
	Call func() (LLMCompleteResult, error)

	// Options is provider-passthrough metadata (model, temperature, …).
	Options map[string]any
}

// LLMMessage is one entry in the conversation history.
type LLMMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

// LLMToolHint is a tool definition surfaced to function-calling
// providers.
type LLMToolHint struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Input       any    `json:"input,omitempty"`
}

// LLMCompleteResult is the normalized provider response. Shape is
// provider-agnostic — callers narrow as needed.
type LLMCompleteResult struct {
	// Content is the raw assistant message content.
	Content any `json:"content,omitempty"`

	// ToolCalls lists tool calls requested by the model, if any.
	ToolCalls []LLMToolCall `json:"toolCalls,omitempty"`

	// FinishReason is the provider-supplied finish reason hint. When
	// set, the wrapper inspects it to raise classified errors:
	//   - "refusal" / "safety" / "content_filter" → LLMRefusalError
	//   - "max_tokens" / "length" → LLMMaxTokensError
	// Anything else passes through.
	FinishReason string `json:"finishReason,omitempty"`

	// Metadata is provider-passthrough metadata (usage, raw, …).
	Metadata map[string]any `json:"metadata,omitempty"`
}

// LLMToolCall is one tool invocation requested by the model.
type LLMToolCall struct {
	Name  string `json:"name"`
	Input any    `json:"input,omitempty"`
}

// ============================================================================
// Approve
// ============================================================================

// ApproveOptions configures an Approve() call.
type ApproveOptions[T any] struct {
	// TTL is how long to wait for the approval event before timing
	// out. Required.
	TTL time.Duration

	// Payload is attached to the pending approval (visible to
	// approvers).
	Payload T
}

// ApproveResult is the outcome of Approve(). Approved=false on
// timeout — the handler can distinguish timeout vs explicit rejection
// by inspecting the Reason field.
type ApproveResult[T any] struct {
	// Approved indicates whether the request was approved.
	Approved bool

	// Approver is the user who approved/rejected, if recorded by the
	// approver.
	Approver string

	// Payload is echoed back from the approval event.
	Payload T

	// Reason is supplied by the approver, or "timeout" when the wait
	// elapsed without a matching event.
	Reason string
}

// ============================================================================
// Memory
// ============================================================================

// MemoryGetOptions configures a Memory().Get call.
type MemoryGetOptions struct {
	// BypassCache disables the in-run cache for this read. Default
	// false (cache on). The cache invalidates on own writes within
	// the same run.
	BypassCache bool
}

// MemoryAppendOptions configures a Memory().Append call.
type MemoryAppendOptions struct {
	// Metadata attaches optional cross-cutting metadata to the
	// appended event.
	Metadata map[string]any
}

// MemoryClient is the per-run handle returned by Memory(ctx).
//
// Wraps an entity stream keyed by the agent run. EntityStream requires
// a projection — raw replay is not exposed.
type MemoryClient interface {
	// Get reads the projected memory state. Returns (nil, nil) when
	// the projection has no record for this run.
	Get(opts ...MemoryGetOptions) (map[string]any, error)

	// Append a memory event (durable). Auto-WaitForProjection ensures
	// read-your-writes inside the same run.
	Append(eventName string, data map[string]any, opts ...MemoryAppendOptions) error

	// EntityStream opens a projection-backed entity stream view. Stub
	// for parity with @ironflow/node/agent — returns
	// MemoryProjectionRequiredError on empty projection name and
	// otherwise returns NotImplemented.
	EntityStream(streamID, projectionName string) (map[string]any, error)
}

// MemoryConfig configures durable memory for an agent.
//
// Memory is opt-in per agent: callers wire a stream ID + projection
// name via AgentConfig.Memory.
type MemoryConfig struct {
	// StreamID is the entity stream ID for this agent's memory.
	StreamID string

	// Projection is the projection name used by Get().
	Projection string

	// EntityType is recorded with appended events. Informational on
	// the server side; surfaces in audit/admin views. Defaults to
	// "agent" when empty.
	EntityType string
}

// ============================================================================
// Spawn
// ============================================================================

// SpawnOptions configures a Spawn() call.
type SpawnOptions[I any] struct {
	// FunctionID is the sub-agent function to invoke.
	FunctionID string

	// Input is the event payload for the sub-agent.
	Input I

	// Await controls whether to wait for completion. Defaults to true
	// (i.e. synchronous Invoke). Set false for fire-and-forget
	// InvokeAsync.
	Await *bool
}

// SpawnResult is the outcome of a Spawn() call.
//
// Field availability depends on the await mode:
//   - Await=true (default): Output is the resolved sub-agent value;
//     RunID is empty because Invoke does not return the run ID.
//   - Await=false: RunID is populated (from InvokeAsync); Output is
//     zero because the caller did not wait for completion.
type SpawnResult[O any] struct {
	// RunID is the sub-run ID (for log/audit correlation). Present
	// when Await=false.
	RunID string

	// Output is the sub-agent output. Present when Await=true.
	Output O
}

// ============================================================================
// MCP
// ============================================================================

// McpToolDef defines a single MCP-exposed tool. Note: client-side scope
// hints are advisory. Authoritative authorization is enforced
// server-side via api_keys + tool_scopes.
type McpToolDef struct {
	// Name is the tool name as exposed via MCP.
	Name string

	// Description is the human-readable description for MCP clients.
	Description string

	// InputSchemaJSON is the JSON Schema (Draft 2020-12) for tool input,
	// serialized as a JSON string. Sent to the server at register time
	// so it can validate inbound MCP InvokeTool requests before
	// dispatching to this SDK process. Required for ExposeMcp to
	// register; the JS SDK derives this from a Zod schema, but Go has
	// no native runtime schema so callers supply it directly.
	InputSchemaJSON string

	// Validate is an optional caller-supplied input validator. Runs at
	// dispatch time on the decoded input (typically map[string]any).
	// Returning a non-nil error rejects the dispatch with HTTP 400 +
	// INPUT_SCHEMA_INVALID. Server-side schema validation runs first,
	// so this is a defense-in-depth check.
	Validate func(input any) error

	// Scopes lists required scope strings for server-side RBAC.
	// Hint to clients only — authoritative enforcement is server-side.
	Scopes []string

	// TimeoutMS overrides the default per-tool dispatch timeout. Zero
	// uses the server's default (30s). Bounded [1, 60_000] by the SDK
	// contract; the server caps further.
	TimeoutMS uint32

	// Handler is the tool implementation. Called by the SDK dispatch
	// handler with the decoded input from the server's InvokeTool
	// request. Returning a non-nil error maps to a 200 + HANDLER_ERROR
	// envelope so the server's dispatcher decodes it as a tool error
	// rather than a transport failure.
	Handler func(input any) (any, error)
}

// ExposeMcpConfig configures ExposeMcp().
type ExposeMcpConfig struct {
	// Name is the agent namespace. Tools register under this name as
	// `{Name}.{tool.Name}`.
	Name string

	// Version is the server version reported to MCP clients.
	Version string

	// Tools is the registry exposed to MCP clients.
	Tools []McpToolDef

	// CallbackURL is where the Ironflow server POSTs HMAC-signed
	// dispatch requests. Required. Should point at your Serve() mount
	// (e.g. https://app.example.com/api/ironflow). The SDK's dispatch
	// handler routes the suffix /ironflow/agent-tools/dispatch.
	CallbackURL string

	// ServerURL is the Ironflow server URL. Falls back to
	// IRONFLOW_URL or IRONFLOW_SERVER_URL env vars when empty.
	ServerURL string

	// APIKey authenticates the RegisterTool call. Must hold the
	// agent:tools:register action. Falls back to IRONFLOW_API_KEY env
	// when empty.
	APIKey string
}

// ExposeMcpHandle is returned from ExposeMcp(). Call Unregister() to
// remove tools from both the server's registry and the local SDK
// mirror.
type ExposeMcpHandle struct {
	// Name is the agent namespace registered with the server.
	Name string

	// ToolCount is the number of tools registered.
	ToolCount int

	// Status is "active" once RegisterTool succeeds.
	Status string

	// ToolNames are the qualified names registered (`{agentName}.{toolName}`).
	ToolNames []string

	// internal fields drive Unregister().
	agentName    string
	serverURL    string
	apiKey       string
	unregistered bool
	unregMu      sync.Mutex
}

// ============================================================================
// Agent — config, context, handler
// ============================================================================

// AgentConfig extends ironflow.FunctionConfig with agent-shaped fields.
type AgentConfig struct {
	// Function is the underlying Ironflow function configuration.
	Function ironflow.FunctionConfig

	// Tools are the definitions available for LLM-driven dispatch via
	// ToolByName. Direct Tool(ctx, def, input) calls do not require
	// registration.
	Tools []ToolEntry

	// Memory configures durable memory. When zero-valued, calls into
	// Memory(ctx) raise CodeMemoryUnconfigured.
	Memory *MemoryConfig

	// MaxTurns is the agent turn budget (LLM() calls before
	// MaxTurnsExceededError). Default 20.
	MaxTurns int
}

// ToolEntry is the registry shape — DefineTool[I,O] returns a
// ToolDefinition that consumers convert to a ToolEntry via .Entry()
// for storage on AgentConfig.Tools.
type ToolEntry = toolEntry

// AgentHandler is the agent handler signature. The returned value is
// surfaced as the function's output (untyped any in the underlying
// FunctionHandler).
type AgentHandler func(ctx Context) (any, error)

// Context is passed to agent handlers. Embeds ironflow.Context for
// pass-through and adds agent-runtime hooks (tools, turn counter,
// memory cache).
type Context struct {
	// Inner is the underlying Ironflow function context. Use Inner.Step
	// equivalents (ironflow.Run, ironflow.Sleep, etc.) for low-level
	// access.
	Inner ironflow.Context

	// runtime is the per-run agent runtime state (counters, registry,
	// caches). Hidden — accessed via the top-level helpers.
	runtime *agentRuntime
}

// Run returns the running run info.
func (c Context) Run() ironflow.RunInfo { return c.Inner.Run }

// Event returns the triggering event.
func (c Context) Event() ironflow.Event { return c.Inner.Event }

// Secrets returns the secrets reader.
func (c Context) Secrets() ironflow.SecretsReader { return c.Inner.Secrets }

// Turn returns the number of LLM() turns consumed so far in this run.
func (c Context) Turn() int {
	if c.runtime == nil {
		return 0
	}
	return c.runtime.turnCount
}
