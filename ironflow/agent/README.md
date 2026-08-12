# `github.com/sahina/ironflow-go/ironflow/agent`

Durable agent primitives for Ironflow on Go. Mirrors `@ironflow/node/agent` so cross-language stacks share semantics — codes, idempotency, turn counter, classified LLM errors.

## Why

Ironflow already gives you durable step execution, event-sourced memory, scoped injection, MCP server, single-binary deploy. The `agent` package wraps those primitives behind agent-shaped helpers so reasoning frameworks (LangGraph, the Claude Agent SDK, CrewAI) sit on top cleanly.

```
   handler logic
        │
   ┌────▼────────────────────────────────────────┐
   │ agent.Tool / LLM / Approve / Spawn / Memory │   ← this package
   └────┬────────────────────────────────────────┘
        │ records durable steps under the hood
   ┌────▼────────────────────────────────────────┐
   │           ironflow.Run / Invoke /            │
   │       WaitForEvent / Streams / Projections   │
   └─────────────────────────────────────────────┘
        │
   ┌────▼────────────────┐
   │  Ironflow server    │   crash-resume, replay, audit
   └─────────────────────┘
```

## Anti-scope

- No LLM provider router. Callers bring their own provider SDK and pass the call closure into `LLM()`.
- No prompt templating. Reasoning frameworks own that surface.
- No graph execution. `Agent()` runs a plain handler.

## Quick start

```go
package main

import (
	"fmt"
	"time"

	"github.com/sahina/ironflow-go/ironflow"
	"github.com/sahina/ironflow-go/ironflow/agent"
)

type fetchInput  struct{ PR int `json:"pr"` }
type fetchOutput struct{ Diff string `json:"diff"` }

var fetchDiff = agent.DefineTool[fetchInput, fetchOutput](agent.ToolDefinition[fetchInput, fetchOutput]{
	Name: "fetch-diff",
	Validate: func(in fetchInput) error {
		if in.PR <= 0 { return fmt.Errorf("pr must be > 0") }
		return nil
	},
	Handler: func(in fetchInput) (fetchOutput, error) {
		return githubFetchDiff(in.PR)
	},
})

var ReviewAgent = agent.Agent(agent.AgentConfig{
	Function: ironflow.FunctionConfig{
		ID:       "code-review",
		Triggers: []ironflow.Trigger{{Event: "pr.opened"}},
	},
	Tools:    []agent.ToolEntry{fetchDiff.Entry()},
	MaxTurns: 10,
}, func(ctx agent.Context) (any, error) {
	var ev struct{ PRNumber int `json:"prNumber"` }
	if err := ctx.Event().Data(&ev); err != nil { return nil, err }

	diff, err := agent.Tool(ctx, fetchDiff, fetchInput{PR: ev.PRNumber})
	if err != nil { return nil, err }

	res, err := agent.LLM(ctx, agent.LLMCompleteRequest{
		Messages: []agent.LLMMessage{{Role: "user", Content: diff.Diff}},
		Call: func() (agent.LLMCompleteResult, error) {
			return providerComplete(diff.Diff)
		},
	})
	if err != nil { return nil, err }

	approval, err := agent.Approve[map[string]any](ctx, "ship-it", agent.ApproveOptions[map[string]any]{
		TTL:     24 * time.Hour,
		Payload: map[string]any{"findings": res.Content},
	})
	return map[string]any{"approved": approval.Approved}, err
})
```

## Surface

| Helper | Purpose | Backed by |
| --- | --- | --- |
| `Agent(cfg, handler)` | Define a durable agent function | `ironflow.CreateFunction` |
| `DefineTool[I, O](spec)` | Type-safe tool definition | (pure factory) |
| `Tool(ctx, def, input)` | Run tool by reference | `ironflow.Run` |
| `ToolByName(ctx, name, args)` | Run tool by registered name (LLM dispatch) | `ironflow.Run` |
| `LLM(ctx, req)` | Memoized completion + classified errors | `ironflow.Run` |
| `Approve[T](ctx, name, opts)` | Wait for human approval | `ironflow.WaitForEvent` |
| `Spawn[I, O](ctx, name, opts)` | Sub-agent invoke | `ironflow.Invoke` / `InvokeAsync` |
| `Memory(ctx)` | Durable agent memory | `client.AppendStreamEvent` + projections |
| `ExposeMcp(cfg)` | Register agent tools with Ironflow's MCP server | `AgentToolsService.RegisterTool` + HMAC callback |

## Stable error codes

Match the JS module so cross-SDK consumers branch on the same constants:

| Code | Type |
| --- | --- |
| `AGENT_MAX_TURNS_EXCEEDED` | `MaxTurnsExceededError` |
| `AGENT_TOOL_VALIDATION` | `ToolValidationError` |
| `AGENT_DUPLICATE_TOOL` | `DuplicateToolError` |
| `AGENT_TOOL_NOT_FOUND` | `ToolNotFoundError` |
| `AGENT_MEMORY_UNCONFIGURED` | `IronflowError` |
| `AGENT_MEMORY_NO_BACKEND` | `IronflowError` |
| `AGENT_MEMORY_PROJECTION_REQUIRED` | `MemoryProjectionRequiredError` |
| `AGENT_MEMORY_INVALID_DATA` | `IronflowError` |
| `AGENT_MEMORY_NOT_IMPLEMENTED` | `IronflowError` |
| `AGENT_MCP_NO_TOOLS` | `IronflowError` |
| `AGENT_MCP_MISSING_CALLBACK_URL` | `IronflowError` |
| `AGENT_MCP_MISSING_SERVER_URL` | `IronflowError` |
| `AGENT_MCP_MISSING_API_KEY` | `IronflowError` |
| `AGENT_MCP_DUPLICATE_TOOL` | `IronflowError` |
| `AGENT_MCP_MISSING_SCHEMA` | `IronflowError` |
| `AGENT_MCP_TRANSPORT_ERROR` | `IronflowError` |
| `AGENT_MCP_INVALID_RESPONSE` | `IronflowError` |
| `AGENT_MCP_UNREGISTER_FAILED` | `IronflowError` |
| `LLM_REFUSAL` | `LLMRefusalError` |
| `LLM_INVALID_JSON` | `LLMInvalidJSONError` |
| `LLM_MAX_TOKENS` | `LLMMaxTokensError` |

## Idempotency

`ToolDefinition.Idempotent` selects the strategy:

- `IdempotentByCall` (default) — each `Tool()` invocation memoizes by call site, matching `ironflow.Run` semantics.
- `IdempotentByArgs` — memoize by SHA-256 hash of stable-JSON args. Same input → same memoized result, even from different call sites.

## Memory

```go
agent.Agent(agent.AgentConfig{
	Function: ironflow.FunctionConfig{...},
	Memory: &agent.MemoryConfig{
		StreamID:   "agent-memory:" + runID,
		Projection: "agent-memory-view",
		EntityType: "agent",
	},
}, func(ctx agent.Context) (any, error) {
	state, err := agent.Memory(ctx).Get()
	if err != nil { return nil, err }

	if err := agent.Memory(ctx).Append("turn.completed", map[string]any{
		"turn": ctx.Turn(),
	}); err != nil { return nil, err }

	return state, nil
})
```

Cross-run retry safety: `Append` generates a deterministic idempotency key (`{runId}:memory.append:{counter}`) so a replayed handler appends the same logical event and the server dedupes server-side. Auto-`WaitForProjection` after each append guarantees the next `Get()` in the same run sees the write.

## Testing

The `ironflowtest.NewClient` test harness drives the underlying step interceptor; agent helpers compose over it transparently. See `agent/*_test.go` for examples mocking `tool.{name}`, `llm.turn`, `approve.{name}`, sub-function invokes, and memory backends.

## See also

- JS counterpart: [`@ironflow/node/agent`](../../../js/node/README.md#agent-primitives)
- LangGraph adapter: [`@ironflow/langgraph`](../../../js/langgraph/README.md)
- Layering model: [`docs/explanation/comparison-agents.md`](../../../../docs/explanation/comparison-agents.md)
- Crash-resume tutorial: [`docs/tutorials/agent-survives-crash.md`](../../../../docs/tutorials/agent-survives-crash.md)
- Working examples: [`examples/agents/doc-processor-agent`](../../../../examples/agents/doc-processor-agent), [`examples/agents/code-review-agent`](../../../../examples/agents/code-review-agent), [`examples/ai-agent`](../../../../examples/ai-agent)
