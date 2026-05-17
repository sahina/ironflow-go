// Package agent provides durable agent primitives for Ironflow on Go.
//
// Agent is sugar over the existing step client. Each helper (Tool, LLM,
// Approve, Memory, Spawn) records durable steps under the hood, so
// agents inherit Ironflow's crash-resume, replay, audit, and
// scoped-injection semantics with no new server primitives.
//
// Anti-scope (locked in CEO + eng review of #588):
//   - No LLM provider router. Callers bring their own provider SDK and
//     pass the provider call closure into LLM() — the wrapper memoizes
//     the result.
//   - No prompt templating. Reasoning frameworks (LangGraph, the Claude
//     Agent SDK, CrewAI) own that surface. Ironflow hosts them, not
//     replaces them.
//   - No graph execution. Agent() runs a plain handler.
//
// Mirrors the surface of @ironflow/node/agent (Lane B-1, JS) so
// cross-language agent stacks share semantics — codes, idempotency,
// turn counter, classified LLM errors. See docs/explanation/comparison-agents.md
// for the layering model.
package agent
