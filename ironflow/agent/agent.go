package agent

import (
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/sahina/ironflow-go/ironflow"
)

// DefaultMaxTurns is the agent turn budget when AgentConfig.MaxTurns is
// zero. Mirrors the JS DEFAULT_MAX_TURNS.
const DefaultMaxTurns = 20

// agentRuntime holds per-run state shared by the wrapper helpers.
// Lifetime is one handler invocation. The runtime is fresh on every
// run (including replays after crash-resume) — replay correctness
// comes from the underlying ironflow.Run memoization, not from
// runtime state.
type agentRuntime struct {
	maxTurns int

	// turnCount is the number of LLM() turns consumed so far. Drives
	// Context.Turn() and MaxTurnsExceededError.
	turnCount int

	// registry indexes tools by name for ToolByName dispatch.
	registry map[string]toolEntry

	// byArgsGroup folds concurrent same-args byArgs invocations into a
	// single in-flight call. Once that call resolves, the result is
	// cached on byArgsCache (success) or byArgsErr (failure) for
	// subsequent sequential calls within the run.
	byArgsGroup singleflight.Group

	// byArgsCache memoizes successful byArgs tool invocations within
	// the run.
	byArgsCache map[string]any

	// byArgsErr memoizes failed byArgs tool invocations within the
	// run — subsequent calls return the same error rather than
	// retrying the handler.
	byArgsErr map[string]error

	// memoryConfig is the resolved memory config for this run, if any.
	memoryConfig *MemoryConfig

	// memoryBackend is the resolved memory backend for this run, if any.
	memoryBackend MemoryBackend

	// memoryCache holds the cached projection state for the run.
	memoryCache       any
	memoryCacheLoaded bool

	// memoryAppendCount drives deterministic idempotency-key generation
	// for Memory().Append calls.
	memoryAppendCount int

	// mu guards mutations of the fields above. Helpers may call into
	// goroutines — keep contention minimal and per-run.
	mu sync.Mutex
}

// Agent constructs a durable agent function.
//
// The returned ironflow.Function registers via ironflow.RegisterFunction
// like any other function — serve() and worker setups need no
// agent-specific awareness.
//
// Example:
//
//	var ReviewAgent = agent.Agent(agent.AgentConfig{
//	    Function: ironflow.FunctionConfig{
//	        ID:       "code-review",
//	        Triggers: []ironflow.Trigger{{Event: "pr.opened"}},
//	    },
//	    Tools: []agent.ToolEntry{fetchDiff.Entry()},
//	}, func(ctx agent.Context) (any, error) {
//	    diff, err := agent.Tool(ctx, fetchDiff, fetchDiffInput{PR: 42})
//	    if err != nil { return nil, err }
//	    res, err := agent.LLM(ctx, agent.LLMCompleteRequest{
//	        Messages: []agent.LLMMessage{{Role: "user", Content: diff}},
//	        Call: func() (agent.LLMCompleteResult, error) {
//	            return providerComplete(...)
//	        },
//	    })
//	    if err != nil { return nil, err }
//	    decision, err := agent.Approve[reviewPayload](ctx, "ship-it",
//	        agent.ApproveOptions[reviewPayload]{TTL: 24 * time.Hour})
//	    return map[string]any{"approved": decision.Approved}, err
//	})
func Agent(cfg AgentConfig, handler AgentHandler) ironflow.Function {
	registry, err := buildRegistry(cfg.Tools)
	if err != nil {
		// CreateFunction panics on invalid config — match that
		// behavior for duplicate tools so registration fails loudly.
		panic(err)
	}

	maxTurns := cfg.MaxTurns
	if maxTurns <= 0 {
		maxTurns = DefaultMaxTurns
	}

	memCfg := cfg.Memory

	return ironflow.CreateFunction(cfg.Function, func(ictx ironflow.Context) (any, error) {
		runtime := &agentRuntime{
			maxTurns:     maxTurns,
			registry:     registry,
			byArgsCache:  make(map[string]any),
			byArgsErr:    make(map[string]error),
			memoryConfig: memCfg,
		}

		if memCfg != nil {
			backend, berr := defaultMemoryBackend()
			if berr == nil {
				runtime.memoryBackend = backend
			}
		}

		actx := Context{Inner: ictx, runtime: runtime}
		return handler(actx)
	})
}

// buildRegistry indexes tool entries by name and rejects duplicates.
func buildRegistry(entries []toolEntry) (map[string]toolEntry, error) {
	m := make(map[string]toolEntry, len(entries))
	for _, e := range entries {
		if _, exists := m[e.name]; exists {
			return nil, NewDuplicateToolError(e.name)
		}
		m[e.name] = e
	}
	return m, nil
}
