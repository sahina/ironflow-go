package agent

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sahina/ironflow-go/ironflow"
)

// DefaultToolTimeout is the per-tool timeout applied when
// ToolDefinition.Timeout is zero. Mirrors JS DEFAULT_TOOL_TIMEOUT (60s).
const DefaultToolTimeout = 60 * time.Second

// DefineTool is a pass-through factory that preserves the input/output
// generic types for callers. Tools are typically registered on
// AgentConfig.Tools so the agent can dispatch LLM-requested calls by
// name (via ToolByName).
func DefineTool[I any, O any](spec ToolDefinition[I, O]) ToolDefinition[I, O] {
	return spec
}

// Entry erases the I/O generics so a heterogeneous registry can store
// the definition. The returned ToolEntry is what AgentConfig.Tools
// accepts; ToolByName uses it to dispatch a tool by name.
func (def ToolDefinition[I, O]) Entry() ToolEntry {
	return toolEntry{
		name:        def.Name,
		description: def.Description,
		idempotent:  def.Idempotent,
		timeout:     def.Timeout,
		invoke: func(ctx ironflow.Context, runtime *agentRuntime, args any) (any, error) {
			input, err := decodeAs[I](args)
			if err != nil {
				return nil, NewToolValidationError(def.Name, fmt.Errorf("decode input: %w", err))
			}
			return runTool(ctx, runtime, def, input)
		},
	}
}

// Tool runs a tool by reference. Type-safe — input validates against
// def.Validate (when set) and the resolved output type is the
// definition's O. Wraps ironflow.Run with idempotency keying and
// timeout enforcement.
func Tool[I any, O any](ctx Context, def ToolDefinition[I, O], input I) (O, error) {
	return runTool(ctx.Inner, ctx.runtime, def, input)
}

// ToolByName runs a tool by registered name. Required for LLM-driven
// dispatch where the model returns a tool name + args. Output is
// untyped any — callers narrow as needed.
func ToolByName(ctx Context, name string, args any) (any, error) {
	if ctx.runtime == nil {
		return nil, NewToolNotFoundError(name)
	}
	entry, ok := ctx.runtime.registry[name]
	if !ok {
		return nil, NewToolNotFoundError(name)
	}
	return entry.invoke(ctx.Inner, ctx.runtime, args)
}

// runTool is the shared invocation path used by both Tool and
// ToolByName (after generic erasure).
func runTool[I any, O any](ctx ironflow.Context, runtime *agentRuntime, def ToolDefinition[I, O], input I) (O, error) {
	var zero O

	if def.Validate != nil {
		if err := def.Validate(input); err != nil {
			return zero, NewToolValidationError(def.Name, err)
		}
	}

	timeout := def.Timeout
	if timeout <= 0 {
		timeout = DefaultToolTimeout
	}

	idempotent := def.Idempotent
	if idempotent == "" {
		idempotent = IdempotentByCall
	}

	if idempotent == IdempotentByArgs && runtime != nil {
		hash, err := hashArgs(input)
		if err != nil {
			return zero, NewToolValidationError(def.Name, fmt.Errorf("hash args: %w", err))
		}
		cacheKey := def.Name + ":" + hash

		// Cache fast-path — sequential repeat calls or post-resolve
		// arrivals short-circuit without re-entering singleflight.
		runtime.mu.Lock()
		cachedErr, hasErr := runtime.byArgsErr[cacheKey]
		cached, hasCached := runtime.byArgsCache[cacheKey]
		runtime.mu.Unlock()
		if hasErr {
			return zero, cachedErr
		}
		if hasCached {
			return castCached[O](cached)
		}

		// Concurrent same-args calls fold into one durable step via
		// singleflight. Result is stored on the cache before the
		// inner func returns so post-resolve waiters from the same
		// goroutine ordering see the cached value.
		stepName := fmt.Sprintf("tool.%s.%s", def.Name, hash)
		raw, err, _ := runtime.byArgsGroup.Do(cacheKey, func() (any, error) {
			out, runErr := ironflow.Run(ctx, stepName, func() (O, error) {
				return def.Handler(input)
			}, ironflow.WithTimeout(timeout))
			runtime.mu.Lock()
			if runErr != nil {
				runtime.byArgsErr[cacheKey] = runErr
			} else {
				runtime.byArgsCache[cacheKey] = out
			}
			runtime.mu.Unlock()
			return out, runErr
		})
		if err != nil {
			return zero, err
		}
		return castCached[O](raw)
	}

	stepName := "tool." + def.Name
	return ironflow.Run(ctx, stepName, func() (O, error) {
		return def.Handler(input)
	}, ironflow.WithTimeout(timeout))
}

// castCached narrows a cached any value (stored at the typed handler's
// output type) back to the caller's expected O. Direct type assertion
// suffices when O matches; JSON round-trip handles the rare case where
// O differs from the stored shape (e.g., re-typed call sites).
func castCached[O any](cached any) (O, error) {
	if typed, ok := cached.(O); ok {
		return typed, nil
	}
	out, err := decodeAs[O](cached)
	if err != nil {
		var zero O
		return zero, fmt.Errorf("byArgs cache decode: %w", err)
	}
	return out, nil
}

// decodeAs converts an any value to T, preferring direct type
// assertion and falling back to JSON round-trip for shape coercion.
// Used when ToolByName receives untyped args from an LLM.
//
// Coercion is intentionally loose: extra fields are ignored, missing
// fields zero-default, numeric widening follows encoding/json rules.
// Callers that need strict validation should attach a
// ToolDefinition.Validate closure that checks the decoded input.
func decodeAs[T any](value any) (T, error) {
	var zero T
	if value == nil {
		return zero, nil
	}
	if typed, ok := value.(T); ok {
		return typed, nil
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		return zero, err
	}
	var out T
	if err := json.Unmarshal(bytes, &out); err != nil {
		return zero, err
	}
	return out, nil
}
