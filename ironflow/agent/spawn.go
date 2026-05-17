package agent

import (
	"github.com/sahina/ironflow-go/ironflow"
)

// Spawn invokes a sub-agent function and (optionally) waits for its
// result. Wraps ironflow.Invoke (Await=true, default) or
// ironflow.InvokeAsync wrapped in ironflow.Run as "spawn.<name>"
// (Await=false). Crash-resume applies: re-running the parent after a
// crash replays the cached sub-agent output without re-invoking.
//
// The `name` argument names the durable step for the async path so
// log/audit views show "spawn.<name>" rather than the underlying
// function ID. On the await path it is informational — Invoke records
// its own step keyed by the target function ID.
//
// Parent ↔ child run linkage is recorded server-side by the existing
// invoke implementation. Spawn does not add new linkage state.
func Spawn[I any, O any](ctx Context, name string, opts SpawnOptions[I]) (SpawnResult[O], error) {
	shouldAwait := true
	if opts.Await != nil {
		shouldAwait = *opts.Await
	}

	if shouldAwait {
		out, err := ironflow.Invoke[O](ctx.Inner, opts.FunctionID, opts.Input)
		if err != nil {
			return SpawnResult[O]{}, err
		}
		return SpawnResult[O]{Output: out}, nil
	}

	stepName := "spawn." + name
	handle, err := ironflow.Run(ctx.Inner, stepName, func() (ironflow.InvokeAsyncResult, error) {
		return ironflow.InvokeAsync(ctx.Inner, opts.FunctionID, opts.Input)
	})
	if err != nil {
		return SpawnResult[O]{}, err
	}
	return SpawnResult[O]{RunID: handle.RunID}, nil
}
