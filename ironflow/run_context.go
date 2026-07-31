package ironflow

import "context"

// HeaderRunID is the HTTP header carrying the run that emitted an event. The
// engine persists it on the event row so the flow map can learn function→event
// causation edges (#1262). Mirrors the engine's internal/constants.HeaderRunID
// (duplicated here because the SDK is a separate module and cannot import it).
const HeaderRunID = "X-Ironflow-Run-ID"

type runIDKey struct{}

// WithRunID returns a context carrying runID. Client requests made with this
// context attach it as the X-Ironflow-Run-ID header, so events a function emits
// are attributed to the run for the flow map's learned emit edges (#1262).
// An empty runID returns ctx unchanged.
//
// Inside a function handler the run-scoped context is available via
// Context.RunContext(); use WithRunID directly only to tag a different parent
// context (e.g. to keep cancellation):
//
//	client.Emit(ironflow.WithRunID(ctx, c.Run.ID), "order.shipped", data)
func WithRunID(ctx context.Context, runID string) context.Context {
	if runID == "" {
		return ctx
	}
	return context.WithValue(ctx, runIDKey{}, runID)
}

// runIDFromContext returns the run id carried by ctx, or "" if none.
func runIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(runIDKey{}).(string); ok {
		return v
	}
	return ""
}

// RunContext returns a context.Context tagged with this run's id, ready to pass
// to Client methods so events the function emits are attributed to the run and
// show up as learned edges in the flow map (#1262):
//
//	client.Emit(c.RunContext(), "order.shipped", data)
//
// It derives from context.Background(); to keep cancellation from another
// context, use WithRunID(parent, c.Run.ID) instead.
func (c Context) RunContext() context.Context {
	return WithRunID(context.Background(), c.Run.ID)
}
