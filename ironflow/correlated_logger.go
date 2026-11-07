package ironflow

import (
	"context"
	"log/slog"
	"slices"
	"sync/atomic"

	"go.opentelemetry.io/otel/trace"
)

type logScope struct {
	ctx    context.Context
	stepID string
	active atomic.Bool
}

// Logger returns an application-owned slog logger bound to this execution.
// Plain Info calls carry the execution context to the handler, including OTel bridges.
func (c Context) Logger() *slog.Logger {
	return slog.New(&correlatedHandler{sink: slog.Default().Handler(), execution: c})
}

type loggerBinding struct {
	group string
	attrs []slog.Attr
}

type correlatedHandler struct {
	sink      slog.Handler
	execution Context
	bindings  []loggerBinding
}

//nolint:contextcheck // Plain slog calls supply Background; use the bound execution context for bridge filtering.
func (h *correlatedHandler) Enabled(_ context.Context, level slog.Level) bool {
	return h.sink.Enabled(h.execution.Context(), level)
}

func reservedLogKey(key string) bool {
	switch key {
	case "run_id", "function_id", "step_id", "attempt", "trace_id":
		return true
	}
	return false
}

//nolint:contextcheck // The execution context must reach the app bridge even for plain Info calls.
func (h *correlatedHandler) Handle(_ context.Context, r slog.Record) error {
	c := h.execution
	clean := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	var attrs []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	for _, b := range slices.Backward(h.bindings) {
		if b.group != "" {
			attrs = []slog.Attr{{Key: b.group, Value: slog.GroupValue(attrs...)}}
		} else {
			attrs = append(slices.Clone(b.attrs), attrs...)
		}
	}
	clean.AddAttrs(cleanLogAttrs(attrs)...)
	runID, functionID, attempt := c.Run.ID, c.Run.FunctionID, c.Run.Attempt
	if c.exec != nil {
		runID, functionID, attempt = c.exec.runID, c.exec.functionID, c.exec.attempt
	}
	stepID := ""
	if c.scope != nil && c.scope.active.Load() {
		stepID = c.scope.stepID
	}
	traceID := ""
	if sc := trace.SpanContextFromContext(c.Context()); sc.IsValid() {
		traceID = sc.TraceID().String()
	}
	clean.AddAttrs(slog.String("run_id", runID), slog.String("function_id", functionID), slog.String("step_id", stepID), slog.Int("attempt", attempt), slog.String("trace_id", traceID))
	return h.sink.Handle(c.Context(), clean)
}

// Empty-key groups inline their attributes, so collisions there are root collisions too.
func cleanLogAttrs(attrs []slog.Attr) []slog.Attr {
	clean := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		if reservedLogKey(a.Key) {
			continue
		}
		a.Value = a.Value.Resolve()
		if a.Key == "" && a.Value.Kind() == slog.KindGroup {
			a.Value = slog.GroupValue(cleanLogAttrs(a.Value.Group())...)
		}
		clean = append(clean, a)
	}
	return clean
}

func resolveBoundLogAttr(a slog.Attr) slog.Attr {
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindGroup {
		children := slices.Clone(a.Value.Group())
		for i := range children {
			children[i] = resolveBoundLogAttr(children[i])
		}
		a.Value = slog.GroupValue(children...)
	}
	return a
}

func (h *correlatedHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	bound := slices.Clone(attrs)
	for i := range bound {
		bound[i] = resolveBoundLogAttr(bound[i])
	}
	next := *h
	next.bindings = append(slices.Clone(h.bindings), loggerBinding{attrs: bound})
	return &next
}

func (h *correlatedHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	next := *h
	next.bindings = append(slices.Clone(h.bindings), loggerBinding{group: name})
	return &next
}

// RunWithContext executes a durable step and passes its logging and trace scope
// to fn. Nested RunWithContext calls retain the current branch allocator.
func RunWithContext[T any](ctx Context, name string, fn func(Context) (T, error), opts ...StepOption) (T, error) {
	return runScoped(ctx, name, func(scoped Context, _ string) (T, error) { return fn(scoped) }, opts...)
}

// RunWithBranchContext is RunWithContext using the branch's durable allocator.
func RunWithBranchContext[T any](b *BranchContext, name string, fn func(Context) (T, error), opts ...StepOption) (T, error) {
	return RunWithContext(branchLoggingContext(b), name, fn, opts...)
}

func branchLoggingContext(b *BranchContext) Context {
	c := Context{exec: b.parent}
	if b.logContext != nil {
		c = *b.logContext
	}
	c.branch = b
	return c
}
