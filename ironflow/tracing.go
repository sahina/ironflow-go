package ironflow

import (
	"context"
	"errors"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/sahina/ironflow-go/ironflow"

func traceExecutionContext(ctx context.Context, parent, state string) context.Context {
	// A poll or stream request is not the producer of its assigned jobs.
	ctx = trace.ContextWithSpanContext(ctx, trace.SpanContext{})
	ctx = baggage.ContextWithBaggage(ctx, baggage.Baggage{})
	if len(parent) > 512 || (strings.HasPrefix(parent, "00-") && len(parent) != 55) {
		return ctx
	}
	if len(state) > 512 {
		state = ""
	}
	return (propagation.TraceContext{}).Extract(ctx, propagation.MapCarrier{
		"traceparent": parent, "tracestate": state,
	})
}

func startProcess(ctx context.Context, parent, state string) (context.Context, trace.Span) {
	return otel.Tracer(tracerName).Start(traceExecutionContext(ctx, parent, state), "process", trace.WithSpanKind(trace.SpanKindConsumer))
}

func failSpan(span trace.Span, err error) {
	if err == nil {
		return
	}
	if _, yielded := isYieldSignal(err); yielded {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	// Handler errors may contain event data or credentials.
	span.RecordError(errors.New("execution failed"))
	span.SetStatus(codes.Error, "execution failed")
}

func finishSpan(span trace.Span, err *error) {
	if r := recover(); r != nil {
		if _, yielded := r.(*yieldSignal); !yielded {
			failSpan(span, errors.New("panic"))
		}
		// End outside panic unwinding so OTel cannot record the raw panic value.
		span.End()
		panic(r)
	}
	failSpan(span, *err)
	span.End()
}

// Context returns the execution's Go context, including cancellation and its scoped span.
func (c Context) Context() context.Context {
	if c.scope != nil {
		return c.scope.ctx
	}
	if c.exec == nil || c.exec.ctx == nil {
		return context.Background()
	}
	return c.exec.ctx
}

// Context returns the execution's Go context, including cancellation and its process span.
func (c *HandlerContext) Context() context.Context { return c.ctx.Context() }
