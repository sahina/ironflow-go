package ironflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

const testTraceparent = "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01"

func traceExporter(t *testing.T, sampler sdktrace.Sampler) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sampler))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })
	return exporter
}

func TestTracingJobContinuation(t *testing.T) {
	for _, transport := range []string{"poll", "stream", "push"} {
		t.Run(transport, func(t *testing.T) {
			exporter := traceExporter(t, sdktrace.AlwaysSample())
			ambientCtx, ambientSpan := otel.Tracer("test").Start(context.Background(), "unrelated request")
			defer ambientSpan.End()
			member, _ := baggage.NewMember("credential", "hidden")
			bag, _ := baggage.New(member)
			ambientCtx = baggage.ContextWithBaggage(ambientCtx, bag)
			var handlerSC trace.SpanContext
			fn := Function{Config: FunctionConfig{ID: "fn"}, Handler: func(ctx Context) (any, error) {
				handlerSC = trace.SpanContextFromContext(ctx.Context())
				if baggage.FromContext(ctx.Context()).Len() != 0 {
					t.Fatal("inherited baggage")
				}
				return Run(ctx, "work", func() (string, error) { return "done", nil })
			}}
			switch transport {
			case "poll":
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_ = json.NewEncoder(w).Encode(map[string]any{"jobs": []any{map[string]any{"job_id": "j", "run_id": "r", "function_id": "fn", "context": map[string]string{"traceparent": testTraceparent, "tracestate": "vendor=value"}}}})
				}))
				defer server.Close()
				jobs, _, err := newTestWorker(server.URL).requestJobs(ambientCtx, 1)
				if err != nil {
					t.Fatal(err)
				}
				executor := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
				if err := executor.execute(ambientCtx, jobs[0], &recordingReporter{}); err != nil {
					t.Fatal(err)
				}
			case "stream":
				job, err := protoToJobAssignment(&ironflowv1.JobAssignment{JobId: "j", RunId: "r", FunctionId: "fn", Context: &ironflowv1.JobContext{Traceparent: testTraceparent, Tracestate: "vendor=value"}})
				if err != nil {
					t.Fatal(err)
				}
				executor := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
				if err := executor.execute(ambientCtx, job, &recordingReporter{}); err != nil {
					t.Fatal(err)
				}
			case "push":
				body, _ := json.Marshal(PushRequest{RunID: "r", FunctionID: "fn"})
				req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)).WithContext(ambientCtx)
				req.Header.Set("traceparent", testTraceparent)
				req.Header.Set("tracestate", "vendor=value")
				req.Header.Set("baggage", "credential=hidden")
				rr := httptest.NewRecorder()
				Serve(ServeConfig{Functions: []Function{fn}, SkipVerification: true}).ServeHTTP(rr, req)
				if rr.Code != 200 {
					t.Fatal(rr.Body.String())
				}
			}
			spans := exporter.GetSpans()
			if len(spans) != 2 {
				t.Fatalf("spans = %v", spans)
			}
			step, process := spans[0], spans[1]
			if process.Name != "process" || process.Parent.TraceID().String() != "1234567890abcdef1234567890abcdef" || process.Parent.SpanID().String() != "1234567890abcdef" || !process.Parent.IsRemote() {
				t.Fatalf("process = %+v", process)
			}
			if !handlerSC.Equal(process.SpanContext) || step.Name != "step.run" || step.Parent.SpanID() != process.SpanContext.SpanID() || step.SpanContext.TraceID() != process.SpanContext.TraceID() {
				t.Fatalf("step = %+v", step)
			}
			if handlerSC.TraceState().String() != "vendor=value" {
				t.Fatal(handlerSC)
			}
			for _, s := range spans {
				if s.EndTime.IsZero() || len(s.Attributes) != 0 {
					t.Fatalf("span lifecycle or attributes = %+v", s)
				}
			}
		})
	}
}

func TestTracingCarrierFallbackAndUnsampled(t *testing.T) {
	for _, tc := range []struct {
		name, parent, state string
		valid               bool
	}{
		{"unsampled", strings.TrimSuffix(testTraceparent, "01") + "00", "vendor=value", true},
		{"bad parent", "bad", "vendor=value", false},
		{"version zero trailing delimiter", testTraceparent + "-", "", false},
		{"version zero extension", testTraceparent + "-extension", "", false},
		{"missing", "", "", false},
		{"bad state", testTraceparent, "bad state", true},
		{"oversized state", testTraceparent, strings.Repeat("x", 513), true},
		{"oversized parent", strings.Repeat("x", 513), "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exporter := traceExporter(t, sdktrace.ParentBased(sdktrace.AlwaysSample()))
			ambientCtx, ambientSpan := otel.Tracer("test").Start(context.Background(), "ambient")
			defer ambientSpan.End()
			var got trace.SpanContext
			outgoing := propagation.MapCarrier{}
			fn := Function{Handler: func(ctx Context) (any, error) {
				got = trace.SpanContextFromContext(ctx.Context())
				(propagation.TraceContext{}).Inject(ctx.Context(), outgoing)
				return nil, nil
			}}
			executor := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
			err := executor.execute(ambientCtx, &jobAssignment{FunctionID: "fn", Context: &jobContext{Traceparent: tc.parent, Tracestate: tc.state}}, &recordingReporter{})
			if err != nil {
				t.Fatal(err)
			}
			if !got.IsValid() {
				t.Fatal("missing span context")
			}
			if tc.valid != (got.TraceID().String() == "1234567890abcdef1234567890abcdef") {
				t.Fatal(got)
			}
			if got.TraceID() == ambientSpan.SpanContext().TraceID() {
				t.Fatal("inherited poll ancestry")
			}
			if tc.name == "unsampled" {
				if outgoing.Get("traceparent") != "00-1234567890abcdef1234567890abcdef-"+got.SpanID().String()+"-00" {
					t.Fatalf("outgoing carrier = %v", outgoing)
				}
				if got.IsSampled() || got.TraceState().String() != "vendor=value" || len(exporter.GetSpans()) != 0 {
					t.Fatal(got)
				}
			}
			if strings.Contains(tc.name, "state") && got.TraceState().String() != "" {
				t.Fatal(got)
			}
		})
	}
}

func TestTracingExecutionLifecycle(t *testing.T) {
	for _, mode := range []string{"success", "error", "panic", "yield", "cancel", "timeout", "replay", "branch", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			exporter := traceExporter(t, sdktrace.AlwaysSample())
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			job := &jobAssignment{RunID: "r", FunctionID: "fn", Context: &jobContext{Traceparent: testTraceparent}}
			if mode == "replay" {
				job.CompletedSteps = []completedStep{{StepID: "r:work:0", Status: "completed", Output: json.RawMessage(`"cached"`)}}
			}
			called := false
			fn := Function{Handler: func(ctx Context) (any, error) {
				if mode == "cancel" {
					cancel()
					return nil, context.Canceled
				}
				if mode == "yield" {
					return nil, Sleep(ctx, "pause", time.Second)
				}
				if mode == "branch" {
					return Parallel(ctx, "p", []func(*BranchContext) (any, error){func(b *BranchContext) (any, error) {
						return RunWithBranch(b, "work", func() (any, error) { return nil, nil })
					}})
				}
				opts := []StepOption{}
				if mode == "timeout" {
					opts = append(opts, WithTimeout(time.Millisecond))
				}
				return Run(ctx, "work", func() (string, error) {
					called = true
					switch mode {
					case "error":
						return "", errors.New("credential must stay private")
					case "panic":
						panic("private panic")
					case "timeout":
						time.Sleep(10 * time.Millisecond)
					}
					return "ok", nil
				}, opts...)
			}}
			executor := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
			if mode == "unknown" {
				job.FunctionID = "missing"
			}
			panicked := false
			func() {
				defer func() { panicked = recover() != nil }()
				if err := executor.execute(parent, job, &recordingReporter{}); err != nil {
					t.Fatal(err)
				}
			}()
			if panicked != (mode == "panic") {
				t.Fatalf("panic = %v", panicked)
			}
			spans := exporter.GetSpans()
			want := 2
			if mode == "yield" || mode == "cancel" || mode == "replay" || mode == "unknown" {
				want = 1
			}
			if len(spans) != want {
				t.Fatalf("got %d spans, want %d", len(spans), want)
			}
			if mode == "replay" && called {
				t.Fatal("replayed callback executed")
			}
			failed := mode == "error" || mode == "panic" || mode == "timeout" || mode == "unknown"
			for _, s := range spans {
				if s.EndTime.IsZero() {
					t.Fatal("unended span")
				}
				if (s.Status.Code == codes.Error) != failed {
					t.Fatalf("%s status = %v", s.Name, s.Status)
				}
				if failed && len(s.Events) != 1 {
					t.Fatalf("error events = %v", s.Events)
				}
				for _, event := range s.Events {
					for _, attr := range event.Attributes {
						if strings.Contains(attr.Value.String(), "private") {
							t.Fatal("leaked error detail")
						}
					}
				}
			}
		})
	}
}

func TestTracingPushLifecycle(t *testing.T) {
	for _, mode := range []string{"error", "panic", "yield", "replay", "serialization"} {
		t.Run(mode, func(t *testing.T) {
			exporter := traceExporter(t, sdktrace.AlwaysSample())
			fn := Function{Config: FunctionConfig{ID: "fn"}, Handler: func(ctx Context) (any, error) {
				if mode == "yield" {
					return nil, Sleep(ctx, "pause", time.Second)
				}
				if mode == "serialization" {
					return make(chan int), nil
				}
				return Run(ctx, "work", func() (string, error) {
					if mode == "replay" {
						t.Fatal("replayed callback executed")
					}
					if mode == "panic" {
						panic("private panic")
					}
					return "", errors.New("private error")
				})
			}}
			body := PushRequest{RunID: "r", FunctionID: "fn"}
			if mode == "replay" {
				body.Steps = []CompletedStep{{ID: "r:work:0", Status: "completed", Output: "cached"}}
			}
			encoded, _ := json.Marshal(body)
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(encoded))
			req.Header.Set("traceparent", testTraceparent)
			rr := httptest.NewRecorder()
			panicked := false
			func() {
				defer func() { panicked = recover() != nil }()
				Serve(ServeConfig{Functions: []Function{fn}, SkipVerification: true}).ServeHTTP(rr, req)
			}()
			if panicked != (mode == "panic") {
				t.Fatalf("panic = %v", panicked)
			}
			spans := exporter.GetSpans()
			want := 1
			if mode == "error" || mode == "panic" {
				want = 2
			}
			if len(spans) != want {
				t.Fatalf("spans = %v", spans)
			}
			failed := mode == "error" || mode == "panic" || mode == "serialization"
			for _, s := range spans {
				if s.EndTime.IsZero() || (s.Status.Code == codes.Error) != failed {
					t.Fatalf("span = %+v", s)
				}
				for _, event := range s.Events {
					for _, attr := range event.Attributes {
						if strings.Contains(attr.Value.String(), "private") {
							t.Fatal("leaked error detail")
						}
					}
				}
			}
		})
	}
}
