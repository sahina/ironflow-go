package ironflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

func TestCorrelatedLoggerNativeAcceptance(t *testing.T) {
	var out synchronizedLogBuffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&out, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}})
	ctx := Context{exec: &executionContext{ctx: trace.ContextWithSpanContext(context.Background(), sc), runID: "full-run-12345678", functionID: "dispatch-function", attempt: 3, stepCounters: map[string]int{}}}
	_, err := RunWithContext(ctx, "fetch:part", func(step Context) (int, error) { step.Logger().Info("hello", "app", "kept"); return 42, nil })
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]any{"run_id": "full-run-12345678", "function_id": "dispatch-function", "step_id": ctx.exec.executedSteps[0].ID, "attempt": float64(3), "trace_id": sc.TraceID().String(), "app": "kept", "msg": "hello"} {
		if got[k] != want {
			t.Errorf("%s: got %v want %v", k, got[k], want)
		}
	}
}

func logTestContext() Context {
	return Context{exec: &executionContext{ctx: trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}})), runID: "run", functionID: "fn", attempt: 2, stepCounters: map[string]int{}}}
}

func captureUserLogs(t *testing.T) *synchronizedLogBuffer {
	t.Helper()
	out := new(synchronizedLogBuffer)
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(out, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return out
}

func decodeUserLogs(t *testing.T, out *synchronizedLogBuffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	dec := json.NewDecoder(bytes.NewReader(out.Bytes()))
	for dec.More() {
		var r map[string]any
		if err := dec.Decode(&r); err != nil {
			t.Fatal(err)
		}
		records = append(records, r)
	}
	return records
}

func TestCorrelatedLoggerScopeLifetime(t *testing.T) {
	for _, mode := range []string{"success", "error", "cancel", "yield", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			out := captureUserLogs(t)
			c := logTestContext()
			var cached *slog.Logger
			ready, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			invoke := func() {
				_, _ = RunWithContext(c, "outer", func(s Context) (int, error) {
					cached = s.Logger()
					cached.Info("inside")
					close(ready)
					switch mode {
					case "timeout":
						<-release
						cached.Info("late")
						close(done)
					case "error":
						return 0, errors.New("failure")
					case "cancel":
						return 0, context.Canceled
					case "yield":
						panic(newSleepYield("wait", "later"))
					}
					return 1, nil
				}, WithTimeout(100*time.Millisecond))
			}
			func() { defer func() { _ = recover() }(); invoke() }()
			<-ready
			cached.Info("expired")
			if mode == "timeout" {
				close(release)
				<-done
			}
			for i, r := range decodeUserLogs(t, out) {
				want := ""
				if i == 0 {
					want = "run:outer:0"
				}
				if r["step_id"] != want || r["trace_id"] == "" {
					t.Fatalf("record=%v", r)
				}
			}
		})
	}
}

func TestCorrelatedLoggerNestedBranches(t *testing.T) {
	out := captureUserLogs(t)
	c := logTestContext()
	_, err := Parallel(c, "fan", []func(*BranchContext) (int, error){func(b *BranchContext) (int, error) {
		return RunWithBranchContext(b, "outer", func(s Context) (int, error) {
			s.Logger().Info("outer")
			_, err := RunWithContext(s, "inner", func(inner Context) (int, error) { inner.Logger().Info("inner"); return 1, nil })
			if err != nil {
				return 0, err
			}
			_, err = Parallel(s, "nested", []func(*BranchContext) (int, error){func(child *BranchContext) (int, error) {
				return RunWithBranchContext(child, "leaf", func(leaf Context) (int, error) { leaf.Logger().Info("leaf"); return 1, nil })
			}})
			s.Logger().Info("restored")
			return 1, err
		})
	}, func(b *BranchContext) (int, error) {
		return RunWithBranchContext(b, "other", func(s Context) (int, error) { s.Logger().Info("other"); return 2, nil })
	}})
	if err != nil {
		t.Fatal(err)
	}

	records := decodeUserLogs(t, out)
	expected := map[string]string{"outer": "run:fan:0:outer:0", "restored": "run:fan:0:outer:0", "inner": "run:fan:0:inner:0", "other": "run:fan:1:other:0", "leaf": "run:fan:0:nested:0:leaf:0"}
	if len(records) != len(expected) {
		t.Fatal(records)
	}
	for _, r := range records {
		if r["step_id"] != expected[r["msg"].(string)] {
			t.Fatal(r)
		}
	}
}

func TestCorrelatedLoggerMemoizationAndLegacy(t *testing.T) {
	out := captureUserLogs(t)
	c := logTestContext()
	c.exec.completedSteps = map[string]*CompletedStep{"run:cached:0": {Status: "completed", Output: 7}}
	got, err := RunWithContext(c, "cached", func(Context) (int, error) { t.Fatal("memoized callback executed"); return 0, nil })
	if got != 7 || err != nil {
		t.Fatalf("%v %v", got, err)
	}
	_, _ = Run(c, "legacy", func() (int, error) { c.Logger().Info("legacy"); return 1, nil })
	records := decodeUserLogs(t, out)
	if len(records) != 1 || records[0]["step_id"] != "" {
		t.Fatal(records)
	}
}

func TestCorrelatedLoggerGroupsAndCollisions(t *testing.T) {
	out := captureUserLogs(t)
	c := logTestContext()
	c.Logger().With("run_id", "fake", "app", "bound").WithGroup("group").With("attempt", 999, "a", 1).Info("hello", "trace_id", "fake", "b", 2)
	r := decodeUserLogs(t, out)[0]
	if r["run_id"] != "run" || r["attempt"] != float64(2) || r["app"] != "bound" {
		t.Fatal(r)
	}
	group := r["group"].(map[string]any)
	if group["a"] != float64(1) || group["b"] != float64(2) || group["attempt"] != float64(999) {
		t.Fatal(r)
	}
	// A handler configured before the SDK wrapper owns its preexisting group.
	slog.SetDefault(slog.New(slog.NewJSONHandler(out, nil).WithGroup("opaque")))
	c.Logger().Info("pregrouped", "app", true)
	r = decodeUserLogs(t, out)[1]
	if r["opaque"].(map[string]any)["run_id"] != "run" {
		t.Fatal(r)
	}
}

type memoryLogExporter struct{ records []sdklog.Record }

func (e *memoryLogExporter) Export(_ context.Context, rs []sdklog.Record) error {
	for _, r := range rs {
		e.records = append(e.records, r.Clone())
	}
	return nil
}
func (*memoryLogExporter) Shutdown(context.Context) error   { return nil }
func (*memoryLogExporter) ForceFlush(context.Context) error { return nil }

func TestCorrelatedLoggerOTel(t *testing.T) {
	exporter := new(memoryLogExporter)
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)))
	defer provider.Shutdown(context.Background())
	old := slog.Default()
	slog.SetDefault(slog.New(otelslog.NewHandler("app", otelslog.WithLoggerProvider(provider))))
	defer slog.SetDefault(old)
	c := logTestContext()
	var want trace.SpanContext
	_, err := RunWithContext(c, "work", func(s Context) (int, error) {
		want = trace.SpanContextFromContext(s.Context())
		s.Logger().Warn("body", "app", "kept")
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(exporter.records) != 1 {
		t.Fatalf("records=%d", len(exporter.records))
	}
	r := exporter.records[0]
	if r.TraceID() != want.TraceID() || r.SpanID() != want.SpanID() || r.TraceFlags() != want.TraceFlags() || r.Body().AsString() != "body" || r.Severity() != otellog.SeverityWarn {
		t.Fatal(r)
	}
	attrs := map[string]attribute.Value{}
	r.WalkAttributes(func(kv attribute.KeyValue) bool { attrs[string(kv.Key)] = kv.Value; return true })
	if attrs["attempt"].AsInt64() != 2 || attrs["step_id"].AsString() != "run:work:0" || attrs["run_id"].AsString() != "run" || attrs["function_id"].AsString() != "fn" || attrs["trace_id"].AsString() != want.TraceID().String() || attrs["app"].AsString() != "kept" {
		t.Fatal(attrs)
	}
}

type synchronizedLogBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *synchronizedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}
func (b *synchronizedLogBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.Buffer.Bytes())
}
func TestCorrelatedLoggerTransports(t *testing.T) {
	for _, transport := range []string{"poll", "stream", "push"} {
		t.Run(transport, func(t *testing.T) {
			_ = traceExporter(t, sdktrace.AlwaysSample())
			out := captureUserLogs(t)
			exporter := new(memoryLogExporter)
			provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)))
			defer provider.Shutdown(context.Background())
			slog.SetDefault(slog.New(&logTee{native: slog.Default().Handler(), bridge: otelslog.NewHandler("app", otelslog.WithLoggerProvider(provider))}))
			ambientCtx, ambientSpan := otel.Tracer("test").Start(context.Background(), "unrelated request")
			defer ambientSpan.End()
			member, _ := baggage.NewMember("credential", "hidden")
			bag, _ := baggage.New(member)
			ambientCtx = baggage.ContextWithBaggage(ambientCtx, bag)
			var handlerSC trace.SpanContext
			var stepSC trace.SpanContext
			fn := Function{Config: FunctionConfig{ID: "fn"}, Handler: func(ctx Context) (any, error) {
				handlerSC = trace.SpanContextFromContext(ctx.Context())
				if baggage.FromContext(ctx.Context()).Len() != 0 {
					t.Fatal("inherited baggage")
				}
				ctx.Logger().Info("run")
				return RunWithContext(ctx, "work", func(step Context) (string, error) {
					stepSC = trace.SpanContextFromContext(step.Context())
					step.Logger().Info("step")
					return "done", nil
				})
			}}
			switch transport {
			case "poll":
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_ = json.NewEncoder(w).Encode(map[string]any{"jobs": []any{map[string]any{"job_id": "j", "run_id": "full-run-12345678", "function_id": "fn", "attempt": 4, "context": map[string]string{"traceparent": testTraceparent, "tracestate": "vendor=value"}}}})
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
				job, err := protoToJobAssignment(&ironflowv1.JobAssignment{JobId: "j", RunId: "full-run-12345678", FunctionId: "fn", Attempt: 4, Context: &ironflowv1.JobContext{Traceparent: testTraceparent, Tracestate: "vendor=value"}})
				if err != nil {
					t.Fatal(err)
				}
				executor := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
				if err := executor.execute(ambientCtx, job, &recordingReporter{}); err != nil {
					t.Fatal(err)
				}
			case "push":
				body, _ := json.Marshal(PushRequest{RunID: "full-run-12345678", FunctionID: "fn", Attempt: 4})
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
			records := decodeUserLogs(t, out)
			if len(records) != 2 || len(exporter.records) != 2 {
				t.Fatalf("native=%v otel=%d", records, len(exporter.records))
			}
			for i, r := range records {
				wantStep := ""
				sc := handlerSC
				if i == 1 {
					wantStep = "full-run-12345678:work:0"
					sc = stepSC
				}
				for k, want := range map[string]any{"run_id": "full-run-12345678", "function_id": "fn", "step_id": wantStep, "attempt": float64(4), "trace_id": sc.TraceID().String()} {
					if r[k] != want {
						t.Fatalf("%s got %v want %v record %v", k, r[k], want, r)
					}
				}
				lr := exporter.records[i]
				attrs := map[string]attribute.Value{}
				lr.WalkAttributes(func(kv attribute.KeyValue) bool { attrs[string(kv.Key)] = kv.Value; return true })
				if attrs["run_id"].AsString() != r["run_id"] || attrs["function_id"].AsString() != r["function_id"] || attrs["step_id"].AsString() != wantStep || attrs["attempt"].AsInt64() != 4 || attrs["trace_id"].AsString() != sc.TraceID().String() {
					t.Fatal(attrs)
				}
				if lr.TraceID() != sc.TraceID() || lr.SpanID() != sc.SpanID() {
					t.Fatal(lr)
				}
			}
		})
	}
}

type logTee struct{ native, bridge slog.Handler }

func (h *logTee) Enabled(c context.Context, l slog.Level) bool {
	return h.native.Enabled(c, l) || h.bridge.Enabled(c, l)
}
func (h *logTee) Handle(c context.Context, r slog.Record) error {
	if h.native.Enabled(c, r.Level) {
		if err := h.native.Handle(c, r.Clone()); err != nil {
			return err
		}
	}
	if h.bridge.Enabled(c, r.Level) {
		return h.bridge.Handle(c, r)
	}
	return nil
}
func (h *logTee) WithAttrs(a []slog.Attr) slog.Handler {
	return &logTee{h.native.WithAttrs(a), h.bridge.WithAttrs(a)}
}
func (h *logTee) WithGroup(g string) slog.Handler {
	return &logTee{h.native.WithGroup(g), h.bridge.WithGroup(g)}
}

func TestCorrelatedLoggerMissingContextFilteringAndPanic(t *testing.T) {
	out := captureUserLogs(t)
	c := Context{}
	c.Logger().Debug("filtered")
	c.Logger().Info("zero")
	records := decodeUserLogs(t, out)
	if len(records) != 1 {
		t.Fatal(records)
	}
	for _, key := range []string{"run_id", "function_id", "step_id", "trace_id"} {
		if records[0][key] != "" {
			t.Fatal(records)
		}
	}
	if records[0]["attempt"] != float64(0) {
		t.Fatal(records)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})))
	if c.Logger().Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("widened level")
	}
	slog.SetDefault(slog.New(&panicLogHandler{}))
	defer func() {
		if recover() != "app failure" {
			t.Fatal("handler panic was swallowed")
		}
	}()
	c.Logger().Info("panic")
}

type panicLogHandler struct{}

func (*panicLogHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (*panicLogHandler) Handle(context.Context, slog.Record) error { panic("app failure") }
func (h *panicLogHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h *panicLogHandler) WithGroup(string) slog.Handler           { return h }

func TestCorrelatedLoggerTextIdentityAndBoundAttributes(t *testing.T) {
	var out synchronizedLogBuffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, nil)))
	defer slog.SetDefault(old)
	for _, id := range []string{"first-12345678", "second-12345678"} {
		c := logTestContext()
		c.exec.runID = id
		c.Logger().With("attempt", 900).Info("visible", slog.Group("", slog.String("run_id", "fake")), "err", errors.New("native error"))
	}
	text := out.String()
	for _, want := range []string{"run_id=first-12345678", "run_id=second-12345678", "function_id=fn", "step_id=\"\"", "attempt=2", "trace_id=01000000000000000000000000000000", "native error"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in %s", want, text)
		}
	}
	if strings.Contains(text, "fake") || strings.Contains(text, "900") {
		t.Fatal(text)
	}
}

func TestCorrelatedLoggerHandlerDelegation(t *testing.T) {
	out := captureUserLogs(t)
	fn := CreateHandler(HandlerConfig[map[string]any]{Event: "trigger", Options: &HandlerOptions{ID: "dispatch-id", Name: "display-name"}, Handler: func(_ map[string]any, h *HandlerContext) (any, error) { h.Logger.Info("callback"); return nil, nil }})
	response := (&serveHandler{}).executeHandler(context.Background(), "", "", fn, &PushRequest{RunID: "handler-run", FunctionID: "dispatch-id", Attempt: 5, Event: PushEvent{Data: json.RawMessage(`{}`)}})
	if response.Status != "completed" {
		t.Fatal(response)
	}
	records := decodeUserLogs(t, out)
	if len(records) != 1 || records[0]["function_id"] != "dispatch-id" || records[0]["attempt"] != float64(5) {
		t.Fatal(records)
	}
}

func TestCorrelatedLoggerTraceValidity(t *testing.T) {
	for _, carrier := range []string{"", "invalid", "00-00000000000000000000000000000000-1234567890abcdef-01", "00-1234567890abcdef1234567890abcdef-1234567890abcdef-00"} {
		out := captureUserLogs(t)
		c := logTestContext()
		c.exec.ctx = traceExecutionContext(context.Background(), carrier, "")
		c.Logger().Info("trace")
		records := decodeUserLogs(t, out)
		want := ""
		if strings.HasSuffix(carrier, "-00") {
			want = "1234567890abcdef1234567890abcdef"
		}
		if records[0]["trace_id"] != want {
			t.Fatal(records)
		}
	}
}

func TestCorrelatedLoggerInterleavedBranches(t *testing.T) {
	out := captureUserLogs(t)
	c := logTestContext()
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	go func() { <-ready; <-ready; close(release) }()
	branches := make([]func(*BranchContext) (int, error), 2)
	for i := range branches {
		branches[i] = func(b *BranchContext) (int, error) {
			return RunWithBranchContext(b, "same", func(s Context) (int, error) {
				logger := s.Logger().With("app", "retained")
				ready <- struct{}{}
				<-release
				logger.Info("interleaved")
				return 1, nil
			})
		}
	}
	if _, err := Parallel(c, "fan", branches); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, r := range decodeUserLogs(t, out) {
		seen[r["step_id"].(string)] = true
	}
	if len(seen) != 2 || !seen["run:fan:0:same:0"] || !seen["run:fan:1:same:0"] {
		t.Fatal(seen)
	}
}

func TestCorrelatedLoggerNestedTraceContext(t *testing.T) {
	exporter := traceExporter(t, sdktrace.AlwaysSample())
	c := logTestContext()
	var outerSC, innerSC trace.SpanContext
	_, err := RunWithContext(c, "outer", func(outer Context) (int, error) {
		outerSC = trace.SpanContextFromContext(outer.Context())
		return RunWithContext(outer, "inner", func(inner Context) (int, error) {
			innerSC = trace.SpanContextFromContext(inner.Context())
			return 1, nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 2 || spans[0].SpanContext.SpanID() != innerSC.SpanID() || spans[0].Parent.SpanID() != outerSC.SpanID() || spans[1].Parent.SpanID() != trace.SpanContextFromContext(c.Context()).SpanID() {
		t.Fatal(spans)
	}
}

type changingLogValue struct{ value *string }

func (v changingLogValue) LogValue() slog.Value { return slog.StringValue(*v.value) }

func TestCorrelatedLoggerWithResolvesBoundValues(t *testing.T) {
	out := captureUserLogs(t)
	value := "bound"
	logger := logTestContext().Logger().With(slog.Group("app", slog.Any("value", changingLogValue{&value})))
	value = "changed"
	logger.Info("bound-value")
	records := decodeUserLogs(t, out)
	if records[0]["app"].(map[string]any)["value"] != "bound" {
		t.Fatal(records)
	}
}
