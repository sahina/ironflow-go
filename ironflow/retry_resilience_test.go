package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

// registerBody registers one function against a stub server and returns the
// RegisterFunction body it sent.
func registerBody(t *testing.T, cfg FunctionConfig) map[string]any {
	t.Helper()
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	cfg.ID = "fn_resilience"
	cfg.Triggers = []Trigger{{Event: "x"}}
	fn := CreateFunction(cfg, func(ctx Context) (any, error) { return nil, nil })
	if err := registerFunctions(context.Background(), srv.URL, nil, map[string]Function{fn.Config.ID: fn}, srv.Client(), NewNoopLogger()); err != nil {
		t.Fatal(err)
	}
	return got
}

// Unset retry fields must not reach the wire: the server resolves them from
// spec.engine.retry.* when each run is created (#3151).
func TestRegister_SendsNoRetryOrTimeoutWhenUnset(t *testing.T) {
	got := registerBody(t, FunctionConfig{})
	for _, k := range []string{"retry", "timeoutMs", "circuitBreaker", "throttle"} {
		if v, present := got[k]; present {
			t.Errorf("%s = %v sent although the user set nothing", k, v)
		}
	}
}

func TestRegister_SendsOnlyTheRetryFieldsTheUserSet(t *testing.T) {
	got := registerBody(t, FunctionConfig{Retry: &RetryConfig{OnExhausted: OnExhaustedFail}})
	retry, ok := got["retry"].(map[string]any)
	if !ok {
		t.Fatalf("retry missing from body %v", got)
	}
	if len(retry) != 1 || retry["onExhausted"] != "fail" {
		t.Errorf("retry = %v, want only onExhausted=fail", retry)
	}

	got = registerBody(t, FunctionConfig{Retry: &RetryConfig{MaxAttempts: 7}})
	retry = got["retry"].(map[string]any)
	if len(retry) != 1 || retry["maxAttempts"] != float64(7) {
		t.Errorf("retry = %v, want only maxAttempts=7", retry)
	}
}

func TestRegister_SendsCircuitBreakerAndThrottle(t *testing.T) {
	got := registerBody(t, FunctionConfig{
		Retry: &RetryConfig{
			MaxAttempts: 4, InitialDelay: 2 * time.Second, BackoffFactor: 3,
			MaxDelay: time.Minute, OnExhausted: OnExhaustedPause,
		},
		CircuitBreaker: &CircuitBreakerConfig{
			FailureThreshold: 8, OpenDuration: 30 * time.Second, MaxOpenDuration: 20 * time.Minute,
		},
		Throttle: &ThrottleConfig{Limit: 10, Period: 5 * time.Second, Key: "customerId"},
	})

	wantRetry := map[string]any{
		"maxAttempts": float64(4), "initialDelayMs": float64(2000), "backoffFactor": float64(3),
		"maxDelayMs": float64(60000), "onExhausted": "pause",
	}
	assertJSONMap(t, "retry", got["retry"], wantRetry)
	assertJSONMap(t, "circuitBreaker", got["circuitBreaker"], map[string]any{
		"failureThreshold": float64(8), "openDurationMs": float64(30000), "maxOpenDurationMs": float64(1200000),
	})
	assertJSONMap(t, "throttle", got["throttle"], map[string]any{
		"limit": float64(10), "periodMs": float64(5000), "key": "customerId",
	})
}

func assertJSONMap(t *testing.T, name string, got any, want map[string]any) {
	t.Helper()
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("%s = %v, want an object", name, got)
	}
	if len(m) != len(want) {
		t.Errorf("%s = %v, want %v", name, m, want)
		return
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s[%q] = %v, want %v", name, k, m[k], v)
		}
	}
}

func TestGetFunctionMetadata_ResilienceFields(t *testing.T) {
	unset := GetFunctionMetadata(CreateFunction(FunctionConfig{ID: "u", Triggers: []Trigger{{Event: "x"}}},
		func(ctx Context) (any, error) { return nil, nil }))
	for _, k := range []string{"retry", "circuit_breaker", "throttle"} {
		if _, present := unset[k]; present {
			t.Errorf("%s present in metadata although unset", k)
		}
	}
	if unset["timeout_ms"] != int64(0) {
		t.Errorf("timeout_ms = %v, want 0", unset["timeout_ms"])
	}

	md := GetFunctionMetadata(CreateFunction(FunctionConfig{
		ID: "s", Triggers: []Trigger{{Event: "x"}},
		Retry:          &RetryConfig{OnExhausted: OnExhaustedFail},
		CircuitBreaker: &CircuitBreakerConfig{FailureThreshold: 3},
		Throttle:       &ThrottleConfig{Limit: 2, Period: 2 * time.Second, Key: "a.b"},
	}, func(ctx Context) (any, error) { return nil, nil }))
	if r := md["retry"].(map[string]any); len(r) != 1 || r["on_exhausted"] != "fail" {
		t.Errorf("retry = %v, want only on_exhausted=fail", r)
	}
	if cb := md["circuit_breaker"].(map[string]any); cb["failure_threshold"] != 3 || cb["open_duration_ms"] != int64(0) {
		t.Errorf("circuit_breaker = %v", cb)
	}
	if th := md["throttle"].(map[string]any); th["limit"] != 2 || th["period_ms"] != int64(2000) || th["key"] != "a.b" {
		t.Errorf("throttle = %v", th)
	}
}

func TestValidateFunctionConfig_ResilienceFields(t *testing.T) {
	base := func() FunctionConfig {
		return FunctionConfig{ID: "v", Triggers: []Trigger{{Event: "x"}}}
	}
	tests := []struct {
		name    string
		mutate  func(*FunctionConfig)
		wantErr string // empty: valid
	}{
		{"on_exhausted only is valid", func(c *FunctionConfig) { c.Retry = &RetryConfig{OnExhausted: OnExhaustedFail} }, ""},
		{"empty retry config is valid", func(c *FunctionConfig) { c.Retry = &RetryConfig{} }, ""},
		{"unknown on_exhausted", func(c *FunctionConfig) { c.Retry = &RetryConfig{OnExhausted: "retry"} }, "onExhausted"},
		{"negative max attempts", func(c *FunctionConfig) { c.Retry = &RetryConfig{MaxAttempts: -1} }, "maxAttempts"},
		{"backoff below 1", func(c *FunctionConfig) { c.Retry = &RetryConfig{BackoffFactor: 0.5} }, "backoffFactor"},
		{"negative max delay", func(c *FunctionConfig) { c.Retry = &RetryConfig{MaxDelay: -time.Second} }, "maxDelay"},
		{"breaker defaults are valid", func(c *FunctionConfig) { c.CircuitBreaker = &CircuitBreakerConfig{} }, ""},
		{"breaker negative threshold", func(c *FunctionConfig) { c.CircuitBreaker = &CircuitBreakerConfig{FailureThreshold: -1} }, "circuitBreaker"},
		{"breaker max below open", func(c *FunctionConfig) {
			c.CircuitBreaker = &CircuitBreakerConfig{OpenDuration: time.Minute, MaxOpenDuration: time.Second}
		}, "maxOpenDuration"},
		{"breaker max alone is valid", func(c *FunctionConfig) {
			c.CircuitBreaker = &CircuitBreakerConfig{MaxOpenDuration: time.Second}
		}, ""},
		{"throttle valid", func(c *FunctionConfig) { c.Throttle = &ThrottleConfig{Limit: 1, Period: time.Second, Key: "$.a.0.b"} }, ""},
		{"throttle limit zero", func(c *FunctionConfig) { c.Throttle = &ThrottleConfig{Period: time.Second} }, "limit"},
		{"throttle period under 1s", func(c *FunctionConfig) { c.Throttle = &ThrottleConfig{Limit: 1, Period: 999 * time.Millisecond} }, "period"},
		{"throttle key leading dot", func(c *FunctionConfig) {
			c.Throttle = &ThrottleConfig{Limit: 1, Period: time.Second, Key: ".a"}
		}, "key"},
		{"throttle key empty segment", func(c *FunctionConfig) {
			c.Throttle = &ThrottleConfig{Limit: 1, Period: time.Second, Key: "a..b"}
		}, "key"},
		{"throttle key bare root prefix", func(c *FunctionConfig) {
			c.Throttle = &ThrottleConfig{Limit: 1, Period: time.Second, Key: "$."}
		}, "key"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(&cfg)
			err := validateFunctionConfig(cfg)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

// Unset Timeout registers nothing: the engine never applied the old 10m
// default (#3148).
func TestTimeout_UnsetRegistersZero(t *testing.T) {
	fn := CreateFunction(FunctionConfig{ID: "cov-fn", Triggers: []Trigger{{Event: "cov.evt"}}},
		func(ctx Context) (any, error) { return nil, nil })
	if v := GetFunctionMetadata(fn)["timeout_ms"]; v != int64(0) {
		t.Fatalf("timeout_ms = %v, want 0", v)
	}
}

func TestTimeout_SetStillRegistered(t *testing.T) {
	got := registerBody(t, FunctionConfig{Timeout: 90 * time.Second})
	if got["timeoutMs"] != float64(90000) {
		t.Fatalf("timeoutMs = %v, want 90000", got["timeoutMs"])
	}
}

func pushOnce(t *testing.T, fn Function, attempt, maxAttempts int, onExhausted string) PushResponse {
	t.Helper()
	req := PushRequest{
		RunID: "run-001", FunctionID: fn.Config.ID, Attempt: attempt,
		MaxAttempts: maxAttempts, OnExhausted: onExhausted,
		Event: PushEvent{ID: "evt-001", Name: "test.event", Version: 1, Data: json.RawMessage(`{}`), Timestamp: "2024-01-01T00:00:00Z"},
	}
	body, _ := json.Marshal(req)
	handler := Serve(ServeConfig{Functions: []Function{fn}, SkipVerification: true})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(string(body))))
	var resp PushResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return resp
}

func TestPush_RetryAfterReachesTheWire(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int64
	}{
		{"IronflowError.RetryAfter", &IronflowError{Message: "limited", Code: "RATE_LIMITED", Retryable: true, RetryAfter: 30 * time.Second}, 30000},
		{"NewRetryAfterError", NewRetryAfterError("slow down", 1500*time.Millisecond), 1500},
		{"wrapped", fmt.Errorf("charge: %w", NewRetryAfterError("slow down", 2*time.Second)), 2000},
		{"sub-millisecond rounds up", NewRetryAfterError("slow down", time.Microsecond), 1},
		{"zero is no hint", NewRetryAfterError("slow down", 0), 0},
		{"negative is no hint", NewRetryAfterError("slow down", -time.Second), 0},
		{"non-retryable carries none", &IronflowError{Message: "no", Retryable: false, RetryAfter: time.Minute}, 0},
		{"plain error", errors.New("boom"), 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fn := CreateFunction(FunctionConfig{ID: "ra-fn", Triggers: []Trigger{{Event: "test.event"}}},
				func(ctx Context) (any, error) { return nil, tc.err })
			resp := pushOnce(t, fn, 1, 3, "")
			if resp.Status != "failed" || resp.Error == nil {
				t.Fatalf("response = %+v, want failed with an error", resp)
			}
			if resp.Error.RetryAfterMs != tc.want {
				t.Errorf("retry_after_ms = %d, want %d", resp.Error.RetryAfterMs, tc.want)
			}
		})
	}
}

func TestPush_RetryAfterJSONName(t *testing.T) {
	fn := CreateFunction(FunctionConfig{ID: "ra-fn", Triggers: []Trigger{{Event: "test.event"}}},
		func(ctx Context) (any, error) { return nil, NewRetryAfterError("slow down", 30*time.Second) })
	handler := Serve(ServeConfig{Functions: []Function{fn}, SkipVerification: true})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(validPushBody("ra-fn"))))
	var raw struct {
		Error map[string]any `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if raw.Error["retry_after_ms"] != float64(30000) || raw.Error["retryable"] != true {
		t.Errorf("error = %v, want retryable with retry_after_ms=30000", raw.Error)
	}
}

func TestPush_RunInfoCarriesBudgetAndPolicy(t *testing.T) {
	var got RunInfo
	fn := CreateFunction(FunctionConfig{ID: "ri-fn", Triggers: []Trigger{{Event: "test.event"}}},
		func(ctx Context) (any, error) { got = ctx.Run; return nil, nil })
	pushOnce(t, fn, 2, 25, OnExhaustedPause)
	if got.MaxAttempts != 25 || got.OnExhausted != OnExhaustedPause || got.Attempt != 2 {
		t.Errorf("RunInfo = %+v, want attempt 2 of 25 with on_exhausted=pause", got)
	}
}

// Compensations run when a retryable error is the run's end: the last attempt
// under on_exhausted=fail. Under pause the run is held, not finished, so the
// completed work must stay.
func TestPush_CompensationsOnExhaustion(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		attempt     int
		maxAttempts int
		onExhausted string
		wantComp    bool
	}{
		{"fail policy, last attempt", io.ErrUnexpectedEOF, 3, 3, OnExhaustedFail, true},
		{"fail policy, beyond budget", io.ErrUnexpectedEOF, 4, 3, OnExhaustedFail, true},
		{"fail policy, attempts left", io.ErrUnexpectedEOF, 2, 3, OnExhaustedFail, false},
		{"pause policy, last attempt", io.ErrUnexpectedEOF, 3, 3, OnExhaustedPause, false},
		{"older engine (no policy), last attempt", io.ErrUnexpectedEOF, 3, 3, "", false},
		{"fail policy, unknown budget", io.ErrUnexpectedEOF, 1, 0, OnExhaustedFail, false},
		{"non-retryable always compensates", NewNonRetryableError("nope"), 1, 3, OnExhaustedPause, true},
		{"non-retryable with no policy", NewNonRetryableError("nope"), 1, 3, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compensated := false
			fn := CreateFunction(FunctionConfig{ID: "exh-fn", Triggers: []Trigger{{Event: "test.event"}}},
				func(ctx Context) (any, error) {
					_, _ = Run(ctx, "charge", func() (string, error) { return "tx", nil })
					Compensate(ctx, "charge", func() error { compensated = true; return nil })
					return nil, tc.err
				})
			resp := pushOnce(t, fn, tc.attempt, tc.maxAttempts, tc.onExhausted)
			if resp.Status != "failed" {
				t.Fatalf("status = %q, want failed", resp.Status)
			}
			if compensated != tc.wantComp {
				t.Errorf("compensated = %v, want %v", compensated, tc.wantComp)
			}
		})
	}
}

func runPullJob(t *testing.T, fn Function, jobJSON string) *recordingReporter {
	t.Helper()
	exec := &jobExecutor{functions: map[string]Function{fn.Config.ID: fn}, logger: NewNoopLogger()}
	var job jobAssignment
	if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
		t.Fatal(err)
	}
	rep := &recordingReporter{}
	if err := exec.execute(context.Background(), &job, rep); err != nil {
		t.Fatalf("execute: %v", err)
	}
	return rep
}

func TestPull_CompensationsOnExhaustionAndRetryAfter(t *testing.T) {
	const eventJSON = `"event":{"id":"e1","name":"e","data":{}}`
	var compensated bool
	var seen RunInfo
	newFn := func(err error) Function {
		compensated = false
		return CreateFunction(FunctionConfig{ID: "pull-fn", Triggers: []Trigger{{Event: "e"}}},
			func(ctx Context) (any, error) {
				seen = ctx.Run
				_, _ = Run(ctx, "charge", func() (string, error) { return "tx", nil })
				Compensate(ctx, "charge", func() error { compensated = true; return nil })
				return nil, err
			})
	}

	rep := runPullJob(t, newFn(NewRetryAfterError("slow", 45*time.Second)),
		`{"job_id":"j","run_id":"r","function_id":"pull-fn","attempt":3,"max_attempts":3,"on_exhausted":"fail",`+eventJSON+`}`)
	if !compensated {
		t.Error("fail policy on the last attempt: compensations did not run")
	}
	if seen.OnExhausted != "fail" || seen.MaxAttempts != 3 {
		t.Errorf("RunInfo = %+v, want on_exhausted=fail max_attempts=3 from the job JSON", seen)
	}
	if rep.failed == nil || rep.failed.RetryAfterMs != 45000 {
		t.Errorf("reported error = %+v, want retry_after_ms=45000", rep.failed)
	}

	runPullJob(t, newFn(io.ErrUnexpectedEOF),
		`{"job_id":"j","run_id":"r","function_id":"pull-fn","attempt":3,"max_attempts":3,"on_exhausted":"pause",`+eventJSON+`}`)
	if compensated {
		t.Error("pause policy: compensations ran on a run that is only held")
	}

	runPullJob(t, newFn(io.ErrUnexpectedEOF),
		`{"job_id":"j","run_id":"r","function_id":"pull-fn","attempt":3,"max_attempts":3,`+eventJSON+`}`)
	if compensated {
		t.Error("no policy (older engine): compensations ran on a retryable error")
	}
}

func TestPull_RESTJobUpdateCarriesRetryAfterMs(t *testing.T) {
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	rep := &httpJobReporter{worker: newTestWorker(server.URL)}
	err := rep.ReportFailed(context.Background(), "job-1",
		&PushError{Message: "slow", Retryable: true, RetryAfterMs: 12000}, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	errObj, _ := gotBody["error"].(map[string]any)
	if errObj["retry_after_ms"] != float64(12000) || errObj["retryable"] != true {
		t.Errorf("job update error = %v, want retryable with retry_after_ms=12000", errObj)
	}
}

func TestStreaming_JobAssignmentAndFailureCarryNewFields(t *testing.T) {
	job := makeJobAssignment("job-ra", "run-ra", "ra-fn", map[string]any{})
	job.MaxAttempts = 5
	job.OnExhausted = "fail"
	converted, err := protoToJobAssignment(job)
	if err != nil {
		t.Fatal(err)
	}
	if converted.OnExhausted != "fail" || converted.MaxAttempts != 5 {
		t.Errorf("converted = max %d on_exhausted %q, want 5 and fail", converted.MaxAttempts, converted.OnExhausted)
	}

	handler := &mockWorkerHandler{}
	var failed atomic.Pointer[ironflowv1.Error]
	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		if _, err := doRegistrationHandshake(stream, handler, 30000); err != nil {
			return err
		}
		if err := stream.Send(&ironflowv1.EngineMessage{Payload: &ironflowv1.EngineMessage_Job{Job: job}}); err != nil {
			return err
		}
		for {
			msg, err := stream.Receive()
			if err != nil {
				return err
			}
			if jf, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_JobFailed); ok {
				failed.Store(jf.JobFailed.GetError())
				<-ctx.Done()
				return nil
			}
		}
	}
	server := startMockServer(t, handler)

	fn := testFn("ra-fn", func(ctx Context) (any, error) {
		return nil, NewRetryAfterError("slow down", 30*time.Second)
	})
	w := NewStreamingWorker(WorkerConfig{
		ServerURL: server.URL, Functions: []Function{fn},
		ReconnectDelay: 50 * time.Millisecond, Logger: NewNoopLogger(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	deadline := time.After(4 * time.Second)
	for failed.Load() == nil {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for JobFailed")
		case <-time.After(10 * time.Millisecond):
		}
	}
	e := failed.Load()
	if e.GetRetryAfterMs() != 30000 || !e.GetRetryable() {
		t.Errorf("JobFailed.error = retryable %v retry_after_ms %d, want true and 30000", e.GetRetryable(), e.GetRetryAfterMs())
	}
}

func TestNewRetryAfterError(t *testing.T) {
	err := NewRetryAfterError("later", 5*time.Second)
	if !IsRetryable(err) || err.RetryAfter != 5*time.Second || err.Message != "later" {
		t.Errorf("err = %+v, want retryable with RetryAfter 5s", err)
	}
}

// The HTTP-date form of Retry-After is accepted; a past date, a negative delta
// and garbage all clamp to zero, as the JS SDK does.
func TestParseRetryAfter_HTTPDate(t *testing.T) {
	when := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	got := restErrorFor(t, 503, when, `{"error":"x"}`).RetryAfter
	if got < 25*time.Second || got > 31*time.Second {
		t.Fatalf("HTTP-date Retry-After parsed as %v, want ~30s", got)
	}
}

func TestParseRetryAfter_ClampsNegativeAndPast(t *testing.T) {
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	for _, h := range []string{past, "soon", "-5"} {
		if got := restErrorFor(t, 503, h, `{"error":"x"}`).RetryAfter; got != 0 {
			t.Errorf("Retry-After %q surfaced RetryAfter=%v, want 0", h, got)
		}
		if got := parseRetryAfter(h); got != 0 {
			t.Errorf("parseRetryAfter(%q) = %v, want 0", h, got)
		}
	}
	if got := parseRetryAfter("7"); got != 7*time.Second {
		t.Errorf("parseRetryAfter(7) = %v, want 7s", got)
	}
}

// A "forever" delay must not wrap negative on the wire.
func TestRetryAfterMs_HugeDurationDoesNotOverflow(t *testing.T) {
	err := NewRetryAfterError("x", time.Duration(math.MaxInt64))
	if got := retryAfterMs(err, true); got <= 0 {
		t.Fatalf("retryAfterMs(MaxInt64 duration) = %d, want a positive value", got)
	}
	if got := retryAfterMs(NewRetryAfterError("x", 1500*time.Microsecond), true); got != 2 {
		t.Fatalf("retryAfterMs(1.5ms) = %d, want 2 (rounded up)", got)
	}
}
