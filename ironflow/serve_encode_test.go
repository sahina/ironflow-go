package ironflow

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
)

func servePush(t *testing.T, cfg ServeConfig) *httptest.ResponseRecorder {
	t.Helper()
	cfg.SkipVerification = true
	h := Serve(cfg)
	req := PushRequest{RunID: "run-1", FunctionID: "f", Attempt: 1, MaxAttempts: 3,
		Event: PushEvent{ID: "e1", Name: "order.created", Version: 1,
			Data: json.RawMessage(`{"a":1}`), Timestamp: "2024-01-01T00:00:00Z"}}
	b, _ := json.Marshal(req)
	r, _ := http.NewRequest("POST", "/api/ironflow", bytes.NewReader(b))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	return rr
}

func serveFn(h func(Context) (any, error)) Function {
	return Function{Config: FunctionConfig{ID: "f", Name: "f", Triggers: []Trigger{{Event: "order.created"}}}, Handler: h}
}

// A step output JSON cannot encode fails the run non-retryably and keeps the
// steps before it; it was a 200 with an empty body the engine retried, which
// re-ran the earlier side effect (#2873).
func TestServe_UnencodableStepOutputFailsKeepingEarlierSteps(t *testing.T) {
	rr := servePush(t, ServeConfig{Functions: []Function{serveFn(func(ctx Context) (any, error) {
		_, err := Run(ctx, "charge", func() (any, error) { return 1, nil }) // earlier side effect
		if err != nil {
			return nil, err
		}
		v, err := Run(ctx, "calc", func() (map[string]float64, error) { return map[string]float64{"x": math.NaN()}, nil })
		return v, err
	})}})
	var resp PushResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body %q: %v", rr.Body.String(), err)
	}
	if resp.Status != "failed" || resp.Error == nil || resp.Error.Code != "SERIALIZATION_ERROR" || resp.Error.Retryable {
		t.Fatalf("response = %s, want a non-retryable SERIALIZATION_ERROR failure", rr.Body.String())
	}
	if len(resp.Steps) != 1 || resp.Steps[0].Name != "charge" {
		t.Errorf("steps = %s, want the earlier charge step kept", rr.Body.String())
	}
}

// Push step results carry their timing, as the push protocol documents.
func TestServe_StepResultsCarryTiming(t *testing.T) {
	rr := servePush(t, ServeConfig{Functions: []Function{serveFn(func(ctx Context) (any, error) {
		return Run(ctx, "s1", func() (int, error) { return 1, nil })
	})}})
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	steps, _ := resp["steps"].([]any)
	if len(steps) != 1 {
		t.Fatalf("steps=%v", resp)
	}
	s := steps[0].(map[string]any)
	for _, k := range []string{"started_at", "ended_at", "duration_ms"} {
		if _, ok := s[k]; !ok {
			t.Errorf("step JSON lacks %s", k)
		}
	}
}

// A result JSON cannot encode fails the push run non-retryably and runs the
// compensations, keeping the steps.
func TestServe_UnencodableResultRunsCompensations(t *testing.T) {
	compensated := false
	rr := servePush(t, ServeConfig{Functions: []Function{serveFn(func(ctx Context) (any, error) {
		if _, err := Run(ctx, "charge", func() (int, error) { return 1, nil }); err != nil {
			return nil, err
		}
		Compensate(ctx, "charge", func() error { compensated = true; return nil })
		return math.Inf(-1), nil
	})}})
	var resp PushResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body %q: %v", rr.Body.String(), err)
	}
	if !compensated || resp.Status != "failed" || resp.Error == nil || resp.Error.Code != "SERIALIZATION_ERROR" || resp.Error.Retryable {
		t.Fatalf("compensated=%v response=%s", compensated, rr.Body.String())
	}
}
