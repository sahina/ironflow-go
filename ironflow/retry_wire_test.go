package ironflow

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestRegisterFunctions_SendsMaxDelayMs pins #2160: registerFunctions
// omitted maxDelayMs from the RegisterFunction body, so the server fell
// back to its 5-minute DefaultRetryMaxDelay and every declared MaxDelay
// was silently discarded. A function that declares one must have it
// reach the wire.
func TestRegisterFunctions_SendsMaxDelayMs(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	fn := CreateFunction(FunctionConfig{
		ID:       "fn_retry_wire",
		Name:     "Retry Wire",
		Triggers: []Trigger{{Event: "x.happened"}},
		Retry: &RetryConfig{
			MaxAttempts:   2,
			InitialDelay:  30 * time.Second,
			BackoffFactor: 2.0,
			MaxDelay:      10 * time.Minute,
		},
	}, func(ctx Context) (any, error) { return nil, nil })

	err := registerFunctions(context.Background(), srv.URL, nil,
		map[string]Function{fn.Config.ID: fn}, srv.Client(), NewNoopLogger())
	if err != nil {
		t.Fatalf("registerFunctions: %v", err)
	}

	retry, ok := got["retry"].(map[string]any)
	if !ok {
		t.Fatalf("no retry object on the wire; body was %v", got)
	}
	maxDelay, present := retry["maxDelayMs"]
	if !present {
		t.Fatalf("maxDelayMs absent from the wire — the server will apply its 5m default instead of the declared 10m (#2160); retry was %v", retry)
	}
	if want := float64((10 * time.Minute).Milliseconds()); maxDelay != want {
		t.Fatalf("maxDelayMs = %v, want %v", maxDelay, want)
	}

	// The three fields that already worked must not regress.
	for k, want := range map[string]float64{
		"maxAttempts":    2,
		"initialDelayMs": float64((30 * time.Second).Milliseconds()),
		"backoffFactor":  2.0,
	} {
		if retry[k] != want {
			t.Errorf("retry[%q] = %v, want %v", k, retry[k], want)
		}
	}
}

func TestRegisterFunctions_SendsRecordingProfileWithoutLegacyBoolean(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	fn := CreateFunction(FunctionConfig{
		ID:               "fn_profile_wire",
		Name:             "Profile Wire",
		RecordingProfile: RecordingProfileSteps,
	}, func(ctx Context) (any, error) { return nil, nil })

	if err := registerFunctions(context.Background(), srv.URL, nil,
		map[string]Function{fn.Config.ID: fn}, srv.Client(), NewNoopLogger()); err != nil {
		t.Fatalf("registerFunctions: %v", err)
	}
	if got["recordingProfile"] != string(RecordingProfileSteps) {
		t.Fatalf("recordingProfile = %v, want %q", got["recordingProfile"], RecordingProfileSteps)
	}
	if _, present := got["recording"]; present {
		t.Fatalf("legacy recording boolean should be omitted for profile-only config: %v", got)
	}
}

// TestPushRequest_CarriesMaxAttempts pins the push half of the #2160
// deploy-skew fix: the run's own retry budget has to survive the wire so
// a handler can branch on RunInfo.MaxAttempts rather than a constant
// compiled into its binary.
func TestPushRequest_CarriesMaxAttempts(t *testing.T) {
	raw := []byte(`{"run_id":"run_1","function_id":"fn_1","attempt":2,"max_attempts":5,
		"event":{"id":"evt_1","name":"x.happened","version":1,"data":{}}}`)

	var req PushRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if req.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2", req.Attempt)
	}
	if req.MaxAttempts != 5 {
		t.Fatalf("MaxAttempts = %d, want 5 — the run's budget did not survive the wire (#2160)", req.MaxAttempts)
	}
}

// TestJobAssignment_CarriesMaxAttempts is the pull-mode sibling of
// TestPushRequest_CarriesMaxAttempts. An engine that predates the field
// sends nothing, which must decode as zero so the handler falls back to
// its declared value rather than to "no attempts left".
func TestJobAssignment_CarriesMaxAttempts(t *testing.T) {
	var job jobAssignment
	if err := json.Unmarshal([]byte(`{"job_id":"j","run_id":"r","function_id":"f",
		"attempt":1,"max_attempts":3,"event":{"id":"e","name":"n","version":1,"data":{}}}`), &job); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if job.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d, want 3 (#2160)", job.MaxAttempts)
	}

	var legacy jobAssignment
	if err := json.Unmarshal([]byte(`{"job_id":"j","run_id":"r","function_id":"f",
		"attempt":1,"event":{"id":"e","name":"n","version":1,"data":{}}}`), &legacy); err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}
	if legacy.MaxAttempts != 0 {
		t.Fatalf("MaxAttempts = %d from an engine without the field, want 0 so callers fall back", legacy.MaxAttempts)
	}
}
