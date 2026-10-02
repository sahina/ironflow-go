package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func rejectAll(json.RawMessage) error { return errors.New("bad payload") }

func TestValidateEventInput(t *testing.T) {
	tests := []struct {
		name      string
		validate  func(json.RawMessage) error
		event     Event
		wantErr   string // substring; "" means no error
		wantCalls int
	}{
		{"nil validator passes", nil, Event{RawData: json.RawMessage(`{}`)}, "", 0},
		{"valid payload passes", func(json.RawMessage) error { return nil }, Event{RawData: json.RawMessage(`{}`)}, "", 1},
		{"cron tick skips validator", rejectAll, Event{Source: EventSourceCron, RawData: json.RawMessage(`{}`)}, "", 0},
		{"validator error fails", rejectAll, Event{Name: "order.placed", RawData: json.RawMessage(`{}`)}, "validation failed", 1},
		{"redacted payload fails without calling validator", rejectAll,
			Event{Name: "order.placed", RawData: json.RawMessage(`{"$redacted":true}`)}, "redacted", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			cfg := FunctionConfig{ID: "fn"}
			if tc.validate != nil {
				cfg.Validate = func(d json.RawMessage) error { calls++; return tc.validate(d) }
			}
			err := validateEventInput(Function{Config: cfg}, tc.event)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				if IsRetryable(err) {
					t.Error("validation failure must be non-retryable")
				}
			}
			if calls != tc.wantCalls {
				t.Errorf("validator calls = %d, want %d", calls, tc.wantCalls)
			}
		})
	}
}

func TestValidateEventInputWrapsValidatorError(t *testing.T) {
	sentinel := errors.New("sentinel")
	fn := Function{Config: FunctionConfig{ID: "fn", Validate: func(json.RawMessage) error { return sentinel }}}
	err := validateEventInput(fn, Event{RawData: json.RawMessage(`{}`)})
	if !errors.Is(err, sentinel) {
		t.Errorf("errors.Is(err, sentinel) = false; err = %v", err)
	}
}

func TestServeSkipsHandlerOnInvalidInput(t *testing.T) {
	ran := false
	fn := CreateFunction(FunctionConfig{
		ID: "fn", Triggers: []Trigger{{Event: "test.event"}}, Validate: rejectAll,
	}, func(Context) (any, error) { ran = true; return nil, nil })
	h := Serve(ServeConfig{Functions: []Function{fn}, SkipVerification: true})

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/ironflow", strings.NewReader(validPushBody("fn"))))

	var resp PushResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if ran {
		t.Error("handler ran despite invalid input")
	}
	if resp.Status != "failed" || resp.Error == nil || resp.Error.Retryable {
		t.Errorf("resp = %+v, want failed + non-retryable", resp)
	}
}

func TestJobExecutorSkipsHandlerOnInvalidInput(t *testing.T) {
	ran := false
	fn := Function{
		Config:  FunctionConfig{ID: "fn", Validate: rejectAll},
		Handler: func(Context) (any, error) { ran = true; return nil, nil },
	}
	exec := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
	var raw jobAssignment
	if err := json.Unmarshal([]byte(`{"job_id":"j","run_id":"r","function_id":"fn","attempt":1,
		"event":{"id":"e1","name":"e","data":{}}}`), &raw); err != nil {
		t.Fatal(err)
	}
	rep := &recordingReporter{}
	if err := exec.execute(context.Background(), &raw, rep); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if ran {
		t.Error("handler ran despite invalid input")
	}
	if rep.failed == nil || rep.failed.Retryable {
		t.Errorf("failed = %+v, want reported failed + non-retryable", rep.failed)
	}
}
