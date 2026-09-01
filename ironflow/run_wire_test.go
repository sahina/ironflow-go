package ironflow

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// The run APIs decode protobuf JSON: IronflowService runs on Connect's default
// codec, which emits lowerCamel field names, canonical RUN_STATUS_* enum names,
// and omits zero-valued fields entirely. These tests pin that contract (#1919).

// runWireClient wraps newTestClient (publish_test.go) so each test can defer a
// single closer.
func runWireClient(t *testing.T, handler http.HandlerFunc) (*Client, func()) {
	t.Helper()
	client, server := newTestClient(t, handler)
	return client, server.Close
}

// The defect this issue fixes: the SDK read snake_case, the server sends
// lowerCamel, and every affected field decoded to its zero value with no error.
// A snake_case body must not populate the run.
func TestRunResponseRejectsLegacySnakeCase(t *testing.T) {
	body := `{
		"id": "run-1",
		"function_id": "fn-legacy",
		"event_id": "evt-legacy",
		"status": "RUN_STATUS_COMPLETED",
		"max_attempts": 7,
		"started_at": "2025-01-01T00:00:00Z"
	}`

	client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	})
	defer closeFn()

	run, err := client.GetRun(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("GetRun = %v", err)
	}

	// If any of these ever populate, the snake_case tags have crept back.
	if run.FunctionID != "" {
		t.Errorf("function_id must not decode, got %q", run.FunctionID)
	}
	if run.EventID != "" {
		t.Errorf("event_id must not decode, got %q", run.EventID)
	}
	if run.MaxAttempts != 0 {
		t.Errorf("max_attempts must not decode, got %d", run.MaxAttempts)
	}
	if run.StartedAt != nil {
		t.Errorf("started_at must not decode, got %v", run.StartedAt)
	}
}

// Connect marshals with EmitUnpopulated:false, so a zero attempt, an empty
// eventId and an unfinished run's endedAt are absent keys, not zero values.
// Decoding must succeed and leave the Go zero values in place.
func TestGetRunOmittedZeroFields(t *testing.T) {
	client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"run-1","functionId":"fn-1","status":"RUN_STATUS_RUNNING","createdAt":"2025-01-01T00:00:00Z"}`))
	})
	defer closeFn()

	run, err := client.GetRun(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("GetRun = %v", err)
	}
	if run.Status != RunStatusRunning {
		t.Errorf("status = %q, want running", run.Status)
	}
	if run.Attempt != 0 || run.EventID != "" || run.EndedAt != nil {
		t.Errorf("omitted fields should stay zero, got attempt=%d eventId=%q endedAt=%v",
			run.Attempt, run.EventID, run.EndedAt)
	}
	if run.CreatedAt.IsZero() {
		t.Error("createdAt should have parsed")
	}
}

// A status the SDK cannot name is an error, never a silently different status.
// RUN_STATUS_UNSPECIFIED reaches the client as an ABSENT key, because Connect
// omits the zero enum value — so the empty-string case is the one that matters.
func TestRunStatusStrictPolicy(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"absent status (UNSPECIFIED on the wire)", `{"id":"run-1","functionId":"fn-1"}`},
		{"explicit unspecified", `{"id":"run-1","status":"RUN_STATUS_UNSPECIFIED"}`},
		{"retired pending", `{"id":"run-1","status":"RUN_STATUS_PENDING"}`},
		{"unknown future status", `{"id":"run-1","status":"RUN_STATUS_QUARANTINED"}`},
		{"lowercase public value", `{"id":"run-1","status":"completed"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			})
			defer closeFn()

			if _, err := client.GetRun(context.Background(), "run-1"); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}
}

// One unreadable run fails the whole page rather than returning a Runs slice
// that silently disagrees with TotalCount. The error names the offending run.
func TestListRunsFailsOnBadStatusAndNamesTheRun(t *testing.T) {
	client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"runs":[
			{"id":"run-ok","functionId":"fn-1","status":"RUN_STATUS_COMPLETED"},
			{"id":"run-bad","functionId":"fn-2","status":"RUN_STATUS_NONSENSE"}
		],"totalCount":2}`))
	})
	defer closeFn()

	_, err := client.ListRuns(context.Background(), nil)
	if err == nil {
		t.Fatal("expected an error for the unreadable run")
	}
	if !strings.Contains(err.Error(), "run-bad") {
		t.Errorf("error should name the offending run, got %q", err.Error())
	}
}

// A decode-shape mismatch leaves the run's ID empty too, so naming the run is
// useless in exactly the case that triggers this branch most often. Fall back to
// the index so the operator still knows which row to look at.
func TestListRunsNamesTheIndexWhenTheRunIDIsEmpty(t *testing.T) {
	client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"runs":[
			{"id":"run-ok","functionId":"fn-1","status":"RUN_STATUS_COMPLETED"},
			{"functionId":"fn-2","status":"RUN_STATUS_NONSENSE"}
		],"totalCount":2}`))
	})
	defer closeFn()

	_, err := client.ListRuns(context.Background(), nil)
	if err == nil {
		t.Fatal("expected an error for the unreadable run")
	}
	if !strings.Contains(err.Error(), "run at index 1") {
		t.Errorf("error should name the index when the ID is empty, got %q", err.Error())
	}
	// The empty-string form is the bug this branch exists to avoid.
	if strings.Contains(err.Error(), `run ""`) {
		t.Errorf("error names an empty run ID instead of the index: %q", err.Error())
	}
}

// The status filter lands on a protobuf enum field. RunStatusPending has no
// wire equivalent (the proto reserves the name), so it must fail client-side
// rather than reach the server as a value it silently discards.
func TestListRunsStatusFilterEncoding(t *testing.T) {
	for status, want := range map[RunStatus]string{
		RunStatusRunning:            "RUN_STATUS_RUNNING",
		RunStatusCompleted:          "RUN_STATUS_COMPLETED",
		RunStatusFailed:             "RUN_STATUS_FAILED",
		RunStatusCancelled:          "RUN_STATUS_CANCELLED",
		RunStatusPaused:             "RUN_STATUS_PAUSED",
		RunStatusWaitingForCapacity: "RUN_STATUS_WAITING_FOR_CAPACITY",
		RunStatusWaiting:            "RUN_STATUS_WAITING",
	} {
		t.Run(string(status), func(t *testing.T) {
			var got map[string]any
			client, closeFn := runWireClient(t, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(body, &got)
				_, _ = w.Write([]byte(`{"runs":[]}`))
			})
			defer closeFn()

			if _, err := client.ListRuns(context.Background(), &ListRunsOptions{Status: status}); err != nil {
				t.Fatalf("ListRuns = %v", err)
			}
			if got["status"] != want {
				t.Errorf("wire status = %v, want %s", got["status"], want)
			}
		})
	}

	t.Run("rejects the retired pending filter", func(t *testing.T) {
		client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
			t.Error("server should not have been called")
			_, _ = w.Write([]byte(`{"runs":[]}`))
		})
		defer closeFn()

		if _, err := client.ListRuns(context.Background(), &ListRunsOptions{Status: RunStatusPending}); err == nil {
			t.Fatal("expected RunStatusPending to be rejected client-side")
		}
	})
}

// ResumeRun had no response-decoding coverage before #1919.
func TestResumeRunDecodesConnectShape(t *testing.T) {
	var gotPath string
	var gotBody map[string]any

	client, closeFn := runWireClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		_, _ = w.Write([]byte(`{
			"id": "run-resume",
			"functionId": "fn-resume",
			"eventId": "evt-resume",
			"status": "RUN_STATUS_RUNNING",
			"attempt": 2,
			"maxAttempts": 3,
			"input": {"order": "ORD-1"},
			"startedAt": "2025-01-01T00:00:00Z",
			"createdAt": "2025-01-01T00:00:00Z",
			"updatedAt": "2025-01-01T00:02:00Z"
		}`))
	})
	defer closeFn()

	run, err := client.ResumeRun(context.Background(), "run-resume", "step-xyz")
	if err != nil {
		t.Fatalf("ResumeRun = %v", err)
	}

	if gotPath != "/ironflow.v1.IronflowService/ResumeRun" {
		t.Errorf("path = %s", gotPath)
	}
	// Request field names stay snake_case: protojson accepts the proto name.
	if gotBody["run_id"] != "run-resume" || gotBody["from_step"] != "step-xyz" {
		t.Errorf("request body = %#v", gotBody)
	}

	if run.FunctionID != "fn-resume" || run.EventID != "evt-resume" {
		t.Errorf("ids = %q/%q", run.FunctionID, run.EventID)
	}
	if run.Status != RunStatusRunning {
		t.Errorf("status = %q, want running", run.Status)
	}
	if run.Attempt != 2 || run.MaxAttempts != 3 {
		t.Errorf("attempt=%d maxAttempts=%d", run.Attempt, run.MaxAttempts)
	}
	if run.StartedAt == nil {
		t.Error("startedAt should have parsed")
	}
	if run.CreatedAt.IsZero() || run.UpdatedAt.IsZero() {
		t.Error("createdAt/updatedAt should have parsed")
	}
}

// ResumeRun omits from_step when resuming from the last successful step.
func TestResumeRunOmitsFromStep(t *testing.T) {
	var gotBody map[string]any
	client, closeFn := runWireClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		_, _ = w.Write([]byte(`{"id":"run-1","status":"RUN_STATUS_RUNNING"}`))
	})
	defer closeFn()

	if _, err := client.ResumeRun(context.Background(), "run-1", ""); err != nil {
		t.Fatalf("ResumeRun = %v", err)
	}
	if _, present := gotBody["from_step"]; present {
		t.Errorf("from_step should be absent, got %#v", gotBody)
	}
}

// CancelRun and GetRun share mapRunResponse; pin the enum round-trip for the
// terminal statuses a cancel can produce.
func TestCancelRunDecodesCanonicalStatus(t *testing.T) {
	client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"run-1","functionId":"fn-1","status":"RUN_STATUS_CANCELLED","endedAt":"2025-01-01T00:05:00Z"}`))
	})
	defer closeFn()

	run, err := client.CancelRun(context.Background(), "run-1", "operator request")
	if err != nil {
		t.Fatalf("CancelRun = %v", err)
	}
	if run.Status != RunStatusCancelled {
		t.Errorf("status = %q, want cancelled", run.Status)
	}
	if run.EndedAt == nil {
		t.Error("endedAt should have parsed")
	}
}

// mapRunResponse's error return changed the signature of all four callers, so
// each one has to actually propagate it. GetRun and ListRuns are covered above;
// these pin the other two.
func TestCancelAndResumeRunPropagateStatusErrors(t *testing.T) {
	const badStatus = `{"id":"run-1","functionId":"fn-1","status":"RUN_STATUS_NONSENSE"}`

	t.Run("CancelRun", func(t *testing.T) {
		client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(badStatus))
		})
		defer closeFn()

		run, err := client.CancelRun(context.Background(), "run-1", "reason")
		if err == nil {
			t.Fatal("expected an error for the unreadable status")
		}
		if run != nil {
			t.Errorf("expected a nil run alongside the error, got %#v", run)
		}
	})

	t.Run("ResumeRun", func(t *testing.T) {
		client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(badStatus))
		})
		defer closeFn()

		run, err := client.ResumeRun(context.Background(), "run-1", "")
		if err == nil {
			t.Fatal("expected an error for the unreadable status")
		}
		if run != nil {
			t.Errorf("expected a nil run alongside the error, got %#v", run)
		}
	})
}

// Timestamps arrive as protojson RFC3339, which may carry up to nine
// fractional digits.
func TestRunTimestampsParseFractionalSeconds(t *testing.T) {
	client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"run-1","status":"RUN_STATUS_COMPLETED","createdAt":"2025-01-01T00:00:00.123456789Z"}`))
	})
	defer closeFn()

	run, err := client.GetRun(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("GetRun = %v", err)
	}
	if run.CreatedAt.Nanosecond() != 123456789 {
		t.Errorf("nanoseconds = %d, want 123456789", run.CreatedAt.Nanosecond())
	}
}

// Proto `bytes` fields cross protojson as BASE64, not raw JSON. The scoped-
// injection RPCs put JSON payloads in bytes fields, so the SDK must encode on
// the way out and decode on the way back (#1919).
func TestScopedInjectionBytesRoundTrip(t *testing.T) {
	const payload = `{"old_value":100}`
	const encoded = "eyJvbGRfdmFsdWUiOjEwMH0="

	t.Run("GetPausedState decodes base64 output", func(t *testing.T) {
		client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"steps":[{"id":"step-1","name":"charge",
				"output":"` + encoded + `","injected":true,
				"completedAt":"2025-01-01T00:00:00Z"}],
				"nextStepHint":"next","pauseReason":"injection"}`))
		})
		defer closeFn()

		state, err := client.GetPausedState(context.Background(), "run-1")
		if err != nil {
			t.Fatalf("GetPausedState = %v", err)
		}
		if len(state.Steps) != 1 {
			t.Fatalf("steps = %#v", state.Steps)
		}
		if string(state.Steps[0].Output) != payload {
			t.Errorf("output = %s, want %s", state.Steps[0].Output, payload)
		}
		// The pre-fix bug: the caller received base64 inside a json.RawMessage.
		if string(state.Steps[0].Output) == encoded {
			t.Error("output is still base64 — it was not decoded")
		}
		var decoded map[string]any
		if err := json.Unmarshal(state.Steps[0].Output, &decoded); err != nil {
			t.Errorf("output is not valid JSON: %v", err)
		}
	})

	// Failed steps are exposed by GetPausedState precisely so they can be
	// repaired; without stepType/status/error a caller cannot tell a failed step
	// from a completed one, which defeats the purpose of the API.
	t.Run("GetPausedState surfaces stepType, status and error", func(t *testing.T) {
		client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"steps":[
				{"id":"ok","name":"charge","stepType":"invoke","status":"completed",
				 "output":"` + encoded + `"},
				{"id":"bad","name":"ship","stepType":"invoke_function","status":"failed",
				 "error":"eyJtZXNzYWdlIjoiYm9vbSJ9"}
			],"nextStepHint":"retry"}`))
		})
		defer closeFn()

		state, err := client.GetPausedState(context.Background(), "run-1")
		if err != nil {
			t.Fatalf("GetPausedState = %v", err)
		}
		if len(state.Steps) != 2 {
			t.Fatalf("steps = %#v", state.Steps)
		}

		if state.Steps[0].Status != "completed" || state.Steps[0].StepType != "invoke" {
			t.Errorf("step[0] type/status = %q/%q", state.Steps[0].StepType, state.Steps[0].Status)
		}
		if state.Steps[0].Error != nil {
			t.Errorf("a completed step should carry no error, got %s", state.Steps[0].Error)
		}

		if state.Steps[1].Status != "failed" || state.Steps[1].StepType != "invoke_function" {
			t.Errorf("step[1] type/status = %q/%q", state.Steps[1].StepType, state.Steps[1].Status)
		}
		// error is a proto bytes field too, so it also arrives base64-encoded.
		if string(state.Steps[1].Error) != `{"message":"boom"}` {
			t.Errorf("step[1].Error = %s, want decoded JSON", state.Steps[1].Error)
		}
	})

	t.Run("InjectStepOutput base64-encodes the request", func(t *testing.T) {
		var gotBody map[string]any
		client, closeFn := runWireClient(t, func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &gotBody)
			_, _ = w.Write([]byte(`{"stepId":"step-1","previousOutput":"` + encoded + `"}`))
		})
		defer closeFn()

		previous, err := client.InjectStepOutput(context.Background(),
			"run-1", "step-1", json.RawMessage(`{"corrected":true}`), "fix")
		if err != nil {
			t.Fatalf("InjectStepOutput = %v", err)
		}

		// Raw JSON here is rejected by protojson before the handler runs.
		if gotBody["new_output"] != "eyJjb3JyZWN0ZWQiOnRydWV9" {
			t.Errorf("new_output = %v, want base64", gotBody["new_output"])
		}
		if string(previous) != payload {
			t.Errorf("previousOutput = %s, want %s", previous, payload)
		}
	})

	t.Run("an empty bytes field decodes to nil", func(t *testing.T) {
		client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"stepId":"step-1"}`))
		})
		defer closeFn()

		previous, err := client.InjectStepOutput(context.Background(),
			"run-1", "step-1", json.RawMessage(`{"a":1}`), "")
		if err != nil {
			t.Fatalf("InjectStepOutput = %v", err)
		}
		if previous != nil {
			t.Errorf("expected nil for an absent previousOutput, got %s", previous)
		}
	})

	t.Run("malformed base64 is an error, not silent corruption", func(t *testing.T) {
		client, closeFn := runWireClient(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"steps":[{"id":"step-1","output":"!!!not-base64!!!"}]}`))
		})
		defer closeFn()

		if _, err := client.GetPausedState(context.Background(), "run-1"); err == nil {
			t.Fatal("expected malformed base64 to error")
		}
	})
}
