package ironflow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

// A replayed step returns exactly what it first returned: an int64 above 2^53
// survived the poll decode only as a rounded float64 (#2873).
func TestReplay_Int64OutputStaysExact(t *testing.T) {
	type out struct{ N int64 }
	cases := []struct {
		name, outputJSON string
		want             int64
	}{
		{"above 2^53", `{"N":1234567890123456789}`, 1234567890123456789},
		{"MaxInt64", `{"N":9223372036854775807}`, 9223372036854775807},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got out
			var gotErr error
			fn := Function{
				Config: FunctionConfig{ID: "fn"},
				Handler: func(ctx Context) (any, error) {
					got, gotErr = Run(ctx, "calc", func() (out, error) { return out{}, nil })
					return nil, gotErr
				},
			}
			exec := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
			// Decode exactly as Worker.requestJobs does.
			var pr jobPollResponse
			body := `{"job_id":"job-1","run_id":"run-1","function_id":"fn","attempt":2,
				"event":{"id":"e1","name":"e","data":{}},
				"completed_steps":[{"step_id":"run-1:calc:0","name":"calc","output":` + tc.outputJSON + `}]}`
			if err := json.NewDecoder(strings.NewReader(body)).Decode(&pr); err != nil {
				t.Fatal(err)
			}
			single := pr.jobAssignment
			if err := exec.execute(context.Background(), &single, &recordingReporter{}); err != nil {
				t.Fatal(err)
			}
			if gotErr != nil {
				t.Fatalf("replay error: %v", gotErr)
			}
			if got.N != tc.want {
				t.Errorf("replayed N = %d, want %d", got.N, tc.want)
			}
		})
	}

	t.Run("1e21 string-form float", func(t *testing.T) {
		// A step output of a huge integer-valued JSON number (e.g. a big.Int / json.Number field).
		var got struct{ N json.Number }
		var gotErr error
		fn := Function{Config: FunctionConfig{ID: "fn"}, Handler: func(ctx Context) (any, error) {
			got, gotErr = Run(ctx, "calc", func() (struct{ N json.Number }, error) { return got, nil })
			return nil, gotErr
		}}
		exec := &jobExecutor{functions: map[string]Function{"fn": fn}, logger: NewNoopLogger()}
		var pr jobPollResponse
		body := `{"job_id":"job-1","run_id":"run-1","function_id":"fn","attempt":2,"event":{"id":"e1","name":"e","data":{}},
			"completed_steps":[{"step_id":"run-1:calc:0","name":"calc","output":{"N":1000000000000000000000}}]}`
		if err := json.NewDecoder(strings.NewReader(body)).Decode(&pr); err != nil {
			t.Fatal(err)
		}
		single := pr.jobAssignment
		_ = exec.execute(context.Background(), &single, &recordingReporter{})
		if gotErr != nil || got.N.String() != "1000000000000000000000" {
			t.Errorf("replayed N = %q err=%v, want 1000000000000000000000", got.N, gotErr)
		}
	})
}

// Push: PushRequest.Steps decode keeps each output's exact bytes.
func TestReplay_PushCompletedStepKeepsExactOutput(t *testing.T) {
	var req PushRequest
	if err := json.Unmarshal([]byte(`{"run_id":"r","steps":[{"id":"s","name":"s","status":"completed","output":{"N":1234567890123456789}}]}`), &req); err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(req.Steps[0].Output)
	if err != nil || string(got) != `{"N":1234567890123456789}` {
		t.Fatalf("output = %s, %v", got, err)
	}
	// A null output stays absent, as when Output was decoded into any.
	if err := json.Unmarshal([]byte(`{"run_id":"r","steps":[{"id":"s","output":null}]}`), &req); err != nil || req.Steps[0].Output != nil {
		t.Fatalf("null output = %#v, %v", req.Steps[0].Output, err)
	}
}

// Streaming: output_json carries the exact bytes and wins over the Struct.
func TestReplay_StreamingPrefersOutputJSON(t *testing.T) {
	pa := &ironflowv1.JobAssignment{JobId: "j", RunId: "r", FunctionId: "fn",
		CompletedSteps: []*ironflowv1.CompletedStep{{
			StepId: "r:calc:0", Name: "r:calc:0",
			OutputValue: structpb.NewNumberValue(1234567890123456789),
			OutputJson:  []byte(`1234567890123456789`),
		}}}
	job, err := protoToJobAssignment(pa)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(job.CompletedSteps[0].Output); got != "1234567890123456789" {
		t.Fatalf("output = %s, want the exact digits", got)
	}
}
