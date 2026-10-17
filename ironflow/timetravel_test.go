package ironflow

import (
	"encoding/json"
	"testing"
)

func TestTimeTravelTypes_Marshal(t *testing.T) {
	snapshot := TimeTravelRunStateSnapshot{
		RunID: "run-1", FunctionID: "fn-1", Status: "completed",
		Steps: []TimeTravelStepSnapshot{
			{StepID: "step-a", Name: "step-a", Status: "completed", Injected: false},
		},
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var decoded TimeTravelRunStateSnapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.RunID != "run-1" {
		t.Errorf("got %s", decoded.RunID)
	}
	if len(decoded.Steps) != 1 {
		t.Errorf("got %d steps", len(decoded.Steps))
	}
}

func TestTimeTravelTimelineEvent_Marshal(t *testing.T) {
	event := TimeTravelTimelineEvent{
		ID: "e1", EventType: "step.completed", Summary: "Step 'x' completed",
		Significant: true, Timestamp: "2026-01-15T10:30:00Z",
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var decoded TimeTravelTimelineEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Significant {
		t.Error("expected significant")
	}
}

func TestTimeTravelStepOutputSnapshot_Marshal(t *testing.T) {
	snap := TimeTravelStepOutputSnapshot{
		StepID: "step-1", Status: "completed",
		Output: json.RawMessage(`{"result":"ok"}`), OriginalOutput: json.RawMessage(`{"result":"old"}`),
		Patched: true, Injected: true,
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	var decoded TimeTravelStepOutputSnapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Patched {
		t.Error("expected patched")
	}
	if !decoded.Injected {
		t.Error("expected injected")
	}
	if decoded.StepID != "step-1" {
		t.Errorf("got stepId %s", decoded.StepID)
	}
	if string(decoded.Output) != `{"result":"ok"}` {
		t.Errorf("round-trip output = %s", decoded.Output)
	}
}

// The server sends proto bytes base64-encoded; callers get the JSON (#2942).
func TestTimeTravelSnapshots_DecodeProtoBytes(t *testing.T) {
	var step TimeTravelStepOutputSnapshot
	if err := json.Unmarshal([]byte(`{"stepId":"r:a:0","output":"eyJuIjo0Mn0=","originalOutput":null}`), &step); err != nil {
		t.Fatal(err)
	}
	if string(step.Output) != `{"n":42}` || step.OriginalOutput != nil {
		t.Errorf("output = %s, original = %s", step.Output, step.OriginalOutput)
	}
	var run TimeTravelRunStateSnapshot
	if err := json.Unmarshal([]byte(`{"runId":"r","input":"e30=","steps":[{"stepId":"r:a:0","output":"eyJuIjo0Mn0=","error":"Ingi"}]}`), &run); err != nil {
		t.Fatal(err)
	}
	if string(run.Input) != `{}` || string(run.Steps[0].Output) != `{"n":42}` || string(run.Steps[0].Error) != `"x"` {
		t.Errorf("run = input %s, output %s, error %s", run.Input, run.Steps[0].Output, run.Steps[0].Error)
	}
	if err := json.Unmarshal([]byte(`{"output":"%%%"}`), &step); err == nil {
		t.Error("bad base64 should fail")
	}
}

func TestTimeTravelRunStateSnapshot_OmitEmpty(t *testing.T) {
	snapshot := TimeTravelRunStateSnapshot{
		RunID: "run-2", FunctionID: "fn-2", Status: "running",
		Steps: []TimeTravelStepSnapshot{},
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	// Input and CreatedAt should be omitted when empty
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if _, exists := raw["input"]; exists {
		t.Error("expected input to be omitted when empty")
	}
	if _, exists := raw["createdAt"]; exists {
		t.Error("expected createdAt to be omitted when empty")
	}
}
