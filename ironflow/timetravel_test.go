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
		Output: `{"result":"ok"}`, OriginalOutput: `{"result":"old"}`,
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
