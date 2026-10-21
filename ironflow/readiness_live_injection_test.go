package ironflow

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// TestLiveReadinessInjection covers the scoped-injection group (#3003): the
// seed gives each SDK its own function with a completed run to patch, and a
// run held in a one-hour sleep behind two failed steps. Resuming a run after an
// injection and following the injected value through later steps is left to
// the engine tests, because the held run cannot get past its sleep here.
func TestLiveReadinessInjection(t *testing.T) {
	var seed struct {
		StepOutput map[string]any `json:"stepOutput"`
		Groups     struct {
			Injection map[string]struct {
				DoneRunID    string `json:"doneRunId"`
				DoneStepID   string `json:"doneStepId"`
				HoldRunID    string `json:"holdRunId"`
				PatchStepID  string `json:"patchStepId"`
				InjectStepID string `json:"injectStepId"`
				SleepStepID  string `json:"sleepStepId"`
			} `json:"injection"`
		} `json:"groups"`
	}
	client, ctx := liveReadiness(t, &seed)
	runs := seed.Groups.Injection["go"]
	stepByID := func(t *testing.T, runID, id string) RunStep {
		t.Helper()
		steps, err := client.GetRunSteps(ctx, runID)
		if err != nil {
			t.Fatalf("GetRunSteps: %v", err)
		}
		for _, st := range steps.Steps {
			if st.ID == id {
				return st
			}
		}
		t.Fatalf("run %s has no step %s", runID, id)
		return RunStep{}
	}

	t.Run("step-patch", func(t *testing.T) {
		patched := map[string]any{"n": float64(43), "who": "patched"}
		if err := client.PatchStep(ctx, runs.DoneStepID, patched, "readiness patch"); err != nil {
			t.Fatalf("PatchStep: %v", err)
		}
		st := stepByID(t, runs.DoneRunID, runs.DoneStepID)
		if !reflect.DeepEqual(st.Output, patched) || !reflect.DeepEqual(st.OriginalOutput, seed.StepOutput) || st.PatchedAt == nil {
			t.Fatalf("step = output %v, original %v, patchedAt %v, want %v patched over %v", st.Output, st.OriginalOutput, st.PatchedAt, patched, seed.StepOutput)
		}

		// A failed step becomes completed and loses its error.
		if before := stepByID(t, runs.HoldRunID, runs.PatchStepID); before.Status != "failed" || before.Error == nil {
			t.Fatalf("step before = %+v, want failed with an error", before)
		}
		if err := client.PatchStep(ctx, runs.PatchStepID, patched, "readiness recover"); err != nil {
			t.Fatalf("PatchStep of a failed step: %v", err)
		}
		st = stepByID(t, runs.HoldRunID, runs.PatchStepID)
		if st.Status != "completed" || st.Error != nil || !reflect.DeepEqual(st.Output, patched) || st.PatchedAt == nil {
			t.Fatalf("recovered step = %+v, want completed, no error, output %v", st, patched)
		}

		// Only completed and failed steps can be patched.
		if err := client.PatchStep(ctx, runs.SleepStepID, patched, ""); !hasErrorCode(err, "failed_precondition") {
			t.Fatalf("PatchStep of the sleeping step = %v, want failed_precondition", err)
		}
		if err := client.PatchStep(ctx, "no-such-step", patched, ""); !hasErrorCode(err, "not_found") {
			t.Fatalf("PatchStep of a missing step = %v, want not_found", err)
		}
	})

	t.Run("step-inject-output", func(t *testing.T) {
		// A run that is not paused for injection refuses it.
		if _, err := client.InjectStepOutput(ctx, runs.DoneRunID, runs.DoneStepID, json.RawMessage(`{}`), ""); !hasErrorCode(err, "failed_precondition") {
			t.Fatalf("InjectStepOutput into a completed run = %v, want failed_precondition", err)
		}
		if status, err := client.PauseRun(ctx, runs.HoldRunID); err != nil || status != "paused" {
			t.Fatalf("PauseRun = %q, %v, want paused", status, err)
		}
		state, err := client.GetPausedState(ctx, runs.HoldRunID)
		if err != nil {
			t.Fatalf("GetPausedState: %v", err)
		}
		byID := map[string]PausedStepInfo{}
		for _, ps := range state.Steps {
			byID[ps.ID] = ps
		}
		if ps := byID[runs.InjectStepID]; ps.Status != "failed" || ps.Error == nil {
			t.Fatalf("paused state of the failed step = %+v, want failed with an error", ps)
		}
		var write RunStep
		steps, _ := client.GetRunSteps(ctx, runs.HoldRunID)
		for _, st := range steps.Steps {
			if st.Status == "completed" && st.OriginalOutput == nil && st.PatchedAt == nil {
				write = st
			}
		}
		if write.ID == "" {
			t.Fatalf("no untouched completed step in %+v", steps.Steps)
		}

		// A completed step: the call returns the output it replaced.
		injected := map[string]any{"n": float64(7), "who": "José injected"}
		raw, _ := json.Marshal(injected)
		previous, err := client.InjectStepOutput(ctx, runs.HoldRunID, write.ID, raw, "readiness inject")
		if err != nil {
			t.Fatalf("InjectStepOutput: %v", err)
		}
		if !sameJSON(previous, seed.StepOutput) {
			t.Fatalf("previous output = %s, want %v", previous, seed.StepOutput)
		}
		st := stepByID(t, runs.HoldRunID, write.ID)
		if !reflect.DeepEqual(st.Output, injected) || !reflect.DeepEqual(st.OriginalOutput, seed.StepOutput) {
			t.Fatalf("step after inject = output %v, original %v, want %v over %v", st.Output, st.OriginalOutput, injected, seed.StepOutput)
		}

		// A failed step becomes completed and loses its error.
		if _, err := client.InjectStepOutput(ctx, runs.HoldRunID, runs.InjectStepID, raw, "readiness recover"); err != nil {
			t.Fatalf("InjectStepOutput into a failed step: %v", err)
		}
		st = stepByID(t, runs.HoldRunID, runs.InjectStepID)
		if st.Status != "completed" || st.Error != nil || !reflect.DeepEqual(st.Output, injected) {
			t.Fatalf("recovered step = %+v, want completed, no error, output %v", st, injected)
		}
		state, err = client.GetPausedState(ctx, runs.HoldRunID)
		if err != nil {
			t.Fatalf("GetPausedState after inject: %v", err)
		}
		for _, ps := range state.Steps {
			if ps.ID == runs.InjectStepID && (!ps.Injected || ps.Status != "completed" || !sameJSON(ps.Output, injected)) {
				t.Fatalf("paused state of the recovered step = %+v, want injected and completed with %v", ps, injected)
			}
		}

		// The function records, so each injection is a step.injected audit event.
		deadline := time.Now().Add(10 * time.Second)
		for {
			trail, err := client.GetAuditTrail(ctx, runs.HoldRunID, GetAuditTrailOpts{EventType: "step.injected"})
			if err != nil {
				t.Fatalf("GetAuditTrail: %v", err)
			}
			if len(trail.Events) == 2 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("step.injected events = %+v, want 2", trail.Events)
			}
			time.Sleep(200 * time.Millisecond)
		}
	})
}
