package ironflow

import (
	"testing"
	"time"
)

func TestSleepMemoization(t *testing.T) {
	t.Run("Sleep skips when already completed (memoized)", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:        "run_sleep_memo",
				stepCounters: make(map[string]int),
				completedSteps: map[string]*CompletedStep{
					"run_sleep_memo:test-sleep:0": {
						ID:     "run_sleep_memo:test-sleep:0",
						Name:   "test-sleep",
						Status: "completed",
					},
				},
				executedSteps: make([]*StepResult, 0),
			},
		}

		start := time.Now()

		// This should return immediately because the step is memoized
		err := Sleep(ctx, "test-sleep", 10*time.Hour)

		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Should complete nearly instantly (not wait 10 hours!)
		if elapsed > 100*time.Millisecond {
			t.Errorf("Sleep should have been skipped due to memoization, but took %v", elapsed)
		}
	})

	t.Run("Sleep skips when resuming from same step", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_sleep_resume",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
				resumeContext: &ResumeContext{
					StepID: "run_sleep_resume:test-sleep:0",
					Type:   "sleep",
				},
			},
		}

		start := time.Now()

		// This should return immediately because we're resuming from this sleep
		err := Sleep(ctx, "test-sleep", 10*time.Hour)

		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if elapsed > 100*time.Millisecond {
			t.Errorf("Sleep should have been skipped due to resume, but took %v", elapsed)
		}

		// Verify resume was marked as handled
		if !ctx.exec.resumeHandled {
			t.Error("Expected resumeHandled to be true")
		}
	})

	t.Run("Sleep yields when not memoized", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_sleep_new",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		var capturedSignal *yieldSignal

		func() {
			defer func() {
				if r := recover(); r != nil {
					if signal, ok := r.(*yieldSignal); ok {
						capturedSignal = signal
					} else {
						panic(r)
					}
				}
			}()

			Sleep(ctx, "new-sleep", 1*time.Hour)
		}()

		if capturedSignal == nil {
			t.Fatal("Expected yield signal for new sleep")
		}
		if capturedSignal.info.Type != "sleep" {
			t.Errorf("Expected type 'sleep', got '%s'", capturedSignal.info.Type)
		}
	})

	t.Run("multiple Sleeps with same name increment counter", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:        "run_multi_sleep",
				stepCounters: make(map[string]int),
				completedSteps: map[string]*CompletedStep{
					"run_multi_sleep:wait:0": {
						ID:     "run_multi_sleep:wait:0",
						Name:   "wait",
						Status: "completed",
					},
					"run_multi_sleep:wait:1": {
						ID:     "run_multi_sleep:wait:1",
						Name:   "wait",
						Status: "completed",
					},
				},
				executedSteps: make([]*StepResult, 0),
			},
		}

		// First sleep - should skip (memoized)
		err1 := Sleep(ctx, "wait", 1*time.Hour)
		if err1 != nil {
			t.Fatalf("First sleep error: %v", err1)
		}

		// Second sleep - should also skip (memoized)
		err2 := Sleep(ctx, "wait", 1*time.Hour)
		if err2 != nil {
			t.Fatalf("Second sleep error: %v", err2)
		}

		// Verify counter was incremented
		if ctx.exec.stepCounters["wait"] != 2 {
			t.Errorf("Expected step counter 2, got %d", ctx.exec.stepCounters["wait"])
		}

		// Third sleep should yield (not memoized)
		var capturedSignal *yieldSignal
		func() {
			defer func() {
				if r := recover(); r != nil {
					if signal, ok := r.(*yieldSignal); ok {
						capturedSignal = signal
					} else {
						panic(r)
					}
				}
			}()
			Sleep(ctx, "wait", 1*time.Hour)
		}()

		if capturedSignal == nil {
			t.Fatal("Expected yield signal for third sleep")
		}
	})
}

func TestSleepUntilMemoization(t *testing.T) {
	t.Run("SleepUntil skips when already completed", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:        "run_until_memo",
				stepCounters: make(map[string]int),
				completedSteps: map[string]*CompletedStep{
					"run_until_memo:wait-until:0": {
						ID:     "run_until_memo:wait-until:0",
						Name:   "wait-until",
						Status: "completed",
					},
				},
				executedSteps: make([]*StepResult, 0),
			},
		}

		futureTime := time.Now().Add(10 * time.Hour)
		start := time.Now()

		err := SleepUntil(ctx, "wait-until", futureTime)

		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if elapsed > 100*time.Millisecond {
			t.Errorf("SleepUntil should have been skipped, but took %v", elapsed)
		}
	})

	t.Run("SleepUntil skips when resuming", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_until_resume",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
				resumeContext: &ResumeContext{
					StepID: "run_until_resume:wait-until:0",
					Type:   "sleep",
				},
			},
		}

		futureTime := time.Now().Add(10 * time.Hour)
		start := time.Now()

		err := SleepUntil(ctx, "wait-until", futureTime)

		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if elapsed > 100*time.Millisecond {
			t.Errorf("SleepUntil should have been skipped, but took %v", elapsed)
		}

		if !ctx.exec.resumeHandled {
			t.Error("Expected resumeHandled to be true")
		}
	})

	t.Run("SleepUntil yields with correct timestamp", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_until_yield",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		targetTime := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		var capturedSignal *yieldSignal

		func() {
			defer func() {
				if r := recover(); r != nil {
					if signal, ok := r.(*yieldSignal); ok {
						capturedSignal = signal
					} else {
						panic(r)
					}
				}
			}()

			SleepUntil(ctx, "wait-until-2030", targetTime)
		}()

		if capturedSignal == nil {
			t.Fatal("Expected yield signal")
		}

		// Verify the Until timestamp
		parsedTime, err := time.Parse(time.RFC3339, capturedSignal.info.Until)
		if err != nil {
			t.Fatalf("Failed to parse Until timestamp: %v", err)
		}

		if !parsedTime.Equal(targetTime) {
			t.Errorf("Expected Until %v, got %v", targetTime, parsedTime)
		}
	})
}

func TestSleepWithBranchMemoization(t *testing.T) {
	t.Run("SleepWithBranch skips when memoized", func(t *testing.T) {
		parent := &executionContext{
			runID:        "run_branch_sleep",
			stepCounters: make(map[string]int),
			completedSteps: map[string]*CompletedStep{
				"run_branch_sleep:parallel:0:branch-sleep:0": {
					ID:     "run_branch_sleep:parallel:0:branch-sleep:0",
					Name:   "branch-sleep",
					Status: "completed",
				},
			},
			executedSteps: make([]*StepResult, 0),
		}
		branch := parent.createBranchContext("parallel", 0)

		start := time.Now()

		err := SleepWithBranch(branch, "branch-sleep", 10*time.Hour)

		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if elapsed > 100*time.Millisecond {
			t.Errorf("SleepWithBranch should have been skipped, but took %v", elapsed)
		}
	})

	t.Run("SleepWithBranch skips when resuming", func(t *testing.T) {
		parent := &executionContext{
			runID:          "run_branch_resume",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
			resumeContext: &ResumeContext{
				StepID: "run_branch_resume:parallel:0:branch-sleep:0",
				Type:   "sleep",
			},
		}
		branch := parent.createBranchContext("parallel", 0)

		start := time.Now()

		err := SleepWithBranch(branch, "branch-sleep", 10*time.Hour)

		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if elapsed > 100*time.Millisecond {
			t.Errorf("SleepWithBranch should have been skipped, but took %v", elapsed)
		}

		if !parent.resumeHandled {
			t.Error("Expected resumeHandled to be true")
		}
	})
}

func TestPartialSleepResume(t *testing.T) {
	t.Run("workflow resumes correctly after sleep interrupt", func(t *testing.T) {
		// Simulate a workflow that was interrupted during sleep
		// On resume, Step1 should be skipped (memoized), Sleep should be skipped (resume),
		// and Step2 should execute
		ctx := Context{
			exec: &executionContext{
				runID:        "run_partial_resume",
				stepCounters: make(map[string]int),
				completedSteps: map[string]*CompletedStep{
					"run_partial_resume:step1:0": {
						ID:     "run_partial_resume:step1:0",
						Name:   "step1",
						Status: "completed",
						Output: "step1-result",
					},
				},
				executedSteps: make([]*StepResult, 0),
				resumeContext: &ResumeContext{
					StepID: "run_partial_resume:wait:0",
					Type:   "sleep",
				},
			},
		}

		// Step 1 - should be memoized
		step1Called := false
		result1, err := Run(ctx, "step1", func() (string, error) {
			step1Called = true
			return "new-result", nil
		})

		if err != nil {
			t.Fatalf("Step1 error: %v", err)
		}
		if step1Called {
			t.Error("Step1 should have been memoized, not called")
		}
		if result1 != "step1-result" {
			t.Errorf("Expected memoized 'step1-result', got '%s'", result1)
		}

		// Sleep - should be skipped due to resume
		sleepStart := time.Now()
		err = Sleep(ctx, "wait", 1*time.Hour)
		sleepElapsed := time.Since(sleepStart)

		if err != nil {
			t.Fatalf("Sleep error: %v", err)
		}
		if sleepElapsed > 100*time.Millisecond {
			t.Errorf("Sleep should have been instant, took %v", sleepElapsed)
		}

		// Step 2 - should execute (not memoized)
		step2Called := false
		result2, err := Run(ctx, "step2", func() (string, error) {
			step2Called = true
			return "step2-result", nil
		})

		if err != nil {
			t.Fatalf("Step2 error: %v", err)
		}
		if !step2Called {
			t.Error("Step2 should have been called")
		}
		if result2 != "step2-result" {
			t.Errorf("Expected 'step2-result', got '%s'", result2)
		}

		// Verify executed steps
		if len(ctx.exec.executedSteps) != 1 {
			t.Errorf("Expected 1 executed step (step2), got %d", len(ctx.exec.executedSteps))
		}
	})
}

func TestSleepResumeVsMemoization(t *testing.T) {
	t.Run("resume takes precedence when both resume and memoized exist", func(t *testing.T) {
		// Edge case: both resume context AND completed step exist
		ctx := Context{
			exec: &executionContext{
				runID:        "run_both",
				stepCounters: make(map[string]int),
				completedSteps: map[string]*CompletedStep{
					"run_both:sleep:0": {
						ID:     "run_both:sleep:0",
						Name:   "sleep",
						Status: "completed",
					},
				},
				executedSteps: make([]*StepResult, 0),
				resumeContext: &ResumeContext{
					StepID: "run_both:sleep:0",
					Type:   "sleep",
				},
			},
		}

		err := Sleep(ctx, "sleep", 1*time.Hour)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Resume should be checked first and handled
		if !ctx.exec.resumeHandled {
			t.Error("Expected resumeHandled to be true (resume should take precedence)")
		}
	})

	t.Run("resume only matches exact step ID and type", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_mismatch",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
				resumeContext: &ResumeContext{
					StepID: "run_mismatch:other-sleep:0",
					Type:   "sleep",
				},
			},
		}

		var capturedSignal *yieldSignal
		func() {
			defer func() {
				if r := recover(); r != nil {
					if signal, ok := r.(*yieldSignal); ok {
						capturedSignal = signal
					} else {
						panic(r)
					}
				}
			}()

			// Different step name, so resume context doesn't match
			Sleep(ctx, "different-sleep", 1*time.Hour)
		}()

		// Should yield because resume context is for different step
		if capturedSignal == nil {
			t.Fatal("Expected yield signal since resume context doesn't match")
		}

		// Resume should NOT be marked as handled
		if ctx.exec.resumeHandled {
			t.Error("Resume should not be marked as handled for mismatched step")
		}
	})
}
