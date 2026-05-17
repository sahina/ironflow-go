package ironflow

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBranchContext(t *testing.T) {
	t.Run("generates scoped step IDs", func(t *testing.T) {
		exec := &executionContext{
			runID:          "run_123",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		}

		branch := exec.createBranchContext("process-items", 0)

		id1 := branch.generateStepID("fetch")
		id2 := branch.generateStepID("fetch")
		id3 := branch.generateStepID("process")

		if id1 != "run_123:process-items:0:fetch:0" {
			t.Errorf("expected 'run_123:process-items:0:fetch:0', got '%s'", id1)
		}
		if id2 != "run_123:process-items:0:fetch:1" {
			t.Errorf("expected 'run_123:process-items:0:fetch:1', got '%s'", id2)
		}
		if id3 != "run_123:process-items:0:process:0" {
			t.Errorf("expected 'run_123:process-items:0:process:0', got '%s'", id3)
		}
	})

	t.Run("different branches have different prefixes", func(t *testing.T) {
		exec := &executionContext{
			runID:          "run_123",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		}

		branch0 := exec.createBranchContext("parallel-op", 0)
		branch1 := exec.createBranchContext("parallel-op", 1)

		id0 := branch0.generateStepID("step")
		id1 := branch1.generateStepID("step")

		if id0 == id1 {
			t.Errorf("expected different IDs, got same: %s", id0)
		}
		if id0 != "run_123:parallel-op:0:step:0" {
			t.Errorf("expected 'run_123:parallel-op:0:step:0', got '%s'", id0)
		}
		if id1 != "run_123:parallel-op:1:step:0" {
			t.Errorf("expected 'run_123:parallel-op:1:step:0', got '%s'", id1)
		}
	})

	t.Run("accesses memoized steps from parent", func(t *testing.T) {
		exec := &executionContext{
			runID:        "run_123",
			stepCounters: make(map[string]int),
			completedSteps: map[string]*CompletedStep{
				"run_123:parallel:0:step:0": {
					ID:     "run_123:parallel:0:step:0",
					Name:   "step",
					Status: "completed",
					Output: "memoized",
				},
			},
			executedSteps: make([]*StepResult, 0),
		}

		branch := exec.createBranchContext("parallel", 0)

		completed, ok := branch.getCompletedStep("run_123:parallel:0:step:0")
		if !ok {
			t.Fatal("expected to find memoized step")
		}
		if completed.Output != "memoized" {
			t.Errorf("expected output 'memoized', got '%v'", completed.Output)
		}
	})

	t.Run("records steps to parent context", func(t *testing.T) {
		exec := &executionContext{
			runID:          "run_123",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		}

		branch := exec.createBranchContext("parallel", 0)
		branch.recordStep(&StepResult{
			ID:     "run_123:parallel:0:step:0",
			Name:   "step",
			Status: "completed",
			Output: "result",
		})

		if len(exec.executedSteps) != 1 {
			t.Fatalf("expected 1 step recorded, got %d", len(exec.executedSteps))
		}
		if exec.executedSteps[0].ID != "run_123:parallel:0:step:0" {
			t.Errorf("expected step ID 'run_123:parallel:0:step:0', got '%s'", exec.executedSteps[0].ID)
		}
	})

	t.Run("creates nested branch contexts", func(t *testing.T) {
		exec := &executionContext{
			runID:          "run_123",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		}

		branch := exec.createBranchContext("outer", 0)
		nested := branch.createBranchContext("inner", 1)

		id := nested.generateStepID("step")
		if id != "run_123:outer:0:inner:1:step:0" {
			t.Errorf("expected 'run_123:outer:0:inner:1:step:0', got '%s'", id)
		}
	})
}

func TestParallel(t *testing.T) {
	t.Run("executes all branches and returns results in order", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		results, err := Parallel(ctx, "process", []func(*BranchContext) (int, error){
			func(b *BranchContext) (int, error) { return 1, nil },
			func(b *BranchContext) (int, error) { return 2, nil },
			func(b *BranchContext) (int, error) { return 3, nil },
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}
		if results[0] != 1 || results[1] != 2 || results[2] != 3 {
			t.Errorf("expected [1, 2, 3], got %v", results)
		}
	})

	t.Run("runs branches concurrently", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		var maxConcurrent int32
		var currentConcurrent atomic.Int32
		var mu sync.Mutex

		results, err := Parallel(ctx, "concurrent", []func(*BranchContext) (int, error){
			func(b *BranchContext) (int, error) {
				current := currentConcurrent.Add(1)
				mu.Lock()
				if current > maxConcurrent {
					maxConcurrent = current
				}
				mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				currentConcurrent.Add(-1)
				return 1, nil
			},
			func(b *BranchContext) (int, error) {
				current := currentConcurrent.Add(1)
				mu.Lock()
				if current > maxConcurrent {
					maxConcurrent = current
				}
				mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				currentConcurrent.Add(-1)
				return 2, nil
			},
			func(b *BranchContext) (int, error) {
				current := currentConcurrent.Add(1)
				mu.Lock()
				if current > maxConcurrent {
					maxConcurrent = current
				}
				mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				currentConcurrent.Add(-1)
				return 3, nil
			},
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}
		if maxConcurrent < 2 {
			t.Errorf("expected concurrent execution (max concurrent >= 2), got %d", maxConcurrent)
		}
	})

	t.Run("respects concurrency limit", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		var maxConcurrent int32
		var currentConcurrent atomic.Int32
		var mu sync.Mutex

		createBranch := func(val int) func(*BranchContext) (int, error) {
			return func(b *BranchContext) (int, error) {
				current := currentConcurrent.Add(1)
				mu.Lock()
				if current > maxConcurrent {
					maxConcurrent = current
				}
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				currentConcurrent.Add(-1)
				return val, nil
			}
		}

		branches := []func(*BranchContext) (int, error){
			createBranch(1), createBranch(2), createBranch(3),
			createBranch(4), createBranch(5),
		}

		results, err := Parallel(ctx, "limited", branches, ParallelOptions{Concurrency: 2})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 5 {
			t.Fatalf("expected 5 results, got %d", len(results))
		}
		if maxConcurrent > 2 {
			t.Errorf("expected max concurrent <= 2 (limit), got %d", maxConcurrent)
		}
	})

	t.Run("handles empty branches", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		results, err := Parallel(ctx, "empty", []func(*BranchContext) (int, error){})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("expected empty results, got %d", len(results))
		}
	})

	t.Run("fails fast on error by default", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		var executed atomic.Int32

		_, err := Parallel(ctx, "failfast", []func(*BranchContext) (int, error){
			func(b *BranchContext) (int, error) {
				executed.Add(1)
				time.Sleep(50 * time.Millisecond)
				return 1, nil
			},
			func(b *BranchContext) (int, error) {
				executed.Add(1)
				return 0, errors.New("branch failed")
			},
			func(b *BranchContext) (int, error) {
				executed.Add(1)
				time.Sleep(50 * time.Millisecond)
				return 3, nil
			},
		})

		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "branch failed" {
			t.Errorf("expected error 'branch failed', got '%s'", err.Error())
		}
	})

	t.Run("collects all results in allSettled mode", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		results, err := Parallel(ctx, "allsettled", []func(*BranchContext) (int, error){
			func(b *BranchContext) (int, error) { return 1, nil },
			func(b *BranchContext) (int, error) { return 0, errors.New("branch 2 failed") },
			func(b *BranchContext) (int, error) { return 3, nil },
		}, ParallelOptions{OnError: "allSettled"})

		// In allSettled mode, errors don't cause early return
		if err != nil {
			t.Fatalf("unexpected error in allSettled mode: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}
		// Results are in order - branch 2 will have zero value due to error
		if results[0] != 1 || results[2] != 3 {
			t.Errorf("expected results[0]=1, results[2]=3, got %v", results)
		}
		if results[1] != 0 {
			t.Errorf("expected results[1]=0 for failed branch, got %v", results[1])
		}
	})

	t.Run("fails fast with concurrency limit on error", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		var executed atomic.Int32

		createBranch := func(val int, sleepMs int) func(*BranchContext) (int, error) {
			return func(b *BranchContext) (int, error) {
				executed.Add(1)
				if sleepMs > 0 {
					time.Sleep(time.Duration(sleepMs) * time.Millisecond)
				}
				return val, nil
			}
		}

		_, err := Parallel(ctx, "failfast-conc", []func(*BranchContext) (int, error){
			createBranch(1, 50),
			func(b *BranchContext) (int, error) {
				executed.Add(1)
				return 0, errors.New("branch 1 failed")
			},
			createBranch(3, 50),
			createBranch(4, 50),
			createBranch(5, 50),
		}, ParallelOptions{Concurrency: 2, OnError: "failFast"})

		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "branch 1 failed" {
			t.Errorf("expected 'branch 1 failed', got '%s'", err.Error())
		}
	})

	t.Run("allSettled with concurrency limit processes all branches despite errors", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		var executed atomic.Int32

		results, err := Parallel(ctx, "allsettled-conc", []func(*BranchContext) (int, error){
			func(b *BranchContext) (int, error) {
				executed.Add(1)
				time.Sleep(10 * time.Millisecond)
				return 1, nil
			},
			func(b *BranchContext) (int, error) {
				executed.Add(1)
				return 0, errors.New("branch 1 failed")
			},
			func(b *BranchContext) (int, error) {
				executed.Add(1)
				time.Sleep(10 * time.Millisecond)
				return 3, nil
			},
			func(b *BranchContext) (int, error) {
				executed.Add(1)
				return 0, errors.New("branch 3 failed")
			},
			func(b *BranchContext) (int, error) {
				executed.Add(1)
				time.Sleep(10 * time.Millisecond)
				return 5, nil
			},
		}, ParallelOptions{Concurrency: 2, OnError: "allSettled"})

		if err != nil {
			t.Fatalf("unexpected error in allSettled mode: %v", err)
		}

		// All branches should execute in allSettled mode
		if executed.Load() != 5 {
			t.Errorf("expected all 5 branches to execute, got %d", executed.Load())
		}

		// Successful branches should have their values
		if results[0] != 1 {
			t.Errorf("expected results[0]=1, got %d", results[0])
		}
		if results[2] != 3 {
			t.Errorf("expected results[2]=3, got %d", results[2])
		}
		if results[4] != 5 {
			t.Errorf("expected results[4]=5, got %d", results[4])
		}

		// Failed branches should have zero values
		if results[1] != 0 {
			t.Errorf("expected results[1]=0 for failed branch, got %d", results[1])
		}
		if results[3] != 0 {
			t.Errorf("expected results[3]=0 for failed branch, got %d", results[3])
		}
	})

	t.Run("propagates yield signal from branch", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic with yield signal")
			}
			signal, ok := r.(*yieldSignal)
			if !ok {
				t.Fatalf("expected yieldSignal, got %T", r)
			}
			if signal.info.Type != "sleep" {
				t.Errorf("expected type 'sleep', got '%s'", signal.info.Type)
			}
		}()

		Parallel(ctx, "with-sleep", []func(*BranchContext) (int, error){
			func(b *BranchContext) (int, error) {
				return 1, nil
			},
			func(b *BranchContext) (int, error) {
				SleepWithBranch(b, "wait", 1*time.Hour)
				return 2, nil
			},
		})

		t.Fatal("expected Parallel to panic with yield signal")
	})
}

func TestRunWithBranch(t *testing.T) {
	t.Run("executes function and returns result", func(t *testing.T) {
		exec := &executionContext{
			runID:          "run_123",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		}
		branch := exec.createBranchContext("parallel", 0)

		result, err := RunWithBranch(branch, "my-step", func() (string, error) {
			return "hello", nil
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "hello" {
			t.Errorf("expected 'hello', got '%s'", result)
		}

		// Check that step was recorded
		if len(exec.executedSteps) != 1 {
			t.Fatalf("expected 1 executed step, got %d", len(exec.executedSteps))
		}
		if exec.executedSteps[0].ID != "run_123:parallel:0:my-step:0" {
			t.Errorf("expected step ID 'run_123:parallel:0:my-step:0', got '%s'", exec.executedSteps[0].ID)
		}
	})

	t.Run("returns memoized result", func(t *testing.T) {
		exec := &executionContext{
			runID:        "run_123",
			stepCounters: make(map[string]int),
			completedSteps: map[string]*CompletedStep{
				"run_123:parallel:0:my-step:0": {
					ID:     "run_123:parallel:0:my-step:0",
					Name:   "my-step",
					Status: "completed",
					Output: "memoized-value",
				},
			},
			executedSteps: make([]*StepResult, 0),
		}
		branch := exec.createBranchContext("parallel", 0)

		callCount := 0
		result, err := RunWithBranch(branch, "my-step", func() (string, error) {
			callCount++
			return "new-value", nil
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "memoized-value" {
			t.Errorf("expected 'memoized-value', got '%s'", result)
		}
		if callCount != 0 {
			t.Errorf("function should not have been called, but was called %d times", callCount)
		}
	})

	t.Run("handles function error", func(t *testing.T) {
		exec := &executionContext{
			runID:          "run_123",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		}
		branch := exec.createBranchContext("parallel", 0)

		_, err := RunWithBranch(branch, "failing-step", func() (string, error) {
			return "", errors.New("step failed")
		})

		if err == nil {
			t.Fatal("expected error")
		}

		stepErr, ok := err.(*StepError)
		if !ok {
			t.Fatalf("expected StepError, got %T", err)
		}
		if stepErr.StepName != "failing-step" {
			t.Errorf("expected step name 'failing-step', got '%s'", stepErr.StepName)
		}
	})
}

func TestMap(t *testing.T) {
	t.Run("maps over items and returns results in order", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		items := []int{1, 2, 3, 4, 5}
		results, err := Map(ctx, "double", items, func(item int, b *BranchContext, index int) (int, error) {
			return item * 2, nil
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 5 {
			t.Fatalf("expected 5 results, got %d", len(results))
		}

		expected := []int{2, 4, 6, 8, 10}
		for i, r := range results {
			if r != expected[i] {
				t.Errorf("expected results[%d]=%d, got %d", i, expected[i], r)
			}
		}
	})

	t.Run("passes correct index to function", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		items := []string{"a", "b", "c"}
		var receivedIndices []int
		var mu sync.Mutex

		_, err := Map(ctx, "index-check", items, func(item string, b *BranchContext, index int) (string, error) {
			mu.Lock()
			receivedIndices = append(receivedIndices, index)
			mu.Unlock()
			return item, nil
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Check all indices were received (order may vary due to concurrency)
		if len(receivedIndices) != 3 {
			t.Fatalf("expected 3 indices, got %d", len(receivedIndices))
		}

		// Sort and check
		found := make(map[int]bool)
		for _, idx := range receivedIndices {
			found[idx] = true
		}
		if !found[0] || !found[1] || !found[2] {
			t.Errorf("expected indices 0, 1, 2, got %v", receivedIndices)
		}
	})

	t.Run("handles empty items", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		results, err := Map(ctx, "empty-map", []int{}, func(item int, b *BranchContext, index int) (int, error) {
			return item, nil
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("expected empty results, got %d", len(results))
		}
	})

	t.Run("respects concurrency option", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		var maxConcurrent int32
		var currentConcurrent atomic.Int32
		var mu sync.Mutex

		items := []int{1, 2, 3, 4, 5, 6}
		_, err := Map(ctx, "limited-map", items, func(item int, b *BranchContext, index int) (int, error) {
			current := currentConcurrent.Add(1)
			mu.Lock()
			if current > maxConcurrent {
				maxConcurrent = current
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			currentConcurrent.Add(-1)
			return item * 2, nil
		}, ParallelOptions{Concurrency: 3})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if maxConcurrent > 3 {
			t.Errorf("expected max concurrent <= 3, got %d", maxConcurrent)
		}
	})

	t.Run("fails fast on error by default", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		items := []int{1, 2, 3, 4, 5}
		_, err := Map(ctx, "failfast-map", items, func(item int, b *BranchContext, index int) (int, error) {
			if item == 3 {
				return 0, errors.New("item 3 failed")
			}
			return item * 2, nil
		})

		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "item 3 failed" {
			t.Errorf("expected 'item 3 failed', got '%s'", err.Error())
		}
	})

	t.Run("allSettled collects errors without failing", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		var executed atomic.Int32
		items := []int{1, 2, 3, 4, 5}
		results, err := Map(ctx, "allsettled-map", items, func(item int, b *BranchContext, index int) (int, error) {
			executed.Add(1)
			if item == 2 || item == 4 {
				return 0, errors.New("item failed")
			}
			return item * 2, nil
		}, ParallelOptions{OnError: "allSettled"})

		if err != nil {
			t.Fatalf("unexpected error in allSettled mode: %v", err)
		}

		// All items should be processed
		if executed.Load() != 5 {
			t.Errorf("expected all 5 items to execute, got %d", executed.Load())
		}

		// Successful items have their values, failed items have zero values
		if results[0] != 2 {
			t.Errorf("expected results[0]=2, got %d", results[0])
		}
		if results[1] != 0 {
			t.Errorf("expected results[1]=0 for failed item, got %d", results[1])
		}
		if results[2] != 6 {
			t.Errorf("expected results[2]=6, got %d", results[2])
		}
		if results[3] != 0 {
			t.Errorf("expected results[3]=0 for failed item, got %d", results[3])
		}
		if results[4] != 10 {
			t.Errorf("expected results[4]=10, got %d", results[4])
		}
	})

	t.Run("fails fast with concurrency limit on error", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		items := []int{1, 2, 3, 4, 5}
		_, err := Map(ctx, "failfast-conc-map", items, func(item int, b *BranchContext, index int) (int, error) {
			if item == 2 {
				return 0, errors.New("item 2 failed")
			}
			time.Sleep(20 * time.Millisecond)
			return item * 2, nil
		}, ParallelOptions{Concurrency: 2, OnError: "failFast"})

		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "item 2 failed" {
			t.Errorf("expected 'item 2 failed', got '%s'", err.Error())
		}
	})

	t.Run("allSettled with concurrency limit processes all items despite errors", func(t *testing.T) {
		ctx := Context{
			exec: &executionContext{
				runID:          "run_123",
				stepCounters:   make(map[string]int),
				completedSteps: make(map[string]*CompletedStep),
				executedSteps:  make([]*StepResult, 0),
			},
		}

		var executed atomic.Int32
		items := []int{1, 2, 3, 4, 5}
		results, err := Map(ctx, "allsettled-conc-map", items, func(item int, b *BranchContext, index int) (int, error) {
			executed.Add(1)
			if item == 2 || item == 4 {
				return 0, errors.New("item failed")
			}
			time.Sleep(10 * time.Millisecond)
			return item * 2, nil
		}, ParallelOptions{Concurrency: 2, OnError: "allSettled"})

		if err != nil {
			t.Fatalf("unexpected error in allSettled mode: %v", err)
		}

		// All items should be processed
		if executed.Load() != 5 {
			t.Errorf("expected all 5 items to execute, got %d", executed.Load())
		}

		if results[0] != 2 {
			t.Errorf("expected results[0]=2, got %d", results[0])
		}
		if results[1] != 0 {
			t.Errorf("expected results[1]=0 for failed item, got %d", results[1])
		}
		if results[2] != 6 {
			t.Errorf("expected results[2]=6, got %d", results[2])
		}
		if results[3] != 0 {
			t.Errorf("expected results[3]=0 for failed item, got %d", results[3])
		}
		if results[4] != 10 {
			t.Errorf("expected results[4]=10, got %d", results[4])
		}
	})
}

func TestSleepWithBranch(t *testing.T) {
	t.Run("yields with sleep signal", func(t *testing.T) {
		exec := &executionContext{
			runID:          "run_123",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		}
		branch := exec.createBranchContext("parallel", 0)

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic with yield signal")
			}
			signal, ok := r.(*yieldSignal)
			if !ok {
				t.Fatalf("expected yieldSignal, got %T", r)
			}
			if signal.info.Type != "sleep" {
				t.Errorf("expected type 'sleep', got '%s'", signal.info.Type)
			}
			if signal.info.StepID != "run_123:parallel:0:wait:0" {
				t.Errorf("expected step ID 'run_123:parallel:0:wait:0', got '%s'", signal.info.StepID)
			}
		}()

		SleepWithBranch(branch, "wait", 1*time.Hour)
		t.Fatal("expected SleepWithBranch to panic")
	})

	t.Run("returns immediately when memoized", func(t *testing.T) {
		exec := &executionContext{
			runID:        "run_123",
			stepCounters: make(map[string]int),
			completedSteps: map[string]*CompletedStep{
				"run_123:parallel:0:wait:0": {
					ID:     "run_123:parallel:0:wait:0",
					Name:   "wait",
					Status: "completed",
				},
			},
			executedSteps: make([]*StepResult, 0),
		}
		branch := exec.createBranchContext("parallel", 0)

		// Should not panic
		err := SleepWithBranch(branch, "wait", 1*time.Hour)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestParallelWithBranch(t *testing.T) {
	t.Run("supports nested parallel execution", func(t *testing.T) {
		exec := &executionContext{
			runID:          "run_123",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		}
		outerBranch := exec.createBranchContext("outer", 0)

		results, err := ParallelWithBranch(outerBranch, "inner", []func(*BranchContext) (int, error){
			func(b *BranchContext) (int, error) {
				return RunWithBranch(b, "step", func() (int, error) {
					return 1, nil
				})
			},
			func(b *BranchContext) (int, error) {
				return RunWithBranch(b, "step", func() (int, error) {
					return 2, nil
				})
			},
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}
		if results[0] != 1 || results[1] != 2 {
			t.Errorf("expected [1, 2], got %v", results)
		}

		// Check step IDs are correctly nested
		if len(exec.executedSteps) != 2 {
			t.Fatalf("expected 2 executed steps, got %d", len(exec.executedSteps))
		}

		// Steps should have nested scope prefix
		expectedPrefixes := []string{
			"run_123:outer:0:inner:0:step:0",
			"run_123:outer:0:inner:1:step:0",
		}
		foundPrefixes := make(map[string]bool)
		for _, step := range exec.executedSteps {
			foundPrefixes[step.ID] = true
		}
		for _, expected := range expectedPrefixes {
			if !foundPrefixes[expected] {
				t.Errorf("expected step ID '%s' not found in %v", expected, exec.executedSteps)
			}
		}
	})
}

func TestParallelMemoization(t *testing.T) {
	t.Run("memoizes branch steps on retry", func(t *testing.T) {
		// Simulate a retry scenario where branch 0 completed but branch 1 failed
		exec := &executionContext{
			runID:        "run_123",
			stepCounters: make(map[string]int),
			completedSteps: map[string]*CompletedStep{
				"run_123:process:0:step:0": {
					ID:     "run_123:process:0:step:0",
					Name:   "step",
					Status: "completed",
					Output: "branch-0-result",
				},
			},
			executedSteps: make([]*StepResult, 0),
		}

		ctx := Context{exec: exec}

		var branch0Called, branch1Called bool

		results, err := Parallel(ctx, "process", []func(*BranchContext) (string, error){
			func(b *BranchContext) (string, error) {
				return RunWithBranch(b, "step", func() (string, error) {
					branch0Called = true
					return "new-branch-0-result", nil
				})
			},
			func(b *BranchContext) (string, error) {
				return RunWithBranch(b, "step", func() (string, error) {
					branch1Called = true
					return "branch-1-result", nil
				})
			},
		})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Branch 0 should use memoized result
		if branch0Called {
			t.Error("branch 0 should not have been called (memoized)")
		}
		if results[0] != "branch-0-result" {
			t.Errorf("expected memoized 'branch-0-result', got '%s'", results[0])
		}

		// Branch 1 should execute
		if !branch1Called {
			t.Error("branch 1 should have been called")
		}
		if results[1] != "branch-1-result" {
			t.Errorf("expected 'branch-1-result', got '%s'", results[1])
		}
	})
}
