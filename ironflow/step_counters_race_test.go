package ironflow

import (
	"fmt"
	"testing"
)

// TestEnclosingContextConcurrentStepIDs pins the #1792 concurrency fix.
//
// A branch that reaches for the ENCLOSING context instead of the *BranchContext
// it was handed is the "third case" the #1671 warning describes: it records real
// steps, just outside the parallel's scope. In Go that case also drove every
// branch goroutine into executionContext.generateStepID at once, which read and
// wrote an unguarded map[string]int. That is a concurrent map write — a runtime
// throw that recover() cannot catch and that takes the whole process with it.
//
// This test is green-only by construction: there is no red-then-green shape to
// commit, because the failure mode kills the test binary rather than failing an
// assertion. It was verified red by removing the mutex from generateStepID and
// running it by hand. Keep it free of a Concurrency limit and keep the branch
// count high — the detector needs real overlap to see the write pair.
func TestEnclosingContextConcurrentStepIDs(t *testing.T) {
	const branches = 64

	ctx := Context{
		exec: &executionContext{
			runID:          "run_race",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
		},
	}

	fns := make([]func(*BranchContext) (int, error), branches)
	for i := range fns {
		n := i
		// Deliberately ignores b and uses the enclosing ctx — the shape the
		// #1671 warning flags, and the one that used to race.
		fns[i] = func(_ *BranchContext) (int, error) {
			return Run(ctx, "shared-name", func() (int, error) { return n, nil })
		}
	}

	results, err := Parallel(ctx, "fanout", fns)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != branches {
		t.Fatalf("expected %d results, got %d", branches, len(results))
	}

	// Every branch shared one step name, so the counter must have handed out
	// exactly `branches` distinct ids. A lost update under the race would
	// collapse two branches onto one id and shrink this set.
	ids := make(map[string]bool, branches)
	for _, s := range ctx.exec.executedSteps {
		ids[s.ID] = true
	}
	if len(ids) != branches {
		t.Errorf("expected %d distinct step IDs, got %d — counter lost an update", branches, len(ids))
	}
	for i := 0; i < branches; i++ {
		want := fmt.Sprintf("run_race:shared-name:%d", i)
		if !ids[want] {
			t.Errorf("missing step ID %q", want)
		}
	}
}

// TestNestedEnclosingBranchConcurrentStepIDs is the branch-level twin of the
// test above, and the one the first cut of #1792 missed.
//
// Guarding only executionContext.stepCounters looked sufficient because each
// branch gets its own *BranchContext before any goroutine starts. That holds
// only while callbacks use the context they are handed — and the whole reason
// this warning exists is that they sometimes don't. In a NESTED fan-out the
// misuse is N inner goroutines all reaching for the same ENCLOSING branch, so
// they land in one BranchContext.generateStepID and one markScopedUsed at once.
//
// Verified red by reverting stepCountersMu/atomic.Bool on BranchContext: the
// race detector reports a DATA RACE on scopedClientUsed, and without -race it
// dies with `fatal error: concurrent map writes` — a runtime throw the branch
// handler's recover() cannot catch, so it kills the worker before the
// diagnostic this feature exists to print can ever run.
func TestNestedEnclosingBranchConcurrentStepIDs(t *testing.T) {
	const inner = 64

	ctx := Context{exec: &executionContext{
		runID:          "r1",
		stepCounters:   make(map[string]int),
		completedSteps: make(map[string]*CompletedStep),
		executedSteps:  make([]*StepResult, 0),
		logger:         &captureLogger{},
	}}

	_, err := Parallel(ctx, "outer", []func(*BranchContext) (int, error){
		func(outer *BranchContext) (int, error) {
			branches := make([]func(*BranchContext) (int, error), inner)
			for i := range branches {
				n := i
				// Misuse under test: reaches for the ENCLOSING branch context
				// instead of the child each callback is handed.
				branches[i] = func(_ *BranchContext) (int, error) {
					return RunWithBranch(outer, "shared", func() (int, error) { return n, nil })
				}
			}
			_, e := ParallelWithBranch(outer, "inner", branches)
			return 0, e
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// One shared step name across `inner` calls on one branch scope: the
	// counter must have issued that many distinct ids.
	ids := make(map[string]bool, inner)
	for _, s := range ctx.exec.executedSteps {
		ids[s.ID] = true
	}
	if len(ids) != inner {
		t.Errorf("expected %d distinct step IDs, got %d — branch counter lost an update", inner, len(ids))
	}
}
