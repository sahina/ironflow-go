package ironflow

import (
	"testing"
	"time"
)

// Tests for the branch-scoped step primitives added in #1792. Before them, a
// branch could only Run / Sleep / WaitForEvent / Parallel / Compensate under its
// own scope; SleepUntil, Invoke, InvokeAsync, Publish and Map silently took
// their step IDs from the ENCLOSING function instead.

func newBranch(t *testing.T, completed map[string]*CompletedStep) (*BranchContext, *executionContext) {
	t.Helper()
	if completed == nil {
		completed = make(map[string]*CompletedStep)
	}
	exec := &executionContext{
		runID:          "r1",
		stepCounters:   make(map[string]int),
		completedSteps: completed,
		executedSteps:  make([]*StepResult, 0),
		logger:         &captureLogger{},
	}
	return exec.createBranchContext("outer", 0), exec
}

// captureYield runs fn and returns the YieldInfo it panicked with.
func captureYield(t *testing.T, fn func()) *YieldInfo {
	t.Helper()
	var info *YieldInfo
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected a yield signal, got none")
			}
			sig, ok := r.(*yieldSignal)
			if !ok {
				panic(r)
			}
			info = sig.info
		}()
		fn()
	}()
	return info
}

func TestSleepUntilWithBranchScopesStepID(t *testing.T) {
	b, _ := newBranch(t, nil)
	until := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	info := captureYield(t, func() { _ = SleepUntilWithBranch(b, "wait", until) })

	if want := "r1:outer:0:wait:0"; info.StepID != want {
		t.Errorf("step ID = %q, want %q", info.StepID, want)
	}
	if info.Type != "sleep" {
		t.Errorf("type = %q, want sleep", info.Type)
	}
}

func TestSleepUntilWithBranchMemoizes(t *testing.T) {
	b, _ := newBranch(t, map[string]*CompletedStep{
		"r1:outer:0:wait:0": {ID: "r1:outer:0:wait:0", Status: "completed"},
	})
	// Memoized: returns instead of yielding.
	if err := SleepUntilWithBranch(b, "wait", time.Now()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInvokeWithBranchScopesStepID(t *testing.T) {
	b, _ := newBranch(t, nil)

	info := captureYield(t, func() { _, _ = InvokeWithBranch[string](b, "child-fn", nil) })

	if want := "r1:outer:0:child-fn:0"; info.StepID != want {
		t.Errorf("step ID = %q, want %q", info.StepID, want)
	}
	if info.FunctionID != "child-fn" {
		t.Errorf("functionID = %q", info.FunctionID)
	}
}

func TestInvokeWithBranchMemoizes(t *testing.T) {
	b, _ := newBranch(t, map[string]*CompletedStep{
		"r1:outer:0:child-fn:0": {ID: "r1:outer:0:child-fn:0", Status: "completed", Output: "MEMOIZED"},
	})
	got, err := InvokeWithBranch[string](b, "child-fn", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "MEMOIZED" {
		t.Errorf("got %q, want MEMOIZED", got)
	}
}

// The forcing case for the whole seam (#1792). Invoke keys its step ID on the
// functionID, not a name you pick. At the root, N branches invoking the SAME
// function draw from one shared counter, so which branch gets ":0" and which
// gets ":1" depends on goroutine scheduling — and on resume the assignment can
// flip, handing a branch the other branch's memoized output. Branch scoping
// removes the shared counter: each branch's ID is fixed by its own index.
func TestInvokeWithBranchGivesEachBranchADeterministicStepID(t *testing.T) {
	ctx := Context{
		exec: &executionContext{
			runID:          "r1",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
			logger:         &captureLogger{},
		},
	}

	// Two branches, same functionID, each invoking through its own scope.
	// Collect the IDs each branch would yield with.
	ids := make([]string, 2)
	for i := 0; i < 2; i++ {
		b := ctx.exec.createBranchContext("p", i)
		info := captureYield(t, func() { _, _ = InvokeWithBranch[string](b, "same-fn", nil) })
		ids[i] = info.StepID
	}

	want := []string{"r1:p:0:same-fn:0", "r1:p:1:same-fn:0"}
	for i := range want {
		if ids[i] != want[i] {
			t.Errorf("branch %d step ID = %q, want %q", i, ids[i], want[i])
		}
	}
	if ids[0] == ids[1] {
		t.Fatal("both branches got the same step ID — they would share one memoization key")
	}
}

func TestInvokeAsyncWithBranchScopesStepID(t *testing.T) {
	b, _ := newBranch(t, nil)

	info := captureYield(t, func() { _, _ = InvokeAsyncWithBranch(b, "child-fn", nil) })

	if want := "r1:outer:0:child-fn:0"; info.StepID != want {
		t.Errorf("step ID = %q, want %q", info.StepID, want)
	}
	if info.Type != ResumeTypeInvokeFunctionAsync {
		t.Errorf("type = %q, want %q", info.Type, ResumeTypeInvokeFunctionAsync)
	}
}

func TestInvokeAsyncWithBranchMemoizes(t *testing.T) {
	b, _ := newBranch(t, map[string]*CompletedStep{
		"r1:outer:0:child-fn:0": {
			ID: "r1:outer:0:child-fn:0", Status: "completed",
			Output: map[string]any{"run_id": "run_child"},
		},
	})
	got, err := InvokeAsyncWithBranch(b, "child-fn", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.RunID != "run_child" {
		t.Errorf("runID = %q, want run_child", got.RunID)
	}
}

// Publish is a durable step named "publish:{topic}". Memoized here so the test
// needs no HTTP server — what matters is which scope the ID came from.
func TestPublishWithBranchScopesStepID(t *testing.T) {
	// "publish:" is a stepIDNamespaces prefix, so it is NOT escaped and the
	// leaf "order.processed" has no colon — the ID is plain.
	b, exec := newBranch(t, map[string]*CompletedStep{
		"r1:outer:0:publish:order.processed:0": {
			ID: "r1:outer:0:publish:order.processed:0", Status: "completed",
		},
	})
	// serverURL deliberately empty: if the step were NOT memoized under the
	// branch scope, doPublish would run and fail with "server URL not configured".
	if err := PublishWithBranch(b, "order.processed", map[string]any{"id": 1}); err != nil {
		t.Fatalf("expected the branch-scoped publish to be memoized, got: %v", err)
	}
	if len(exec.executedSteps) != 0 {
		t.Errorf("memoized publish should record no new step, got %d", len(exec.executedSteps))
	}
}

func TestMapWithBranchScopesStepIDs(t *testing.T) {
	b, exec := newBranch(t, nil)

	got, err := MapWithBranch(b, "inner", []int{10, 20},
		func(n int, item *BranchContext, _ int) (int, error) {
			return RunWithBranch(item, "double", func() (int, error) { return n * 2, nil })
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got[0] != 20 || got[1] != 40 {
		t.Errorf("results = %v, want [20 40]", got)
	}

	ids := map[string]bool{}
	for _, s := range exec.executedSteps {
		ids[s.ID] = true
	}
	for _, want := range []string{"r1:outer:0:inner:0:double:0", "r1:outer:0:inner:1:double:0"} {
		if !ids[want] {
			t.Errorf("missing nested step ID %q; got %v", want, ids)
		}
	}
}
