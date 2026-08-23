package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// Regression tests for #1792 (the Go twin of #1671): Parallel / Map branches are
// durable under the fan-out's scope only if the callback uses the *BranchContext
// it is handed. All three shapes compile and all three appear to work, so the
// SDK warns about the inert one.

// captureLogger records Warn messages. Concurrent because branches warn from
// nested fan-outs running in parallel goroutines.
type captureLogger struct {
	mu   sync.Mutex
	warn []string
}

func (l *captureLogger) Debug(msg string, args ...any) {}
func (l *captureLogger) Info(msg string, args ...any)  {}
func (l *captureLogger) Error(msg string, args ...any) {}
func (l *captureLogger) Warn(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warn = append(l.warn, msg)
}
func (l *captureLogger) warnings() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.warn...)
}

func newWarnCtx(t *testing.T) (Context, *captureLogger) {
	t.Helper()
	log := &captureLogger{}
	return Context{
		exec: &executionContext{
			runID:          "r1",
			stepCounters:   make(map[string]int),
			completedSteps: make(map[string]*CompletedStep),
			executedSteps:  make([]*StepResult, 0),
			logger:         log,
		},
	}, log
}

func wantNoWarnings(t *testing.T, log *captureLogger) {
	t.Helper()
	if got := log.warnings(); len(got) != 0 {
		t.Errorf("expected no warnings, got %v", got)
	}
}

func wantOneWarning(t *testing.T, log *captureLogger, substr string) {
	t.Helper()
	got := log.warnings()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 warning, got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], substr) {
		t.Errorf("warning %q does not contain %q", got[0], substr)
	}
}

// 1
func TestWarnsWhenMapBranchesIgnoreScope(t *testing.T) {
	ctx, log := newWarnCtx(t)
	got, err := Map(ctx, "ingest", []string{"a", "b"},
		func(item string, _ *BranchContext, _ int) (string, error) {
			return strings.ToUpper(item), nil
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got[0] != "A" || got[1] != "B" {
		t.Errorf("results wrong: %v", got)
	}
	wantOneWarning(t, log, "ingest: 2/2 branches did not use the scoped")
}

// 2
func TestQuietWhenMapBranchesUseScope(t *testing.T) {
	ctx, log := newWarnCtx(t)
	_, err := Map(ctx, "ingest", []string{"a", "b"},
		func(item string, b *BranchContext, _ int) (string, error) {
			return RunWithBranch(b, "up", func() (string, error) { return strings.ToUpper(item), nil })
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// 3 — a mixed result means the author already knows about the scope.
func TestQuietWhenOnlySomeBranchesShortCircuit(t *testing.T) {
	ctx, log := newWarnCtx(t)
	_, err := Map(ctx, "ingest", []string{"a", "b", "skip"},
		func(item string, b *BranchContext, _ int) (string, error) {
			if item == "skip" {
				return "", nil
			}
			return RunWithBranch(b, "up", func() (string, error) { return strings.ToUpper(item), nil })
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// 4 — a branch whose only work is a nested fan-out has used its scope.
func TestQuietWhenBranchOnlyOpensNestedFanout(t *testing.T) {
	ctx, log := newWarnCtx(t)
	_, err := Parallel(ctx, "outer", []func(*BranchContext) (int, error){
		func(b *BranchContext) (int, error) {
			_, err := MapWithBranch(b, "inner", []int{1, 2},
				func(n int, item *BranchContext, _ int) (int, error) {
					return RunWithBranch(item, "double", func() (int, error) { return n * 2, nil })
				})
			return 0, err
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// 5 — pins the mark on ENTRY, not per branch: a nested fan-out with zero items
// never creates a child branch context, so a per-branch mark would false-positive.
func TestQuietWhenNestedFanoutHasNoItems(t *testing.T) {
	ctx, log := newWarnCtx(t)
	_, err := Parallel(ctx, "outer", []func(*BranchContext) (int, error){
		func(b *BranchContext) (int, error) {
			_, err := MapWithBranch(b, "inner", []int{},
				func(n int, item *BranchContext, _ int) (int, error) { return n, nil })
			return 0, err
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// 6
func TestQuietForEmptyCollection(t *testing.T) {
	ctx, log := newWarnCtx(t)
	if _, err := Map(ctx, "empty", []int{},
		func(n int, _ *BranchContext, _ int) (int, error) { return n, nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// 7 — the concurrency-limited path takes a different code path through the semaphore.
func TestWarnsOnceOnConcurrencyLimitedPath(t *testing.T) {
	ctx, log := newWarnCtx(t)
	_, err := Map(ctx, "conc", []int{1, 2, 3},
		func(n int, _ *BranchContext, _ int) (int, error) { return n, nil },
		ParallelOptions{Concurrency: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantOneWarning(t, log, "conc: 3/3 branches did not use the scoped")
}

// 8 — reaching for the enclosing Context DOES record steps, just outside scope.
// The message must not claim otherwise.
//
// Detector 2 owns this case: every branch claimed on the enclosing context, so
// it reports the scheduling-ordered-index hazard rather than the all-inert
// "did not use the scoped context" line. That is the sharper message for this
// shape — these branches DID persist steps, they are just keyed off a counter
// shared by every branch.
func TestFlagsBranchesReachingForEnclosingContext(t *testing.T) {
	ctx, log := newWarnCtx(t)
	_, err := Parallel(ctx, "p", []func(*BranchContext) (int, error){
		func(_ *BranchContext) (int, error) {
			return Run(ctx, "branch-a", func() (int, error) { return 1, nil })
		},
		func(_ *BranchContext) (int, error) {
			return Run(ctx, "branch-b", func() (int, error) { return 2, nil })
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantOneWarning(t, log, "p: 2 step(s) were claimed on the ENCLOSING")
	if strings.Contains(log.warnings()[0], "recorded no durable step") {
		t.Error("message must not claim no steps were recorded — the enclosing context does record them")
	}
	if len(ctx.exec.executedSteps) != 2 {
		t.Errorf("expected 2 real steps recorded at the root, got %d", len(ctx.exec.executedSteps))
	}
}

// 9 — the migration hazard the docs warn about, for a colon-free name.
// preferLegacyStepID early-returns here (legacy == id), so nothing bridges the
// root-scoped id to the branch-scoped one and the side effect re-runs.
func TestMigratingBranchToScopeReExecutesCompletedStep(t *testing.T) {
	sideEffects := 0
	ctx := Context{
		exec: &executionContext{
			runID:        "r1",
			stepCounters: make(map[string]int),
			completedSteps: map[string]*CompletedStep{
				// What a prior run persisted when the branch used the enclosing Context.
				"r1:branch-a:0": {ID: "r1:branch-a:0", Name: "branch-a", Status: "completed", Output: "MEMOIZED"},
			},
			executedSteps: make([]*StepResult, 0),
			logger:        &captureLogger{},
		},
	}

	got, err := Parallel(ctx, "p", []func(*BranchContext) (string, error){
		func(b *BranchContext) (string, error) {
			// Same step name, now routed through the scoped context.
			return RunWithBranch(b, "branch-a", func() (string, error) {
				sideEffects++
				return "RE-EXECUTED", nil
			})
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got[0] != "RE-EXECUTED" {
		t.Errorf("expected the memoized row to be missed, got %q", got[0])
	}
	if sideEffects != 1 {
		t.Errorf("expected the side effect to re-run once, got %d", sideEffects)
	}
}

// 9b — the same hazard for a publish step.
//
// "publish:" is a stepIDNamespaces prefix (step.go), so it is deliberately NOT
// escaped and a topic without a colon still yields legacy == id — the same
// early-return path as 9. Pinned separately because publish is the primitive
// most likely to be migrated root -> branch, and re-running one is a duplicate
// message on the wire rather than a missing timeline row.
func TestMigratingPublishStepToScopeReExecutes(t *testing.T) {
	sideEffects := 0
	const topic = "order.processed"
	ctx := Context{
		exec: &executionContext{
			runID:        "r1",
			stepCounters: make(map[string]int),
			completedSteps: map[string]*CompletedStep{
				// Root-scoped publish from a prior run.
				"r1:publish:order.processed:0": {
					ID: "r1:publish:order.processed:0", Name: publishStepName(topic),
					Status: "completed", Output: "MEMOIZED",
				},
			},
			executedSteps: make([]*StepResult, 0),
			logger:        &captureLogger{},
		},
	}

	_, err := Parallel(ctx, "p", []func(*BranchContext) (string, error){
		func(b *BranchContext) (string, error) {
			return RunWithBranch(b, publishStepName(topic), func() (string, error) {
				sideEffects++
				return "RE-EXECUTED", nil
			})
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sideEffects != 1 {
		t.Errorf("expected the branch-scoped publish to re-run once, got %d", sideEffects)
	}
	// And the root-scoped spelling still resolves to its memoized row.
	got, err := Run(ctx, publishStepName(topic), func() (string, error) {
		sideEffects++
		return "SHOULD NOT RUN", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "MEMOIZED" {
		t.Errorf("root-scoped publish should still be memoized, got %q", got)
	}
	if sideEffects != 1 {
		t.Errorf("root-scoped publish re-ran; side effects now %d", sideEffects)
	}
}

// 9c — the genuine escaping interaction: a colon in the USER's own step name.
// Unlike 9 and 9b, escapeStepIDPart rewrites this one, so legacy != id and
// preferLegacyStepID's lookup actually runs instead of early-returning. Pins
// that #1694 escaping and #1792 re-scoping compose: the legacy bridge resolves
// a root-scoped row, and re-scoping to a branch still misses it.
func TestColonBearingNameBridgesLegacyAtRootButNotAcrossScopes(t *testing.T) {
	const name = "ingest:a:b"
	sideEffects := 0
	exec := &executionContext{
		runID:        "r1",
		stepCounters: make(map[string]int),
		completedSteps: map[string]*CompletedStep{
			// Persisted before #1694 escaping shipped: the UNESCAPED spelling.
			"r1:ingest:a:b:0": {ID: "r1:ingest:a:b:0", Name: name, Status: "completed", Output: "MEMOIZED"},
		},
		executedSteps: make([]*StepResult, 0),
		logger:        &captureLogger{},
	}
	ctx := Context{exec: exec}

	// At the root, preferLegacyStepID bridges to the pre-escaping row.
	got, err := Run(ctx, name, func() (string, error) {
		sideEffects++
		return "RE-EXECUTED", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "MEMOIZED" {
		t.Errorf("root call should bridge to the legacy row, got %q", got)
	}
	if sideEffects != 0 {
		t.Errorf("root call re-executed; side effects = %d", sideEffects)
	}

	// Re-scoped into a branch, the legacy bridge cannot help — different prefix.
	b := exec.createBranchContext("p", 0)
	got, err = RunWithBranch(b, name, func() (string, error) {
		sideEffects++
		return "RE-EXECUTED", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "RE-EXECUTED" || sideEffects != 1 {
		t.Errorf("branch-scoped call should have re-executed once; got %q, side effects = %d", got, sideEffects)
	}
}

// 10 — one line per name, not one per nested fan-out.
func TestWarnsOncePerNameNotPerNestedFanout(t *testing.T) {
	ctx, log := newWarnCtx(t)
	_, err := Map(ctx, "outer", []int{1, 2, 3},
		func(_ int, b *BranchContext, _ int) (int, error) {
			_, err := ParallelWithBranch(b, "inner", []func(*BranchContext) (int, error){
				func(_ *BranchContext) (int, error) { return 1, nil },
				func(_ *BranchContext) (int, error) { return 2, nil },
			})
			return 0, err
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantOneWarning(t, log, "inner: 2/2 branches did not use the scoped")
}

// 11 — two genuinely different bad call sites still both report.
func TestReportsTwoDistinctBadCallSites(t *testing.T) {
	ctx, log := newWarnCtx(t)
	inert := func(n int, _ *BranchContext, _ int) (int, error) { return n, nil }
	if _, err := Map(ctx, "first", []int{1, 2}, inert); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := Map(ctx, "second", []int{1, 2}, inert); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := log.warnings(); len(got) != 2 {
		t.Errorf("expected 2 warnings, got %d: %v", len(got), got)
	}
}

// 12
func TestQuietWhenCallerOptsOut(t *testing.T) {
	ctx, log := newWarnCtx(t)
	_, err := Map(ctx, "transform", []int{1, 2},
		func(n int, _ *BranchContext, _ int) (int, error) { return n * 2, nil },
		ParallelOptions{SkipScopedClientCheck: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// 13 — never blame a branch that errored.
func TestDoesNotBlameBranchesThatErrored(t *testing.T) {
	ctx, log := newWarnCtx(t)
	_, err := Map(ctx, "boom", []int{1, 2},
		func(n int, _ *BranchContext, _ int) (int, error) { return 0, errors.New("boom") },
		ParallelOptions{OnError: "allSettled"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// stubInterceptor is the minimum TestInterceptor: every step succeeds.
type stubInterceptor struct{}

func (stubInterceptor) RunStep(name string) (any, error) { return "ok", nil }
func (stubInterceptor) SleepStep(name string)            {}
func (stubInterceptor) WaitForEventStep(name string, f EventFilter) (Event, error) {
	return Event{}, nil
}
func (stubInterceptor) InvokeStep(fnID string, in any) (any, error) { return "ok", nil }
func (stubInterceptor) InvokeAsyncStep(fnID string, in any) (InvokeAsyncResult, error) {
	return InvokeAsyncResult{}, nil
}
func (stubInterceptor) CompensateStep(stepName string, fn func() error)      {}
func (stubInterceptor) RecordStep(name, stepType string, out any, err error) {}

// 14 — CORRECT code must not warn under a test interceptor.
//
// ironflowtest is how users unit-test their own functions, so this is the first
// place a false positive would be seen. The branch primitives return early when
// exec.testInterceptor != nil, so a mark that lived only in generateStepID was
// skipped and correct callbacks were counted as unscoped. Every other test in
// this file builds an executionContext with a nil interceptor and structurally
// cannot reach this path.
func TestQuietUnderTestInterceptorWhenBranchesUseScope(t *testing.T) {
	log := &captureLogger{}
	ctx := NewTestContext(Event{Name: "e"}, "r1", "fn", stubInterceptor{})
	ctx.exec.logger = log

	_, err := Map(ctx, "ingest", []string{"a", "b"},
		func(item string, b *BranchContext, _ int) (string, error) {
			return RunWithBranch(b, "up", func() (string, error) { return item, nil })
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// 15 — a branch whose ONLY work is SleepWithBranch / WaitForEventWithBranch /
// CompensateInBranch has used its scope. Each gained markScopedUsed() in #1792
// but only RunWithBranch was covered, so a regression in any of the other three
// would have shipped silently.
func TestQuietWhenBranchUsesNonRunPrimitives(t *testing.T) {
	for _, tc := range []struct {
		name string
		fn   func(*BranchContext) (int, error)
	}{
		{"sleep", func(b *BranchContext) (int, error) {
			// Memoized so it returns instead of yielding.
			return 0, SleepWithBranch(b, "nap", time.Second)
		}},
		{"waitForEvent", func(b *BranchContext) (int, error) {
			_, err := WaitForEventWithBranch(b, "wait", EventFilter{Event: "e"})
			return 0, err
		}},
		{"compensate", func(b *BranchContext) (int, error) {
			CompensateInBranch(b, "undo", func() error { return nil })
			return 0, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &captureLogger{}
			exec := &executionContext{
				runID:         "r1",
				stepCounters:  make(map[string]int),
				executedSteps: make([]*StepResult, 0),
				logger:        log,
				completedSteps: map[string]*CompletedStep{
					"r1:p:0:nap:0":  {ID: "r1:p:0:nap:0", Status: "completed"},
					"r1:p:0:wait:0": {ID: "r1:p:0:wait:0", Status: "completed", Output: map[string]any{}},
				},
			}
			if _, err := Parallel(Context{exec: exec}, "p",
				[]func(*BranchContext) (int, error){tc.fn}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			wantNoWarnings(t, log)
		})
	}
}

// 16 — nesting deeper than two levels. Pins that the scope prefix chains
// correctly at depth 3 and that dedup stays once-per-name across the tree.
func TestThreeLevelNestingScopesAndWarnsOnce(t *testing.T) {
	ctx, log := newWarnCtx(t)

	_, err := Parallel(ctx, "l1", []func(*BranchContext) (int, error){
		func(b1 *BranchContext) (int, error) {
			_, e := ParallelWithBranch(b1, "l2", []func(*BranchContext) (int, error){
				func(b2 *BranchContext) (int, error) {
					_, e2 := ParallelWithBranch(b2, "l3", []func(*BranchContext) (int, error){
						func(b3 *BranchContext) (int, error) {
							return RunWithBranch(b3, "leaf", func() (int, error) { return 1, nil })
						},
						// Inert sibling — but a MIXED result must stay quiet.
						func(_ *BranchContext) (int, error) { return 0, nil },
					})
					return 0, e2
				},
			})
			return 0, e
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)

	want := "r1:l1:0:l2:0:l3:0:leaf:0"
	found := false
	for _, s := range ctx.exec.executedSteps {
		if s.ID == want {
			found = true
		}
	}
	if !found {
		var got []string
		for _, s := range ctx.exec.executedSteps {
			got = append(got, s.ID)
		}
		t.Errorf("expected depth-3 step ID %q, got %v", want, got)
	}
}

// ---------------------------------------------------------------------------
// Detector 2: the MIXED shape (#1792 review finding F2).
//
// A branch that uses BOTH the enclosing context and its own scope marks itself
// scoped, so the all-inert gate leaves unscoped == 0 and stays silent — while
// the enclosing-context call draws its index from the run-wide counter in
// goroutine-scheduling order. Measured before this detector existed: 25 distinct
// step-id -> output mappings across 30 identical runs, 0 warnings.
// ---------------------------------------------------------------------------

// 17 — the shape that was silent.
func TestWarnsWhenBranchClaimsOnEnclosingContext(t *testing.T) {
	ctx, log := newWarnCtx(t)

	_, err := Map(ctx, "items", []int{1, 2, 3},
		func(n int, b *BranchContext, _ int) (int, error) {
			// Enclosing context — shared root counter, scheduling-ordered index.
			if _, e := Run(ctx, "audit", func() (int, error) { return n, nil }); e != nil {
				return 0, e
			}
			// ...and the scope, which is what used to mask the misuse.
			return RunWithBranch(b, "process", func() (int, error) { return n, nil })
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantOneWarning(t, log, "items: 3 step(s) were claimed on the ENCLOSING")
}

// 18 — gate #1's false-positive protection must NOT regress. A callback that
// uses its scope only when there is work to do never touches the enclosing
// context, so detector 2 has nothing to report.
func TestQuietWhenBranchesShortCircuitWithoutEnclosingClaims(t *testing.T) {
	ctx, log := newWarnCtx(t)

	_, err := Map(ctx, "items", []string{"a", "skip", "b"},
		func(item string, b *BranchContext, _ int) (string, error) {
			if item == "skip" {
				return "", nil
			}
			return RunWithBranch(b, "work", func() (string, error) { return item, nil })
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// 19 — root steps OUTSIDE the fan-out window are ordinary steps.
func TestQuietForRootStepsBeforeAndAfterFanout(t *testing.T) {
	ctx, log := newWarnCtx(t)

	if _, err := Run(ctx, "before", func() (int, error) { return 1, nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := Map(ctx, "items", []int{1, 2},
		func(n int, b *BranchContext, _ int) (int, error) {
			return RunWithBranch(b, "work", func() (int, error) { return n, nil })
		}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := Run(ctx, "after", func() (int, error) { return 1, nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// 20 — the opt-out silences detector 2 as well as the all-inert gate.
func TestOptOutSilencesEnclosingClaimWarning(t *testing.T) {
	ctx, log := newWarnCtx(t)

	_, err := Map(ctx, "items", []int{1, 2},
		func(n int, b *BranchContext, _ int) (int, error) {
			if _, e := Run(ctx, "audit", func() (int, error) { return n, nil }); e != nil {
				return 0, e
			}
			return RunWithBranch(b, "work", func() (int, error) { return n, nil })
		}, ParallelOptions{SkipScopedClientCheck: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// 21 — detector 2 dedups per name like the all-inert gate, so a fan-out inside
// a loop reports once rather than once per iteration.
func TestEnclosingClaimWarningDedupsPerName(t *testing.T) {
	ctx, log := newWarnCtx(t)

	for i := 0; i < 3; i++ {
		if _, err := Map(ctx, "items", []int{1, 2},
			func(n int, b *BranchContext, _ int) (int, error) {
				if _, e := Run(ctx, "audit", func() (int, error) { return n, nil }); e != nil {
					return 0, e
				}
				return RunWithBranch(b, "work", func() (int, error) { return n, nil })
			}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if got := log.warnings(); len(got) != 1 {
		t.Errorf("expected 1 warning across 3 identical fan-outs, got %d: %v", len(got), got)
	}
}

// 22 — a SIBLING fan-out must not be blamed for another branch's misuse.
//
// enclosingClaims is one run-wide counter, so a naive before/after window spans
// everything concurrent, not just this fan-out's subtree. Before
// enclosingClaimsFor gated on the outermost depth, branch B's entirely correct
// nested fan-out was blamed for branch A's misuse on the FIRST run.
func TestSiblingFanoutNotBlamedForAnothersEnclosingClaims(t *testing.T) {
	for run := 0; run < 20; run++ {
		ctx, log := newWarnCtx(t)

		_, err := Parallel(ctx, "outer", []func(*BranchContext) (int, error){
			// A: nested fan-out whose branch misuses the root context.
			func(a *BranchContext) (int, error) {
				_, e := ParallelWithBranch(a, "dirty", []func(*BranchContext) (int, error){
					func(_ *BranchContext) (int, error) {
						return Run(ctx, "audit", func() (int, error) { return 1, nil })
					},
				})
				return 0, e
			},
			// B: nested fan-out that is entirely correct.
			func(b *BranchContext) (int, error) {
				_, e := ParallelWithBranch(b, "clean", []func(*BranchContext) (int, error){
					func(inner *BranchContext) (int, error) {
						time.Sleep(time.Millisecond) // widen the overlap window
						return RunWithBranch(inner, "work", func() (int, error) { return 2, nil })
					},
				})
				return 0, e
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// "dirty" legitimately warns via detector 2 — its branch skipped its own
		// scope entirely. "outer" legitimately warns via detector 1. The only
		// unacceptable outcome is blaming "clean", which did nothing wrong.
		for _, w := range log.warnings() {
			if strings.HasPrefix(w, "clean") {
				t.Fatalf("run %d: correct sibling fan-out was blamed: %s", run, w)
			}
		}
	}
}

// 23 — the misuse is still reported by detector 1, attributed to the OUTERMOST
// fan-out. Guards against "fix the sibling false positive by going silent".
//
// The inner fan-out also warns, but via detector 2: its branch skipped its own
// scope entirely. Two warnings at two real call sites, each accurate.
func TestNestedEnclosingClaimReportedAtOutermostFanout(t *testing.T) {
	ctx, log := newWarnCtx(t)

	_, err := Parallel(ctx, "outer", []func(*BranchContext) (int, error){
		func(a *BranchContext) (int, error) {
			_, e := ParallelWithBranch(a, "inner", []func(*BranchContext) (int, error){
				func(_ *BranchContext) (int, error) {
					return Run(ctx, "audit", func() (int, error) { return 1, nil })
				},
			})
			return 0, e
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var outer string
	for _, w := range log.warnings() {
		if strings.HasPrefix(w, "outer:") {
			outer = w
		}
	}
	if outer == "" {
		t.Fatalf("outermost fan-out did not report the enclosing claim: %v", log.warnings())
	}
	if !strings.Contains(outer, "1 step(s) were claimed on the ENCLOSING") {
		t.Errorf("outer warning is not the detector-1 message: %s", outer)
	}
}

// 24 — the docs claim enclosing Invoke and Publish are caught (they claim their
// step id before yielding) while Sleep/WaitForEvent yield out first. Pin the
// caught half; the yield hole is documented, not tested, because reaching it
// requires unwinding the whole fan-out.
func TestEnclosingInvokeIsCaught(t *testing.T) {
	ctx, log := newWarnCtx(t)
	// Memoized so Invoke returns instead of yielding.
	ctx.exec.completedSteps["r1:child-fn:0"] = &CompletedStep{
		ID: "r1:child-fn:0", Status: "completed", Output: "done",
	}

	_, err := Map(ctx, "items", []int{1},
		func(n int, b *BranchContext, _ int) (int, error) {
			if _, e := Invoke[string](ctx, "child-fn", nil); e != nil {
				return 0, e
			}
			return RunWithBranch(b, "work", func() (int, error) { return n, nil })
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantOneWarning(t, log, "items: 1 step(s) were claimed on the ENCLOSING")
}

// 25 — correct use of the branch-scoped twins must not trip detector 1.
func TestBranchScopedTwinsDoNotTripEnclosingDetector(t *testing.T) {
	ctx, log := newWarnCtx(t)
	ctx.exec.completedSteps["r1:items:0:child-fn:0"] = &CompletedStep{
		ID: "r1:items:0:child-fn:0", Status: "completed", Output: "done",
	}

	_, err := Map(ctx, "items", []int{1},
		func(n int, b *BranchContext, _ int) (int, error) {
			if _, e := InvokeWithBranch[string](b, "child-fn", nil); e != nil {
				return 0, e
			}
			return RunWithBranch(b, "work", func() (int, error) { return n, nil })
		})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// ---------------------------------------------------------------------------
// The three gaps left open after the first review pass: the yield hole, the
// pull-worker logger wiring, and allSettled + enclosing claims.
// ---------------------------------------------------------------------------

// 26 — the yield hole, pinned rather than assumed.
//
// A branch calling the ENCLOSING Sleep/SleepUntil/WaitForEvent claims its root
// step id and THEN panics with a yield signal. Parallel re-raises that panic
// before warnUnscopedBranches runs, so the diagnostic never fires. The docs say
// so; this proves it, and proves it is the yield doing the suppressing rather
// than the claim simply not happening — enclosingClaims is asserted non-zero.
//
// If a future refactor moves the warn call above the yield re-panic, this test
// starts failing and the doc comment stops being a lie.
func TestYieldSuppressesEnclosingClaimWarning(t *testing.T) {
	ctx, log := newWarnCtx(t)

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected the enclosing Sleep to yield")
			}
			if _, ok := r.(*yieldSignal); !ok {
				panic(r)
			}
		}()
		_, _ = Map(ctx, "items", []int{1},
			func(n int, b *BranchContext, _ int) (int, error) {
				// Enclosing context: claims a root step id, then yields.
				return 0, Sleep(ctx, "nap", time.Hour)
			})
	}()

	if got := ctx.exec.enclosingClaims.Load(); got == 0 {
		t.Fatal("expected the enclosing Sleep to have claimed a root step id")
	}
	// The claim happened, and the warning still did not — the yield unwound
	// past the detector. This is the documented hole, not a silent regression.
	wantNoWarnings(t, log)
}

// 27 — enclosing Run followed by a yielding enclosing Sleep in the SAME branch.
// Confirms the hole is about unwinding, not about which primitive claimed.
func TestYieldSuppressesEvenAfterANonYieldingEnclosingClaim(t *testing.T) {
	ctx, log := newWarnCtx(t)

	func() {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(*yieldSignal); !ok {
					panic(r)
				}
			}
		}()
		_, _ = Map(ctx, "items", []int{1},
			func(n int, b *BranchContext, _ int) (int, error) {
				if _, e := Run(ctx, "audit", func() (int, error) { return n, nil }); e != nil {
					return 0, e
				}
				return 0, Sleep(ctx, "nap", time.Hour)
			})
	}()

	if got := ctx.exec.enclosingClaims.Load(); got < 2 {
		t.Fatalf("expected both enclosing claims to be recorded, got %d", got)
	}
	wantNoWarnings(t, log)
}

// 28 — allSettled: detector 1 still reports, detector 2 still does not.
//
// The two detectors treat errored branches differently on purpose. Detector 2
// excludes them (an errored branch is not evidence the author ignored the
// scope). Detector 1 does not care: the claim on the shared root counter
// happened regardless of how the branch finished, and it is the claim that
// makes a resume ambiguous.
func TestAllSettledStillReportsEnclosingClaims(t *testing.T) {
	ctx, log := newWarnCtx(t)

	_, err := Map(ctx, "items", []int{1, 2},
		func(n int, b *BranchContext, _ int) (int, error) {
			if _, e := Run(ctx, "audit", func() (int, error) { return n, nil }); e != nil {
				return 0, e
			}
			return 0, errors.New("boom")
		}, ParallelOptions{OnError: "allSettled"})
	if err != nil {
		t.Fatalf("allSettled should not surface a branch error: %v", err)
	}
	wantOneWarning(t, log, "items: 2 step(s) were claimed on the ENCLOSING")
}

// 29 — the complement: allSettled with errors but NO enclosing claims stays
// silent, so test 28 above is pinning detector 1 rather than a blanket
// "allSettled always warns".
func TestAllSettledWithoutEnclosingClaimsStaysQuiet(t *testing.T) {
	ctx, log := newWarnCtx(t)

	if _, err := Map(ctx, "items", []int{1, 2},
		func(n int, _ *BranchContext, _ int) (int, error) {
			return 0, errors.New("boom")
		}, ParallelOptions{OnError: "allSettled"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantNoWarnings(t, log)
}

// 30 — the pull-worker logger wiring (job_executor.go).
//
// This is the ONLY path where the #1792 diagnostic reaches a logger the user
// actually configured: push mode has none and falls back to a fresh default,
// and every other test here injects exec.logger directly. A one-line wiring
// regression would silence the warning for every real worker and no other test
// would notice.
func TestJobExecutorThreadsWorkerLoggerIntoStepDiagnostics(t *testing.T) {
	log := &captureLogger{}

	fn := Function{
		Config: FunctionConfig{ID: "fn"},
		Handler: func(ctx Context) (any, error) {
			// Inert fan-out: every branch ignores the scope it was handed.
			_, err := Map(ctx, "ingest", []int{1, 2},
				func(n int, _ *BranchContext, _ int) (int, error) { return n, nil })
			return nil, err
		},
	}

	exec := &jobExecutor{
		functions: map[string]Function{"fn": fn},
		logger:    log,
	}
	job := &jobAssignment{
		JobID:      "job-1",
		RunID:      "run-1",
		FunctionID: "fn",
		Attempt:    1,
		Event:      jobEvent{ID: "e1", Name: "e", Data: json.RawMessage(`{}`)},
	}

	if err := exec.execute(context.Background(), job, &recordingReporter{}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	wantOneWarning(t, log, "ingest: 2/2 branches did not use the scoped")
}
