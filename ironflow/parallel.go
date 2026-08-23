package ironflow

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// StepContext is an interface for step execution that both Context and BranchContext implement.
type StepContext interface {
	generateStepID(name string) string
	getCompletedStep(stepID string) (*CompletedStep, bool)
	recordStep(step *StepResult)
	isResumingFrom(stepID, stepType string) bool
	getResumeData() any
	markResumeHandled()
	createBranchContext(parallelName string, branchIndex int) *BranchContext
	// execution returns the run-wide execution context behind this scope.
	//
	// Named neither "exec" nor "parent" because Context has a field `exec` and
	// BranchContext has a field `parent`, and Go forbids a method sharing a
	// name with a field. Step primitives read run-wide config through this
	// (serverURL, apiKey, testInterceptor, runID) while taking their step IDs
	// from the scope itself — which is the whole point of the seam: one body
	// per primitive, correct at the root and inside a branch.
	//
	// Unexported, so StepContext is sealed to this package.
	execution() *executionContext
	// markScopedUsed records that this scope was used. No-op at the root.
	markScopedUsed()
}

// Ensure Context implements StepContext
func (c Context) generateStepID(name string) string {
	return c.exec.generateStepID(name)
}

func (c Context) getCompletedStep(stepID string) (*CompletedStep, bool) {
	step, ok := c.exec.completedSteps[stepID]
	return step, ok
}

func (c Context) recordStep(step *StepResult) {
	c.exec.executedStepsMu.Lock()
	c.exec.executedSteps = append(c.exec.executedSteps, step)
	c.exec.executedStepsMu.Unlock()
}

func (c Context) isResumingFrom(stepID, stepType string) bool {
	return c.exec.isResumingFrom(stepID, stepType)
}

func (c Context) getResumeData() any {
	if c.exec.resumeContext == nil {
		return nil
	}
	return c.exec.resumeContext.Data
}

func (c Context) markResumeHandled() {
	c.exec.resumeHandled = true
}

func (c Context) createBranchContext(parallelName string, branchIndex int) *BranchContext {
	return c.exec.createBranchContext(parallelName, branchIndex)
}

func (c Context) execution() *executionContext {
	return c.exec
}

// markScopedUsed is a no-op: the root Context is never a branch.
func (c Context) markScopedUsed() {}

// enclosingClaimsFor attributes root step-id claims to a fan-out.
//
// enclosingClaims is a single run-wide counter, so a naive
// "load after minus load before" window spans everything that ran concurrently
// — including SIBLING fan-outs. Measured: with branch A's nested fan-out
// misusing the root context and branch B's nested fan-out entirely correct,
// B was blamed on the first run. A sibling is not an ancestor; blaming it sends
// the reader to code that is fine.
//
// Only the OUTERMOST fan-out in flight (myDepth == 1) reports, so every claim
// it counts is genuinely somewhere in its own subtree. That costs precision —
// the message names the outer fan-out rather than the exact inner one — but the
// remedy it prints is the same either way, and a warning that points at the
// wrong call site is worse than one that points at a coarser right one.
func enclosingClaimsFor(exec *executionContext, myDepth, claimsBefore int64) int64 {
	if myDepth != 1 {
		return 0
	}
	return exec.enclosingClaims.Load() - claimsBefore
}

// warnUnscopedBranches emits the #1671/#1792 diagnostic for a fan-out whose
// branches all ignored the scope they were handed.
//
// A branch is durable under this scope only if its callback uses the
// *BranchContext it is given. Ignoring it compiles and appears to work — the
// only symptom is a missing run timeline and no memoization on retry.
//
// TWO detectors, because one gate cannot see both misuses:
//
//  1. ENCLOSING-CONTEXT CLAIMS (enclosingClaims > 0) — any root step id claimed
//     while this fan-out was in flight. Reported first, because it is the shape
//     with the worse consequence: those branches DID persist steps, but off a
//     counter shared by every branch, so which branch gets which index is
//     scheduling-dependent and a resume can hand one branch another's output.
//  2. ALL-INERT (unscoped == len(branchContexts)) — every branch skipped its
//     scope entirely, so nothing was persisted under it. Gated on EVERY branch
//     because a mixed result means the author knows about the scope and some
//     branches short-circuit — correct code that would otherwise warn forever.
//     Detector 1 is what keeps that gate safe: the mixed shape it used to hide
//     is now caught above instead of passed over.
//
// Both share two gates: once per distinct name per run (shouldWarnUnscoped), so
// a nested fan-out emits one line rather than one per outer item; and
// SkipScopedClientCheck, the opt-out for a fan-out with nothing to memoize.
//
// ponytail: a best-effort lint, not a guarantee. Remaining holes, all
// deliberate:
//   - Silent whenever any branch errors (under the default failFast the caller
//     returns before reaching here; under "allSettled" one error keeps
//     unscoped < len(branchContexts)) or YIELDS. The yield hole is the sharpest
//     one left: a branch calling the enclosing context's Sleep / SleepUntil /
//     WaitForEvent yields out through a panic before this runs, so detector 1
//     never gets to report it. Enclosing Run / Invoke / Publish ARE caught,
//     because they claim their id before any yield. Pinned by
//     TestYieldSuppressesEnclosingClaimWarning, which asserts the claim WAS
//     recorded and the warning still did not fire — so this stays a known hole
//     rather than quietly becoming a regression.
//   - Detector 2 can still false-positive on correct conditional code where
//     every item happens to short-circuit on this particular run.
//     SkipScopedClientCheck is the escape hatch.
//   - Dedup is keyed on `name`, so two distinct call sites that share a name
//     collapse to one warning.
//   - Detector 1 reports only at the OUTERMOST fan-out (see
//     enclosingClaimsFor), so a nested misuse is attributed one level coarser
//     than where it happened. That is deliberate: the run-wide counter cannot
//     tell a descendant's claim from a concurrent sibling's, and blaming a
//     sibling that did nothing wrong is worse than a coarser attribution.
//     Detector 2 still names the exact inner fan-out when its branches skipped
//     their scope outright.
//
// Widening any of them reintroduces a worse false positive; live with the holes
// until someone hits one.
func warnUnscopedBranches(exec *executionContext, name string, branchContexts []*BranchContext, errs []error, options ParallelOptions, enclosingClaims int64) {
	if options.SkipScopedClientCheck {
		return
	}

	// Detector 1: enclosing-context claims. First, because it is the shape with
	// the worse consequence — those branches did claim real step ids, just from
	// the shared root counter in goroutine-scheduling order, so on resume one
	// branch can read another's memoized output. The all-inert gate below
	// structurally cannot see this: a branch that also touched its own scope
	// leaves unscoped == 0.
	if enclosingClaims > 0 && exec.shouldWarnUnscoped(name) {
		exec.warnLogger().Warn(fmt.Sprintf(
			"%s: %d step(s) were claimed on the ENCLOSING function's context while this "+
				"fan-out was running. Those steps are memoized outside this scope, and their "+
				"index comes from a counter shared by every branch — which branch gets which "+
				"index depends on goroutine scheduling, so on resume a branch can read another "+
				"branch's output. Route them through the *BranchContext instead — "+
				"ironflow.RunWithBranch(b, \"work\", ...), ironflow.InvokeWithBranch(b, ...), "+
				"ironflow.PublishWithBranch(b, ...). "+
				"If this fan-out genuinely has nothing to memoize, set ParallelOptions.SkipScopedClientCheck.",
			name, enclosingClaims))
		return
	}

	unscoped := 0
	for i, b := range branchContexts {
		if !b.scopedClientUsed.Load() && errs[i] == nil {
			unscoped++
		}
	}
	if unscoped == 0 || unscoped != len(branchContexts) {
		return
	}
	// Last, because it burns the name — let the cheap gates short-circuit first.
	if !exec.shouldWarnUnscoped(name) {
		return
	}

	exec.warnLogger().Warn(fmt.Sprintf(
		"%s: %d/%d branches did not use the scoped context they were handed. "+
			"Work done directly in the callback body is never memoized; work routed "+
			"through the enclosing function's context is memoized outside this scope. "+
			"Either way, use the *BranchContext — e.g. ironflow.RunWithBranch(b, \"work\", ...). "+
			"If this fan-out genuinely has nothing to memoize, set ParallelOptions.SkipScopedClientCheck.",
		name, unscoped, len(branchContexts)))
}

// Parallel executes multiple branches concurrently and returns results in order.
//
// Each branch receives a BranchContext for scoped step execution.
// Use ParallelOptions to control concurrency and error handling.
//
// WHY: a branch is NOT itself a recorded step. Parallel only calls your callback
// with a scoped *BranchContext — durability comes from what that callback does
// with it. RunWithBranch(b, "fetch", ...) persists a step; calling fetch()
// directly persists nothing, memoizes nothing, and re-runs in full on every
// retry. Both compile, so the SDK logs a warning when EVERY branch in one call
// skipped the scope. That check is a best-effort lint, not a guarantee — it
// stays silent when any branch errors or yields, and when only some branches
// skip. Set ParallelOptions.SkipScopedClientCheck to opt out.
//
// Reaching for the enclosing function's Context is a third case: it does record
// real steps, just outside this parallel's scope. Migrating such a branch to the
// scoped form CHANGES its step ID from "{runID}:{name}:0" to
// "{runID}:{parallelName}:{branchIndex}:{name}:0", and nothing bridges the two —
// preferLegacyStepID does not, because it early-returns for any colon-free name.
// Let in-flight runs drain before shipping that rewrite or they re-execute
// completed steps for real.
//
// Example:
//
//	results, err := ironflow.Parallel(ctx, "process-chunks",
//	    []func(*ironflow.BranchContext) (string, error){
//	        func(b *ironflow.BranchContext) (string, error) {
//	            return RunWithBranch[string](b, "chunk-1", func() (string, error) {
//	                return processChunk(data[0])
//	            })
//	        },
//	        func(b *ironflow.BranchContext) (string, error) {
//	            return RunWithBranch[string](b, "chunk-2", func() (string, error) {
//	                return processChunk(data[1])
//	            })
//	        },
//	    },
//	)
func Parallel[T any](ctx Context, name string, branches []func(*BranchContext) (T, error), opts ...ParallelOptions) ([]T, error) {
	var options ParallelOptions
	if len(opts) > 0 {
		options = opts[0]
	}
	if options.OnError == "" {
		options.OnError = "failFast"
	}

	branchCount := len(branches)
	if branchCount == 0 {
		return []T{}, nil
	}

	results := make([]T, branchCount)
	errors := make([]error, branchCount)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var yieldSig *yieldSignal
	cancelled := false

	// Semaphore for concurrency control
	var sem chan struct{}
	if options.Concurrency > 0 {
		sem = make(chan struct{}, options.Concurrency)
	}

	// Track that a fan-out is in flight so executionContext.generateStepID can
	// tell a root step claimed from inside a branch from an ordinary one. The
	// handler goroutine is parked in wg.Wait() below, so while this is raised
	// only branch goroutines can reach the root counter (#1792).
	exec := ctx.execution()
	myDepth := exec.fanOutDepth.Add(1)
	defer exec.fanOutDepth.Add(-1)
	claimsBefore := exec.enclosingClaims.Load()

	// Pre-create branch contexts
	branchContexts := make([]*BranchContext, branchCount)
	for i := 0; i < branchCount; i++ {
		branchContexts[i] = ctx.createBranchContext(name, i)
	}

	for i := 0; i < branchCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Check cancellation
			mu.Lock()
			if cancelled && options.OnError == "failFast" {
				mu.Unlock()
				return
			}
			mu.Unlock()

			// Acquire semaphore
			if sem != nil {
				sem <- struct{}{}
				defer func() { <-sem }()
			}

			// Execute with panic recovery (for yield signals)
			func() {
				defer func() {
					if r := recover(); r != nil {
						if signal, ok := r.(*yieldSignal); ok {
							// Capture yield signal
							mu.Lock()
							if yieldSig == nil {
								yieldSig = signal
							}
							cancelled = true
							mu.Unlock()
						} else {
							// Re-panic non-yield panics
							panic(r)
						}
					}
				}()

				result, err := branches[idx](branchContexts[idx])
				if err != nil {
					mu.Lock()
					errors[idx] = err
					if options.OnError == "failFast" && firstErr == nil {
						firstErr = err
						cancelled = true
					}
					mu.Unlock()
				} else {
					mu.Lock()
					results[idx] = result
					mu.Unlock()
				}
			}()
		}(i)
	}

	wg.Wait()

	// Handle yield signal (re-panic to pause execution)
	if yieldSig != nil {
		panic(yieldSig)
	}

	// Handle errors
	if firstErr != nil {
		return nil, firstErr
	}

	// Check for any errors in results (for non-failFast modes)
	if options.OnError != "allSettled" {
		for _, err := range errors {
			if err != nil {
				return nil, err
			}
		}
	}

	warnUnscopedBranches(exec, name, branchContexts, errors, options,
		enclosingClaimsFor(exec, myDepth, claimsBefore))

	return results, nil
}

// Map executes a function for each item in parallel and returns results in order.
//
// Each item callback receives the item, a BranchContext, and the index.
//
// WHY: a branch is NOT itself a recorded step. Map persists one step per item
// only if your callback uses the *BranchContext it is handed:
//
//	// One memoized step per file — retries skip finished files.
//	ironflow.Map(ctx, "ingest", files, func(f string, b *ironflow.BranchContext, i int) (Doc, error) {
//	    return ironflow.RunWithBranch(b, fmt.Sprintf("ingest-%d", i), func() (Doc, error) { return ingest(f) })
//	})
//
//	// Persists NOTHING — the whole map re-runs on every retry.
//	ironflow.Map(ctx, "ingest", files, func(f string, b *ironflow.BranchContext, i int) (Doc, error) {
//	    return ingest(f)
//	})
//
// Keep "." out of a step ID: it goes verbatim into the NATS subject
// system.run.{runID}.step.{stepID}.{event}, so a filename-derived ID adds a
// subject token and fixed-arity consumers stop matching it.
//
// See Parallel for the warning's three gates and for the drain-first caveat when
// migrating a branch off the enclosing Context.
//
// Example:
//
//	results, err := ironflow.Map(ctx, "process-items", items,
//	    func(item Item, b *ironflow.BranchContext, index int) (Result, error) {
//	        return RunWithBranch[Result](b, fmt.Sprintf("process-%d", index), func() (Result, error) {
//	            return processItem(item)
//	        })
//	    },
//	)
func Map[T, R any](ctx Context, name string, items []T, fn func(T, *BranchContext, int) (R, error), opts ...ParallelOptions) ([]R, error) {
	return Parallel(ctx, name, mapBranches(items, fn), opts...)
}

// MapWithBranch is the branch-scoped equivalent of Map — a nested fan-out over
// items from inside a parallel branch.
//
// WHY: the JS SDK hands every branch a full step client, so `branchStep.map(...)`
// has always worked there. Go had ParallelWithBranch but no MapWithBranch, so a
// nested fan-out over a slice had to be hand-rolled into a []func slice, and the
// obvious alternative — calling Map with the enclosing ctx — records every item's
// step at the function's top level instead of under the branch (#1792).
//
// As with Map, a branch is not itself a recorded step: use the *BranchContext
// each callback is handed.
//
// Example:
//
//	results, err := ironflow.ParallelWithBranch(b, "shards", []func(*ironflow.BranchContext) ([]Doc, error){
//	    func(shard *ironflow.BranchContext) ([]Doc, error) {
//	        return ironflow.MapWithBranch(shard, "ingest", files,
//	            func(f string, item *ironflow.BranchContext, i int) (Doc, error) {
//	                return ironflow.RunWithBranch(item, fmt.Sprintf("ingest-%d", i), func() (Doc, error) {
//	                    return ingest(f)
//	                })
//	            })
//	    },
//	})
func MapWithBranch[T, R any](b *BranchContext, name string, items []T, fn func(T, *BranchContext, int) (R, error), opts ...ParallelOptions) ([]R, error) {
	return ParallelWithBranch(b, name, mapBranches(items, fn), opts...)
}

// mapBranches adapts an item slice to the branch-callback slice Parallel takes.
func mapBranches[T, R any](items []T, fn func(T, *BranchContext, int) (R, error)) []func(*BranchContext) (R, error) {
	branches := make([]func(*BranchContext) (R, error), len(items))
	for i, item := range items {
		idx := i
		it := item
		branches[i] = func(b *BranchContext) (R, error) {
			return fn(it, b, idx)
		}
	}
	return branches
}

// RunWithBranch executes a step with memoization using a BranchContext.
//
// This is the branch-scoped equivalent of ironflow.Run() for use within parallel branches.
//
// Example:
//
//	result, err := ironflow.RunWithBranch[MyResult](branchCtx, "fetch-data", func() (MyResult, error) {
//	    return fetchData()
//	})
func RunWithBranch[T any](b *BranchContext, name string, fn func() (T, error), opts ...StepOption) (T, error) {
	b.markScopedUsed()

	var zero T

	if b.parent.testInterceptor != nil {
		return testRunStep[T](b.parent, name)
	}

	stepID := b.generateStepID(name)

	// Check if step is already completed (memoized)
	if completed, ok := b.getCompletedStep(stepID); ok && completed.Status == "completed" {
		// Unmarshal the output to the expected type
		outputBytes, err := json.Marshal(completed.Output)
		if err != nil {
			return zero, NewStepError(fmt.Sprintf("failed to marshal memoized output: %v", err), stepID, name, false, err)
		}

		var result T
		if err := json.Unmarshal(outputBytes, &result); err != nil {
			return zero, NewStepError(fmt.Sprintf("failed to unmarshal memoized output: %v", err), stepID, name, false, err)
		}

		return result, nil
	}

	// Resolve options
	var options stepOptions
	for _, opt := range opts {
		opt(&options)
	}

	// Resolve timeout: step-level > function-level > no timeout
	timeout := options.Timeout
	if timeout == 0 && b.parent.stepTimeout > 0 {
		timeout = b.parent.stepTimeout
	}

	// Execute the step
	startedAt := time.Now()

	result, err, timedOut := runWithTimeout(fn, timeout, stepID, name, startedAt, b.recordStep)
	if timedOut {
		return zero, err
	}

	endedAt := time.Now()
	duration := endedAt.Sub(startedAt)

	if err != nil {
		// Record failed step
		stepResult := &StepResult{
			ID:        stepID,
			Name:      name,
			Type:      "invoke",
			Status:    "failed",
			StartedAt: startedAt,
			EndedAt:   &endedAt,
			Duration:  duration,
			Error: &StepErrorInfo{
				Message:   err.Error(),
				Retryable: IsRetryable(err),
			},
		}
		b.recordStep(stepResult)

		return zero, NewStepError(err.Error(), stepID, name, IsRetryable(err), err)
	}

	// Record successful step
	stepResult := &StepResult{
		ID:        stepID,
		Name:      name,
		Type:      "invoke",
		Status:    "completed",
		StartedAt: startedAt,
		EndedAt:   &endedAt,
		Duration:  duration,
		Output:    result,
	}
	b.recordStep(stepResult)

	return result, nil
}

// SleepWithBranch pauses execution for a duration using a BranchContext.
//
// This is the branch-scoped equivalent of ironflow.Sleep() for use within parallel branches.
func SleepWithBranch(b *BranchContext, name string, duration time.Duration) error {
	b.markScopedUsed()

	if b.parent.testInterceptor != nil {
		b.parent.testInterceptor.SleepStep(name)
		b.parent.testInterceptor.RecordStep(name, "sleep", nil, nil)
		return nil
	}

	stepID := b.generateStepID(name)

	// Check if resuming from this sleep
	if b.isResumingFrom(stepID, "sleep") {
		b.markResumeHandled()
		return nil
	}

	// Check if step is already completed (memoized)
	if completed, ok := b.getCompletedStep(stepID); ok && completed.Status == "completed" {
		return nil
	}

	// Calculate wake time
	wakeAt := time.Now().Add(duration)

	// Throw yield signal
	panic(newSleepYield(stepID, wakeAt.Format(time.RFC3339)))
}

// WaitForEventWithBranch waits for an external event using a BranchContext.
//
// This is the branch-scoped equivalent of ironflow.WaitForEvent() for use within parallel branches.
func WaitForEventWithBranch(b *BranchContext, name string, filter EventFilter) (Event, error) {
	b.markScopedUsed()

	if b.parent.testInterceptor != nil {
		event, err := b.parent.testInterceptor.WaitForEventStep(name, filter)
		b.parent.testInterceptor.RecordStep(name, "waitForEvent", event, err)
		if err != nil {
			return Event{}, err
		}
		return event, nil
	}

	stepID := b.generateStepID(name)

	// Check if resuming from this wait with the event data
	if b.isResumingFrom(stepID, "wait_for_event") {
		b.markResumeHandled()

		// The resume data contains the event that matched
		if data := b.getResumeData(); data != nil {
			eventBytes, err := json.Marshal(data)
			if err != nil {
				return Event{}, NewStepError(fmt.Sprintf("failed to marshal resume event: %v", err), stepID, name, false, err)
			}

			var event Event
			if err := json.Unmarshal(eventBytes, &event); err != nil {
				return Event{}, NewStepError(fmt.Sprintf("failed to unmarshal resume event: %v", err), stepID, name, false, err)
			}

			return event, nil
		}
	}

	// Check if step is already completed (memoized)
	if completed, ok := b.getCompletedStep(stepID); ok && completed.Status == "completed" {
		eventBytes, err := json.Marshal(completed.Output)
		if err != nil {
			return Event{}, NewStepError(fmt.Sprintf("failed to marshal memoized event: %v", err), stepID, name, false, err)
		}

		var event Event
		if err := json.Unmarshal(eventBytes, &event); err != nil {
			return Event{}, NewStepError(fmt.Sprintf("failed to unmarshal memoized event: %v", err), stepID, name, false, err)
		}

		return event, nil
	}

	// Apply default timeout
	if filter.Timeout == 0 {
		filter.Timeout = 7 * 24 * time.Hour
	}

	// Throw yield signal
	panic(newWaitEventYield(stepID, &filter))
}

// ParallelWithBranch executes multiple branches in parallel within a branch context (for nested parallel).
//
// WHY: as with Parallel, a nested branch is NOT itself a recorded step — use the
// *BranchContext each callback is handed. Opening this call marks the ENCLOSING
// branch as having used its scope, even when branches is empty, so a correct
// callback whose only work is an empty nested fan-out is not warned about.
func ParallelWithBranch[T any](b *BranchContext, name string, branches []func(*BranchContext) (T, error), opts ...ParallelOptions) ([]T, error) {
	// Opening a nested parallel/map counts as using the enclosing branch's
	// scope, even if this call turns out to have zero items. Marking in
	// createBranchContext instead would miss the empty-collection case and
	// warn about a correct callback (#1792).
	b.markScopedUsed()

	var options ParallelOptions
	if len(opts) > 0 {
		options = opts[0]
	}
	if options.OnError == "" {
		options.OnError = "failFast"
	}

	branchCount := len(branches)
	if branchCount == 0 {
		return []T{}, nil
	}

	results := make([]T, branchCount)
	errors := make([]error, branchCount)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	var yieldSig *yieldSignal
	cancelled := false

	// Semaphore for concurrency control
	var sem chan struct{}
	if options.Concurrency > 0 {
		sem = make(chan struct{}, options.Concurrency)
	}

	// Track that a fan-out is in flight so executionContext.generateStepID can
	// tell a root step claimed from inside a branch from an ordinary one. The
	// handler goroutine is parked in wg.Wait() below, so while this is raised
	// only branch goroutines can reach the root counter (#1792).
	exec := b.execution()
	myDepth := exec.fanOutDepth.Add(1)
	defer exec.fanOutDepth.Add(-1)
	claimsBefore := exec.enclosingClaims.Load()

	// Pre-create nested branch contexts
	branchContexts := make([]*BranchContext, branchCount)
	for i := 0; i < branchCount; i++ {
		branchContexts[i] = b.createBranchContext(name, i)
	}

	for i := 0; i < branchCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Check cancellation
			mu.Lock()
			if cancelled && options.OnError == "failFast" {
				mu.Unlock()
				return
			}
			mu.Unlock()

			// Acquire semaphore
			if sem != nil {
				sem <- struct{}{}
				defer func() { <-sem }()
			}

			// Execute with panic recovery
			func() {
				defer func() {
					if r := recover(); r != nil {
						if signal, ok := r.(*yieldSignal); ok {
							mu.Lock()
							if yieldSig == nil {
								yieldSig = signal
							}
							cancelled = true
							mu.Unlock()
						} else {
							panic(r)
						}
					}
				}()

				result, err := branches[idx](branchContexts[idx])
				if err != nil {
					mu.Lock()
					errors[idx] = err
					if options.OnError == "failFast" && firstErr == nil {
						firstErr = err
						cancelled = true
					}
					mu.Unlock()
				} else {
					mu.Lock()
					results[idx] = result
					mu.Unlock()
				}
			}()
		}(i)
	}

	wg.Wait()

	// Handle yield signal
	if yieldSig != nil {
		panic(yieldSig)
	}

	// Handle errors
	if firstErr != nil {
		return nil, firstErr
	}

	if options.OnError != "allSettled" {
		for _, err := range errors {
			if err != nil {
				return nil, err
			}
		}
	}

	warnUnscopedBranches(exec, name, branchContexts, errors, options,
		enclosingClaimsFor(exec, myDepth, claimsBefore))

	return results, nil
}

// CompensateInBranch registers a compensation handler for a step within a parallel branch.
//
// This is the branch-scoped equivalent of ironflow.Compensate() for use within parallel branches.
// Compensations are stored on the parent execution context and run in reverse registration order.
//
// Example:
//
//	result, err := ironflow.RunWithBranch[Payment](b, "charge-card", func() (Payment, error) {
//	    return chargeCard(order)
//	})
//	if err != nil { return zero, err }
//	ironflow.CompensateInBranch(b, "charge-card", func() error {
//	    return refundPayment(result.TransactionID)
//	})
func CompensateInBranch(b *BranchContext, stepName string, fn func() error) {
	// Counts as using the scope even though a compensation registers run-wide
	// and claims no branch-scoped step id. A false negative (staying quiet for
	// a branch that only compensates) is the right way for a lint to be wrong.
	b.markScopedUsed()

	if b.parent.testInterceptor != nil {
		b.parent.testInterceptor.CompensateStep(stepName, fn)
		return
	}
	b.registerCompensation(stepName, fn)
}
