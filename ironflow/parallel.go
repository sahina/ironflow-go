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

// Parallel executes multiple branches concurrently and returns results in order.
//
// Each branch receives a BranchContext for scoped step execution.
// Use ParallelOptions to control concurrency and error handling.
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

	return results, nil
}

// Map executes a function for each item in parallel and returns results in order.
//
// Each item callback receives the item, a BranchContext, and the index.
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
	branches := make([]func(*BranchContext) (R, error), len(items))
	for i, item := range items {
		idx := i
		it := item
		branches[i] = func(b *BranchContext) (R, error) {
			return fn(it, b, idx)
		}
	}
	return Parallel(ctx, name, branches, opts...)
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
func ParallelWithBranch[T any](b *BranchContext, name string, branches []func(*BranchContext) (T, error), opts ...ParallelOptions) ([]T, error) {
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
	if b.parent.testInterceptor != nil {
		b.parent.testInterceptor.CompensateStep(stepName, fn)
		return
	}
	b.registerCompensation(stepName, fn)
}
