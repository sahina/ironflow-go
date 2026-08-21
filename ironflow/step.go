package ironflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// compensationEntry stores a registered compensation handler.
type compensationEntry struct {
	stepName string
	fn       func() error
}

// executionContext manages step execution state.
type executionContext struct {
	runID           string
	functionID      string
	attempt         int
	stepCounters    map[string]int
	completedSteps  map[string]*CompletedStep
	executedSteps   []*StepResult
	executedStepsMu sync.Mutex
	resumeContext   *ResumeContext
	resumeHandled   bool
	// Compensation registry
	compensations   []compensationEntry
	compensationsMu sync.Mutex
	// testInterceptor is set in test mode to intercept step execution.
	// nil in production.
	testInterceptor TestInterceptor
	// stepReporter reports step lifecycle events over the bidirectional stream.
	// nil for polling worker and push mode.
	stepReporter stepLifecycleReporter
	// stepTimeout is the function-level default step timeout.
	stepTimeout time.Duration
	// serverURL is the Ironflow server URL, used by Publish step.
	serverURL string
	// apiKey is the API key for authenticated requests from steps.
	apiKey string
}

// newExecutionContext creates a new execution context from a push request.
func newExecutionContext(req *PushRequest) *executionContext {
	ctx := &executionContext{
		runID:          req.RunID,
		functionID:     req.FunctionID,
		attempt:        req.Attempt,
		stepCounters:   make(map[string]int),
		completedSteps: make(map[string]*CompletedStep),
		executedSteps:  make([]*StepResult, 0),
		resumeContext:  req.Resume,
	}

	// Index completed steps by ID
	for i := range req.Steps {
		step := &req.Steps[i]
		ctx.completedSteps[step.ID] = step
	}

	return ctx
}

// stepIDPartEscaper escapes one segment of a composite step id.
//
// Step ids are "{runID}:{name}:{index}", and a parallel branch scope is
// "{runID}:{parallelName}:{branchIndex}". Unescaped, a top-level step literally
// named "a:0:b" at index 0 and a step named "b" at index 0 inside parallel "a"
// branch 0 both render as "run:a:0:b:0" — two different steps sharing one
// memoization key and one steps row (#1694 item 4).
//
// The step id is the memoization key on BOTH sides of the wire, so this must
// stay byte-identical to escapeStepIdPart in
// sdk/js/node/src/internal/context.ts. Names containing neither ":" nor a
// backslash are returned unchanged, which is what keeps ids stable for
// in-flight runs.
var stepIDPartEscaper = strings.NewReplacer(`\`, `\\`, `:`, `\:`)

// stepIDNamespaces are the prefixes the SDK itself prepends to a user-supplied
// name (step.Publish, compensation). They are structure, not user input:
// escaping their colon would change the id of every existing publish and
// compensation step, so the first resume after an upgrade would miss the
// memoized row and re-run the side effect. Only the leaf after the prefix is
// escaped, which is enough — a branch scope's index segment is always numeric,
// so the escaped leaf cannot line up with one.
var stepIDNamespaces = []string{"compensate:", "publish:"}

func escapeStepIDPart(part string) string {
	for _, ns := range stepIDNamespaces {
		if after, ok := strings.CutPrefix(part, ns); ok {
			return ns + stepIDPartEscaper.Replace(after)
		}
	}
	return stepIDPartEscaper.Replace(part)
}

// generateStepID generates a unique step ID.
func (c *executionContext) generateStepID(name string) string {
	index := c.stepCounters[name]
	c.stepCounters[name] = index + 1
	return c.preferLegacyStepID(
		fmt.Sprintf("%s:%s:%d", c.runID, escapeStepIDPart(name), index),
		fmt.Sprintf("%s:%s:%d", c.runID, name, index),
	)
}

// preferLegacyStepID bridges the escaping rollout (#1694 item 4).
//
// The step id is computed SDK-side, but completedSteps is indexed by whatever id
// the server sent back — i.e. whatever a PRIOR invocation's SDK wrote. A run that
// paused at a segment boundary (sleep / wait_for_event / invoke) before this
// change shipped has rows keyed by the UNESCAPED name. After the deploy the newly
// escaped id would miss that row and re-execute an already-completed step for
// real: a double charge or a duplicate publish, once, at the upgrade boundary,
// for exactly the colon-using names this fix targets.
//
// So: use the legacy id only when it is the one actually memoized. Names without
// ":" or a backslash escape to themselves and never reach the lookup; new runs
// have no legacy rows, so they always get the escaped id.
func (c *executionContext) preferLegacyStepID(id, legacy string) string {
	if legacy == id {
		return id
	}
	if _, ok := c.completedSteps[id]; ok {
		return id
	}
	if _, ok := c.completedSteps[legacy]; ok {
		return legacy
	}
	return id
}

// isResumingFrom checks if we're resuming from a specific step.
func (c *executionContext) isResumingFrom(stepID, stepType string) bool {
	if c.resumeContext == nil {
		return false
	}
	return c.resumeContext.StepID == stepID && c.resumeContext.Type == stepType
}

// createBranchContext creates a scoped context for a parallel branch.
func (c *executionContext) createBranchContext(parallelName string, branchIndex int) *BranchContext {
	scopePrefix := fmt.Sprintf("%s:%s:%d", c.runID, escapeStepIDPart(parallelName), branchIndex)
	return &BranchContext{
		parent:            c,
		scopePrefix:       scopePrefix,
		legacyScopePrefix: fmt.Sprintf("%s:%s:%d", c.runID, parallelName, branchIndex),
		stepCounters:      make(map[string]int),
	}
}

// generateStepID generates a unique step ID with the branch scope prefix.
func (b *BranchContext) generateStepID(name string) string {
	index := b.stepCounters[name]
	b.stepCounters[name] = index + 1
	return b.parent.preferLegacyStepID(
		fmt.Sprintf("%s:%s:%d", b.scopePrefix, escapeStepIDPart(name), index),
		fmt.Sprintf("%s:%s:%d", b.legacyScopePrefix, name, index),
	)
}

// getCompletedStep returns a completed step by ID from the parent context.
func (b *BranchContext) getCompletedStep(stepID string) (*CompletedStep, bool) {
	step, ok := b.parent.completedSteps[stepID]
	return step, ok
}

// recordStep records a step result to the parent context.
func (b *BranchContext) recordStep(step *StepResult) {
	b.parent.executedStepsMu.Lock()
	b.parent.executedSteps = append(b.parent.executedSteps, step)
	b.parent.executedStepsMu.Unlock()
}

// isResumingFrom delegates to the parent context.
func (b *BranchContext) isResumingFrom(stepID, stepType string) bool {
	return b.parent.isResumingFrom(stepID, stepType)
}

// getResumeData returns the resume data from the parent context.
func (b *BranchContext) getResumeData() any {
	if b.parent.resumeContext == nil {
		return nil
	}
	return b.parent.resumeContext.Data
}

// markResumeHandled marks the resume as handled.
func (b *BranchContext) markResumeHandled() {
	b.parent.resumeHandled = true
}

// registerCompensation delegates to the parent context.
func (b *BranchContext) registerCompensation(stepName string, fn func() error) {
	b.parent.registerCompensation(stepName, fn)
}

// createNestedBranchContext creates a nested branch context for nested parallel execution.
func (b *BranchContext) createBranchContext(parallelName string, branchIndex int) *BranchContext {
	scopePrefix := fmt.Sprintf("%s:%s:%d", b.scopePrefix, escapeStepIDPart(parallelName), branchIndex)
	return &BranchContext{
		parent:            b.parent,
		scopePrefix:       scopePrefix,
		legacyScopePrefix: fmt.Sprintf("%s:%s:%d", b.legacyScopePrefix, parallelName, branchIndex),
		stepCounters:      make(map[string]int),
	}
}

// stepOptions holds resolved options for a step.
type stepOptions struct {
	Timeout time.Duration
}

// StepOption configures step execution.
type StepOption func(*stepOptions)

// WithTimeout sets a timeout for step execution.
// If the step function does not complete within the timeout, it fails
// with a StepTimeoutError.
func WithTimeout(d time.Duration) StepOption {
	return func(o *stepOptions) {
		o.Timeout = d
	}
}

// runWithTimeout executes fn with an optional timeout. If timeout > 0 and fn
// doesn't complete in time, it records a failed step via recordStep and returns
// a StepTimeoutError. The third return value indicates whether a timeout occurred;
// if true, the caller should return immediately as the step is already recorded.
func runWithTimeout[T any](fn func() (T, error), timeout time.Duration, stepID, name string, startedAt time.Time, recordStep func(*StepResult)) (T, error, bool) {
	var zero T
	if timeout <= 0 {
		r, e := fn()
		return r, e, false
	}

	type resultPair struct {
		result T
		err    error
	}
	ch := make(chan resultPair, 1)
	go func() {
		r, e := fn()
		ch <- resultPair{r, e}
	}()

	select {
	case pair := <-ch:
		return pair.result, pair.err, false
	case <-time.After(timeout):
		endedAt := time.Now()
		duration := endedAt.Sub(startedAt)
		timeoutErr := NewStepTimeoutError(name, timeout)
		stepResult := &StepResult{
			ID:        stepID,
			Name:      name,
			Type:      "invoke",
			Status:    "failed",
			StartedAt: startedAt,
			EndedAt:   &endedAt,
			Duration:  duration,
			Error: &StepErrorInfo{
				Message:   timeoutErr.Error(),
				Retryable: true,
			},
		}
		recordStep(stepResult)
		return zero, timeoutErr, true
	}
}

// Run executes a step with memoization.
//
// If the step was already executed in a previous invocation, the cached
// result is returned. Otherwise, the function is executed and the result
// is recorded.
//
// Example:
//
//	result, err := ironflow.Run(ctx, "fetch-data", func() (Data, error) {
//	    return fetchData(ctx.Event.Data)
//	})
func Run[T any](ctx Context, name string, fn func() (T, error), opts ...StepOption) (T, error) {
	var zero T
	exec := ctx.exec

	if exec.testInterceptor != nil {
		return testRunStep[T](exec, name)
	}

	stepID := exec.generateStepID(name)

	// Check if step is already completed (memoized)
	if completed, ok := exec.completedSteps[stepID]; ok && completed.Status == "completed" {
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
	if timeout == 0 && exec.stepTimeout > 0 {
		timeout = exec.stepTimeout
	}

	// Execute the step
	startedAt := time.Now()

	// Report step started (streaming worker only)
	if exec.stepReporter != nil {
		exec.stepReporter.ReportStepStarted(stepID, name, "invoke")
	}

	recordStep := func(step *StepResult) {
		exec.executedStepsMu.Lock()
		exec.executedSteps = append(exec.executedSteps, step)
		exec.executedStepsMu.Unlock()
		// Report step failed for timeout (streaming worker only)
		if exec.stepReporter != nil && step.Status == "failed" && step.Error != nil {
			exec.stepReporter.ReportStepFailed(step.ID, step.Name, step.Type, step.Error.Message, int(step.Duration.Milliseconds()))
		}
	}

	result, err, timedOut := runWithTimeout(fn, timeout, stepID, name, startedAt, recordStep)
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
		exec.executedStepsMu.Lock()
		exec.executedSteps = append(exec.executedSteps, stepResult)
		exec.executedStepsMu.Unlock()

		// Report step failed (streaming worker only)
		if exec.stepReporter != nil {
			exec.stepReporter.ReportStepFailed(stepID, name, "invoke", err.Error(), int(duration.Milliseconds()))
		}

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
	exec.executedStepsMu.Lock()
	exec.executedSteps = append(exec.executedSteps, stepResult)
	exec.executedStepsMu.Unlock()

	// Report step completed (streaming worker only)
	if exec.stepReporter != nil {
		exec.stepReporter.ReportStepCompleted(stepID, name, "invoke", result, int(duration.Milliseconds()))
	}

	return result, nil
}

// Compensate registers a compensation handler for a previously completed step.
//
// WHY: Use Compensate to implement the Saga pattern. If a workflow fails later,
// Ironflow automatically executes all registered compensations in reverse order.
// This ensures that previous side effects (e.g., a payment) are rolled back
// (e.g., a refund) when a subsequent step (e.g., shipping) fails.
//
// Example:
//
//	result, err := ironflow.Run(ctx, "charge-payment", func() (Payment, error) {
//	    return chargeCard(order.CardID, order.Total)
//	})
//	if err != nil { return nil, err }
//	ironflow.Compensate(ctx, "charge-payment", func() error {
//	    return refundPayment(result.TransactionID)
//	})
func Compensate(ctx Context, stepName string, fn func() error) {
	if ctx.exec.testInterceptor != nil {
		ctx.exec.testInterceptor.CompensateStep(stepName, fn)
		return
	}
	ctx.exec.registerCompensation(stepName, fn)
}

// Publish sends a message to a developer pub/sub topic from within a workflow.
// This is a durable step -- memoized, retried, and observable like any other step.
// Unlike the standalone client.Publish(), this is tracked in step history.
//
// Example:
//
//	err := ironflow.Publish(ctx, "order.processed", map[string]any{
//	    "orderId": event.Data.OrderID,
//	})
func Publish(ctx Context, topic string, data any) error {
	_, err := Run[any](ctx, "publish:"+topic, func() (any, error) {
		serverURL := ctx.exec.serverURL
		if serverURL == "" {
			return nil, fmt.Errorf("server URL not configured for publish step")
		}

		reqBody := map[string]any{
			"topic": topic,
			"data":  data,
		}
		bodyJSON, err := json.Marshal(reqBody)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal publish request: %w", err)
		}

		publishCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(publishCtx, http.MethodPost, serverURL+"/ironflow.v1.PubSubService/Publish", bytes.NewReader(bodyJSON))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		if ctx.exec.apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+ctx.exec.apiKey)
		}
		// Attribute the publish to this run so the flow map can learn
		// function→topic edges (#1706). This request is hand-rolled rather than
		// routed through Client.request, so it does not get the header for free.
		if ctx.exec.runID != "" {
			req.Header.Set(HeaderRunID, ctx.exec.runID)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("publish failed: %s %s", resp.Status, string(body))
		}

		var result map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("failed to decode publish response: %w", err)
		}

		return result, nil
	})
	return err
}

// registerCompensation appends a compensation entry to the registry.
func (exec *executionContext) registerCompensation(stepName string, fn func() error) {
	exec.compensationsMu.Lock()
	defer exec.compensationsMu.Unlock()
	exec.compensations = append(exec.compensations, compensationEntry{
		stepName: stepName,
		fn:       fn,
	})
}

// hasCompensations returns true if any compensations are registered.
func (exec *executionContext) hasCompensations() bool {
	exec.compensationsMu.Lock()
	defer exec.compensationsMu.Unlock()
	return len(exec.compensations) > 0
}

// executeCompensations runs registered compensations in reverse order.
// Each compensation is executed as a durable step (memoized).
// Compensation failures are recorded but don't stop remaining compensations.
func (exec *executionContext) executeCompensations() {
	exec.compensationsMu.Lock()
	compensations := make([]compensationEntry, len(exec.compensations))
	copy(compensations, exec.compensations)
	exec.compensationsMu.Unlock()

	// Iterate in reverse order
	for _, entry := range slices.Backward(compensations) {
		compName := fmt.Sprintf("compensate:%s", entry.stepName)
		stepID := exec.generateStepID(compName)

		// Check memoization - skip if already completed
		if completed, ok := exec.completedSteps[stepID]; ok && completed.Status == "completed" {
			continue
		}

		startedAt := time.Now()
		err := entry.fn()
		endedAt := time.Now()
		duration := endedAt.Sub(startedAt)

		if err != nil {
			// Record failed compensation step
			stepResult := &StepResult{
				ID:              stepID,
				Name:            compName,
				Type:            "compensate",
				Status:          "failed",
				StartedAt:       startedAt,
				EndedAt:         &endedAt,
				Duration:        duration,
				CompensationFor: entry.stepName,
				Error: &StepErrorInfo{
					Message:   err.Error(),
					Retryable: false,
				},
			}
			exec.executedStepsMu.Lock()
			exec.executedSteps = append(exec.executedSteps, stepResult)
			exec.executedStepsMu.Unlock()
			continue
		}

		// Record successful compensation step
		stepResult := &StepResult{
			ID:              stepID,
			Name:            compName,
			Type:            "compensate",
			Status:          "completed",
			StartedAt:       startedAt,
			EndedAt:         &endedAt,
			Duration:        duration,
			CompensationFor: entry.stepName,
		}
		exec.executedStepsMu.Lock()
		exec.executedSteps = append(exec.executedSteps, stepResult)
		exec.executedStepsMu.Unlock()
	}
}

// Sleep pauses execution for a duration (durable).
//
// WHY: Use Sleep for long-running pauses (minutes, hours, or days). Unlike time.Sleep,
// this is durable—the worker can restart or the server can be upgraded, and the
// workflow will resume exactly where it left off once the duration has elapsed.
// It also frees up worker resources while waiting.
//
// Example:
//
//	err := ironflow.Sleep(ctx, "wait-24h", 24*time.Hour)
func Sleep(ctx Context, name string, duration time.Duration) error {
	exec := ctx.exec

	if exec.testInterceptor != nil {
		exec.testInterceptor.SleepStep(name)
		exec.testInterceptor.RecordStep(name, "sleep", nil, nil)
		return nil
	}

	stepID := exec.generateStepID(name)

	// Check if resuming from this sleep
	if exec.isResumingFrom(stepID, "sleep") {
		exec.resumeHandled = true
		return nil
	}

	// Check if step is already completed (memoized)
	if completed, ok := exec.completedSteps[stepID]; ok && completed.Status == "completed" {
		return nil
	}

	// Calculate wake time
	wakeAt := time.Now().Add(duration)

	// Throw yield signal
	panic(newSleepYield(stepID, wakeAt.Format(time.RFC3339)))
}

// SleepUntil pauses execution until a specific time (durable).
//
// Example:
//
//	err := ironflow.SleepUntil(ctx, "wait-until-midnight", midnight)
func SleepUntil(ctx Context, name string, until time.Time) error {
	exec := ctx.exec

	if exec.testInterceptor != nil {
		exec.testInterceptor.SleepStep(name)
		exec.testInterceptor.RecordStep(name, "sleep", nil, nil)
		return nil
	}

	stepID := exec.generateStepID(name)

	// Check if resuming from this sleep
	if exec.isResumingFrom(stepID, "sleep") {
		exec.resumeHandled = true
		return nil
	}

	// Check if step is already completed (memoized)
	if completed, ok := exec.completedSteps[stepID]; ok && completed.Status == "completed" {
		return nil
	}

	// Throw yield signal
	panic(newSleepYield(stepID, until.Format(time.RFC3339)))
}

// WaitForEvent waits for an external event to occur (durable).
//
// WHY: Use WaitForEvent to implement choreography-based orchestration. The workflow
// pauses durably until an external event (e.g., "payment.completed", "user.verified")
// arrives that matches the provided filter. This is the primary way to handle
// human-in-the-loop or asynchronous external callbacks.
//
// Example:
//
//	event, err := ironflow.WaitForEvent[ApprovalEvent](ctx, "wait-approval", ironflow.EventFilter{
//	    Event:   "order.approved",
//	    Match:   "data.orderId",
//	    Timeout: 7 * 24 * time.Hour,
//	})
func WaitForEvent[T any](ctx Context, name string, filter EventFilter) (Event, error) {
	exec := ctx.exec

	if exec.testInterceptor != nil {
		event, err := exec.testInterceptor.WaitForEventStep(name, filter)
		exec.testInterceptor.RecordStep(name, "waitForEvent", event, err)
		if err != nil {
			return Event{}, err
		}
		return event, nil
	}

	stepID := exec.generateStepID(name)

	// Check if resuming from this wait with the event data
	if exec.isResumingFrom(stepID, "wait_for_event") {
		exec.resumeHandled = true

		// The resume data contains the event that matched
		if exec.resumeContext.Data != nil {
			eventBytes, err := json.Marshal(exec.resumeContext.Data)
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
	if completed, ok := exec.completedSteps[stepID]; ok && completed.Status == "completed" {
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

// Invoke calls another Ironflow function and waits for its result.
// The call is durable: if the caller crashes and resumes, the cached result is
// returned without re-invoking the target function.
//
// The target function must have no event triggers (command-style only).
// Default timeout is 30s; override with WithInvokeTimeout option.
//
// Example:
//
//	charge, err := ironflow.Invoke[ChargeResult](ctx, "charge-card", map[string]any{
//	    "customerId": orderData.CustomerID,
//	    "amount":     orderData.Total,
//	})
func Invoke[T any](ctx Context, functionID string, input any, opts ...InvokeOptions) (T, error) {
	var zero T
	exec := ctx.exec

	if exec.testInterceptor != nil {
		return testInvokeStep[T](exec, functionID, input)
	}

	stepID := exec.generateStepID(functionID)

	// Check memoization
	if completed, ok := exec.completedSteps[stepID]; ok {
		switch completed.Status {
		case "completed":
			outputBytes, err := json.Marshal(completed.Output)
			if err != nil {
				return zero, &InvokeError{FunctionID: functionID, Cause: fmt.Sprintf("failed to marshal memoized output: %v", err)}
			}
			var result T
			if err := json.Unmarshal(outputBytes, &result); err != nil {
				return zero, &InvokeError{FunctionID: functionID, Cause: fmt.Sprintf("failed to unmarshal memoized output: %v", err)}
			}
			return result, nil
		case "failed":
			return zero, parseInvokeError(functionID, completed.Error)
		case "timed_out":
			return zero, &InvokeError{FunctionID: functionID, Cause: "invoke timed out"}
		}
	}

	// Not memoized — yield to engine to create child run
	timeoutMs := 30000
	if len(opts) > 0 && opts[0].Timeout > 0 {
		timeoutMs = int(opts[0].Timeout.Milliseconds())
	}

	panic(newInvokeYield(&YieldInfo{
		StepID:          stepID,
		Type:            ResumeTypeInvokeFunction,
		FunctionID:      functionID,
		Input:           input,
		InvokeTimeoutMs: timeoutMs,
	}))
}

// InvokeAsync calls another Ironflow function without waiting for its result.
// Returns immediately with the child run ID.
// The invoke step is memoized — retrying the caller returns the same run ID.
//
// Example:
//
//	result, err := ironflow.InvokeAsync(ctx, "send-notification", map[string]any{
//	    "userID": order.UserID,
//	})
func InvokeAsync(ctx Context, functionID string, input any) (InvokeAsyncResult, error) {
	exec := ctx.exec

	if exec.testInterceptor != nil {
		result, err := exec.testInterceptor.InvokeAsyncStep(functionID, input)
		exec.testInterceptor.RecordStep(functionID, "invoke", result, err)
		if err != nil {
			return InvokeAsyncResult{}, err
		}
		return result, nil
	}

	stepID := exec.generateStepID(functionID)

	// Check memoization
	if completed, ok := exec.completedSteps[stepID]; ok {
		switch completed.Status {
		case "completed":
			outputBytes, err := json.Marshal(completed.Output)
			if err != nil {
				return InvokeAsyncResult{}, fmt.Errorf("failed to marshal memoized output: %w", err)
			}
			var result InvokeAsyncResult
			if err := json.Unmarshal(outputBytes, &result); err != nil {
				return InvokeAsyncResult{}, fmt.Errorf("failed to unmarshal memoized output: %w", err)
			}
			return result, nil
		case "failed":
			return InvokeAsyncResult{}, parseInvokeError(functionID, completed.Error)
		}
	}

	panic(newInvokeYield(&YieldInfo{
		StepID:     stepID,
		Type:       ResumeTypeInvokeFunctionAsync,
		FunctionID: functionID,
		Input:      input,
	}))
}

// parseInvokeError converts a raw memoized error value into an *InvokeError.
func parseInvokeError(functionID string, errRaw any) *InvokeError {
	errJSON, _ := json.Marshal(errRaw)
	var e struct {
		Message    string `json:"message"`
		FunctionID string `json:"function_id"`
		ChildRunID string `json:"child_run_id"`
		Cause      string `json:"cause"`
	}
	if json.Unmarshal(errJSON, &e) == nil {
		fnID := e.FunctionID
		if fnID == "" {
			fnID = functionID
		}
		cause := e.Cause
		if cause == "" {
			cause = e.Message
		}
		return &InvokeError{FunctionID: fnID, ChildRunID: e.ChildRunID, Cause: cause}
	}
	return &InvokeError{FunctionID: functionID, Cause: string(errJSON)}
}

// ParseDuration parses a duration string like "1h", "30m", "7d".
func ParseDuration(s string) (time.Duration, error) {
	// Try standard Go duration first
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	// Parse extended format with days and weeks
	re := regexp.MustCompile(`(\d+)(ms|s|m|h|d|w)`)
	matches := re.FindAllStringSubmatch(s, -1)

	var total time.Duration
	for _, match := range matches {
		value, _ := strconv.ParseInt(match[1], 10, 64)
		switch match[2] {
		case "ms":
			total += time.Duration(value) * time.Millisecond
		case "s":
			total += time.Duration(value) * time.Second
		case "m":
			total += time.Duration(value) * time.Minute
		case "h":
			total += time.Duration(value) * time.Hour
		case "d":
			total += time.Duration(value) * 24 * time.Hour
		case "w":
			total += time.Duration(value) * 7 * 24 * time.Hour
		}
	}

	if total == 0 {
		return 0, fmt.Errorf("invalid duration: %s", s)
	}

	return total, nil
}
