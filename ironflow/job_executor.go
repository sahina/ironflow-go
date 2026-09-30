package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// jobReporter defines how job results are sent back to the server.
// The polling worker implements this via HTTP PUT; the streaming worker
// implements it via bidirectional stream messages.
type jobReporter interface {
	ReportCompleted(ctx context.Context, jobID string, output any, steps []*StepResult, stepOffset int) error
	ReportFailed(ctx context.Context, jobID string, err *PushError, steps []*StepResult, stepOffset int) error
	ReportYielded(ctx context.Context, jobID string, yield *YieldInfo, steps []*StepResult, stepOffset int) error
}

// pullJobReporter adds REST-only checkpoint and offset reporting. Streaming
// workers report each step over their stream and keep using jobReporter.
type pullJobReporter interface {
	ReportProgress(ctx context.Context, jobID string, steps []*StepResult, offset int) error
	ReportCompletedAt(ctx context.Context, jobID string, output any, steps []*StepResult, offset int) error
	ReportFailedAt(ctx context.Context, jobID string, err *PushError, steps []*StepResult, offset int) error
	ReportYieldedAt(ctx context.Context, jobID string, yield *YieldInfo, steps []*StepResult, offset int) error
}

const checkpointInterval = time.Second
const maxCheckpointSteps = 500

// stepLifecycleReporter reports step lifecycle events over the bidirectional stream.
// The streaming worker implements this; the polling worker has none.
type stepLifecycleReporter interface {
	ReportStepStarted(stepID, name, stepType string)
	ReportStepCompleted(stepID, name, stepType string, output any, durationMs int)
	ReportStepFailed(stepID, name, stepType string, errMsg string, durationMs int)
}

// stepReporterFor returns the job's step reporter, or nil when the reporter has
// none (the polling worker).
func stepReporterFor(r jobReporter, jobID string) stepLifecycleReporter {
	if p, ok := r.(interface {
		stepReporter(jobID string) stepLifecycleReporter
	}); ok {
		return p.stepReporter(jobID)
	}
	return nil
}

// jobExecutor encapsulates shared job execution logic used by both
// the polling worker and the streaming worker. It handles function lookup,
// execution context setup, memoization, upcasting, handler invocation,
// yield/error classification, and compensation.
type jobExecutor struct {
	functions map[string]Function
	upcasters *UpcasterRegistry
	serverURL string
	// apiKey is the worker's configured key. Empty falls back to the env var,
	// so an explicit WorkerConfig.APIKey reaches durable step callbacks
	// (step.Publish) the same way it reaches the polling transport.
	apiKey             string
	logger             Logger
	onError            func(err error, ctx ErrorContext)
	checkpointInterval time.Duration // zero uses checkpointInterval
}

// execute runs a job assignment through the function handler and reports
// the result via the provided reporter.
func (e *jobExecutor) execute(ctx context.Context, job *jobAssignment, reporter jobReporter) error {
	fn, ok := e.functions[job.FunctionID]
	if !ok {
		fnErr := fmt.Errorf("function not found: %s", job.FunctionID)
		e.callOnError(fnErr, job)
		return reportFailed(ctx, reporter, job.JobID, &PushError{
			Message:   fnErr.Error(),
			Code:      "FUNCTION_NOT_FOUND",
			Retryable: false,
		}, nil, job.StepSequenceBase)
	}

	e.logger.Info("Processing job", "jobId", job.JobID, "functionId", job.FunctionID)

	// Build execution context
	exec := &executionContext{
		runID:          job.RunID,
		functionID:     job.FunctionID,
		attempt:        job.Attempt,
		stepCounters:   make(map[string]int),
		completedSteps: make(map[string]*CompletedStep),
		executedSteps:  make([]*StepResult, 0),
		stepReporter:   stepReporterFor(reporter, job.JobID),
		logger:         e.logger,
	}

	for _, step := range job.CompletedSteps {
		status := step.Status
		if status == "" {
			status = "completed"
		}
		exec.completedSteps[step.StepID] = &CompletedStep{
			ID:     step.StepID,
			Name:   step.Name,
			Status: status,
			Output: step.Output,
			Error:  step.Error,
		}
	}
	checkpointer := newStepCheckpointer(ctx, job, exec, reporter, e.checkpointInterval)
	if checkpointer != nil {
		defer checkpointer.stop()
	}

	exec.stepTimeout = fn.Config.StepTimeout
	exec.serverURL = e.serverURL
	if apiKey := e.apiKey; apiKey != "" {
		exec.apiKey = apiKey
	} else if apiKey := GetAPIKey(); apiKey != "" {
		exec.apiKey = apiKey
	}

	// Parse event timestamp
	timestamp, _ := time.Parse(time.RFC3339, job.Event.Timestamp)

	// Apply upcasting if registry is provided
	eventData := job.Event.Data
	if e.upcasters != nil {
		upcasted, err := e.upcasters.UpcastToLatest(job.Event.Name, eventData, job.Event.Version)
		if err == nil {
			eventData = upcasted
		}
	}

	var eventMetadata map[string]any
	if len(job.Event.Metadata) > 0 {
		if err := json.Unmarshal(job.Event.Metadata, &eventMetadata); err != nil {
			e.logger.Warn("Failed to unmarshal event metadata; handler will receive nil metadata",
				"error", err,
				"jobId", job.JobID,
				"eventId", job.Event.ID,
			)
			eventMetadata = nil
		}
	}

	// Build context
	fnCtx := Context{
		Event: Event{
			ID:        job.Event.ID,
			Name:      job.Event.Name,
			Version:   job.Event.Version,
			RawData:   eventData,
			Timestamp: timestamp,
			Metadata:  eventMetadata,
		},
		Run: RunInfo{
			ID:          job.RunID,
			FunctionID:  job.FunctionID,
			Attempt:     job.Attempt,
			MaxAttempts: job.MaxAttempts,
			StartedAt:   time.Now(),
		},
		Secrets: NewSecretsReader(jobSecrets(job)),
		exec:    exec,
	}

	// Execute with panic recovery
	var result any
	var execErr error

	func() {
		defer func() {
			if r := recover(); r != nil {
				if signal, ok := r.(*yieldSignal); ok {
					execErr = signal
				} else {
					panic(r)
				}
			}
		}()

		result, execErr = fn.Handler(fnCtx)
	}()

	// Handle yield signal
	if signal, ok := isYieldSignal(execErr); ok {
		steps, offset := checkpointer.finish()
		return reportYielded(ctx, reporter, job.JobID, signal.info, steps, offset)
	}

	// Handle error
	if execErr != nil {
		retryable := IsRetryable(execErr)

		// Run compensations only if error is not retryable (terminal failure)
		if exec.hasCompensations() && !retryable {
			exec.executeCompensations()
		}

		e.callOnError(execErr, job)

		steps, offset := checkpointer.finish()
		if checkpointer == nil {
			// Streaming has no checkpointer, and its regular steps already went
			// out as StepCompleted/StepFailed; only compensations still need to
			// ride on JobFailed (#2413).
			exec.executedStepsMu.Lock()
			for _, s := range exec.executedSteps {
				if s.Type == "compensate" {
					steps = append(steps, s)
				}
			}
			exec.executedStepsMu.Unlock()
		}
		return reportFailed(ctx, reporter, job.JobID, &PushError{
			Message:   execErr.Error(),
			Retryable: retryable,
		}, steps, offset)
	}

	// Success - include executed steps
	steps, offset := checkpointer.finish()
	return reportCompleted(ctx, reporter, job.JobID, result, steps, offset)
}

func reportCompleted(ctx context.Context, reporter jobReporter, jobID string, output any, steps []*StepResult, offset int) error {
	if r, ok := reporter.(pullJobReporter); ok {
		return r.ReportCompletedAt(ctx, jobID, output, steps, offset)
	}
	return reporter.ReportCompleted(ctx, jobID, output, steps, offset)
}

func reportFailed(ctx context.Context, reporter jobReporter, jobID string, pushErr *PushError, steps []*StepResult, offset int) error {
	if r, ok := reporter.(pullJobReporter); ok {
		return r.ReportFailedAt(ctx, jobID, pushErr, steps, offset)
	}
	return reporter.ReportFailed(ctx, jobID, pushErr, steps, offset)
}

func reportYielded(ctx context.Context, reporter jobReporter, jobID string, yield *YieldInfo, steps []*StepResult, offset int) error {
	if r, ok := reporter.(pullJobReporter); ok {
		return r.ReportYieldedAt(ctx, jobID, yield, steps, offset)
	}
	return reporter.ReportYielded(ctx, jobID, yield, steps, offset)
}

// stepCheckpointer periodically persists completed steps and returns the
// uncheckpointed tail for the terminal update.
type stepCheckpointer struct {
	ctx      context.Context
	jobID    string
	base     int
	exec     *executionContext
	reporter pullJobReporter
	mu       sync.Mutex
	cursor   int
	stopCh   chan struct{}
	doneCh   chan struct{}
	interval time.Duration
}

func newStepCheckpointer(ctx context.Context, job *jobAssignment, exec *executionContext, reporter jobReporter, interval time.Duration) *stepCheckpointer {
	r, ok := reporter.(pullJobReporter)
	if !ok {
		return nil
	}
	if interval <= 0 {
		interval = checkpointInterval
	}
	c := &stepCheckpointer{
		ctx: ctx, jobID: job.JobID, base: job.StepSequenceBase,
		exec: exec, reporter: r, stopCh: make(chan struct{}), doneCh: make(chan struct{}), interval: interval,
	}
	go c.run()
	return c
}

func (c *stepCheckpointer) run() {
	defer close(c.doneCh)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = c.flush()
		case <-c.stopCh:
			return
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *stepCheckpointer) flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.exec.executedStepsMu.Lock()
	steps := append([]*StepResult(nil), c.exec.executedSteps[c.cursor:]...)
	c.exec.executedStepsMu.Unlock()
	if len(steps) > maxCheckpointSteps {
		steps = steps[:maxCheckpointSteps]
	}
	if len(steps) == 0 {
		return nil
	}
	if err := c.reporter.ReportProgress(c.ctx, c.jobID, steps, c.base+c.cursor); err != nil {
		return err
	}
	c.cursor += len(steps)
	return nil
}

func (c *stepCheckpointer) finish() ([]*StepResult, int) {
	if c == nil {
		return nil, 0
	}
	close(c.stopCh)
	<-c.doneCh
	return c.tail()
}

func (c *stepCheckpointer) stop() {
	if c == nil {
		return
	}
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
	<-c.doneCh
}

func (c *stepCheckpointer) tail() ([]*StepResult, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.exec.executedStepsMu.Lock()
	defer c.exec.executedStepsMu.Unlock()
	return append([]*StepResult(nil), c.exec.executedSteps[c.cursor:]...), c.base + c.cursor
}

// callOnError invokes the OnError callback with panic recovery.
func (e *jobExecutor) callOnError(err error, job *jobAssignment) {
	if e.onError == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error("OnError callback panicked", "panic", r)
		}
	}()
	e.onError(err, ErrorContext{
		FunctionID: job.FunctionID,
		JobID:      job.JobID,
		RunID:      job.RunID,
		Attempt:    job.Attempt,
		EventName:  job.Event.Name,
	})
}

// jobAssignment is a job assignment from the server.
type jobAssignment struct {
	JobID      string `json:"job_id"`
	RunID      string `json:"run_id"`
	FunctionID string `json:"function_id"`
	Attempt    int    `json:"attempt"`
	// MaxAttempts is this run's retry budget; see [RunInfo.MaxAttempts].
	// Zero from an engine that predates the field.
	MaxAttempts      int             `json:"max_attempts,omitempty"`
	Event            jobEvent        `json:"event"`
	CompletedSteps   []completedStep `json:"completed_steps"`
	StepSequenceBase int             `json:"step_sequence_base,omitempty"`
	ActorID          string          `json:"actor_id,omitempty"`
	Context          *jobContext     `json:"context,omitempty"`

	// Execution fence (#1206, ADR 0037). Carried from the gRPC JobAssignment
	// (chunk 3e, set programmatically) OR decoded from the REST poll response
	// (chunk 2 / T9, set from JSON) so the worker can ack and echo it on every
	// mutating message back to the engine. Zero / empty for legacy /
	// non-capacity assignments. The JSON tags are decode-only — nothing marshals
	// a jobAssignment outbound — and omitempty leaves the gRPC path, which never
	// JSON-encodes this struct, unaffected.
	ExecutionSeq int64  `json:"execution_seq,omitempty"`
	LeaseToken   string `json:"lease_token,omitempty"`
}

type jobEvent struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Version   int             `json:"version"`
	Data      json.RawMessage `json:"data"`
	Timestamp string          `json:"timestamp"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

type completedStep struct {
	StepID string `json:"step_id"`
	Name   string `json:"name"`
	Output any    `json:"output"`
	// Status and Error are set only on a failed invoke row (#2385).
	Status string `json:"status,omitempty"`
	Error  any    `json:"error,omitempty"`
}

// jobContext carries per-job context (secrets, trace info, etc.).
type jobContext struct {
	Secrets map[string]string `json:"secrets,omitempty"`
}

// jobSecrets extracts the secrets map from a jobAssignment's context.
func jobSecrets(job *jobAssignment) map[string]string {
	if job.Context != nil {
		return job.Context.Secrets
	}
	return nil
}
