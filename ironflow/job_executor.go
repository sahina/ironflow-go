package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// jobReporter defines how job results are sent back to the server.
// The polling worker implements this via HTTP PUT; the streaming worker
// implements it via bidirectional stream messages.
type jobReporter interface {
	ReportCompleted(ctx context.Context, jobID string, output any, steps []*StepResult) error
	ReportFailed(ctx context.Context, jobID string, err *PushError, steps []*StepResult) error
	ReportYielded(ctx context.Context, jobID string, yield *YieldInfo) error
}

// stepLifecycleReporter reports step lifecycle events over the bidirectional stream.
// The streaming worker implements this; the polling worker leaves the field nil.
type stepLifecycleReporter interface {
	ReportStepStarted(stepID, name, stepType string)
	ReportStepCompleted(stepID, name, stepType string, output any, durationMs int)
	ReportStepFailed(stepID, name, stepType string, errMsg string, durationMs int)
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
	apiKey       string
	logger       Logger
	onError      func(err error, ctx ErrorContext)
	stepReporter stepLifecycleReporter // nil for polling worker; set for streaming worker
}

// execute runs a job assignment through the function handler and reports
// the result via the provided reporter.
func (e *jobExecutor) execute(ctx context.Context, job *jobAssignment, reporter jobReporter) error {
	fn, ok := e.functions[job.FunctionID]
	if !ok {
		fnErr := fmt.Errorf("function not found: %s", job.FunctionID)
		e.callOnError(fnErr, job)
		return reporter.ReportFailed(ctx, job.JobID, &PushError{
			Message:   fnErr.Error(),
			Code:      "FUNCTION_NOT_FOUND",
			Retryable: false,
		}, nil)
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
		stepReporter:   e.stepReporter,
	}

	for _, step := range job.CompletedSteps {
		exec.completedSteps[step.StepID] = &CompletedStep{
			ID:     step.StepID,
			Name:   step.Name,
			Status: "completed",
			Output: step.Output,
		}
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
			ID:         job.RunID,
			FunctionID: job.FunctionID,
			Attempt:    job.Attempt,
			StartedAt:  time.Now(),
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
		return reporter.ReportYielded(ctx, job.JobID, signal.info)
	}

	// Handle error
	if execErr != nil {
		retryable := IsRetryable(execErr)

		// Run compensations only if error is not retryable (terminal failure)
		if exec.hasCompensations() && !retryable {
			exec.executeCompensations()
		}

		e.callOnError(execErr, job)

		return reporter.ReportFailed(ctx, job.JobID, &PushError{
			Message:   execErr.Error(),
			Retryable: retryable,
		}, exec.executedSteps)
	}

	// Success - include executed steps
	return reporter.ReportCompleted(ctx, job.JobID, result, exec.executedSteps)
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
	JobID          string          `json:"job_id"`
	RunID          string          `json:"run_id"`
	FunctionID     string          `json:"function_id"`
	Attempt        int             `json:"attempt"`
	Event          jobEvent        `json:"event"`
	CompletedSteps []completedStep `json:"completed_steps"`
	ActorID        string          `json:"actor_id,omitempty"`
	Context        *jobContext     `json:"context,omitempty"`

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
