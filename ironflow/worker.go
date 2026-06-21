package ironflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerConfig configures the worker.
type WorkerConfig struct {
	// ServerURL is the Ironflow server URL.
	ServerURL string

	// Functions are the functions this worker handles.
	Functions []Function

	// Projections are the projections this worker runs.
	Projections []Projection

	// MaxConcurrentJobs is the maximum number of concurrent jobs (default: 10).
	MaxConcurrentJobs int

	// Labels are worker labels for routing.
	Labels map[string]string

	// HeartbeatInterval is the heartbeat interval (default: 30s).
	HeartbeatInterval time.Duration

	// ReconnectDelay is the delay before reconnecting (default: 5s).
	ReconnectDelay time.Duration

	// Logger is the logger to use. If nil, uses the default console logger.
	// Set to NewNoopLogger() to disable logging.
	Logger Logger

	// Upcasters is an optional registry for automatic event schema upcasting.
	// When set, event data is upcasted to the latest version before being passed to handlers.
	Upcasters *UpcasterRegistry

	// APIKey is the API key for authentication. If empty, falls back to IRONFLOW_API_KEY env var.
	APIKey string

	// OnError is called when a job fails during async execution.
	// It is called after compensations run but before reporting the failure to the server.
	// It does not affect retry behavior. The callback should not block —
	// run expensive operations in a goroutine.
	OnError func(err error, ctx ErrorContext)
}

// Worker is a gRPC streaming worker for long-running tasks.
type Worker struct {
	config     WorkerConfig
	functions  map[string]Function
	workerID   string
	httpClient *http.Client
	logger     Logger
	executor   *jobExecutor

	state             atomic.Int32 // workerState
	activeJobs        sync.Map     // map[string]*activeJob
	jobCount          atomic.Int32
	stopCh            chan struct{}
	heartbeatWg       sync.WaitGroup
	projMu            sync.Mutex
	projectionRunners []*ProjectionRunner
}

type workerState int32

const (
	stateIdle workerState = iota
	stateConnecting
	stateConnected
	stateDraining
	stateStopped
)

type activeJob struct {
	jobID     string
	runID     string
	startedAt time.Time
	cancel    context.CancelFunc
}

// NewWorker creates a new worker.
//
// Example:
//
//	worker := ironflow.NewWorker(ironflow.WorkerConfig{
//	    ServerURL:         "http://localhost:9123",  // or use GetServerURL()
//	    Functions:         []ironflow.Function{GenerateVideo},
//	    MaxConcurrentJobs: 4,
//	    Labels:            map[string]string{"gpu": "nvidia-a100"},
//	})
//
//	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
//	defer cancel()
//
//	if err := worker.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
func NewWorker(config WorkerConfig) *Worker {
	PrintBanner()

	// Defaults
	if config.ServerURL == "" {
		config.ServerURL = GetServerURL()
	}
	if config.MaxConcurrentJobs == 0 {
		config.MaxConcurrentJobs = DefaultWorkerMaxConcurrentJobs
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = DefaultWorkerHeartbeatInterval
	}
	if config.ReconnectDelay == 0 {
		config.ReconnectDelay = DefaultWorkerReconnectDelay
	}

	// Initialize logger
	logger := config.Logger
	if logger == nil {
		logger = NewLogger(LoggerConfig{Prefix: "[ironflow-worker]"})
	}

	// Build function map
	functions := make(map[string]Function)
	for _, fn := range config.Functions {
		if _, exists := functions[fn.Config.ID]; exists {
			logger.Warn("duplicate function ID %q — the later definition will overwrite the earlier one", fn.Config.ID)
		}
		functions[fn.Config.ID] = fn
	}

	return &Worker{
		config:     config,
		functions:  functions,
		workerID:   generateWorkerID(),
		httpClient: &http.Client{Timeout: DefaultClientTimeout},
		logger:     logger,
		executor: &jobExecutor{
			functions: functions,
			upcasters: config.Upcasters,
			serverURL: config.ServerURL,
			logger:    logger,
			onError:   config.OnError,
		},
		stopCh: make(chan struct{}),
	}
}

// Run starts the worker and blocks until stopped.
func (w *Worker) Run(ctx context.Context) error {
	if !w.state.CompareAndSwap(int32(stateIdle), int32(stateConnecting)) {
		return NewError("worker is already running", "WORKER_ALREADY_RUNNING", false)
	}

	w.logger.Info("Starting worker", "workerId", w.workerID, "functions", len(w.functions))

	// Connect loop with auto-reconnect
	for {
		select {
		case <-ctx.Done():
			w.state.Store(int32(stateStopped))
			return ctx.Err()
		case <-w.stopCh:
			return nil
		default:
		}

		if err := w.connect(ctx); err != nil {
			if w.state.Load() == int32(stateStopped) {
				return nil
			}

			w.logger.Error("Connection error", "error", err)
			w.logger.Info("Reconnecting", "delay", w.config.ReconnectDelay)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(w.config.ReconnectDelay):
				continue
			}
		}
	}
}

// Drain gracefully drains and stops the worker.
func (w *Worker) Drain() {
	if w.state.Load() == int32(stateStopped) {
		return
	}

	w.logger.Info("Draining worker...")
	w.state.Store(int32(stateDraining))

	// Wait for active jobs to complete
	for w.jobCount.Load() > 0 {
		w.logger.Info("Waiting for jobs to complete", "jobs", w.jobCount.Load())
		time.Sleep(time.Second)
	}

	w.Stop()
}

// Stop immediately stops the worker.
func (w *Worker) Stop() {
	w.state.Store(int32(stateStopped))
	select {
	case <-w.stopCh:
		// Already stopped
	default:
		close(w.stopCh)
	}

	// Stop projection runners
	w.stopProjectionRunners()

	// Cancel all active jobs
	w.activeJobs.Range(func(key, value any) bool {
		if job, ok := value.(*activeJob); ok {
			job.cancel()
		}
		return true
	})
}

// connect establishes a connection to the server.
func (w *Worker) connect(ctx context.Context) error {
	w.state.Store(int32(stateConnecting))

	// Register functions so the event router can find them
	if err := registerFunctions(ctx, w.config.ServerURL, w.getHeaders(), w.functions, w.httpClient, w.logger); err != nil {
		return err
	}

	// Register worker
	if err := w.registerWorker(ctx); err != nil {
		return err
	}

	w.state.Store(int32(stateConnected))
	w.logger.Info("Connected to server")

	// Stop any existing projection runners before starting new ones (prevents leak on reconnect)
	w.stopProjectionRunners()

	// Start projection runners
	w.startProjectionRunners()

	// Start heartbeat
	w.startHeartbeat(ctx)

	// Poll for jobs
	return w.pollForJobs(ctx)
}

// startProjectionRunners starts a ProjectionRunner for each configured projection.
func (w *Worker) startProjectionRunners() {
	if len(w.config.Projections) == 0 {
		return
	}

	w.projMu.Lock()
	defer w.projMu.Unlock()

	for _, proj := range w.config.Projections {
		runner := NewProjectionRunner(proj, w.config.ServerURL, w.getHeaders(), w.logger)
		w.projectionRunners = append(w.projectionRunners, runner)
		if err := runner.Start(); err != nil {
			w.logger.Error("Failed to start projection runner", "projection", proj.Config.Name, "error", err)
		}
	}

	w.logger.Info("Started projection runners", "count", len(w.config.Projections))
}

// stopProjectionRunners stops all running projection runners.
func (w *Worker) stopProjectionRunners() {
	w.projMu.Lock()
	runners := w.projectionRunners
	w.projectionRunners = nil
	w.projMu.Unlock()

	for _, runner := range runners {
		runner.Stop()
	}
}

// getHeaders returns the headers for HTTP requests (e.g., API key).
func (w *Worker) getHeaders() map[string]string {
	return buildAuthHeaders(w.config.APIKey)
}

// registerWorker registers the worker with the server.
func (w *Worker) registerWorker(ctx context.Context) error {
	functionIDs := make([]string, 0, len(w.functions))
	for id := range w.functions {
		functionIDs = append(functionIDs, id)
	}

	body := map[string]any{
		"worker_id":           w.workerID,
		"hostname":            getHostname(),
		"function_ids":        functionIDs,
		"max_concurrent_jobs": w.config.MaxConcurrentJobs,
		"labels":              w.config.Labels,
		"version": map[string]string{
			"sdk":     SDKVersion,
			"runtime": "go",
		},
	}

	return w.httpPut(ctx, fmt.Sprintf("/api/v1/workers/%s/register", w.workerID), body, nil)
}

// startHeartbeat starts the heartbeat goroutine.
func (w *Worker) startHeartbeat(ctx context.Context) {
	w.heartbeatWg.Add(1)
	go func() {
		defer w.heartbeatWg.Done()

		ticker := time.NewTicker(w.config.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-w.stopCh:
				return
			case <-ticker.C:
				if w.state.Load() != int32(stateConnected) {
					return
				}

				w.sendHeartbeat(ctx)
			}
		}
	}()
}

// sendHeartbeat sends a heartbeat to the server.
func (w *Worker) sendHeartbeat(ctx context.Context) {
	jobs := make([]map[string]any, 0)
	w.activeJobs.Range(func(key, value any) bool {
		if job, ok := value.(*activeJob); ok {
			jobs = append(jobs, map[string]any{
				"job_id":     job.jobID,
				"started_at": job.startedAt.Format(time.RFC3339),
			})
		}
		return true
	})

	body := map[string]any{
		"worker_id":   w.workerID,
		"active_jobs": len(jobs),
		"jobs":        jobs,
	}

	if err := w.httpPut(ctx, fmt.Sprintf("/api/v1/workers/%s/heartbeat", w.workerID), body, nil); err != nil {
		w.logger.Warn("Heartbeat failed", "error", err)
	}
}

// pollForJobs polls for jobs from the server.
func (w *Worker) pollForJobs(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.stopCh:
			return nil
		default:
		}

		if w.state.Load() != int32(stateConnected) {
			return nil
		}

		// Check if draining
		if w.state.Load() == int32(stateDraining) {
			return nil
		}

		// Advertise free slots as the available count (#1206, T9): a capacity
		// server returns up to this many fenced assignments; a legacy server
		// ignores the param and returns at most one. <= 0 means full — back off.
		free := w.config.MaxConcurrentJobs - int(w.jobCount.Load())
		if free <= 0 {
			time.Sleep(time.Second)
			continue
		}

		jobs, err := w.requestJobs(ctx, free)
		if err != nil {
			w.logger.Warn("Job request error", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if len(jobs) == 0 {
			// No jobs available
			time.Sleep(time.Second)
			continue
		}

		for _, job := range jobs {
			w.processJob(ctx, job)
		}
	}
}

// jobPollResponse decodes the poll response in either shape: the capacity
// batched response ({"jobs":[...]}, #1206 T9) or the legacy single-assignment
// object. The embedded jobAssignment captures the legacy top-level fields; Jobs
// captures the batch. A new SDK therefore works against both a capacity-enabled
// and a capacity-disabled server (default-off), echoing the fence only when the
// assignment carries one — the same tolerance the gRPC SDK has.
type jobPollResponse struct {
	jobAssignment
	Jobs []*jobAssignment `json:"jobs"`
}

// requestJobs requests up to `available` jobs from the server.
func (w *Worker) requestJobs(ctx context.Context, available int) ([]*jobAssignment, error) {
	url := w.config.ServerURL + fmt.Sprintf("/api/v1/workers/%s/jobs?available=%d", w.workerID, available)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range w.getHeaders() {
		req.Header.Set(k, v)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var pr jobPollResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, err
	}
	if len(pr.Jobs) > 0 {
		return pr.Jobs, nil // capacity batched response
	}
	if pr.JobID != "" {
		single := pr.jobAssignment
		return []*jobAssignment{&single}, nil // legacy single-object response
	}
	return nil, nil
}

// processJob processes a job asynchronously.
func (w *Worker) processJob(ctx context.Context, job *jobAssignment) {
	jobCtx, cancel := context.WithCancel(ctx)

	aj := &activeJob{
		jobID:     job.JobID,
		runID:     job.RunID,
		startedAt: time.Now(),
		cancel:    cancel,
	}

	w.activeJobs.Store(job.JobID, aj)
	w.jobCount.Add(1)

	// Per-job reporter carrying the execution fence (#1206, T9), so every
	// terminal/yield it emits echoes the fence. Captured here from the assignment
	// rather than looked up from activeJobs at report time — immune to map state,
	// mirroring the gRPC streamJobReporter and avoiding the chunk-3e cancel-race
	// that produced tokenless (rejected) updates. Empty fence for legacy jobs.
	reporter := &httpJobReporter{worker: w, executionSeq: job.ExecutionSeq, leaseToken: job.LeaseToken}

	go func() {
		defer func() {
			w.activeJobs.Delete(job.JobID)
			w.jobCount.Add(-1)
			cancel()
		}()

		// A fenced (capacity) assignment must be acknowledged before user code
		// runs. A failed or stale (409) ack means the segment was recovered or
		// superseded — drop it without executing; the lease expires server-side
		// and the scanner recovers the run.
		if job.LeaseToken != "" {
			if err := w.ackJob(jobCtx, job); err != nil {
				w.logger.Warn("Assignment ack failed, dropping job", "jobId", job.JobID, "error", err)
				return
			}
		}

		if err := w.executor.execute(jobCtx, job, reporter); err != nil {
			w.logger.Error("Job failed", "jobId", job.JobID, "error", err)
		}
	}()
}

// ackJob acknowledges a fenced capacity assignment before executing user code
// (#1206, T9). The body echoes the execution fence; a stale fence returns 409,
// surfaced as an error by httpPut, so the caller drops the job.
func (w *Worker) ackJob(ctx context.Context, job *jobAssignment) error {
	return w.httpPut(ctx, fmt.Sprintf("/api/v1/workers/%s/jobs/%s/ack", w.workerID, job.JobID), map[string]any{
		"run_id":        job.RunID,
		"execution_seq": job.ExecutionSeq,
		"lease_token":   job.LeaseToken,
	}, nil)
}

// httpJobReporter implements jobReporter by sending results via HTTP PUT. It
// carries the assignment's execution fence (#1206, T9) and stamps it onto every
// outbound update so the engine can validate the mutation; the fence is empty for
// legacy (non-capacity) assignments, in which case the body is unchanged.
type httpJobReporter struct {
	worker       *Worker
	executionSeq int64
	leaseToken   string
}

// stampFence merges the execution-fence fields into an outbound update body when
// this is a capacity assignment. A no-op for legacy assignments.
func (r *httpJobReporter) stampFence(body map[string]any) {
	if r.leaseToken == "" {
		return
	}
	body["execution_seq"] = r.executionSeq
	body["lease_token"] = r.leaseToken
}

func (r *httpJobReporter) ReportCompleted(ctx context.Context, jobID string, output any, steps []*StepResult) error {
	body := map[string]any{
		"status": "completed",
		"output": output,
		"steps":  steps,
	}
	r.stampFence(body)
	return r.worker.httpPut(ctx, fmt.Sprintf("/api/v1/workers/%s/jobs/%s", r.worker.workerID, jobID), body, nil)
}

func (r *httpJobReporter) ReportFailed(ctx context.Context, jobID string, err *PushError, steps []*StepResult) error {
	body := map[string]any{
		"status": "failed",
		"error":  err,
	}
	if len(steps) > 0 {
		body["steps"] = steps
	}
	r.stampFence(body)
	return r.worker.httpPut(ctx, fmt.Sprintf("/api/v1/workers/%s/jobs/%s", r.worker.workerID, jobID), body, nil)
}

func (r *httpJobReporter) ReportYielded(ctx context.Context, jobID string, yield *YieldInfo) error {
	body := map[string]any{
		"status": "yielded",
		"yield":  yield,
	}
	r.stampFence(body)
	return r.worker.httpPut(ctx, fmt.Sprintf("/api/v1/workers/%s/jobs/%s", r.worker.workerID, jobID), body, nil)
}

// httpPut makes an HTTP PUT request.
//
//nolint:unparam // result kept for future flexibility
func (w *Worker) httpPut(ctx context.Context, path string, body any, result any) error {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.config.ServerURL+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range w.getHeaders() {
		req.Header.Set(k, v)
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed: %s", string(respBody))
	}

	if result != nil {
		return json.NewDecoder(resp.Body).Decode(result)
	}

	return nil
}
