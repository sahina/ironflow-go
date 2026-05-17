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
	// Create an HTTP-based job reporter for the polling worker
	reporter := &httpJobReporter{worker: w}

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

		// Check capacity
		if int(w.jobCount.Load()) >= w.config.MaxConcurrentJobs {
			time.Sleep(time.Second)
			continue
		}

		// Check if draining
		if w.state.Load() == int32(stateDraining) {
			return nil
		}

		// Request a job
		job, err := w.requestJob(ctx)
		if err != nil {
			w.logger.Warn("Job request error", "error", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if job == nil {
			// No jobs available
			time.Sleep(time.Second)
			continue
		}

		// Process the job
		w.processJob(ctx, job, reporter)
	}
}

// requestJob requests a job from the server.
func (w *Worker) requestJob(ctx context.Context) (*jobAssignment, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.config.ServerURL+fmt.Sprintf("/api/v1/workers/%s/jobs", w.workerID), nil)
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

	var job jobAssignment
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, err
	}

	return &job, nil
}

// processJob processes a job asynchronously.
func (w *Worker) processJob(ctx context.Context, job *jobAssignment, reporter jobReporter) {
	jobCtx, cancel := context.WithCancel(ctx)

	aj := &activeJob{
		jobID:     job.JobID,
		runID:     job.RunID,
		startedAt: time.Now(),
		cancel:    cancel,
	}

	w.activeJobs.Store(job.JobID, aj)
	w.jobCount.Add(1)

	go func() {
		defer func() {
			w.activeJobs.Delete(job.JobID)
			w.jobCount.Add(-1)
			cancel()
		}()

		if err := w.executor.execute(jobCtx, job, reporter); err != nil {
			w.logger.Error("Job failed", "jobId", job.JobID, "error", err)
		}
	}()
}

// httpJobReporter implements jobReporter by sending results via HTTP PUT.
type httpJobReporter struct {
	worker *Worker
}

func (r *httpJobReporter) ReportCompleted(ctx context.Context, jobID string, output any, steps []*StepResult) error {
	return r.worker.httpPut(ctx, fmt.Sprintf("/api/v1/workers/%s/jobs/%s", r.worker.workerID, jobID), map[string]any{
		"status": "completed",
		"output": output,
		"steps":  steps,
	}, nil)
}

func (r *httpJobReporter) ReportFailed(ctx context.Context, jobID string, err *PushError, steps []*StepResult) error {
	body := map[string]any{
		"status": "failed",
		"error":  err,
	}
	if len(steps) > 0 {
		body["steps"] = steps
	}
	return r.worker.httpPut(ctx, fmt.Sprintf("/api/v1/workers/%s/jobs/%s", r.worker.workerID, jobID), body, nil)
}

func (r *httpJobReporter) ReportYielded(ctx context.Context, jobID string, yield *YieldInfo) error {
	return r.worker.httpPut(ctx, fmt.Sprintf("/api/v1/workers/%s/jobs/%s", r.worker.workerID, jobID), map[string]any{
		"status": "yielded",
		"yield":  yield,
	}, nil)
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
