package ironflow

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
	"github.com/sahina/ironflow-go/ironflow/discovery"
)

// StreamingWorker is a ConnectRPC bidirectional-stream worker that communicates
// with the Ironflow engine over a single persistent connection. It provides the
// same capabilities as the polling Worker but with lower latency and real-time
// step lifecycle reporting.
type StreamingWorker struct {
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
	stopOnce          sync.Once
	flushCh           chan struct{} // closed by Drain: send the queue, then half-close
	flushOnce         sync.Once
	projMu            sync.Mutex
	projectionRunners []*ProjectionRunner
	// drainTimeout is resolved from config.DrainTimeout and workerDrainTimeout
	// once, at construction: a worker can drain after its Run context ends,
	// while a test changes the variable.
	drainTimeout time.Duration

	// outCh buffers outgoing WorkerMessages for the sendLoop.
	outCh chan *ironflowv1.WorkerMessage
}

// NewStreamingWorker creates a new streaming worker.
//
// Example:
//
//	worker := ironflow.NewStreamingWorker(ironflow.WorkerConfig{
//	    ServerURL:         "http://localhost:9123",
//	    Functions:         []ironflow.Function{GenerateVideo},
//	    MaxConcurrentJobs: 4,
//	    Labels:            map[string]string{"gpu": "nvidia-a100"},
//	})
//
//	// A cancelled context drains the worker: up to 30 seconds for active jobs.
//	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
//	defer cancel()
//
//	if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
//	    log.Fatal(err)
//	}
func NewStreamingWorker(config WorkerConfig) *StreamingWorker {
	discovery.Hydrate()
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
		logger = NewLogger(LoggerConfig{Prefix: "[ironflow-streaming]"})
	}

	// Build function map
	functions := make(map[string]Function)
	for _, fn := range config.Functions {
		if _, exists := functions[fn.Config.ID]; exists {
			logger.Warn("duplicate function ID %q — the later definition will overwrite the earlier one", fn.Config.ID)
		}
		functions[fn.Config.ID] = fn
	}

	w := &StreamingWorker{
		config:       config,
		functions:    functions,
		workerID:     generateWorkerID(),
		logger:       logger,
		outCh:        make(chan *ironflowv1.WorkerMessage, 256),
		stopCh:       make(chan struct{}),
		flushCh:      make(chan struct{}),
		drainTimeout: drainTimeout(config.DrainTimeout),
	}

	w.httpClient = newH2CClient(config.ServerURL)

	w.executor = &jobExecutor{
		functions:   functions,
		upcasters:   config.Upcasters,
		serverURL:   config.ServerURL,
		apiKey:      config.APIKey,
		environment: config.Environment,
		logger:      logger,
		onError:     config.OnError,
	}

	return w
}

// Run starts the streaming worker and blocks until stopped.
// It auto-reconnects on disconnect. A cancelled ctx starts a drain, and Run
// returns ctx.Err() when the drain is done.
func (w *StreamingWorker) Run(ctx context.Context) error {
	if !w.state.CompareAndSwap(int32(stateIdle), int32(stateConnecting)) {
		return NewError("worker is already running", "WORKER_ALREADY_RUNNING", false)
	}

	// The stream and the jobs outlive ctx: the drain needs the stream to report
	// the active jobs. Stop cancels them, also a registration that hangs on an
	// engine that does not answer (the h2c client has no timeout).
	life, cancelLife := context.WithCancel(context.WithoutCancel(ctx))
	go func() {
		select {
		case <-ctx.Done():
			w.Drain()
		case <-w.stopCh:
		}
		cancelLife()
	}()

	w.logger.Info("Starting streaming worker", "workerId", w.workerID, "functions", len(w.functions), "environment", resolveEnvironment(w.config.Environment))

	for {
		select {
		case <-w.stopCh:
			return ctx.Err()
		default:
		}
		// Without a stream no result can reach the engine, so a draining worker
		// stops here and does not reconnect.
		if w.state.Load() == int32(stateDraining) {
			w.Stop()
			return ctx.Err()
		}

		if err := w.connectStream(life); err != nil {
			if w.state.Load() == int32(stateStopped) {
				return ctx.Err()
			}
			if w.state.Load() == int32(stateDraining) {
				continue
			}

			// Auth failures do not fix themselves on the reconnect cadence
			// (#1673). Covers both the HTTP register call and the stream itself,
			// which surfaces auth as a Connect code rather than an ironflow error.
			code := connect.CodeOf(err)
			if errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrForbidden) ||
				code == connect.CodeUnauthenticated || code == connect.CodePermissionDenied {
				// A raw Connect permission-denied carries neither the hint
				// registerFunctions' 403 already has nor a 403's environment
				// hint, so add it here (#2472).
				hint := AuthHelp
				if code == connect.CodePermissionDenied && !errors.Is(err, ErrForbidden) {
					hint = environmentHintFor(resolveEnvironment(w.config.Environment)) + AuthHelp
				}
				w.logger.Error(fmt.Sprintf("stream authentication failed: %v. %s", err, hint))
				w.Stop()
				return err
			}

			if !IsRetryable(err) {
				w.logger.Error(err.Error())
				w.Stop()
				return err
			}

			w.logger.Error("Stream connection error", "error", err)
			w.logger.Info("Reconnecting", "delay", w.config.ReconnectDelay)

			select {
			case <-w.stopCh:
				continue
			case <-w.flushCh:
				// Drain waits for this loop to stop the worker.
				continue
			case <-time.After(w.config.ReconnectDelay):
				continue
			}
		}
	}
}

// Drain stops accepting jobs and waits up to DrainTimeout (default 30 seconds)
// for active jobs. Then it sends the queued results and closes the stream
// gracefully. At the deadline it cancels the remaining jobs and closes the
// stream immediately, so the server can reclaim their leases.
func (w *StreamingWorker) Drain() {
	w.drain(w.drainTimeout)
}

func (w *StreamingWorker) drain(timeout time.Duration) {
	switch w.state.Load() {
	case int32(stateStopped):
		return
	case int32(stateIdle):
		// Run never started: there is no stream to close and no Run loop to stop
		// the worker.
		w.Stop()
		return
	}

	w.logger.Info("Draining streaming worker...")
	storeStateUnlessStopped(&w.state, stateDraining)

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for w.jobCount.Load() > 0 {
		select {
		case <-w.stopCh:
			return
		case <-deadline.C:
			w.logger.Warn("Drain deadline reached; cancelling active jobs", "jobs", w.jobCount.Load())
			w.Stop()
			return
		case <-ticker.C:
			w.logger.Info("Waiting for jobs to complete", "jobs", w.jobCount.Load())
		}
	}

	// Close the stream gracefully. Stop aborts it, and the engine can then miss
	// the last results. The send loop sends what is queued and half-closes the
	// stream, the engine reads to the end and closes it, and Run stops the
	// worker.
	w.flushOnce.Do(func() { close(w.flushCh) })
	select {
	case <-w.stopCh:
	case <-deadline.C:
		w.logger.Warn("Drain deadline reached before the engine closed the stream")
		w.Stop()
	}
}

// Stop immediately stops the streaming worker.
func (w *StreamingWorker) Stop() {
	w.state.Store(int32(stateStopped))
	w.stopOnce.Do(func() { close(w.stopCh) })

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

// connectStream establishes a single bidirectional stream connection.
func (w *StreamingWorker) connectStream(ctx context.Context) error {
	storeStateUnlessDraining(&w.state, stateConnecting)

	// Register functions via HTTP (not the stream) so the event router can find them.
	headers := w.getHeaders()
	if err := registerFunctions(ctx, w.config.ServerURL, headers, w.functions, w.httpClient, w.logger); err != nil {
		return fmt.Errorf("register functions: %w", err)
	}
	// A drain or Stop during the registration: a new stream could only take
	// jobs that the worker then ignores.
	if s := w.state.Load(); s == int32(stateDraining) || s == int32(stateStopped) {
		return nil
	}

	// Create ConnectRPC client
	client := ironflowv1connect.NewWorkerServiceClient(w.httpClient, w.config.ServerURL)

	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()

	// Open bidirectional stream
	stream := client.Connect(connCtx)
	for k, v := range headers {
		stream.RequestHeader().Set(k, v)
	}

	// Stop must close the stream itself: a cancelled context does not unblock
	// Receive while the engine holds the stream open, and the engine reclaims
	// this worker's leases only after the stream is gone. Start this after the headers are set: CloseRequest sends
	// the request, and that reads the header map.
	go func() {
		select {
		case <-w.stopCh:
		case <-connCtx.Done():
		}
		connCancel()
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
	}()

	// Send Register message
	functionIDs := make([]string, 0, len(w.functions))
	for id := range w.functions {
		functionIDs = append(functionIDs, id)
	}

	if err := stream.Send(&ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_Register{
			Register: &ironflowv1.WorkerRegister{
				WorkerId:          w.workerID,
				Hostname:          getHostname(),
				FunctionIds:       functionIDs,
				MaxConcurrentJobs: int32(w.config.MaxConcurrentJobs),
				Labels:            w.config.Labels,
				Version: &ironflowv1.WorkerVersion{
					Sdk:     SDKVersion,
					Runtime: "go",
				},
			},
		},
	}); err != nil {
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
		return fmt.Errorf("send register: %w", err)
	}

	// Wait for Registered response
	msg, err := stream.Receive()
	if err != nil {
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
		return fmt.Errorf("receive registered: %w", err)
	}

	reg, ok := msg.GetPayload().(*ironflowv1.EngineMessage_Registered)
	if !ok {
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
		return fmt.Errorf("expected Registered message, got %T", msg.GetPayload())
	}

	w.logger.Info("Registered with server", "workerId", reg.Registered.GetWorkerId(),
		"heartbeatMs", reg.Registered.GetHeartbeatIntervalMs())

	// Use server-suggested heartbeat interval if provided
	heartbeatInterval := w.config.HeartbeatInterval
	if serverMs := reg.Registered.GetHeartbeatIntervalMs(); serverMs > 0 {
		heartbeatInterval = time.Duration(serverMs) * time.Millisecond
	}

	storeStateUnlessDraining(&w.state, stateConnected)
	w.logger.Info("Connected to server (streaming)")

	// Stop any existing projection runners before starting new ones (prevents leak on reconnect)
	w.stopProjectionRunners()

	// Start projection runners
	w.startProjectionRunners()

	// Drain the outCh buffer from previous connection (non-blocking)
	w.drainOutCh()

	// Start send loop and recv loop
	var wg sync.WaitGroup

	// Channel to signal the recv loop has finished (which means disconnect)
	recvDone := make(chan error, 1)

	// sendLoop: reads from outCh and sends on the stream
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.sendLoop(connCtx, stream, heartbeatInterval)
	}()

	// recvLoop: reads from the stream and dispatches messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		recvDone <- w.recvLoop(connCtx, stream)
	}()

	// Wait for recv to exit (i.e., disconnect or context cancel)
	recvErr := <-recvDone

	// Cancel the connection context to stop the send loop
	connCancel()

	// Cancel all active jobs for this connection
	w.cancelAllJobs()

	// Wait for both loops to exit
	wg.Wait()

	// Close the stream
	_ = stream.CloseRequest()
	_ = stream.CloseResponse()

	return recvErr
}

// sendLoop reads from outCh and writes to the stream. It also sends heartbeats.
func (w *StreamingWorker) sendLoop(ctx context.Context, stream *connect.BidiStreamForClient[ironflowv1.WorkerMessage, ironflowv1.EngineMessage], heartbeatInterval time.Duration) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-w.flushCh:
			// Drain's graceful close. This loop is the only sender, so no Send is
			// in progress here: send what is queued, then half-close.
			for len(w.outCh) > 0 {
				if err := stream.Send(<-w.outCh); err != nil {
					w.logger.Warn("Send error", "error", err)
					return
				}
			}
			_ = stream.CloseRequest()
			return
		case msg := <-w.outCh:
			if err := stream.Send(msg); err != nil {
				w.logger.Warn("Send error", "error", err)
				return
			}
		case <-ticker.C:
			if err := w.sendHeartbeat(stream); err != nil {
				w.logger.Warn("Heartbeat send error", "error", err)
				return
			}
		}
	}
}

// sendHeartbeat sends a heartbeat message on the stream.
func (w *StreamingWorker) sendHeartbeat(stream *connect.BidiStreamForClient[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
	jobs := make([]*ironflowv1.ActiveJob, 0)
	w.activeJobs.Range(func(key, value any) bool {
		if job, ok := value.(*activeJob); ok {
			jobs = append(jobs, &ironflowv1.ActiveJob{
				JobId:     job.jobID,
				StartedAt: timestamppb.New(job.startedAt),
			})
		}
		return true
	})

	return stream.Send(&ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_Heartbeat{
			Heartbeat: &ironflowv1.WorkerHeartbeat{
				WorkerId:   w.workerID,
				ActiveJobs: w.jobCount.Load(),
				Jobs:       jobs,
			},
		},
	})
}

// recvLoop reads messages from the stream and dispatches them.
func (w *StreamingWorker) recvLoop(ctx context.Context, stream *connect.BidiStreamForClient[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := stream.Receive()
		if err != nil {
			return fmt.Errorf("receive: %w", err)
		}

		switch p := msg.GetPayload().(type) {
		case *ironflowv1.EngineMessage_Job:
			w.handleJobAssignment(ctx, p.Job)
		case *ironflowv1.EngineMessage_Cancel:
			w.handleCancelJob(p.Cancel)
		case *ironflowv1.EngineMessage_Shutdown:
			// Keep the stream open: the drain needs it to report the active jobs.
			w.handleShutdown(p.Shutdown)
		case *ironflowv1.EngineMessage_StepAck:
			// Step acknowledgement — logged for debugging, no action needed
			w.logger.Debug("Step ack received", "stepId", p.StepAck.GetStepId(), "accepted", p.StepAck.GetAccepted())
		case *ironflowv1.EngineMessage_Resume:
			// Resume is not yet handled in the streaming worker; the job executor
			// handles resume via the completedSteps memoization in the job assignment.
			w.logger.Debug("Resume received", "jobId", p.Resume.GetJobId(), "stepId", p.Resume.GetStepId())
		default:
			w.logger.Warn("Unknown engine message type", "type", fmt.Sprintf("%T", msg.GetPayload()))
		}
	}
}

// handleJobAssignment processes an incoming job assignment.
func (w *StreamingWorker) handleJobAssignment(ctx context.Context, protoJob *ironflowv1.JobAssignment) {
	// A job the worker cannot run gets a nack (#2456). A nack uses no run
	// attempt, and the engine re-queues the job at once. An engine older than
	// the nack drops the message; it then recovers the job after the lease
	// expires.
	//
	// Count the job before the draining check. Drain sets the state and then
	// reads the count, so Drain sees this job or this check sees the drain.
	count := w.jobCount.Add(1)
	if w.state.Load() == int32(stateDraining) {
		w.jobCount.Add(-1)
		w.logger.Info("Draining, refusing job", "jobId", protoJob.GetJobId())
		w.nackJob(ctx, protoJob, ironflowv1.JobNackReason_JOB_NACK_REASON_DRAINING)
		return
	}
	if int(count) > w.config.MaxConcurrentJobs {
		w.jobCount.Add(-1)
		w.logger.Warn("At max capacity, refusing job", "jobId", protoJob.GetJobId())
		w.nackJob(ctx, protoJob, ironflowv1.JobNackReason_JOB_NACK_REASON_AT_CAPACITY)
		return
	}

	// Send JobAck, echoing the execution fence (#1206, ADR 0037, chunk 3e) so the
	// engine can validate the ack against the assigned segment.
	w.enqueue(ctx, &ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_JobAck{
			JobAck: &ironflowv1.JobAck{
				JobId:        protoJob.GetJobId(),
				ExecutionSeq: protoJob.GetExecutionSeq(),
				LeaseToken:   protoJob.GetLeaseToken(),
			},
		},
	})

	// Convert proto assignment to SDK type
	job, err := protoToJobAssignment(protoJob)
	if err != nil {
		w.jobCount.Add(-1)
		w.logger.Error("Failed to convert job assignment", "jobId", protoJob.GetJobId(), "error", err)
		return
	}

	// Create reporter that sends results via the stream. It carries the per-job
	// execution fence so every terminal/yield it emits echoes back to the engine.
	reporter := &streamJobReporter{
		outCh:        w.outCh,
		logger:       w.logger,
		executionSeq: job.ExecutionSeq,
		leaseToken:   job.LeaseToken,
	}

	// Process asynchronously
	jobCtx, cancel := context.WithCancel(ctx)

	aj := &activeJob{
		jobID:     job.JobID,
		runID:     job.RunID,
		startedAt: time.Now(),
		cancel:    cancel,
	}

	w.activeJobs.Store(job.JobID, aj)

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

// nackJob refuses an assignment the worker did not start, echoing its fence. If
// the connection ends before the nack is queued, the engine stops refreshing the
// job's lease, because heartbeats do not list it, and recovers the job.
func (w *StreamingWorker) nackJob(ctx context.Context, job *ironflowv1.JobAssignment, reason ironflowv1.JobNackReason) {
	w.enqueue(ctx, &ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_JobNack{
			JobNack: &ironflowv1.JobNack{
				JobId:        job.GetJobId(),
				RunId:        job.GetRunId(),
				ExecutionSeq: job.GetExecutionSeq(),
				LeaseToken:   job.GetLeaseToken(),
				Reason:       reason,
			},
		},
	})
}

// handleCancelJob cancels an active job.
func (w *StreamingWorker) handleCancelJob(cancel *ironflowv1.CancelJob) {
	jobID := cancel.GetJobId()
	w.logger.Info("Cancel job requested", "jobId", jobID, "reason", cancel.GetReason())

	if val, ok := w.activeJobs.Load(jobID); ok {
		if aj, ok := val.(*activeJob); ok {
			aj.cancel()
		}
	}
}

// handleShutdown handles a shutdown message from the engine.
func (w *StreamingWorker) handleShutdown(shutdown *ironflowv1.Shutdown) {
	w.logger.Info("Shutdown requested by server", "reason", shutdown.GetReason())
	// Set the state here, not in the goroutine: the receive loop continues and
	// must not accept a job that is already in the stream behind this message.
	storeStateUnlessStopped(&w.state, stateDraining)
	// The engine stops waiting after its drain timeout, so use it when it is set.
	timeout := time.Duration(shutdown.GetDrainTimeoutMs()) * time.Millisecond
	if timeout <= 0 {
		timeout = w.drainTimeout
	}
	go w.drain(timeout)
}

// cancelAllJobs cancels all active jobs.
func (w *StreamingWorker) cancelAllJobs() {
	w.activeJobs.Range(func(key, value any) bool {
		if aj, ok := value.(*activeJob); ok {
			aj.cancel()
		}
		return true
	})
}

// enqueue queues a message without blocking the receive loop. When outCh is full
// a goroutine waits for space until ctx (the connection) ends (#2500): a dropped
// JobNack leaves the job to lease expiry. The engine ignores a JobAck today, so
// waiting for it costs nothing.
// A waiter that wins the select just as ctx ends can queue one message after the
// next drainOutCh; the engine discards a message with a stale fence.
func (w *StreamingWorker) enqueue(ctx context.Context, msg *ironflowv1.WorkerMessage) {
	select {
	case w.outCh <- msg:
		return
	default:
	}
	w.logger.Warn("outCh full, waiting for space")
	go func() {
		select {
		case w.outCh <- msg:
		case <-ctx.Done():
			w.logger.Warn("connection ended while outCh was full, message not sent")
		}
	}()
}

// drainOutCh empties the outCh buffer (non-blocking). Messages left from the
// previous connection are discarded on purpose: they echo that connection's
// execution fence, a stale fence makes the engine kill the stream, and the
// engine reclaims those jobs' leases when the old connection drops.
func (w *StreamingWorker) drainOutCh() {
	discarded := 0
	for {
		select {
		case <-w.outCh:
			discarded++
		default:
			if discarded > 0 {
				w.logger.Warn("discarded queued messages from the previous connection", "count", discarded)
			}
			return
		}
	}
}

// getHeaders returns the headers for HTTP requests (API key, environment).
func (w *StreamingWorker) getHeaders() map[string]string {
	return buildWorkerHeaders(w.config.APIKey, w.config.Environment)
}

// startProjectionRunners starts a ProjectionRunner for each configured projection.
func (w *StreamingWorker) startProjectionRunners() {
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
func (w *StreamingWorker) stopProjectionRunners() {
	w.projMu.Lock()
	runners := w.projectionRunners
	w.projectionRunners = nil
	w.projMu.Unlock()

	for _, runner := range runners {
		runner.Stop()
	}
}

// ---------------------------------------------------------------------------
// streamJobReporter implements jobReporter for the streaming worker.
// It sends JobCompleted / JobFailed via the outCh channel.
// ---------------------------------------------------------------------------

type streamJobReporter struct {
	outCh  chan<- *ironflowv1.WorkerMessage
	logger Logger

	// Execution fence (#1206, ADR 0037, chunk 3e), captured per job from the
	// JobAssignment. send() stamps it onto every outgoing message so the engine's
	// ingress fence guard can validate it. Zero for legacy / non-capacity jobs.
	executionSeq int64
	leaseToken   string
}

func (r *streamJobReporter) ReportCompleted(ctx context.Context, jobID string, output any, _ []*StepResult, _ int) error {
	outputStruct, outputValue, err := anyToPayload(output)
	if err != nil {
		r.logger.Warn("Failed to convert output to struct", "jobId", jobID, "error", err)
	}

	return r.send(ctx, &ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_JobCompleted{
			JobCompleted: &ironflowv1.JobCompleted{
				JobId:       jobID,
				Output:      outputStruct,
				OutputValue: outputValue,
			},
		},
	})
}

func (r *streamJobReporter) ReportFailed(ctx context.Context, jobID string, pushErr *PushError, steps []*StepResult, _ int) error {
	protoSteps := make([]*ironflowv1.ExecutedStep, 0, len(steps))
	for _, s := range steps {
		ps := &ironflowv1.ExecutedStep{
			Id:              s.ID,
			Name:            s.Name,
			Type:            s.Type,
			Status:          s.Status,
			CompensationFor: s.CompensationFor,
			DurationMs:      int32(s.Duration.Milliseconds()),
		}
		if s.Output != nil {
			if outStruct, outValue, convErr := anyToPayload(s.Output); convErr == nil {
				ps.Output, ps.OutputValue = outStruct, outValue
			}
		}
		if s.Error != nil {
			ps.Error = &ironflowv1.Error{
				Message:   s.Error.Message,
				Retryable: s.Error.Retryable,
			}
		}
		protoSteps = append(protoSteps, ps)
	}

	return r.send(ctx, &ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_JobFailed{
			JobFailed: &ironflowv1.JobFailed{
				JobId: jobID,
				Error: &ironflowv1.Error{
					Message:   pushErr.Message,
					Code:      pushErr.Code,
					Retryable: pushErr.Retryable,
				},
				Steps: protoSteps,
			},
		},
	})
}

// sendTerminalFailure sends a non-retryable JobFailed for a yield that cannot
// be sent, so the job is not left leased until the engine's lease expiry.
func (r *streamJobReporter) sendTerminalFailure(ctx context.Context, jobID, code string, err error) {
	// The caller returns err, the root cause; a failed send here is only logged.
	_ = r.send(ctx, &ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_JobFailed{
			JobFailed: &ironflowv1.JobFailed{
				JobId: jobID,
				Error: &ironflowv1.Error{Message: err.Error(), Code: code, Retryable: false},
			},
		},
	})
}

func (r *streamJobReporter) ReportYielded(ctx context.Context, jobID string, yield *YieldInfo, _ []*StepResult, _ int) error {
	// For yields, we send a JobFailed with a special status code so the engine
	// recognizes it as a yield. However, the proto defines step-level yield.
	// We need to send the yield info at the step level, then a completed job
	// won't include the yield. Actually the proper way is to send StepYielded
	// and then the engine handles the rest. But since the jobReporter interface
	// only has ReportYielded and the engine expects it at the job level for
	// polling workers, let's translate the SDK yield to the proper proto type.

	switch yield.Type {
	case "sleep":
		until, err := time.Parse(time.RFC3339, yield.Until)
		if err != nil {
			// Best-effort: send a raw failed message
			return r.send(ctx, &ironflowv1.WorkerMessage{
				Payload: &ironflowv1.WorkerMessage_JobFailed{
					JobFailed: &ironflowv1.JobFailed{
						JobId: jobID,
						Error: &ironflowv1.Error{
							Message: fmt.Sprintf("yield (sleep until %s)", yield.Until),
							Code:    "YIELD_SLEEP",
						},
					},
				},
			})
		}
		return r.send(ctx, &ironflowv1.WorkerMessage{
			Payload: &ironflowv1.WorkerMessage_StepYielded{
				StepYielded: &ironflowv1.StepYielded{
					JobId:  jobID,
					StepId: yield.StepID,
					YieldInfo: &ironflowv1.StepYielded_Sleep{
						Sleep: &ironflowv1.SleepYield{
							Until: timestamppb.New(until),
						},
					},
				},
			},
		})

	case "wait_for_event":
		var timeout *timestamppb.Timestamp
		if yield.EventFilter != nil && yield.EventFilter.Timeout > 0 {
			timeout = timestamppb.New(time.Now().Add(yield.EventFilter.Timeout))
		}
		var payloadJSON []byte
		var eventName, matchExpr, matchValue string
		if yield.EventFilter != nil {
			if yield.EventFilter.Payload != nil {
				var err error
				payloadJSON, err = json.Marshal(yield.EventFilter.Payload)
				if err != nil {
					return fmt.Errorf("marshal wait request payload: %w", err)
				}
			}
			eventName = yield.EventFilter.Event
			matchExpr = yield.EventFilter.Match
			matchValue = yield.EventFilter.MatchValue
		}
		return r.send(ctx, &ironflowv1.WorkerMessage{
			Payload: &ironflowv1.WorkerMessage_StepYielded{
				StepYielded: &ironflowv1.StepYielded{
					JobId:  jobID,
					StepId: yield.StepID,
					YieldInfo: &ironflowv1.StepYielded_WaitEvent{
						WaitEvent: &ironflowv1.WaitEventYield{
							EventName:       eventName,
							PayloadJson:     payloadJSON,
							MatchExpression: matchExpr,
							MatchValue:      matchValue,
							Timeout:         timeout,
						},
					},
				},
			},
		})

	case "invoke_function", "invoke_function_async":
		var inputJSON []byte
		if yield.Input != nil {
			var err error
			if inputJSON, err = json.Marshal(yield.Input); err != nil {
				err = fmt.Errorf("marshal invoke input: %w", err)
				r.sendTerminalFailure(ctx, jobID, "SERIALIZATION_ERROR", err)
				return err
			}
		}
		sy := &ironflowv1.StepYielded{JobId: jobID, StepId: yield.StepID}
		if yield.Type == "invoke_function" {
			sy.YieldInfo = &ironflowv1.StepYielded_InvokeFunction{InvokeFunction: &ironflowv1.InvokeFunctionYield{
				FunctionId: yield.FunctionID, InputJson: inputJSON, InvokeTimeoutMs: int64(yield.InvokeTimeoutMs),
			}}
		} else {
			sy.YieldInfo = &ironflowv1.StepYielded_InvokeFunctionAsync{InvokeFunctionAsync: &ironflowv1.InvokeFunctionAsyncYield{
				FunctionId: yield.FunctionID, InputJson: inputJSON,
			}}
		}
		return r.send(ctx, &ironflowv1.WorkerMessage{Payload: &ironflowv1.WorkerMessage_StepYielded{StepYielded: sy}})

	default:
		// Send a JobFailed so the job isn't left leased and stranded: without a
		// terminal message the engine only recovers it on lease expiry.
		err := fmt.Errorf("unsupported yield type %q", yield.Type)
		r.sendTerminalFailure(ctx, jobID, "UNSUPPORTED_YIELD_TYPE", err)
		return err
	}
}

// send queues a terminal report and waits for space when the queue is full: a
// dropped JobCompleted/JobFailed/StepYielded leaves the job leased with no result
// (#2478). It fails only when ctx ends, so the executor sees the report as unsent.
func (r *streamJobReporter) send(ctx context.Context, msg *ironflowv1.WorkerMessage) error {
	r.stampFence(msg)
	// Try first so a cancelled ctx cannot win the select against free space.
	select {
	case r.outCh <- msg:
		return nil
	default:
	}
	select {
	case r.outCh <- msg:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("send queue full, job report not sent: %w", ctx.Err())
	}
}

// stampFence copies the per-job execution fence onto a mutating message before it
// is sent (#1206, ADR 0037, chunk 3e). Centralizing it here covers every
// JobCompleted / JobFailed / StepYielded construction in this reporter — including
// the yield-parse fallbacks — so no echo site can be missed. Every call site
// allocates the inner message, so the oneof inner is never nil today; the nil
// guards are defensive against a future caller passing a bare oneof wrapper.
func (r *streamJobReporter) stampFence(msg *ironflowv1.WorkerMessage) {
	switch p := msg.GetPayload().(type) {
	case *ironflowv1.WorkerMessage_JobCompleted:
		if p.JobCompleted != nil {
			p.JobCompleted.ExecutionSeq = r.executionSeq
			p.JobCompleted.LeaseToken = r.leaseToken
		}
	case *ironflowv1.WorkerMessage_JobFailed:
		if p.JobFailed != nil {
			p.JobFailed.ExecutionSeq = r.executionSeq
			p.JobFailed.LeaseToken = r.leaseToken
		}
	case *ironflowv1.WorkerMessage_StepYielded:
		if p.StepYielded != nil {
			p.StepYielded.ExecutionSeq = r.executionSeq
			p.StepYielded.LeaseToken = r.leaseToken
		}
	}
}

// ---------------------------------------------------------------------------
// streamStepReporter implements stepLifecycleReporter for the streaming worker.
// ---------------------------------------------------------------------------

// One reporter per job: the engine reads Step*.JobId as the run ID (an empty one
// fails the step-row FK), and a capacity job's Step* must echo the execution
// fence like every other mutating message or the engine kills the stream
// (#1206, ADR 0037).
type streamStepReporter struct {
	outCh        chan<- *ironflowv1.WorkerMessage
	logger       Logger
	jobID        string
	executionSeq int64
	leaseToken   string
}

// stepReporter binds a step reporter to this job and its fence.
func (r *streamJobReporter) stepReporter(jobID string) stepLifecycleReporter {
	return &streamStepReporter{outCh: r.outCh, logger: r.logger, jobID: jobID, executionSeq: r.executionSeq, leaseToken: r.leaseToken}
}

// trySend never blocks: the step interface has no error return, and waiting
// would stall the handler. A dropped step message is logged, not silent (#2478).
func (r *streamStepReporter) trySend(kind, stepID string, msg *ironflowv1.WorkerMessage) {
	select {
	case r.outCh <- msg:
	default:
		r.logger.Warn("outCh full, dropping step message", "type", kind, "jobId", r.jobID, "stepId", stepID)
	}
}

func (r *streamStepReporter) ReportStepStarted(stepID, name, stepType string) {
	r.trySend("StepStarted", stepID, &ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_StepStarted{
			StepStarted: &ironflowv1.StepStarted{
				JobId:        r.jobID,
				StepId:       stepID,
				Name:         name,
				StepType:     sdkStepTypeToProto(stepType),
				ExecutionSeq: r.executionSeq,
				LeaseToken:   r.leaseToken,
			},
		},
	})
}

func (r *streamStepReporter) ReportStepCompleted(stepID, name, stepType string, output any, durationMs int) {
	outputStruct, outputValue, _ := anyToPayload(output)
	r.trySend("StepCompleted", stepID, &ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_StepCompleted{
			StepCompleted: &ironflowv1.StepCompleted{
				JobId:        r.jobID,
				StepId:       stepID,
				Output:       outputStruct,
				OutputValue:  outputValue,
				DurationMs:   int32(durationMs),
				ExecutionSeq: r.executionSeq,
				LeaseToken:   r.leaseToken,
			},
		},
	})
}

func (r *streamStepReporter) ReportStepFailed(stepID, name, stepType string, errMsg string, durationMs int) {
	r.trySend("StepFailed", stepID, &ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_StepFailed{
			StepFailed: &ironflowv1.StepFailed{
				JobId:  r.jobID,
				StepId: stepID,
				Error: &ironflowv1.Error{
					Message: errMsg,
				},
				DurationMs:   int32(durationMs),
				ExecutionSeq: r.executionSeq,
				LeaseToken:   r.leaseToken,
			},
		},
	})
}

// ---------------------------------------------------------------------------
// Proto ↔ SDK conversion helpers
// ---------------------------------------------------------------------------

// protoToJobAssignment converts a proto JobAssignment to the SDK's jobAssignment.
func protoToJobAssignment(pa *ironflowv1.JobAssignment) (*jobAssignment, error) {
	// Convert event data (google.protobuf.Struct → json.RawMessage)
	var eventData json.RawMessage
	if pa.GetEvent() != nil && pa.GetEvent().GetData() != nil {
		b, err := protojson.Marshal(pa.GetEvent().GetData())
		if err != nil {
			return nil, fmt.Errorf("marshal event data: %w", err)
		}
		eventData = b
	} else {
		eventData = json.RawMessage("{}")
	}

	// Convert event timestamp
	var eventTimestamp string
	if pa.GetEvent() != nil && pa.GetEvent().GetTimestamp() != nil {
		eventTimestamp = pa.GetEvent().GetTimestamp().AsTime().Format(time.RFC3339)
	}

	// Convert completed steps
	completedSteps := make([]completedStep, 0, len(pa.GetCompletedSteps()))
	for _, cs := range pa.GetCompletedSteps() {
		var output any
		// A non-object output arrives only in output_value (#1963).
		if cs.GetOutput() != nil || cs.GetOutputValue() != nil {
			output = payloadAny(cs.GetOutput(), cs.GetOutputValue())
		}
		var stepErr any
		if len(cs.GetErrorJson()) > 0 {
			if err := json.Unmarshal(cs.GetErrorJson(), &stepErr); err != nil {
				stepErr = string(cs.GetErrorJson())
			}
		}
		completedSteps = append(completedSteps, completedStep{
			StepID: cs.GetStepId(),
			Name:   cs.GetName(),
			Output: output,
			Status: cs.GetStatus(),
			Error:  stepErr,
		})
	}

	// Extract secrets from context
	var jobCtx *jobContext
	if pa.GetContext() != nil && len(pa.GetContext().GetSecrets()) > 0 {
		jobCtx = &jobContext{
			Secrets: pa.GetContext().GetSecrets(),
		}
	}

	eventVersion := 1
	if pa.GetEvent() != nil && pa.GetEvent().GetVersion() > 0 {
		eventVersion = int(pa.GetEvent().GetVersion())
	}

	var eventMetadata json.RawMessage
	if pa.GetEvent() != nil && pa.GetEvent().GetMetadata() != nil {
		b, err := protojson.Marshal(pa.GetEvent().GetMetadata())
		if err != nil {
			return nil, fmt.Errorf("marshal event metadata: %w", err)
		}
		eventMetadata = b
	}

	return &jobAssignment{
		JobID:       pa.GetJobId(),
		RunID:       pa.GetRunId(),
		FunctionID:  pa.GetFunctionId(),
		Attempt:     int(pa.GetAttempt()),
		MaxAttempts: int(pa.GetMaxAttempts()),
		Event: jobEvent{
			ID:             pa.GetEvent().GetId(),
			Name:           pa.GetEvent().GetName(),
			Version:        eventVersion,
			Data:           eventData,
			Timestamp:      eventTimestamp,
			IdempotencyKey: pa.GetEvent().GetIdempotencyKey(),
			Source:         pa.GetEvent().GetSource(),
			Metadata:       eventMetadata,
		},
		CompletedSteps: completedSteps,
		ActorID:        pa.GetActorId(),
		Context:        jobCtx,
		ExecutionSeq:   pa.GetExecutionSeq(),
		LeaseToken:     pa.GetLeaseToken(),
	}, nil
}

// anyToStruct converts an arbitrary Go value to a protobuf Struct.
// anyToPayload splits a step or job output across the two fields that carry
// it: an object goes in the Struct field exactly as before, anything else in
// the companion Value field.
//
// It used to return only a Struct, which forced every output through
// map[string]any -- so a handler returning [1,2,3] or "ok" had its output
// dropped on the way out (#1963). Adding a second field rather than changing
// the first keeps the wire compatible for readers that know only the original.
func anyToPayload(v any) (*structpb.Struct, *structpb.Value, error) {
	if v == nil {
		return nil, nil, nil
	}
	if m, ok := v.(map[string]any); ok {
		s, err := structpb.NewStruct(m)
		return s, nil, err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal to JSON: %w", err)
	}
	var decoded any
	if err := json.Unmarshal(b, &decoded); err != nil {
		return nil, nil, fmt.Errorf("unmarshal from JSON: %w", err)
	}
	if m, ok := decoded.(map[string]any); ok {
		s, err := structpb.NewStruct(m)
		return s, nil, err
	}
	val, err := structpb.NewValue(decoded)
	return nil, val, err
}

// payloadAny reads whichever field carries a payload (#1963).
func payloadAny(s *structpb.Struct, v *structpb.Value) any {
	if v != nil {
		return v.AsInterface()
	}
	if s != nil {
		return s.AsMap()
	}
	return nil
}

// sdkStepTypeToProto maps SDK step type strings to proto StepType.
func sdkStepTypeToProto(t string) ironflowv1.StepType {
	switch t {
	case "invoke":
		return ironflowv1.StepType_STEP_TYPE_INVOKE
	case "sleep":
		return ironflowv1.StepType_STEP_TYPE_SLEEP
	case "wait_for_event":
		return ironflowv1.StepType_STEP_TYPE_WAIT_FOR_EVENT
	case "compensate":
		return ironflowv1.StepType_STEP_TYPE_COMPENSATE
	case "invoke_function":
		return ironflowv1.StepType_STEP_TYPE_INVOKE_FUNCTION
	default:
		return ironflowv1.StepType_STEP_TYPE_UNSPECIFIED
	}
}

// newH2CClient creates an HTTP client that supports HTTP/2 cleartext (h2c).
// This is needed for development against http://localhost:9123.
// For HTTPS URLs it falls back to standard TLS-based HTTP/2.
func newH2CClient(serverURL string) *http.Client {
	if strings.HasPrefix(serverURL, "https://") {
		return &http.Client{
			Timeout: 0, // streaming: no timeout on the client
		}
	}

	// HTTP/2 cleartext transport
	return &http.Client{
		Timeout: 0,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		},
	}
}
