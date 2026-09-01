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
	projMu            sync.Mutex
	projectionRunners []*ProjectionRunner

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
//	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
//	defer cancel()
//
//	if err := worker.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
func NewStreamingWorker(config WorkerConfig) *StreamingWorker {
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
		config:    config,
		functions: functions,
		workerID:  generateWorkerID(),
		logger:    logger,
		outCh:     make(chan *ironflowv1.WorkerMessage, 256),
		stopCh:    make(chan struct{}),
	}

	w.httpClient = newH2CClient(config.ServerURL)

	w.executor = &jobExecutor{
		functions: functions,
		upcasters: config.Upcasters,
		serverURL: config.ServerURL,
		apiKey:    config.APIKey,
		logger:    logger,
		onError:   config.OnError,
		stepReporter: &streamStepReporter{
			outCh: w.outCh,
		},
	}

	return w
}

// Run starts the streaming worker and blocks until stopped.
// It auto-reconnects on disconnect.
func (w *StreamingWorker) Run(ctx context.Context) error {
	if !w.state.CompareAndSwap(int32(stateIdle), int32(stateConnecting)) {
		return NewError("worker is already running", "WORKER_ALREADY_RUNNING", false)
	}

	w.logger.Info("Starting streaming worker", "workerId", w.workerID, "functions", len(w.functions))

	for {
		select {
		case <-ctx.Done():
			w.state.Store(int32(stateStopped))
			return ctx.Err()
		case <-w.stopCh:
			return nil
		default:
		}

		if err := w.connectStream(ctx); err != nil {
			if w.state.Load() == int32(stateStopped) {
				return nil
			}

			// Auth failures do not fix themselves on the reconnect cadence
			// (#1673). Covers both the HTTP register call and the stream itself,
			// which surfaces auth as a Connect code rather than an ironflow error.
			code := connect.CodeOf(err)
			if errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrForbidden) ||
				code == connect.CodeUnauthenticated || code == connect.CodePermissionDenied {
				w.logger.Error(fmt.Sprintf("stream authentication failed: %v. %s", err, AuthHelp))
				w.Stop()
				return err
			}

			w.logger.Error("Stream connection error", "error", err)
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

// Drain gracefully drains and stops the streaming worker.
func (w *StreamingWorker) Drain() {
	if w.state.Load() == int32(stateStopped) {
		return
	}

	w.logger.Info("Draining streaming worker...")
	w.state.Store(int32(stateDraining))

	// Wait for active jobs to complete
	for w.jobCount.Load() > 0 {
		w.logger.Info("Waiting for jobs to complete", "jobs", w.jobCount.Load())
		time.Sleep(time.Second)
	}

	w.Stop()
}

// Stop immediately stops the streaming worker.
func (w *StreamingWorker) Stop() {
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

// connectStream establishes a single bidirectional stream connection.
func (w *StreamingWorker) connectStream(ctx context.Context) error {
	w.state.Store(int32(stateConnecting))

	// Register functions via HTTP (not the stream) so the event router can find them.
	headers := w.getHeaders()
	if err := registerFunctions(ctx, w.config.ServerURL, headers, w.functions, w.httpClient, w.logger); err != nil {
		return fmt.Errorf("register functions: %w", err)
	}

	// Create ConnectRPC client
	client := ironflowv1connect.NewWorkerServiceClient(w.httpClient, w.config.ServerURL)

	// Open bidirectional stream
	stream := client.Connect(ctx)

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

	w.state.Store(int32(stateConnected))
	w.logger.Info("Connected to server (streaming)")

	// Stop any existing projection runners before starting new ones (prevents leak on reconnect)
	w.stopProjectionRunners()

	// Start projection runners
	w.startProjectionRunners()

	// Drain the outCh buffer from previous connection (non-blocking)
	w.drainOutCh()

	// Create a context that is cancelled when this connection ends
	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()

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
			w.handleShutdown(p.Shutdown)
			return nil
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
	// Check capacity
	if int(w.jobCount.Load()) >= w.config.MaxConcurrentJobs {
		w.logger.Warn("At max capacity, ignoring job", "jobId", protoJob.GetJobId())
		return
	}

	// Check if draining
	if w.state.Load() == int32(stateDraining) {
		w.logger.Info("Draining, ignoring job", "jobId", protoJob.GetJobId())
		return
	}

	// Send JobAck, echoing the execution fence (#1206, ADR 0037, chunk 3e) so the
	// engine can validate the ack against the assigned segment.
	w.enqueue(&ironflowv1.WorkerMessage{
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
	go w.Drain()
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

// enqueue sends a message to the outCh buffer. Drops the message if the buffer
// is full (non-blocking) to prevent deadlocks.
func (w *StreamingWorker) enqueue(msg *ironflowv1.WorkerMessage) {
	select {
	case w.outCh <- msg:
	default:
		w.logger.Warn("outCh full, dropping message")
	}
}

// drainOutCh empties the outCh buffer (non-blocking).
func (w *StreamingWorker) drainOutCh() {
	for {
		select {
		case <-w.outCh:
		default:
			return
		}
	}
}

// getHeaders returns the headers for HTTP requests (e.g., API key).
func (w *StreamingWorker) getHeaders() map[string]string {
	return buildAuthHeaders(w.config.APIKey)
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

func (r *streamJobReporter) ReportCompleted(_ context.Context, jobID string, output any, _ []*StepResult) error {
	outputStruct, outputValue, err := anyToPayload(output)
	if err != nil {
		r.logger.Warn("Failed to convert output to struct", "jobId", jobID, "error", err)
	}

	r.send(&ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_JobCompleted{
			JobCompleted: &ironflowv1.JobCompleted{
				JobId:       jobID,
				Output:      outputStruct,
				OutputValue: outputValue,
			},
		},
	})
	return nil
}

func (r *streamJobReporter) ReportFailed(_ context.Context, jobID string, pushErr *PushError, steps []*StepResult) error {
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

	r.send(&ironflowv1.WorkerMessage{
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
	return nil
}

func (r *streamJobReporter) ReportYielded(_ context.Context, jobID string, yield *YieldInfo) error {
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
			r.send(&ironflowv1.WorkerMessage{
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
			return nil
		}
		r.send(&ironflowv1.WorkerMessage{
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
		var eventName, matchExpr string
		if yield.EventFilter != nil {
			eventName = yield.EventFilter.Event
			matchExpr = yield.EventFilter.Match
		}
		r.send(&ironflowv1.WorkerMessage{
			Payload: &ironflowv1.WorkerMessage_StepYielded{
				StepYielded: &ironflowv1.StepYielded{
					JobId:  jobID,
					StepId: yield.StepID,
					YieldInfo: &ironflowv1.StepYielded_WaitEvent{
						WaitEvent: &ironflowv1.WaitEventYield{
							EventName:       eventName,
							MatchExpression: matchExpr,
							Timeout:         timeout,
						},
					},
				},
			},
		})

	default:
		// invoke_function / invoke_function_async — send as job failed with code
		r.send(&ironflowv1.WorkerMessage{
			Payload: &ironflowv1.WorkerMessage_JobFailed{
				JobFailed: &ironflowv1.JobFailed{
					JobId: jobID,
					Error: &ironflowv1.Error{
						Message: fmt.Sprintf("yield (%s)", yield.Type),
						Code:    "YIELD_" + strings.ToUpper(yield.Type),
					},
				},
			},
		})
	}

	return nil
}

func (r *streamJobReporter) send(msg *ironflowv1.WorkerMessage) {
	r.stampFence(msg)
	select {
	case r.outCh <- msg:
	default:
		r.logger.Warn("outCh full, dropping job report message")
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

// GO-LIVE COUPLING (#1206, ADR 0037): this reporter is a per-worker singleton
// with no per-job binding, so its Step* messages carry an empty JobId and no
// execution fence — the engine's ingress guard treats them as "unknown job" and
// passes them UNFENCED today. Per-job step attribution must land before pull
// capacity is armed, and it MUST add the fence in the same change: the instant a
// real JobId is stamped on a capacity job's Step*, an absent lease token flips
// the engine verdict to fenceDisconnect (stream kill) on every step. Give this
// reporter per-job fence binding (like streamJobReporter) at that time.
type streamStepReporter struct {
	outCh chan<- *ironflowv1.WorkerMessage
}

func (r *streamStepReporter) ReportStepStarted(stepID, name, stepType string) {
	select {
	case r.outCh <- &ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_StepStarted{
			StepStarted: &ironflowv1.StepStarted{
				StepId:   stepID,
				Name:     name,
				StepType: sdkStepTypeToProto(stepType),
			},
		},
	}:
	default:
	}
}

func (r *streamStepReporter) ReportStepCompleted(stepID, name, stepType string, output any, durationMs int) {
	outputStruct, outputValue, _ := anyToPayload(output)
	select {
	case r.outCh <- &ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_StepCompleted{
			StepCompleted: &ironflowv1.StepCompleted{
				StepId:      stepID,
				Output:      outputStruct,
				OutputValue: outputValue,
				DurationMs:  int32(durationMs),
			},
		},
	}:
	default:
	}
}

func (r *streamStepReporter) ReportStepFailed(stepID, name, stepType string, errMsg string, durationMs int) {
	select {
	case r.outCh <- &ironflowv1.WorkerMessage{
		Payload: &ironflowv1.WorkerMessage_StepFailed{
			StepFailed: &ironflowv1.StepFailed{
				StepId: stepID,
				Error: &ironflowv1.Error{
					Message: errMsg,
				},
				DurationMs: int32(durationMs),
			},
		},
	}:
	default:
	}
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
		if cs.GetOutput() != nil {
			output = payloadAny(cs.GetOutput(), cs.GetOutputValue())
		}
		completedSteps = append(completedSteps, completedStep{
			StepID: cs.GetStepId(),
			Name:   cs.GetName(),
			Output: output,
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
		JobID:      pa.GetJobId(),
		RunID:      pa.GetRunId(),
		FunctionID: pa.GetFunctionId(),
		Attempt:    int(pa.GetAttempt()),
		Event: jobEvent{
			ID:        pa.GetEvent().GetId(),
			Name:      pa.GetEvent().GetName(),
			Version:   eventVersion,
			Data:      eventData,
			Timestamp: eventTimestamp,
			Metadata:  eventMetadata,
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
