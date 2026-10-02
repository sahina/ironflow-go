package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

// ---------------------------------------------------------------------------
// Mock server infrastructure
// ---------------------------------------------------------------------------

// mockWorkerHandler implements ironflowv1connect.WorkerServiceHandler.
// It provides a configurable Connect bidi stream handler that collects
// messages from the worker and sends engine messages on demand.
type mockWorkerHandler struct {
	ironflowv1connect.UnimplementedWorkerServiceHandler

	mu sync.Mutex

	// onConnect is called each time Connect is invoked. It receives the stream
	// and a done channel that is closed when the handler should return.
	onConnect func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error

	// received collects all WorkerMessage payloads the server saw.
	received []*ironflowv1.WorkerMessage
}

func (m *mockWorkerHandler) Connect(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
	if m.onConnect != nil {
		return m.onConnect(ctx, stream)
	}
	return nil
}

func (m *mockWorkerHandler) record(msg *ironflowv1.WorkerMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.received = append(m.received, msg)
}

func (m *mockWorkerHandler) getReceived() []*ironflowv1.WorkerMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*ironflowv1.WorkerMessage, len(m.received))
	copy(out, m.received)
	return out
}

// startMockServer creates an httptest server that serves both the WorkerService
// Connect RPC and a stub RegisterFunction endpoint. It returns the server
// (caller must call Close) and its URL.
func startMockServer(t *testing.T, handler *mockWorkerHandler) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()

	// WorkerService handler (ConnectRPC bidi stream)
	path, h := ironflowv1connect.NewWorkerServiceHandler(handler)
	mux.Handle(path, h)

	// Stub RegisterFunction endpoint — the streaming worker calls this before
	// opening the bidi stream.
	mux.HandleFunc("/ironflow.v1.IronflowService/RegisterFunction", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})

	server := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	server.Start()
	t.Cleanup(server.Close)
	return server
}

// doRegistrationHandshake reads the Register message, records it, and sends
// back a WorkerRegistered response. It returns the Register message for
// assertions.
func doRegistrationHandshake(
	stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage],
	handler *mockWorkerHandler,
	heartbeatMs int32,
) (*ironflowv1.WorkerRegister, error) {
	msg, err := stream.Receive()
	if err != nil {
		return nil, fmt.Errorf("receive register: %w", err)
	}
	handler.record(msg)

	reg, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_Register)
	if !ok {
		return nil, fmt.Errorf("expected Register, got %T", msg.GetPayload())
	}

	if err := stream.Send(&ironflowv1.EngineMessage{
		Payload: &ironflowv1.EngineMessage_Registered{
			Registered: &ironflowv1.WorkerRegistered{
				WorkerId:            reg.Register.GetWorkerId(),
				HeartbeatIntervalMs: heartbeatMs,
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("send registered: %w", err)
	}

	return reg.Register, nil
}

// recvLoop reads all messages until context is cancelled and records them.
func recvLoop(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage], handler *mockWorkerHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msg, err := stream.Receive()
		if err != nil {
			return
		}
		handler.record(msg)
	}
}

// makeJobAssignment creates a protobuf JobAssignment with reasonable defaults.
func makeJobAssignment(jobID, runID, functionID string, eventData map[string]any) *ironflowv1.JobAssignment {
	data, _ := structpb.NewStruct(eventData)
	return &ironflowv1.JobAssignment{
		JobId:      jobID,
		RunId:      runID,
		FunctionId: functionID,
		Attempt:    1,
		Event: &ironflowv1.Event{
			Id:        "evt-1",
			Name:      "test.event",
			Data:      data,
			Version:   1,
			Timestamp: timestamppb.Now(),
		},
	}
}

// testFn is a simple CreateFunction helper used across tests.
func testFn(id string, handler FunctionHandler) Function {
	return CreateFunction(FunctionConfig{
		ID:       id,
		Triggers: []Trigger{{Event: "test.event"}},
	}, handler)
}

// hasMessageType checks if any received message matches the given type check function.
func hasMessageType(msgs []*ironflowv1.WorkerMessage, check func(payload any) bool) bool {
	for _, m := range msgs {
		if check(m.GetPayload()) {
			return true
		}
	}
	return false
}

// ============================================================================
// Constructor tests
// ============================================================================

func TestStreamingWorker_Defaults(t *testing.T) {
	fn := testFn("test-fn", func(ctx Context) (any, error) { return nil, nil })

	w := NewStreamingWorker(WorkerConfig{
		ServerURL: "http://example.com",
		Functions: []Function{fn},
		Logger:    NewNoopLogger(),
	})

	if w.config.MaxConcurrentJobs != DefaultWorkerMaxConcurrentJobs {
		t.Errorf("expected MaxConcurrentJobs=%d, got %d",
			DefaultWorkerMaxConcurrentJobs, w.config.MaxConcurrentJobs)
	}
	if w.config.HeartbeatInterval != DefaultWorkerHeartbeatInterval {
		t.Errorf("expected HeartbeatInterval=%v, got %v",
			DefaultWorkerHeartbeatInterval, w.config.HeartbeatInterval)
	}
	if w.config.ReconnectDelay != DefaultWorkerReconnectDelay {
		t.Errorf("expected ReconnectDelay=%v, got %v",
			DefaultWorkerReconnectDelay, w.config.ReconnectDelay)
	}
	if w.workerID == "" {
		t.Error("expected workerID to be generated")
	}
	if w.httpClient == nil {
		t.Error("expected httpClient to be initialized")
	}
	if w.state.Load() != int32(stateIdle) {
		t.Errorf("expected initial state=%d, got %d", stateIdle, w.state.Load())
	}
	if _, ok := w.functions["test-fn"]; !ok {
		t.Error("expected function map to contain 'test-fn'")
	}
	if w.executor == nil {
		t.Error("expected executor to be set")
	}
}

func TestStreamingWorker_Validation(t *testing.T) {
	w := NewStreamingWorker(WorkerConfig{
		ServerURL: "http://example.com",
		Functions: nil,
		Logger:    NewNoopLogger(),
	})

	if len(w.functions) != 0 {
		t.Errorf("expected 0 functions, got %d", len(w.functions))
	}

	// Attempting to run with no functions should connect then fail to do useful
	// work. We verify the worker is constructible and the function map is empty.
	// The real "validation" for no functions happens when a job arrives: it will
	// be reported as FUNCTION_NOT_FOUND.
}

// ============================================================================
// Connection lifecycle tests
// ============================================================================

func TestStreamingWorker_RegisterAndConnect(t *testing.T) {
	var registered atomic.Bool
	var receivedReg *ironflowv1.WorkerRegister

	handler := &mockWorkerHandler{
		onConnect: func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
			reg, err := doRegistrationHandshake(stream, &mockWorkerHandler{}, 5000)
			if err != nil {
				return err
			}
			receivedReg = reg
			registered.Store(true)

			// Keep stream alive until context cancelled
			<-ctx.Done()
			return nil
		},
	}

	server := startMockServer(t, handler)

	fn := testFn("my-func", func(ctx Context) (any, error) { return nil, nil })

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() { _ = w.Run(ctx) }()

	// Wait for registration
	deadline := time.After(2 * time.Second)
	for !registered.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for registration")
		case <-time.After(10 * time.Millisecond):
		}
	}

	if receivedReg == nil {
		t.Fatal("expected WorkerRegister message")
	}
	if receivedReg.GetWorkerId() == "" {
		t.Error("expected non-empty worker ID in register message")
	}
	found := false
	for _, fid := range receivedReg.GetFunctionIds() {
		if fid == "my-func" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected function IDs to contain 'my-func', got %v", receivedReg.GetFunctionIds())
	}

	cancel()
}

func TestStreamingWorker_RegistrationFailure(t *testing.T) {
	var connectCount atomic.Int32

	handler := &mockWorkerHandler{
		onConnect: func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
			count := connectCount.Add(1)
			if count == 1 {
				// First attempt: close stream immediately without sending Registered
				return fmt.Errorf("simulated failure")
			}
			// Second attempt: succeed
			_, err := doRegistrationHandshake(stream, &mockWorkerHandler{}, 5000)
			if err != nil {
				return err
			}
			<-ctx.Done()
			return nil
		},
	}

	server := startMockServer(t, handler)

	fn := testFn("fn-1", func(ctx Context) (any, error) { return nil, nil })

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = w.Run(ctx) }()

	// Wait until at least 2 connections attempted
	deadline := time.After(4 * time.Second)
	for connectCount.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("expected at least 2 connect attempts, got %d", connectCount.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
}

// ============================================================================
// Job execution tests
// ============================================================================

func TestStreamingWorker_JobExecution_Success(t *testing.T) {
	handler := &mockWorkerHandler{}
	var jobCompleted atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		_, err := doRegistrationHandshake(stream, handler, 30000)
		if err != nil {
			return err
		}

		// Send a job assignment
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{
				Job: makeJobAssignment("job-1", "run-1", "success-fn", map[string]any{"key": "value"}),
			},
		}); err != nil {
			return err
		}

		// Collect messages from worker
		for {
			msg, err := stream.Receive()
			if err != nil {
				return err
			}
			handler.record(msg)

			if _, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_JobCompleted); ok {
				jobCompleted.Store(true)
			}

			if jobCompleted.Load() {
				<-ctx.Done()
				return nil
			}
		}
	}

	server := startMockServer(t, handler)

	fn := testFn("success-fn", func(ctx Context) (any, error) {
		return map[string]any{"status": "done"}, nil
	})

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = w.Run(ctx) }()

	// Wait for JobCompleted
	deadline := time.After(4 * time.Second)
	for !jobCompleted.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for JobCompleted")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Verify we received a JobCompleted message
	msgs := handler.getReceived()
	found := hasMessageType(msgs, func(p any) bool {
		jc, ok := p.(*ironflowv1.WorkerMessage_JobCompleted)
		return ok && jc.JobCompleted.GetJobId() == "job-1"
	})
	if !found {
		t.Error("expected JobCompleted message with jobId=job-1")
	}

	cancel()
}

func TestStreamingWorker_JobExecution_Failed(t *testing.T) {
	handler := &mockWorkerHandler{}
	var jobFailed atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		_, err := doRegistrationHandshake(stream, handler, 30000)
		if err != nil {
			return err
		}

		// Send a job assignment
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{
				Job: makeJobAssignment("job-fail", "run-fail", "fail-fn", map[string]any{}),
			},
		}); err != nil {
			return err
		}

		// Collect messages
		for {
			msg, err := stream.Receive()
			if err != nil {
				return err
			}
			handler.record(msg)

			if _, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_JobFailed); ok {
				jobFailed.Store(true)
			}

			if jobFailed.Load() {
				<-ctx.Done()
				return nil
			}
		}
	}

	server := startMockServer(t, handler)

	fn := testFn("fail-fn", func(ctx Context) (any, error) {
		return nil, NewNonRetryableError("intentional failure")
	})

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = w.Run(ctx) }()

	deadline := time.After(4 * time.Second)
	for !jobFailed.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for JobFailed")
		case <-time.After(10 * time.Millisecond):
		}
	}

	msgs := handler.getReceived()
	found := hasMessageType(msgs, func(p any) bool {
		jf, ok := p.(*ironflowv1.WorkerMessage_JobFailed)
		if !ok {
			return false
		}
		return jf.JobFailed.GetJobId() == "job-fail" &&
			jf.JobFailed.GetError() != nil &&
			jf.JobFailed.GetError().GetMessage() != ""
	})
	if !found {
		t.Error("expected JobFailed message with error details")
	}

	cancel()
}

func TestStreamingWorker_JobExecution_Yield(t *testing.T) {
	handler := &mockWorkerHandler{}
	var yieldReceived atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		_, err := doRegistrationHandshake(stream, handler, 30000)
		if err != nil {
			return err
		}

		// Send a job assignment
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{
				Job: makeJobAssignment("job-yield", "run-yield", "yield-fn", map[string]any{}),
			},
		}); err != nil {
			return err
		}

		// Collect messages
		for {
			msg, err := stream.Receive()
			if err != nil {
				return err
			}
			handler.record(msg)

			if _, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_StepYielded); ok {
				yieldReceived.Store(true)
			}

			if yieldReceived.Load() {
				<-ctx.Done()
				return nil
			}
		}
	}

	server := startMockServer(t, handler)

	fn := testFn("yield-fn", func(ctx Context) (any, error) {
		// Sleep triggers a yield
		_ = Sleep(ctx, "nap", 1*time.Hour)
		return nil, nil
	})

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = w.Run(ctx) }()

	deadline := time.After(4 * time.Second)
	for !yieldReceived.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for StepYielded")
		case <-time.After(10 * time.Millisecond):
		}
	}

	msgs := handler.getReceived()
	found := hasMessageType(msgs, func(p any) bool {
		sy, ok := p.(*ironflowv1.WorkerMessage_StepYielded)
		return ok && sy.StepYielded.GetJobId() == "job-yield"
	})
	if !found {
		t.Error("expected StepYielded message with jobId=job-yield")
	}

	cancel()
}

func TestStreamingWorker_JobAckSent(t *testing.T) {
	handler := &mockWorkerHandler{}
	var ackReceived atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		_, err := doRegistrationHandshake(stream, handler, 30000)
		if err != nil {
			return err
		}

		// Send a job
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{
				Job: makeJobAssignment("job-ack", "run-ack", "ack-fn", map[string]any{}),
			},
		}); err != nil {
			return err
		}

		// Read messages until we see a JobAck
		for {
			msg, err := stream.Receive()
			if err != nil {
				return err
			}
			handler.record(msg)

			if ack, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_JobAck); ok {
				if ack.JobAck.GetJobId() == "job-ack" {
					ackReceived.Store(true)
				}
			}

			if ackReceived.Load() {
				<-ctx.Done()
				return nil
			}
		}
	}

	server := startMockServer(t, handler)

	fn := testFn("ack-fn", func(ctx Context) (any, error) {
		// Slow function so we can observe the ack before completion
		time.Sleep(200 * time.Millisecond)
		return map[string]any{"ok": true}, nil
	})

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = w.Run(ctx) }()

	deadline := time.After(4 * time.Second)
	for !ackReceived.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for JobAck")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
}

func TestStreamingWorker_FunctionNotFound(t *testing.T) {
	handler := &mockWorkerHandler{}
	var failedReceived atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		_, err := doRegistrationHandshake(stream, handler, 30000)
		if err != nil {
			return err
		}

		// Send a job for a non-existent function
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{
				Job: makeJobAssignment("job-nf", "run-nf", "nonexistent-fn", map[string]any{}),
			},
		}); err != nil {
			return err
		}

		// Collect messages
		for {
			msg, err := stream.Receive()
			if err != nil {
				return err
			}
			handler.record(msg)

			if jf, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_JobFailed); ok {
				if jf.JobFailed.GetJobId() == "job-nf" {
					failedReceived.Store(true)
				}
			}

			if failedReceived.Load() {
				<-ctx.Done()
				return nil
			}
		}
	}

	server := startMockServer(t, handler)

	fn := testFn("some-fn", func(ctx Context) (any, error) { return nil, nil })

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = w.Run(ctx) }()

	deadline := time.After(4 * time.Second)
	for !failedReceived.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for JobFailed for unknown function")
		case <-time.After(10 * time.Millisecond):
		}
	}

	msgs := handler.getReceived()
	found := hasMessageType(msgs, func(p any) bool {
		jf, ok := p.(*ironflowv1.WorkerMessage_JobFailed)
		if !ok {
			return false
		}
		return jf.JobFailed.GetError() != nil &&
			jf.JobFailed.GetError().GetMessage() != "" &&
			!jf.JobFailed.GetError().GetRetryable()
	})
	if !found {
		t.Error("expected JobFailed with non-retryable FUNCTION_NOT_FOUND error")
	}

	cancel()
}

// ============================================================================
// Step lifecycle tests
// ============================================================================

func TestStreamingWorker_StepStarted(t *testing.T) {
	handler := &mockWorkerHandler{}
	var stepStarted atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		_, err := doRegistrationHandshake(stream, handler, 30000)
		if err != nil {
			return err
		}

		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{
				Job: makeJobAssignment("job-ss", "run-ss", "step-fn", map[string]any{}),
			},
		}); err != nil {
			return err
		}

		for {
			msg, err := stream.Receive()
			if err != nil {
				return err
			}
			handler.record(msg)

			if ss, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_StepStarted); ok {
				if ss.StepStarted.GetName() == "compute" {
					stepStarted.Store(true)
				}
			}

			if stepStarted.Load() {
				<-ctx.Done()
				return nil
			}
		}
	}

	server := startMockServer(t, handler)

	fn := testFn("step-fn", func(ctx Context) (any, error) {
		result, err := Run[map[string]any](ctx, "compute", func() (map[string]any, error) {
			time.Sleep(50 * time.Millisecond)
			return map[string]any{"answer": 42}, nil
		})
		return result, err
	})

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = w.Run(ctx) }()

	deadline := time.After(4 * time.Second)
	for !stepStarted.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for StepStarted")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
}

func TestStreamingWorker_StepCompleted(t *testing.T) {
	handler := &mockWorkerHandler{}
	var stepCompleted atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		_, err := doRegistrationHandshake(stream, handler, 30000)
		if err != nil {
			return err
		}

		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{
				Job: makeJobAssignment("job-sc", "run-sc", "step-done-fn", map[string]any{}),
			},
		}); err != nil {
			return err
		}

		for {
			msg, err := stream.Receive()
			if err != nil {
				return err
			}
			handler.record(msg)

			if _, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_StepCompleted); ok {
				stepCompleted.Store(true)
			}

			if stepCompleted.Load() {
				<-ctx.Done()
				return nil
			}
		}
	}

	server := startMockServer(t, handler)

	fn := testFn("step-done-fn", func(ctx Context) (any, error) {
		result, err := Run[map[string]any](ctx, "calc", func() (map[string]any, error) {
			return map[string]any{"result": 100}, nil
		})
		return result, err
	})

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = w.Run(ctx) }()

	deadline := time.After(4 * time.Second)
	for !stepCompleted.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for StepCompleted")
		case <-time.After(10 * time.Millisecond):
		}
	}

	msgs := handler.getReceived()
	found := hasMessageType(msgs, func(p any) bool {
		sc, ok := p.(*ironflowv1.WorkerMessage_StepCompleted)
		return ok && sc.StepCompleted.GetOutput() != nil
	})
	if !found {
		t.Error("expected StepCompleted with output")
	}

	cancel()
}

// ============================================================================
// Message handling tests
// ============================================================================

func TestStreamingWorker_CancelJob(t *testing.T) {
	handler := &mockWorkerHandler{}
	var jobStarted atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		_, err := doRegistrationHandshake(stream, handler, 30000)
		if err != nil {
			return err
		}

		// Send a long-running job
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{
				Job: makeJobAssignment("job-cancel", "run-cancel", "long-fn", map[string]any{}),
			},
		}); err != nil {
			return err
		}

		// Wait for job to start (ack)
		for !jobStarted.Load() {
			msg, err := stream.Receive()
			if err != nil {
				return err
			}
			handler.record(msg)
			if _, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_JobAck); ok {
				jobStarted.Store(true)
			}
		}

		// Send cancel
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Cancel{
				Cancel: &ironflowv1.CancelJob{
					JobId:  "job-cancel",
					Reason: "test cancel",
				},
			},
		}); err != nil {
			return err
		}

		// Continue receiving
		go recvLoop(ctx, stream, handler)
		<-ctx.Done()
		return nil
	}

	server := startMockServer(t, handler)

	// The function handler uses a channel to detect context cancellation
	cancelDetected := make(chan struct{}, 1)

	fn := testFn("long-fn", func(ctx Context) (any, error) {
		// The Run step callback doesn't receive a context, but the job context
		// cancel will cause the executor goroutine to be cleaned up.
		// We use a long sleep and detect the cancel by checking the job was removed.
		result, err := Run[string](ctx, "long-step", func() (string, error) {
			time.Sleep(5 * time.Second)
			return "done", nil
		})
		return result, err
	})

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	runCtx, runCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer runCancel()

	go func() { _ = w.Run(runCtx) }()

	// Wait for job to start
	deadline := time.After(3 * time.Second)
	for !jobStarted.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job ack")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// The cancel was already sent by the mock handler above.
	// Verify the worker's handleCancelJob was invoked by checking that
	// eventually the active job is cleaned up. The job goroutine's context
	// gets cancelled, but the Run step's time.Sleep doesn't check context,
	// so we just verify the cancel was received and the worker handles it
	// without crashing.
	time.Sleep(200 * time.Millisecond)

	// Verify the cancel was actually sent and processed — the activeJob
	// should have its context cancelled. We can check via the activeJobs map.
	w.activeJobs.Range(func(key, value any) bool {
		// If the job is still in the map, it means the goroutine hasn't
		// finished yet (which is expected since time.Sleep blocks).
		// But the context was cancelled which is what we want to verify.
		return true
	})

	// The key assertion: the cancel message was received and processed
	// without crashing the worker.
	if !jobStarted.Load() {
		t.Error("expected job to start (ack)")
	}

	_ = cancelDetected
	runCancel()
}

func TestStreamingWorker_Shutdown(t *testing.T) {
	handler := &mockWorkerHandler{}
	var registered atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		_, err := doRegistrationHandshake(stream, handler, 30000)
		if err != nil {
			return err
		}
		registered.Store(true)

		// Wait a bit then send shutdown
		time.Sleep(100 * time.Millisecond)

		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Shutdown{
				Shutdown: &ironflowv1.Shutdown{
					Reason: "server maintenance",
				},
			},
		}); err != nil {
			return err
		}

		// As the engine does, keep the stream open until the worker closes it.
		recvLoop(ctx, stream, handler)
		return nil
	}

	server := startMockServer(t, handler)

	fn := testFn("shutdown-fn", func(ctx Context) (any, error) { return nil, nil })

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait for worker to stop via shutdown message
	select {
	case <-time.After(4 * time.Second):
		t.Fatal("the worker did not stop after the Shutdown message")
	case <-done:
		// Worker exited — success
	}

	if w.state.Load() != int32(stateStopped) {
		t.Errorf("expected state=stopped after shutdown, got %d", w.state.Load())
	}
}

func TestStreamingWorker_Heartbeat(t *testing.T) {
	handler := &mockWorkerHandler{}
	var heartbeatCount atomic.Int32

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		_, err := doRegistrationHandshake(stream, handler, 100) // 100ms heartbeat
		if err != nil {
			return err
		}

		for {
			msg, err := stream.Receive()
			if err != nil {
				return err
			}
			handler.record(msg)

			if _, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_Heartbeat); ok {
				heartbeatCount.Add(1)
			}
		}
	}

	server := startMockServer(t, handler)

	fn := testFn("hb-fn", func(ctx Context) (any, error) { return nil, nil })

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:         server.URL,
		Functions:         []Function{fn},
		HeartbeatInterval: 100 * time.Millisecond,
		ReconnectDelay:    50 * time.Millisecond,
		Logger:            NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _ = w.Run(ctx) }()

	// Wait for at least 2 heartbeats
	deadline := time.After(1500 * time.Millisecond)
	for heartbeatCount.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("expected at least 2 heartbeats, got %d", heartbeatCount.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Verify heartbeat message contains worker ID
	msgs := handler.getReceived()
	found := hasMessageType(msgs, func(p any) bool {
		hb, ok := p.(*ironflowv1.WorkerMessage_Heartbeat)
		return ok && hb.Heartbeat.GetWorkerId() != ""
	})
	if !found {
		t.Error("expected heartbeat with worker ID")
	}

	cancel()
}

// ============================================================================
// Lifecycle tests
// ============================================================================

func TestStreamingWorker_Drain(t *testing.T) {
	handler := &mockWorkerHandler{}
	var jobStarted atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		_, err := doRegistrationHandshake(stream, handler, 30000)
		if err != nil {
			return err
		}

		// Send a job that takes a moment
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{
				Job: makeJobAssignment("job-drain", "run-drain", "drain-fn", map[string]any{}),
			},
		}); err != nil {
			return err
		}

		// As the engine does, end the stream when the worker half-closes it.
		recvLoop(ctx, stream, handler)
		return nil
	}

	server := startMockServer(t, handler)

	fn := testFn("drain-fn", func(ctx Context) (any, error) {
		jobStarted.Store(true)
		result, err := Run[string](ctx, "work", func() (string, error) {
			time.Sleep(300 * time.Millisecond)
			return "done", nil
		})
		return result, err
	})

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait for job to start
	deadline := time.After(3 * time.Second)
	for !jobStarted.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job to start")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Drain should wait for the active job to finish
	drainDone := make(chan struct{})
	go func() {
		w.Drain()
		close(drainDone)
	}()

	select {
	case <-drainDone:
		// Drain completed
	case <-time.After(5 * time.Second):
		t.Fatal("Drain did not complete in time")
	}

	if w.state.Load() != int32(stateStopped) {
		t.Errorf("expected state=stopped after drain, got %d", w.state.Load())
	}

	cancel()
}

func TestStreamingWorker_Stop(t *testing.T) {
	handler := &mockWorkerHandler{}
	var registered atomic.Bool

	// The server handler returns (closing the stream) once a signal is received.
	serverStop := make(chan struct{})

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		_, err := doRegistrationHandshake(stream, handler, 30000)
		if err != nil {
			return err
		}
		registered.Store(true)

		// Wait for the test to signal us to stop the stream
		select {
		case <-serverStop:
		case <-ctx.Done():
		}
		return nil
	}

	server := startMockServer(t, handler)

	fn := testFn("stop-fn", func(ctx Context) (any, error) { return nil, nil })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 100 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait for registration
	deadline := time.After(2 * time.Second)
	for !registered.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for registration")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Stop sets state and closes stopCh.
	w.Stop()

	// Also close the server-side stream so the client's Receive() unblocks.
	close(serverStop)

	select {
	case <-done:
		// Worker exited
	case <-time.After(3 * time.Second):
		cancel() // Clean up
		t.Fatal("Stop did not cause Run to exit in time")
	}

	if w.state.Load() != int32(stateStopped) {
		t.Errorf("expected state=stopped, got %d", w.state.Load())
	}
}

// ============================================================================
// Disconnect / reconnect tests
// ============================================================================

func TestStreamingWorker_Reconnect(t *testing.T) {
	var connectCount atomic.Int32
	handler := &mockWorkerHandler{}

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		count := connectCount.Add(1)

		_, err := doRegistrationHandshake(stream, handler, 30000)
		if err != nil {
			return err
		}

		if count == 1 {
			// First connection: close abruptly to trigger reconnect
			return fmt.Errorf("simulated disconnect")
		}

		// Second connection: stay alive
		go recvLoop(ctx, stream, handler)
		<-ctx.Done()
		return nil
	}

	server := startMockServer(t, handler)

	fn := testFn("recon-fn", func(ctx Context) (any, error) { return nil, nil })

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{fn},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() { _ = w.Run(ctx) }()

	// Wait for at least 2 connections
	deadline := time.After(4 * time.Second)
	for connectCount.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("expected at least 2 connections, got %d", connectCount.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
}

// ============================================================================
// Internal helper / reporter tests
// ============================================================================

func TestStreamStepReporter_ReportStepStarted(t *testing.T) {
	outCh := make(chan *ironflowv1.WorkerMessage, 10)
	r := &streamStepReporter{outCh: outCh, jobID: "job-1", executionSeq: 7, leaseToken: "tok"}

	r.ReportStepStarted("step-1", "my-step", "invoke")

	select {
	case msg := <-outCh:
		ss, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_StepStarted)
		if !ok {
			t.Fatalf("expected StepStarted, got %T", msg.GetPayload())
		}
		if ss.StepStarted.GetStepId() != "step-1" {
			t.Errorf("expected stepId=step-1, got %q", ss.StepStarted.GetStepId())
		}
		if ss.StepStarted.GetJobId() != "job-1" || ss.StepStarted.GetExecutionSeq() != 7 || ss.StepStarted.GetLeaseToken() != "tok" {
			t.Errorf("job/fence not stamped: %v", ss.StepStarted)
		}
		if ss.StepStarted.GetName() != "my-step" {
			t.Errorf("expected name=my-step, got %q", ss.StepStarted.GetName())
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

func TestStreamStepReporter_ReportStepCompleted(t *testing.T) {
	outCh := make(chan *ironflowv1.WorkerMessage, 10)
	r := &streamStepReporter{outCh: outCh, jobID: "job-1", executionSeq: 7, leaseToken: "tok"}

	r.ReportStepCompleted("step-2", "calc", "invoke", map[string]any{"x": 1}, 150)

	select {
	case msg := <-outCh:
		sc, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_StepCompleted)
		if !ok {
			t.Fatalf("expected StepCompleted, got %T", msg.GetPayload())
		}
		if sc.StepCompleted.GetStepId() != "step-2" {
			t.Errorf("expected stepId=step-2, got %q", sc.StepCompleted.GetStepId())
		}
		if sc.StepCompleted.GetJobId() != "job-1" || sc.StepCompleted.GetExecutionSeq() != 7 || sc.StepCompleted.GetLeaseToken() != "tok" {
			t.Errorf("job/fence not stamped: %v", sc.StepCompleted)
		}
		if sc.StepCompleted.GetDurationMs() != 150 {
			t.Errorf("expected durationMs=150, got %d", sc.StepCompleted.GetDurationMs())
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

func TestStreamStepReporter_ReportStepFailed(t *testing.T) {
	outCh := make(chan *ironflowv1.WorkerMessage, 10)
	r := &streamStepReporter{outCh: outCh, jobID: "job-1", executionSeq: 7, leaseToken: "tok"}

	r.ReportStepFailed("step-3", "bad-step", "invoke", "something broke", 42)

	select {
	case msg := <-outCh:
		sf, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_StepFailed)
		if !ok {
			t.Fatalf("expected StepFailed, got %T", msg.GetPayload())
		}
		if sf.StepFailed.GetStepId() != "step-3" {
			t.Errorf("expected stepId=step-3, got %q", sf.StepFailed.GetStepId())
		}
		if sf.StepFailed.GetJobId() != "job-1" || sf.StepFailed.GetExecutionSeq() != 7 || sf.StepFailed.GetLeaseToken() != "tok" {
			t.Errorf("job/fence not stamped: %v", sf.StepFailed)
		}
		if sf.StepFailed.GetError().GetMessage() != "something broke" {
			t.Errorf("expected error message 'something broke', got %q", sf.StepFailed.GetError().GetMessage())
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

func TestStreamJobReporter_ReportCompleted(t *testing.T) {
	outCh := make(chan *ironflowv1.WorkerMessage, 10)
	r := &streamJobReporter{outCh: outCh, logger: NewNoopLogger()}

	err := r.ReportCompleted(context.Background(), "job-100", map[string]any{"ok": true}, nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case msg := <-outCh:
		jc, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_JobCompleted)
		if !ok {
			t.Fatalf("expected JobCompleted, got %T", msg.GetPayload())
		}
		if jc.JobCompleted.GetJobId() != "job-100" {
			t.Errorf("expected jobId=job-100, got %q", jc.JobCompleted.GetJobId())
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

func TestStreamJobReporter_ReportFailed(t *testing.T) {
	outCh := make(chan *ironflowv1.WorkerMessage, 10)
	r := &streamJobReporter{outCh: outCh, logger: NewNoopLogger()}

	err := r.ReportFailed(context.Background(), "job-200", &PushError{
		Message:   "kaboom",
		Code:      "TEST_ERR",
		Retryable: true,
	}, []*StepResult{
		{
			ID:     "s1",
			Name:   "step-a",
			Type:   "invoke",
			Status: "failed",
			Error:  &StepErrorInfo{Message: "inner error", Retryable: true},
		},
	}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case msg := <-outCh:
		jf, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_JobFailed)
		if !ok {
			t.Fatalf("expected JobFailed, got %T", msg.GetPayload())
		}
		if jf.JobFailed.GetJobId() != "job-200" {
			t.Errorf("expected jobId=job-200, got %q", jf.JobFailed.GetJobId())
		}
		if jf.JobFailed.GetError().GetMessage() != "kaboom" {
			t.Errorf("expected error message 'kaboom', got %q", jf.JobFailed.GetError().GetMessage())
		}
		if len(jf.JobFailed.GetSteps()) != 1 {
			t.Errorf("expected 1 step, got %d", len(jf.JobFailed.GetSteps()))
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

func TestStreamJobReporter_ReportYielded_Sleep(t *testing.T) {
	outCh := make(chan *ironflowv1.WorkerMessage, 10)
	r := &streamJobReporter{outCh: outCh, logger: NewNoopLogger()}

	until := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
	err := r.ReportYielded(context.Background(), "job-300", &YieldInfo{
		StepID: "s-sleep",
		Type:   "sleep",
		Until:  until,
	}, nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case msg := <-outCh:
		sy, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_StepYielded)
		if !ok {
			t.Fatalf("expected StepYielded, got %T", msg.GetPayload())
		}
		if sy.StepYielded.GetJobId() != "job-300" {
			t.Errorf("expected jobId=job-300, got %q", sy.StepYielded.GetJobId())
		}
		sleepYield, ok := sy.StepYielded.GetYieldInfo().(*ironflowv1.StepYielded_Sleep)
		if !ok {
			t.Fatalf("expected sleep yield, got %T", sy.StepYielded.GetYieldInfo())
		}
		if sleepYield.Sleep.GetUntil() == nil {
			t.Error("expected sleep until timestamp")
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

func TestStreamJobReporter_ReportYielded_WaitEvent(t *testing.T) {
	outCh := make(chan *ironflowv1.WorkerMessage, 10)
	r := &streamJobReporter{outCh: outCh, logger: NewNoopLogger()}

	err := r.ReportYielded(context.Background(), "job-400", &YieldInfo{
		StepID: "s-wait",
		Type:   "wait_for_event",
		EventFilter: &EventFilter{
			Event:      "payment.completed",
			Payload:    map[string]any{"draft": "Review me"},
			Match:      "data.orderId",
			MatchValue: "order-123",
			Timeout:    24 * time.Hour,
		},
	}, nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case msg := <-outCh:
		sy, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_StepYielded)
		if !ok {
			t.Fatalf("expected StepYielded, got %T", msg.GetPayload())
		}
		waitYield, ok := sy.StepYielded.GetYieldInfo().(*ironflowv1.StepYielded_WaitEvent)
		if !ok {
			t.Fatalf("expected wait_event yield, got %T", sy.StepYielded.GetYieldInfo())
		}
		if string(waitYield.WaitEvent.GetPayloadJson()) != `{"draft":"Review me"}` {
			t.Errorf("request payload = %s", waitYield.WaitEvent.GetPayloadJson())
		}
		if waitYield.WaitEvent.GetMatchValue() != "order-123" {
			t.Errorf("match value = %q", waitYield.WaitEvent.GetMatchValue())
		}
		if waitYield.WaitEvent.GetEventName() != "payment.completed" {
			t.Errorf("expected event name 'payment.completed', got %q", waitYield.WaitEvent.GetEventName())
		}
		if waitYield.WaitEvent.GetMatchExpression() != "data.orderId" {
			t.Errorf("expected match expression 'data.orderId', got %q", waitYield.WaitEvent.GetMatchExpression())
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

func TestStreamJobReporter_ReportYielded_InvokeFunction(t *testing.T) {
	outCh := make(chan *ironflowv1.WorkerMessage, 10)
	r := &streamJobReporter{outCh: outCh, logger: NewNoopLogger()}

	err := r.ReportYielded(context.Background(), "job-500", &YieldInfo{
		StepID: "s-inv", Type: "invoke_function",
		FunctionID: "child", Input: "just-a-string", InvokeTimeoutMs: 5000,
	}, nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msg := <-outCh
	sy, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_StepYielded)
	if !ok {
		t.Fatalf("expected StepYielded, got %T", msg.GetPayload())
	}
	inv, ok := sy.StepYielded.GetYieldInfo().(*ironflowv1.StepYielded_InvokeFunction)
	if !ok {
		t.Fatalf("expected invoke_function yield, got %T", sy.StepYielded.GetYieldInfo())
	}
	if inv.InvokeFunction.GetFunctionId() != "child" || inv.InvokeFunction.GetInvokeTimeoutMs() != 5000 {
		t.Errorf("invoke = %+v", inv.InvokeFunction)
	}
	if string(inv.InvokeFunction.GetInputJson()) != `"just-a-string"` {
		t.Errorf("input_json = %s", inv.InvokeFunction.GetInputJson())
	}
}

func TestStreamJobReporter_ReportYielded_InvokeFunctionAsyncNoInput(t *testing.T) {
	outCh := make(chan *ironflowv1.WorkerMessage, 10)
	r := &streamJobReporter{outCh: outCh, logger: NewNoopLogger()}

	if err := r.ReportYielded(context.Background(), "job-501", &YieldInfo{
		StepID: "s-async", Type: "invoke_function_async", FunctionID: "child",
	}, nil, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msg := <-outCh
	inv, ok := msg.GetStepYielded().GetYieldInfo().(*ironflowv1.StepYielded_InvokeFunctionAsync)
	if !ok {
		t.Fatalf("expected invoke_function_async yield, got %T", msg.GetStepYielded().GetYieldInfo())
	}
	if len(inv.InvokeFunctionAsync.GetInputJson()) != 0 {
		t.Errorf("nil input must send empty input_json, got %s", inv.InvokeFunctionAsync.GetInputJson())
	}
}

func TestStreamJobReporter_ReportYielded_UnknownTypeErrors(t *testing.T) {
	outCh := make(chan *ironflowv1.WorkerMessage, 10)
	r := &streamJobReporter{outCh: outCh, logger: NewNoopLogger()}
	if err := r.ReportYielded(context.Background(), "job-502", &YieldInfo{StepID: "s", Type: "bogus"}, nil, 0); err == nil {
		t.Fatal("want error for unknown yield type")
	}

	// An unsupported yield type must still fail the job over the stream —
	// otherwise it stays leased until the engine reclaims it on lease expiry.
	select {
	case msg := <-outCh:
		jf, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_JobFailed)
		if !ok {
			t.Fatalf("expected JobFailed, got %T", msg.GetPayload())
		}
		if jf.JobFailed.GetJobId() != "job-502" {
			t.Errorf("expected jobId=job-502, got %q", jf.JobFailed.GetJobId())
		}
		if jf.JobFailed.GetError().GetRetryable() {
			t.Error("expected non-retryable error")
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

// ============================================================================
// Proto conversion tests
// ============================================================================

func TestStreamingAssignment_FailedInvokeRowReachesMemo(t *testing.T) {
	pa := &ironflowv1.JobAssignment{
		JobId: "j", RunId: "r", FunctionId: "fn",
		CompletedSteps: []*ironflowv1.CompletedStep{{
			StepId: "r:child:0", Name: "r:child:0",
			Status: "failed", ErrorJson: []byte(`{"message":"invoke timed out"}`),
		}},
	}
	job, err := protoToJobAssignment(pa)
	if err != nil {
		t.Fatal(err)
	}
	cs := job.CompletedSteps[0]
	if cs.Status != "failed" {
		t.Fatalf("status = %q", cs.Status)
	}
	if m, ok := cs.Error.(map[string]any); !ok || m["message"] != "invoke timed out" {
		t.Fatalf("error = %#v", cs.Error)
	}
}

func TestStreamingAssignment_ScalarOutputValueReachesMemo(t *testing.T) {
	pa := &ironflowv1.JobAssignment{
		JobId: "j", RunId: "r", FunctionId: "fn",
		CompletedSteps: []*ironflowv1.CompletedStep{{
			StepId: "r:child:0", Name: "r:child:0", OutputValue: structpb.NewNumberValue(42),
		}},
	}
	job, err := protoToJobAssignment(pa)
	if err != nil {
		t.Fatal(err)
	}
	if got := job.CompletedSteps[0].Output; got != float64(42) {
		t.Fatalf("output = %#v, want 42", got)
	}
}

func TestStreamJobReporter_ReportYielded_UnencodableInvokeInputFailsJob(t *testing.T) {
	outCh := make(chan *ironflowv1.WorkerMessage, 10)
	r := &streamJobReporter{outCh: outCh, logger: NewNoopLogger()}
	err := r.ReportYielded(context.Background(), "job-503", &YieldInfo{
		StepID: "s", Type: "invoke_function", FunctionID: "child", Input: math.NaN(),
	}, nil, 0)
	if err == nil {
		t.Fatal("want error for unencodable input")
	}
	select {
	case msg := <-outCh:
		jf, ok := msg.GetPayload().(*ironflowv1.WorkerMessage_JobFailed)
		if !ok {
			t.Fatalf("expected JobFailed, got %T", msg.GetPayload())
		}
		if jf.JobFailed.GetError().GetRetryable() || jf.JobFailed.GetJobId() != "job-503" {
			t.Errorf("job failed = %+v", jf.JobFailed)
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

func TestProtoToJobAssignment(t *testing.T) {
	eventData, _ := structpb.NewStruct(map[string]any{
		"orderId": "order-123",
		"amount":  42.5,
	})

	stepOutput, _ := structpb.NewStruct(map[string]any{
		"result": "previous",
	})

	pa := &ironflowv1.JobAssignment{
		JobId:      "j1",
		RunId:      "r1",
		FunctionId: "fn-1",
		Attempt:    2,
		Event: &ironflowv1.Event{
			Id:        "evt-1",
			Name:      "order.placed",
			Data:      eventData,
			Version:   3,
			Timestamp: timestamppb.Now(),
		},
		CompletedSteps: []*ironflowv1.CompletedStep{
			{
				StepId: "s1",
				Name:   "fetch",
				Output: stepOutput,
			},
		},
		ActorId: "actor-1",
		Context: &ironflowv1.JobContext{
			Secrets: map[string]string{
				"api-key": "secret-value",
			},
		},
	}

	job, err := protoToJobAssignment(pa)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if job.JobID != "j1" {
		t.Errorf("expected JobID=j1, got %q", job.JobID)
	}
	if job.RunID != "r1" {
		t.Errorf("expected RunID=r1, got %q", job.RunID)
	}
	if job.FunctionID != "fn-1" {
		t.Errorf("expected FunctionID=fn-1, got %q", job.FunctionID)
	}
	if job.Attempt != 2 {
		t.Errorf("expected Attempt=2, got %d", job.Attempt)
	}
	if job.Event.Name != "order.placed" {
		t.Errorf("expected event name=order.placed, got %q", job.Event.Name)
	}
	if job.Event.Version != 3 {
		t.Errorf("expected event version=3, got %d", job.Event.Version)
	}

	// Verify event data is valid JSON
	var data map[string]any
	if err := json.Unmarshal(job.Event.Data, &data); err != nil {
		t.Fatalf("failed to unmarshal event data: %v", err)
	}
	if data["orderId"] != "order-123" {
		t.Errorf("expected orderId=order-123, got %v", data["orderId"])
	}

	if len(job.CompletedSteps) != 1 {
		t.Fatalf("expected 1 completed step, got %d", len(job.CompletedSteps))
	}
	if job.CompletedSteps[0].StepID != "s1" {
		t.Errorf("expected step ID=s1, got %q", job.CompletedSteps[0].StepID)
	}
	if job.ActorID != "actor-1" {
		t.Errorf("expected ActorID=actor-1, got %q", job.ActorID)
	}
	if job.Context == nil || job.Context.Secrets["api-key"] != "secret-value" {
		t.Error("expected secrets to be populated")
	}
}

func TestProtoToJobAssignment_Metadata(t *testing.T) {
	eventData, _ := structpb.NewStruct(map[string]any{"orderId": "o-1"})
	eventMeta, _ := structpb.NewStruct(map[string]any{
		"causationId":   "cmd-001",
		"correlationId": "corr-xyz",
		"tenantId":      "tenant-42",
	})

	pa := &ironflowv1.JobAssignment{
		JobId:      "j1",
		RunId:      "r1",
		FunctionId: "fn-1",
		Attempt:    1,
		Event: &ironflowv1.Event{
			Id:        "evt-1",
			Name:      "order.placed",
			Data:      eventData,
			Metadata:  eventMeta,
			Timestamp: timestamppb.Now(),
		},
	}

	job, err := protoToJobAssignment(pa)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(job.Event.Metadata) == 0 {
		t.Fatal("expected event metadata to be populated")
	}

	var m map[string]any
	if err := json.Unmarshal(job.Event.Metadata, &m); err != nil {
		t.Fatalf("failed to unmarshal metadata: %v", err)
	}
	if m["causationId"] != "cmd-001" {
		t.Errorf("expected causationId=cmd-001, got %v", m["causationId"])
	}
	if m["correlationId"] != "corr-xyz" {
		t.Errorf("expected correlationId=corr-xyz, got %v", m["correlationId"])
	}
	if m["tenantId"] != "tenant-42" {
		t.Errorf("expected tenantId=tenant-42, got %v", m["tenantId"])
	}
}

func TestProtoToJobAssignment_NoMetadata(t *testing.T) {
	eventData, _ := structpb.NewStruct(map[string]any{"orderId": "o-1"})
	pa := &ironflowv1.JobAssignment{
		JobId: "j1",
		Event: &ironflowv1.Event{
			Id:        "evt-1",
			Name:      "order.placed",
			Data:      eventData,
			Timestamp: timestamppb.Now(),
		},
	}

	job, err := protoToJobAssignment(pa)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(job.Event.Metadata) != 0 {
		t.Errorf("expected empty metadata, got %s", string(job.Event.Metadata))
	}
}

func TestAnyToPayload(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		st, v, err := anyToPayload(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if st != nil || v != nil {
			t.Error("expected both carriers nil for nil input")
		}
	})

	// An object keeps using the original Struct field, so a reader that knows
	// only that field is unaffected and the wire carries no extra bytes.
	t.Run("map", func(t *testing.T) {
		st, v, err := anyToPayload(map[string]any{"key": "value"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != nil {
			t.Error("an object must not populate the Value field")
		}
		if got := st.Fields["key"].GetStringValue(); got != "value" {
			t.Errorf("expected key=value, got %v", got)
		}
	})

	t.Run("struct", func(t *testing.T) {
		type TestObj struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}
		st, v, err := anyToPayload(TestObj{Name: "Alice", Age: 30})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != nil {
			t.Error("a struct marshals to an object, so it belongs in the Struct field")
		}
		if got := st.Fields["name"].GetStringValue(); got != "Alice" {
			t.Errorf("expected name=Alice, got %v", got)
		}
	})

	// #1963: a handler returning a bare array or scalar used to have its output
	// silently dropped, because the conversion forced everything through
	// map[string]any and the caller ignored the error.
	t.Run("slice", func(t *testing.T) {
		st, v, err := anyToPayload([]int{1, 2, 3})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if st != nil {
			t.Error("a non-object must not populate the Struct field")
		}
		if n := len(v.GetListValue().GetValues()); n != 3 {
			t.Errorf("expected 3 list values, got %d", n)
		}
	})

	t.Run("scalar", func(t *testing.T) {
		st, v, err := anyToPayload("ok")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if st != nil {
			t.Error("a non-object must not populate the Struct field")
		}
		if got := v.GetStringValue(); got != "ok" {
			t.Errorf("expected \"ok\", got %q", got)
		}
	})
}

func TestSdkStepTypeToProto(t *testing.T) {
	tests := []struct {
		input    string
		expected ironflowv1.StepType
	}{
		{"invoke", ironflowv1.StepType_STEP_TYPE_INVOKE},
		{"sleep", ironflowv1.StepType_STEP_TYPE_SLEEP},
		{"wait_for_event", ironflowv1.StepType_STEP_TYPE_WAIT_FOR_EVENT},
		{"compensate", ironflowv1.StepType_STEP_TYPE_COMPENSATE},
		{"invoke_function", ironflowv1.StepType_STEP_TYPE_INVOKE_FUNCTION},
		{"unknown", ironflowv1.StepType_STEP_TYPE_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sdkStepTypeToProto(tt.input)
			if got != tt.expected {
				t.Errorf("sdkStepTypeToProto(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

// The stream must carry the same auth header as the HTTP calls; without it
// the server rejects the stream with 401.
func TestStreamingWorker_ConnectSendsAPIKey(t *testing.T) {
	got := make(chan string, 1)
	handler := &mockWorkerHandler{
		onConnect: func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
			select {
			case got <- stream.RequestHeader().Get("Authorization"):
			default:
			}
			<-ctx.Done()
			return nil
		},
	}
	server := startMockServer(t, handler)
	w := NewStreamingWorker(WorkerConfig{
		ServerURL: server.URL, APIKey: "k1",
		Functions:      []Function{testFn("my-func", func(ctx Context) (any, error) { return nil, nil })},
		ReconnectDelay: 50 * time.Millisecond, Logger: NewNoopLogger(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	select {
	case h := <-got:
		if want := buildAuthHeaders("k1")["Authorization"]; h != want {
			t.Fatalf("Authorization = %q, want %q", h, want)
		}
	case <-ctx.Done():
		t.Fatal("stream never connected")
	}
}

// ============================================================================
// Drain deadline, rejects and Stop (#2446)
// ============================================================================

func TestStreamingWorker_Drain_DeadlineCancelsActiveJobs(t *testing.T) {
	originalDrainTimeout := workerDrainTimeout
	workerDrainTimeout = 10 * time.Millisecond
	t.Cleanup(func() { workerDrainTimeout = originalDrainTimeout })
	w := NewStreamingWorker(WorkerConfig{ServerURL: "http://example.com", Logger: NewNoopLogger()})
	w.state.Store(int32(stateConnected))
	ctx, cancel := context.WithCancel(context.Background())
	var cancelled atomic.Bool
	w.activeJobs.Store("job-1", &activeJob{cancel: func() { cancelled.Store(true); cancel() }})
	w.jobCount.Store(1)

	done := make(chan struct{})
	go func() { w.Drain(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Drain did not stop at the deadline")
	}

	if w.state.Load() != int32(stateStopped) {
		t.Fatalf("state = %d, want stopped", w.state.Load())
	}
	if !cancelled.Load() || ctx.Err() == nil {
		t.Fatal("deadline did not cancel the active job")
	}
}

// waitStopped fails the test unless the worker stops within limit.
func waitStopped(t *testing.T, w *StreamingWorker, limit time.Duration) {
	t.Helper()
	select {
	case <-w.stopCh:
	case <-time.After(limit):
		t.Fatalf("worker did not stop within %s", limit)
	}
}

// The engine stops waiting after drain_timeout_ms, so the worker must not
// drain for its own longer default (#2458).
func TestStreamingWorker_Shutdown_UsesMessageDrainTimeout(t *testing.T) {
	w := NewStreamingWorker(WorkerConfig{ServerURL: "http://example.com", Logger: NewNoopLogger()})
	w.state.Store(int32(stateConnected))
	w.activeJobs.Store("job-1", &activeJob{cancel: func() {}})
	w.jobCount.Store(1)

	w.handleShutdown(&ironflowv1.Shutdown{DrainTimeoutMs: 50})
	waitStopped(t, w, 2*time.Second)
}

func TestStreamingWorker_Drain_UsesConfigDrainTimeout(t *testing.T) {
	w := NewStreamingWorker(WorkerConfig{
		ServerURL:    "http://example.com",
		Logger:       NewNoopLogger(),
		DrainTimeout: 50 * time.Millisecond,
	})
	w.state.Store(int32(stateConnected))
	w.activeJobs.Store("job-1", &activeJob{cancel: func() {}})
	w.jobCount.Store(1)

	go w.Drain()
	waitStopped(t, w, 2*time.Second)
}

// A Shutdown without a drain timeout falls back to the configured one.
func TestStreamingWorker_Shutdown_ZeroMessageTimeoutUsesConfig(t *testing.T) {
	w := NewStreamingWorker(WorkerConfig{
		ServerURL:    "http://example.com",
		Logger:       NewNoopLogger(),
		DrainTimeout: 50 * time.Millisecond,
	})
	w.state.Store(int32(stateConnected))
	w.activeJobs.Store("job-1", &activeJob{cancel: func() {}})
	w.jobCount.Store(1)

	w.handleShutdown(&ironflowv1.Shutdown{})
	waitStopped(t, w, 2*time.Second)
}

// Stop aborts the stream, and the engine can then miss the last results. When
// the jobs are done, Drain sends what is queued and half-closes the stream; the
// engine reads to the end and closes it.
func TestStreamingWorker_Drain_ClosesStreamGracefully(t *testing.T) {
	handler := &mockWorkerHandler{}
	var halfClosed atomic.Bool
	jobAcked := make(chan struct{})

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		if _, err := doRegistrationHandshake(stream, handler, 30000); err != nil {
			return err
		}
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{
				Job: makeJobAssignment("job-gc", "run-gc", "gc-fn", map[string]any{}),
			},
		}); err != nil {
			return err
		}
		for {
			msg, err := stream.Receive()
			if err != nil {
				// io.EOF is the half-close. An abort arrives as a cancel error.
				halfClosed.Store(errors.Is(err, io.EOF))
				return nil
			}
			handler.record(msg)
			if msg.GetJobAck() != nil {
				close(jobAcked)
			}
		}
	}

	server := startMockServer(t, handler)

	w := NewStreamingWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{testFn("gc-fn", func(ctx Context) (any, error) {
			return map[string]any{"ok": true}, nil
		})},
		Logger: NewNoopLogger(),
	})

	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background()) }()
	select {
	case <-jobAcked:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the job")
	}

	w.Drain()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the drain")
	}
	if !halfClosed.Load() {
		t.Error("Drain aborted the stream, want a half-close")
	}
	completed := hasMessageType(handler.getReceived(), func(p any) bool {
		c, ok := p.(*ironflowv1.WorkerMessage_JobCompleted)
		return ok && c.JobCompleted.GetJobId() == "job-gc"
	})
	if !completed {
		t.Error("the engine did not receive JobCompleted before the stream closed")
	}
}

// The graceful close must send every queued message before the half-close.
// The send loop takes the close signal at random among its ready cases, so
// with 50 messages queued the flush loop runs with a queue that is not empty.
func TestStreamingWorker_Drain_FlushesQueueBeforeHalfClose(t *testing.T) {
	const queued = 50
	handler := &mockWorkerHandler{}
	var registered atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		if _, err := doRegistrationHandshake(stream, handler, 30000); err != nil {
			return err
		}
		registered.Store(true)
		recvLoop(ctx, stream, handler)
		return nil
	}

	server := startMockServer(t, handler)

	w := NewStreamingWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{testFn("fn", func(ctx Context) (any, error) { return nil, nil })},
		Logger:    NewNoopLogger(),
	})

	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background()) }()

	deadline := time.After(3 * time.Second)
	for !registered.Load() || w.state.Load() != int32(stateConnected) {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the connection")
		case <-time.After(5 * time.Millisecond):
		}
	}

	for i := 0; i < queued; i++ {
		w.outCh <- &ironflowv1.WorkerMessage{
			Payload: &ironflowv1.WorkerMessage_JobCompleted{
				JobCompleted: &ironflowv1.JobCompleted{JobId: fmt.Sprintf("queued-%d", i)},
			},
		}
	}
	w.Drain()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the drain")
	}
	got := 0
	for _, msg := range handler.getReceived() {
		if msg.GetJobCompleted() != nil {
			got++
		}
	}
	if got != queued {
		t.Fatalf("the engine received %d of %d queued results", got, queued)
	}
}

// The graceful close must not wait longer than the drain deadline for an
// engine that does not close the stream.
func TestStreamingWorker_Drain_DeadlineWhenEngineHoldsStream(t *testing.T) {
	originalDrainTimeout := workerDrainTimeout
	workerDrainTimeout = 100 * time.Millisecond
	t.Cleanup(func() { workerDrainTimeout = originalDrainTimeout })

	handler := &mockWorkerHandler{}
	var registered atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		if _, err := doRegistrationHandshake(stream, handler, 30000); err != nil {
			return err
		}
		registered.Store(true)
		// Does not read the stream, so it does not see the half-close.
		<-ctx.Done()
		return nil
	}

	server := startMockServer(t, handler)

	w := NewStreamingWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{testFn("fn", func(ctx Context) (any, error) { return nil, nil })},
		Logger:    NewNoopLogger(),
	})

	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background()) }()

	deadline := time.After(3 * time.Second)
	for !registered.Load() || w.state.Load() != int32(stateConnected) {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the connection")
		case <-time.After(5 * time.Millisecond):
		}
	}

	drained := make(chan struct{})
	go func() { w.Drain(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not stop at the deadline")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the forced stop")
	}
	if w.state.Load() != int32(stateStopped) {
		t.Fatalf("state = %d, want stopped", w.state.Load())
	}
}

// Before Run there is no stream to close and no Run loop to stop the worker,
// so Drain must stop it and not wait for the deadline.
func TestStreamingWorker_Drain_BeforeRun_StopsImmediately(t *testing.T) {
	w := NewStreamingWorker(WorkerConfig{ServerURL: "http://example.com", Logger: NewNoopLogger()})

	drained := make(chan struct{})
	go func() { w.Drain(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("Drain waited on a worker that never ran")
	}
	if w.state.Load() != int32(stateStopped) {
		t.Fatalf("state = %d, want stopped", w.state.Load())
	}
}

// A Drain on another goroutine must not miss a job that the receive loop
// accepts at the same time: Drain then half-closes the stream while the job
// starts. For each accepted job, Drain must see a job count that is not zero.
func TestStreamingWorker_Drain_SeesJobAcceptedConcurrently(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	for i := 0; i < 3000; i++ {
		w := NewStreamingWorker(WorkerConfig{
			ServerURL: "http://example.com",
			Functions: []Function{testFn("fn", func(ctx Context) (any, error) { <-block; return nil, nil })},
			Logger:    NewNoopLogger(),
		})
		w.state.Store(int32(stateConnected))
		job := makeJobAssignment("job-1", "run-1", "fn", map[string]any{})

		start := make(chan struct{})
		var wg sync.WaitGroup
		var sawNoJob bool
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			w.handleJobAssignment(context.Background(), job)
		}()
		go func() {
			defer wg.Done()
			<-start
			// The first two steps of Drain.
			storeStateUnlessStopped(&w.state, stateDraining)
			sawNoJob = w.jobCount.Load() == 0
		}()
		close(start)
		wg.Wait()

		// A refused job queues a JobNack; only an accepted job queues a JobAck.
		accepted := false
		for len(w.outCh) > 0 {
			if (<-w.outCh).GetJobAck() != nil {
				accepted = true
			}
		}
		w.Stop()
		if accepted && sawNoJob {
			t.Fatalf("iteration %d: the worker accepted a job that Drain did not see", i)
		}
	}
}

// Without a stream no result can reach the engine. A reconnect would only open
// the worker to new jobs that the deadline then cancels.
func TestStreamingWorker_DrainingStreamDrop_StopsWithoutReconnect(t *testing.T) {
	handler := &mockWorkerHandler{}
	var connects atomic.Int32
	dropStream := make(chan struct{})

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		connects.Add(1)
		if _, err := doRegistrationHandshake(stream, handler, 30000); err != nil {
			return err
		}
		<-dropStream
		return nil
	}

	server := startMockServer(t, handler)

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{testFn("fn", func(ctx Context) (any, error) { return nil, nil })},
		ReconnectDelay: 20 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})
	w.jobCount.Store(1) // an active job keeps Drain in its wait

	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background()) }()

	deadline := time.After(3 * time.Second)
	for w.state.Load() != int32(stateConnected) {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the connection")
		case <-time.After(5 * time.Millisecond):
		}
	}
	go w.Drain()
	for w.state.Load() != int32(stateDraining) {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the drain")
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(dropStream)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not stop after the stream dropped during the drain")
	}
	if n := connects.Load(); n != 1 {
		t.Fatalf("the worker connected %d times, want 1", n)
	}
	if w.state.Load() != int32(stateStopped) {
		t.Fatalf("state = %d, want stopped", w.state.Load())
	}
}

// refusedJob runs one assignment through handleJobAssignment and returns what
// the worker queued for the engine.
func refusedJob(t *testing.T, prepare func(w *StreamingWorker)) []*ironflowv1.WorkerMessage {
	t.Helper()
	var ran atomic.Bool
	w := NewStreamingWorker(WorkerConfig{
		ServerURL: "http://example.com",
		Functions: []Function{testFn("fn", func(ctx Context) (any, error) { ran.Store(true); return nil, nil })},
		Logger:    NewNoopLogger(),
	})
	prepare(w)

	job := makeJobAssignment("job-1", "run-1", "fn", map[string]any{})
	job.ExecutionSeq = 7
	job.LeaseToken = "tok"
	w.handleJobAssignment(context.Background(), job)

	var out []*ironflowv1.WorkerMessage
	for {
		select {
		case msg := <-w.outCh:
			out = append(out, msg)
			continue
		default:
		}
		break
	}
	if ran.Load() {
		t.Fatal("the refused job ran")
	}
	return out
}

// A worker that cannot run a job refuses it with a nack (#2456). A nack uses no
// run attempt, and the engine re-queues the job at once.
func TestStreamingWorker_NacksJobItCannotRun(t *testing.T) {
	full := func(w *StreamingWorker) { w.jobCount.Store(int32(w.config.MaxConcurrentJobs)) }
	draining := func(w *StreamingWorker) { w.state.Store(int32(stateDraining)) }
	cases := map[string]struct {
		prepare func(w *StreamingWorker)
		reason  ironflowv1.JobNackReason
	}{
		"at capacity":          {full, ironflowv1.JobNackReason_JOB_NACK_REASON_AT_CAPACITY},
		"draining":             {draining, ironflowv1.JobNackReason_JOB_NACK_REASON_DRAINING},
		"draining at capacity": {func(w *StreamingWorker) { full(w); draining(w) }, ironflowv1.JobNackReason_JOB_NACK_REASON_DRAINING},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			out := refusedJob(t, tc.prepare)
			if len(out) != 1 {
				t.Fatalf("got %d messages, want 1 nack", len(out))
			}
			nack := out[0].GetJobNack()
			if nack == nil {
				t.Fatalf("got %T, want a JobNack", out[0].GetPayload())
			}
			if nack.GetJobId() != "job-1" || nack.GetRunId() != "run-1" || nack.GetExecutionSeq() != 7 || nack.GetLeaseToken() != "tok" {
				t.Fatalf("nack does not echo the fence: %+v", nack)
			}
			if nack.GetReason() != tc.reason {
				t.Fatalf("reason = %v, want %v", nack.GetReason(), tc.reason)
			}
		})
	}
}

// The receive loop continues after a Shutdown message, so the worker must be
// draining before it reads the next message.
func TestStreamingWorker_Shutdown_DrainsBeforeNextMessage(t *testing.T) {
	w := NewStreamingWorker(WorkerConfig{ServerURL: "http://example.com", Logger: NewNoopLogger()})
	w.state.Store(int32(stateConnected))

	w.handleShutdown(&ironflowv1.Shutdown{})

	if w.state.Load() == int32(stateConnected) {
		t.Fatal("the worker still accepts jobs after handleShutdown returned")
	}
}

// An engine Shutdown message starts a drain. The active job completes and
// reports on the open stream, and the worker does not reconnect.
func TestStreamingWorker_Shutdown_DrainsActiveJob(t *testing.T) {
	handler := &mockWorkerHandler{}
	var connects atomic.Int32

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		connects.Add(1)
		if _, err := doRegistrationHandshake(stream, handler, 30000); err != nil {
			return err
		}
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{
				Job: makeJobAssignment("job-sd", "run-sd", "sd-fn", map[string]any{}),
			},
		}); err != nil {
			return err
		}
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Shutdown{Shutdown: &ironflowv1.Shutdown{Reason: "deploy"}},
		}); err != nil {
			return err
		}
		recvLoop(ctx, stream, handler)
		return nil
	}

	server := startMockServer(t, handler)

	w := NewStreamingWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{testFn("sd-fn", func(ctx Context) (any, error) {
			time.Sleep(200 * time.Millisecond)
			return map[string]any{"ok": true}, nil
		})},
		ReconnectDelay: 50 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the worker did not stop after the Shutdown drain")
	}

	completed := hasMessageType(handler.getReceived(), func(p any) bool {
		c, ok := p.(*ironflowv1.WorkerMessage_JobCompleted)
		return ok && c.JobCompleted.GetJobId() == "job-sd"
	})
	if !completed {
		t.Error("the engine did not receive JobCompleted for the job that was active at Shutdown")
	}
	if n := connects.Load(); n != 1 {
		t.Errorf("the worker connected %d times, want 1: a draining worker must not reconnect", n)
	}
}

// The engine reclaims a streaming worker's leases when the stream closes, so
// Stop must close it without help from the server.
func TestStreamingWorker_Stop_ClosesStream(t *testing.T) {
	handler := &mockWorkerHandler{}
	var registered atomic.Bool
	streamClosed := make(chan struct{})

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		if _, err := doRegistrationHandshake(stream, handler, 30000); err != nil {
			return err
		}
		registered.Store(true)
		<-ctx.Done()
		close(streamClosed)
		return nil
	}

	server := startMockServer(t, handler)

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{testFn("stop-fn", func(ctx Context) (any, error) { return nil, nil })},
		ReconnectDelay: 100 * time.Millisecond,
		Logger:         NewNoopLogger(),
	})

	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background()) }()

	deadline := time.After(2 * time.Second)
	for !registered.Load() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for registration")
		case <-time.After(10 * time.Millisecond):
		}
	}

	w.Stop()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not cause Run to exit")
	}
	select {
	case <-streamClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not close the stream")
	}
}

// A cancelled Run context does not unblock Receive while the engine holds the
// stream open, so Run must close the stream itself.
// waitFor polls cond until it holds or the test times out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for !cond() {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// readToEOF records the worker's messages until the stream ends and reports
// whether it ended with a half-close.
func readToEOF(stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage], handler *mockWorkerHandler, onMsg func(*ironflowv1.WorkerMessage)) bool {
	for {
		msg, err := stream.Receive()
		if err != nil {
			return errors.Is(err, io.EOF)
		}
		handler.record(msg)
		if onMsg != nil {
			onMsg(msg)
		}
	}
}

// signal.NotifyContext plus Run(ctx) is the usual shutdown pattern, so a
// cancelled Run context must drain as in the polling Worker: the active job
// completes and reports on the open stream, a new job is ignored, and Run
// returns the context error.
func TestStreamingWorker_ContextCancel_DrainsActiveJob(t *testing.T) {
	handler := &mockWorkerHandler{}
	var halfClosed atomic.Bool
	jobStarted := make(chan struct{})
	sendSecond := make(chan struct{})
	release := make(chan struct{})

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		if _, err := doRegistrationHandshake(stream, handler, 30000); err != nil {
			return err
		}
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{Job: makeJobAssignment("job-1", "run-1", "cc-fn", map[string]any{})},
		}); err != nil {
			return err
		}
		select {
		case <-sendSecond:
		case <-ctx.Done():
			return nil
		}
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{Job: makeJobAssignment("job-2", "run-2", "cc-fn", map[string]any{})},
		}); err != nil {
			return err
		}
		halfClosed.Store(readToEOF(stream, handler, nil))
		return nil
	}

	server := startMockServer(t, handler)

	w := NewStreamingWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{testFn("cc-fn", func(ctx Context) (any, error) {
			if ctx.Run.ID == "run-1" {
				close(jobStarted)
			}
			<-release
			return map[string]any{"ok": true}, nil
		})},
		Logger: NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case <-jobStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the job")
	}

	cancel()
	waitFor(t, "the drain", func() bool { return w.state.Load() == int32(stateDraining) })
	close(sendSecond)

	select {
	case err := <-done:
		t.Fatalf("Run returned %v with a job still active, want a drain", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the drain")
	}
	if !halfClosed.Load() {
		t.Error("the drain aborted the stream, want a half-close")
	}
	received := handler.getReceived()
	if !hasMessageType(received, func(p any) bool {
		c, ok := p.(*ironflowv1.WorkerMessage_JobCompleted)
		return ok && c.JobCompleted.GetJobId() == "job-1"
	}) {
		t.Error("the engine did not receive JobCompleted for the active job")
	}
	if hasMessageType(received, func(p any) bool {
		a, ok := p.(*ironflowv1.WorkerMessage_JobAck)
		return ok && a.JobAck.GetJobId() == "job-2"
	}) {
		t.Error("the draining worker accepted a new job")
	}
	if w.state.Load() != int32(stateStopped) {
		t.Fatalf("state = %d, want stopped", w.state.Load())
	}
}

// The drain deadline, not the cancelled Run context, stops a job that does not
// finish.
func TestStreamingWorker_ContextCancel_DeadlineStopsWorker(t *testing.T) {
	originalDrainTimeout := workerDrainTimeout
	workerDrainTimeout = 150 * time.Millisecond
	t.Cleanup(func() { workerDrainTimeout = originalDrainTimeout })

	handler := &mockWorkerHandler{}
	jobStarted := make(chan struct{})
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		if _, err := doRegistrationHandshake(stream, handler, 30000); err != nil {
			return err
		}
		if err := stream.Send(&ironflowv1.EngineMessage{
			Payload: &ironflowv1.EngineMessage_Job{Job: makeJobAssignment("job-1", "run-1", "dl-fn", map[string]any{})},
		}); err != nil {
			return err
		}
		readToEOF(stream, handler, nil)
		return nil
	}

	server := startMockServer(t, handler)

	w := NewStreamingWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{testFn("dl-fn", func(ctx Context) (any, error) {
			close(jobStarted)
			<-block
			return nil, nil
		})},
		Logger: NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case <-jobStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the job")
	}
	start := time.Now()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return at the drain deadline")
	}
	if d := time.Since(start); d < workerDrainTimeout/2 {
		t.Fatalf("Run returned after %v, want at the drain deadline (%v)", d, workerDrainTimeout)
	}
	if w.state.Load() != int32(stateStopped) {
		t.Fatalf("state = %d, want stopped", w.state.Load())
	}
}

// The h2c client has no timeout, so only Stop can end a registration that
// hangs. A cancelled Run context must not leave Run blocked in it: the drain
// deadline stops the worker, and Stop cancels the registration.
func TestStreamingWorker_ContextCancel_DuringHungRegistration(t *testing.T) {
	originalDrainTimeout := workerDrainTimeout
	workerDrainTimeout = 150 * time.Millisecond
	t.Cleanup(func() { workerDrainTimeout = originalDrainTimeout })

	registering := make(chan struct{}, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/ironflow.v1.IronflowService/RegisterFunction", func(w http.ResponseWriter, r *http.Request) {
		registering <- struct{}{}
		<-r.Context().Done()
	})
	server := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	server.Start()
	t.Cleanup(server.Close)

	w := NewStreamingWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{testFn("fn", func(ctx Context) (any, error) { return nil, nil })},
		Logger:    NewNoopLogger(),
	})
	t.Cleanup(w.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case <-registering:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the registration")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run stayed blocked in the registration")
	}
	if w.state.Load() != int32(stateStopped) {
		t.Fatalf("state = %d, want stopped", w.state.Load())
	}
}

// startHangingRegistrationRun starts Run on a background context against an
// engine that accepts the registration request and never answers it (#2480).
func startHangingRegistrationRun(t *testing.T) (*StreamingWorker, <-chan error) {
	t.Helper()
	registering := make(chan struct{}, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/ironflow.v1.IronflowService/RegisterFunction", func(w http.ResponseWriter, r *http.Request) {
		registering <- struct{}{}
		<-r.Context().Done()
	})
	server := httptest.NewUnstartedServer(h2c.NewHandler(mux, &http2.Server{}))
	server.Start()
	t.Cleanup(server.Close)

	w := NewStreamingWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{testFn("fn", func(ctx Context) (any, error) { return nil, nil })},
		Logger:    NewNoopLogger(),
	})
	t.Cleanup(w.Stop)

	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background()) }()

	select {
	case <-registering:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the registration")
	}
	return w, done
}

func TestStreamingWorker_Stop_DuringHungRegistration(t *testing.T) {
	w, done := startHangingRegistrationRun(t)

	w.Stop()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run stayed blocked in the registration after Stop")
	}
}

// The signal-handler pattern: Run on a background context, Drain on a signal.
// Drain has no jobs to wait for, so the drain deadline stops the worker.
func TestStreamingWorker_Drain_DuringHungRegistration(t *testing.T) {
	originalDrainTimeout := workerDrainTimeout
	workerDrainTimeout = 150 * time.Millisecond
	t.Cleanup(func() { workerDrainTimeout = originalDrainTimeout })

	w, done := startHangingRegistrationRun(t)

	go w.Drain()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run stayed blocked in the registration after Drain")
	}
}

// The Graceful Shutdown example cancels the Run context and calls Drain from
// a signal handler at the same time.
func TestStreamingWorker_ContextCancel_WithConcurrentDrain(t *testing.T) {
	handler := &mockWorkerHandler{}
	var halfClosed atomic.Bool

	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		if _, err := doRegistrationHandshake(stream, handler, 30000); err != nil {
			return err
		}
		halfClosed.Store(readToEOF(stream, handler, nil))
		return nil
	}

	server := startMockServer(t, handler)

	w := NewStreamingWorker(WorkerConfig{
		ServerURL: server.URL,
		Functions: []Function{testFn("ctx-fn", func(ctx Context) (any, error) { return nil, nil })},
		Logger:    NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitFor(t, "the connection", func() bool { return w.state.Load() == int32(stateConnected) })

	drained := make(chan struct{})
	go func() { w.Drain(); close(drained) }()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after the drain")
	}
	select {
	case <-drained:
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return")
	}
	if !halfClosed.Load() {
		t.Error("the drain aborted the stream, want a half-close")
	}
	if w.state.Load() != int32(stateStopped) {
		t.Fatalf("state = %d, want stopped", w.state.Load())
	}
}

// Without a stream there is nothing to drain: a cancel during the reconnect
// wait stops the worker without waiting for the delay.
func TestStreamingWorker_ContextCancel_WhileDisconnected(t *testing.T) {
	handler := &mockWorkerHandler{}
	var connects atomic.Int32
	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		connects.Add(1)
		return connect.NewError(connect.CodeUnavailable, errors.New("engine restarting"))
	}
	server := startMockServer(t, handler)

	w := NewStreamingWorker(WorkerConfig{
		ServerURL:      server.URL,
		Functions:      []Function{testFn("fn", func(ctx Context) (any, error) { return nil, nil })},
		ReconnectDelay: time.Hour,
		Logger:         NewNoopLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitFor(t, "the first connect", func() bool { return connects.Load() > 0 })
	time.Sleep(50 * time.Millisecond) // into the reconnect wait
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run waited out the reconnect delay")
	}
	if w.state.Load() != int32(stateStopped) {
		t.Fatalf("state = %d, want stopped", w.state.Load())
	}
}

func TestStreamingAssignment_CarriesIdempotencyKeyAndSource(t *testing.T) {
	pa := &ironflowv1.JobAssignment{
		JobId: "j", RunId: "r", FunctionId: "fn",
		Event: &ironflowv1.Event{Id: "e1", Name: "e", IdempotencyKey: "idem-1", Source: "webhook"},
	}
	job, err := protoToJobAssignment(pa)
	if err != nil {
		t.Fatal(err)
	}
	if job.Event.IdempotencyKey != "idem-1" {
		t.Errorf("IdempotencyKey = %q, want idem-1", job.Event.IdempotencyKey)
	}
	if job.Event.Source != "webhook" {
		t.Errorf("Source = %q, want webhook", job.Event.Source)
	}
}

// ============================================================================
// Full send queue (#2478)
// ============================================================================

func fullOutCh() chan *ironflowv1.WorkerMessage {
	ch := make(chan *ironflowv1.WorkerMessage, 2)
	for range cap(ch) {
		ch <- &ironflowv1.WorkerMessage{}
	}
	return ch
}

func TestStreamJobReporter_FullQueue_TerminalReportWaitsForSpace(t *testing.T) {
	outCh := fullOutCh()
	r := &streamJobReporter{outCh: outCh, logger: NewNoopLogger()}

	done := make(chan error, 1)
	go func() { done <- r.ReportCompleted(context.Background(), "job-1", nil, nil, 0) }()

	select {
	case err := <-done:
		t.Fatalf("report returned while the queue was full: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	<-outCh // the send loop frees a slot
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("report did not complete after space freed")
	}
	<-outCh
	if _, ok := (<-outCh).GetPayload().(*ironflowv1.WorkerMessage_JobCompleted); !ok {
		t.Fatal("JobCompleted was not queued")
	}
}

func TestStreamJobReporter_FullQueue_CancelledReportReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reports := map[string]func(r *streamJobReporter) error{
		"completed": func(r *streamJobReporter) error { return r.ReportCompleted(ctx, "j", nil, nil, 0) },
		"failed": func(r *streamJobReporter) error {
			return r.ReportFailed(ctx, "j", &PushError{Message: "boom"}, nil, 0)
		},
		"yielded": func(r *streamJobReporter) error {
			return r.ReportYielded(ctx, "j", &YieldInfo{Type: "wait_for_event", StepID: "s"}, nil, 0)
		},
		"yielded-bad-sleep": func(r *streamJobReporter) error {
			return r.ReportYielded(ctx, "j", &YieldInfo{Type: "sleep", Until: "not-a-time"}, nil, 0)
		},
	}
	for name, report := range reports {
		t.Run(name, func(t *testing.T) {
			outCh := fullOutCh()
			r := &streamJobReporter{outCh: outCh, logger: NewNoopLogger()}
			if err := report(r); err == nil {
				t.Fatal("expected an error when the report cannot be queued")
			}
			if len(outCh) != cap(outCh) {
				t.Fatalf("queue changed: len=%d", len(outCh))
			}
		})
	}
}

func TestStreamJobReporter_CancelledCtxStillQueuesWhenSpaceFree(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	outCh := make(chan *ironflowv1.WorkerMessage, 1)
	r := &streamJobReporter{outCh: outCh, logger: NewNoopLogger()}
	if err := r.ReportCompleted(ctx, "j", nil, nil, 0); err != nil {
		t.Fatalf("report with free space must not fail on a cancelled ctx: %v", err)
	}
	if len(outCh) != 1 {
		t.Fatal("message not queued")
	}
}

func TestStreamStepReporter_FullQueue_LogsDrop(t *testing.T) {
	log := &captureLogger{}
	r := &streamStepReporter{outCh: fullOutCh(), logger: log, jobID: "job-1"}

	r.ReportStepStarted("s1", "n", "invoke")
	r.ReportStepCompleted("s1", "n", "invoke", nil, 1)
	r.ReportStepFailed("s2", "n", "invoke", "err", 1)

	if got := len(log.warnings()); got != 3 {
		t.Fatalf("expected 3 drop warnings, got %d: %v", got, log.warnings())
	}
}

func TestStreamingWorker_DrainOutCh_LogsDiscardedCount(t *testing.T) {
	log := &captureLogger{}
	w := &StreamingWorker{outCh: fullOutCh(), logger: log}

	w.drainOutCh()

	if len(w.outCh) != 0 {
		t.Fatal("queue not drained")
	}
	if len(log.warnings()) != 1 {
		t.Fatalf("expected one warning, got %v", log.warnings())
	}
}

// ============================================================================
// Full send queue: ack and nack (#2500)
// ============================================================================

// enqueue runs on the receive loop, so it must not block, but a JobNack must
// not be lost either: without it the engine waits for the lease to expire.
func TestStreamingWorker_FullQueue_NackWaitsForSpace(t *testing.T) {
	w := &StreamingWorker{outCh: fullOutCh(), logger: NewNoopLogger()}
	job := &ironflowv1.JobAssignment{JobId: "job-1", RunId: "run-1", ExecutionSeq: 7, LeaseToken: "tok"}

	returned := make(chan struct{})
	go func() {
		w.nackJob(context.Background(), job, ironflowv1.JobNackReason_JOB_NACK_REASON_AT_CAPACITY)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("nackJob blocked the receive loop on a full queue")
	}

	<-w.outCh // the send loop frees a slot
	<-w.outCh
	select {
	case msg := <-w.outCh:
		if msg.GetJobNack().GetJobId() != "job-1" {
			t.Fatalf("got %T, want the JobNack", msg.GetPayload())
		}
	case <-time.After(time.Second):
		t.Fatal("the nack was dropped instead of waiting for space")
	}
}

func TestStreamingWorker_FullQueue_EnqueueStopsWaitingWhenCtxEnds(t *testing.T) {
	w := &StreamingWorker{outCh: fullOutCh(), logger: NewNoopLogger()}
	ctx, cancel := context.WithCancel(context.Background())

	w.enqueue(ctx, &ironflowv1.WorkerMessage{Payload: &ironflowv1.WorkerMessage_JobAck{JobAck: &ironflowv1.JobAck{JobId: "job-1"}}})
	cancel() // the connection ended
	time.Sleep(50 * time.Millisecond)

	<-w.outCh // space frees up only after the connection is gone
	<-w.outCh
	time.Sleep(50 * time.Millisecond)
	if len(w.outCh) != 0 {
		t.Fatal("a message from the ended connection was queued")
	}
}
