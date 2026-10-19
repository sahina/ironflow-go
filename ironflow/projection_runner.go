package ironflow

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"time"

	"connectrpc.com/connect"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

const (
	// projectionServicePath is the ConnectRPC service path prefix.
	projectionServicePath = "/ironflow.v1.ProjectionService/"

	// projectionMinBackoff is the initial backoff for empty polls.
	projectionMinBackoff = 1 * time.Second

	// projectionMaxBackoff is the maximum backoff for empty/error polls.
	projectionMaxBackoff = 10 * time.Second

	// Stream timings match the Python and Node runners.
	projectionStreamEscalate  = 3
	projectionStreamEndedWait = 1 * time.Second
)

// Variables so tests can change them. They match the Python and Node runners.
var (
	projectionFlushInterval = 100 * time.Millisecond
	projectionReconnectBase = 2 * time.Second
	projectionReconnectMax  = 30 * time.Second
	projectionStopTimeout   = 5 * time.Second

	// Lease timings (ADR 0120 section 7). The TTL comes from the server.
	projectionStandbyRetry = 5 * time.Second // with ±20 % jitter
	projectionRenewRetry   = 1 * time.Second
	projectionLeaseTTL     = 30 * time.Second // only when a grant has no ttl
)

// errStreamingUnsupported means the server has no StreamProjectionEvents;
// the runner polls instead.
var errStreamingUnsupported = errors.New("projection streaming unsupported")

// errProjectionBatch wraps a failed batch (handler, load, save or ack), so the
// reconnect ladder does not take it for a transport error.
var errProjectionBatch = errors.New("projection batch failed")

// errProjectionLeaseLost is the server's Aborted "projection lease lost", the
// one lease error (ADR 0120 section 3). The runner goes back to standby.
var errProjectionLeaseLost = errors.New("projection lease lost")

// errProjectionUnimplemented means the server has no such method: it is
// older than the method.
var errProjectionUnimplemented = errors.New("projection method unimplemented")

// errProjectionInvalidArgument means the server rejected the request as
// invalid. On register it is a config error, which a retry cannot fix.
var errProjectionInvalidArgument = errors.New("projection request invalid")

// projectionLease is the runner lease (ADR 0120). The server fences on Epoch.
type projectionLease struct {
	Token string     `json:"token"`
	Epoch ProtoInt64 `json:"epoch"`
}

// ProjectionRunner streams events for a single projection and processes them.
// It polls when the server does not support the stream. It reads only while it
// holds the projection lease (ADR 0120); other replicas wait as standbys.
type ProjectionRunner struct {
	projection Projection
	baseURL    string
	headers    map[string]string
	logger     Logger
	httpClient *http.Client
	// ctx stops intake (register, stream, poll, sleeps). work cancels the
	// requests of an accepted batch; Stop cancels it only after its time limit.
	ctx        context.Context
	cancel     context.CancelFunc
	work       context.Context
	cancelWork context.CancelFunc
	// detached is never cancelled: the Renew of a handler past the Stop timeout
	// and the Release after Stop use it.
	detached context.Context
	done     chan struct{}
	// lease is the held lease, or nil when the runner is unguarded. lost
	// closes when Renew returns Aborted. Only the run goroutine writes them.
	lease     *projectionLease
	lost      chan struct{}
	unguarded bool // the server has no lease RPCs
}

// NewProjectionRunner creates a new projection runner.
func NewProjectionRunner(proj Projection, baseURL string, headers map[string]string, logger Logger) *ProjectionRunner {
	ctx, cancel := context.WithCancel(context.Background())
	work, cancelWork := context.WithCancel(context.Background())
	if logger == nil {
		logger = NewNoopLogger()
	}
	return &ProjectionRunner{
		projection: proj,
		baseURL:    baseURL,
		headers:    headers,
		logger:     logger,
		httpClient: withSDKHeader(&http.Client{Timeout: DefaultClientTimeout}),
		ctx:        ctx,
		cancel:     cancel,
		work:       work,
		cancelWork: cancelWork,
		detached:   context.Background(),
	}
}

// Start registers the projection and begins the stream (or poll) loop in a
// goroutine. It returns an error only for a 401 or 403. Other register errors
// are retried with backoff in the goroutine, as the Python and Node runners do.
func (r *ProjectionRunner) Start() error {
	err := r.register()
	if registerFatal(err) {
		return fmt.Errorf("register projection %q: %w", r.projection.Config.Name, err)
	}
	r.done = make(chan struct{})
	go r.run(err)
	return nil
}

// Stop ends intake, lets an accepted batch finish and flushes the frames
// already taken into the pending batch, as the Python and Node runners do. It waits at most
// projectionStopTimeout, so the stream is closed before a reconnecting worker
// opens a new one. A handler cannot be cancelled: past the limit, Stop cancels
// the batch requests and returns. The runner keeps renewing its lease until the
// handler returns, so no other runner reads the same events meanwhile. A handler
// that never returns holds the lease until the process exits. An unguarded
// runner (a server without lease RPCs) has no such protection.
func (r *ProjectionRunner) Stop() {
	r.cancel()
	defer r.cancelWork()
	if r.done == nil {
		return
	}
	select {
	case <-r.done:
	case <-time.After(projectionStopTimeout):
		r.logger.Warn("Projection runner did not stop in time, abandoning the in-flight batch", "projection", r.projection.Config.Name, "timeout", projectionStopTimeout)
	}
}

// run retries a failed register, then streams events and falls back to
// polling when the server has no stream.
func (r *ProjectionRunner) run(registerErr error) {
	defer close(r.done)
	for failures := 1; registerErr != nil; failures++ {
		if r.ctx.Err() != nil {
			return
		}
		if registerFatal(registerErr) {
			r.logger.Error("Projection register failed, stopping", "projection", r.projection.Config.Name, "error", registerErr)
			return
		}
		if failures < projectionStreamEscalate {
			r.logger.Warn("Projection register failed, retrying", "projection", r.projection.Config.Name, "attempt", failures, "error", registerErr)
		} else {
			r.logger.Error("Projection register failed, retrying", "projection", r.projection.Config.Name, "attempt", failures, "error", registerErr)
		}
		r.sleep(reconnectDelay(failures))
		registerErr = r.register()
	}
	r.logger.Info("Projection runner started", "projection", r.projection.Config.Name)
	polling := false
	for {
		ttl, err := r.acquireLease()
		if err != nil {
			r.logger.Error(err.Error())
			return
		}
		if r.ctx.Err() != nil {
			r.releaseLease() // a grant that arrived while Stop ran
			return
		}
		// r.detached, not r.ctx: a handler that outlives the Stop timeout keeps
		// the lease, so a replacement runner cannot read the same events (#2822).
		stopRenew := r.startRenew(r.detached, ttl)
		if !polling {
			err = r.streamLoop()
			if errors.Is(err, errStreamingUnsupported) {
				r.logger.Info("Projection streaming unsupported, polling", "projection", r.projection.Config.Name, "error", err)
				polling = true
			}
		}
		if polling {
			err = r.pollLoop()
		}
		stopRenew()
		if errors.Is(err, errProjectionLeaseLost) && r.lease != nil && r.ctx.Err() == nil {
			r.logger.Info("Projection lease lost, standing by", "projection", r.projection.Config.Name, "epoch", int64(r.lease.Epoch), "error", err)
			continue
		}
		r.releaseLease()
		if err != nil {
			r.logger.Error(err.Error())
		}
		return
	}
}

// acquireLease waits as a hot standby until the server grants the lease
// (ADR 0120). It sets r.lease, or leaves it nil when the server has no lease
// RPCs. It returns the server TTL, or an auth error. On Stop it returns no
// error and no lease.
func (r *ProjectionRunner) acquireLease() (time.Duration, error) {
	r.lease, r.lost = nil, nil
	if r.unguarded {
		return 0, nil
	}
	name := r.projection.Config.Name
	standby := false
	for r.ctx.Err() == nil {
		var resp struct {
			Granted bool            `json:"granted"`
			Lease   projectionLease `json:"lease"`
			TTL     string          `json:"ttl"`
		}
		// sdkcoverage: POST /ironflow.v1.ProjectionService/AcquireProjectionLease
		// r.work, not r.ctx: Stop must not cut off a grant in flight, or the
		// lease stays held until it expires. Stop bounds r.work.
		err := r.post(r.work, "AcquireProjectionLease", map[string]any{"name": name}, &resp)
		switch {
		case errors.Is(err, errProjectionUnimplemented):
			r.logger.Warn("Projection lease unsupported by the server, running unguarded", "projection", name)
			r.unguarded = true
			return 0, nil
		case errors.Is(err, ErrUnauthorized), errors.Is(err, ErrForbidden):
			return 0, err
		case err != nil:
			// A lost grant also lands here: the next tries get granted == false
			// until that lease expires.
			if r.ctx.Err() == nil {
				r.logger.Warn("Projection lease acquire failed, retrying", "projection", name, "error", err)
			}
		case resp.Granted:
			r.lease = &resp.Lease
			r.logger.Info("Projection lease granted", "projection", name, "epoch", int64(resp.Lease.Epoch))
			ttl, _ := time.ParseDuration(resp.TTL)
			if ttl <= 0 {
				ttl = projectionLeaseTTL
			}
			return ttl, nil
		case !standby:
			standby = true
			r.logger.Info("Projection lease held by another runner, standing by", "projection", name)
		}
		r.sleep(time.Duration(float64(projectionStandbyRetry) * (0.8 + 0.4*rand.Float64())))
	}
	return 0, nil
}

// startRenew renews the lease every TTL/3 until stop runs. On Aborted it
// closes r.lost. On any other error it retries every projectionRenewRetry and
// the runner keeps processing (ADR 0120 section 3). The runner keeps no
// expiry clock: only the server decides that the lease is lost.
func (r *ProjectionRunner) startRenew(ctx context.Context, ttl time.Duration) (stop func()) {
	if r.lease == nil {
		return func() {}
	}
	name := r.projection.Config.Name
	body := map[string]any{"name": name, "lease": *r.lease}
	lost := make(chan struct{})
	r.lost = lost
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		wait := ttl / 3
		failed := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			var resp struct {
				TTL string `json:"ttl"`
			}
			// One Renew must not outlive the next one: a hung Renew would let the
			// lease expire with no failure to retry (ADR 0120 section 3).
			call, cancelCall := context.WithTimeout(ctx, ttl/3)
			// sdkcoverage: POST /ironflow.v1.ProjectionService/RenewProjectionLease
			err := r.post(call, "RenewProjectionLease", body, &resp)
			cancelCall()
			switch {
			case err == nil:
				if d, _ := time.ParseDuration(resp.TTL); d > 0 {
					ttl = d
				}
				if failed > 0 {
					r.logger.Info("Projection lease renew recovered", "projection", name, "failures", failed)
					failed = 0
				}
				wait = ttl / 3
			case errors.Is(err, errProjectionLeaseLost):
				close(lost)
				return
			case ctx.Err() != nil:
				return
			default:
				// Log failures 1, 2, 4, 8 ... as the Node runner does: the retry runs every second.
				if failed++; failed&(failed-1) == 0 {
					r.logger.Warn("Projection lease renew failed, retrying", "projection", name, "attempt", failed, "error", err)
				}
				wait = projectionRenewRetry
			}
		}
	}()
	// Wait for the renew to exit, so a late Renew cannot report a lost lease
	// after Release.
	return func() { cancel(); <-done }
}

// releaseLease frees the lease on a clean stop, so a standby takes it on its
// next try. The server never fails a Release, so the error is ignored.
func (r *ProjectionRunner) releaseLease() {
	if r.lease == nil {
		return
	}
	// r.detached, not r.ctx or r.work: Stop has cancelled r.ctx, and after the
	// Stop timeout also r.work. The client timeout bounds it. A late Release with
	// a stale epoch changes nothing.
	// sdkcoverage: POST /ironflow.v1.ProjectionService/ReleaseProjectionLease
	_ = r.post(r.detached, "ReleaseProjectionLease", map[string]any{"name": r.projection.Config.Name, "lease": r.lease}, nil)
}

// leaseLost reports whether Renew lost the lease of the current grant.
func (r *ProjectionRunner) leaseLost() bool {
	select {
	case <-r.lost:
		return true
	default:
		return false
	}
}

// withLease adds the held lease to a request body. An unguarded runner sends
// no lease: the server rejects an empty one.
func (r *ProjectionRunner) withLease(body map[string]any) map[string]any {
	if r.lease != nil {
		body["lease"] = r.lease
	}
	return body
}

// streamLoop reads the event stream and reconnects with backoff. It returns
// errStreamingUnsupported for a fallback, errProjectionLeaseLost for standby,
// an auth error to stop the runner, or nil on Stop.
func (r *ProjectionRunner) streamLoop() error {
	// No client timeout: the stream stays open. Server streaming works over
	// HTTP/1.1, as the Node runner uses it.
	client := ironflowv1connect.NewProjectionServiceClient(withSDKHeader(&http.Client{}), r.baseURL, connect.WithProtoJSON())
	failures := 0
	for {
		if r.leaseLost() {
			return errProjectionLeaseLost // do not open a stream with a fenced lease
		}
		received, progressed, err := r.readStream(r.ctx, client)
		if r.ctx.Err() != nil {
			if errors.Is(err, errProjectionBatch) {
				r.logger.Warn("Projection final batch failed on stop", "projection", r.projection.Config.Name, "error", err)
			}
			return nil
		}
		delay := projectionStreamEndedWait
		switch {
		case errors.Is(err, errProjectionLeaseLost):
			// No reconnect ladder and no fallback to poll (ADR 0120 section 3).
			return err
		case errors.Is(err, errStreamingUnsupported), errors.Is(err, ErrUnauthorized), errors.Is(err, ErrForbidden):
			return err
		case err == nil:
			failures = 0
			r.logger.Info("Projection stream ended, reconnecting", "projection", r.projection.Config.Name)
		default:
			// A link that works between drops must not stay at the backoff ceiling.
			// A failed batch fails again on the same event, so it must escalate.
			// A batch that succeeded proves the processing works too.
			if progressed || (received && !errors.Is(err, errProjectionBatch)) {
				failures = 0
			}
			// The first drops are often idle timeouts; escalate when they repeat.
			failures++
			if failures < projectionStreamEscalate {
				r.logger.Info("Projection stream failed, reconnecting", "projection", r.projection.Config.Name, "attempt", failures, "error", err)
			} else {
				r.logger.Error("Projection stream failed, reconnecting", "projection", r.projection.Config.Name, "attempt", failures, "error", err)
			}
			delay = reconnectDelay(failures)
		}
		r.sleep(delay)
	}
}

// registerFatal reports a register error that a retry cannot fix.
func registerFatal(err error) bool {
	return errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrForbidden) || errors.Is(err, errProjectionInvalidArgument)
}

// reconnectDelay is the reconnect ladder: 2s, 4s, 8s ... up to 30s.
func reconnectDelay(failures int) time.Duration {
	return min(projectionReconnectBase<<min(failures-1, 4), projectionReconnectMax)
}

// readStream opens one stream and processes its events in batches until the
// stream ends, a batch fails, the lease is lost, or the runner stops. A failed
// batch drops the stream; the server rewinds to the saved cursor on the next
// open. On stop it processes the pending frames; on a lost lease it drops them.
// received reports whether any frame (heartbeats too) arrived; progressed
// whether a batch with events succeeded.
func (r *ProjectionRunner) readStream(ctx context.Context, client ironflowv1connect.ProjectionServiceClient) (received, progressed bool, err error) {
	batchSize := r.projection.Config.BatchSize
	if batchSize == 0 {
		batchSize = 100
	}
	msg := &ironflowv1.StreamProjectionEventsRequest{
		Name:             r.projection.Config.Name,
		BatchSize:        int32(batchSize),
		AcceptHeartbeats: true,
	}
	if r.lease != nil {
		msg.Lease = &ironflowv1.ProjectionLease{Token: r.lease.Token, Epoch: int64(r.lease.Epoch)}
	}
	req := connect.NewRequest(msg)
	for k, v := range r.headers {
		req.Header().Set(k, v)
	}

	ctx, cancel := context.WithCancel(ctx)
	// sdkcoverage: POST /ironflow.v1.ProjectionService/StreamProjectionEvents
	stream, err := client.StreamProjectionEvents(ctx, req)
	if err != nil {
		cancel()
		return false, false, streamError(err)
	}

	frames := make(chan pollEvent)
	got := false
	go func() {
		defer close(frames)
		for stream.Receive() {
			got = true
			msg := stream.Msg()
			if msg.Kind == ironflowv1.ProjectionEventKind_PROJECTION_EVENT_KIND_HEARTBEAT {
				continue
			}
			select {
			case frames <- toPollEvent(msg):
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() {
		// Close only after the reader has stopped calling Receive.
		cancel()
		for range frames {
		}
		_ = stream.Close()
		received = got // safe: the reader has exited
	}()

	var pending []pollEvent
	var flush <-chan time.Time
	for {
		select {
		case evt, ok := <-frames:
			if !ok {
				streamErr := streamError(stream.Err())
				if errors.Is(streamErr, errProjectionLeaseLost) {
					return false, progressed, streamErr // drop the pending frames
				}
				// Events already received are valid: process them before the stream error.
				if err := r.processBatch(pending); err != nil {
					return false, progressed, err
				}
				return false, progressed || len(pending) > 0, streamErr
			}
			pending = append(pending, evt)
			if len(pending) < batchSize {
				if flush == nil {
					flush = time.After(projectionFlushInterval)
				}
				continue
			}
		case <-flush:
		case <-r.lost:
			return false, progressed, errProjectionLeaseLost // drop the pending frames
		case <-r.ctx.Done():
			// select picks at random when both are ready: a lost lease wins.
			if r.leaseLost() {
				return false, progressed, errProjectionLeaseLost
			}
			return false, progressed, r.processBatch(pending)
		}
		flush = nil
		batch := pending
		pending = nil
		if err := r.processBatch(batch); err != nil {
			return false, progressed, err
		}
		progressed = progressed || len(batch) > 0
	}
}

// streamError maps a stream error. Unimplemented and NotFound mean the
// server cannot stream this projection, as in the Python runner.
func streamError(err error) error {
	if err == nil {
		return nil
	}
	code := connect.CodeOf(err)
	if code == connect.CodeUnimplemented || code == connect.CodeNotFound {
		return fmt.Errorf("%w: %w", errStreamingUnsupported, err)
	}
	if code == connect.CodeAborted {
		return fmt.Errorf("%w: %w", errProjectionLeaseLost, err)
	}
	return connectError(err)
}

// toPollEvent converts a stream frame to the poll event shape, so both
// transports share one batch path.
func toPollEvent(msg *ironflowv1.ProjectionEvent) pollEvent {
	evt := pollEvent{ID: msg.Id, Name: msg.Name, Seq: ProtoInt64(msg.Seq), Source: msg.Source}
	if msg.Data != nil {
		evt.Data = msg.Data.AsMap()
	}
	if msg.DataValue != nil {
		evt.DataValue = msg.DataValue.AsInterface()
	}
	if msg.Metadata != nil {
		evt.Metadata = msg.Metadata.AsMap()
	}
	if msg.Timestamp != nil {
		evt.Timestamp = msg.Timestamp.AsTime().Format(time.RFC3339Nano)
	}
	return evt
}

// processBatch handles a batch of stream events the way poll handles a
// response. Its errors wrap errProjectionBatch.
func (r *ProjectionRunner) processBatch(events []pollEvent) (err error) {
	if len(events) == 0 {
		return nil
	}
	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %w", errProjectionBatch, err)
		}
	}()
	config := r.projection.Config
	resp := pollResponse{Events: events}
	if config.Mode != ProjectionModeManaged {
		_, err := r.processExternal(config, resp)
		return err
	}
	// A stream frame carries no state: load the global state a poll response
	// carries, when the batch uses it.
	if !slices.ContainsFunc(events, func(e pollEvent) bool { return partitionOf(e) == "__global__" }) {
		_, err = r.processManaged(config, resp)
		return err
	}
	var global struct {
		State      map[string]any  `json:"state"`
		StateValue json.RawMessage `json:"stateValue"`
	}
	if err := r.post(r.work, "GetProjection", map[string]any{"name": config.Name}, &global); err != nil {
		return fmt.Errorf("load global state: %w", err)
	}
	resp.CurrentState, resp.CurrentStateValue = global.State, global.StateValue
	_, err = r.processManaged(config, resp)
	return err
}

// register sends a RegisterProjection request to the server.
func (r *ProjectionRunner) register() error {
	body := map[string]any{
		"name":    r.projection.Config.Name,
		"events":  r.projection.Config.Events,
		"mode":    string(r.projection.Config.Mode),
		"version": 1,
	}
	if r.projection.Config.PartitionKey != "" {
		body["partitionKey"] = r.projection.Config.PartitionKey
	}

	// sdkcoverage: POST /ironflow.v1.ProjectionService/RegisterProjection
	return r.post(r.ctx, "RegisterProjection", body, nil)
}

// pollLoop continuously polls for events with exponential backoff. It returns
// errProjectionLeaseLost for standby, an auth error to stop the runner, or nil
// on Stop.
func (r *ProjectionRunner) pollLoop() error {
	backoff := projectionMinBackoff

	for {
		if r.ctx.Err() != nil {
			return nil
		}
		if r.leaseLost() {
			return errProjectionLeaseLost
		}

		processed, err := r.poll()
		if r.ctx.Err() != nil {
			return nil // Stop cancelled the poll; that is not an error
		}
		if err != nil {
			// Auth failure: stop this runner with an actionable message rather
			// than polling a 401 forever (#1673). The worker keeps running.
			if errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrForbidden) || errors.Is(err, errProjectionLeaseLost) {
				return err
			}
			r.logger.Error("Projection poll error", "projection", r.projection.Config.Name, "error", err)
			r.sleep(backoff)
			backoff = minDuration(backoff*2, projectionMaxBackoff)
			continue
		}

		if processed > 0 {
			backoff = projectionMinBackoff // reset on work done
		} else {
			r.sleep(backoff)
			backoff = minDuration(backoff*2, projectionMaxBackoff)
		}
	}
}

// pollResponse is the JSON shape of PollProjectionEvents response.
type pollResponse struct {
	Events            []pollEvent     `json:"events"`
	CurrentState      map[string]any  `json:"currentState"`
	CurrentStateValue json.RawMessage `json:"currentStateValue"`
	LastEventSeq      ProtoInt64      `json:"lastEventSeq"`
}

// pollEvent is the JSON shape of a single event from PollProjectionEvents.
type pollEvent struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Data      map[string]any `json:"data"`
	DataValue any            `json:"dataValue"`
	Seq       ProtoInt64     `json:"seq"`
	Timestamp string         `json:"timestamp"`
	Source    string         `json:"source"`
	Metadata  map[string]any `json:"metadata"`
}

// toProjectionEvent converts a pollEvent to a ProjectionEvent.
// cloneState returns a deep copy of s via JSON round-trip. Used to isolate
// handler invocations from each other so in-place mutation of nested maps or
// slices cannot leak across iterations (issue #486 I3). An empty or nil map
// short-circuits to a fresh empty map so callers never see nil.
func cloneState(s map[string]any) map[string]any {
	if len(s) == 0 {
		return make(map[string]any)
	}
	buf, err := json.Marshal(s)
	if err != nil {
		// Non-JSON-safe value in the state map — fall back to shallow copy.
		out := make(map[string]any, len(s))
		maps.Copy(out, s)
		return out
	}
	var out map[string]any
	if err := json.Unmarshal(buf, &out); err != nil {
		return make(map[string]any)
	}
	return out
}

func toProjectionEvent(evt pollEvent) ProjectionEvent {
	data, value := evt.Data, evt.DataValue
	if m, ok := value.(map[string]any); ok {
		data, value = m, nil
	}
	return ProjectionEvent{
		ID:        evt.ID,
		Name:      evt.Name,
		Data:      data,
		DataValue: value,
		Seq:       int64(evt.Seq),
		Timestamp: evt.Timestamp,
		Source:    evt.Source,
		Metadata:  evt.Metadata,
	}
}

// poll fetches a batch of events and processes them.
// Returns the number of events processed.
func (r *ProjectionRunner) poll() (int, error) {
	// Request events from server
	batchSize := r.projection.Config.BatchSize
	if batchSize == 0 {
		batchSize = 100
	}

	var resp pollResponse
	if err := r.post(r.ctx, "PollProjectionEvents", r.withLease(map[string]any{
		"name":      r.projection.Config.Name,
		"batchSize": batchSize,
	}), &resp); err != nil {
		return 0, err
	}

	if len(resp.Events) == 0 {
		return 0, nil
	}

	config := r.projection.Config

	if config.Mode == ProjectionModeManaged {
		return r.processManaged(config, resp)
	}
	return r.processExternal(config, resp)
}

// partitionBatch is a run of consecutive same-partition events.
type partitionBatch struct {
	pk            string
	events        []pollEvent
	lastEventID   string
	lastEventSeq  int64
	lastEventTime string
}

// processManaged runs the handler as a reducer and saves the resulting state.
// Each partition (from event metadata) has its own state, so partitions never
// corrupt each other.
func (r *ProjectionRunner) processManaged(config ProjectionConfig, resp pollResponse) (int, error) {
	// Reduce and save in seq order, one run of consecutive same-partition
	// events at a time. Each save moves the server cursor to the run's last
	// seq, so a failed save leaves only unsaved events past the cursor.
	events := slices.Clone(resp.Events)
	slices.SortStableFunc(events, func(a, b pollEvent) int { return cmp.Compare(a.Seq, b.Seq) })
	var runs []*partitionBatch
	partitions := make(map[string]struct{})
	for _, evt := range events {
		pk := partitionOf(evt)
		partitions[pk] = struct{}{}
		if n := len(runs); n > 0 && runs[n-1].pk == pk {
			runs[n-1].events = append(runs[n-1].events, evt)
		} else {
			runs = append(runs, &partitionBatch{pk: pk, events: []pollEvent{evt}})
		}
	}

	if _, ok := partitions["__global__"]; ok {
		if err := nonObjectState(resp.CurrentState, resp.CurrentStateValue); err != nil {
			return 0, fmt.Errorf("load global state: %w", err)
		}
	}
	// Load every partition's saved state before the first save, so a failed
	// load saves nothing.
	states, err := r.loadPartitionStates(config.Name, partitions)
	if err != nil {
		return 0, err
	}
	// A saved {} means "no state", as in the Python and Node runners (#2796).
	if len(resp.CurrentState) > 0 {
		states["__global__"] = resp.CurrentState
	}

	for _, batch := range runs {
		pk := batch.pk
		state := states[pk]
		if state == nil && config.InitialState != nil {
			state = config.InitialState()
		}
		if state == nil {
			state = make(map[string]any)
		}

		for _, evt := range batch.events {
			projEvent := toProjectionEvent(evt)
			ctx := r.buildContext(evt)

			// Defuse aliasing bugs (issue #486 I3): hand the handler an
			// isolated copy of state so in-place mutation of nested maps or
			// slices cannot leak across iterations. The handler's documented
			// contract is still pure (state, event) → newState, but this
			// removes the sharpest edge of the footgun.
			handlerState := cloneState(state)

			newState, err := config.Handler(handlerState, projEvent, ctx)
			if err != nil {
				return 0, fmt.Errorf("handler error on event %s: %w", evt.ID, err)
			}

			state = newState
			batch.lastEventID = evt.ID
			batch.lastEventSeq = int64(evt.Seq)
			batch.lastEventTime = evt.Timestamp
		}

		// Save state for this partition.
		saveBody := map[string]any{
			"name":         config.Name,
			"partitionKey": pk,
			"state":        state,
			"lastEventId":  batch.lastEventID,
			"lastEventSeq": batch.lastEventSeq,
		}
		if batch.lastEventTime != "" {
			saveBody["lastEventTime"] = batch.lastEventTime
		}

		// Renew lost the lease: stop the batch before its next save.
		if r.leaseLost() {
			return 0, errProjectionLeaseLost
		}
		if err := r.post(r.work, "SaveProjectionState", r.withLease(saveBody), nil); err != nil {
			return 0, fmt.Errorf("save state for partition %q: %w", pk, err)
		}
		states[pk] = state
	}

	return len(resp.Events), nil
}

// partitionOf is the event's partition key, or "__global__" when it has none.
func partitionOf(evt pollEvent) string {
	if p, ok := evt.Metadata["__partition"].(string); ok && p != "" {
		return p
	}
	return "__global__"
}

// loadPartitionStates fetches the saved state of every non-global partition
// concurrently. A missing or {} partition maps to nil (the caller falls back
// to InitialState).
func (r *ProjectionRunner) loadPartitionStates(name string, partitions map[string]struct{}) (map[string]map[string]any, error) {
	var (
		mu     sync.Mutex
		wg     sync.WaitGroup
		errs   []error
		sem    = make(chan struct{}, 8)
		states = make(map[string]map[string]any, len(partitions))
	)
	for pk := range partitions {
		if pk == "__global__" {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			var saved struct {
				State      map[string]any  `json:"state"`
				StateValue json.RawMessage `json:"stateValue"`
			}
			err := r.post(r.work, "GetProjection", map[string]any{"name": name, "partition": pk}, &saved)
			if err == nil {
				err = nonObjectState(saved.State, saved.StateValue)
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, fmt.Errorf("load state for partition %q: %w", pk, err))
				return
			}
			if len(saved.State) > 0 {
				states[pk] = saved.State
			}
		}()
	}
	wg.Wait()
	return states, errors.Join(errs...)
}

// nonObjectState fails a saved state that is not a JSON object (#1963). A Go
// handler reduces map state only; falling back to InitialState would overwrite
// the saved value on the next save.
func nonObjectState(state map[string]any, value json.RawMessage) error {
	if state == nil && len(value) > 0 && string(value) != "null" {
		return fmt.Errorf("saved state is not a JSON object: %s", value)
	}
	return nil
}

// processExternal runs the handler for each event and acks.
func (r *ProjectionRunner) processExternal(config ProjectionConfig, resp pollResponse) (int, error) {
	var lastEventID string
	var lastEventSeq int64

	for _, evt := range resp.Events {
		// External handlers have side effects: stop at once when the lease is lost.
		if r.leaseLost() {
			return 0, errProjectionLeaseLost
		}
		// Past the Stop timeout the ack cannot be sent: a replacement runner
		// handles these events again.
		if err := r.work.Err(); err != nil {
			return 0, err
		}
		projEvent := toProjectionEvent(evt)
		ctx := r.buildContext(evt)

		if _, err := config.Handler(nil, projEvent, ctx); err != nil {
			return 0, fmt.Errorf("handler error on event %s: %w", evt.ID, err)
		}

		lastEventID = evt.ID
		lastEventSeq = int64(evt.Seq)
	}

	if r.leaseLost() {
		return 0, errProjectionLeaseLost
	}
	if err := r.post(r.work, "AckProjectionEvents", r.withLease(map[string]any{
		"name":         config.Name,
		"lastEventId":  lastEventID,
		"lastEventSeq": lastEventSeq,
	}), nil); err != nil {
		return 0, fmt.Errorf("ack events: %w", err)
	}

	return len(resp.Events), nil
}

// buildContext creates a ProjectionContext for the given event.
func (r *ProjectionRunner) buildContext(evt pollEvent) ProjectionContext {
	return ProjectionContext{
		Event: struct {
			ID        string
			Name      string
			Seq       int64
			Timestamp string
		}{
			ID:        evt.ID,
			Name:      evt.Name,
			Seq:       int64(evt.Seq),
			Timestamp: evt.Timestamp,
		},
		Projection: struct {
			Name    string
			Version int
		}{
			Name:    r.projection.Config.Name,
			Version: 1,
		},
	}
}

// post makes a POST request to a ConnectRPC endpoint. Intake requests use
// r.ctx; the requests of an accepted batch use r.work.
func (r *ProjectionRunner) post(ctx context.Context, method string, body any, result any) error {
	url := r.baseURL + projectionServicePath + method

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range r.headers {
		req.Header.Set(k, v)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		if authErr := authError(resp, fmt.Sprintf("projection %s %s", r.projection.Config.Name, method)); authErr != nil {
			return authErr
		}
		respBody, _ := io.ReadAll(resp.Body)
		return projectionStatusError(resp.StatusCode, respBody)
	}

	if result != nil {
		return json.NewDecoder(resp.Body).Decode(result)
	}

	return nil
}

// projectionStatusError reads the code from a Connect JSON error body. Connect
// sends aborted as HTTP 409. An older server answers an unknown method with
// HTTP 404 and no Connect body, which a Connect client reads as unimplemented.
func projectionStatusError(status int, body []byte) error {
	err := fmt.Errorf("HTTP %d: %s", status, body)
	var payload struct {
		Code connect.Code `json:"code"`
	}
	_ = json.Unmarshal(body, &payload) // a body that is not a Connect error leaves the code 0
	code := payload.Code
	if code == 0 && status == http.StatusNotFound {
		code = connect.CodeUnimplemented
	}
	if code == connect.CodeAborted {
		return fmt.Errorf("%w: %w", errProjectionLeaseLost, err)
	}
	if code == connect.CodeUnimplemented {
		return fmt.Errorf("%w: %w", errProjectionUnimplemented, err)
	}
	if code == connect.CodeInvalidArgument {
		return fmt.Errorf("%w: %w", errProjectionInvalidArgument, err)
	}
	return err
}

// sleep pauses for the given duration. Stop and a lost lease end it early.
func (r *ProjectionRunner) sleep(d time.Duration) {
	select {
	case <-r.ctx.Done():
	case <-r.lost:
	case <-time.After(d):
	}
}

// minDuration returns the smaller of two durations.
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
