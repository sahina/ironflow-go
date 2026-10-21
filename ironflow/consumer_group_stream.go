package ironflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

// ErrStaleAck is returned by Ack, Nak and Term for an event that is not pending
// on the current stream: it arrived on an earlier stream, or this stream has
// settled every copy of it already. The ack is never sent, as the server would
// answer a false OK. An event of an earlier stream is redelivered.
var ErrStaleAck = errors.New("ironflow: the event is not pending on the current stream (settled already, or delivered before a reconnect)")

// AckError is returned when the server rejects an ack, nak or term.
type AckError struct {
	EventID string
	// Code is "invalid_argument", "not_found" (the group or this member is
	// gone; do not retry) or "internal" (the server may succeed on a retry).
	Code    string
	Message string
}

func (e *AckError) Error() string {
	return fmt.Sprintf("ironflow: ack of event %s rejected (%s): %s", e.EventID, e.Code, e.Message)
}

// ErrSubscriptionClosed is returned by Ack, Nak and Term after Unsubscribe, a
// done join ctx, or a terminal stream error.
var ErrSubscriptionClosed = errors.New("ironflow: the subscription is closed")

// groupStreamInitialDelay is the first reconnect delay. A variable so tests can shorten it.
var groupStreamInitialDelay = time.Second

// groupStreamIdleTimeout is the longest wait for any frame. The server sends a
// heartbeat after 30 s idle, so a longer silence is a dead connection (a NAT or
// load balancer dropped it without a reset): the stream reconnects. A variable
// so tests can shorten it.
var groupStreamIdleTimeout = 90 * time.Second

const (
	groupStreamMaxDelay = 30 * time.Second
	groupStreamBackoff  = 1.5
)

// groupAcker settles events of a consumer-group subscription.
type groupAcker interface {
	Ack(eventID string) error
	Nak(eventID string, delay time.Duration) error
	Term(eventID string) error
}

var (
	_ groupAcker = (*groupStream)(nil)
	_ groupAcker = (*GrpcAckableSubscription)(nil)
)

// groupStream is the SubscribeBidirectional consumer-group subscription behind
// JoinConsumerGroup(WithJoinTransport("grpc")).
//
// Goroutines: run (owns the reconnect loop and reads the current stream),
// pump (moves queued MANUAL events to the Events channel), and one teardown
// watcher per stream. Ack callers write to the stream directly under sendMu.
type groupStream struct {
	rpc        ironflowv1connect.PubSubServiceClient
	httpClient interface{ CloseIdleConnections() }
	logger     Logger
	join       *ironflowv1.JoinConsumerGroupRequest
	firstDelay time.Duration
	idleWait   time.Duration // groupStreamIdleTimeout at join
	maxAge     time.Duration // deliveryMaxAge at join

	ctx      context.Context
	cancel   context.CancelFunc
	stopCh   chan struct{} // closed by Unsubscribe
	stop     sync.Once
	exiting  atomic.Bool   // run has decided to end: no reconnect will follow
	runEnd   chan struct{} // closed when run returns
	pumpDone chan struct{}
	warnAuto sync.Once

	mu  sync.Mutex
	cur *streamConn

	// MANUAL streams only. The reader appends here and never blocks: it also
	// delivers ack results, so a consumer that acks from its Events loop must
	// not stall it. The queue cannot outgrow the group's MaxInflight (the
	// JetStream MaxAckPending), which caps unacked events. AUTO streams skip it
	// and send to events directly, so flow control pushes back to the server.
	qmu      sync.Mutex
	queue    []queuedEvent
	finished bool
	wake     chan struct{}
	events   chan *SubscriptionEvent
	errors   chan *SubscriptionError
}

type queuedEvent struct {
	ev *SubscriptionEvent
	sc *streamConn
}

func (c *Client) joinConsumerGroupGrpc(ctx context.Context, groupName string, options *JoinConsumerGroupOptions) (*AckableSubscription, error) {
	httpClient := newH2CClient(c.serverURL)
	gctx, cancel := context.WithCancel(ctx)
	g := &groupStream{
		rpc: ironflowv1connect.NewPubSubServiceClient(httpClient, c.serverURL,
			connect.WithInterceptors(c.interceptor())),
		httpClient: httpClient,
		logger:     c.logger,
		join: &ironflowv1.JoinConsumerGroupRequest{
			Namespace:  options.Namespace,
			GroupName:  groupName,
			ConsumerId: options.ConsumerID,
		},
		firstDelay: groupStreamInitialDelay,
		idleWait:   groupStreamIdleTimeout,
		maxAge:     deliveryMaxAge,
		ctx:        gctx,
		cancel:     cancel,
		stopCh:     make(chan struct{}),
		runEnd:     make(chan struct{}),
		pumpDone:   make(chan struct{}),
		wake:       make(chan struct{}, 1),
		events:     make(chan *SubscriptionEvent, 100),
		errors:     make(chan *SubscriptionError, 10),
	}

	sc, err := g.open(gctx)
	if err != nil {
		cancel()
		httpClient.CloseIdleConnections()
		err = serverTooOld(err)
		return nil, fmt.Errorf("failed to join consumer group: %w", err)
	}

	go g.pump()
	go g.run(gctx, sc)

	return &AckableSubscription{
		Subscription: &Subscription{
			ID:        sc.subID,
			Pattern:   sc.pattern,
			events:    g.events,
			errors:    g.errors,
			done:      make(chan struct{}),
			stop:      g.unsubscribe,
			currentID: func() string { return g.current().subID },
		},
		acker: g,
	}, nil
}

func (g *groupStream) current() *streamConn {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cur
}

var errGroupStreamStopped = errors.New("ironflow: the subscription was unsubscribed while it reconnected")

// open starts a stream, sends the join and waits for joined.
func (g *groupStream) open(ctx context.Context) (*streamConn, error) {
	connCtx, connCancel := context.WithCancel(ctx)
	// sdkcoverage: POST /ironflow.v1.PubSubService/SubscribeBidirectional
	stream := g.rpc.SubscribeBidirectional(connCtx)

	// A cancelled ctx alone leaves an idle bidi stream open (no RST_STREAM, and
	// Receive stays blocked), so every teardown closes both halves.
	go func() {
		<-connCtx.Done()
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
	}()

	fail := func(err error) (*streamConn, error) {
		connCancel()
		return nil, err
	}

	if err := stream.Send(&ironflowv1.SubscribeBidirectionalRequest{Payload: &ironflowv1.SubscribeBidirectionalRequest_Join{Join: g.join}}); err != nil {
		if errors.Is(err, io.EOF) {
			_, err = stream.Receive() // the server's real status
		}
		return fail(err)
	}
	msg, err := stream.Receive()
	if err != nil {
		return fail(err)
	}
	joined := msg.GetJoined()
	if joined == nil {
		return fail(connect.NewError(connect.CodeInternal, errors.New("first frame was not joined")))
	}

	sc := &streamConn{
		stream:   stream,
		cancel:   connCancel,
		endCh:    make(chan struct{}),
		subID:    joined.GetSubscriptionId(),
		pattern:  joined.GetPattern(),
		manual:   joined.GetAckMode() == ironflowv1.AckMode_ACK_MODE_MANUAL,
		pending:  map[uint64]chan ackOutcome{},
		delivery: map[string]delivered{},
		maxAge:   g.maxAge,
	}
	// unsubscribe closes stopCh before it reads cur under this lock, so a join
	// that lands after Unsubscribe is either seen and half-closed there, or
	// refused here. Either way no member outlives Unsubscribe.
	g.mu.Lock()
	if g.stopped() {
		g.mu.Unlock()
		return fail(errGroupStreamStopped)
	}
	g.cur = sc
	g.mu.Unlock()
	return sc, nil
}

// run reads streams until Unsubscribe, ctx cancel or a terminal error.
func (g *groupStream) run(ctx context.Context, sc *streamConn) {
	defer func() {
		// ctx (the join ctx) ended the subscription: say so on Errors().
		if err := ctx.Err(); err != nil && !g.stopped() {
			code := connect.CodeCanceled
			if errors.Is(err, context.DeadlineExceeded) {
				code = connect.CodeDeadlineExceeded
			}
			g.report(g.current().subID, connect.NewError(code, fmt.Errorf("the join ctx ended the subscription: %w", err)))
		}
		g.exiting.Store(true)
		g.qmu.Lock()
		g.finished = true
		g.qmu.Unlock()
		g.signal()
		// Every queued event belongs to an ended stream by now, so the pump drops
		// the rest and exits. Closing events after it keeps one sender-closer.
		<-g.pumpDone
		g.cancel()
		g.httpClient.CloseIdleConnections()
		close(g.events)
		close(g.errors) // run is the only sender
		close(g.runEnd)
	}()

	delay := g.firstDelay
	for {
		accepted, err := g.read(ctx, sc)
		if ctx.Err() != nil || g.stopped() {
			return
		}
		// A clean server end (for instance a deleted group) reconnects; a rejoin
		// that finds the group gone is NotFound, which is terminal.
		if err != nil && !isRetryableSubscriptionError(err) {
			g.report(sc.subID, err)
			return
		}
		// Only a stream that got past joined earns a fresh backoff, as the
		// Subscribe path's "accepted" does: a server that joins and hangs up must
		// not be retried at the base rate forever.
		if accepted {
			delay = g.firstDelay
		}
		for {
			if !waitForSubscriptionReconnect(ctx, g.stopCh, nil, delay) {
				return
			}
			delay = nextSubscriptionReconnectDelay(delay, groupStreamMaxDelay, groupStreamBackoff)
			next, err := g.open(ctx)
			if err == nil {
				sc = next
				break
			}
			if ctx.Err() != nil || g.stopped() {
				return
			}
			if !isRetryableSubscriptionError(err) {
				g.report(sc.subID, serverTooOld(err))
				return
			}
		}
	}
}

// read consumes one stream to its end. err is nil for a clean end; accepted
// reports that a frame past joined arrived.
func (g *groupStream) read(ctx context.Context, sc *streamConn) (accepted bool, err error) {
	idle := time.AfterFunc(g.idleWait, func() {
		sc.reset(connect.NewError(connect.CodeUnavailable, fmt.Errorf("no frame from the server within %s", g.idleWait)))
	})
	defer func() {
		idle.Stop()
		sc.finish()
		sc.cancel() // releases the teardown watcher, which closes both halves
	}()
	for {
		// Only the wait in Receive counts: an AUTO reader blocked on a full
		// Events channel is not an idle connection.
		idle.Reset(g.idleWait)
		msg, err := sc.stream.Receive()
		idle.Stop()
		if err != nil {
			if reason := sc.resetReason(); reason != nil {
				return accepted, reason
			}
			if errors.Is(err, io.EOF) {
				return accepted, nil
			}
			return accepted, err
		}
		switch p := msg.GetPayload().(type) {
		case *ironflowv1.SubscribeBidirectionalResponse_Event:
			accepted = true
			ev, convErr := subscriptionEventFromProto(p.Event)
			if convErr != nil {
				continue // Skip malformed messages, as the Subscribe stream does.
			}
			if !sc.manual {
				select {
				case g.events <- ev:
				case <-g.stopCh:
					return accepted, nil
				case <-ctx.Done():
					return accepted, nil
				}
				continue
			}
			sc.markDelivered(ev.ID)
			g.push(queuedEvent{ev: ev, sc: sc})
		case *ironflowv1.SubscribeBidirectionalResponse_AckResult:
			accepted = true
			sc.resolve(p.AckResult)
		case *ironflowv1.SubscribeBidirectionalResponse_Heartbeat:
			accepted = true
		}
	}
}

func (g *groupStream) push(qe queuedEvent) {
	g.qmu.Lock()
	g.queue = append(g.queue, qe)
	g.qmu.Unlock()
	g.signal()
}

func (g *groupStream) signal() {
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

// pump forwards queued events to Events(). An event of an ended stream is
// dropped, not delivered: the server redelivers it on the new stream, and an
// ack of it here could only fail with ErrStaleAck.
func (g *groupStream) pump() {
	defer close(g.pumpDone)
	for {
		g.qmu.Lock()
		var qe queuedEvent
		if len(g.queue) > 0 {
			qe, g.queue = g.queue[0], g.queue[1:]
		}
		finished := g.finished
		g.qmu.Unlock()

		switch {
		case qe.ev != nil:
			select {
			case <-qe.sc.endCh:
				continue
			default:
			}
			select {
			case g.events <- qe.ev:
			case <-qe.sc.endCh:
			case <-g.stopCh:
				return
			case <-g.ctx.Done():
				return
			}
		case finished:
			return
		default:
			select {
			case <-g.wake:
			case <-g.stopCh:
				return
			case <-g.ctx.Done():
				return
			}
		}
	}
}

func (g *groupStream) stopped() bool {
	select {
	case <-g.stopCh:
		return true
	default:
		return false
	}
}

func (g *groupStream) closed() bool {
	return g.stopped() || g.exiting.Load() || g.ctx.Err() != nil
}

func (g *groupStream) report(subID string, err error) {
	select {
	case g.errors <- &SubscriptionError{
		SubscriptionID: subID,
		Code:           connect.CodeOf(err).String(),
		Message:        err.Error(),
	}:
	default:
	}
}

// unsubscribe half-closes so the server leaves the group, then forces the
// teardown if the server does not end the stream.
func (g *groupStream) unsubscribe() {
	g.stop.Do(func() {
		close(g.stopCh)
		if sc := g.current(); sc != nil {
			_ = sc.stream.CloseRequest()
		}
		go func() {
			select {
			case <-g.runEnd:
			case <-time.After(2 * time.Second):
			}
			g.cancel()
		}()
	})
}

func (g *groupStream) settle(eventID string, ackType ironflowv1.AckType, delay time.Duration) error {
	if g.closed() {
		return ErrSubscriptionClosed
	}
	sc := g.current()
	if !sc.manual {
		// The server settles an AUTO event on handoff and would answer OK to
		// anything. Waiting for that answer could also deadlock against a reader
		// blocked on a full Events channel.
		g.warnAuto.Do(func() {
			g.logger.Warn("ack, nak and term do nothing on an AUTO consumer group; events are settled when delivered",
				"group", g.join.GetGroupName())
		})
		return nil
	}
	return sc.settle(eventID, ackType, delay)
}

func (g *groupStream) Ack(eventID string) error {
	return g.settle(eventID, ironflowv1.AckType_ACK_TYPE_ACK, 0)
}

func (g *groupStream) Nak(eventID string, delay time.Duration) error {
	return g.settle(eventID, ironflowv1.AckType_ACK_TYPE_NAK, delay)
}

func (g *groupStream) Term(eventID string) error {
	return g.settle(eventID, ironflowv1.AckType_ACK_TYPE_TERM, 0)
}
