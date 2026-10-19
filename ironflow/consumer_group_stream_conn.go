package ironflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

type ackOutcome struct {
	res *ironflowv1.AckResult
	err error
}

// streamConn is one SubscribeBidirectional stream. A reconnect makes a new one,
// so its delivered set and ack_id counter die with the stream.
type streamConn struct {
	stream *connect.BidiStreamForClient[ironflowv1.SubscribeBidirectionalRequest, ironflowv1.SubscribeBidirectionalResponse]
	cancel context.CancelFunc
	endCh  chan struct{} // closed by finish; the queue drops events of an ended stream

	subID    string
	pattern  string
	manual   bool
	sendMu   sync.Mutex // connect forbids concurrent Send
	mu       sync.Mutex
	ended    bool
	resetErr error // why reset tore the stream down; read reports it
	nextAck  uint64
	pending  map[uint64]chan ackOutcome
	// MANUAL groups only: the copies of each event delivered on this stream and
	// not yet settled. A count, so a redelivery that arrives before the first
	// copy's result keeps its own entry.
	delivery  map[string]delivered
	lastPrune time.Time
	maxAge    time.Duration // deliveryMaxAge
}

type delivered struct {
	copies int
	at     time.Time // the latest copy's arrival
}

// deliveryMaxAge drops an unsettled entry this long after its latest copy
// arrived, so events a handler never settles do not grow the map forever.
//
// shortcut: a fixed age, as the server's AckWait is not known here. A late ack
// of a pruned event fails with ErrStaleAck and the server redelivers it, so
// delivery stays at-least-once. Upgrade if joined starts carrying the AckWait.
var deliveryMaxAge = time.Hour

func (sc *streamConn) deliveredCount() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return len(sc.delivery)
}

// finish marks the stream ended and fails every pending ack.
func (sc *streamConn) finish() {
	sc.mu.Lock()
	if !sc.ended {
		close(sc.endCh)
	}
	sc.ended = true
	for id, ch := range sc.pending {
		ch <- ackOutcome{err: fmt.Errorf("ironflow: the stream ended before the ack result arrived: %w", ErrStaleAck)} // buffered 1, one send per ack_id
		delete(sc.pending, id)
	}
	sc.mu.Unlock()
}

func (sc *streamConn) markDelivered(id string) {
	if !sc.manual {
		return
	}
	now := time.Now()
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if now.Sub(sc.lastPrune) >= sc.maxAge {
		for k, d := range sc.delivery {
			if now.Sub(d.at) >= sc.maxAge {
				delete(sc.delivery, k)
			}
		}
		sc.lastPrune = now
	}
	d := sc.delivery[id]
	d.copies++
	d.at = now
	sc.delivery[id] = d
}

// reset records why the stream is torn down, then tears it down. read reports
// that reason, which is retryable, instead of the Canceled the teardown causes.
func (sc *streamConn) reset(reason error) {
	sc.mu.Lock()
	if sc.resetErr == nil {
		sc.resetErr = reason
	}
	sc.mu.Unlock()
	sc.cancel()
}

func (sc *streamConn) resetReason() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.resetErr
}

func (sc *streamConn) resolve(res *ironflowv1.AckResult) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, ok := sc.pending[res.GetAckId()]
	if !ok {
		return // timed out already
	}
	delete(sc.pending, res.GetAckId())
	ch <- ackOutcome{res: res}
}

func (sc *streamConn) settle(eventID string, ackType ironflowv1.AckType, delay time.Duration) error {
	delayMs, err := nakDelayMs(eventID, delay)
	if err != nil {
		return err
	}
	sc.mu.Lock()
	if sc.ended {
		sc.mu.Unlock()
		return ErrStaleAck
	}
	if sc.manual {
		if _, ok := sc.delivery[eventID]; !ok {
			sc.mu.Unlock()
			return ErrStaleAck
		}
	}
	sc.nextAck++
	ackID := sc.nextAck
	ch := make(chan ackOutcome, 1)
	sc.pending[ackID] = ch
	sc.mu.Unlock()

	forget := func() {
		sc.mu.Lock()
		delete(sc.pending, ackID)
		sc.mu.Unlock()
	}

	// The deadline covers the wait for sendMu and a Send that blocks on HTTP/2
	// flow control. A Send still blocked then means the server stopped reading:
	// reset the stream, which frees this Send and every acker queued behind it,
	// and the reader reconnects.
	timeout := ackTimeout
	deadline := time.Now().Add(timeout)
	stuck := time.AfterFunc(timeout, func() {
		sc.reset(connect.NewError(connect.CodeUnavailable, fmt.Errorf("an ack send blocked for %s", timeout)))
	})
	sc.sendMu.Lock()
	err = sc.stream.Send(&ironflowv1.SubscribeBidirectionalRequest{Payload: &ironflowv1.SubscribeBidirectionalRequest_Ack{
		Ack: &ironflowv1.SubscriptionAck{
			EventId:          eventID,
			AckType:          ackType,
			RedeliverDelayMs: delayMs,
			AckId:            ackID,
		},
	}})
	sc.sendMu.Unlock()
	if !stuck.Stop() {
		forget()
		return fmt.Errorf("ironflow: ack of event %s not sent within %s", eventID, timeout)
	}
	if err != nil {
		forget()
		// The stream can end between the ended check above and this Send, before
		// the reader's finish() runs. That is a stale ack, not a send failure.
		sc.mu.Lock()
		ended := sc.ended
		sc.mu.Unlock()
		if ended || errors.Is(err, io.EOF) {
			return fmt.Errorf("ironflow: the stream ended before the ack was sent: %w", ErrStaleAck)
		}
		return fmt.Errorf("ironflow: send ack: %w", err)
	}

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case out := <-ch:
		if out.err != nil {
			return out.err
		}
		return sc.applyResult(eventID, out.res)
	case <-timer.C:
		forget()
		return fmt.Errorf("ironflow: no ack result for event %s within %s", eventID, timeout)
	}
}

func (sc *streamConn) applyResult(eventID string, res *ironflowv1.AckResult) error {
	if res.GetCode() != ironflowv1.AckResultCode_ACK_RESULT_CODE_OK {
		return &AckError{
			EventID: eventID,
			Code:    strings.ToLower(strings.TrimPrefix(res.GetCode().String(), "ACK_RESULT_CODE_")),
			Message: res.GetMessage(),
		}
	}
	// One copy is settled. A NAK's redelivery adds its own copy when it arrives.
	sc.mu.Lock()
	if d, ok := sc.delivery[eventID]; ok {
		if d.copies <= 1 {
			delete(sc.delivery, eventID)
		} else {
			d.copies--
			sc.delivery[eventID] = d
		}
	}
	sc.mu.Unlock()
	return nil
}
