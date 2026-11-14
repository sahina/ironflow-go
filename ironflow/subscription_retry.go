package ironflow

import (
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	ambiguousBodyRetries       = 3
	subscriptionStableInterval = 60 * time.Second
)

var subscriptionRetryNow = time.Now

// A body UnexpectedEOF can mean either a cut or a clean short HTTP/2 response.
// Progress alone cannot distinguish them, so only a stable stream resets it.
type subscriptionRetryBudget struct {
	used          int
	establishedAt time.Time
	deliveryWait  time.Duration
	now           func() time.Time
}

func newSubscriptionRetryBudget() subscriptionRetryBudget {
	return subscriptionRetryBudget{now: subscriptionRetryNow}
}

func (b *subscriptionRetryBudget) established() {
	if b.establishedAt.IsZero() {
		b.establishedAt = b.now()
	}
}

func (b *subscriptionRetryBudget) deliveryBlockedSince(start time.Time) {
	b.deliveryWait += b.now().Sub(start)
}

func (b *subscriptionRetryBudget) ended(err error) error {
	if !b.establishedAt.IsZero() && b.now().Sub(b.establishedAt)-b.deliveryWait >= subscriptionStableInterval {
		b.used = 0
	}
	b.establishedAt = time.Time{}
	b.deliveryWait = 0
	if !isAmbiguousBodyError(err) {
		return err
	}
	if b.used == ambiguousBodyRetries {
		return &subscriptionRetryExhausted{err}
	}
	b.used++
	return err
}

func isAmbiguousBodyError(err error) bool {
	var lost *lostConnectionError
	return errors.As(err, &lost) && errors.Is(lost.err, io.ErrUnexpectedEOF)
}

type subscriptionRetryExhausted struct{ err error }

func (e *subscriptionRetryExhausted) Error() string {
	return fmt.Sprintf("subscription retry budget exhausted after %d retries for ambiguous response-body failures (connection cut or short response): %v", ambiguousBodyRetries, e.err)
}
func (e *subscriptionRetryExhausted) Unwrap() error { return e.err }
