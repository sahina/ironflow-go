package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// DefaultCommandDedupTTL is the recommended TTL for command dedup entries (7 days).
// Pass this to CommandDedupOptions.TTL for a 7-day expiry.
const DefaultCommandDedupTTL = 7 * 24 * time.Hour

// CommandDedupOptions configures a CommandDedup instance.
type CommandDedupOptions struct {
	// TTL is the time-to-live for dedup entries.
	// 0 (zero value) = no expiry. Pass DefaultCommandDedupTTL for a 7-day TTL.
	// This matches the Node SDK convention where ttlSeconds=0 means no expiry.
	TTL time.Duration
}

// CommandDedup provides atomic command-level idempotency backed by NATS KV.
//
// Uses the claim-first pattern: TryClaim atomically reserves the commandId
// before any handler work is done. The winner returns (nil, nil) and proceeds.
// Losers receive the prior entry immediately without re-running the handler.
//
// Typical usage:
//
//	prior, err := dedup.TryClaim(ctx, commandId, claim)
//	if err != nil { return err }
//	if prior != nil { return prior, nil } // duplicate — return cached result
//	defer func() {
//	    if err != nil { _ = dedup.Release(ctx, commandId) }
//	}()
//	result, err := runHandler()
//	if err != nil { return nil, err }
//	if err = dedup.Finalize(ctx, commandId, result); err != nil { return nil, err }
//	return result, nil
//
// IMPORTANT: Do not call Release after Finalize. Release must only be called
// on handler failure, before Finalize succeeds. Calling Release after Finalize
// deletes the finalized result and allows replay of the command.
//
// Use NewCommandDedup to create an instance.
type CommandDedup[T any] struct {
	kv         *KVClient
	bucketName string
	ttl        time.Duration
	once       sync.Once
	initErr    error
}

// NewCommandDedup creates a CommandDedup and ensures the KV bucket exists.
// bucketName is the KV bucket to use for storing dedup entries.
// Store the returned instance and reuse it — do not call NewCommandDedup per request.
func NewCommandDedup[T any](ctx context.Context, kv *KVClient, bucketName string, opts CommandDedupOptions) (*CommandDedup[T], error) {
	d := &CommandDedup[T]{
		kv:         kv,
		bucketName: bucketName,
		ttl:        opts.TTL,
	}
	if err := d.ensureBucket(ctx); err != nil {
		return nil, err
	}
	return d, nil
}

func isHTTPCode(err error, code string) bool {
	var e *IronflowError
	return errors.As(err, &e) && e.Code == code
}

// ensureBucket creates the KV bucket if it doesn't exist.
// Uses sync.Once: NewCommandDedup calls this eagerly, so it runs exactly once
// across all callers and the fast path is lock-free after initialization.
//
// If bucket creation fails with a transient error (e.g. 503), initErr is set
// permanently — all subsequent calls on this instance return the same error.
// Recovery requires creating a new NewCommandDedup instance.
func (d *CommandDedup[T]) ensureBucket(ctx context.Context) error {
	d.once.Do(func() {
		var cfg BucketConfig
		cfg.Name = d.bucketName
		if d.ttl > 0 {
			cfg.TTL = d.ttl
		}
		_, err := d.kv.CreateBucket(ctx, cfg)
		if err != nil && !isHTTPCode(err, "HTTP_409") {
			d.initErr = err
		}
	})
	return d.initErr
}

// TryClaim atomically claims commandId. Returns (nil, nil) if this caller wins
// the race and should proceed with the handler. Returns (*T, nil) with the prior
// entry if another caller already claimed this commandId (dedup hit — return the
// prior result to your caller without re-running the handler).
//
// The returned *T may be the initial claim if the winner has not yet called
// Finalize. Design T with optional fields for data only available after Finalize.
func (d *CommandDedup[T]) TryClaim(ctx context.Context, commandId string, claim T) (*T, error) {
	if err := d.ensureBucket(ctx); err != nil {
		return nil, err
	}
	b, err := json.Marshal(claim)
	if err != nil {
		return nil, err
	}
	// The KV layer (kv.go) URL-encodes keys internally — pass commandId verbatim.
	_, err = d.kv.Bucket(d.bucketName).Create(ctx, commandId, b)
	if err == nil {
		return nil, nil // winner — proceed
	}
	if !isHTTPCode(err, "HTTP_412") {
		return nil, err
	}
	// loser — read winner's entry
	entry, err := d.kv.Bucket(d.bucketName).Get(ctx, commandId)
	if err != nil {
		if isHTTPCode(err, "HTTP_404") {
			return nil, nil // concurrent delete race — treat as winner
		}
		return nil, err
	}
	var prior T
	if jsonErr := json.Unmarshal(entry.Value, &prior); jsonErr != nil {
		return nil, jsonErr // corrupt entry — propagate so the operator can investigate
	}
	return &prior, nil
}

// Finalize updates the dedup entry with the handler's final result. Subsequent
// callers that TryClaim the same commandId will receive this value.
func (d *CommandDedup[T]) Finalize(ctx context.Context, commandId string, result T) error {
	if err := d.ensureBucket(ctx); err != nil {
		return err
	}
	b, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = d.kv.Bucket(d.bucketName).Put(ctx, commandId, b)
	return err
}

// Release deletes the claim so retries can proceed after a handler failure.
// Swallows 404 (already released — idempotent).
//
// Only call Release in error-handling paths before Finalize succeeds.
func (d *CommandDedup[T]) Release(ctx context.Context, commandId string) error {
	if err := d.ensureBucket(ctx); err != nil {
		return err
	}
	err := d.kv.Bucket(d.bucketName).Delete(ctx, commandId)
	if isHTTPCode(err, "HTTP_404") {
		return nil
	}
	return err
}
