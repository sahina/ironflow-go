package ironflow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

// getStreamHTTPClient returns the Client's shared h2c-capable HTTP
// client for server-streaming RPCs. Built lazily on first use and
// reused across calls so repeated streams share the H2 connection
// pool rather than churning transports.
func (c *Client) getStreamHTTPClient() *http.Client {
	c.streamHTTPOnce.Do(func() {
		c.streamHTTPClient = newH2CClient(c.serverURL)
	})
	return c.streamHTTPClient
}

// WaitProgress is one frame delivered on the channel returned by
// WaitForProjectionStream. Progress frames report the current cursor as
// the projection advances; the terminal frame (Terminal=true) is emitted
// exactly once with either CaughtUp, TimedOut, or Err set. Heartbeat
// frames are filtered out inside the SDK and do not reach the channel.
//
// Issue #476. Pairs with the unary WaitResult returned by WaitForProjection.
type WaitProgress struct {
	CurrentSeq     uint64
	TargetSeq      uint64
	BehindByEvents int64
	CaughtUp       bool
	TimedOut       bool
	Mode           string
	Err            error
	Terminal       bool
}

// WaitForProjectionStream opens a streaming wait against the given
// projection and returns a receive-only channel that yields WaitProgress
// frames plus a cancel func that closes the stream and drains the channel.
//
// The channel is closed after the terminal frame (CaughtUp, TimedOut, or
// Err). Heartbeats are filtered inside the SDK — callers only see progress
// and the terminal frame. Server-side cap is 300s; requesting a timeout
// above that is rejected with an InvalidArgument error.
//
// Example (issue #476):
//
//	ch, cancel, err := client.WaitForProjectionStream(ctx, "order-view", ironflow.WaitForProjectionOpts{
//	    MinSeq:  42,
//	    Timeout: 5 * time.Minute,
//	})
//	if err != nil { return err }
//	defer cancel()
//	for p := range ch {
//	    if p.Terminal {
//	        if p.CaughtUp { return nil }
//	        return fmt.Errorf("wait failed: timedOut=%v err=%v", p.TimedOut, p.Err)
//	    }
//	    log.Printf("progress: %d/%d behind=%d", p.CurrentSeq, p.TargetSeq, p.BehindByEvents)
//	}
//	return nil
//
// The cancel func is idempotent and safe to call from any goroutine. It
// terminates the background stream reader; any frames already buffered
// in the channel are drained before the channel is closed.
func (c *Client) WaitForProjectionStream(ctx context.Context, name string, opts WaitForProjectionOpts) (<-chan WaitProgress, func(), error) {
	if name == "" {
		return nil, nil, fmt.Errorf("name is required")
	}

	req := &ironflowv1.WaitProjectionCatchupRequest{
		Name:      name,
		MinSeq:    opts.MinSeq,
		Partition: opts.Partition,
	}
	if opts.Timeout > 0 {
		req.Timeout = durationpb.New(opts.Timeout)
	}

	// Streaming needs an HTTP client without a per-request timeout. The
	// Client's shared httpClient typically has one (default 60s) which
	// would kill a long wait. Use the shared h2c-capable streaming
	// client; repeated stream calls reuse the H2 connection pool.
	projClient := ironflowv1connect.NewProjectionServiceClient(
		c.getStreamHTTPClient(),
		c.serverURL,
		connect.WithProtoJSON(),
	)

	streamCtx, cancelCtx := context.WithCancel(ctx)
	creq := connect.NewRequest(req)
	if c.apiKey != "" {
		creq.Header().Set("Authorization", "Bearer "+c.apiKey)
	}

	stream, err := projClient.WaitProjectionCatchupStream(streamCtx, creq)
	if err != nil {
		cancelCtx()
		return nil, nil, err
	}

	out := make(chan WaitProgress, 8)

	var cancelOnce sync.Once
	cancel := func() {
		cancelOnce.Do(func() {
			cancelCtx()
			_ = stream.Close()
		})
	}

	go runWaitStream(streamCtx, &connectStreamAdapter{stream}, opts.MinSeq, out, cancel)

	return out, cancel, nil
}

// waitFrameStream is the minimal receiver surface runWaitStream consumes.
// The generated *connect.ServerStreamForClient satisfies it via the
// connectStreamAdapter; tests supply their own implementation.
type waitFrameStream interface {
	Receive() bool
	Msg() *ironflowv1.WaitProjectionCatchupStreamResponse
	Err() error
}

// connectStreamAdapter satisfies waitFrameStream without taking on the
// concrete *ServerStreamForClient type so tests can swap it out.
type connectStreamAdapter struct {
	s *connect.ServerStreamForClient[ironflowv1.WaitProjectionCatchupStreamResponse]
}

func (a *connectStreamAdapter) Receive() bool { return a.s.Receive() }
func (a *connectStreamAdapter) Msg() *ironflowv1.WaitProjectionCatchupStreamResponse {
	return a.s.Msg()
}
func (a *connectStreamAdapter) Err() error { return a.s.Err() }

// runWaitStream filters frames from the server stream and pushes
// user-visible WaitProgress values onto out. Closes out and invokes
// cancel on exit. Factored out of WaitForProjectionStream so it can be
// unit-tested against a mock waitFrameStream.
func runWaitStream(streamCtx context.Context, stream waitFrameStream, minSeq uint64, out chan<- WaitProgress, cancel func()) {
	defer close(out)
	defer cancel()

	send := func(p WaitProgress) bool {
		select {
		case out <- p:
			return true
		case <-streamCtx.Done():
			return false
		}
	}

	// Track the last-seen cursor so a synthetic fallback terminal frame
	// (on transport error without DONE) can report a useful CurrentSeq
	// rather than 0.
	var lastSeen uint64
	for stream.Receive() {
		m := stream.Msg()
		switch m.Kind {
		case ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_HEARTBEAT:
			// Transport-only keepalive — do not surface.
			continue
		case ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_PROGRESS:
			lastSeen = m.CurrentSeq
			if !send(WaitProgress{
				CurrentSeq:     m.CurrentSeq,
				TargetSeq:      m.TargetSeq,
				BehindByEvents: m.BehindByEvents,
				Mode:           m.Mode,
			}) {
				return
			}
		case ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_DONE:
			p := WaitProgress{
				CurrentSeq:     m.CurrentSeq,
				TargetSeq:      m.TargetSeq,
				BehindByEvents: m.BehindByEvents,
				CaughtUp:       m.CaughtUp,
				TimedOut:       m.TimedOut,
				Mode:           m.Mode,
				Terminal:       true,
			}
			if m.Error != "" {
				p.Err = errors.New(m.Error)
			}
			_ = send(p)
			return
		default:
			// Unknown / UNSPECIFIED frame kind — ignore for forward compat.
			continue
		}
	}

	// Stream ended without a DONE frame — surface any transport error
	// as a terminal frame so consumers don't see a silent close.
	if err := stream.Err(); err != nil && !errors.Is(err, context.Canceled) {
		_ = send(WaitProgress{
			CurrentSeq: lastSeen,
			TargetSeq:  minSeq,
			Err:        err,
			Terminal:   true,
		})
	}
}
