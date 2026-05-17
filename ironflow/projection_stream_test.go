package ironflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

// fakeFrameStream implements waitFrameStream for unit tests, yielding
// canned frames in order and optionally an error after the last frame.
type fakeFrameStream struct {
	frames []*ironflowv1.WaitProjectionCatchupStreamResponse
	i      int
	err    error
}

func (f *fakeFrameStream) Receive() bool {
	if f.i >= len(f.frames) {
		return false
	}
	f.i++
	return true
}

func (f *fakeFrameStream) Msg() *ironflowv1.WaitProjectionCatchupStreamResponse {
	return f.frames[f.i-1]
}

func (f *fakeFrameStream) Err() error { return f.err }

// drainWaitProgress collects frames from out until it closes OR deadline
// fires. Fails the test on deadline.
func drainWaitProgress(t *testing.T, out <-chan WaitProgress, deadline time.Duration) []WaitProgress {
	t.Helper()
	var frames []WaitProgress
	d := time.After(deadline)
	for {
		select {
		case p, ok := <-out:
			if !ok {
				return frames
			}
			frames = append(frames, p)
		case <-d:
			t.Fatalf("drainWaitProgress: timeout after %v; got %d frames", deadline, len(frames))
			return frames
		}
	}
}

func TestRunWaitStream_FiltersHeartbeats(t *testing.T) {
	fs := &fakeFrameStream{
		frames: []*ironflowv1.WaitProjectionCatchupStreamResponse{
			{Kind: ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_PROGRESS, CurrentSeq: 10, TargetSeq: 100, BehindByEvents: 90},
			{Kind: ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_HEARTBEAT, TargetSeq: 100},
			{Kind: ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_HEARTBEAT, TargetSeq: 100},
			{Kind: ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_PROGRESS, CurrentSeq: 50, TargetSeq: 100, BehindByEvents: 50},
			{Kind: ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_DONE, CurrentSeq: 100, TargetSeq: 100, CaughtUp: true},
		},
	}
	out := make(chan WaitProgress, 8)
	var cancelCalls int
	cancel := func() { cancelCalls++ }

	runWaitStream(context.Background(), fs, 100, out, cancel)

	frames := drainWaitProgress(t, out, time.Second)
	if len(frames) != 3 {
		t.Fatalf("expected 3 user-visible frames (2 progress + 1 done), got %d: %+v", len(frames), frames)
	}
	if frames[0].CurrentSeq != 10 || frames[1].CurrentSeq != 50 {
		t.Errorf("progress frames out of order: %+v", frames)
	}
	last := frames[2]
	if !last.Terminal || !last.CaughtUp {
		t.Errorf("expected terminal caught-up frame, got %+v", last)
	}
	if cancelCalls != 1 {
		t.Errorf("expected cancel to be called exactly once, got %d", cancelCalls)
	}
}

func TestRunWaitStream_SkipsUnspecified(t *testing.T) {
	fs := &fakeFrameStream{
		frames: []*ironflowv1.WaitProjectionCatchupStreamResponse{
			{Kind: ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_UNSPECIFIED},
			{Kind: ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_DONE, CaughtUp: true, TargetSeq: 1, CurrentSeq: 1},
		},
	}
	out := make(chan WaitProgress, 4)
	runWaitStream(context.Background(), fs, 1, out, func() {})
	frames := drainWaitProgress(t, out, time.Second)
	if len(frames) != 1 || !frames[0].Terminal {
		t.Errorf("expected single terminal frame (UNSPECIFIED skipped), got %+v", frames)
	}
}

func TestRunWaitStream_DoneWithError_SurfacesErr(t *testing.T) {
	fs := &fakeFrameStream{
		frames: []*ironflowv1.WaitProjectionCatchupStreamResponse{
			{
				Kind:       ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_DONE,
				CurrentSeq: 5,
				TargetSeq:  100,
				Error:      "projection paused",
			},
		},
	}
	out := make(chan WaitProgress, 4)
	runWaitStream(context.Background(), fs, 100, out, func() {})
	frames := drainWaitProgress(t, out, time.Second)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	f := frames[0]
	if !f.Terminal || f.Err == nil || f.Err.Error() != "projection paused" {
		t.Errorf("expected terminal frame with Err='projection paused', got %+v", f)
	}
}

func TestRunWaitStream_TransportErrorWithoutDone_EmitsSyntheticTerminal(t *testing.T) {
	fs := &fakeFrameStream{
		frames: []*ironflowv1.WaitProjectionCatchupStreamResponse{
			{Kind: ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_PROGRESS, CurrentSeq: 42, TargetSeq: 100, BehindByEvents: 58},
		},
		err: errors.New("connection reset"),
	}
	out := make(chan WaitProgress, 4)
	runWaitStream(context.Background(), fs, 100, out, func() {})
	frames := drainWaitProgress(t, out, time.Second)
	if len(frames) != 2 {
		t.Fatalf("expected progress + synthetic terminal, got %d frames: %+v", len(frames), frames)
	}
	term := frames[1]
	if !term.Terminal || term.Err == nil || term.CurrentSeq != 42 {
		t.Errorf("expected synthetic terminal with Err set and lastSeen=42, got %+v", term)
	}
}

func TestRunWaitStream_CancelledCtx_IsNotSurfaced(t *testing.T) {
	// context.Canceled on transport is expected when the caller cancels.
	// Should NOT produce a synthetic terminal — caller already knows.
	fs := &fakeFrameStream{frames: nil, err: context.Canceled}
	out := make(chan WaitProgress, 4)
	runWaitStream(context.Background(), fs, 100, out, func() {})
	frames := drainWaitProgress(t, out, time.Second)
	if len(frames) != 0 {
		t.Errorf("expected no frames on context.Canceled transport error, got %+v", frames)
	}
}

func TestRunWaitStream_CtxCancelMidStream_StopsCleanly(t *testing.T) {
	fs := &fakeFrameStream{
		frames: []*ironflowv1.WaitProjectionCatchupStreamResponse{
			{Kind: ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_PROGRESS, CurrentSeq: 1, TargetSeq: 100},
			{Kind: ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_PROGRESS, CurrentSeq: 2, TargetSeq: 100},
			{Kind: ironflowv1.WaitStreamFrameKind_WAIT_STREAM_FRAME_KIND_DONE, CaughtUp: true, TargetSeq: 100, CurrentSeq: 100},
		},
	}
	// Buffer 0 so send() blocks without a reader.
	out := make(chan WaitProgress)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	done := make(chan struct{})
	go func() {
		runWaitStream(ctx, fs, 100, out, func() {})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runWaitStream did not exit on ctx.Cancel with no reader")
	}
}

func TestClient_GetStreamHTTPClient_IsCached(t *testing.T) {
	// Use a local Client with a stub serverURL to verify the lazy
	// initializer returns the same *http.Client on repeated calls.
	c := &Client{serverURL: "http://localhost:0"}
	var got1, got2 interface{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); got1 = c.getStreamHTTPClient() }()
	go func() { defer wg.Done(); got2 = c.getStreamHTTPClient() }()
	wg.Wait()
	if got1 == nil || got2 == nil {
		t.Fatal("getStreamHTTPClient returned nil")
	}
	if got1 != got2 {
		t.Errorf("expected same *http.Client on concurrent calls; got two different instances")
	}
}
