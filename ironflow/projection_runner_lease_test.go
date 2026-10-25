package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

// leaseEvent is one event that the fake server sends on Poll or Stream.
type leaseEvent struct {
	id  string
	seq int64
	pk  string
}

// leaseCall is a Save or Ack that the fake server received.
type leaseCall struct {
	pk      string
	epoch   int64 // 0: no lease
	aborted bool
}

// leaseFake is a server with the ADR 0120 lease rules: one holder, a fence on
// the epoch, and Aborted for a stale epoch on Poll, Stream, Save, Ack and Renew.
type leaseFake struct {
	mu      sync.Mutex
	epoch   int64
	held    bool
	ttl     string
	noLease bool // an older server: the lease RPCs answer 404

	batches     [][]leaseEvent // each pull takes the next batch
	pulls       []int64        // epoch of each Poll and Stream (0: no lease)
	saves       []leaseCall
	acquires    int
	renews      int
	renewStatus int                  // non-zero: Renew fails with this HTTP status
	fenceSave   func(pk string) bool // true: a rebuild start fences this save
	streamFence bool                 // the stream ends with Aborted when the epoch changes
	order       []string             // Save, Ack and Release, in arrival order

	// Outside mu: these hold a request open without blocking the fake.
	renewHang    atomic.Bool  // Renew never answers
	hungRenews   atomic.Int32 // Renews that hung
	acquireDelay time.Duration
	acquiring    atomic.Bool // an Acquire is in flight
}

func newLeaseFake(t *testing.T, batches ...[]leaseEvent) *leaseFake {
	t.Helper()
	// Short timings: Renew every 100 ms (ttl 0.3s), standby every ~50 ms.
	standby, renew := projectionStandbyRetry, projectionRenewRetry
	projectionStandbyRetry, projectionRenewRetry = 50*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { projectionStandbyRetry, projectionRenewRetry = standby, renew })
	return &leaseFake{ttl: "0.3s", batches: batches}
}

func leaseLostError() *connect.Error {
	return connect.NewError(connect.CodeAborted, errors.New("projection lease lost"))
}

func writeLeaseLost(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)
	_, _ = w.Write([]byte(`{"code":"aborted","message":"projection lease lost"}`))
}

// stale reports whether a request lease no longer holds. Caller holds mu.
func (f *leaseFake) stale(epoch int64) bool {
	return epoch != 0 && (!f.held || epoch != f.epoch)
}

// takeOver gives the lease to another runner, as when the lease expires.
func (f *leaseFake) takeOver() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.epoch++
	f.held = true
}

// free ends the other runner's lease.
func (f *leaseFake) free() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.epoch++
	f.held = false
}

func (f *leaseFake) nextBatch() []leaseEvent {
	if len(f.batches) == 0 {
		return nil
	}
	b := f.batches[0]
	f.batches = f.batches[1:]
	return b
}

func (f *leaseFake) snapshot() (pulls []int64, saves []leaseCall, epoch int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.pulls), slices.Clone(f.saves), f.epoch
}

func requestEpoch(body map[string]any) int64 {
	lease, ok := body["lease"].(map[string]any)
	if !ok {
		return 0
	}
	var epoch ProtoInt64
	raw, _ := json.Marshal(lease["epoch"])
	_ = epoch.UnmarshalJSON(raw)
	return int64(epoch)
}

func (f *leaseFake) unary(w http.ResponseWriter, req *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(req.Body).Decode(&body)
	method := strings.TrimPrefix(req.URL.Path, projectionServicePath)
	epoch := requestEpoch(body)
	if method == "RenewProjectionLease" && f.renewHang.Load() {
		f.hungRenews.Add(1)
		<-req.Context().Done()
		return
	}
	if method == "AcquireProjectionLease" && f.acquireDelay > 0 {
		f.acquiring.Store(true)
		time.Sleep(f.acquireDelay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.noLease && strings.HasSuffix(method, "ProjectionLease") {
		http.NotFound(w, req)
		return
	}
	switch method {
	case "RegisterProjection", "GetProjection":
		_, _ = w.Write([]byte(`{}`))
	case "AcquireProjectionLease":
		f.acquires++
		if f.held {
			_, _ = w.Write([]byte(`{}`)) // granted == false
			return
		}
		f.epoch++
		f.held = true
		_, _ = fmt.Fprintf(w, `{"granted":true,"lease":{"token":"sdk:%d","epoch":"%d"},"ttl":%q}`, f.epoch, f.epoch, f.ttl)
	case "RenewProjectionLease":
		f.renews++
		switch {
		case f.stale(epoch):
			writeLeaseLost(w)
		case f.renewStatus != 0:
			http.Error(w, "unavailable", f.renewStatus)
		default:
			_, _ = fmt.Fprintf(w, `{"ttl":%q}`, f.ttl)
		}
	case "ReleaseProjectionLease":
		f.order = append(f.order, "release")
		if epoch == f.epoch {
			f.epoch++
			f.held = false
		}
		_, _ = w.Write([]byte(`{}`))
	case "PollProjectionEvents":
		f.pulls = append(f.pulls, epoch)
		if f.stale(epoch) {
			writeLeaseLost(w)
			return
		}
		var events []map[string]any
		for _, e := range f.nextBatch() {
			evt := map[string]any{"id": e.id, "name": "inc", "seq": e.seq}
			if e.pk != "" {
				evt["metadata"] = map[string]any{"__partition": e.pk}
			}
			events = append(events, evt)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"events": events})
	case "SaveProjectionState", "AckProjectionEvents":
		pk, _ := body["partitionKey"].(string)
		call := leaseCall{pk: pk, epoch: epoch, aborted: f.stale(epoch)}
		if !call.aborted && f.fenceSave != nil && f.fenceSave(pk) {
			f.epoch++ // a rebuild start: the lease is free at once
			f.held = false
			call.aborted = true
		}
		f.saves = append(f.saves, call)
		f.order = append(f.order, method)
		if call.aborted {
			writeLeaseLost(w)
		}
	default:
		http.NotFound(w, req)
	}
}

func (f *leaseFake) stream(ctx context.Context, req *connect.Request[ironflowv1.StreamProjectionEventsRequest], s *connect.ServerStream[ironflowv1.ProjectionEvent]) error {
	var epoch int64
	if req.Msg.Lease != nil {
		epoch = req.Msg.Lease.Epoch
	}
	f.mu.Lock()
	f.pulls = append(f.pulls, epoch)
	stale, batch := f.stale(epoch), []leaseEvent(nil)
	if !stale {
		batch = f.nextBatch()
	}
	f.mu.Unlock()
	if stale {
		return leaseLostError()
	}
	for _, e := range batch {
		if err := s.Send(streamEvent(e.id, e.seq, e.pk)); err != nil {
			return err
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(10 * time.Millisecond):
		}
		f.mu.Lock()
		stale := f.streamFence && f.stale(epoch)
		f.mu.Unlock()
		if stale {
			return leaseLostError() // the server's check before each pull cycle
		}
	}
}

// server serves the fake over Connect. The poll transport answers 404 for
// StreamProjectionEvents, so the runner falls back to poll.
func (f *leaseFake) server(t *testing.T, transport string) string {
	t.Helper()
	var stream streamFunc
	if transport == "stream" {
		stream = f.stream
	}
	return newProjectionServer(t, http.NewServeMux(), stream, f.unary).URL
}

// leaseLogger records Info and Warn messages.
type leaseLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (l *leaseLogger) Debug(string, ...any) {}
func (l *leaseLogger) Info(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.msgs = append(l.msgs, msg)
}
func (l *leaseLogger) Warn(msg string, args ...any)  { l.Info(msg, args...) }
func (l *leaseLogger) Error(msg string, args ...any) { l.Info(msg, args...) }

func (l *leaseLogger) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, m := range l.msgs {
		if m == msg {
			n++
		}
	}
	return n
}

func startLeaseRunner(t *testing.T, url string, log Logger) *ProjectionRunner {
	t.Helper()
	return startLeaseProjection(t, counterProjection(10), url, log)
}

func startLeaseProjection(t *testing.T, proj Projection, url string, log Logger) *ProjectionRunner {
	t.Helper()
	runner := NewProjectionRunner(proj, url, nil, log)
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Stop)
	return runner
}

var leaseTransports = []string{"poll", "stream"}

const (
	logGranted = "Projection lease granted"
	logStandby = "Projection lease held by another runner, standing by"
	logLost    = "Projection lease lost, standing by"
)

func TestProjectionLease_TwoRunnersOnlyHolderReads(t *testing.T) {
	for _, transport := range leaseTransports {
		t.Run(transport, func(t *testing.T) {
			f := newLeaseFake(t, []leaseEvent{{"e1", 1, ""}})
			url := f.server(t, transport)
			holderLog, standbyLog := &leaseLogger{}, &leaseLogger{}
			startLeaseRunner(t, url, holderLog)
			waitFor(t, "the first grant", func() bool { return holderLog.count(logGranted) == 1 })
			startLeaseRunner(t, url, standbyLog)
			waitFor(t, "standby retries", func() bool {
				f.mu.Lock()
				defer f.mu.Unlock()
				return f.acquires >= 4
			})
			waitFor(t, "the holder's save", func() bool { _, saves, _ := f.snapshot(); return len(saves) == 1 })

			pulls, saves, _ := f.snapshot()
			for _, epoch := range pulls {
				if epoch != 1 {
					t.Fatalf("pull with epoch %d, want only the holder (epoch 1): %v", epoch, pulls)
				}
			}
			if len(saves) == 0 || saves[0].epoch != 1 {
				t.Errorf("saves = %v, want the holder's save with epoch 1", saves)
			}
			if standbyLog.count(logStandby) != 1 || standbyLog.count(logGranted) != 0 {
				t.Errorf("standby logs = %v, want one standby line and no grant", standbyLog.msgs)
			}
		})
	}
}

func TestProjectionLease_StandbyTakesOverAfterHolderStops(t *testing.T) {
	for _, transport := range leaseTransports {
		t.Run(transport, func(t *testing.T) {
			f := newLeaseFake(t, []leaseEvent{{"e1", 1, ""}})
			url := f.server(t, transport)
			holderLog, standbyLog := &leaseLogger{}, &leaseLogger{}
			holder := startLeaseRunner(t, url, holderLog)
			waitFor(t, "the first grant", func() bool { return holderLog.count(logGranted) == 1 })
			startLeaseRunner(t, url, standbyLog)
			waitFor(t, "standby", func() bool { return standbyLog.count(logStandby) == 1 })

			holder.Stop() // Release: epoch 2, the lease is free
			f.mu.Lock()
			f.batches = [][]leaseEvent{{{"e2", 2, ""}}}
			f.mu.Unlock()
			waitFor(t, "the takeover", func() bool { return standbyLog.count(logGranted) == 1 })
			waitFor(t, "a save from the new holder", func() bool {
				_, saves, _ := f.snapshot()
				return slices.ContainsFunc(saves, func(c leaseCall) bool { return c.epoch == 3 })
			})
			pulls, _, _ := f.snapshot()
			if !slices.Contains(pulls, 3) {
				t.Errorf("pulls = %v, want the new holder to read with epoch 3", pulls)
			}
		})
	}
}

func TestProjectionLease_FencedSaveStopsBatchNoRetry(t *testing.T) {
	for _, transport := range leaseTransports {
		t.Run(transport, func(t *testing.T) {
			batch := []leaseEvent{{"a1", 1, "a"}, {"b1", 2, "b"}, {"a2", 3, "a"}}
			// The server rewinds after the fence, so the new holder reads the batch again.
			f := newLeaseFake(t, batch, batch)
			fenced := false
			f.fenceSave = func(pk string) bool {
				if pk == "b" && !fenced {
					fenced = true
					return true
				}
				return false
			}
			log := &leaseLogger{}
			startLeaseRunner(t, f.server(t, transport), log)
			waitFor(t, "the batch saved under the new lease", func() bool {
				_, saves, _ := f.snapshot()
				return len(saves) == 5
			})

			_, saves, _ := f.snapshot()
			want := []leaseCall{
				{pk: "a", epoch: 1}, {pk: "b", epoch: 1, aborted: true},
				{pk: "a", epoch: 3}, {pk: "b", epoch: 3}, {pk: "a", epoch: 3},
			}
			if !slices.Equal(saves, want) {
				t.Errorf("saves = %v, want %v (stop at the fenced save, no retry with epoch 1)", saves, want)
			}
			if log.count(logLost) != 1 || log.count(logGranted) != 2 {
				t.Errorf("logs = %v, want one lost and two grants", log.msgs)
			}
		})
	}
}

func TestProjectionLease_ExpiredLeaseGoesToStandby(t *testing.T) {
	for _, transport := range leaseTransports {
		t.Run(transport, func(t *testing.T) {
			// The stream does not end on its own: Renew finds the lost lease and
			// the runner drops the two pending frames.
			interval := projectionFlushInterval
			projectionFlushInterval = time.Hour
			t.Cleanup(func() { projectionFlushInterval = interval })
			f := newLeaseFake(t, []leaseEvent{{"e1", 1, ""}, {"e2", 2, ""}}, []leaseEvent{{"e3", 3, ""}})
			log := &leaseLogger{}
			var handled atomic.Int32
			proj := counterProjection(10)
			count := proj.Config.Handler
			proj.Config.Handler = func(s map[string]any, e ProjectionEvent, c ProjectionContext) (map[string]any, error) {
				handled.Add(1)
				return count(s, e, c)
			}
			startLeaseProjection(t, proj, f.server(t, transport), log)
			waitFor(t, "the first grant", func() bool { return log.count(logGranted) == 1 })
			if transport == "stream" {
				waitFor(t, "the first stream", func() bool { pulls, _, _ := f.snapshot(); return len(pulls) == 1 })
			}

			f.takeOver() // the lease expired and another runner took it
			waitFor(t, "standby", func() bool { return log.count(logLost) == 1 && log.count(logStandby) == 1 })
			f.free()
			waitFor(t, "a new grant", func() bool { return log.count(logGranted) == 2 })
			_, _, epoch := f.snapshot()
			waitFor(t, "a pull with the new lease", func() bool {
				pulls, _, _ := f.snapshot()
				return slices.Contains(pulls, epoch)
			})
			if transport == "stream" {
				_, saves, _ := f.snapshot()
				if n := handled.Load(); n != 0 || len(saves) != 0 {
					t.Errorf("handled %d events, saves = %v; want the frames of the lost lease dropped", n, saves)
				}
			}
		})
	}
}

func TestProjectionLease_StreamAbortedDropsPendingFrames(t *testing.T) {
	interval := projectionFlushInterval
	projectionFlushInterval = time.Hour
	t.Cleanup(func() { projectionFlushInterval = interval })
	f := newLeaseFake(t, []leaseEvent{{"e1", 1, ""}, {"e2", 2, ""}})
	f.ttl = "30s" // no Renew in this test: only the stream reports the lost lease
	f.streamFence = true
	log := &leaseLogger{}
	startLeaseRunner(t, f.server(t, "stream"), log)
	waitFor(t, "the first stream", func() bool { pulls, _, _ := f.snapshot(); return len(pulls) == 1 })

	f.takeOver()
	waitFor(t, "standby", func() bool { return log.count(logLost) == 1 && log.count(logStandby) == 1 })
	_, saves, _ := f.snapshot()
	if len(saves) != 0 {
		t.Errorf("saves = %v, want the pending frames dropped", saves)
	}
	if n := log.count("Projection stream failed, reconnecting"); n != 0 {
		t.Errorf("reconnect ladder used %d times, want none", n)
	}
}

func TestProjectionLease_OldServerStops(t *testing.T) {
	for _, transport := range leaseTransports {
		t.Run(transport, func(t *testing.T) {
			f := newLeaseFake(t, []leaseEvent{{"e1", 1, ""}})
			f.noLease = true
			log := &leaseLogger{}
			runner := startLeaseRunner(t, f.server(t, transport), log)
			select {
			case <-runner.done:
			case <-time.After(5 * time.Second):
				t.Fatal("the runner did not stop on a server without the lease RPCs")
			}
			want := "AcquireProjectionLease needs Ironflow server >= 0.44.0; upgrade the server"
			log.mu.Lock()
			n := 0
			for _, m := range log.msgs {
				if strings.HasPrefix(m, want) {
					n++
				}
			}
			log.mu.Unlock()
			if n != 1 {
				t.Errorf("%q logged %d times, want 1", want, n)
			}
			if pulls, saves, _ := f.snapshot(); len(pulls) != 0 || len(saves) != 0 {
				t.Errorf("pulls = %v, saves = %v, want no Poll, Stream, Save or Ack", pulls, saves)
			}
		})
	}
}

func TestProjectionLease_RenewTransportErrorKeepsProcessing(t *testing.T) {
	f := newLeaseFake(t)
	f.renewStatus = http.StatusServiceUnavailable
	log := &leaseLogger{}
	startLeaseRunner(t, f.server(t, "poll"), log)
	// The first Renew comes after TTL/3 (100 ms), then a retry every 20 ms.
	waitFor(t, "renew retries", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.renews >= 5 })
	f.mu.Lock()
	f.batches = [][]leaseEvent{{{"e1", 1, ""}}}
	f.mu.Unlock()
	waitFor(t, "a save while Renew fails", func() bool { _, saves, _ := f.snapshot(); return len(saves) == 1 })
	if log.count(logLost) != 0 {
		t.Errorf("logs = %v, want no lost lease on a transport error", log.msgs)
	}
}

func TestProjectionLease_LostLeaseStopsExternalBatch(t *testing.T) {
	f := newLeaseFake(t, []leaseEvent{{"e1", 1, ""}, {"e2", 2, ""}, {"e3", 3, ""}})
	var mu sync.Mutex
	var handled []string
	proj := externalProjection(10, func(_ map[string]any, e ProjectionEvent, _ ProjectionContext) (map[string]any, error) {
		mu.Lock()
		handled = append(handled, e.ID)
		mu.Unlock()
		if e.ID == "e1" {
			f.mu.Lock()
			before := f.renews
			f.mu.Unlock()
			f.takeOver()
			// Renew runs every 100 ms; the next one finds the lease lost.
			waitFor(t, "the lost renew", func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.renews > before })
			time.Sleep(20 * time.Millisecond)
		}
		return nil, nil
	})
	log := &leaseLogger{}
	startLeaseProjection(t, proj, f.server(t, "poll"), log)
	waitFor(t, "the lost lease", func() bool { return log.count(logLost) == 1 })
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(handled, []string{"e1"}) {
		t.Errorf("handled = %v, want only e1: external handlers must stop when the lease is lost", handled)
	}
	if _, saves, _ := f.snapshot(); len(saves) != 0 {
		t.Errorf("acks = %v, want none for a batch whose lease was lost", saves)
	}
}

func TestProjectionLease_HungRenewIsBounded(t *testing.T) {
	f := newLeaseFake(t)
	f.renewHang.Store(true)
	startLeaseRunner(t, f.server(t, "poll"), &leaseLogger{})
	// TTL 0.3s: each Renew gets 100 ms, then the 20 ms retry sends the next one.
	waitFor(t, "renew retries past a hung renew", func() bool { return f.hungRenews.Load() >= 3 })
}

func TestProjectionLease_StopDuringAcquireReleasesTheGrant(t *testing.T) {
	f := newLeaseFake(t)
	f.acquireDelay = 200 * time.Millisecond
	runner := startLeaseRunner(t, f.server(t, "poll"), &leaseLogger{})
	waitFor(t, "an acquire in flight", f.acquiring.Load)
	runner.Stop()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.held || f.epoch != 2 {
		t.Errorf("held=%v epoch=%d, want the grant released (epoch 2)", f.held, f.epoch)
	}
}

func TestProjectionLease_StopReleasesAfterTheFinalSave(t *testing.T) {
	for _, transport := range leaseTransports {
		t.Run(transport, func(t *testing.T) {
			f := newLeaseFake(t, []leaseEvent{{"e1", 1, ""}})
			runner := startLeaseRunner(t, f.server(t, transport), &leaseLogger{})
			waitFor(t, "the save", func() bool { _, saves, _ := f.snapshot(); return len(saves) == 1 })
			runner.Stop()
			f.mu.Lock()
			defer f.mu.Unlock()
			if !slices.Equal(f.order, []string{"SaveProjectionState", "release"}) {
				t.Errorf("order = %v, want the save, then the release", f.order)
			}
		})
	}
}

// A handler that outlives the Stop timeout keeps the lease renewed, so no
// replacement runner reads the same events until the handler returns (#2822).
func TestProjectionLease_HandlerPastStopTimeoutKeepsTheLease(t *testing.T) {
	timeout := projectionStopTimeout
	projectionStopTimeout = 50 * time.Millisecond
	t.Cleanup(func() { projectionStopTimeout = timeout })

	for _, transport := range leaseTransports {
		t.Run(transport, func(t *testing.T) {
			f := newLeaseFake(t, []leaseEvent{{"e1", 1, ""}, {"e2", 2, ""}})
			var handled atomic.Int32
			entered, release := make(chan struct{}), make(chan struct{})
			runner := startLeaseProjection(t, externalProjection(10, func(map[string]any, ProjectionEvent, ProjectionContext) (map[string]any, error) {
				if handled.Add(1) == 1 {
					close(entered)
					<-release // a handler cannot be cancelled
				}
				return nil, nil
			}), f.server(t, transport), &leaseLogger{})
			<-entered
			runner.Stop()

			f.mu.Lock()
			renews := f.renews
			f.mu.Unlock()
			// TTL 0.3s: a Renew every 100 ms. Past one TTL the lease would expire.
			waitFor(t, "renews after Stop", func() bool {
				f.mu.Lock()
				defer f.mu.Unlock()
				return f.renews >= renews+4
			})
			f.mu.Lock()
			held := f.held
			f.mu.Unlock()
			if !held {
				t.Fatal("the lease was freed while the handler still ran")
			}

			close(release)
			<-runner.done
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.held {
				t.Error("the lease is still held after the handler returned, want it released")
			}
			if !slices.Equal(f.order, []string{"release"}) {
				t.Errorf("order = %v, want only the release: Stop cancelled the ack", f.order)
			}
			if n := handled.Load(); n != 1 {
				t.Errorf("handled = %d, want 1: no handler may start after the Stop timeout", n)
			}
		})
	}
}

// The poll transport answers 404 for the stream: the runner falls back with the
// lease it already holds.
func TestProjectionLease_StreamFallbackKeepsOneLease(t *testing.T) {
	f := newLeaseFake(t, []leaseEvent{{"e1", 1, ""}})
	startLeaseRunner(t, f.server(t, "poll"), &leaseLogger{})
	waitFor(t, "the save", func() bool { _, saves, _ := f.snapshot(); return len(saves) == 1 })
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.acquires != 1 {
		t.Errorf("acquires = %d, want 1: the fallback keeps the lease", f.acquires)
	}
	for _, epoch := range f.pulls {
		if epoch != 1 {
			t.Errorf("pulls = %v, want every poll with epoch 1", f.pulls)
		}
	}
}

func TestProjectionStatusError(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
		want   error
	}{
		{http.StatusConflict, `{"code":"aborted","message":"projection lease lost"}`, errProjectionLeaseLost},
		{http.StatusNotFound, "404 page not found\n", errProjectionUnimplemented},
		{http.StatusNotImplemented, `{"code":"unimplemented"}`, errProjectionUnimplemented},
		{http.StatusNotFound, `{"code":"not_found","message":"no projection"}`, nil},
		{http.StatusInternalServerError, "boom", nil},
	} {
		err := projectionStatusError(tc.status, []byte(tc.body))
		for _, sentinel := range []error{errProjectionLeaseLost, errProjectionUnimplemented} {
			if errors.Is(err, sentinel) != (sentinel == tc.want) {
				t.Errorf("projectionStatusError(%d, %q) = %v, want sentinel %v", tc.status, tc.body, err, tc.want)
			}
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("HTTP %d", tc.status)) {
			t.Errorf("error %q lost the HTTP status", err)
		}
	}
}
