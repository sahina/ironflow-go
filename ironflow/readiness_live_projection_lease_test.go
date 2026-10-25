package ironflow

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// leaseChild is one runner process of TestLiveReadinessProjectionLease.
type leaseChild struct {
	cmd              *exec.Cmd
	mu               sync.Mutex
	granted, standby bool
	once             sync.Once
}

func (c *leaseChild) state() (granted, standby bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.granted, c.standby
}

func (c *leaseChild) kill() {
	c.once.Do(func() { _ = c.cmd.Process.Kill(); _ = c.cmd.Wait() })
}

// TestLiveReadinessProjectionLease runs two runner processes on one managed
// projection (#504). One holds the lease and the other stands by. A Poll
// without the lease is refused. When the holder is killed, the standby takes
// over, and the read model has every event exactly once.
func TestLiveReadinessProjectionLease(t *testing.T) {
	var seed readinessSeed
	client, ctx := liveReadiness(t, &seed)
	serverURL, apiKey := os.Getenv("IRONFLOW_TEST_SERVER"), os.Getenv("IRONFLOW_TEST_API_KEY")
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	name, event := "gosdk-lease-procs-"+suffix, "gosdk.lease.procs."+suffix

	start := func() *leaseChild {
		cmd := exec.Command(os.Args[0], "-test.run=^TestLeaseRunnerHelperProcess$") // #nosec G204 -- this test binary
		cmd.Env = append(os.Environ(), "IRONFLOW_LEASE_HELPER="+name, "IRONFLOW_LEASE_EVENT="+event)
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		// The child exits when this pipe closes, so it dies with the parent, even on a -timeout panic.
		if _, err := cmd.StdinPipe(); err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		c := &leaseChild{cmd: cmd}
		t.Cleanup(c.kill)
		go func() {
			sc := bufio.NewScanner(out)
			for sc.Scan() {
				line := sc.Text()
				c.mu.Lock()
				c.granted = c.granted || strings.Contains(line, "Projection lease granted")
				c.standby = c.standby || strings.Contains(line, "Projection lease held by another runner, standing by")
				c.mu.Unlock()
			}
		}()
		return c
	}
	waitUntil := func(what string, within time.Duration, cond func() bool) {
		t.Helper()
		for end := time.Now().Add(within); !cond(); time.Sleep(200 * time.Millisecond) {
			if time.Now().After(end) {
				t.Fatalf("timed out after %s waiting for %s", within, what)
			}
		}
	}

	a, b := start(), start()
	var holder, standby *leaseChild
	waitUntil("one holder and one standby", 30*time.Second, func() bool {
		ag, as := a.state()
		bg, bs := b.state()
		switch {
		case ag && bs && !bg:
			holder, standby = a, b
		case bg && as && !ag:
			holder, standby = b, a
		}
		return holder != nil
	})

	t.Run("a poll without the lease is refused", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{"name": name})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, serverURL+"/ironflow.v1.ProjectionService/PollProjectionEvents", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var got struct{ Code, Message string }
		_ = json.NewDecoder(resp.Body).Decode(&got)
		if resp.StatusCode != http.StatusBadRequest || got.Code != "failed_precondition" || got.Message != "projection lease required" {
			t.Fatalf("Poll without a lease = HTTP %d %+v, want 400 failed_precondition \"projection lease required\"", resp.StatusCode, got)
		}
	})

	emitted := map[string]bool{}
	emit := func(n int) {
		t.Helper()
		for range n {
			res, err := client.Emit(ctx, event, map[string]any{})
			if err != nil {
				t.Fatalf("emit: %v", err)
			}
			emitted[res.EventID] = true
		}
	}
	readModel := func() map[string]any {
		res, err := client.Projections().Get(ctx, name)
		if err != nil {
			return nil
		}
		state, _ := res.State.(map[string]any)
		return state
	}
	hasAll := func() bool {
		state := readModel()
		for id := range emitted {
			if _, ok := state[id]; !ok {
				return false
			}
		}
		return true
	}

	emit(50)
	waitUntil("the holder to apply the first 50 events", 30*time.Second, hasAll)
	if g, _ := standby.state(); g {
		t.Fatal("the standby was granted the lease while the holder ran")
	}

	holder.kill() // a crash: the lease is not released, so it must expire first
	began := time.Now()
	emit(50)
	waitUntil("the standby to take over and apply all 100 events", 60*time.Second, func() bool {
		g, _ := standby.state()
		return g && hasAll()
	})
	t.Logf("takeover after %s", time.Since(began).Round(100*time.Millisecond))

	state := readModel()
	if len(state) != len(emitted) {
		t.Fatalf("read model has %d keys, want %d", len(state), len(emitted))
	}
	for id, n := range state {
		if !emitted[id] || n != float64(1) {
			t.Errorf("read model[%s] = %v, want 1 for an emitted event", id, n)
		}
	}
}

// TestLeaseRunnerHelperProcess is one runner process of
// TestLiveReadinessProjectionLease. It returns at once unless the parent set
// IRONFLOW_LEASE_HELPER; the readiness target fails on any skipped test.
func TestLeaseRunnerHelperProcess(t *testing.T) {
	name := os.Getenv("IRONFLOW_LEASE_HELPER")
	if name == "" {
		return
	}
	proj := CreateProjection(ProjectionConfig{Name: name, Events: []string{os.Getenv("IRONFLOW_LEASE_EVENT")},
		InitialState: func() map[string]any { return map[string]any{} },
		Handler: func(state map[string]any, e ProjectionEvent, _ ProjectionContext) (map[string]any, error) {
			n, _ := state[e.ID].(float64)
			state[e.ID] = n + 1
			return state, nil
		}})
	// A worker with no functions never starts its runners, so it carries a no-op function.
	noop := CreateFunction(FunctionConfig{ID: name + "-noop", Triggers: []Trigger{{Event: name + ".noop"}}},
		func(Context) (any, error) { return nil, nil })
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, _ = io.Copy(io.Discard, os.Stdin); cancel() }() // EOF: the parent is gone
	_ = NewWorker(WorkerConfig{ServerURL: os.Getenv("IRONFLOW_TEST_SERVER"), APIKey: os.Getenv("IRONFLOW_TEST_API_KEY"),
		Functions: []Function{noop}, Projections: []Projection{proj}, Logger: NewLogger(LoggerConfig{Level: LogLevelInfo})}).Run(ctx)
}
