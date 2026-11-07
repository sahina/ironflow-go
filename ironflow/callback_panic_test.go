package ironflow

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestCallbackPanicSubprocess(t *testing.T) {
	if mode := os.Getenv("IRONFLOW_CALLBACK_PANIC_PROBE"); mode != "" {
		callbackPanicProbe(t, mode)
		return
	}
	for _, mode := range []string{"timed", "timed-nil", "parallel", "nested", "timed-branch", "allSettled", "panic-and-yield", "multiple", "joined", "late", "late-branch", "timed-yield", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCallbackPanicSubprocess$", "-test.v")
			cmd.Env = append(os.Environ(), "IRONFLOW_CALLBACK_PANIC_PROBE="+mode)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("callback probe terminated: %v\n%s", err, out)
			}
			if !strings.Contains(string(out), "callback cleanup verified") {
				t.Fatalf("missing cleanup evidence:\n%s", out)
			}
			if strings.Contains(string(out), "private-panic-payload") {
				t.Fatalf("panic payload leaked:\n%s", out)
			}
		})
	}
}

func callbackPanicProbe(t *testing.T, mode string) {
	ctx := Context{exec: newExecutionContext(&PushRequest{RunID: "panic-probe"})}
	payload := &struct{ secret string }{"private-panic-payload"}
	cleaned := false
	var recovered any
	joined := false
	func() {
		defer func() { recovered = recover() }()
		defer func() { cleaned = true }()
		var panicValue any = payload
		if mode == "timed-nil" {
			panicValue = nil
		}
		callback := func() (int, error) { panic(panicValue) }
		branch := func(b *BranchContext) (int, error) { return RunWithBranch(b, "work", callback) }
		switch mode {
		case "timed", "timed-nil":
			_, _ = Run(ctx, "work", callback, WithTimeout(time.Hour))
		case "parallel", "allSettled", "panic-and-yield":
			branches := []func(*BranchContext) (int, error){branch}
			if mode == "panic-and-yield" {
				branches = append([]func(*BranchContext) (int, error){func(b *BranchContext) (int, error) { _ = SleepWithBranch(b, "sleep", time.Hour); return 0, nil }}, branches...)
			}
			options := ParallelOptions{}
			if mode == "allSettled" {
				options.OnError = "allSettled"
			}
			_, _ = Parallel(ctx, "fanout", branches, options)
		case "multiple":
			started := make(chan struct{})
			_, _ = Parallel(ctx, "fanout", []func(*BranchContext) (int, error){
				func(*BranchContext) (int, error) { <-started; panic(payload) },
				func(*BranchContext) (int, error) { close(started); panic("second panic") },
			})
		case "joined":
			started := make(chan struct{})
			_, _ = Parallel(ctx, "fanout", []func(*BranchContext) (int, error){
				func(*BranchContext) (int, error) { close(started); panic(payload) },
				func(*BranchContext) (int, error) { <-started; joined = true; return 0, nil },
			})
		case "nested":
			_, _ = Parallel(ctx, "outer", []func(*BranchContext) (int, error){func(b *BranchContext) (int, error) {
				_, err := ParallelWithBranch(b, "inner", []func(*BranchContext) (int, error){branch})
				return 0, err
			}})
		case "timed-branch":
			_, _ = Parallel(ctx, "fanout", []func(*BranchContext) (int, error){func(b *BranchContext) (int, error) { return RunWithBranch(b, "work", callback, WithTimeout(time.Hour)) }})
		case "late", "late-branch":
			release := make(chan struct{})
			done := make(chan struct{})
			callback = func() (int, error) { defer close(done); <-release; panic(payload) }
			var err error
			if mode == "late" {
				_, err = Run(ctx, "work", callback, WithTimeout(time.Millisecond))
			} else {
				_, err = Parallel(ctx, "fanout", []func(*BranchContext) (int, error){func(b *BranchContext) (int, error) {
					return RunWithBranch(b, "work", callback, WithTimeout(time.Millisecond))
				}})
			}
			if err == nil {
				t.Fatal("timeout did not fail")
			}
			close(release)
			<-done
			// Give a process-terminating panic time to reach the runtime after the callback unwinds.
			time.Sleep(100 * time.Millisecond)
			if len(ctx.exec.executedSteps) != 1 || ctx.exec.executedSteps[0].Status != "failed" {
				t.Fatal("late panic changed timeout result")
			}
		case "timed-yield":
			_, _ = Run(ctx, "work", func() (int, error) { _ = Sleep(ctx, "sleep", time.Hour); return 0, nil }, WithTimeout(time.Hour))
		case "cancelled":
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			ctx.exec.ctx = cancelled
			_, err := Run(ctx, "work", callback, WithTimeout(time.Hour))
			if err != context.Canceled {
				t.Fatal("cancellation did not prevent callback")
			}
		default:
			t.Fatal("unknown probe mode")
		}
	}()
	if mode == "joined" && !joined {
		t.Fatal("started branch was not joined")
	}
	if !cleaned {
		t.Fatal("execution cleanup did not run")
	}
	switch mode {
	case "late", "late-branch", "cancelled":
		if recovered != nil {
			t.Fatal("unexpected panic after completion")
		}
	case "timed-yield":
		if signal, ok := recovered.(*yieldSignal); !ok || signal.info.Type != "sleep" {
			t.Fatal("durable yield was lost")
		}
	case "timed-nil":
		if recovered == nil {
			t.Fatal("nil panic was lost")
		}
	default:
		if recovered != payload {
			t.Fatal("original panic identity was lost")
		}
	}
	t.Log("callback cleanup verified")
}
