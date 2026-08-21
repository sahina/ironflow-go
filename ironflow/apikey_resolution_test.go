package ironflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/gorilla/websocket"
)

// Every constructor that can authenticate should reach the same key: the
// explicit config value when set, IRONFLOW_API_KEY otherwise. Before #1672
// several of these dropped it, which surfaced as a 401 far from the cause.

type recordingReporter struct {
	failed *PushError
}

func (r *recordingReporter) ReportCompleted(context.Context, string, any, []*StepResult) error {
	return nil
}
func (r *recordingReporter) ReportFailed(_ context.Context, _ string, e *PushError, _ []*StepResult) error {
	r.failed = e
	return nil
}
func (r *recordingReporter) ReportYielded(context.Context, string, *YieldInfo) error { return nil }

// authEchoServer records the Authorization header of the first request.
func authEchoServer(t *testing.T, got *string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if *got == "" {
			*got = r.Header.Get("Authorization")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// publishJob runs one job through the executor whose handler publishes once,
// and returns the Authorization header the publish carried.
func publishJob(t *testing.T, configKey string) string {
	t.Helper()
	var gotAuth string
	srv := authEchoServer(t, &gotAuth)

	fn := Function{
		Config: FunctionConfig{ID: "fn"},
		Handler: func(ctx Context) (any, error) {
			return nil, Publish(ctx, "orders", map[string]any{"id": "1"})
		},
	}
	exec := &jobExecutor{
		functions: map[string]Function{"fn": fn},
		serverURL: srv.URL,
		apiKey:    configKey,
		logger:    NewNoopLogger(),
	}
	job := &jobAssignment{
		JobID:      "job-1",
		RunID:      "run-1",
		FunctionID: "fn",
		Attempt:    1,
		Event:      jobEvent{ID: "e1", Name: "e", Data: json.RawMessage(`{}`)},
	}
	if err := exec.execute(context.Background(), job, &recordingReporter{}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	return gotAuth
}

func TestJobExecutorStepCallbackAuth(t *testing.T) {
	t.Run("uses an explicit WorkerConfig key", func(t *testing.T) {
		t.Setenv(EnvAPIKey, "")
		if got := publishJob(t, "cfg-key"); got != "Bearer cfg-key" {
			t.Errorf("expected 'Bearer cfg-key', got %q", got)
		}
	})

	t.Run("explicit key wins over the env var", func(t *testing.T) {
		t.Setenv(EnvAPIKey, "env-key")
		if got := publishJob(t, "cfg-key"); got != "Bearer cfg-key" {
			t.Errorf("expected 'Bearer cfg-key', got %q", got)
		}
	})

	t.Run("falls back to the env var", func(t *testing.T) {
		t.Setenv(EnvAPIKey, "env-key")
		if got := publishJob(t, ""); got != "Bearer env-key" {
			t.Errorf("expected 'Bearer env-key', got %q", got)
		}
	})
}

func TestWorkerConstructorsPassKeyToExecutor(t *testing.T) {
	t.Setenv(EnvAPIKey, "")

	w := NewWorker(WorkerConfig{ServerURL: "http://localhost:9123", APIKey: "cfg-key"})
	if w.executor.apiKey != "cfg-key" {
		t.Errorf("polling worker: expected executor key 'cfg-key', got %q", w.executor.apiKey)
	}

	sw := NewStreamingWorker(WorkerConfig{ServerURL: "http://localhost:9123", APIKey: "cfg-key"})
	if sw.executor.apiKey != "cfg-key" {
		t.Errorf("streaming worker: expected executor key 'cfg-key', got %q", sw.executor.apiKey)
	}
}

func TestSubscriptionClientsCarryClientKey(t *testing.T) {
	t.Setenv(EnvAPIKey, "")
	c := NewClient(ClientConfig{ServerURL: "http://localhost:9123", APIKey: "cfg-key", Logger: NewNoopLogger()})

	if got := c.CreateSubscriptionClient().apiKey; got != "cfg-key" {
		t.Errorf("CreateSubscriptionClient: expected 'cfg-key', got %q", got)
	}
	if got := c.CreateGrpcSubscriptionClient().apiKey; got != "cfg-key" {
		t.Errorf("CreateGrpcSubscriptionClient: expected 'cfg-key', got %q", got)
	}
}

// The /ws upgrade is not a public route, so the key has to reach the dial
// itself, not just the client struct.
func TestSubscriptionClientDialSendsBearer(t *testing.T) {
	var gotAuth string
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		_ = conn.Close()
	}))
	defer srv.Close()

	client := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL:  "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws",
		Logger: NewNoopLogger(),
	})
	client.apiKey = "cfg-key"
	_ = client.Connect(context.Background())
	defer client.Close()

	if gotAuth != "Bearer cfg-key" {
		t.Errorf("expected 'Bearer cfg-key' on the upgrade, got %q", gotAuth)
	}
}

func TestBearerInterceptorSetsHeader(t *testing.T) {
	var gotAuth string
	srv := authEchoServer(t, &gotAuth)

	// Unary path. The streaming path shares WrapStreamingClient, which sets the
	// same header on the connection before the stream opens.
	client := connect.NewClient[struct{}, struct{}](
		srv.Client(), srv.URL+"/svc/Method",
		connect.WithInterceptors(bearerInterceptor("cfg-key")),
	)
	_, _ = client.CallUnary(context.Background(), connect.NewRequest(&struct{}{}))

	if gotAuth != "Bearer cfg-key" {
		t.Errorf("expected 'Bearer cfg-key', got %q", gotAuth)
	}
}
