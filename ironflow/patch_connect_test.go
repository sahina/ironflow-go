package ironflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type patchService struct {
	ironflowv1connect.UnimplementedIronflowServiceHandler
	output   string
	failures int
	calls    int
}

func (s *patchService) PatchStep(_ context.Context, req *connect.Request[ironflowv1.PatchStepRequest]) (*connect.Response[ironflowv1.Step], error) {
	s.calls++
	if s.failures > 0 {
		s.failures--
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("try again"))
	}
	if req.Msg.Reason != "manual fix" || req.Header().Get("Authorization") != "Bearer test-key" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("reason and authentication required"))
	}
	if req.Msg.StepId == "missing" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("step not found"))
	}
	s.output = req.Msg.Output.AsMap()["result"].(string)
	return connect.NewResponse(&ironflowv1.Step{Id: req.Msg.StepId, Output: req.Msg.Output}), nil
}
func TestPatchStep_Connect(t *testing.T) {
	service := &patchService{}
	mux := http.NewServeMux()
	mux.Handle(ironflowv1connect.NewIronflowServiceHandler(service))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := NewClient(ClientConfig{ServerURL: srv.URL, APIKey: "test-key"})
	if err := client.PatchStep(context.Background(), "step-1", map[string]any{"result": "fixed"}, "manual fix"); err != nil {
		t.Fatal(err)
	}
	if service.output != "fixed" {
		t.Fatalf("patched output = %q", service.output)
	}
	err := client.PatchStep(context.Background(), "missing", map[string]any{"result": "fixed"}, "manual fix")
	var sdkError *IronflowError
	if !errors.As(err, &sdkError) || IsRetryable(err) {
		t.Fatalf("expected permanent SDK error, got %v", err)
	}
}

func TestPatchStepPreservesConfiguredRetries(t *testing.T) {
	service := &patchService{failures: 1}
	mux := http.NewServeMux()
	mux.Handle(ironflowv1connect.NewIronflowServiceHandler(service))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	var retries []RetryEvent
	client := NewClient(ClientConfig{ServerURL: srv.URL, APIKey: "test-key", Retry: &ClientRetryConfig{MaxAttempts: 2, InitialDelay: time.Millisecond, OnRetry: func(event RetryEvent) { retries = append(retries, event) }}})
	err := client.PatchStep(context.Background(), "step", map[string]any{"result": "fixed"}, "manual fix")
	if err != nil {
		t.Fatal(err)
	}
	if service.calls != 2 || len(retries) != 1 || retries[0].Attempt != 1 || service.output != "fixed" {
		t.Fatalf("calls=%d retries=%v output=%q", service.calls, retries, service.output)
	}
}
