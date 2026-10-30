package ironflow

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

func optionalWaitPublicFunction(id string) Function {
	return CreateFunction(FunctionConfig{ID: id, Triggers: []Trigger{{Event: "start"}}}, func(ctx Context) (any, error) {
		event, err := WaitForEventOptional(ctx, "wait", EventFilter{Event: "reply"})
		return map[string]any{"timedOut": event == nil}, err
	})
}

func TestOptionalWaitPublicRESTReplaysJSONNull(t *testing.T) {
	var served atomic.Bool
	terminal := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/jobs") {
			if served.Swap(true) {
				w.WriteHeader(204)
				return
			}
			_, _ = w.Write([]byte(`{"jobs":[{"job_id":"j","run_id":"r","function_id":"f","attempt":1,"event":{"id":"trigger","name":"start","data":{},"timestamp":"2026-01-01T00:00:00Z"},"completed_steps":[{"step_id":"r:wait:0","name":"wait","status":"completed","output":null}]}]}`))
			return
		}
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/jobs/j") {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["status"] != "progress" {
				terminal <- body
			}
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	worker := NewWorker(WorkerConfig{ServerURL: server.URL, Functions: []Function{optionalWaitPublicFunction("f")}, Logger: NewNoopLogger()})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	defer func() {
		worker.Stop()
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("worker did not stop")
		}
	}()
	select {
	case result := <-terminal:
		if result["status"] != "completed" {
			t.Fatalf("terminal=%v", result)
		}
		output, ok := result["output"].(map[string]any)
		if !ok || output["timedOut"] != true {
			t.Fatalf("null replay produced an event: %v", result)
		}
	case <-ctx.Done():
		t.Fatal("no terminal report")
	}
}

func TestOptionalWaitPublicStreamReplaysJSONNull(t *testing.T) {
	terminal := make(chan *ironflowv1.JobCompleted, 1)
	handler := &mockWorkerHandler{}
	handler.onConnect = func(ctx context.Context, stream *connect.BidiStream[ironflowv1.WorkerMessage, ironflowv1.EngineMessage]) error {
		if _, err := doRegistrationHandshake(stream, handler, 30000); err != nil {
			return err
		}
		job := makeJobAssignment("j", "r", "f", map[string]any{})
		job.CompletedSteps = []*ironflowv1.CompletedStep{{StepId: "r:wait:0", Name: "wait", OutputJson: []byte("null"), OutputValue: structpb.NewNullValue()}}
		if err := stream.Send(&ironflowv1.EngineMessage{Payload: &ironflowv1.EngineMessage_Job{Job: job}}); err != nil {
			return err
		}
		for {
			message, err := stream.Receive()
			if err != nil {
				return err
			}
			if result := message.GetJobCompleted(); result != nil {
				terminal <- result
				<-ctx.Done()
				return nil
			}
		}
	}
	server := startMockServer(t, handler)
	worker := NewStreamingWorker(WorkerConfig{ServerURL: server.URL, Functions: []Function{optionalWaitPublicFunction("f")}, Logger: NewNoopLogger()})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	defer func() {
		worker.Stop()
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("stream worker did not stop")
		}
	}()
	select {
	case result := <-terminal:
		if result.Output == nil || result.Output.AsMap()["timedOut"] != true {
			t.Fatalf("null replay produced an event: %v", result)
		}
	case <-ctx.Done():
		t.Fatal("no stream completion")
	}
}

func TestOptionalWaitPushUsesCompletedOutputWhenResumeDataAbsent(t *testing.T) {
	fn := serveFn(func(ctx Context) (any, error) {
		event, err := WaitForEventOptional(ctx, "wait", EventFilter{Event: "reply"})
		if event == nil {
			return "invented timeout", err
		}
		return event.ID, err
	})
	handler := Serve(ServeConfig{Functions: []Function{fn}, SkipVerification: true})
	req := httptest.NewRequest(http.MethodPost, "/api/ironflow", bytes.NewBufferString(`{"run_id":"r","function_id":"f","attempt":1,"event":{"id":"trigger","name":"start","data":{},"timestamp":"2026-01-01T00:00:00Z"},"steps":[{"id":"r:wait:0","name":"wait","status":"completed","output":{"id":"reply","name":"reply","data":{},"timestamp":"2026-01-01T00:00:00Z"}}],"resume":{"step_id":"r:wait:0","type":"wait_for_event"}}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	var body struct {
		Status string `json:"status"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "completed" || body.Result != "reply" {
		t.Fatalf("saved event lost: %s", response.Body.String())
	}
}
