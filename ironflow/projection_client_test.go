package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

func setupMockProjectionServer(t *testing.T, handler http.Handler) (*Client, func()) {
	t.Helper()
	server := httptest.NewServer(handler)
	return &Client{serverURL: server.URL, httpClient: server.Client(), apiKey: "key", retryConfig: &ClientRetryConfig{MaxAttempts: 1}, logger: NewNoopLogger()}, server.Close
}

type projectionClientService struct {
	ironflowv1connect.UnimplementedProjectionServiceHandler
}

func (projectionClientService) GetProjection(_ context.Context, r *connect.Request[ironflowv1.GetProjectionRequest]) (*connect.Response[ironflowv1.GetProjectionResponse], error) {
	if r.Header().Get("Authorization") != "Bearer key" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("key required"))
	}
	if r.Msg.Name == "missing" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("missing"))
	}
	value, _ := structpb.NewValue([]any{"array", false})
	return connect.NewResponse(&ironflowv1.GetProjectionResponse{Name: r.Msg.Name, Partition: r.Msg.Partition, StateValue: value, Version: 3, Mode: "managed", LastEventId: "event", LastEventTime: timestamppb.New(time.Unix(123, 0)), Registry: &ironflowv1.ProjectionInfo{VersionFull: 2147483648, LastEventSeq: 9007199254740993, Status: "active", ErrorMessage: "previous failure"}}), nil
}
func (projectionClientService) ListProjections(context.Context, *connect.Request[ironflowv1.ListProjectionsRequest]) (*connect.Response[ironflowv1.ListProjectionsResponse], error) {
	return connect.NewResponse(&ironflowv1.ListProjectionsResponse{Projections: []*ironflowv1.ProjectionInfo{{Name: "orders", Status: "active", ErrorMessage: "previous"}}}), nil
}
func (projectionClientService) GetProjectionStatus(context.Context, *connect.Request[ironflowv1.GetProjectionStatusRequest]) (*connect.Response[ironflowv1.GetProjectionStatusResponse], error) {
	return connect.NewResponse(&ironflowv1.GetProjectionStatusResponse{Name: "orders", Status: "paused", ErrorMessage: "previous"}), nil
}
func (projectionClientService) RebuildProjection(context.Context, *connect.Request[ironflowv1.RebuildProjectionRequest]) (*connect.Response[ironflowv1.RebuildProjectionResponse], error) {
	return connect.NewResponse(&ironflowv1.RebuildProjectionResponse{Job: &ironflowv1.RebuildJob{ProjectionName: "orders", Status: "running", Progress: 50, StartedAt: timestamppb.New(time.Unix(123, 0))}}), nil
}
func (projectionClientService) GetRebuildJob(context.Context, *connect.Request[ironflowv1.GetRebuildJobRequest]) (*connect.Response[ironflowv1.GetRebuildJobResponse], error) {
	return connect.NewResponse(&ironflowv1.GetRebuildJobResponse{Job: &ironflowv1.RebuildJob{ProjectionName: "orders", Status: "running", Progress: 50, StartedAt: timestamppb.New(time.Unix(123, 0))}}), nil
}
func (projectionClientService) PauseProjection(_ context.Context, r *connect.Request[ironflowv1.PauseProjectionRequest]) (*connect.Response[ironflowv1.PauseProjectionResponse], error) {
	if r.Msg.Name == "missing" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("missing"))
	}
	return connect.NewResponse(&ironflowv1.PauseProjectionResponse{Status: "ok"}), nil
}
func (projectionClientService) ResumeProjection(_ context.Context, r *connect.Request[ironflowv1.ResumeProjectionRequest]) (*connect.Response[ironflowv1.ResumeProjectionResponse], error) {
	if r.Msg.Name == "missing" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("missing"))
	}
	return connect.NewResponse(&ironflowv1.ResumeProjectionResponse{Status: "ok"}), nil
}
func (projectionClientService) CancelRebuild(_ context.Context, r *connect.Request[ironflowv1.CancelRebuildRequest]) (*connect.Response[ironflowv1.CancelRebuildResponse], error) {
	if r.Msg.Name == "missing" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("missing"))
	}
	return connect.NewResponse(&ironflowv1.CancelRebuildResponse{Status: "ok"}), nil
}
func TestProjectionClientConnect(t *testing.T) {
	_, handler := ironflowv1connect.NewProjectionServiceHandler(projectionClientService{})
	client, cleanup := setupMockProjectionServer(t, handler)
	defer cleanup()
	pc := client.Projections()
	ctx := context.Background()
	result, err := pc.Get(ctx, "order totals", WithPartition("customer"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != 2147483648 || result.LastEventSeq != 9007199254740993 || result.Partition != "customer" || result.Status != "active" || result.ErrorMessage != "previous failure" || result.LastEventTime.Unix() != 123 || !reflect.DeepEqual(result.State, []any{"array", false}) {
		t.Fatalf("lost state: %+v", result)
	}
	_, err = pc.Get(ctx, "missing")
	var typed *IronflowError
	if !errors.As(err, &typed) || IsRetryable(err) || typed.Code != "NOT_FOUND" {
		t.Fatalf("error contract: %v", err)
	}
	list, err := pc.List(ctx)
	if err != nil || len(list) != 1 || list[0].Name != "orders" || list[0].LastError != "previous" {
		t.Fatalf("list: %+v %v", list, err)
	}
	status, err := pc.GetStatus(ctx, "orders")
	if err != nil || status.Status != "paused" {
		t.Fatalf("status: %+v %v", status, err)
	}
	for _, read := range []func(context.Context, string) (*RebuildJob, error){pc.Rebuild, pc.GetRebuildJob} {
		job, err := read(ctx, "orders")
		if err != nil || job.Name != "orders" || job.Progress != 50 || job.StartedAt != "1970-01-01T00:02:03Z" {
			t.Fatalf("job: %+v %v", job, err)
		}
	}
	for _, action := range []func(context.Context, string) error{pc.Pause, pc.Resume, pc.CancelRebuild} {
		if err := action(ctx, "orders"); err != nil {
			t.Fatal(err)
		}
		err := action(ctx, "missing")
		if !errors.As(err, &typed) || IsRetryable(err) {
			t.Fatalf("action error: %v", err)
		}
	}
}
func TestProjectionClient_Delete(t *testing.T) {
	t.Run("deletes projection successfully", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "DELETE" {
				t.Errorf("expected method DELETE, got %s", r.Method)
			}
			if r.URL.Path != "/api/v1/projections/order-totals" {
				t.Errorf("expected path /api/v1/projections/order-totals, got %s", r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer cleanup()

		err := client.Projections().Delete(context.Background(), "order-totals")
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
	})

	t.Run("returns error on 404", func(t *testing.T) {
		client, cleanup := setupMockProjectionServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{"error": "projection not found"})
		}))
		defer cleanup()

		err := client.Projections().Delete(context.Background(), "nonexistent")
		if err == nil {
			t.Fatal("expected error for 404, got nil")
		}
	})
}

// ============================================================================
// ProjectionClient.Pause
// ============================================================================
