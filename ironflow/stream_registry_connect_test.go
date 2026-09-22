package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type streamRegistryService struct {
	ironflowv1connect.UnimplementedEntityStreamServiceHandler
	snapshot *ironflowv1.GetSnapshotResponse
	failure  error
}

func (s *streamRegistryService) ListStreams(context.Context, *connect.Request[ironflowv1.ListStreamsRequest]) (*connect.Response[ironflowv1.ListStreamsResponse], error) {
	if s.failure != nil {
		return nil, s.failure
	}
	return connect.NewResponse(&ironflowv1.ListStreamsResponse{Streams: []*ironflowv1.GetStreamInfoResponse{{EntityId: "order/1", EntityType: "order", Version: 9007199254740993, EventCount: 2, UpdatedAt: timestamppb.New(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))}}}), nil
}
func (s *streamRegistryService) GetEntityHistory(_ context.Context, r *connect.Request[ironflowv1.GetEntityHistoryRequest]) (*connect.Response[ironflowv1.GetEntityHistoryResponse], error) {
	if r.Msg.EntityId != "order/1" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("wrong entity"))
	}
	return connect.NewResponse(&ironflowv1.GetEntityHistoryResponse{Entries: []*ironflowv1.EntityHistoryEntry{{EventName: "order.created", EntityVersion: 9007199254740993, EventDataValue: s.snapshot.StateValue}}}), nil
}
func (s *streamRegistryService) CreateSnapshot(_ context.Context, r *connect.Request[ironflowv1.CreateSnapshotRequest]) (*connect.Response[ironflowv1.CreateSnapshotResponse], error) {
	if r.Header().Get("Authorization") != "Bearer key" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing key"))
	}
	s.snapshot = &ironflowv1.GetSnapshotResponse{SnapshotId: "snap-1", EntityId: r.Msg.EntityId, EntityType: r.Msg.EntityType, EntityVersion: r.Msg.EntityVersion, State: r.Msg.State, StateValue: r.Msg.StateValue, CreatedAt: timestamppb.New(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))}
	return connect.NewResponse(&ironflowv1.CreateSnapshotResponse{SnapshotId: "snap-1"}), nil
}
func (s *streamRegistryService) GetSnapshot(context.Context, *connect.Request[ironflowv1.GetSnapshotRequest]) (*connect.Response[ironflowv1.GetSnapshotResponse], error) {
	return connect.NewResponse(s.snapshot), nil
}
func TestStreamRegistryConnect(t *testing.T) {
	service := &streamRegistryService{}
	mux := http.NewServeMux()
	mux.Handle(ironflowv1connect.NewEntityStreamServiceHandler(service))
	server := httptest.NewServer(mux)
	defer server.Close()
	client := NewClient(ClientConfig{ServerURL: server.URL, APIKey: "key"})
	ctx := context.Background()
	streams, err := client.ListStreams(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 1 || streams[0].Version != 9007199254740993 || streams[0].LastEventAt != "2026-09-06T12:00:00Z" {
		t.Fatalf("streams=%+v", streams)
	}
	input := CreateSnapshotInput{EntityType: "order", EntityVersion: 9007199254740993, State: []any{"created", true}}
	created, err := client.CreateSnapshot(ctx, "order/1", input)
	if err != nil {
		t.Fatal(err)
	}
	if created.SnapshotID != "snap-1" || created.EntityVersion != input.EntityVersion || !reflect.DeepEqual(created.State, input.State) {
		t.Fatalf("created=%+v", created)
	}
	snapshot, err := client.GetSnapshot(ctx, "order/1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.EntityVersion != input.EntityVersion || !reflect.DeepEqual(snapshot.State, input.State) || snapshot.CreatedAt != "2026-09-06T12:00:00Z" {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	history, err := client.GetEntityHistory(ctx, "order/1")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].EventName != "order.created" || history[0].EntityVersion != input.EntityVersion || !reflect.DeepEqual(history[0].Data, input.State) {
		t.Fatalf("history=%+v", history)
	}
	service.failure = connect.NewError(connect.CodeUnauthenticated, errors.New("denied"))
	_, err = client.ListStreams(ctx)
	if !errors.Is(err, ErrUnauthorized) || IsRetryable(err) {
		t.Fatalf("error=%v", err)
	}
}

func TestDeleteStream(t *testing.T) {
	var receivedPath string
	var receivedBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("failed to read request body: %v", err)
		}
		if err := json.Unmarshal(body, &receivedBody); err != nil {
			t.Errorf("failed to unmarshal request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"entityVersion":"3"}`))
	}))
	defer server.Close()

	client := NewClient(ClientConfig{ServerURL: server.URL, APIKey: "key"})
	version, err := client.DeleteStream(context.Background(), "order-1", true)
	if err != nil {
		t.Fatal(err)
	}
	if receivedPath != "/ironflow.v1.EntityStreamService/DeleteStream" {
		t.Fatalf("path=%s", receivedPath)
	}
	if receivedBody["entityId"] != "order-1" || receivedBody["purge"] != true {
		t.Fatalf("body=%+v", receivedBody)
	}
	if version != 3 {
		t.Fatalf("version=%d", version)
	}
}
