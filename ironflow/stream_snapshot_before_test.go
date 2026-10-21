package ironflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type snapshotRequestService struct {
	ironflowv1connect.UnimplementedEntityStreamServiceHandler
	got *ironflowv1.GetSnapshotRequest
}

func (s *snapshotRequestService) GetSnapshot(_ context.Context, r *connect.Request[ironflowv1.GetSnapshotRequest]) (*connect.Response[ironflowv1.GetSnapshotResponse], error) {
	s.got = r.Msg
	return connect.NewResponse(&ironflowv1.GetSnapshotResponse{SnapshotId: "snap-1", EntityId: r.Msg.EntityId}), nil
}

// BeforeVersion must reach the server, and a call without options must keep
// asking for the latest snapshot (before_version 0).
func TestGetSnapshotBeforeVersion(t *testing.T) {
	service := &snapshotRequestService{}
	mux := http.NewServeMux()
	mux.Handle(ironflowv1connect.NewEntityStreamServiceHandler(service))
	server := httptest.NewServer(mux)
	defer server.Close()
	client := NewClient(ClientConfig{ServerURL: server.URL})

	if _, err := client.GetSnapshot(context.Background(), "order-1"); err != nil {
		t.Fatalf("without options: %v", err)
	}
	if service.got.BeforeVersion != 0 {
		t.Fatalf("without options: before_version = %d, want 0", service.got.BeforeVersion)
	}
	// A value past 2^53 proves the int64 survives the JSON round trip.
	const before = 9007199254740993
	if _, err := client.GetSnapshot(context.Background(), "order-1", GetSnapshotOpts{BeforeVersion: before}); err != nil {
		t.Fatalf("with BeforeVersion: %v", err)
	}
	if service.got.BeforeVersion != before || service.got.EntityId != "order-1" {
		t.Fatalf("with BeforeVersion: request = %+v, want before_version %d", service.got, before)
	}
}
