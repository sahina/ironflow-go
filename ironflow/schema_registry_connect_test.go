package ironflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"reflect"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type schemaRegistryService struct {
	ironflowv1connect.UnimplementedEventSchemaServiceHandler
	schema *ironflowv1.GetSchemaResponse
	calls  int
}

func (s *schemaRegistryService) RegisterSchema(_ context.Context, req *connect.Request[ironflowv1.RegisterSchemaRequest]) (*connect.Response[ironflowv1.RegisterSchemaResponse], error) {
	s.calls++
	if req.Header().Get("Authorization") != "Bearer key" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing key"))
	}
	s.schema = &ironflowv1.GetSchemaResponse{EventName: req.Msg.EventName, Version: req.Msg.Version, SchemaJson: req.Msg.SchemaJson, CreatedAt: timestamppb.New(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))}
	return connect.NewResponse(&ironflowv1.RegisterSchemaResponse{Status: "created"}), nil
}
func (s *schemaRegistryService) GetSchema(_ context.Context, req *connect.Request[ironflowv1.GetSchemaRequest]) (*connect.Response[ironflowv1.GetSchemaResponse], error) {
	s.calls++
	if s.schema == nil || req.Msg.EventName != s.schema.EventName || (req.Msg.Version != 0 && req.Msg.Version != s.schema.Version) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("schema not found"))
	}
	return connect.NewResponse(s.schema), nil
}
func (s *schemaRegistryService) ListSchemas(context.Context, *connect.Request[ironflowv1.ListSchemasRequest]) (*connect.Response[ironflowv1.ListSchemasResponse], error) {
	result := &ironflowv1.ListSchemasResponse{}
	if s.schema != nil {
		result.Schemas = []*ironflowv1.SchemaInfo{{EventName: s.schema.EventName, Version: s.schema.Version, CreatedAt: s.schema.CreatedAt, SchemaJson: s.schema.SchemaJson}}
	}
	return connect.NewResponse(result), nil
}
func (s *schemaRegistryService) DeleteSchema(_ context.Context, req *connect.Request[ironflowv1.DeleteSchemaRequest]) (*connect.Response[ironflowv1.DeleteSchemaResponse], error) {
	s.calls++
	if s.schema == nil || req.Msg.EventName != s.schema.EventName || req.Msg.Version != s.schema.Version {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("schema not found"))
	}
	s.schema = nil
	return connect.NewResponse(&ironflowv1.DeleteSchemaResponse{}), nil
}
func TestSchemaRegistryConnectLifecycle(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(ironflowv1connect.NewEventSchemaServiceHandler(&schemaRegistryService{}))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := NewClient(ClientConfig{ServerURL: srv.URL, APIKey: "key"}).Schemas()
	ctx := context.Background()
	input := RegisterSchemaInput{Name: "order/placed", Version: 2, Schema: map[string]any{"type": "object"}}
	registered, err := client.Register(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(input.Schema, registered.Schema) {
		t.Fatalf("registered=%+v", registered)
	}
	latest, err := client.Get(ctx, input.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(input.Schema, latest.Schema) || latest.CreatedAt != "2026-09-06T12:00:00Z" {
		t.Fatalf("latest=%+v", latest)
	}
	version, err := client.GetVersion(ctx, input.Name, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(latest, version) {
		t.Fatalf("version=%+v, latest=%+v", version, latest)
	}
	listed, err := client.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Name != input.Name || !reflect.DeepEqual(input.Schema, listed[0].Schema) {
		t.Fatalf("listed=%+v", listed)
	}
	if err := client.Delete(ctx, input.Name, 2); err != nil {
		t.Fatal(err)
	}
	listed, err = client.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 0 {
		t.Fatalf("listed=%+v", listed)
	}
	_, err = client.Get(ctx, input.Name)
	if err == nil || IsRetryable(err) {
		t.Fatalf("expected permanent not-found, got %v", err)
	}
}

func TestSchemaVersionRejectsOverflowWithoutRequest(t *testing.T) {
	large := int64(1) << 32
	for _, version := range []int{int(large), int(large + 1)} {
		service := &schemaRegistryService{}
		mux := http.NewServeMux()
		mux.Handle(ironflowv1connect.NewEventSchemaServiceHandler(service))
		srv := httptest.NewServer(mux)
		client := NewClient(ClientConfig{ServerURL: srv.URL, APIKey: "key"}).Schemas()
		_, registerErr := client.Register(context.Background(), RegisterSchemaInput{Name: "order", Version: version, Schema: map[string]any{}})
		_, getErr := client.GetVersion(context.Background(), "order", version)
		deleteErr := client.Delete(context.Background(), "order", version)
		srv.Close()
		for _, err := range []error{registerErr, getErr, deleteErr} {
			if err == nil || IsRetryable(err) {
				t.Errorf("version %d: want permanent error, got %v", version, err)
			}
		}
		if service.calls != 0 {
			t.Errorf("version %d reached handler %d times", version, service.calls)
		}
	}
}
