package ironflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type functionListService struct {
	ironflowv1connect.UnimplementedIronflowServiceHandler
	failure error
}

func (s *functionListService) ListFunctions(_ context.Context, req *connect.Request[ironflowv1.ListFunctionsRequest]) (*connect.Response[ironflowv1.ListFunctionsResponse], error) {
	if req.Header().Get("Authorization") != "Bearer key" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing key"))
	}
	if s.failure != nil {
		return nil, s.failure
	}
	return connect.NewResponse(&ironflowv1.ListFunctionsResponse{Functions: []*ironflowv1.Function{{Id: "fn", Name: "Name", Status: ironflowv1.FunctionStatus_FUNCTION_STATUS_ACTIVE, PreferredMode: ironflowv1.ExecutionMode_EXECUTION_MODE_PULL, CreatedAt: timestamppb.New(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))}}}), nil
}
func TestFunctionListConnect(t *testing.T) {
	service := &functionListService{}
	mux := http.NewServeMux()
	mux.Handle(ironflowv1connect.NewIronflowServiceHandler(service))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := NewClient(ClientConfig{ServerURL: srv.URL, APIKey: "key"})
	result, err := client.ListFunctions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0].Status != "active" || result[0].PreferredMode != "pull" || result[0].CreatedAt != "2026-09-06T12:00:00Z" {
		t.Fatalf("functions=%+v", result)
	}
	service.failure = connect.NewError(connect.CodeUnauthenticated, errors.New("expired"))
	_, err = client.ListFunctions(context.Background())
	if !errors.Is(err, ErrUnauthorized) || IsRetryable(err) {
		t.Fatalf("error=%v", err)
	}
}
