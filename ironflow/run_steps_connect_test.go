package ironflow

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type runStepsService struct {
	ironflowv1connect.UnimplementedIronflowServiceHandler
}

func (runStepsService) GetRunSteps(_ context.Context, req *connect.Request[ironflowv1.GetRunStepsRequest]) (*connect.Response[ironflowv1.GetRunStepsResponse], error) {
	if req.Header().Get("Authorization") != "Bearer key" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing key"))
	}
	if req.Msg.RunId == "missing" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("missing"))
	}
	duration := int64(2147483648)
	return connect.NewResponse(&ironflowv1.GetRunStepsResponse{Steps: []*ironflowv1.Step{{Id: "row", RunId: req.Msg.RunId, StepId: "charge", StepType: ironflowv1.StepType_STEP_TYPE_COMPENSATE, Status: ironflowv1.StepStatus_STEP_STATUS_COMPLETED, DurationMsFull: &duration, OutputValue: structpb.NewBoolValue(false), CreatedAt: timestamppb.New(time.Unix(123, 0)), WaitEventName: "wake", CompensationFor: "payment"}}}), nil
}
func TestRunStepsConnect(t *testing.T) {
	_, handler := ironflowv1connect.NewIronflowServiceHandler(runStepsService{})
	server := httptest.NewServer(handler)
	defer server.Close()
	client := &Client{serverURL: server.URL, httpClient: server.Client(), apiKey: "key", logger: NewNoopLogger()}
	result, err := client.GetRunSteps(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	if result.Count != 1 || result.Steps[0].StepType != "compensate" || result.Steps[0].Output != false || result.Steps[0].DurationMs == nil || *result.Steps[0].DurationMs != 2147483648 || result.Steps[0].WaitEventName != "wake" || result.Steps[0].CompensationFor != "payment" || result.Steps[0].CreatedAt.Unix() != 123 {
		t.Fatalf("lost step fields: %+v", result)
	}
	_, err = client.GetRunSteps(context.Background(), "missing")
	var typed *IronflowError
	if !errors.As(err, &typed) || typed.Code != "NOT_FOUND" || IsRetryable(err) {
		t.Fatalf("error contract: %v", err)
	}
}
