package ironflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"connectrpc.com/connect"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type upcastService struct {
	ironflowv1connect.UnimplementedEventSchemaServiceHandler
}

func (upcastService) TestUpcast(_ context.Context, req *connect.Request[ironflowv1.TestUpcastRequest]) (*connect.Response[ironflowv1.TestUpcastResponse], error) {
	if req.Msg.EventName == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("event_name is required"))
	}
	return connect.NewResponse(&ironflowv1.TestUpcastResponse{Data: req.Msg.Data, DataValue: req.Msg.DataValue, StepsApplied: []*ironflowv1.UpcastStep{{FromVersion: 1, ToVersion: 2, Description: "upcast to v2"}}}), nil
}
func TestSchemaClient_TestUpcast_ServerShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(ironflowv1connect.NewEventSchemaServiceHandler(upcastService{}))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := NewClient(ClientConfig{ServerURL: srv.URL})
	for _, data := range []any{map[string]any{"orderId": "order-1"}, []any{"order-1", true, nil}, "value", nil} {
		result, err := client.Schemas().TestUpcast(context.Background(), TestUpcastInput{EventName: "order.placed", FromVersion: 1, ToVersion: 2, Data: data})
		if err != nil {
			t.Fatal(err)
		}
		if !result.Success || !reflect.DeepEqual(result.Data, data) {
			t.Fatalf("result = %#v, want data %#v and success", result, data)
		}
	}
	_, err := client.Schemas().TestUpcast(context.Background(), TestUpcastInput{})
	var sdkError *IronflowError
	if !errors.As(err, &sdkError) || IsRetryable(err) {
		t.Fatalf("want permanent SDK error, got %v", err)
	}
}
