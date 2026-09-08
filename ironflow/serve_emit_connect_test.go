package ironflow

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

type webhookEmitService struct {
	ironflowv1connect.UnimplementedIronflowServiceHandler
	received []*ironflowv1.TriggerRequest
}

func (s *webhookEmitService) Emit(_ context.Context, r *connect.Request[ironflowv1.TriggerRequest]) (*connect.Response[ironflowv1.TriggerResponse], error) {
	if r.Header().Get("Authorization") != "Bearer env-key" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("key required"))
	}
	s.received = append(s.received, r.Msg)
	return connect.NewResponse(&ironflowv1.TriggerResponse{EventId: "event", RunIds: []string{"run"}}), nil
}
func TestServeWebhookConnectEmit(t *testing.T) {
	t.Setenv("IRONFLOW_API_KEY", "env-key")
	service := &webhookEmitService{}
	_, rpc := ironflowv1connect.NewIronflowServiceHandler(service)
	server := httptest.NewServer(rpc)
	defer server.Close()
	for _, data := range []string{`{"order_id":"o1"}`, `false`, `null`, `["payload"]`} {
		handler := Serve(ServeConfig{ServerURL: server.URL, SkipVerification: true, Webhooks: []Webhook{CreateWebhook(WebhookConfig{ID: "test", Transform: func([]byte) (*WebhookEvent, error) {
			return &WebhookEvent{Name: "order.placed", Data: json.RawMessage(data), IdempotencyKey: "provider-id"}, nil
		}})}})
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("POST", "/webhooks/test", strings.NewReader(`{}`)))
		if rec.Code != http.StatusOK {
			t.Fatalf("webhook: %d %s", rec.Code, rec.Body.String())
		}
		if len(service.received) == 0 {
			t.Fatal("emit not called")
		}
		msg := service.received[len(service.received)-1]
		if msg.Event != "order.placed" || msg.IdempotencyKey != "provider-id" {
			t.Fatalf("lost emit fields: %v", msg)
		}
		var value any
		if msg.DataValue != nil {
			value = msg.DataValue.AsInterface()
		} else {
			value = msg.Data.AsMap()
		}
		actual, _ := json.Marshal(value)
		if string(actual) != data {
			t.Fatalf("payload: %s != %s", actual, data)
		}
	}
}
