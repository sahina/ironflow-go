package ironflow

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestCreateWebhook(t *testing.T) {
	wh := CreateWebhook(WebhookConfig{
		ID: "stripe",
		Verify: func(req *WebhookRequest) error {
			if req.Header.Get("Stripe-Signature") == "" {
				return NewError("missing signature", "VERIFY_FAILED", false)
			}
			return nil
		},
		Transform: func(payload []byte) (*WebhookEvent, error) {
			var p map[string]any
			if err := json.Unmarshal(payload, &p); err != nil {
				return nil, err
			}
			return &WebhookEvent{
				Name:           "stripe." + p["type"].(string),
				Data:           payload,
				IdempotencyKey: p["id"].(string),
			}, nil
		},
	})

	if wh.Config.ID != "stripe" {
		t.Fatalf("Expected ID 'stripe', got %q", wh.Config.ID)
	}

	// Test verify — missing header
	err := wh.Config.Verify(&WebhookRequest{
		Body:   []byte(`{}`),
		Header: http.Header{},
	})
	if err == nil {
		t.Fatal("Expected verify error for missing signature")
	}

	// Test verify — with header
	err = wh.Config.Verify(&WebhookRequest{
		Body:   []byte(`{}`),
		Header: http.Header{"Stripe-Signature": []string{"sig123"}},
	})
	if err != nil {
		t.Fatalf("Unexpected verify error: %v", err)
	}

	// Test transform
	event, err := wh.Config.Transform([]byte(`{"type":"payment_intent.succeeded","id":"evt_123","data":{"amount":1000}}`))
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}
	if event.Name != "stripe.payment_intent.succeeded" {
		t.Fatalf("Expected name 'stripe.payment_intent.succeeded', got %q", event.Name)
	}
	if event.IdempotencyKey != "evt_123" {
		t.Fatalf("Expected key 'evt_123', got %q", event.IdempotencyKey)
	}
}
