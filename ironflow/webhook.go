package ironflow

import (
	"encoding/json"
	"net/http"
)

// WebhookConfig defines a webhook source.
type WebhookConfig struct {
	// ID is the unique identifier for this webhook source.
	ID string

	// Verify validates the incoming webhook request (e.g., signature check).
	// Return nil if valid, error if invalid.
	Verify func(req *WebhookRequest) error

	// Transform converts the raw webhook payload into an Ironflow event.
	Transform func(payload []byte) (*WebhookEvent, error)
}

// WebhookRequest contains the raw webhook HTTP request data.
type WebhookRequest struct {
	Body   []byte
	Header http.Header
	Method string
	URL    string
}

// WebhookEvent is the transformed event to emit.
type WebhookEvent struct {
	Name           string          `json:"name"`
	Data           json.RawMessage `json:"data"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// Webhook wraps a WebhookConfig.
type Webhook struct {
	Config WebhookConfig
}

// CreateWebhook creates a new Webhook from the given config.
func CreateWebhook(config WebhookConfig) Webhook {
	return Webhook{Config: config}
}
