package ironflow

import (
	"context"
)

// WebhookSource represents a registered webhook source.
type WebhookSource struct {
	ID              string         `json:"id"`
	EventPrefix     string         `json:"event_prefix"`
	VerifyHeader    string         `json:"verify_header,omitempty"`
	VerifyAlgorithm string         `json:"verify_algorithm,omitempty"`
	SourceType      string         `json:"source_type,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
	CreatedAt       string         `json:"created_at,omitempty"`
	UpdatedAt       string         `json:"updated_at,omitempty"`
}

// WebhookDelivery represents a single webhook delivery attempt.
type WebhookDelivery struct {
	ID         string `json:"id"`
	SourceID   string `json:"source_id"`
	ExternalID string `json:"external_id,omitempty"`
	Status     string `json:"status"`
	EventID    string `json:"event_id,omitempty"`
	Error      string `json:"error,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
}

// ListWebhookDeliveriesOpts configures a ListDeliveries query.
type ListWebhookDeliveriesOpts struct {
	// SourceID filters deliveries to a specific webhook source.
	SourceID string
	// Status filters deliveries by status (e.g. "pending", "delivered", "failed").
	Status string
	// Limit is the maximum number of results (0 = server default).
	Limit int
	// Offset is the pagination offset.
	Offset int
}

// CreateWebhookSourceInput contains parameters for creating a webhook source.
type CreateWebhookSourceInput struct {
	// ID is the unique identifier for the webhook source (required).
	ID string `json:"id"`
	// EventPrefix is the event name prefix that triggers this webhook (required).
	EventPrefix string `json:"event_prefix"`
	// VerifyHeader is the HTTP header name used for signature verification (optional).
	VerifyHeader string `json:"verify_header,omitempty"`
	// VerifyAlgorithm is the algorithm for signature verification, e.g. "sha256" (optional).
	VerifyAlgorithm string `json:"verify_algorithm,omitempty"`
	// VerifySecret is the secret used for signature verification (optional).
	VerifySecret string `json:"verify_secret,omitempty"`
	// Metadata is arbitrary key-value data attached to the webhook source (optional).
	Metadata map[string]any `json:"metadata,omitempty"`
}

// WebhookManagementClient provides access to the webhook management API.
type WebhookManagementClient struct {
	client *Client
}

// Webhooks returns a WebhookManagementClient for managing webhook sources and deliveries.
func (c *Client) Webhooks() *WebhookManagementClient {
	return &WebhookManagementClient{client: c}
}

// CreateSource registers a new webhook source.
func (wm *WebhookManagementClient) CreateSource(ctx context.Context, input CreateWebhookSourceInput) (*WebhookSource, error) {
	var result WebhookSource
	if err := wm.client.restRequest(ctx, "POST",
		"/ironflow.v1.WebhookService/CreateWebhookSource",
		input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListSources lists all registered webhook sources.
func (wm *WebhookManagementClient) ListSources(ctx context.Context) ([]WebhookSource, error) {
	var result struct {
		Sources []WebhookSource `json:"sources"`
	}
	if err := wm.client.restRequest(ctx, "POST",
		"/ironflow.v1.WebhookService/ListWebhookSources",
		map[string]any{"limit": 0, "offset": 0},
		&result); err != nil {
		return nil, err
	}
	if result.Sources == nil {
		return []WebhookSource{}, nil
	}
	return result.Sources, nil
}

// DeleteSource deletes a webhook source by ID.
func (wm *WebhookManagementClient) DeleteSource(ctx context.Context, id string) error {
	return wm.client.restRequest(ctx, "POST",
		"/ironflow.v1.WebhookService/DeleteWebhookSource",
		map[string]any{"id": id},
		nil)
}

// ListDeliveries lists webhook deliveries with optional filtering.
// Returns the deliveries, the total count, and any error.
func (wm *WebhookManagementClient) ListDeliveries(ctx context.Context, opts ListWebhookDeliveriesOpts) ([]WebhookDelivery, int, error) {
	body := map[string]any{
		"source_id": opts.SourceID,
		"status":    opts.Status,
		"limit":     opts.Limit,
		"offset":    opts.Offset,
	}
	var result struct {
		Deliveries []WebhookDelivery `json:"deliveries"`
		TotalCount int               `json:"total_count"`
	}
	if err := wm.client.restRequest(ctx, "POST",
		"/ironflow.v1.WebhookService/ListWebhookDeliveries",
		body, &result); err != nil {
		return nil, 0, err
	}
	if result.Deliveries == nil {
		return []WebhookDelivery{}, 0, nil
	}
	return result.Deliveries, result.TotalCount, nil
}
