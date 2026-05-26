package ironflow

import (
	"context"
	"time"
)

// WebhookSource represents a registered webhook source.
// ID is server-generated (UUID with `wh_` prefix). Name is the operator-
// friendly display label and is NOT unique.
type WebhookSource struct {
	ID                        string         `json:"id"`
	Name                      string         `json:"name"`
	EventPrefix               string         `json:"event_prefix"`
	VerifyHeader              string         `json:"verify_header,omitempty"`
	VerifyAlgorithm           string         `json:"verify_algorithm,omitempty"`
	VerifySecretSet           bool           `json:"verify_secret_set,omitempty"`
	VerifySecretPrevSet       bool           `json:"verify_secret_prev_set,omitempty"`
	VerifySecretPrevExpiresAt string         `json:"verify_secret_prev_expires_at,omitempty"`
	SourceType                string         `json:"source_type,omitempty"`
	Metadata                  map[string]any `json:"metadata,omitempty"`
	CreatedAt                 string         `json:"created_at,omitempty"`
	UpdatedAt                 string         `json:"updated_at,omitempty"`
}

// WebhookDelivery represents a single webhook delivery attempt.
type WebhookDelivery struct {
	ID           string `json:"id"`
	SourceID     string `json:"source_id"`
	ExternalID   string `json:"external_id,omitempty"`
	Status       string `json:"status"`
	EventID      string `json:"event_id,omitempty"`
	Error        string `json:"error,omitempty"`
	SignatureKey string `json:"signature_key,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
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
// ID is server-generated — callers do not supply one.
type CreateWebhookSourceInput struct {
	// Name is the operator-friendly display label (required, NOT unique).
	Name string `json:"name"`
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

// GetSource fetches a single webhook source by ID.
func (wm *WebhookManagementClient) GetSource(ctx context.Context, id string) (*WebhookSource, error) {
	var result WebhookSource
	if err := wm.client.restRequest(ctx, "POST",
		"/ironflow.v1.WebhookService/GetWebhookSource",
		map[string]any{"id": id}, &result); err != nil {
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

// UpdateWebhookSourceInput contains parameters for updating a webhook source.
// verify_secret is intentionally NOT included — use RotateSecret to change it.
// event_prefix and source_type are immutable post-create.
//
// Semantics are FULL-REPLACE, not patch. Every editable field on the server is
// overwritten with the value in this struct. Omitting a field (zero value or
// nil map) clears the corresponding column server-side. To preserve existing
// values, fetch the current source first and copy across whatever you do not
// intend to change:
//
//	current, _ := client.Webhooks().GetSource(ctx, id)
//	_, err := client.Webhooks().UpdateSource(ctx, UpdateWebhookSourceInput{
//	    ID:              id,
//	    Name:            "new name",
//	    VerifyHeader:    current.VerifyHeader,    // preserve
//	    VerifyAlgorithm: current.VerifyAlgorithm, // preserve
//	    Metadata:        current.Metadata,        // preserve
//	})
type UpdateWebhookSourceInput struct {
	// ID identifies the webhook source to update (required).
	ID string `json:"id"`
	// Name is the operator-friendly display label (required).
	Name string `json:"name"`
	// VerifyHeader is the HTTP header for signature verification. Empty clears it.
	VerifyHeader string `json:"verify_header,omitempty"`
	// VerifyAlgorithm is the signature algorithm. Empty clears it.
	VerifyAlgorithm string `json:"verify_algorithm,omitempty"`
	// Metadata replaces the metadata blob entirely. nil clears the column.
	Metadata map[string]any `json:"metadata,omitempty"`
}

// UpdateSource modifies editable fields on an existing webhook source.
// Does not touch verify_secret — use RotateSecret.
func (wm *WebhookManagementClient) UpdateSource(ctx context.Context, input UpdateWebhookSourceInput) (*WebhookSource, error) {
	var result WebhookSource
	if err := wm.client.restRequest(ctx, "POST",
		"/ironflow.v1.WebhookService/UpdateWebhookSource",
		input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// RotateWebhookSecretInput configures a secret rotation.
type RotateWebhookSecretInput struct {
	// ID identifies the webhook source to rotate (required).
	ID string
	// VerifySecret is the new secret value (required, non-empty). To
	// disable signature verification entirely use DisableSignatureVerification.
	VerifySecret string
	// GracePeriod is the duration during which the prior current secret
	// keeps verifying as prev. Nil = server default (24 h or env
	// override). Zero pointer-target = instant cutover. The server
	// clamps any value above 7 days with InvalidArgument.
	GracePeriod *time.Duration
	// ExpectedUpdatedAt is the optimistic-concurrency token. When set,
	// the server requires it to match webhook_sources.updated_at and
	// returns Aborted on mismatch. Leave nil to accept last-writer-wins.
	ExpectedUpdatedAt *time.Time
}

// RotateSecret rotates verify_secret on a webhook source, preserving
// the prior current secret as the previous slot for the grace window.
// See ADR 0024.
func (wm *WebhookManagementClient) RotateSecret(ctx context.Context, input RotateWebhookSecretInput) (*WebhookSource, error) {
	var result WebhookSource
	body := map[string]any{
		"id":            input.ID,
		"verify_secret": input.VerifySecret,
	}
	if input.GracePeriod != nil {
		body["grace_seconds"] = int32(input.GracePeriod.Seconds())
	}
	if input.ExpectedUpdatedAt != nil {
		body["expected_updated_at"] = input.ExpectedUpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	if err := wm.client.restRequest(ctx, "POST",
		"/ironflow.v1.WebhookService/RotateWebhookSecret",
		body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ExpireWebhookSecretPrevInput configures a force-expire.
type ExpireWebhookSecretPrevInput struct {
	ID                string
	ExpectedUpdatedAt *time.Time
}

// ExpireSecretPrev force-expires the previous secret slot. Idempotent —
// calling on a source with no prev returns the source unchanged.
func (wm *WebhookManagementClient) ExpireSecretPrev(ctx context.Context, id string) (*WebhookSource, error) {
	return wm.ExpireSecretPrevWithInput(ctx, ExpireWebhookSecretPrevInput{ID: id})
}

// ExpireSecretPrevWithInput supports optimistic concurrency via
// ExpectedUpdatedAt. When set, the server returns Aborted on mismatch.
func (wm *WebhookManagementClient) ExpireSecretPrevWithInput(ctx context.Context, input ExpireWebhookSecretPrevInput) (*WebhookSource, error) {
	var result WebhookSource
	body := map[string]any{"id": input.ID}
	if input.ExpectedUpdatedAt != nil {
		body["expected_updated_at"] = input.ExpectedUpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	if err := wm.client.restRequest(ctx, "POST",
		"/ironflow.v1.WebhookService/ExpireWebhookSecretPrev",
		body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DisableWebhookSignatureVerificationInput configures the disable call.
type DisableWebhookSignatureVerificationInput struct {
	ID                string
	GracePeriod       *time.Duration
	ExpectedUpdatedAt *time.Time
}

// DisableSignatureVerification clears verify_secret and preserves the
// prior current secret as prev for the grace window. After the window
// the source operates unsigned.
func (wm *WebhookManagementClient) DisableSignatureVerification(ctx context.Context, id string, grace *time.Duration) (*WebhookSource, error) {
	return wm.DisableSignatureVerificationWithInput(ctx, DisableWebhookSignatureVerificationInput{ID: id, GracePeriod: grace})
}

// DisableSignatureVerificationWithInput supports optimistic concurrency
// via ExpectedUpdatedAt.
func (wm *WebhookManagementClient) DisableSignatureVerificationWithInput(ctx context.Context, input DisableWebhookSignatureVerificationInput) (*WebhookSource, error) {
	var result WebhookSource
	body := map[string]any{"id": input.ID}
	if input.GracePeriod != nil {
		body["grace_seconds"] = int32(input.GracePeriod.Seconds())
	}
	if input.ExpectedUpdatedAt != nil {
		body["expected_updated_at"] = input.ExpectedUpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	if err := wm.client.restRequest(ctx, "POST",
		"/ironflow.v1.WebhookService/DisableWebhookSignatureVerification",
		body, &result); err != nil {
		return nil, err
	}
	return &result, nil
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
