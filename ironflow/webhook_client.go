package ironflow

import (
	"context"
	"time"
)

// WebhookSource represents a registered webhook source.
// ID is server-generated (UUID with `wh_` prefix). Name is the operator-
// friendly display label and is NOT unique.
// WebhookVerifyConfig describes a provider's signature scheme and where the
// event name and delivery ID live (ADR 0049).
//
// It carries no provider identity — GitHub, Stripe, Shopify, Slack and Standard
// Webhooks are all expressible as field values here — so supporting a provider
// is configuration rather than an Ironflow release.
//
// Nil means the source verifies the legacy way: hex HMAC over the bare request
// body, using VerifyHeader and VerifyAlgorithm.
type WebhookVerifyConfig struct {
	// SignatureHeader carries the signature, e.g. "Stripe-Signature".
	SignatureHeader string `json:"signature_header"`
	// EntrySeparator splits a multi-entry header. Empty means one entry.
	// Stripe uses ",", Standard Webhooks uses " ".
	EntrySeparator string `json:"entry_separator,omitempty"`
	// KVDelimiter splits "key<delim>value" within an entry. Empty means the
	// entry is a bare signature (Shopify). GitHub/Stripe/Slack use "=",
	// Standard Webhooks uses ",".
	KVDelimiter string `json:"kv_delimiter,omitempty"`
	// SignatureKey selects which entries hold signatures, e.g. "v1". EVERY
	// matching entry is tried, because providers send several during rotation.
	SignatureKey string `json:"signature_key,omitempty"`
	// TimestampHeader names a separate timestamp header (Slack).
	TimestampHeader string `json:"timestamp_header,omitempty"`
	// TimestampKey instead reads it from a keyed entry in SignatureHeader
	// (Stripe's "t="). TimestampHeader wins when both are set.
	TimestampKey string `json:"timestamp_key,omitempty"`
	// SigningTemplate is the string that gets signed, with {body}, {ts} and
	// {id} placeholders. {id} resolves through DedupIDPath.
	SigningTemplate string `json:"signing_template"`
	// Encoding is "hex" or "base64".
	Encoding string `json:"encoding"`
	// Algorithm is "hmac-sha256" or "hmac-sha1".
	Algorithm string `json:"algorithm"`
	// ToleranceSeconds bounds replay. Honored ONLY when SigningTemplate
	// contains {ts} — an unsigned timestamp is attacker-controlled, so a
	// tolerance over one is decoration and is rejected rather than ignored.
	ToleranceSeconds int `json:"tolerance_seconds,omitempty"`
	// EventNamePath locates the event type: "header:X-GitHub-Event" or
	// "body:type". Empty falls back to the body "type" key.
	EventNamePath string `json:"event_name_path,omitempty"`
	// DedupIDPath locates the delivery ID: "header:X-GitHub-Delivery" or a
	// dotted body path like "body:data.object.id". Empty falls back to the
	// body "id" and "event_id" keys.
	DedupIDPath string `json:"dedup_id_path,omitempty"`
}

// WebhookSource represents a registered webhook source.
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
	// IngestTokenPrefix is a short display fragment (ifwh_ + 8 chars) of the
	// per-source ingest token. Empty on sources predating migration 046.
	IngestTokenPrefix string `json:"ingest_token_prefix,omitempty"`
	// IngestToken is the RAW ingest token, populated ONLY by CreateSource and
	// RotateIngestToken (ADR 0048). The server stores just a hash, so this is
	// the single opportunity to capture it — every later read returns it
	// empty. Without it the source cannot receive deliveries.
	IngestToken string `json:"ingest_token,omitempty"`
	// VerifyConfig is the signature descriptor (ADR 0049). Nil means legacy
	// verification via VerifyHeader/VerifyAlgorithm.
	VerifyConfig *WebhookVerifyConfig `json:"verify_config,omitempty"`
	CreatedAt    string               `json:"created_at,omitempty"`
	UpdatedAt    string               `json:"updated_at,omitempty"`
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
	// VerifyConfig is the signature descriptor (ADR 0049). Set it to verify a
	// provider whose scheme is not "hex HMAC over the bare body" — which is
	// every provider except GitHub. Omit for legacy verification.
	VerifyConfig *WebhookVerifyConfig `json:"verify_config,omitempty"`
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
	// VerifyConfig is the signature descriptor (ADR 0049). Unlike the fields
	// above, OMITTING IT PRESERVES the stored descriptor rather than clearing
	// it — a partial update would otherwise silently downgrade the source to
	// legacy body-only verification and every real delivery would then fail.
	VerifyConfig *WebhookVerifyConfig `json:"verify_config,omitempty"`
	// ExpectedUpdatedAt is an optimistic-concurrency token: send the UpdatedAt
	// you last read and the write is rejected with ABORTED if the row moved.
	// Worth sending precisely because VerifyConfig is preserve-on-omit — an
	// unguarded rename can otherwise revert someone else's descriptor change.
	// Omit to skip the check.
	ExpectedUpdatedAt string `json:"expected_updated_at,omitempty"`
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

// RotateWebhookIngestTokenInput configures RotateIngestToken.
type RotateWebhookIngestTokenInput struct {
	ID string
	// ExpectedUpdatedAt is an optimistic-concurrency token. When set, the
	// server returns Aborted if the source changed since you read it.
	ExpectedUpdatedAt *time.Time
}

// RotateIngestToken replaces the source's per-source ingest token (ADR 0048).
//
// There is no grace window: the previous token stops working the moment this
// returns, so update the provider's URL immediately. The new raw token is in
// the returned WebhookSource.IngestToken and is unrecoverable afterwards —
// capture it here or you will have to rotate again.
func (wm *WebhookManagementClient) RotateIngestToken(ctx context.Context, input RotateWebhookIngestTokenInput) (*WebhookSource, error) {
	var result WebhookSource
	body := map[string]any{"id": input.ID}
	if input.ExpectedUpdatedAt != nil {
		body["expected_updated_at"] = input.ExpectedUpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	if err := wm.client.restRequest(ctx, "POST",
		"/ironflow.v1.WebhookService/RotateWebhookIngestToken", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
