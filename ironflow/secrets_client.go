package ironflow

import (
	"context"
	"encoding/json"
	"net/url"
)

// SecretsClient provides access to the Ironflow Secrets Management API.
type SecretsClient struct {
	client *Client
}

// PatchSecretInput renames a secret and/or updates its description without changing its value.
type PatchSecretInput struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
}

// Secrets returns a SecretsClient for interacting with the secrets management service.
func (c *Client) Secrets() *SecretsClient {
	return &SecretsClient{client: c}
}

// Get retrieves a secret by name, including its value.
//
// Security: Secret values are never logged.
func (sc *SecretsClient) Get(ctx context.Context, name string) (*Secret, error) {
	var result Secret
	if err := sc.client.restRequest(ctx, "GET", "/api/v1/secrets/"+url.PathEscape(name), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Set creates a new secret with the given name and value.
//
// Security: Secret values are never logged.
func (sc *SecretsClient) Set(ctx context.Context, name, value string) (*Secret, error) {
	body := map[string]string{
		"name":  name,
		"value": value,
	}
	var result Secret
	if err := sc.client.restRequest(ctx, "POST", "/api/v1/secrets", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Update updates the value of an existing secret.
//
// Security: Secret values are never logged.
func (sc *SecretsClient) Update(ctx context.Context, name, value string) (*Secret, error) {
	body := map[string]string{
		"value": value,
	}
	var result Secret
	if err := sc.client.restRequest(ctx, "PUT", "/api/v1/secrets/"+url.PathEscape(name), body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Patch renames a secret and/or updates its description without changing its value.
func (sc *SecretsClient) Patch(ctx context.Context, name string, input PatchSecretInput) (*Secret, error) {
	if input.Name == nil && input.Description == nil {
		return nil, NewError("secret patch requires name or description", "VALIDATION_ERROR", false)
	}
	var result Secret
	if err := sc.client.restRequest(ctx, "PATCH", "/api/v1/secrets/"+url.PathEscape(name), input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// List returns all secrets in the current environment. Values are not included in the listing.
//
// Handles both wrapped {"secrets": [...]} and plain array [...] response formats.
func (sc *SecretsClient) List(ctx context.Context) ([]SecretListEntry, error) {
	var raw json.RawMessage
	if err := sc.client.restRequest(ctx, "GET", "/api/v1/secrets", nil, &raw); err != nil {
		return nil, err
	}
	// Detect format: plain array starts with '['.
	for _, b := range raw {
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		if b == '[' {
			// Plain array format.
			var entries []SecretListEntry
			if err := json.Unmarshal(raw, &entries); err != nil {
				return nil, err
			}
			if entries == nil {
				return []SecretListEntry{}, nil
			}
			return entries, nil
		}
		break
	}
	// Wrapped object format: {"secrets": [...]}.
	var wrapped struct {
		Secrets []SecretListEntry `json:"secrets"`
	}
	if err := json.Unmarshal(raw, &wrapped); err != nil {
		return nil, err
	}
	if wrapped.Secrets == nil {
		return []SecretListEntry{}, nil
	}
	return wrapped.Secrets, nil
}

// Delete removes a secret by name.
func (sc *SecretsClient) Delete(ctx context.Context, name string) error {
	return sc.client.restRequest(ctx, "DELETE", "/api/v1/secrets/"+url.PathEscape(name), nil, nil)
}
