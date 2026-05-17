package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
)

// SchemaClient provides access to the Ironflow Event Schema Registry API.
type SchemaClient struct {
	client *Client
}

// Schemas returns a SchemaClient for interacting with the event schema registry.
func (c *Client) Schemas() *SchemaClient {
	return &SchemaClient{client: c}
}

// registerSchemaRequest is the wire format expected by the server.
type registerSchemaRequest struct {
	EventName   string `json:"event_name"`
	Version     int    `json:"version"`
	SchemaJSON  string `json:"schema_json"`
	Description string `json:"description,omitempty"`
}

// Register registers a new event schema (or a new version of an existing schema).
func (sc *SchemaClient) Register(ctx context.Context, input RegisterSchemaInput) (*EventSchema, error) {
	// The server expects schema_json as a serialized JSON string, not an object.
	schemaBytes, err := json.Marshal(input.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal schema: %w", err)
	}

	wireReq := registerSchemaRequest{
		EventName:  input.Name,
		Version:    input.Version,
		SchemaJSON: string(schemaBytes),
	}

	// Server returns {"status": "created"} on success.
	// Try to decode a full EventSchema; if the server only returned a status object,
	// fall back to building the result from the input.
	var result EventSchema
	if err := sc.client.restRequest(ctx, "POST", "/api/v1/events/schemas", wireReq, &result); err != nil {
		return nil, err
	}

	// If the server didn't return a full schema (e.g. only {"status":"created"}),
	// populate from the input we sent.
	if result.Name == "" {
		result.Name = input.Name
	}
	if result.Version == 0 {
		result.Version = input.Version
	}
	if result.Schema == nil {
		result.Schema = input.Schema
	}

	return &result, nil
}

// List returns all registered event schemas.
func (sc *SchemaClient) List(ctx context.Context) ([]EventSchema, error) {
	var result struct {
		Schemas []EventSchema `json:"schemas"`
	}
	if err := sc.client.restRequest(ctx, "GET", "/api/v1/events/schemas", nil, &result); err != nil {
		return nil, err
	}
	if result.Schemas == nil {
		return []EventSchema{}, nil
	}
	return result.Schemas, nil
}

// Get returns the latest registered version of an event schema by name.
func (sc *SchemaClient) Get(ctx context.Context, name string) (*EventSchema, error) {
	var result EventSchema
	path := fmt.Sprintf("/api/v1/events/schemas/%s", url.PathEscape(name))
	if err := sc.client.restRequest(ctx, "GET", path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetVersion returns a specific version of an event schema.
func (sc *SchemaClient) GetVersion(ctx context.Context, name string, version int) (*EventSchema, error) {
	var result EventSchema
	path := fmt.Sprintf("/api/v1/events/schemas/%s/%d", url.PathEscape(name), version)
	if err := sc.client.restRequest(ctx, "GET", path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Delete removes a specific version of an event schema.
func (sc *SchemaClient) Delete(ctx context.Context, name string, version int) error {
	path := fmt.Sprintf("/api/v1/events/schemas/%s/%d", url.PathEscape(name), version)
	return sc.client.restRequest(ctx, "DELETE", path, nil, nil)
}

// TestUpcast tests an upcast transformation between two schema versions.
func (sc *SchemaClient) TestUpcast(ctx context.Context, input TestUpcastInput) (*UpcastResult, error) {
	var result UpcastResult
	if err := sc.client.restRequest(ctx, "POST", "/api/v1/events/upcast", input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
