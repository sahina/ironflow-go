package ironflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"
)

// ProjectionClient provides access to the Ironflow Projection Management API.
type ProjectionClient struct {
	client *Client
}

// Projections returns a ProjectionClient for interacting with the projection management service.
func (c *Client) Projections() *ProjectionClient {
	return &ProjectionClient{client: c}
}

// Get retrieves the current materialized state of a projection by name.
//
// Returns a flat *ProjectionStateResult after stripping the server REST
// envelope. See peelProjection / issue #610 / CHANGELOG 0.20.0.
//
// Pass WithPartition("key") to read a specific partition. When omitted the
// server returns the __global__ partition.
func (pc *ProjectionClient) Get(ctx context.Context, name string, opts ...GetProjectionOption) (*ProjectionStateResult, error) {
	options := getProjectionOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	path := "/api/v1/projections/" + url.PathEscape(name)
	if options.partition != "" {
		path += "?partition=" + url.QueryEscape(options.partition)
	}

	var raw json.RawMessage
	if err := pc.client.restRequest(ctx, "GET", path, nil, &raw); err != nil {
		return nil, err
	}
	return peelProjection(raw, options.partition)
}

// peelProjection strips the server REST envelope and returns a flat
// ProjectionStateResult.
//
// Server wire shape (`GET /api/v1/projections/{name}`):
//
//	{
//	  name, version, mode, last_event_seq, updated_at,  // registry-level
//	  state: {                                          // optional inner row
//	    projection_name, environment_id, partition_key,
//	    state: <user state>,
//	    last_event_id, last_event_seq, last_event_time, version, updated_at
//	  }
//	}
//
// Behavior:
//   - Outer `state` absent or null: returns empty user state with
//     Partition = requestedPartition (or "__global__"), LastEventTime nil.
//   - Inner `state.state` field absent: returns an error wrapping a
//     PROJECTION_ENVELOPE_DRIFT diagnostic. Indicates server contract drift.
//   - Inner `state.state` is null: treated as empty.
func peelProjection(raw json.RawMessage, requestedPartition string) (*ProjectionStateResult, error) {
	if len(raw) == 0 {
		return nil, driftError("empty response body", nil)
	}

	var env map[string]json.RawMessage
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, driftError("cannot decode response", err)
	}

	name, err := decodeString(env["name"])
	if err != nil || name == "" {
		return nil, driftError("missing name", nil)
	}

	partitionFallback := requestedPartition
	if partitionFallback == "" {
		partitionFallback = "__global__"
	}

	result := &ProjectionStateResult{
		Name:      name,
		Partition: partitionFallback,
		Mode:      decodeMode(env["mode"]),
	}
	result.Version = decodeInt64(env["version"])
	result.LastEventSeq = decodeInt64(env["last_event_seq"])
	if s, _ := decodeString(env["status"]); s != "" {
		result.Status = s
	}
	if msg, _ := decodeString(env["error_message"]); msg != "" {
		result.ErrorMessage = msg
	}
	if t, err := decodeTime(env["updated_at"]); err != nil {
		return nil, driftError("invalid updated_at", err)
	} else if t != nil {
		result.UpdatedAt = *t
	}

	innerRaw, ok := env["state"]
	if !ok || isJSONNull(innerRaw) {
		result.State = map[string]any{}
		return result, nil
	}

	var inner map[string]json.RawMessage
	if err := json.Unmarshal(innerRaw, &inner); err != nil {
		return nil, driftError("state field is not an object", err)
	}

	stateRaw, hasState := inner["state"]
	if !hasState {
		return nil, driftError("expected state.state (inner user state field missing)", nil)
	}

	if isJSONNull(stateRaw) {
		result.State = map[string]any{}
	} else {
		var userState any
		if err := json.Unmarshal(stateRaw, &userState); err != nil {
			return nil, driftError("cannot decode user state", err)
		}
		result.State = userState
	}

	if pk, _ := decodeString(inner["partition_key"]); pk != "" {
		result.Partition = pk
	}
	if eid, _ := decodeString(inner["last_event_id"]); eid != "" {
		result.LastEventID = eid
	}
	// Registry-level Version + LastEventSeq are authoritative; inner state-row
	// values can lag during rebuild and are intentionally NOT used here.
	if t, err := decodeTime(inner["last_event_time"]); err != nil {
		return nil, driftError("invalid last_event_time", err)
	} else {
		result.LastEventTime = t
	}
	if t, err := decodeTime(inner["updated_at"]); err == nil && t != nil && result.UpdatedAt.IsZero() {
		result.UpdatedAt = *t
	}

	return result, nil
}

// driftError returns an *IronflowError with code "PROJECTION_ENVELOPE_DRIFT"
// matching the JS SDK's error code, so cross-language consumers can detect
// the drift class via errors.Is or by inspecting the Code field.
func driftError(reason string, cause error) error {
	msg := "projection envelope drift: " + reason
	if cause != nil {
		msg = msg + ": " + cause.Error()
	}
	return &IronflowError{
		Message:   msg,
		Code:      "PROJECTION_ENVELOPE_DRIFT",
		Retryable: false,
		Cause:     cause,
	}
}

func isJSONNull(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func decodeString(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", err
	}
	return s, nil
}

func decodeInt64(raw json.RawMessage) int64 {
	if len(raw) == 0 || isJSONNull(raw) {
		return 0
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n
	}
	// server may emit int64-as-string for large values (#600 camelCase compat)
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		var parsed int64
		if _, scanErr := fmt.Sscan(s, &parsed); scanErr == nil {
			return parsed
		}
	}
	return 0
}

func decodeTime(raw json.RawMessage) (*time.Time, error) {
	if len(raw) == 0 || isJSONNull(raw) {
		return nil, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		// fall back to RFC3339 without nanos
		t, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, err
		}
	}
	// Server emits Go's `time.Time{}` zero value as "0001-01-01T00:00:00Z"
	// (no omitempty on internal/store/models.go:558). Treat as nil ("no events
	// processed yet") instead of a real epoch-adjacent timestamp.
	if t.IsZero() {
		return nil, nil
	}
	return &t, nil
}

func decodeMode(raw json.RawMessage) string {
	s, _ := decodeString(raw)
	if s == "managed" || s == "external" {
		return s
	}
	return "managed"
}

// List returns the status of all projections.
func (pc *ProjectionClient) List(ctx context.Context) ([]ProjectionStatusInfo, error) {
	var result struct {
		Projections []ProjectionStatusInfo `json:"projections"`
	}
	if err := pc.client.restRequest(ctx, "GET", "/api/v1/projections", nil, &result); err != nil {
		return nil, err
	}
	if result.Projections == nil {
		return []ProjectionStatusInfo{}, nil
	}
	return result.Projections, nil
}

// GetStatus retrieves the operational status of a projection by name.
func (pc *ProjectionClient) GetStatus(ctx context.Context, name string) (*ProjectionStatusInfo, error) {
	var result ProjectionStatusInfo
	if err := pc.client.restRequest(ctx, "GET", "/api/v1/projections/"+url.PathEscape(name)+"/status", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Rebuild triggers a full rebuild of a projection and returns the rebuild job.
func (pc *ProjectionClient) Rebuild(ctx context.Context, name string) (*RebuildJob, error) {
	var result RebuildJob
	if err := pc.client.restRequest(ctx, "POST", "/api/v1/projections/"+url.PathEscape(name)+"/rebuild", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetRebuildJob retrieves the current state of an in-progress or completed rebuild job.
func (pc *ProjectionClient) GetRebuildJob(ctx context.Context, name string) (*RebuildJob, error) {
	var result RebuildJob
	if err := pc.client.restRequest(ctx, "GET", "/api/v1/projections/"+url.PathEscape(name)+"/rebuild", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Delete removes a projection by name.
func (pc *ProjectionClient) Delete(ctx context.Context, name string) error {
	return pc.client.restRequest(ctx, "DELETE", "/api/v1/projections/"+url.PathEscape(name), nil, nil)
}

// Pause pauses a running projection.
func (pc *ProjectionClient) Pause(ctx context.Context, name string) error {
	return pc.client.restRequest(ctx, "POST", "/api/v1/projections/"+url.PathEscape(name)+"/pause", nil, nil)
}

// Resume resumes a paused projection.
func (pc *ProjectionClient) Resume(ctx context.Context, name string) error {
	return pc.client.restRequest(ctx, "POST", "/api/v1/projections/"+url.PathEscape(name)+"/resume", nil, nil)
}

// CancelRebuild cancels an in-progress rebuild job.
func (pc *ProjectionClient) CancelRebuild(ctx context.Context, name string) error {
	return pc.client.restRequest(ctx, "POST", "/api/v1/projections/"+url.PathEscape(name)+"/cancel", nil, nil)
}

// ExecuteSQL runs a SQL query against projection tables.
func (pc *ProjectionClient) ExecuteSQL(ctx context.Context, query string) (*SQLQueryResult, error) {
	var result SQLQueryResult
	if err := pc.client.restRequest(ctx, "POST", "/api/v1/sql", map[string]string{"query": query}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
