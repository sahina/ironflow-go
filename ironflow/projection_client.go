package ironflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"connectrpc.com/connect"

	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
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
// Returns registry metadata and the selected partition state through Connect.
//
// Pass WithPartition("key") to read a specific partition. When omitted the
// server returns the __global__ partition.
func (pc *ProjectionClient) Get(ctx context.Context, name string, opts ...GetProjectionOption) (*ProjectionStateResult, error) {
	options := getProjectionOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	// sdkcoverage: POST /ironflow.v1.ProjectionService/GetProjection
	resp, err := pc.rpc().GetProjection(ctx, connect.NewRequest(&ironflowv1.GetProjectionRequest{Name: name, Partition: options.partition}))
	if err != nil {
		return nil, connectError(err)
	}
	msg := resp.Msg
	result := &ProjectionStateResult{Name: msg.Name, Partition: msg.Partition, Mode: msg.Mode, Version: msg.Version, LastEventID: msg.LastEventId, State: map[string]any{}}
	if msg.StateValue != nil {
		result.State = msg.StateValue.AsInterface()
	} else if msg.State != nil {
		result.State = msg.State.AsMap()
	}
	if result.State == nil {
		result.State = map[string]any{}
	}
	if msg.LastEventTime != nil && !msg.LastEventTime.AsTime().IsZero() {
		t := msg.LastEventTime.AsTime()
		result.LastEventTime = &t
	}
	if reg := msg.Registry; reg != nil {
		result.Version = reg.VersionFull
		result.LastEventSeq = reg.LastEventSeq
		result.Status = reg.Status
		result.ErrorMessage = reg.ErrorMessage
		if reg.UpdatedAt != nil {
			result.UpdatedAt = reg.UpdatedAt.AsTime()
		}
	}
	return result, nil
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
	// sdkcoverage: POST /ironflow.v1.ProjectionService/ListProjections
	resp, err := pc.rpc().ListProjections(ctx, connect.NewRequest(&ironflowv1.ListProjectionsRequest{}))
	if err != nil {
		return nil, connectError(err)
	}
	result := make([]ProjectionStatusInfo, 0, len(resp.Msg.Projections))
	for _, p := range resp.Msg.Projections {
		// ProjectionInfo carries no lag — only GetProjectionStatus reads the
		// consumer. Lag stays zero here, which is why Status is what a caller
		// listing projections should read.
		info := ProjectionStatusInfo{
			Name:               p.Name,
			Status:             p.Status,
			Mode:               p.Mode,
			LastEventSeq:       p.LastEventSeq,
			LastError:          p.ErrorMessage,
			UpdatedAt:          rfc3339OrEmpty(p.UpdatedAt),
			RebuildTargetSeq:   p.GetRebuildTargetSeq(),
			RebuildStartCursor: p.GetRebuildStartCursor(),
			RebuildStartedAt:   rfc3339OrEmpty(p.RebuildStartedAt),
		}
		result = append(result, info)
	}
	return result, nil
}

// GetStatus retrieves the operational status of a projection by name.
func (pc *ProjectionClient) GetStatus(ctx context.Context, name string) (*ProjectionStatusInfo, error) {
	// sdkcoverage: POST /ironflow.v1.ProjectionService/GetProjectionStatus
	resp, err := pc.rpc().GetProjectionStatus(ctx, connect.NewRequest(&ironflowv1.GetProjectionStatusRequest{Name: name}))
	if err != nil {
		return nil, connectError(err)
	}
	return &ProjectionStatusInfo{
		Name:               resp.Msg.Name,
		Status:             resp.Msg.Status,
		Mode:               resp.Msg.Mode,
		LastEventSeq:       resp.Msg.LastEventSeq,
		Lag:                resp.Msg.Lag,
		LastError:          resp.Msg.ErrorMessage,
		UpdatedAt:          rfc3339OrEmpty(resp.Msg.UpdatedAt),
		RebuildTargetSeq:   resp.Msg.RebuildTargetSeq,
		RebuildStartCursor: resp.Msg.RebuildStartCursor,
		RebuildStartedAt:   rfc3339OrEmpty(resp.Msg.RebuildStartedAt),
	}, nil
}

// Rebuild triggers a full rebuild of a projection and returns the rebuild job.
func (pc *ProjectionClient) Rebuild(ctx context.Context, name string) (*RebuildJob, error) {
	// sdkcoverage: POST /ironflow.v1.ProjectionService/RebuildProjection
	resp, err := pc.rpc().RebuildProjection(ctx, connect.NewRequest(&ironflowv1.RebuildProjectionRequest{Name: name}))
	if err != nil {
		return nil, connectError(err)
	}
	return sdkRebuildJob(resp.Msg.Job), nil
}

// GetRebuildJob retrieves the current state of an in-progress or completed rebuild job.
func (pc *ProjectionClient) GetRebuildJob(ctx context.Context, name string) (*RebuildJob, error) {
	// sdkcoverage: POST /ironflow.v1.ProjectionService/GetRebuildJob
	resp, err := pc.rpc().GetRebuildJob(ctx, connect.NewRequest(&ironflowv1.GetRebuildJobRequest{Name: name}))
	if err != nil {
		return nil, connectError(err)
	}
	return sdkRebuildJob(resp.Msg.Job), nil
}

// Delete removes a projection by name.
func (pc *ProjectionClient) Delete(ctx context.Context, name string) error {
	return pc.client.restRequest(ctx, "DELETE", "/api/v1/projections/"+url.PathEscape(name), nil, nil)
}

// Pause pauses a running projection.
func (pc *ProjectionClient) Pause(ctx context.Context, name string) error {
	// sdkcoverage: POST /ironflow.v1.ProjectionService/PauseProjection
	_, err := pc.rpc().PauseProjection(ctx, connect.NewRequest(&ironflowv1.PauseProjectionRequest{Name: name}))
	return connectError(err)
}

// Resume resumes a paused projection.
func (pc *ProjectionClient) Resume(ctx context.Context, name string) error {
	// sdkcoverage: POST /ironflow.v1.ProjectionService/ResumeProjection
	_, err := pc.rpc().ResumeProjection(ctx, connect.NewRequest(&ironflowv1.ResumeProjectionRequest{Name: name}))
	return connectError(err)
}

// CancelRebuild cancels an in-progress rebuild job.
func (pc *ProjectionClient) CancelRebuild(ctx context.Context, name string) error {
	// sdkcoverage: POST /ironflow.v1.ProjectionService/CancelRebuild
	_, err := pc.rpc().CancelRebuild(ctx, connect.NewRequest(&ironflowv1.CancelRebuildRequest{Name: name}))
	return connectError(err)
}

// ExecuteSQL runs a SQL query against projection tables.
//
// Speaks ConnectRPC. The REST route POST /api/v1/sql was removed in #1972 step
// 10; QueryService/ExecuteSQL is the only transport for this capability.
//
// Errors are *IronflowError, as on every other method here — connectError maps
// the Connect code to the same Code, Retryable flag and sentinel Cause the REST
// path produced. Returning the raw *connect.Error would have made IsRetryable
// answer true for a rejected query, because it defaults to true for a type it
// does not recognize.
//
// Cell values are strings. That is not a change in what the server can return
// — store.QuerySQL produces [][]string on both SQLite and PostgreSQL, so the
// REST response carried strings too. What did change is that the values now
// arrive: the REST response listed them positionally, this struct declares
// Rows as []map[string]any, and json.Unmarshal rejected the mismatch, so every
// non-empty result set failed to decode. Count comes from total_rows, which the
// old `json:"count"` tag never matched either.
func (pc *ProjectionClient) ExecuteSQL(ctx context.Context, query string) (*SQLQueryResult, error) {
	queryClient := ironflowv1connect.NewQueryServiceClient(
		pc.client.httpClient,
		pc.client.serverURL,
		connect.WithProtoJSON(),
		connect.WithInterceptors(bearerInterceptor(pc.client.apiKey)),
	)

	req := connect.NewRequest(&ironflowv1.ExecuteSQLRequest{Query: query})

	// sdkcoverage: POST /ironflow.v1.QueryService/ExecuteSQL
	resp, err := queryClient.ExecuteSQL(ctx, req)
	if err != nil {
		return nil, connectError(err)
	}

	msg := resp.Msg
	rows := make([]map[string]any, 0, len(msg.GetRows()))
	for _, row := range msg.GetRows() {
		values := row.GetValues()
		cells := make(map[string]any, len(values))
		for i, col := range msg.GetColumns() {
			if i >= len(values) {
				break
			}
			cells[col] = values[i]
		}
		rows = append(rows, cells)
	}

	return &SQLQueryResult{
		Columns: msg.GetColumns(),
		Rows:    rows,
		Count:   int64(msg.GetTotalRows()),
	}, nil
}

func (pc *ProjectionClient) rpc() ironflowv1connect.ProjectionServiceClient {
	return ironflowv1connect.NewProjectionServiceClient(pc.client.httpClient, pc.client.serverURL, connect.WithProtoJSON(), connect.WithInterceptors(bearerInterceptor(pc.client.apiKey)))
}

// rfc3339OrEmpty renders a protobuf timestamp the way the rest of this package
// renders times, and yields "" for an absent one rather than the zero instant.
func rfc3339OrEmpty(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().Format(time.RFC3339Nano)
}

func sdkRebuildJob(job *ironflowv1.RebuildJob) *RebuildJob {
	if job == nil {
		return nil
	}
	result := &RebuildJob{Name: job.ProjectionName, Status: job.Status, Progress: int(job.Progress)}
	if job.StartedAt != nil {
		result.StartedAt = job.StartedAt.AsTime().Format(time.RFC3339Nano)
	}
	return result
}
