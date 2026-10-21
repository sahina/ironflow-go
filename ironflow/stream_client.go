package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

func (c *Client) entityRPC() ironflowv1connect.EntityStreamServiceClient {
	return ironflowv1connect.NewEntityStreamServiceClient(c.httpClient, c.serverURL, connect.WithProtoJSON(), connect.WithInterceptors(c.interceptor()))
}
func streamTimestamp(value *timestamppb.Timestamp) string {
	if value == nil {
		return ""
	}
	return value.AsTime().Format(time.RFC3339Nano)
}
func streamPayload(object *structpb.Struct, value *structpb.Value) any {
	if value != nil {
		return value.AsInterface()
	}
	if object != nil {
		return object.AsMap()
	}
	return nil
}

// ListStreams returns all entity streams.
func (c *Client) ListStreams(ctx context.Context) ([]StreamListEntry, error) {
	// sdkcoverage: POST /ironflow.v1.EntityStreamService/ListStreams
	resp, err := c.entityRPC().ListStreams(ctx, connect.NewRequest(&ironflowv1.ListStreamsRequest{}))
	if err != nil {
		return nil, connectError(err)
	}
	result := make([]StreamListEntry, 0, len(resp.Msg.Streams))
	for _, s := range resp.Msg.Streams {
		result = append(result, StreamListEntry{EntityID: s.EntityId, EntityType: s.EntityType, Version: s.Version, EventCount: s.EventCount, LastEventAt: streamTimestamp(s.UpdatedAt), CreatedAt: streamTimestamp(s.CreatedAt)})
	}
	return result, nil
}

// entityHistoryPageSize is the events per request; the server default is 50,
// so a long history needs many fewer round trips.
const entityHistoryPageSize = 500

// GetEntityHistory returns the full event history for an entity, oldest
// first. It follows the server's page cursor until the history is complete,
// or until opts.Limit entries. With opts.To set, the result is the events
// that make up the entity's state at that time.
func (c *Client) GetEntityHistory(ctx context.Context, entityID string, opts ...EntityHistoryOpts) ([]EntityHistoryEntry, error) {
	req := &ironflowv1.GetEntityHistoryRequest{EntityId: entityID}
	maxEntries := math.MaxInt
	if len(opts) > 0 {
		req.FromVersion = opts[0].FromVersion
		if !opts[0].From.IsZero() {
			req.FromTimestamp = timestamppb.New(opts[0].From)
		}
		if !opts[0].To.IsZero() {
			req.ToTimestamp = timestamppb.New(opts[0].To)
		}
		if opts[0].Limit < 0 {
			return nil, fmt.Errorf("ironflow: GetEntityHistory limit must be nonnegative, got %d", opts[0].Limit)
		}
		if opts[0].Limit > 0 {
			maxEntries = opts[0].Limit
		}
	}
	result := []EntityHistoryEntry{}
	for len(result) < maxEntries {
		req.Limit = int32(min(entityHistoryPageSize, maxEntries-len(result)))
		// sdkcoverage: POST /ironflow.v1.EntityStreamService/GetEntityHistory
		resp, err := c.entityRPC().GetEntityHistory(ctx, connect.NewRequest(req))
		if err != nil {
			return nil, connectError(err)
		}
		for _, e := range resp.Msg.Entries {
			result = append(result, EntityHistoryEntry{EventName: e.EventName, Data: streamPayload(e.EventData, e.EventDataValue), EntityVersion: e.EntityVersion, Timestamp: streamTimestamp(e.Timestamp)})
		}
		if resp.Msg.NextCursor == 0 {
			break
		}
		if resp.Msg.NextCursor <= req.FromVersion {
			return nil, fmt.Errorf("ironflow: GetEntityHistory cursor did not advance (from %d to %d)", req.FromVersion, resp.Msg.NextCursor)
		}
		req.FromVersion = resp.Msg.NextCursor
	}
	return result, nil
}

// CreateSnapshot creates a snapshot for an entity stream.
func (c *Client) CreateSnapshot(ctx context.Context, entityID string, input CreateSnapshotInput) (*StreamSnapshot, error) {
	data, err := json.Marshal(input.State)
	if err != nil {
		return nil, err
	}
	value := &structpb.Value{}
	if err := protojson.Unmarshal(data, value); err != nil {
		return nil, err
	}
	req := &ironflowv1.CreateSnapshotRequest{EntityId: entityID, EntityType: input.EntityType, EntityVersion: input.EntityVersion}
	if object, ok := value.Kind.(*structpb.Value_StructValue); ok {
		req.State = object.StructValue
	} else {
		req.StateValue = value
	}
	// sdkcoverage: POST /ironflow.v1.EntityStreamService/CreateSnapshot
	resp, err := c.entityRPC().CreateSnapshot(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, connectError(err)
	}
	return &StreamSnapshot{SnapshotID: resp.Msg.SnapshotId, EntityID: entityID, EntityType: input.EntityType, EntityVersion: input.EntityVersion, State: input.State}, nil
}

// GetSnapshot returns the latest snapshot for an entity stream, or with
// opts.BeforeVersion the latest one at or before that version.
func (c *Client) GetSnapshot(ctx context.Context, entityID string, opts ...GetSnapshotOpts) (*StreamSnapshot, error) {
	req := &ironflowv1.GetSnapshotRequest{EntityId: entityID}
	if len(opts) > 0 {
		req.BeforeVersion = opts[0].BeforeVersion
	}
	// sdkcoverage: POST /ironflow.v1.EntityStreamService/GetSnapshot
	resp, err := c.entityRPC().GetSnapshot(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, connectError(err)
	}
	s := resp.Msg
	return &StreamSnapshot{SnapshotID: s.SnapshotId, EntityID: s.EntityId, EntityType: s.EntityType, EntityVersion: s.EntityVersion, State: streamPayload(s.State, s.StateValue), CreatedAt: streamTimestamp(s.CreatedAt)}, nil
}

// DeleteStream appends the $stream.deleted tombstone to an entity stream and
// drops its snapshots; later appends fail. purge also deletes every event
// below the tombstone. Returns the tombstone's version. Requires streams:delete.
func (c *Client) DeleteStream(ctx context.Context, entityID string, purge bool) (int64, error) {
	// sdkcoverage: POST /ironflow.v1.EntityStreamService/DeleteStream
	resp, err := c.entityRPC().DeleteStream(ctx, connect.NewRequest(&ironflowv1.DeleteStreamRequest{EntityId: entityID, Purge: purge}))
	if err != nil {
		return 0, connectError(err)
	}
	return resp.Msg.EntityVersion, nil
}
