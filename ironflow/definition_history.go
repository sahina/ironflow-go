package ironflow

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
)

// DefinitionHistoryOptions pages a definition history, newest first.
type DefinitionHistoryOptions struct {
	// Limit is the page size. 0 means 50; the server caps it at 200.
	Limit int32
	// BeforeRevision is an exclusive cursor. 0 starts at the newest revision;
	// pass the previous page's NextBeforeRevision to continue.
	BeforeRevision int64
}

// ProjectionDefinition is a projection definition as it was before a change.
type ProjectionDefinition struct {
	Name          string
	EnvironmentID string
	Version       int64
	Events        []string
	PartitionKey  string
	Mode          string
	Type          string
	SQLDefinition string
	EventHandlers map[string]string
	Description   string
}

// ProjectionHistoryEntry archives the definition a change replaced or deleted.
type ProjectionHistoryEntry struct {
	EventID            string
	Revision           int64
	PreviousDefinition ProjectionDefinition
	ActorID            string
	ChangeReason       string
	ChangeType         string
	RecordedAt         time.Time
}

// ProjectionHistoryPage is one page of projection history.
type ProjectionHistoryPage struct {
	Entries []ProjectionHistoryEntry
	HasMore bool
	// NextBeforeRevision is 0 when the history is exhausted.
	NextBeforeRevision int64
}

// SchemaDefinition is an event schema version as it was before a change.
type SchemaDefinition struct {
	EventName     string
	EnvironmentID string
	Version       int32
	// SchemaJSON is the archived JSON Schema document, verbatim.
	SchemaJSON  string
	Description string
}

// SchemaHistoryEntry archives the schema document a change replaced or deleted.
type SchemaHistoryEntry struct {
	EventID            string
	Revision           int64
	PreviousDefinition SchemaDefinition
	ActorID            string
	ChangeReason       string
	ChangeType         string
	RecordedAt         time.Time
}

// SchemaHistoryPage is one page of event schema history.
type SchemaHistoryPage struct {
	Entries []SchemaHistoryEntry
	HasMore bool
	// NextBeforeRevision is 0 when the history is exhausted.
	NextBeforeRevision int64
}

// ListHistory returns the archived definitions of a projection, newest first.
// The current definition is not in its history.
func (pc *ProjectionClient) ListHistory(ctx context.Context, name string, opts DefinitionHistoryOptions) (*ProjectionHistoryPage, error) {
	// sdkcoverage: POST /ironflow.v1.ProjectionService/ListProjectionHistory
	resp, err := pc.rpc().ListProjectionHistory(ctx, connect.NewRequest(&ironflowv1.ListProjectionHistoryRequest{Name: name, Limit: opts.Limit, BeforeRevision: opts.BeforeRevision}))
	if err != nil {
		return nil, connectError(err)
	}
	page := &ProjectionHistoryPage{HasMore: resp.Msg.GetHasMore(), NextBeforeRevision: resp.Msg.GetNextBeforeRevision(), Entries: make([]ProjectionHistoryEntry, 0, len(resp.Msg.GetEntries()))}
	for _, e := range resp.Msg.GetEntries() {
		page.Entries = append(page.Entries, projectionHistoryEntryFromProto(e))
	}
	return page, nil
}

// GetRevision returns one archived projection definition by its history revision.
func (pc *ProjectionClient) GetRevision(ctx context.Context, name string, revision int64) (*ProjectionHistoryEntry, error) {
	// sdkcoverage: POST /ironflow.v1.ProjectionService/GetProjectionRevision
	resp, err := pc.rpc().GetProjectionRevision(ctx, connect.NewRequest(&ironflowv1.GetProjectionRevisionRequest{Name: name, Revision: revision}))
	if err != nil {
		return nil, connectError(err)
	}
	entry := projectionHistoryEntryFromProto(resp.Msg.GetEntry())
	return &entry, nil
}

// ListHistory returns the archived documents of one schema version, newest first.
func (sc *SchemaClient) ListHistory(ctx context.Context, eventName string, version int32, opts DefinitionHistoryOptions) (*SchemaHistoryPage, error) {
	// sdkcoverage: POST /ironflow.v1.EventSchemaService/ListSchemaHistory
	resp, err := sc.rpc().ListSchemaHistory(ctx, connect.NewRequest(&ironflowv1.ListSchemaHistoryRequest{EventName: eventName, Version: version, Limit: opts.Limit, BeforeRevision: opts.BeforeRevision}))
	if err != nil {
		return nil, connectError(err)
	}
	page := &SchemaHistoryPage{HasMore: resp.Msg.GetHasMore(), NextBeforeRevision: resp.Msg.GetNextBeforeRevision(), Entries: make([]SchemaHistoryEntry, 0, len(resp.Msg.GetEntries()))}
	for _, e := range resp.Msg.GetEntries() {
		page.Entries = append(page.Entries, schemaHistoryEntryFromProto(e))
	}
	return page, nil
}

// GetRevision returns one archived schema document by its history revision.
func (sc *SchemaClient) GetRevision(ctx context.Context, eventName string, version int32, revision int64) (*SchemaHistoryEntry, error) {
	// sdkcoverage: POST /ironflow.v1.EventSchemaService/GetSchemaRevision
	resp, err := sc.rpc().GetSchemaRevision(ctx, connect.NewRequest(&ironflowv1.GetSchemaRevisionRequest{EventName: eventName, Version: version, Revision: revision}))
	if err != nil {
		return nil, connectError(err)
	}
	entry := schemaHistoryEntryFromProto(resp.Msg.GetEntry())
	return &entry, nil
}

func timeOrZero(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func projectionHistoryEntryFromProto(e *ironflowv1.ProjectionHistoryEntry) ProjectionHistoryEntry {
	d := e.GetPreviousDefinition()
	return ProjectionHistoryEntry{
		EventID:  e.GetEventId(),
		Revision: e.GetRevision(),
		PreviousDefinition: ProjectionDefinition{
			Name: d.GetName(), EnvironmentID: d.GetEnvironmentId(), Version: d.GetVersion(), Events: d.GetEvents(),
			PartitionKey: d.GetPartitionKey(), Mode: d.GetMode(), Type: d.GetType(), SQLDefinition: d.GetSqlDefinition(),
			EventHandlers: d.GetEventHandlers(), Description: d.GetDescription(),
		},
		ActorID:      e.GetActorId(),
		ChangeReason: e.GetChangeReason(),
		ChangeType:   e.GetChangeType(),
		RecordedAt:   timeOrZero(e.GetRecordedAt()),
	}
}

func schemaHistoryEntryFromProto(e *ironflowv1.SchemaHistoryEntry) SchemaHistoryEntry {
	d := e.GetPreviousDefinition()
	return SchemaHistoryEntry{
		EventID:  e.GetEventId(),
		Revision: e.GetRevision(),
		PreviousDefinition: SchemaDefinition{
			EventName: d.GetEventName(), EnvironmentID: d.GetEnvironmentId(), Version: d.GetVersion(),
			SchemaJSON: d.GetSchemaJson(), Description: d.GetDescription(),
		},
		ActorID:      e.GetActorId(),
		ChangeReason: e.GetChangeReason(),
		ChangeType:   e.GetChangeType(),
		RecordedAt:   timeOrZero(e.GetRecordedAt()),
	}
}
