package ironflow

// ProjectionMode represents the execution mode of a projection.
type ProjectionMode string

const (
	// ProjectionModeManaged maintains derived state via a reducer-style handler.
	// The handler receives the current state and an event, returning the new state.
	// Requires InitialState to be set.
	ProjectionModeManaged ProjectionMode = "managed"

	// ProjectionModeExternal triggers side effects without maintaining state.
	// The handler receives events and performs external operations (e.g., send email, call API).
	ProjectionModeExternal ProjectionMode = "external"
)

// ProjectionStatus represents the runtime status of a projection.
type ProjectionStatus string

const (
	// ProjectionStatusActive means the projection is running normally.
	ProjectionStatusActive ProjectionStatus = "active"

	// ProjectionStatusRebuilding means the projection is rebuilding its state.
	ProjectionStatusRebuilding ProjectionStatus = "rebuilding"

	// ProjectionStatusPaused means the projection is paused.
	ProjectionStatusPaused ProjectionStatus = "paused"

	// ProjectionStatusError means the projection has encountered an error.
	ProjectionStatusError ProjectionStatus = "error"
)

// ProjectionEvent represents an event delivered to a projection handler.
type ProjectionEvent struct {
	// ID is the unique event ID.
	ID string

	// Name is the event name (e.g., "order.created").
	Name string

	// Data is the event payload.
	Data map[string]any

	// Seq is the global sequence number of the event.
	Seq int64

	// Timestamp is the ISO8601 timestamp of when the event was produced.
	Timestamp string

	// Source is the origin of the event (e.g., "api", "webhook").
	Source string

	// Metadata contains additional event metadata.
	Metadata map[string]any
}

// ProjectionContext provides context to projection handlers.
type ProjectionContext struct {
	// Event contains metadata about the current event being processed.
	Event struct {
		ID        string
		Name      string
		Seq       int64
		Timestamp string
	}

	// Projection contains metadata about the projection itself.
	Projection struct {
		Name    string
		Version int
	}
}

// ProjectionHandler is the handler function for projections.
//
// For managed mode: receives the current state, event, and context; returns the new state.
// For external mode: state may be nil; perform side effects and return nil state.
//
// REQUIRED (managed mode): the handler MUST be deterministic and idempotent.
// Same (state, event) MUST produce the same newState, every invocation.
// Non-deterministic reducers (time.Now, rand, os.Getenv, uuid.New) produce
// divergent state under at-least-once delivery and concurrent rebuild/live
// application — PG-backed rebuild (#486) and live NATS tail can both call the
// reducer for the same event. The same event may also arrive multiple times
// across retries and node failover, so handler output MUST be invariant under
// replay (prefer keyed-map accumulation over counter += N).
//
// Derive timestamps from event.Timestamp, IDs from event.Data["..."]. The
// projection runner deep-copies state via JSON before each invocation so
// accidental in-place mutation cannot leak across iterations, but managed
// handlers MUST NOT perform side effects (network, DB writes, logging-as-
// intent). External I/O requires mode "external".
//
// See docs/explanation/projections.md#reducer-contract-managed-mode.
type ProjectionHandler func(state map[string]any, event ProjectionEvent, ctx ProjectionContext) (map[string]any, error)

// ProjectionConfig defines the configuration for a projection.
type ProjectionConfig struct {
	// Name is the unique projection name.
	Name string

	// Events are the event names to subscribe to. Supports wildcards (e.g., "order.*").
	Events []string

	// Mode is the execution mode. Auto-detected from InitialState if empty:
	// - If InitialState is set, defaults to "managed"
	// - If InitialState is nil, defaults to "external"
	Mode ProjectionMode

	// Handler is the projection handler function.
	Handler ProjectionHandler

	// InitialState returns the initial state for managed projections.
	// Required for managed mode, must be nil for external mode.
	InitialState func() map[string]any

	// PartitionKey is a JSONPath for partition key extraction (e.g., "$.data.customerId").
	// When set, state is maintained per-partition.
	PartitionKey string

	// MaxRetries is the maximum number of retries per event (default: 3).
	MaxRetries int

	// BatchSize is the number of events to poll at once (default: 100).
	BatchSize int
}

// Projection represents a defined projection instance.
type Projection struct {
	// Config is the projection configuration.
	Config ProjectionConfig
}

// CreateProjection creates a new projection with the given configuration.
//
// Mode is auto-detected from the presence of InitialState:
//   - If InitialState is provided, mode defaults to ProjectionModeManaged
//   - If InitialState is nil, mode defaults to ProjectionModeExternal
//
// Example (managed projection):
//
//	var OrderTotals = ironflow.CreateProjection(ironflow.ProjectionConfig{
//	    Name:   "order-totals",
//	    Events: []string{"order.created", "order.updated"},
//	    InitialState: func() map[string]any {
//	        return map[string]any{"total": 0, "count": 0}
//	    },
//	    Handler: func(state map[string]any, event ironflow.ProjectionEvent, ctx ironflow.ProjectionContext) (map[string]any, error) {
//	        amount, _ := event.Data["amount"].(float64)
//	        count, _ := state["count"].(int)
//	        total, _ := state["total"].(float64)
//	        return map[string]any{"total": total + amount, "count": count + 1}, nil
//	    },
//	})
//
// Example (external projection):
//
//	var EmailNotifier = ironflow.CreateProjection(ironflow.ProjectionConfig{
//	    Name:   "email-notifier",
//	    Events: []string{"order.completed"},
//	    Handler: func(state map[string]any, event ironflow.ProjectionEvent, ctx ironflow.ProjectionContext) (map[string]any, error) {
//	        sendEmail(event.Data["email"].(string), "Order complete!")
//	        return nil, nil
//	    },
//	})
func CreateProjection(config ProjectionConfig) Projection {
	// Validate required fields
	if config.Name == "" {
		panic("invalid projection config: projection name is required")
	}
	if len(config.Events) == 0 {
		panic("invalid projection config: projection must subscribe to at least one event")
	}

	// Auto-detect mode from InitialState
	if config.Mode == "" {
		if config.InitialState != nil {
			config.Mode = ProjectionModeManaged
		} else {
			config.Mode = ProjectionModeExternal
		}
	}

	// Apply defaults
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.BatchSize == 0 {
		config.BatchSize = 100
	}

	return Projection{Config: config}
}

// GetProjectionMetadata returns the projection metadata for registration with the server.
func GetProjectionMetadata(p Projection) map[string]any {
	metadata := map[string]any{
		"name":        p.Config.Name,
		"events":      p.Config.Events,
		"mode":        string(p.Config.Mode),
		"max_retries": p.Config.MaxRetries,
		"batch_size":  p.Config.BatchSize,
	}

	if p.Config.PartitionKey != "" {
		metadata["partition_key"] = p.Config.PartitionKey
	}

	return metadata
}
