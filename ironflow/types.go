// Package ironflow provides the Go SDK for Ironflow, an event-driven backend platform.
//
// Example usage:
//
//	import "github.com/sahina/ironflow-go/ironflow"
//
//	var ProcessOrder = ironflow.CreateFunction(ironflow.FunctionConfig{
//	    ID:       "process-order",
//	    Triggers: []ironflow.Trigger{{Event: "order.placed"}},
//	}, func(ctx ironflow.Context) (any, error) {
//	    result, err := ironflow.Run(ctx, "process", func() (any, error) {
//	        return processOrder(ctx.Event.Data)
//	    })
//	    return result, err
//	})
package ironflow

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ProtoInt64 handles int64 values that may be JSON-encoded as strings
// (protobuf JSON format encodes int64/uint64 as strings for JavaScript compatibility).
type ProtoInt64 int64

func (p *ProtoInt64) UnmarshalJSON(data []byte) error {
	// Try number first
	var n int64
	if err := json.Unmarshal(data, &n); err == nil {
		*p = ProtoInt64(n)
		return nil
	}
	// Try string
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("ProtoInt64: cannot unmarshal %s", string(data))
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("ProtoInt64: invalid number string %q: %w", s, err)
	}
	*p = ProtoInt64(n)
	return nil
}

func (p ProtoInt64) MarshalJSON() ([]byte, error) {
	return json.Marshal(int64(p))
}

func (p ProtoInt64) Int64() int64 {
	return int64(p)
}

// ProtoUint64 handles uint64 values that may be JSON-encoded as strings.
type ProtoUint64 uint64

func (p *ProtoUint64) UnmarshalJSON(data []byte) error {
	var n uint64
	if err := json.Unmarshal(data, &n); err == nil {
		*p = ProtoUint64(n)
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("ProtoUint64: cannot unmarshal %s", string(data))
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fmt.Errorf("ProtoUint64: invalid number string %q: %w", s, err)
	}
	*p = ProtoUint64(n)
	return nil
}

func (p ProtoUint64) MarshalJSON() ([]byte, error) {
	return json.Marshal(uint64(p))
}

func (p ProtoUint64) Uint64() uint64 {
	return uint64(p)
}

// FunctionConfig defines the configuration for a workflow function.
type FunctionConfig struct {
	// ID is the unique function identifier.
	ID string

	// Name is the display name for the function.
	Name string

	// Triggers are the event triggers that invoke this function.
	Triggers []Trigger

	// Retry is the retry configuration for failed steps.
	Retry *RetryConfig

	// Timeout is the function timeout.
	Timeout time.Duration

	// StepTimeout is the default timeout for all step.run() calls.
	// Individual steps can override this with WithTimeout().
	StepTimeout time.Duration

	// Concurrency is the concurrency control configuration.
	Concurrency *ConcurrencyConfig

	// Debounce collapses rapid-fire events into a single invocation
	// after a quiet period. Async-only — TriggerSync rejects debounced
	// functions with FailedPrecondition. Issue #545.
	Debounce *DebounceConfig

	// CancelOn declares cancel-on-event specs. When any spec matches an
	// incoming event whose match-path value equals the corresponding field
	// on the running run, the run is auto-cancelled. OR semantic across
	// specs. Issue #546 P3 / #572.
	CancelOn []CancelOnConfig

	// Mode is the execution mode: "push" for serverless, "pull" for workers.
	Mode ExecutionMode

	// ActorKey is the JSON path for actor-based sticky routing.
	ActorKey string

	// EndpointURL is the HTTP endpoint for push mode.
	EndpointURL string

	// Secrets is the list of secret names this function requires.
	// The engine resolves these secrets and passes their values at execution time.
	Secrets []string

	// Recording enables audit recording for this function.
	Recording bool

	// RecordingRetention is the retention period ("7d", "30d", "90d", "forever").
	RecordingRetention string

	// Metadata is custom metadata (e.g., service, team, owner).
	Metadata map[string]any
}

// Trigger defines an event trigger configuration.
type Trigger struct {
	// Event is the event name pattern to match (e.g., "order.placed").
	Event string

	// Expression is an optional CEL expression for filtering.
	Expression string

	// Cron is a cron schedule expression (e.g., "*/5 * * * *" for every 5 minutes).
	Cron string
}

// RetryConfig defines retry behavior for step failures.
type RetryConfig struct {
	// MaxAttempts is the maximum number of retry attempts (default: 3).
	MaxAttempts int

	// InitialDelay is the initial delay between retries (default: 1s).
	InitialDelay time.Duration

	// BackoffFactor is the backoff multiplier (default: 2.0).
	BackoffFactor float64

	// MaxDelay is the maximum delay between retries (default: 5m).
	MaxDelay time.Duration
}

// ConcurrencyConfig defines concurrency control.
type ConcurrencyConfig struct {
	// Limit is the maximum concurrent executions.
	Limit int

	// Key is the JSON path for grouping (e.g., "event.data.customerId").
	Key string
}

// DebounceConfig collapses rapid-fire events into a single invocation
// after a quiet period. The first event in a window arms a timer;
// subsequent events in the same window (same Key) reset it. When the
// quiet period elapses with no new events, the handler fires once with
// the most recent payload.
//
// Use cases: webhook storms, search-as-you-type, noisy IoT sensors.
//
// Debounce is async-only. Calling TriggerSync on a debounced function
// returns FailedPrecondition.
type DebounceConfig struct {
	// Period is the quiet period. Floor: 1 second. Sub-second values
	// are rejected at registration time because the scheduler tick
	// floor is 1s — entries cannot expire reliably between ticks.
	Period time.Duration

	// Key is the JSON path for per-key debouncing (e.g., "userId",
	// "data.customerId"). Same extraction rules as ConcurrencyConfig.Key.
	// Empty Key collapses all events for the function into a single
	// debounce lane (global key sentinel).
	Key string

	// MaxWait is the starvation cap: the handler fires at least once
	// every MaxWait even if quiet-period resets never stop arriving.
	// Zero means no cap (the default). When non-zero, must be >=
	// Period. Useful for search-as-you-type or continuous IoT streams
	// that may never go quiet. Issue #551.
	MaxWait time.Duration
}

// CancelOnConfig declares an event-match spec that auto-cancels a
// running workflow when a matching event arrives. Multiple specs OR
// together — any match fires cancel. Issue #546 P3 / #572.
type CancelOnConfig struct {
	// Event is the event name to match (e.g., "order.cancelled").
	Event string

	// Match is the JSON-path expression that must equal the running
	// run's corresponding field. Same extraction rules as
	// ConcurrencyConfig.Key. See internal/eventpath for path syntax.
	Match string
}

// ExecutionMode represents the function execution mode.
type ExecutionMode string

const (
	// PushMode executes functions via HTTP POST to serverless endpoints.
	PushMode ExecutionMode = "push"

	// PullMode executes functions via gRPC streaming workers.
	PullMode ExecutionMode = "pull"
)

// Function represents a defined Ironflow function.
type Function struct {
	// Config is the function configuration.
	Config FunctionConfig

	// Handler is the function handler.
	Handler FunctionHandler
}

// FunctionHandler is the type for function handlers.
type FunctionHandler func(ctx Context) (any, error)

// Context is passed to function handlers.
type Context struct {
	// Event is the triggering event.
	Event Event

	// Run contains information about the current run.
	Run RunInfo

	// Secrets provides read-only access to resolved secrets.
	Secrets SecretsReader

	// internal execution context
	exec *executionContext
}

// Event represents an Ironflow event.
type Event struct {
	// ID is the unique event ID.
	ID string

	// Name is the event name (e.g., "order.placed").
	Name string

	// Version is the event schema version (default: 1).
	Version int

	// RawData is the raw event payload.
	RawData json.RawMessage

	// Timestamp is when the event occurred.
	Timestamp time.Time

	// IdempotencyKey is an optional deduplication key.
	IdempotencyKey string

	// Source is the event origin (e.g., "webhook", "sdk", "api").
	Source EventSourceType

	// Metadata contains additional event metadata.
	Metadata map[string]any
}

// Data unmarshals the event data into the provided value.
func (e *Event) Data(v any) error {
	return json.Unmarshal(e.RawData, v)
}

// RunInfo contains information about the current run.
type RunInfo struct {
	// ID is the unique run ID.
	ID string

	// FunctionID is the function being executed.
	FunctionID string

	// Attempt is the current attempt number.
	Attempt int

	// StartedAt is when the run started.
	StartedAt time.Time
}

// EventFilter defines a filter for waitForEvent.
type EventFilter struct {
	// Event is the event name to wait for.
	Event string `json:"event"`

	// Match is the JSON path for matching (e.g., "data.orderId").
	Match string `json:"match,omitempty"`

	// Timeout is how long to wait (default: 7 days).
	// Excluded from default JSON marshaling; the YieldInfo serializes it as a duration string.
	Timeout time.Duration `json:"-"`

	// TimeoutStr is the string representation of Timeout for JSON serialization.
	TimeoutStr string `json:"timeout,omitempty"`
}

// RunStatus represents the status of a run.
type RunStatus string

const (
	// Deprecated: the engine no longer produces this status as of #1222 (run status "pending" retired). Retained for source compatibility.
	RunStatusPending            RunStatus = "pending"
	RunStatusRunning            RunStatus = "running"
	RunStatusCompleted          RunStatus = "completed"
	RunStatusFailed             RunStatus = "failed"
	RunStatusCancelled          RunStatus = "cancelled"
	RunStatusPaused             RunStatus = "paused"
	RunStatusWaitingForCapacity RunStatus = "waiting_for_capacity"
	RunStatusWaiting            RunStatus = "waiting"
)

// WorkflowRun represents a workflow execution instance.
type WorkflowRun struct {
	ID          string
	FunctionID  string
	EventID     string
	Status      RunStatus
	Attempt     int
	MaxAttempts int
	Input       any
	Output      any
	Error       *ErrorInfo
	StartedAt   *time.Time
	EndedAt     *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ErrorInfo contains error details.
type ErrorInfo struct {
	Message string
	Code    string
}

// StepResult represents the result of a step execution.
type StepResult struct {
	ID              string         `json:"id"`
	Name            string         `json:"name"`
	Type            string         `json:"type"`
	Status          string         `json:"status"`
	StartedAt       time.Time      `json:"-"`
	EndedAt         *time.Time     `json:"-"`
	Duration        time.Duration  `json:"-"`
	DurationMs      *int           `json:"duration_ms,omitempty"`
	Output          any            `json:"output,omitempty"`
	Error           *StepErrorInfo `json:"error,omitempty"`
	CompensationFor string         `json:"compensation_for,omitempty"`
}

// StepErrorInfo contains step error details.
type StepErrorInfo struct {
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	Stack     string `json:"stack,omitempty"`
}

// CompletedStep represents a completed step from previous execution.
type CompletedStep struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Output any    `json:"output,omitempty"`
	Error  any    `json:"error,omitempty"`
}

// PushRequest is the request from engine to SDK in push mode.
type PushRequest struct {
	RunID      string            `json:"run_id"`
	FunctionID string            `json:"function_id"`
	Attempt    int               `json:"attempt"`
	Event      PushEvent         `json:"event"`
	Steps      []CompletedStep   `json:"steps"`
	Resume     *ResumeContext    `json:"resume,omitempty"`
	Secrets    map[string]string `json:"secrets,omitempty"`
}

// PushEvent is the event in a push request.
type PushEvent struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Version        int             `json:"version"`
	Data           json.RawMessage `json:"data"`
	Timestamp      string          `json:"timestamp"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Source         string          `json:"source,omitempty"`
	Metadata       map[string]any  `json:"metadata,omitempty"`
}

// ResumeContext contains resume information for sleep/waitForEvent.
type ResumeContext struct {
	StepID string `json:"step_id"`
	Type   string `json:"type"` // "sleep", "wait_for_event", or "invoke_function"
	Data   any    `json:"data,omitempty"`
}

// PushResponse is the response from SDK to engine in push mode.
type PushResponse struct {
	Status string        `json:"status"` // "completed", "yielded", "failed"
	Steps  []*StepResult `json:"steps"`
	Result any           `json:"result,omitempty"`
	Error  *PushError    `json:"error,omitempty"`
	Yield  *YieldInfo    `json:"yield,omitempty"`
}

// PushError is an error in a push response.
type PushError struct {
	Message   string `json:"message"`
	Code      string `json:"code,omitempty"`
	StepID    string `json:"step_id,omitempty"`
	Retryable bool   `json:"retryable"`
	Stack     string `json:"stack,omitempty"`
}

// YieldInfo contains yield information for sleep/waitForEvent/invoke.
type YieldInfo struct {
	StepID          string       `json:"step_id"`
	Type            string       `json:"type"` // "sleep", "wait_for_event", "invoke_function", "invoke_function_async"
	Until           string       `json:"until,omitempty"`
	EventFilter     *EventFilter `json:"event_filter,omitempty"`
	FunctionID      string       `json:"function_id,omitempty"`       // for invoke_function
	Input           any          `json:"input,omitempty"`             // for invoke_function
	InvokeTimeoutMs int          `json:"invoke_timeout_ms,omitempty"` // for invoke_function
}

// InvokeOptions configures a step.invoke() call.
type InvokeOptions struct {
	// Timeout overrides the default 30s invoke timeout.
	Timeout time.Duration
}

// WithInvokeTimeout sets the timeout for an Invoke call.
func WithInvokeTimeout(d time.Duration) InvokeOptions {
	return InvokeOptions{Timeout: d}
}

// InvokeAsyncResult is returned by InvokeAsync with the child run ID.
type InvokeAsyncResult struct {
	RunID string `json:"run_id"`
}

// InvokeError wraps a failure from an invoked function.
type InvokeError struct {
	FunctionID string
	ChildRunID string
	Cause      string
}

func (e *InvokeError) Error() string {
	if e.ChildRunID != "" {
		return fmt.Sprintf("invoke '%s' failed (run %s): %s", e.FunctionID, e.ChildRunID, e.Cause)
	}
	return fmt.Sprintf("invoke '%s' failed: %s", e.FunctionID, e.Cause)
}

// PublishOption configures a Publish call.
type PublishOption func(*publishConfig)

type publishConfig struct {
	idempotencyKey string
}

// WithPublishIdempotencyKey sets an idempotency key for deduplication.
func WithPublishIdempotencyKey(key string) PublishOption {
	return func(c *publishConfig) {
		c.idempotencyKey = key
	}
}

// TopicInfo contains information about a developer pub/sub topic.
type TopicInfo struct {
	Name           string     `json:"name"`
	MessageCount   ProtoInt64 `json:"messageCount"`
	ConsumerCount  int        `json:"consumerCount"`
	FirstMessageAt *time.Time `json:"firstMessageAt,omitempty"`
	LastMessageAt  *time.Time `json:"lastMessageAt,omitempty"`
}

// TopicStats contains detailed statistics for a topic.
type TopicStats struct {
	Name          string      `json:"name"`
	MessageCount  ProtoInt64  `json:"messageCount"`
	ConsumerCount int         `json:"consumerCount"`
	Lag           ProtoInt64  `json:"lag"`
	FirstSeq      ProtoUint64 `json:"firstSeq"`
	LastSeq       ProtoUint64 `json:"lastSeq"`
}

// Resume type constants used in ResumeContext.Type
const (
	ResumeTypeSleep               = "sleep"
	ResumeTypeWaitEvent           = "wait_for_event"
	ResumeTypeInvokeFunction      = "invoke_function"
	ResumeTypeInvokeFunctionAsync = "invoke_function_async"
)

// NewContextForTest creates a Context for unit testing SDK functions.
// Not for production use.
func NewContextForTest(req *PushRequest) Context {
	return Context{exec: newExecutionContext(req)}
}

// ParallelOptions configures parallel step execution behavior.
type ParallelOptions struct {
	// Concurrency limits the number of concurrent branches (0 = unlimited).
	Concurrency int

	// OnError specifies error handling: "failFast" (default) or "allSettled".
	// - "failFast": First failure cancels pending branches and returns immediately
	// - "allSettled": All branches complete, errors are returned in results
	OnError string

	// SkipScopedClientCheck silences the unscoped-branch warning for a fan-out
	// that deliberately has nothing to memoize — a pure in-memory transform run
	// through Map only for its concurrency limit.
	//
	// It suppresses the diagnostic only; nothing about durability changes
	// either way. This is the Go spelling of the JS SDK's
	// `expectScopedClient: false` (#1671). The polarity is inverted because Go
	// struct fields zero-value to false: an `ExpectScopedClient bool` would
	// default every existing caller into the opt-out and the warning would
	// never fire.
	SkipScopedClientCheck bool
}

// BranchContext is a scoped execution context for a parallel branch.
// It provides isolated step ID generation while sharing memoization with the parent.
type BranchContext struct {
	// parent is the original execution context
	parent *executionContext

	// scopePrefix is the prefix for generating step IDs in this branch
	scopePrefix string

	// legacyScopePrefix is scopePrefix built WITHOUT the #1694 name escaping.
	// Carried so a run that started before escaping shipped still matches its
	// already-persisted branch step ids on resume — see preferLegacyStepID.
	legacyScopePrefix string

	// stepCounters tracks step invocation counts for this branch
	stepCounters map[string]int

	// stepCountersMu guards stepCounters.
	//
	// A branch context is NOT goroutine-confined in practice: the misuse this
	// warning exists to catch is a callback ignoring the scope it was handed,
	// and in a NESTED fan-out that means N inner goroutines all reaching for
	// the same ENCLOSING *BranchContext. Unguarded that is a concurrent map
	// write — a runtime throw that kills the worker before the diagnostic can
	// fire (#1792).
	stepCountersMu sync.Mutex

	// scopedClientUsed is true once this branch used the scope it was handed —
	// claimed a step ID through it, opened a nested parallel/map on it, or
	// registered a compensation through it.
	//
	// Deliberately narrower than "was durable": a callback that reaches for the
	// ENCLOSING function's context still records real steps, they just land
	// outside this branch's scope. #1792 flags both, so this tracks use of the
	// scope, not durability.
	//
	// atomic.Bool, not a plain bool: same nested-misuse path writes it from
	// several goroutines at once.
	scopedClientUsed atomic.Bool
}

// ============================================================================
// Entity Stream Types
// ============================================================================

// AppendEventInput contains parameters for appending an entity event.
type AppendEventInput struct {
	Name       string `json:"event_name"`
	Data       any    `json:"data"`
	EntityType string `json:"entity_type"`
}

// AppendOption configures an AppendStreamEvent call.
type AppendOption func(*appendConfig)

type appendConfig struct {
	expectedVersion int64
	idempotencyKey  string
	version         int
	metadata        map[string]any
}

// WithExpectedVersion sets the expected version for optimistic concurrency.
func WithExpectedVersion(v int64) AppendOption {
	return func(c *appendConfig) {
		c.expectedVersion = v
	}
}

// WithAppendIdempotencyKey sets the idempotency key for the append.
func WithAppendIdempotencyKey(key string) AppendOption {
	return func(c *appendConfig) {
		c.idempotencyKey = key
	}
}

// WithEventVersion sets the event schema version.
func WithEventVersion(v int) AppendOption {
	return func(c *appendConfig) {
		c.version = v
	}
}

// WithAppendMetadata attaches cross-cutting metadata (causation, correlation,
// tenant, trace) to the appended event. Metadata is persisted alongside the
// event and delivered to push-mode handlers, pull-mode workers, and
// projection reducers.
func WithAppendMetadata(metadata map[string]any) AppendOption {
	return func(c *appendConfig) {
		c.metadata = metadata
	}
}

// AppendResult contains the result of an append operation.
type AppendResult struct {
	EntityVersion ProtoInt64 `json:"entityVersion"`
	EventID       string     `json:"eventId"`
	// Sequence is the NATS JetStream sequence of this event on the
	// PUBSUB stream (the `events:` namespace projections consume).
	// Use with WaitForProjection's MinSeq for read-your-writes. 0
	// means publish failed or unavailable (e.g., NATS bridge disabled).
	// Issue #473.
	Sequence ProtoInt64 `json:"sequence,omitempty"`
}

// WaitForProjectionOpts configures a wait for a projection to catch up
// to a given NATS sequence. Issue #473.
type WaitForProjectionOpts struct {
	// MinSeq is the NATS sequence the projection must have processed
	// before Wait returns CaughtUp. Typically read from
	// AppendResult.Sequence after a write.
	MinSeq uint64 `json:"minSeq"`

	// Timeout bounds the wait. Server caps at 60s. 0 means "use default"
	// (30s).
	Timeout time.Duration `json:"timeout,omitempty"`

	// Partition optionally scopes the wait to a single partition of a
	// managed projection. Empty means global wait. External projections
	// must omit this.
	Partition string `json:"partition,omitempty"`
}

// WaitItem is one item in a batch wait. Shape matches WaitForProjectionOpts.
type WaitItem struct {
	Name      string `json:"name"`
	MinSeq    uint64 `json:"minSeq"`
	Partition string `json:"partition,omitempty"`
}

// WaitResult is the shape returned by WaitForProjection and WaitForEvent.
// Exactly one of CaughtUp or TimedOut is true on success; on error, the
// SDK returns a typical Go error instead.
type WaitResult struct {
	CaughtUp       bool   `json:"caughtUp"`
	TimedOut       bool   `json:"timedOut"`
	CurrentSeq     uint64 `json:"currentSeq"`
	TargetSeq      uint64 `json:"targetSeq"`
	BehindByEvents int64  `json:"behindByEvents"`
	// Rebuilding is reserved — always false in PR1. Callers should not
	// rely on it until the monotonic-guard PR lands.
	Rebuilding bool   `json:"rebuilding,omitempty"`
	Mode       string `json:"mode,omitempty"`
}

// WaitItemResult is one entry in a batch wait response. When the wait
// item failed (projection not found, etc.), Error is set and Result
// is zero-valued. Otherwise Error is empty and Result holds the outcome.
type WaitItemResult struct {
	Result WaitResult `json:"result"`
	Error  string     `json:"error,omitempty"`
}

// ReadStreamOpts contains options for reading an entity stream.
type ReadStreamOpts struct {
	FromVersion int64  `json:"from_version,omitempty"`
	Limit       int    `json:"limit,omitempty"`
	Direction   string `json:"direction,omitempty"`
}

// StreamEvent is an event in an entity stream.
type StreamEvent struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	Data          map[string]any `json:"data"`
	EntityVersion ProtoInt64     `json:"entityVersion"`
	Version       int            `json:"version"`
	Timestamp     string         `json:"timestamp"`
	Source        string         `json:"source,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

// StreamInfo contains computed metadata for an entity stream.
type StreamInfo struct {
	EntityID   string     `json:"entityId"`
	EntityType string     `json:"entityType"`
	Version    ProtoInt64 `json:"version"`
	EventCount ProtoInt64 `json:"eventCount"`
	CreatedAt  string     `json:"createdAt"`
	UpdatedAt  string     `json:"updatedAt"`
}

// EntitySubscribeOptions configures an entity stream subscription.
type EntitySubscribeOptions struct {
	// EntityType is required to construct the NATS subject pattern.
	EntityType string
	// Replay replays the last N events from the NATS stream before live events.
	Replay int
	// OnEvent is called for each entity stream event received.
	// The raw SubscriptionEvent data is unmarshaled into a StreamEvent.
	// If nil, use the returned Subscription's Events() channel directly.
	OnEvent func(StreamEvent)
	// OnError is called when a subscription error occurs.
	OnError func(error)
}

// StreamListEntry represents a single entity stream in a list result.
type StreamListEntry struct {
	EntityID    string `json:"entity_id"`
	EntityType  string `json:"entity_type"`
	Version     int64  `json:"version"`
	EventCount  int64  `json:"event_count"`
	LastEventAt string `json:"last_event_at"`
	CreatedAt   string `json:"created_at"`
}

// EntityHistoryEntry represents a single event in an entity's history.
type EntityHistoryEntry struct {
	EventName     string `json:"event_name"`
	Data          any    `json:"event_data"`
	EntityVersion int64  `json:"entity_version"`
	Timestamp     string `json:"timestamp"`
	Metadata      any    `json:"metadata,omitempty"`
}

// StreamSnapshot represents a snapshot of an entity stream's state.
type StreamSnapshot struct {
	SnapshotID    string `json:"snapshot_id"`
	EntityID      string `json:"entity_id"`
	EntityType    string `json:"entity_type"`
	EntityVersion int64  `json:"entity_version"`
	State         any    `json:"state"`
	CreatedAt     string `json:"created_at"`
}

// CreateSnapshotInput contains parameters for creating an entity stream snapshot.
type CreateSnapshotInput struct {
	EntityType    string `json:"entity_type"`
	EntityVersion int64  `json:"entity_version"`
	State         any    `json:"state"`
}

// ============================================================================
// Audit Types
// ============================================================================

// AuditEvent represents an event from the audit trail.
type AuditEvent struct {
	ID         string            `json:"id"`
	RunID      string            `json:"run_id"`
	FunctionID string            `json:"function_id"`
	StepID     string            `json:"step_id,omitempty"`
	EventType  string            `json:"event_type"`
	Payload    map[string]any    `json:"payload"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	CreatedAt  string            `json:"created_at"`
}

// GetAuditTrailOpts configures an audit trail query.
type GetAuditTrailOpts struct {
	EventType     string
	FromTimestamp string
	ToTimestamp   string
	Limit         int
	Cursor        string
}

// AuditTrailResult contains paginated audit trail results.
type AuditTrailResult struct {
	Events     []AuditEvent `json:"events"`
	TotalCount int          `json:"total_count"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// ============================================================================
// Paused State Types (Scoped Injection)
// ============================================================================

// PausedStepInfo contains information about a completed step in a paused run.
type PausedStepInfo struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Output      json.RawMessage `json:"output"`
	Injected    bool            `json:"injected"`
	CompletedAt string          `json:"completedAt"`
	// StepType is the step kind ("invoke", "sleep", "wait_for_event",
	// "compensate", "invoke_function") so you can tell what you are patching.
	StepType string `json:"stepType"`
	// Status is the step's terminal status at snapshot time: "completed" or
	// "failed". FAILED steps are exposed here so they can be repaired via
	// InjectStepOutput — without this you cannot tell the two apart.
	Status string `json:"status"`
	// Error is the error payload for a FAILED step, nil for a completed one.
	Error json.RawMessage `json:"error"`
}

// PausedState contains the state of a paused run.
type PausedState struct {
	Steps        []PausedStepInfo `json:"steps"`
	NextStepHint string           `json:"nextStepHint"`
	PauseReason  string           `json:"pauseReason"`
}

// ============================================================================
// Time-Travel Debugging Types
// ============================================================================

// TimeTravelStepSnapshot represents a step's state at a point in time.
type TimeTravelStepSnapshot struct {
	StepID         string `json:"stepId"`
	Name           string `json:"name"`
	Type           string `json:"type"`
	Sequence       int    `json:"sequence"`
	Status         string `json:"status"`
	Output         string `json:"output,omitempty"`
	Error          string `json:"error,omitempty"`
	OriginalOutput string `json:"originalOutput,omitempty"`
	StartedAt      string `json:"startedAt,omitempty"`
	CompletedAt    string `json:"completedAt,omitempty"`
	DurationMs     int    `json:"durationMs"`
	Injected       bool   `json:"injected"`
	Patched        bool   `json:"patched"`
}

// TimeTravelRunStateSnapshot represents the reconstructed state of a run at a point in time.
type TimeTravelRunStateSnapshot struct {
	RunID      string                   `json:"runId"`
	FunctionID string                   `json:"functionId"`
	Status     string                   `json:"status"`
	Input      string                   `json:"input,omitempty"`
	Steps      []TimeTravelStepSnapshot `json:"steps"`
	Timestamp  string                   `json:"timestamp"`
	CreatedAt  string                   `json:"createdAt,omitempty"`
}

// TimeTravelTimelineEvent represents an event in the run's timeline.
type TimeTravelTimelineEvent struct {
	ID          string `json:"id"`
	EventType   string `json:"eventType"`
	StepID      string `json:"stepId,omitempty"`
	StepName    string `json:"stepName,omitempty"`
	Summary     string `json:"summary"`
	Significant bool   `json:"significant"`
	Timestamp   string `json:"timestamp"`
}

// TimeTravelStepOutputSnapshot represents a step's output at a point in time.
type TimeTravelStepOutputSnapshot struct {
	StepID         string `json:"stepId"`
	Status         string `json:"status"`
	Output         string `json:"output,omitempty"`
	OriginalOutput string `json:"originalOutput,omitempty"`
	Patched        bool   `json:"patched"`
	Injected       bool   `json:"injected"`
}

// ============================================================================
// Secrets Types
// ============================================================================

// Secret represents a secret with its name, value, and timestamps.
type Secret struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// SecretListEntry represents a secret entry in a list result (value is omitted).
type SecretListEntry struct {
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// ============================================================================
// Project and Environment Types
// ============================================================================

// Project represents an Ironflow project grouping related environments.
type Project struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	OrgID       string `json:"org_id"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// Environment represents a deployment environment within a project.
type Environment struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	ProjectID string `json:"project_id"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// SQLQueryResult contains the result of a SQL query against projection tables.
type SQLQueryResult struct {
	Columns []string         `json:"columns"`
	Rows    []map[string]any `json:"rows"`
	Count   int64            `json:"count"`
}

// ============================================================================
// Projection Types
// ============================================================================

// ProjectionStateResult is the current materialized state of a projection
// after the SDK strips the server REST envelope. The lossy ProjectionState
// type was removed in v0.20.0 — see CHANGELOG and issue #610.
//
// Field provenance:
//   - Name, Version, Mode, Status, LastEventSeq, UpdatedAt, ErrorMessage:
//     registry-level (envelope) — registry is authoritative for projection
//     metadata; inner state-row values for the same fields are intentionally
//     ignored during peel.
//   - Partition, State, LastEventID, LastEventTime: state-row level (inner)
//
// LastEventTime is nil when no state row exists yet (projection registered,
// no events applied). State is empty (nil) in that case. Status and
// ErrorMessage come from the registry envelope so consumers see error /
// paused projections without a separate GetStatus call. ErrorMessage is
// empty when status is healthy.
type ProjectionStateResult struct {
	Name          string     `json:"name"`
	Partition     string     `json:"partition"`
	State         any        `json:"state"`
	LastEventID   string     `json:"lastEventId"`
	LastEventSeq  int64      `json:"lastEventSeq"`
	LastEventTime *time.Time `json:"lastEventTime,omitempty"`
	Version       int64      `json:"version"`
	Mode          string     `json:"mode"`
	Status        string     `json:"status,omitempty"`
	ErrorMessage  string     `json:"errorMessage,omitempty"`
	UpdatedAt     time.Time  `json:"updatedAt"`
}

// GetProjectionOption configures a ProjectionClient.Get call.
type GetProjectionOption func(*getProjectionOptions)

type getProjectionOptions struct {
	partition string
}

// WithPartition selects a specific partition key for a partitioned projection.
// When omitted, the server returns the __global__ partition.
func WithPartition(key string) GetProjectionOption {
	return func(o *getProjectionOptions) {
		o.partition = key
	}
}

// ProjectionStatusInfo represents the operational status of a projection as returned by the API.
type ProjectionStatusInfo struct {
	Name         string `json:"name"`
	Status       string `json:"status"`
	EventCount   int64  `json:"eventCount"`
	LastEventAt  string `json:"lastEventAt"`
	ErrorCount   int64  `json:"errorCount"`
	LastError    string `json:"lastError"`
	ConsumerName string `json:"consumerName"`
}

// RebuildJob represents a projection rebuild operation.
type RebuildJob struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	Progress  int    `json:"progress"`
	StartedAt string `json:"startedAt"`
}

// ============================================================================
// Event Schema Registry Types
// ============================================================================

// EventSchema represents a registered event schema with version information.
type EventSchema struct {
	Name      string         `json:"event_name"`
	Version   int            `json:"version"`
	Schema    map[string]any `json:"schema"`
	CreatedAt string         `json:"created_at"`
}

// UnmarshalJSON implements json.Unmarshaler to support both server wire formats:
// - "schema_json" (string) from the REST API
// - "schema" (object) for direct SDK-side use
func (e *EventSchema) UnmarshalJSON(data []byte) error {
	// Intermediate type to avoid infinite recursion.
	type rawEventSchema struct {
		Name       string         `json:"event_name"`
		Version    int            `json:"version"`
		Schema     map[string]any `json:"schema"`
		SchemaJSON string         `json:"schema_json"`
		CreatedAt  string         `json:"created_at"`
	}
	var raw rawEventSchema
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	e.Name = raw.Name
	e.Version = raw.Version
	e.CreatedAt = raw.CreatedAt
	// Prefer the object form; fall back to parsing the JSON string form.
	if raw.Schema != nil {
		e.Schema = raw.Schema
	} else if raw.SchemaJSON != "" {
		var m map[string]any
		if err := json.Unmarshal([]byte(raw.SchemaJSON), &m); err == nil {
			e.Schema = m
		}
	}
	return nil
}

// RegisterSchemaInput contains parameters for registering an event schema.
type RegisterSchemaInput struct {
	Name    string         `json:"event_name"`
	Version int            `json:"version"`
	Schema  map[string]any `json:"schema"`
}

// TestUpcastInput contains parameters for testing an upcast transformation.
type TestUpcastInput struct {
	EventName   string `json:"event_name"`
	FromVersion int    `json:"from_version"`
	ToVersion   int    `json:"to_version"`
	Data        any    `json:"data"`
}

// UpcastResult contains the result of a test upcast transformation.
type UpcastResult struct {
	Success bool   `json:"success"`
	Data    any    `json:"data"`
	Error   string `json:"error,omitempty"`
}

// ============================================================================
// Worker Callback Types
// ============================================================================

// ErrorContext provides metadata about an async job failure.
// It is passed to the WorkerConfig.OnError callback.
type ErrorContext struct {
	// FunctionID is the ID of the function that failed.
	FunctionID string

	// JobID is the ID of the specific job execution.
	JobID string

	// RunID is the ID of the run.
	RunID string

	// Attempt is the current retry attempt number.
	Attempt int

	// EventName is the event that triggered this job.
	EventName string
}
