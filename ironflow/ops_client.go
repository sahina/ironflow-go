package ironflow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"strconv"
	"time"
)

// CapacityListOptions filters a capacity view. Every field is optional.
// Leases and credits ignore Function.
type CapacityListOptions struct {
	Environment string
	Function    string
	// Limit is the row cap. 0 means 100; the server caps it at 1000.
	Limit int
}

// CapacityLane is one concurrency lane.
type CapacityLane struct {
	LaneID         string    `json:"lane_id"`
	EnvironmentID  string    `json:"environment_id"`
	FunctionID     string    `json:"function_id"`
	Scope          string    `json:"scope"`
	ScalarType     string    `json:"scalar_type"`
	Limit          int       `json:"limit"`
	ActiveReserved int       `json:"active_reserved"`
	ConfigVersion  int       `json:"config_version"`
	CreatedAt      time.Time `json:"created_at"`
	LastUsedAt     time.Time `json:"last_used_at"`
}

// CapacityQueueEntry is one execution waiting in the dispatch queue.
type CapacityQueueEntry struct {
	QueueSeq             int64      `json:"queue_seq"`
	RunID                string     `json:"run_id"`
	ExecutionSeq         int64      `json:"execution_seq"`
	ExecutionMode        string     `json:"execution_mode"`
	FunctionID           string     `json:"function_id"`
	FunctionVersion      *int       `json:"function_version"`
	LaneID               *string    `json:"lane_id"`
	TimeoutMs            *int       `json:"timeout_ms"`
	EnvironmentID        string     `json:"environment_id"`
	FirstEnqueuedAt      time.Time  `json:"first_enqueued_at"`
	EligibleAt           time.Time  `json:"eligible_at"`
	BlockingReason       string     `json:"blocking_reason"`
	ReservationOwner     *string    `json:"reservation_owner"`
	ReservationExpiresAt *time.Time `json:"reservation_expires_at"`
	CreatedAt            time.Time  `json:"created_at"`
}

// CapacityLease is one active concurrency lease. FunctionID is always empty
// on this view; match a lease to its run by RunID.
type CapacityLease struct {
	LeaseID        string    `json:"lease_id"`
	RunID          string    `json:"run_id"`
	ExecutionSeq   int64     `json:"execution_seq"`
	LaneID         *string   `json:"lane_id"`
	LeaseTokenHash string    `json:"lease_token_hash"`
	ActivatedAt    time.Time `json:"activated_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	EngineNodeID   *string   `json:"engine_node_id"`
	EnvironmentID  string    `json:"environment_id"`
	FunctionID     string    `json:"function_id"`
}

// CapacityBucket is one fairness-ledger bucket.
type CapacityBucket struct {
	EnvironmentID    string     `json:"environment_id"`
	FunctionID       string     `json:"function_id"`
	LaneID           *string    `json:"lane_id"`
	LastDispatchedAt *time.Time `json:"last_dispatched_at"`
	CreatedAt        time.Time  `json:"created_at"`
}

// CapacityWorkerSession is one connected streaming-worker session.
type CapacityWorkerSession struct {
	SessionID       string          `json:"session_id"`
	WorkerID        string          `json:"worker_id"`
	EngineNodeID    string          `json:"engine_node_id"`
	EnvironmentID   string          `json:"environment_id"`
	Labels          json.RawMessage `json:"labels,omitempty"`
	ProtocolCaps    json.RawMessage `json:"protocol_caps,omitempty"`
	Draining        bool            `json:"draining"`
	HeartbeatExpiry time.Time       `json:"heartbeat_expiry"`
	CreatedAt       time.Time       `json:"created_at"`
}

// CapacityWorkerCredit is one execution slot a worker session offers.
type CapacityWorkerCredit struct {
	CreditID            string    `json:"credit_id"`
	EnvironmentID       string    `json:"environment_id"`
	WorkerID            string    `json:"worker_id"`
	SessionID           string    `json:"session_id"`
	EngineNodeID        string    `json:"engine_node_id"`
	SlotOrdinal         int       `json:"slot_ordinal"`
	ExpiresAt           time.Time `json:"expires_at"`
	Claimed             bool      `json:"claimed"`
	ClaimedRunID        string    `json:"claimed_run_id"`
	ClaimedExecutionSeq int64     `json:"claimed_execution_seq"`
	CreatedAt           time.Time `json:"created_at"`
}

// CapacityStat summarizes one function's capacity.
type CapacityStat struct {
	EnvironmentID         string  `json:"environment_id"`
	FunctionID            string  `json:"function_id"`
	ActiveLeases          int     `json:"active_leases"`
	LaneLimit             int     `json:"lane_limit"`
	Queued                int     `json:"queued"`
	Reservations          int     `json:"reservations"`
	OldestQueueAgeSeconds float64 `json:"oldest_queue_age_seconds"`
}

// DebounceEntry is one armed debounce timer.
type DebounceEntry struct {
	EnvironmentID   string    `json:"environment_id"`
	FunctionID      string    `json:"function_id"`
	DebounceKey     string    `json:"debounce_key"`
	EventID         string    `json:"event_id"`
	FunctionVersion int       `json:"function_version"`
	PeriodMs        int       `json:"period_ms"`
	ArmedAt         time.Time `json:"armed_at"`
	FiresAt         time.Time `json:"fires_at"`
}

func capacityPath(route string, opts CapacityListOptions) string {
	q := url.Values{}
	if opts.Environment != "" {
		q.Set("env_id", opts.Environment)
	}
	if opts.Function != "" {
		q.Set("function", opts.Function)
	}
	if opts.Limit != 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	if len(q) == 0 {
		return route
	}
	return route + "?" + q.Encode()
}

// The capacity views need platform credentials (an ifplatform_ key or a
// platform session); a tenant key gets 403.

// ListCapacityLanes returns concurrency lanes.
func (c *Client) ListCapacityLanes(ctx context.Context, opts CapacityListOptions) ([]CapacityLane, error) {
	var out []CapacityLane
	// sdkcoverage: GET /api/v1/capacity/lanes
	err := c.request(ctx, "GET", capacityPath("/api/v1/capacity/lanes", opts), nil, &out)
	return out, err
}

// ListCapacityQueue returns executions waiting for capacity.
func (c *Client) ListCapacityQueue(ctx context.Context, opts CapacityListOptions) ([]CapacityQueueEntry, error) {
	var out []CapacityQueueEntry
	// sdkcoverage: GET /api/v1/capacity/queue
	err := c.request(ctx, "GET", capacityPath("/api/v1/capacity/queue", opts), nil, &out)
	return out, err
}

// ListCapacityLeases returns active concurrency leases.
func (c *Client) ListCapacityLeases(ctx context.Context, opts CapacityListOptions) ([]CapacityLease, error) {
	var out []CapacityLease
	// sdkcoverage: GET /api/v1/capacity/leases
	err := c.request(ctx, "GET", capacityPath("/api/v1/capacity/leases", opts), nil, &out)
	return out, err
}

// ListCapacityBuckets returns fairness-ledger buckets.
func (c *Client) ListCapacityBuckets(ctx context.Context, opts CapacityListOptions) ([]CapacityBucket, error) {
	var out []CapacityBucket
	// sdkcoverage: GET /api/v1/capacity/buckets
	err := c.request(ctx, "GET", capacityPath("/api/v1/capacity/buckets", opts), nil, &out)
	return out, err
}

// ListCapacitySessions returns connected streaming-worker sessions.
func (c *Client) ListCapacitySessions(ctx context.Context, opts CapacityListOptions) ([]CapacityWorkerSession, error) {
	var out []CapacityWorkerSession
	// sdkcoverage: GET /api/v1/capacity/sessions
	err := c.request(ctx, "GET", capacityPath("/api/v1/capacity/sessions", opts), nil, &out)
	return out, err
}

// ListCapacityCredits returns worker execution slots.
func (c *Client) ListCapacityCredits(ctx context.Context, opts CapacityListOptions) ([]CapacityWorkerCredit, error) {
	var out []CapacityWorkerCredit
	// sdkcoverage: GET /api/v1/capacity/credits
	err := c.request(ctx, "GET", capacityPath("/api/v1/capacity/credits", opts), nil, &out)
	return out, err
}

// CapacityStats returns per-function capacity totals across all environments.
func (c *Client) CapacityStats(ctx context.Context) ([]CapacityStat, error) {
	var out []CapacityStat
	err := c.request(ctx, "GET", "/api/v1/capacity/stats", nil, &out)
	return out, err
}

// ListDebounceEntries returns the armed debounce timers in this client's environment.
func (c *Client) ListDebounceEntries(ctx context.Context) ([]DebounceEntry, error) {
	var out []DebounceEntry
	err := c.request(ctx, "GET", "/api/v1/debounce/entries", nil, &out)
	return out, err
}

// CancelDebounce disarms one debounce timer, so the pending invocation never
// fires. Pass the raw debounce key; it is base64url-encoded for the path.
// Cancelling an entry that is not armed succeeds.
func (c *Client) CancelDebounce(ctx context.Context, environmentID, functionID, key string) error {
	// sdkcoverage: DELETE /api/v1/debounce/entries/{envID}/{fnID}/{key}
	return c.request(ctx, "DELETE", "/api/v1/debounce/entries/"+url.PathEscape(environmentID)+"/"+url.PathEscape(functionID)+"/"+base64.RawURLEncoding.EncodeToString([]byte(key)), nil, nil)
}
