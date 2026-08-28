package ironflow

import (
	"context"
	"strings"
	"time"
)

// FunctionStatus is the lifecycle status of a registered function.
type FunctionStatus string

const (
	FunctionStatusUnspecified FunctionStatus = "unspecified"
	FunctionStatusActive      FunctionStatus = "active"
	FunctionStatusPaused      FunctionStatus = "paused"
	FunctionStatusArchived    FunctionStatus = "archived"
)

// RegisteredFunctionRetryConfig describes retry timing in milliseconds.
type RegisteredFunctionRetryConfig struct {
	MaxAttempts    int     `json:"maxAttempts"`
	InitialDelayMs int     `json:"initialDelayMs"`
	BackoffFactor  float64 `json:"backoffFactor"`
	MaxDelayMs     int     `json:"maxDelayMs"`
}

// RegisteredFunctionConcurrencyConfig describes a function's concurrency limit.
type RegisteredFunctionConcurrencyConfig struct {
	Limit int    `json:"limit"`
	Key   string `json:"key"`
}

// RegisteredFunctionDebounceConfig describes a function's debounce window.
type RegisteredFunctionDebounceConfig struct {
	PeriodMs  int    `json:"periodMs"`
	Key       string `json:"key"`
	MaxWaitMs int    `json:"maxWaitMs"`
}

// RegisteredFunction is a function definition returned by the management API.
type RegisteredFunction struct {
	ID                 string                               `json:"id"`
	Name               string                               `json:"name"`
	Description        string                               `json:"description"`
	Triggers           []Trigger                            `json:"triggers"`
	Retry              *RegisteredFunctionRetryConfig       `json:"retry"`
	TimeoutMs          int                                  `json:"timeoutMs"`
	Concurrency        *RegisteredFunctionConcurrencyConfig `json:"concurrency"`
	Debounce           *RegisteredFunctionDebounceConfig    `json:"debounce"`
	PreferredMode      ExecutionMode                        `json:"preferredMode"`
	EndpointURL        string                               `json:"endpointUrl"`
	ActorKey           string                               `json:"actorKey"`
	Status             FunctionStatus                       `json:"status"`
	Version            int                                  `json:"version"`
	CreatedAt          time.Time                            `json:"createdAt"`
	UpdatedAt          time.Time                            `json:"updatedAt"`
	Recording          bool                                 `json:"recording"`
	RecordingRetention string                               `json:"recordingRetention"`
	Metadata           map[string]any                       `json:"metadata"`
	CancelOn           []CancelOnConfig                     `json:"cancelOn"`
}

// FunctionChangeType describes why a history snapshot was recorded.
type FunctionChangeType string

const (
	FunctionChangeCreated      FunctionChangeType = "created"
	FunctionChangeUpdate       FunctionChangeType = "update"
	FunctionChangeStatusChange FunctionChangeType = "status_change"
	FunctionChangeRollback     FunctionChangeType = "rollback"
	FunctionChangeDelete       FunctionChangeType = "delete"
)

// FunctionHistoryEntry is an immutable function configuration snapshot.
type FunctionHistoryEntry struct {
	EventID          string              `json:"eventId"`
	EntityVersion    ProtoInt64          `json:"entityVersion"`
	FunctionID       string              `json:"functionId"`
	FunctionSnapshot *RegisteredFunction `json:"functionSnapshot"`
	ActorID          string              `json:"actorId"`
	ChangeReason     string              `json:"changeReason"`
	ChangeType       FunctionChangeType  `json:"changeType"`
	RecordedAt       time.Time           `json:"recordedAt"`
}

// ListFunctionHistoryOptions configures keyset pagination for function history.
type ListFunctionHistoryOptions struct {
	Limit       int
	FromVersion int64
}

// ListFunctionHistoryResult is one page of function history.
type ListFunctionHistoryResult struct {
	Entries []FunctionHistoryEntry `json:"entries"`
	HasMore bool                   `json:"hasMore"`
}

// GetFunction returns a registered function by ID.
func (c *Client) GetFunction(ctx context.Context, functionID string) (*RegisteredFunction, error) {
	var result RegisteredFunction
	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/GetFunction", map[string]any{
		"id": functionID,
	}, &result); err != nil {
		return nil, err
	}
	normalizeRegisteredFunction(&result)
	return &result, nil
}

// UpdateFunctionStatus changes a function's lifecycle status.
func (c *Client) UpdateFunctionStatus(ctx context.Context, functionID string, status FunctionStatus) (*RegisteredFunction, error) {
	var result RegisteredFunction
	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/UpdateFunctionStatus", map[string]any{
		"id":     functionID,
		"status": "FUNCTION_STATUS_" + strings.ToUpper(string(status)),
	}, &result); err != nil {
		return nil, err
	}
	normalizeRegisteredFunction(&result)
	return &result, nil
}

// DeleteFunction permanently deletes a registered function.
func (c *Client) DeleteFunction(ctx context.Context, functionID string) error {
	return c.request(ctx, "POST", "/ironflow.v1.IronflowService/DeleteFunction", map[string]any{
		"id": functionID,
	}, nil)
}

// ListFunctionHistory returns immutable snapshots, newest first.
func (c *Client) ListFunctionHistory(ctx context.Context, functionID string, opts ListFunctionHistoryOptions) (*ListFunctionHistoryResult, error) {
	body := map[string]any{"functionId": functionID}
	if opts.Limit != 0 {
		body["limit"] = opts.Limit
	}
	if opts.FromVersion != 0 {
		body["fromVersion"] = opts.FromVersion
	}

	var result ListFunctionHistoryResult
	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/ListFunctionHistory", body, &result); err != nil {
		return nil, err
	}
	for i := range result.Entries {
		normalizeRegisteredFunction(result.Entries[i].FunctionSnapshot)
	}
	return &result, nil
}

// GetFunctionAtVersion returns one historical configuration snapshot.
func (c *Client) GetFunctionAtVersion(ctx context.Context, functionID string, version int64) (*FunctionHistoryEntry, error) {
	var result struct {
		Entry FunctionHistoryEntry `json:"entry"`
	}
	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/GetFunctionAtVersion", map[string]any{
		"functionId": functionID,
		"version":    version,
	}, &result); err != nil {
		return nil, err
	}
	normalizeRegisteredFunction(result.Entry.FunctionSnapshot)
	return &result.Entry, nil
}

// RollbackFunction restores a function's configuration from a historical version.
func (c *Client) RollbackFunction(ctx context.Context, functionID string, version int64, changeReason string) (*RegisteredFunction, error) {
	var result struct {
		Function RegisteredFunction `json:"function"`
	}
	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/RollbackFunction", map[string]any{
		"functionId":   functionID,
		"version":      version,
		"changeReason": changeReason,
	}, &result); err != nil {
		return nil, err
	}
	normalizeRegisteredFunction(&result.Function)
	return &result.Function, nil
}

func normalizeRegisteredFunction(fn *RegisteredFunction) {
	if fn == nil {
		return
	}
	fn.Status = FunctionStatus(strings.TrimPrefix(strings.ToLower(string(fn.Status)), "function_status_"))
	fn.PreferredMode = ExecutionMode(strings.TrimPrefix(strings.ToLower(string(fn.PreferredMode)), "execution_mode_"))
}
