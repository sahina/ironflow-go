package ironflow

import (
	"context"
	"net/http"
	"net/url"
	"time"
)

// RunStep is one durable step recorded for a run.
type RunStep struct {
	ID              string     `json:"id"`
	RunID           string     `json:"run_id"`
	StepID          string     `json:"step_id"`
	StepType        string     `json:"step_type"`
	Sequence        int        `json:"sequence"`
	Status          string     `json:"status"`
	Input           any        `json:"input"`
	Output          any        `json:"output"`
	OriginalOutput  any        `json:"original_output"`
	Error           any        `json:"error"`
	InputHash       string     `json:"input_hash"`
	Attempt         int        `json:"attempt"`
	DurationMs      *int       `json:"duration_ms"`
	StartedAt       *time.Time `json:"started_at"`
	EndedAt         *time.Time `json:"ended_at"`
	SleepUntil      *time.Time `json:"sleep_until"`
	WaitEventName   string     `json:"wait_event_name"`
	WaitTimeout     *time.Time `json:"wait_timeout"`
	PatchedAt       *time.Time `json:"patched_at"`
	PatchedBy       string     `json:"patched_by"`
	CompensationFor string     `json:"compensation_for"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// RunStepsResult contains the durable steps recorded for a run.
type RunStepsResult struct {
	Steps []RunStep `json:"steps"`
	Count int       `json:"count"`
}

// RunStreamsResult contains the entity stream IDs touched by a run.
type RunStreamsResult struct {
	EntityIDs []string `json:"entity_ids"`
}

// GetRunSteps returns the durable steps recorded for a run.
func (c *Client) GetRunSteps(ctx context.Context, runID string) (*RunStepsResult, error) {
	var result RunStepsResult
	if err := c.request(ctx, http.MethodGet, "/api/v1/runs/"+url.PathEscape(runID)+"/steps", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetRunStreams returns the entity stream IDs touched by a run.
func (c *Client) GetRunStreams(ctx context.Context, runID string) (*RunStreamsResult, error) {
	var result RunStreamsResult
	if err := c.request(ctx, http.MethodGet, "/api/v1/runs/"+url.PathEscape(runID)+"/streams", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
