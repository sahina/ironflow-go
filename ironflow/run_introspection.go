package ironflow

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
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
	rpc := ironflowv1connect.NewIronflowServiceClient(c.httpClient, c.serverURL, connect.WithProtoJSON(), connect.WithInterceptors(bearerInterceptor(c.apiKey)))
	var response *connect.Response[ironflowv1.GetRunStepsResponse]
	// sdkcoverage: POST /ironflow.v1.IronflowService/GetRunSteps
	err := c.withRetry(ctx, func() error {
		var err error
		response, err = rpc.GetRunSteps(ctx, connect.NewRequest(&ironflowv1.GetRunStepsRequest{RunId: runID}))
		return connectError(err)
	})
	if err != nil {
		return nil, err
	}
	result := &RunStepsResult{Steps: make([]RunStep, 0, len(response.Msg.Steps)), Count: len(response.Msg.Steps)}
	for _, step := range response.Msg.Steps {
		result.Steps = append(result.Steps, runStepFromProto(step))
	}
	return result, nil
}

// GetRunStreams returns the entity stream IDs touched by a run.
func (c *Client) GetRunStreams(ctx context.Context, runID string) (*RunStreamsResult, error) {
	var result RunStreamsResult
	if err := c.request(ctx, http.MethodGet, "/api/v1/runs/"+url.PathEscape(runID)+"/streams", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func inspectionTimestamp(value *timestamppb.Timestamp) *time.Time {
	if value == nil {
		return nil
	}
	result := value.AsTime()
	return &result
}
func runStepFromProto(s *ironflowv1.Step) RunStep {
	result := RunStep{ID: s.Id, RunID: s.RunId, StepID: s.StepId, StepType: strings.ToLower(strings.TrimPrefix(s.StepType.String(), "STEP_TYPE_")), Sequence: int(s.Sequence), Status: s.StoredStatus,
		Input: streamPayload(s.Input, s.InputValue), Output: streamPayload(s.Output, s.OutputValue), OriginalOutput: streamPayload(s.OriginalOutput, s.OriginalOutputValue), InputHash: s.InputHash, Attempt: int(s.Attempt),
		StartedAt: inspectionTimestamp(s.StartedAt), EndedAt: inspectionTimestamp(s.EndedAt), SleepUntil: inspectionTimestamp(s.SleepUntil), WaitTimeout: inspectionTimestamp(s.WaitTimeout), WaitEventName: s.WaitEventName, PatchedAt: inspectionTimestamp(s.PatchedAt), PatchedBy: s.PatchedBy, CompensationFor: s.CompensationFor, CreatedAt: s.CreatedAt.AsTime(), UpdatedAt: s.UpdatedAt.AsTime()}
	if result.Status == "" {
		result.Status = strings.ToLower(strings.TrimPrefix(s.Status.String(), "STEP_STATUS_"))
	}
	if s.DurationMsFull != nil {
		value := int(*s.DurationMsFull)
		result.DurationMs = &value
	} else if s.DurationMs != 0 {
		value := int(s.DurationMs)
		result.DurationMs = &value
	}
	if s.ErrorValue != nil {
		result.Error = s.ErrorValue.AsInterface()
	} else if s.Error != nil {
		stepError := map[string]any{"message": s.Error.Message, "code": s.Error.Code, "stack": s.Error.Stack, "retryable": s.Error.Retryable}
		if s.Error.Details != nil {
			stepError["details"] = s.Error.Details.AsMap()
		}
		result.Error = stepError
	}
	return result
}
