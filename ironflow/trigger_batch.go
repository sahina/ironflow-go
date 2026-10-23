package ironflow

import "context"

// TriggerBatchEvent is one event in a batch trigger request.
type TriggerBatchEvent struct {
	Event          string         `json:"event"`
	Data           any            `json:"data"`
	IdempotencyKey string         `json:"idempotencyKey,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
	Version        int            `json:"version,omitempty"`
}

// TriggerBatchResult is the outcome of one batch item. Error is set when the
// item did not emit; its Code is the Connect code, e.g. "invalid_argument".
// The other items still emit.
type TriggerBatchResult struct {
	EmitResult
	Error *ErrorInfo
}

// TriggerBatch triggers multiple events in one request. It returns one result
// per event, in order. A failed item carries Error; it does not fail the call.
func (c *Client) TriggerBatch(ctx context.Context, events []TriggerBatchEvent) ([]TriggerBatchResult, error) {
	var response struct {
		Results []struct {
			RunIDs  []string   `json:"runIds"`
			EventID string     `json:"eventId"`
			Error   *ErrorInfo `json:"error"`
		} `json:"results"`
	}
	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/TriggerBatch", map[string]any{
		"events": events,
	}, &response); err != nil {
		return nil, err
	}

	results := make([]TriggerBatchResult, len(response.Results))
	for i, result := range response.Results {
		results[i] = TriggerBatchResult{
			EmitResult: EmitResult{RunIDs: result.RunIDs, EventID: result.EventID},
			Error:      result.Error,
		}
	}
	return results, nil
}
