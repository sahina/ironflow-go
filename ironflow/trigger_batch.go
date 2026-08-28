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

// TriggerBatch triggers multiple events in one request.
func (c *Client) TriggerBatch(ctx context.Context, events []TriggerBatchEvent) ([]EmitResult, error) {
	var response struct {
		Results []struct {
			RunIDs  []string `json:"runIds"`
			EventID string   `json:"eventId"`
		} `json:"results"`
	}
	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/TriggerBatch", map[string]any{
		"events": events,
	}, &response); err != nil {
		return nil, err
	}

	results := make([]EmitResult, len(response.Results))
	for i, result := range response.Results {
		results[i] = EmitResult{RunIDs: result.RunIDs, EventID: result.EventID}
	}
	return results, nil
}
