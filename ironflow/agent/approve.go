package agent

import (
	"encoding/json"
	"fmt"

	"github.com/sahina/ironflow-go/ironflow"
)

// approveEventPrefix is the canonical event-name prefix for approval
// events. Mirrors the JS APPROVE_EVENT_PREFIX.
const approveEventPrefix = "agent.approve."

// approvalEventData is the on-the-wire shape an approver publishes.
type approvalEventData[T any] struct {
	RunID    string `json:"runId"`
	Approved bool   `json:"approved"`
	Approver string `json:"approver,omitempty"`
	Payload  T      `json:"payload,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// Approve waits for a human approval event. Wraps ironflow.WaitForEvent
// on a deterministic event name derived from the agent run + approval
// name. Default behavior on TTL elapse is to return Approved=false,
// Reason="timeout" — explicit rejection vs timeout is observable to
// the caller via the Reason field.
//
// Approval events follow the convention:
//
//	name:   "agent.approve.{name}"
//	filter: data.runId == "{ctx.Run().ID}"
//	data:   { runId, approved, approver?, payload?, reason? }
func Approve[T any](ctx Context, name string, opts ApproveOptions[T]) (ApproveResult[T], error) {
	var zero ApproveResult[T]
	runID := ctx.Run().ID
	eventName := approveEventPrefix + name
	stepName := "approve." + name

	filter := ironflow.EventFilter{
		Event:   eventName,
		Match:   fmt.Sprintf(`data.runId == "%s"`, escapeMatchValue(runID)),
		Timeout: opts.TTL,
	}

	event, err := ironflow.WaitForEvent[approvalEventData[T]](ctx.Inner, stepName, filter)
	if err != nil {
		return zero, err
	}

	// Empty-event sentinel means the wait elapsed without a match.
	// Production resume path returns Event{} (zero value) on timeout —
	// real events always carry a non-empty ID assigned server-side.
	if event.ID == "" {
		return ApproveResult[T]{Approved: false, Reason: "timeout"}, nil
	}

	var data approvalEventData[T]
	if len(event.RawData) > 0 {
		if err := json.Unmarshal(event.RawData, &data); err != nil {
			return zero, fmt.Errorf("approve: decode event data: %w", err)
		}
	}

	return ApproveResult[T]{
		Approved: data.Approved,
		Approver: data.Approver,
		Payload:  data.Payload,
		Reason:   data.Reason,
	}, nil
}
