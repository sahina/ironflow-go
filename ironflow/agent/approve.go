package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/sahina/ironflow-go/ironflow"
)

// approvalEventData is the on-the-wire shape an approver publishes.
type approvalEventData[T any] struct {
	RunID    string `json:"runId"`
	Approved bool   `json:"approved"`
	Approver string `json:"approver,omitempty"`
	Payload  T      `json:"payload,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// Approve waits for a human approval event. It is sugar over
// ironflow.WaitForEvent: the caller chooses eventName and the engine has no
// approval concept. The approver emits that same name with
// { runId, approved } through any client (REST POST /api/v1/events, Connect
// Emit, CLI, SDK, webhook). Default behavior on TTL elapse is to return
// Approved=false, Reason="timeout" — explicit rejection vs timeout is
// observable to the caller via the Reason field.
//
// The wait matches the event name and data.runId == ctx.Run().ID:
//
//	data: { runId, approved, approver?, payload?, reason? }
//
// The step name is "approve.{eventName}". The SDK escapes the name in the
// step ID, so any event name is a valid step name.
func Approve[T any](ctx Context, eventName string, opts ApproveOptions[T]) (ApproveResult[T], error) {
	var zero ApproveResult[T]
	if strings.TrimSpace(eventName) == "" {
		return zero, errors.New("approve: event name must not be empty")
	}
	runID := ctx.Run().ID
	stepName := "approve." + eventName

	filter := ironflow.EventFilter{
		Event:      eventName,
		Payload:    opts.Payload,
		Match:      "data.runId",
		MatchValue: runID,
		Timeout:    opts.TTL,
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
		Approved:  data.Approved,
		Approver:  data.Approver,
		EmittedBy: event.EmittedBy,
		Payload:   data.Payload,
		Reason:    data.Reason,
	}, nil
}
