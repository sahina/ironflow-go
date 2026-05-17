package agent

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sahina/ironflow-go/ironflow"
)

type approvalPayload struct {
	Title string `json:"title"`
}

func TestApprove_TimeoutDefault(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	// No wait mock — interceptor returns zero-valued event = treat as timeout.
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)
	res, err := Approve[approvalPayload](ctx, "ship-it", ApproveOptions[approvalPayload]{
		TTL: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Approve: %v", err)
	}
	if res.Approved {
		t.Errorf("Approved = true on timeout, want false")
	}
	if res.Reason != "timeout" {
		t.Errorf("Reason = %q, want 'timeout'", res.Reason)
	}
}

func TestApprove_ApprovedDecodesPayload(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	approvalData := approvalEventData[approvalPayload]{
		RunID:    "run-test-1",
		Approved: true,
		Approver: "alice",
		Payload:  approvalPayload{Title: "ship"},
		Reason:   "lgtm",
	}
	rawData, err := json.Marshal(approvalData)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	interceptor.waitMocks["approve.ship-it"] = func(filter ironflow.EventFilter) (ironflow.Event, error) {
		if !strings.Contains(filter.Match, "run-test-1") {
			t.Errorf("filter.Match missing runID: %q", filter.Match)
		}
		if filter.Event != "agent.approve.ship-it" {
			t.Errorf("filter.Event = %q, want agent.approve.ship-it", filter.Event)
		}
		return ironflow.Event{
			ID:      "evt-approve-1",
			Name:    "agent.approve.ship-it",
			RawData: rawData,
		}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)

	res, err := Approve[approvalPayload](ctx, "ship-it", ApproveOptions[approvalPayload]{
		TTL: time.Second,
	})
	if err != nil {
		t.Fatalf("Approve: %v", err)
	}
	if !res.Approved {
		t.Error("Approved = false, want true")
	}
	if res.Approver != "alice" {
		t.Errorf("Approver = %q, want alice", res.Approver)
	}
	if res.Payload.Title != "ship" {
		t.Errorf("Payload.Title = %q, want ship", res.Payload.Title)
	}
	if res.Reason != "lgtm" {
		t.Errorf("Reason = %q, want lgtm", res.Reason)
	}
}

func TestApprove_RejectedNotApproved(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	approvalData := approvalEventData[approvalPayload]{
		RunID:    "run-test-1",
		Approved: false,
		Approver: "bob",
		Reason:   "needs-fix",
	}
	rawData, _ := json.Marshal(approvalData)
	interceptor.waitMocks["approve.ship-it"] = func(_ ironflow.EventFilter) (ironflow.Event, error) {
		return ironflow.Event{ID: "evt-approve-2", Name: "agent.approve.ship-it", RawData: rawData}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)

	res, err := Approve[approvalPayload](ctx, "ship-it", ApproveOptions[approvalPayload]{TTL: time.Second})
	if err != nil {
		t.Fatalf("Approve: %v", err)
	}
	if res.Approved {
		t.Error("Approved = true on rejection")
	}
	if res.Reason != "needs-fix" {
		t.Errorf("Reason = %q, want needs-fix", res.Reason)
	}
}
