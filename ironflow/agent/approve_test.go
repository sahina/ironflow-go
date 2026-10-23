package agent

import (
	"encoding/json"
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
	res, err := Approve[approvalPayload](ctx, "ship-it.approved", ApproveOptions[approvalPayload]{
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
	interceptor.waitMocks["approve.ship-it.approved"] = func(filter ironflow.EventFilter) (ironflow.Event, error) {
		if filter.Payload != (approvalPayload{Title: "draft"}) {
			t.Errorf("request payload = %#v, want draft", filter.Payload)
		}
		if filter.Match != "data.runId" {
			t.Errorf("filter.Match = %q, want data.runId", filter.Match)
		}
		if filter.MatchValue != "run-test-1" {
			t.Errorf("filter.MatchValue = %q, want run-test-1", filter.MatchValue)
		}
		if filter.Event != "ship-it.approved" {
			t.Errorf("filter.Event = %q, want ship-it.approved", filter.Event)
		}
		return ironflow.Event{
			ID:      "evt-approve-1",
			Name:    "ship-it.approved",
			RawData: rawData,
		}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)

	res, err := Approve[approvalPayload](ctx, "ship-it.approved", ApproveOptions[approvalPayload]{
		TTL:     time.Second,
		Payload: approvalPayload{Title: "draft"},
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
	interceptor.waitMocks["approve.ship-it.approved"] = func(_ ironflow.EventFilter) (ironflow.Event, error) {
		return ironflow.Event{ID: "evt-approve-2", Name: "ship-it.approved", RawData: rawData}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)

	res, err := Approve[approvalPayload](ctx, "ship-it.approved", ApproveOptions[approvalPayload]{TTL: time.Second})
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

func TestApprove_EmptyEventNameIsAnError(t *testing.T) {
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, newFakeInterceptor(t))
	for _, name := range []string{"", "  "} {
		if _, err := Approve[approvalPayload](ctx, name, ApproveOptions[approvalPayload]{TTL: time.Second}); err == nil {
			t.Errorf("Approve(%q) = nil error, want error", name)
		}
	}
}

func TestApprove_EventNameWithSlashAndColon(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	interceptor.waitMocks["approve.deploy/api:approved"] = func(filter ironflow.EventFilter) (ironflow.Event, error) {
		if filter.Event != "deploy/api:approved" {
			t.Errorf("filter.Event = %q, want deploy/api:approved", filter.Event)
		}
		return ironflow.Event{ID: "evt-1", Name: filter.Event, RawData: []byte(`{"runId":"run-test-1","approved":true}`)}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)

	res, err := Approve[approvalPayload](ctx, "deploy/api:approved", ApproveOptions[approvalPayload]{TTL: time.Second})
	if err != nil || !res.Approved {
		t.Fatalf("Approve = %+v, %v; want approved", res, err)
	}
}

func TestApprove_EmittedByIsEngineSetNotApprover(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	rawData, _ := json.Marshal(approvalEventData[approvalPayload]{Approved: true, Approver: "alice"})
	interceptor.waitMocks["approve.ship-it.approved"] = func(_ ironflow.EventFilter) (ironflow.Event, error) {
		return ironflow.Event{ID: "evt-1", Name: "ship-it.approved", RawData: rawData, EmittedBy: "apikey:k1"}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)

	res, err := Approve[approvalPayload](ctx, "ship-it.approved", ApproveOptions[approvalPayload]{TTL: time.Second})
	if err != nil {
		t.Fatalf("Approve: %v", err)
	}
	if res.EmittedBy != "apikey:k1" {
		t.Errorf("EmittedBy = %q, want apikey:k1", res.EmittedBy)
	}
	if res.Approver != "alice" {
		t.Errorf("Approver = %q, want alice (caller text)", res.Approver)
	}
}

func TestApprove_EmittedByEmptyWhenAbsent(t *testing.T) {
	interceptor := newFakeInterceptor(t)
	rawData, _ := json.Marshal(approvalEventData[approvalPayload]{Approved: true})
	interceptor.waitMocks["approve.ship-it.approved"] = func(_ ironflow.EventFilter) (ironflow.Event, error) {
		return ironflow.Event{ID: "evt-1", Name: "ship-it.approved", RawData: rawData}, nil
	}
	ctx, _ := newAgentContext(t, AgentConfig{Function: ironflow.FunctionConfig{ID: "a"}}, interceptor)

	res, err := Approve[approvalPayload](ctx, "ship-it.approved", ApproveOptions[approvalPayload]{TTL: time.Second})
	if err != nil {
		t.Fatalf("Approve: %v", err)
	}
	if res.EmittedBy != "" {
		t.Errorf("EmittedBy = %q, want empty", res.EmittedBy)
	}
}
