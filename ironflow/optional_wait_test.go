package ironflow

import (
	"encoding/json"
	"testing"
)

func TestOptionalWaitReplaysNull(t *testing.T) {
	req := &PushRequest{RunID: "r", Steps: []CompletedStep{{ID: "r:wait:0", Status: "completed", Output: nil}}}
	exec := newExecutionContext(req)
	event, err := WaitForEventOptional(Context{exec: exec}, "wait", EventFilter{Event: "reply"})
	if err != nil || event != nil {
		t.Fatalf("event=%#v err=%v", event, err)
	}
}
func TestOptionalWaitYieldsPolicy(t *testing.T) {
	defer func() {
		y, ok := recover().(*yieldSignal)
		if !ok {
			t.Fatal("expected yield")
		}
		raw, err := json.Marshal(y.info)
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Filter struct {
				OnTimeout string `json:"on_timeout"`
			} `json:"event_filter"`
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatal(err)
		}
		if body.Filter.OnTimeout != "return-null" {
			t.Fatalf("yield=%s", raw)
		}
	}()
	_, _ = WaitForEventOptional(Context{exec: newExecutionContext(&PushRequest{RunID: "r"})}, "wait", EventFilter{Event: "reply"})
}

func TestOptionalWaitBranchReplaysNull(t *testing.T) {
	exec := newExecutionContext(&PushRequest{RunID: "r", Steps: []CompletedStep{{ID: "r:parallel:0:wait:0", Status: "completed", Output: nil}}})
	branch := exec.createBranchContext("parallel", 0)
	event, err := WaitForEventOptionalWithBranch(branch, "wait", EventFilter{Event: "reply"})
	if err != nil || event != nil {
		t.Fatalf("event=%+v err=%v", event, err)
	}
}
