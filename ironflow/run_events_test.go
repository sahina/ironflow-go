package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func runFrame(runID, leaf string, version any) *SubscriptionEvent {
	data := map[string]any{"id": runID}
	if version != nil {
		data["version"] = version
	}
	raw, _ := json.Marshal(data)
	return &SubscriptionEvent{Topic: fmt.Sprintf("system.run.%s.%s", runID, leaf), Data: raw}
}

func TestRunVersionFilter_DropsLowerVersionArrivingLate(t *testing.T) {
	keep := NewRunVersionFilter()
	var seen []string
	for _, e := range []*SubscriptionEvent{runFrame("r1", "completed", 5), runFrame("r1", "updated", 3)} {
		if keep.Accept(e) {
			seen = append(seen, e.Topic)
		}
	}
	if len(seen) != 1 || seen[0] != "system.run.r1.completed" {
		t.Fatalf("delivered %v, want only the completed frame", seen)
	}
}

func TestRunVersionFilter_PassesInOrderAndTies(t *testing.T) {
	keep := NewRunVersionFilter()
	for _, e := range []*SubscriptionEvent{
		runFrame("r1", "created", 1),
		runFrame("r1", "updated", 2),
		runFrame("r1", "failed", 4),
		runFrame("r1", "updated", 4), // one write publishes both frames
	} {
		if !keep.Accept(e) {
			t.Errorf("dropped %s", e.Topic)
		}
	}
}

func TestRunVersionFilter_RanksEachRunOnItsOwn(t *testing.T) {
	keep := NewRunVersionFilter()
	if !keep.Accept(runFrame("r1", "completed", 9)) || !keep.Accept(runFrame("r2", "updated", 2)) {
		t.Fatal("a frame for another run was dropped")
	}
}

func TestRunVersionFilter_KeepsUnversionedAndNeverRanksIt(t *testing.T) {
	keep := NewRunVersionFilter()
	keep.Accept(runFrame("r1", "completed", 5))
	for _, v := range []any{nil, 0, -1, 1.5, "7"} {
		if !keep.Accept(runFrame("r1", "updated", v)) {
			t.Errorf("dropped a frame with version %v", v)
		}
	}
	if !keep.Accept(&SubscriptionEvent{Topic: "system.run.r1.updated", Data: json.RawMessage(`null`)}) {
		t.Error("dropped a frame with a null payload")
	}
	if keep.Accept(runFrame("r1", "updated", 4)) {
		t.Error("an unversioned frame lowered the bar")
	}
}

func TestRunVersionFilter_PassesStepAndForeignTopics(t *testing.T) {
	keep := NewRunVersionFilter()
	keep.Accept(runFrame("r1", "completed", 5))
	for _, topic := range []string{"system.run.r1.step.s1.completed", "events:order.created", "system.function.f1.updated"} {
		if !keep.Accept(&SubscriptionEvent{Topic: topic, Data: json.RawMessage(`{"version":1}`)}) {
			t.Errorf("dropped %s", topic)
		}
	}
}

func TestRunVersionFilter_DeletionAndStepOrdering(t *testing.T) {
	keep := &RunVersionFilter{MaxRuns: 2}
	for _, leaf := range []string{"deleted", "deleted"} {
		if !keep.Accept(runFrame("r1", leaf, nil)) {
			t.Fatal("duplicate deletion must pass")
		}
	}
	for _, v := range []any{nil, 0, 1, 6} {
		for _, leaf := range []string{"updated", "step.s1.updated"} {
			if keep.Accept(runFrame("r1", leaf, v)) {
				t.Fatalf("resurrected deleted run: %s %v", leaf, v)
			}
		}
	}
	for _, tc := range []struct {
		leaf    string
		version any
		want    bool
	}{
		{"step.s1.updated", 5, true}, {"step.s1.updated", 4, false},
		{"step.s1.completed", 5, true}, {"step.s1.updated", nil, true}, {"step.s2.updated", 1, true},
	} {
		if got := keep.Accept(runFrame("r2", tc.leaf, tc.version)); got != tc.want {
			t.Fatalf("%+v: got %v", tc, got)
		}
	}
	if !keep.Accept(runFrame("r1", "updated", 1)) {
		t.Fatal("tombstone was not evicted")
	}
}

func TestRunVersionFilter_ForgetsLeastRecentlySeenPastMaxRuns(t *testing.T) {
	keep := &RunVersionFilter{MaxRuns: 2}
	keep.Accept(runFrame("a", "updated", 5))
	keep.Accept(runFrame("b", "updated", 5))
	keep.Accept(runFrame("a", "updated", 6)) // refreshes a, so b is the oldest
	keep.Accept(runFrame("c", "updated", 5)) // evicts b
	if keep.Accept(runFrame("a", "updated", 4)) {
		t.Error("a was forgotten too early")
	}
	if !keep.Accept(runFrame("b", "updated", 1)) {
		t.Error("b should have been forgotten")
	}
}

func TestParseRunEvent(t *testing.T) {
	run, err := ParseRunEvent(&SubscriptionEvent{
		Topic: "system.run.r1.updated",
		Data:  json.RawMessage(`{"id":"r1","status":"running","version":3,"attempt":1,"maxAttempts":3}`),
	})
	if err != nil {
		t.Fatalf("ParseRunEvent: %v", err)
	}
	if run.ID != "r1" || run.Status != "running" || run.Version != 3 {
		t.Fatalf("parsed %+v", run)
	}
}

// TestRunVersionFilter_WatcherEndsOnCompleted is the #3283 scenario through a
// real subscription: node B's completed frame is stored before node A's running
// frame, and a watcher behind the filter ends on completed.
func TestRunVersionFilter_WatcherEndsOnCompleted(t *testing.T) {
	server := newMockWSServer()
	defer server.Close()
	server.onMessage = func(data []byte) {
		var msg wsSubscribeRequest
		if err := json.Unmarshal(data, &msg); err != nil || msg.Type != "subscribe" {
			return
		}
		server.SendMessage(map[string]any{
			"type": "subscription_result",
			"results": []map[string]any{
				{"pattern": msg.Subscription.Pattern, "status": "ok", "subscriptionId": "sub_1"},
			},
		})
	}

	client := NewSubscriptionClient(SubscriptionClientConfig{WSURL: server.URL()})
	defer client.Close()
	ctx := context.Background()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	sub, err := client.Subscribe(ctx, "system.run.>", nil)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	go func() {
		for _, f := range []struct {
			leaf, status string
			version      int
		}{{"completed", "completed", 5}, {"updated", "running", 4}} {
			server.SendMessage(map[string]any{
				"type":           "event",
				"subscriptionId": "sub_1",
				"topic":          "system.run.run-1." + f.leaf,
				"data":           map[string]any{"id": "run-1", "status": f.status, "version": f.version},
			})
		}
	}()

	keep := NewRunVersionFilter()
	var statuses []string
	deadline := time.After(300 * time.Millisecond)
	for {
		select {
		case event := <-sub.Events():
			if !keep.Accept(event) {
				continue
			}
			run, err := ParseRunEvent(event)
			if err != nil {
				t.Fatalf("ParseRunEvent: %v", err)
			}
			statuses = append(statuses, run.Status)
		case <-deadline:
			if len(statuses) != 1 || statuses[0] != "completed" {
				t.Fatalf("watcher saw %v, want [completed]", statuses)
			}
			return
		}
	}
}

func TestRunVersionFilter_DottedStepOrdering(t *testing.T) {
	keep := NewRunVersionFilter()
	for _, tc := range []struct {
		leaf    string
		version int
		want    bool
	}{
		{"step.r:memory.get:0.completed", 5, true},
		{"step.r:memory.get:0.updated", 4, false},
		{"step.r:memory.put:0.updated", 1, true},
		{"step.r:memory.get:0.updated", 5, true},
	} {
		if got := keep.Accept(runFrame("r", tc.leaf, tc.version)); got != tc.want {
			t.Errorf("%s v%d: %v, want %v", tc.leaf, tc.version, got, tc.want)
		}
	}
}
func TestRunVersionFilter_DottedStepAfterDeletion(t *testing.T) {
	keep := NewRunVersionFilter()
	keep.Accept(runFrame("r", "deleted", nil))
	for _, v := range []any{nil, 0, 6} {
		if keep.Accept(runFrame("r", "step.r:memory.get:0.updated", v)) {
			t.Errorf("late dotted step v%v passed", v)
		}
	}
}
