package ironflow

import (
	"container/list"
	"encoding/json"
	"strings"
	"sync"
)

// RunEvent is the payload of a system.run.{runID}.{event} frame. Step frames
// (system.run.{runID}.step.{stepID}.{event}) have a different shape.
type RunEvent struct {
	ID            string `json:"id"`
	FunctionID    string `json:"functionId"`
	EventID       string `json:"eventId"`
	ExecutionMode string `json:"executionMode"`
	Status        string `json:"status"`
	Attempt       int    `json:"attempt"`
	MaxAttempts   int    `json:"maxAttempts"`
	// Version is the run row's version as written by the transition this frame
	// announces. It rises with every write to the run. Zero means unknown: the
	// deleted frame (the row is gone) and a frame from a server that predates the
	// field carry none.
	Version     int             `json:"version"`
	CreatedAt   string          `json:"createdAt"`
	UpdatedAt   string          `json:"updatedAt"`
	StartedAt   string          `json:"startedAt,omitempty"`
	CompletedAt string          `json:"completedAt,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       json.RawMessage `json:"error,omitempty"`
	Timestamp   string          `json:"timestamp"`
}

// ParseRunEvent decodes the payload of a run frame.
func ParseRunEvent(event *SubscriptionEvent) (*RunEvent, error) {
	var run RunEvent
	if err := json.Unmarshal(event.Data, &run); err != nil {
		return nil, err
	}
	return &run, nil
}

// defaultRunVersionFilterMaxRuns is how many runs a filter remembers before it
// forgets the least recently seen.
const defaultRunVersionFilterMaxRuns = 10_000

// RunVersionFilter drops older run and step frames, and late frames for
// remembered deleted runs.
//
// Frames for one run can arrive out of order: a pull worker may poll one node
// and complete on another, and the completed frame can land before the running
// one. A watcher that shows the status of the last frame it got would then show
// running after completed.
//
// Accept passes without ranking: any frame outside system.run., a
// frame with no usable version (unless its run was deleted), and a frame whose version ties
// the highest seen (one write publishes both .failed and .updated). A deleted
// frame remembers a tombstone that blocks late run and step frames.
//
// Run and step identities share one cache capped at MaxRuns. Steps rank
// independently. Eviction forgets ordering and deletion history.
// One filter belongs to one subscription. It is safe for concurrent use.
//
// Example:
//
//	keep := ironflow.NewRunVersionFilter()
//	for event := range sub.Events() {
//	    if !keep.Accept(event) {
//	        continue
//	    }
//	    render(event)
//	}
type RunVersionFilter struct {
	// MaxRuns caps the run and step identities remembered. Zero means 10,000.
	MaxRuns int

	mu      sync.Mutex
	highest map[string]*list.Element // runID -> element holding *runVersion
	recency *list.List               // front = least recently seen
}

type runVersion struct {
	runID   string
	version int
}

// NewRunVersionFilter remembers up to 10,000 run and step identities.
func NewRunVersionFilter() *RunVersionFilter {
	return &RunVersionFilter{MaxRuns: defaultRunVersionFilterMaxRuns}
}

// Accept reports whether the frame survives ordering and remembered deletion.
func (f *RunVersionFilter) Accept(event *SubscriptionEvent) bool {
	parts := strings.Split(event.Topic, ".")
	isStep := len(parts) >= 6 && parts[3] == "step"
	if (!isStep && len(parts) != 4) || parts[0] != "system" || parts[1] != "run" {
		return true
	}
	runID, leaf := parts[2], parts[3]

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.highest == nil {
		f.highest = make(map[string]*list.Element)
		f.recency = list.New()
	}

	deleted := !isStep && leaf == "deleted"
	if el, ok := f.highest[runID]; ok && !deleted {
		seen, _ := el.Value.(*runVersion) // only *runVersion is ever pushed
		if seen.version == -1 {
			return false
		}
	}
	key := runID
	if isStep {
		key = strings.Join(parts[2:len(parts)-1], ".")
	}
	var payload struct {
		Version int `json:"version"`
	}
	if deleted {
		payload.Version = -1
	} else if err := json.Unmarshal(event.Data, &payload); err != nil || payload.Version < 1 {
		return true
	}
	if el, ok := f.highest[key]; ok {
		seen, _ := el.Value.(*runVersion) // only *runVersion is ever pushed
		if !deleted && payload.Version < seen.version {
			return false
		}
		seen.version = payload.Version
		f.recency.MoveToBack(el)
		return true
	}
	f.highest[key] = f.recency.PushBack(&runVersion{runID: key, version: payload.Version})
	limit := f.MaxRuns
	if limit <= 0 {
		limit = defaultRunVersionFilterMaxRuns
	}
	for f.recency.Len() > limit {
		oldest := f.recency.Front()
		f.recency.Remove(oldest)
		if evicted, ok := oldest.Value.(*runVersion); ok {
			delete(f.highest, evicted.runID)
		}
	}
	return true
}
