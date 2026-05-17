package ironflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"time"
)

const (
	// projectionServicePath is the ConnectRPC service path prefix.
	projectionServicePath = "/ironflow.v1.ProjectionService/"

	// projectionMinBackoff is the initial backoff for empty polls.
	projectionMinBackoff = 1 * time.Second

	// projectionMaxBackoff is the maximum backoff for empty/error polls.
	projectionMaxBackoff = 10 * time.Second
)

// ProjectionRunner polls for events and processes them for a single projection.
// It follows the same pattern as the Worker's pollForJobs loop.
type ProjectionRunner struct {
	projection Projection
	baseURL    string
	headers    map[string]string
	logger     Logger
	httpClient *http.Client
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewProjectionRunner creates a new projection runner.
func NewProjectionRunner(proj Projection, baseURL string, headers map[string]string, logger Logger) *ProjectionRunner {
	ctx, cancel := context.WithCancel(context.Background())
	if logger == nil {
		logger = NewNoopLogger()
	}
	return &ProjectionRunner{
		projection: proj,
		baseURL:    baseURL,
		headers:    headers,
		logger:     logger,
		httpClient: &http.Client{Timeout: DefaultClientTimeout},
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start registers the projection and begins the poll loop in a goroutine.
func (r *ProjectionRunner) Start() error {
	if err := r.register(); err != nil {
		return fmt.Errorf("register projection %q: %w", r.projection.Config.Name, err)
	}

	r.logger.Info("Projection runner started", "projection", r.projection.Config.Name)
	go r.pollLoop()
	return nil
}

// Stop cancels the poll loop.
func (r *ProjectionRunner) Stop() {
	r.cancel()
}

// register sends a RegisterProjection request to the server.
func (r *ProjectionRunner) register() error {
	body := map[string]any{
		"name":    r.projection.Config.Name,
		"events":  r.projection.Config.Events,
		"mode":    string(r.projection.Config.Mode),
		"version": 1,
	}
	if r.projection.Config.PartitionKey != "" {
		body["partitionKey"] = r.projection.Config.PartitionKey
	}

	return r.post("RegisterProjection", body, nil)
}

// pollLoop continuously polls for events with exponential backoff.
func (r *ProjectionRunner) pollLoop() {
	backoff := projectionMinBackoff

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		processed, err := r.poll()
		if err != nil {
			r.logger.Error("Projection poll error", "projection", r.projection.Config.Name, "error", err)
			r.sleep(backoff)
			backoff = minDuration(backoff*2, projectionMaxBackoff)
			continue
		}

		if processed > 0 {
			backoff = projectionMinBackoff // reset on work done
		} else {
			r.sleep(backoff)
			backoff = minDuration(backoff*2, projectionMaxBackoff)
		}
	}
}

// pollResponse is the JSON shape of PollProjectionEvents response.
type pollResponse struct {
	Events       []pollEvent    `json:"events"`
	CurrentState map[string]any `json:"currentState"`
	LastEventSeq ProtoInt64     `json:"lastEventSeq"`
}

// pollEvent is the JSON shape of a single event from PollProjectionEvents.
type pollEvent struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Data      map[string]any `json:"data"`
	Seq       ProtoInt64     `json:"seq"`
	Timestamp string         `json:"timestamp"`
	Source    string         `json:"source"`
	Metadata  map[string]any `json:"metadata"`
}

// toProjectionEvent converts a pollEvent to a ProjectionEvent.
// cloneState returns a deep copy of s via JSON round-trip. Used to isolate
// handler invocations from each other so in-place mutation of nested maps or
// slices cannot leak across iterations (issue #486 I3). An empty or nil map
// short-circuits to a fresh empty map so callers never see nil.
func cloneState(s map[string]any) map[string]any {
	if len(s) == 0 {
		return make(map[string]any)
	}
	buf, err := json.Marshal(s)
	if err != nil {
		// Non-JSON-safe value in the state map — fall back to shallow copy.
		out := make(map[string]any, len(s))
		maps.Copy(out, s)
		return out
	}
	var out map[string]any
	if err := json.Unmarshal(buf, &out); err != nil {
		return make(map[string]any)
	}
	return out
}

func toProjectionEvent(evt pollEvent) ProjectionEvent {
	return ProjectionEvent{
		ID:        evt.ID,
		Name:      evt.Name,
		Data:      evt.Data,
		Seq:       int64(evt.Seq),
		Timestamp: evt.Timestamp,
		Source:    evt.Source,
		Metadata:  evt.Metadata,
	}
}

// poll fetches a batch of events and processes them.
// Returns the number of events processed.
func (r *ProjectionRunner) poll() (int, error) {
	// Request events from server
	batchSize := r.projection.Config.BatchSize
	if batchSize == 0 {
		batchSize = 100
	}

	var resp pollResponse
	if err := r.post("PollProjectionEvents", map[string]any{
		"name":      r.projection.Config.Name,
		"batchSize": batchSize,
	}, &resp); err != nil {
		return 0, err
	}

	if len(resp.Events) == 0 {
		return 0, nil
	}

	config := r.projection.Config

	if config.Mode == ProjectionModeManaged {
		return r.processManaged(config, resp)
	}
	return r.processExternal(config, resp)
}

// partitionBatch groups events for a single partition.
type partitionBatch struct {
	events        []pollEvent
	lastEventID   string
	lastEventSeq  int64
	lastEventTime string
}

// processManaged runs the handler as a reducer and saves the resulting state.
// Events are grouped by partition key (from event metadata) and each partition
// is processed independently to prevent cross-partition state corruption.
func (r *ProjectionRunner) processManaged(config ProjectionConfig, resp pollResponse) (int, error) {
	// Group events by partition key from metadata.
	partitions := make(map[string]*partitionBatch)
	for _, evt := range resp.Events {
		pk := "__global__"
		if p, ok := evt.Metadata["__partition"]; ok {
			if ps, ok := p.(string); ok && ps != "" {
				pk = ps
			}
		}
		if partitions[pk] == nil {
			partitions[pk] = &partitionBatch{}
		}
		partitions[pk].events = append(partitions[pk].events, evt)
	}

	// Process each partition independently.
	for pk, batch := range partitions {
		// Use currentState from server only for __global__ (non-partitioned).
		// For other partitions, start from initialState.
		var state map[string]any
		if pk == "__global__" && resp.CurrentState != nil {
			state = resp.CurrentState
		}
		if state == nil && config.InitialState != nil {
			state = config.InitialState()
		}
		if state == nil {
			state = make(map[string]any)
		}

		for _, evt := range batch.events {
			projEvent := toProjectionEvent(evt)
			ctx := r.buildContext(evt)

			// Defuse aliasing bugs (issue #486 I3): hand the handler an
			// isolated copy of state so in-place mutation of nested maps or
			// slices cannot leak across iterations. The handler's documented
			// contract is still pure (state, event) → newState, but this
			// removes the sharpest edge of the footgun.
			handlerState := cloneState(state)

			newState, err := config.Handler(handlerState, projEvent, ctx)
			if err != nil {
				return 0, fmt.Errorf("handler error on event %s: %w", evt.ID, err)
			}

			state = newState
			batch.lastEventID = evt.ID
			batch.lastEventSeq = int64(evt.Seq)
			batch.lastEventTime = evt.Timestamp
		}

		// Save state for this partition.
		saveBody := map[string]any{
			"name":         config.Name,
			"partitionKey": pk,
			"state":        state,
			"lastEventId":  batch.lastEventID,
			"lastEventSeq": batch.lastEventSeq,
		}
		if batch.lastEventTime != "" {
			saveBody["lastEventTime"] = batch.lastEventTime
		}

		if err := r.post("SaveProjectionState", saveBody, nil); err != nil {
			return 0, fmt.Errorf("save state for partition %q: %w", pk, err)
		}
	}

	return len(resp.Events), nil
}

// processExternal runs the handler for each event and acks.
func (r *ProjectionRunner) processExternal(config ProjectionConfig, resp pollResponse) (int, error) {
	var lastEventID string
	var lastEventSeq int64

	for _, evt := range resp.Events {
		projEvent := toProjectionEvent(evt)
		ctx := r.buildContext(evt)

		if _, err := config.Handler(nil, projEvent, ctx); err != nil {
			return 0, fmt.Errorf("handler error on event %s: %w", evt.ID, err)
		}

		lastEventID = evt.ID
		lastEventSeq = int64(evt.Seq)
	}

	// Ack events
	if err := r.post("AckProjectionEvents", map[string]any{
		"name":         config.Name,
		"lastEventId":  lastEventID,
		"lastEventSeq": lastEventSeq,
	}, nil); err != nil {
		return 0, fmt.Errorf("ack events: %w", err)
	}

	return len(resp.Events), nil
}

// buildContext creates a ProjectionContext for the given event.
func (r *ProjectionRunner) buildContext(evt pollEvent) ProjectionContext {
	return ProjectionContext{
		Event: struct {
			ID        string
			Name      string
			Seq       int64
			Timestamp string
		}{
			ID:        evt.ID,
			Name:      evt.Name,
			Seq:       int64(evt.Seq),
			Timestamp: evt.Timestamp,
		},
		Projection: struct {
			Name    string
			Version int
		}{
			Name:    r.projection.Config.Name,
			Version: 1,
		},
	}
}

// post makes a POST request to a ConnectRPC endpoint.
func (r *ProjectionRunner) post(method string, body any, result any) error {
	url := r.baseURL + projectionServicePath + method

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(r.ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range r.headers {
		req.Header.Set(k, v)
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil {
		return json.NewDecoder(resp.Body).Decode(result)
	}

	return nil
}

// sleep pauses for the given duration, interruptible by context cancellation.
func (r *ProjectionRunner) sleep(d time.Duration) {
	select {
	case <-r.ctx.Done():
	case <-time.After(d):
	}
}

// minDuration returns the smaller of two durations.
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
