package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/sahina/ironflow-go/ironflow"
)

// memoryAppendWaitTimeout is the wait window for the auto-catchup
// after Memory.Append. Mirrors the JS DEFAULT_WAIT_TIMEOUT_MS (5s).
// Stream-level minSeq is sufficient for read-your-writes — partition
// param intentionally omitted.
const memoryAppendWaitTimeout = 5 * time.Second

// memoryDefaultEntityType matches the JS DEFAULT_ENTITY_TYPE. Server
// treats this field as informational; "agent" matches the typical
// streamId convention.
const memoryDefaultEntityType = "agent"

// MemoryBackend is the minimal surface the memory client needs. Wraps
// the IronflowClient so tests can substitute a fake without
// constructing a real client.
type MemoryBackend interface {
	// AppendEvent appends a memory event to the entity stream.
	AppendEvent(ctx context.Context, streamID string, input MemoryAppendInput) (*ironflow.AppendResult, error)

	// GetProjection reads the projection state.
	GetProjection(ctx context.Context, name string) (*ironflow.ProjectionStateResult, error)

	// WaitForEvent blocks until the projection has processed the given
	// event. Appends run through the transactional outbox, so no NATS
	// sequence is known at append time — the server resolves the event ID.
	WaitForEvent(ctx context.Context, eventID, projection string, opts ironflow.WaitForProjectionOpts) error
}

// MemoryAppendInput carries the wire fields for AppendEvent. Mirrors
// the JS shape exactly.
type MemoryAppendInput struct {
	Name           string
	Data           map[string]any
	EntityType     string
	IdempotencyKey string
	Metadata       map[string]any
}

// Memory returns the memory client bound to the running agent.
func Memory(ctx Context) MemoryClient {
	return memoryClient{ctx: ctx}
}

// memoryClient is the bound implementation of MemoryClient.
type memoryClient struct {
	ctx Context
}

// Get reads the projected memory state. Cached within the run unless
// BypassCache is set.
func (m memoryClient) Get(opts ...MemoryGetOptions) (map[string]any, error) {
	cfg, runtime, err := m.requireConfigured()
	if err != nil {
		return nil, err
	}
	bypass := false
	if len(opts) > 0 {
		bypass = opts[0].BypassCache
	}

	runtime.mu.Lock()
	hasCached := runtime.memoryCacheLoaded
	cached := runtime.memoryCache
	runtime.mu.Unlock()
	if !bypass && hasCached {
		return coerceProjectionState(cached), nil
	}

	if runtime.memoryBackend == nil {
		return nil, &ironflow.IronflowError{
			Message:   "memory.Get requires a runtime backend — set IRONFLOW_URL (or IRONFLOW_SERVER_URL) so the agent can construct an IronflowClient, or inject a backend via the test harness",
			Code:      CodeMemoryNoBackend,
			Retryable: false,
		}
	}

	state, err := ironflow.Run(m.ctx.Inner, "memory.get", func() (*ironflow.ProjectionStateResult, error) {
		return runtime.memoryBackend.GetProjection(context.Background(), cfg.Projection)
	})
	if err != nil {
		return nil, err
	}

	out := projectionStateToMap(state)

	runtime.mu.Lock()
	runtime.memoryCache = out
	runtime.memoryCacheLoaded = true
	runtime.mu.Unlock()

	return out, nil
}

// Append durably writes a memory event and waits for the projection to
// catch up so subsequent Get calls within the same run see the write.
func (m memoryClient) Append(eventName string, data map[string]any, opts ...MemoryAppendOptions) error {
	cfg, runtime, err := m.requireConfigured()
	if err != nil {
		return err
	}
	if runtime.memoryBackend == nil {
		return &ironflow.IronflowError{
			Message:   "memory.Append requires a runtime backend — set IRONFLOW_URL (or IRONFLOW_SERVER_URL) so the agent can construct an IronflowClient, or inject a backend via the test harness",
			Code:      CodeMemoryNoBackend,
			Retryable: false,
		}
	}

	if data == nil {
		return &ironflow.IronflowError{
			Message:   "memory.Append requires data to be a non-nil map — primitives must be wrapped explicitly so the projection reducer sees a stable shape",
			Code:      CodeMemoryInvalidData,
			Retryable: false,
		}
	}

	var metadata map[string]any
	if len(opts) > 0 {
		metadata = opts[0].Metadata
	}

	runtime.mu.Lock()
	counterIndex := runtime.memoryAppendCount
	runtime.memoryAppendCount++
	runtime.mu.Unlock()
	idempotencyKey := fmt.Sprintf("%s:memory.append:%d", m.ctx.Run().ID, counterIndex)

	entityType := cfg.EntityType
	if entityType == "" {
		entityType = memoryDefaultEntityType
	}

	appended, err := ironflow.Run(m.ctx.Inner, "memory.append", func() (*ironflow.AppendResult, error) {
		return runtime.memoryBackend.AppendEvent(context.Background(), cfg.StreamID, MemoryAppendInput{
			Name:           eventName,
			Data:           data,
			EntityType:     entityType,
			IdempotencyKey: idempotencyKey,
			Metadata:       metadata,
		})
	})
	if err != nil {
		return err
	}

	if appended != nil && appended.EventID != "" {
		if _, waitErr := ironflow.Run(m.ctx.Inner, "memory.append.wait", func() (struct{}, error) {
			return struct{}{}, runtime.memoryBackend.WaitForEvent(context.Background(), appended.EventID, cfg.Projection, ironflow.WaitForProjectionOpts{
				Timeout: memoryAppendWaitTimeout,
			})
		}); waitErr != nil {
			return waitErr
		}
	}

	runtime.mu.Lock()
	runtime.memoryCache = nil
	runtime.memoryCacheLoaded = false
	runtime.mu.Unlock()
	return nil
}

// EntityStream is a parity stub. Lands when a concrete cross-agent
// peer-memory use case surfaces.
func (m memoryClient) EntityStream(streamID, projectionName string) (map[string]any, error) {
	if projectionName == "" {
		return nil, NewMemoryProjectionRequiredError(streamID)
	}
	return nil, &ironflow.IronflowError{
		Message:   "memory.EntityStream is not yet implemented — entityStream lands when a concrete cross-agent peer-memory use case surfaces",
		Code:      CodeMemoryNotImplemented,
		Retryable: false,
		Details:   map[string]any{"method": "EntityStream"},
	}
}

// requireConfigured returns the resolved config + runtime, raising if
// memory was not configured on the agent.
func (m memoryClient) requireConfigured() (*MemoryConfig, *agentRuntime, error) {
	if m.ctx.runtime == nil || m.ctx.runtime.memoryConfig == nil {
		return nil, nil, &ironflow.IronflowError{
			Message:   "memory requires AgentConfig.Memory ({StreamID, Projection}) to be set",
			Code:      CodeMemoryUnconfigured,
			Retryable: false,
		}
	}
	return m.ctx.runtime.memoryConfig, m.ctx.runtime, nil
}

// projectionStateToMap returns the State field of the ProjectionStateResult
// coerced to map[string]any (or nil if absent or non-object).
func projectionStateToMap(state *ironflow.ProjectionStateResult) map[string]any {
	if state == nil || state.State == nil {
		return nil
	}
	if m, ok := state.State.(map[string]any); ok {
		return m
	}
	return coerceProjectionState(state.State)
}

// coerceProjectionState narrows a cached map[string]any back to the
// expected type. Cache may already hold a typed map.
func coerceProjectionState(cached any) map[string]any {
	if cached == nil {
		return nil
	}
	if typed, ok := cached.(map[string]any); ok {
		return typed
	}
	bytes, err := json.Marshal(cached)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(bytes, &out); err != nil {
		return nil
	}
	return out
}

// defaultMemoryBackend constructs a backend wrapping a fresh
// ironflow.Client built from environment variables. Returns nil + nil
// when no server URL is configured so makeMemory can surface a clear
// "no backend" error on first use.
func defaultMemoryBackend() (MemoryBackend, error) {
	serverURL := os.Getenv("IRONFLOW_URL")
	if serverURL == "" {
		serverURL = os.Getenv("IRONFLOW_SERVER_URL")
	}
	if serverURL == "" {
		return nil, fmt.Errorf("no server URL configured")
	}

	client := ironflow.NewClient(ironflow.ClientConfig{
		ServerURL: serverURL,
		APIKey:    os.Getenv("IRONFLOW_API_KEY"),
	})

	return &clientBackedBackend{client: client}, nil
}

// clientBackedBackend adapts ironflow.Client to MemoryBackend.
type clientBackedBackend struct {
	client *ironflow.Client
}

// AppendEvent forwards to client.AppendStreamEvent.
func (b *clientBackedBackend) AppendEvent(ctx context.Context, streamID string, input MemoryAppendInput) (*ironflow.AppendResult, error) {
	opts := []ironflow.AppendOption{}
	if input.IdempotencyKey != "" {
		opts = append(opts, ironflow.WithAppendIdempotencyKey(input.IdempotencyKey))
	}
	if input.Metadata != nil {
		opts = append(opts, ironflow.WithAppendMetadata(input.Metadata))
	}
	return b.client.AppendStreamEvent(ctx, streamID, ironflow.AppendEventInput{
		Name:       input.Name,
		Data:       input.Data,
		EntityType: input.EntityType,
	}, opts...)
}

// GetProjection forwards to client.Projections().Get.
func (b *clientBackedBackend) GetProjection(ctx context.Context, name string) (*ironflow.ProjectionStateResult, error) {
	return b.client.Projections().Get(ctx, name)
}

// WaitForEvent forwards to client.WaitForEvent. Drops the result —
// callers only need to know whether it succeeded.
func (b *clientBackedBackend) WaitForEvent(ctx context.Context, eventID, projection string, opts ironflow.WaitForProjectionOpts) error {
	_, err := b.client.WaitForEvent(ctx, eventID, projection, opts)
	return err
}
