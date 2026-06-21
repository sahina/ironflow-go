package ironflow

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// HandlerConfig configures an event handler.
//
// This provides a simpler, more intuitive API compared to CreateFunction,
// with progressive disclosure of advanced options.
type HandlerConfig[TEvent any] struct {
	// Event is the event name or pattern to handle (e.g., "order.placed", "order.*").
	Event string

	// Handler is the function that processes events.
	Handler func(event TEvent, ctx *HandlerContext) (any, error)

	// Options contains optional advanced configuration.
	Options *HandlerOptions
}

// HandlerOptions contains optional handler configuration.
type HandlerOptions struct {
	// ID is the unique handler identifier. Auto-generated from event name if not set.
	ID string

	// Name is the human-readable handler name.
	Name string

	// Filter is a CEL expression to filter events.
	Filter string

	// Retry is the retry configuration for failures.
	Retry *RetryConfig

	// Concurrency is the concurrency control configuration.
	Concurrency *ConcurrencyConfig

	// Debounce is the debounce configuration. Async-only — TriggerSync
	// rejects debounced functions. Issue #545.
	Debounce *DebounceConfig

	// Timeout is the handler timeout.
	Timeout time.Duration

	// StepTimeout is the default timeout for all step.run() calls.
	StepTimeout time.Duration

	// Mode is the execution mode (push or pull).
	Mode ExecutionMode

	// ActorKey is the JSON path for actor-based sticky routing.
	ActorKey string

	// Secrets is the list of secret names this handler requires.
	Secrets []string
}

// HandlerContext is passed to handler functions.
// It wraps the underlying Context with a simpler interface.
type HandlerContext struct {
	// Event is the typed event data (same as the first argument for convenience).
	Event any

	// EventMeta contains event metadata.
	EventMeta HandlerEventMeta

	// Run contains run information.
	Run RunInfo

	// Secrets provides read-only access to resolved secrets.
	Secrets SecretsReader

	// Step provides durable step operations.
	Step *StepClient

	// Logger provides logging functionality.
	Logger Logger

	// internal context
	ctx Context
}

// HandlerEventMeta contains event metadata for handlers.
type HandlerEventMeta struct {
	// ID is the event ID.
	ID string

	// Name is the event name.
	Name string

	// Version is the event schema version.
	Version int

	// Timestamp is when the event occurred.
	Timestamp time.Time

	// IdempotencyKey is the deduplication key.
	IdempotencyKey string

	// Source is the event origin.
	Source EventSourceType

	// Metadata contains additional metadata.
	Metadata map[string]any
}

// StepClient provides durable step operations.
type StepClient struct {
	ctx Context
}

// Run executes a named step with durable execution.
func (s *StepClient) Run(name string, fn func() (any, error), opts ...StepOption) (any, error) {
	return Run(s.ctx, name, fn, opts...)
}

// Sleep pauses execution for the specified duration.
func (s *StepClient) Sleep(name string, duration time.Duration) error {
	return Sleep(s.ctx, name, duration)
}

// SleepUntil pauses execution until the specified time.
func (s *StepClient) SleepUntil(name string, until time.Time) error {
	return SleepUntil(s.ctx, name, until)
}

// WaitForEvent waits for a matching event.
func (s *StepClient) WaitForEvent(name string, filter EventFilter) (Event, error) {
	return WaitForEvent[any](s.ctx, name, filter)
}

// Parallel executes multiple steps in parallel.
func (s *StepClient) Parallel(name string, branches []func(bc *BranchContext) (any, error), opts *ParallelOptions) ([]any, error) {
	if opts != nil {
		return Parallel(s.ctx, name, branches, *opts)
	}
	return Parallel(s.ctx, name, branches)
}

// Compensate registers a compensation handler for a previously completed step.
func (s *StepClient) Compensate(stepName string, fn func() error) {
	Compensate(s.ctx, stepName, fn)
}

// Map executes a function for each item in parallel.
func (s *StepClient) Map(name string, items []any, fn func(item any, bc *BranchContext, index int) (any, error), opts *ParallelOptions) ([]any, error) {
	if opts != nil {
		return Map(s.ctx, name, items, fn, *opts)
	}
	return Map(s.ctx, name, items, fn)
}

// CreateHandler creates an event handler with a type-safe, simplified API.
//
// This is the recommended way to create handlers. It provides:
// - Type-safe event handling with Go generics
// - Auto-generated ID from event name
// - Progressive disclosure of advanced options
//
// Example:
//
//	type OrderEvent struct {
//	    OrderID string  `json:"orderId"`
//	    Total   float64 `json:"total"`
//	}
//
//	var ProcessOrder = ironflow.CreateHandler(ironflow.HandlerConfig[OrderEvent]{
//	    Event: "order.placed",
//	    Handler: func(event OrderEvent, ctx *ironflow.HandlerContext) (any, error) {
//	        result, err := ctx.Step.Run("process", func() (any, error) {
//	            return processOrder(event.OrderID, event.Total)
//	        })
//	        return result, err
//	    },
//	})
//
// With options:
//
//	var ProcessHighValueOrder = ironflow.CreateHandler(ironflow.HandlerConfig[OrderEvent]{
//	    Event: "order.placed",
//	    Handler: func(event OrderEvent, ctx *ironflow.HandlerContext) (any, error) {
//	        return processHighValueOrder(event)
//	    },
//	    Options: &ironflow.HandlerOptions{
//	        ID:     "high-value-order-handler",
//	        Filter: `data.total > 1000`,
//	        Retry:  &ironflow.RetryConfig{MaxAttempts: 5},
//	    },
//	})
func CreateHandler[TEvent any](config HandlerConfig[TEvent]) Function {
	// Generate ID from event name if not provided
	id := ""
	if config.Options != nil && config.Options.ID != "" {
		id = config.Options.ID
	} else {
		id = generateHandlerID(config.Event)
	}

	// Build trigger
	trigger := Trigger{
		Event: config.Event,
	}
	if config.Options != nil && config.Options.Filter != "" {
		trigger.Expression = config.Options.Filter
	}

	// Build function config
	fnConfig := FunctionConfig{
		ID:       id,
		Triggers: []Trigger{trigger},
	}

	if config.Options != nil {
		if config.Options.Name != "" {
			fnConfig.Name = config.Options.Name
		}
		if config.Options.Retry != nil {
			fnConfig.Retry = config.Options.Retry
		}
		if config.Options.Concurrency != nil {
			fnConfig.Concurrency = config.Options.Concurrency
		}
		if config.Options.Debounce != nil {
			fnConfig.Debounce = config.Options.Debounce
		}
		if config.Options.Timeout > 0 {
			fnConfig.Timeout = config.Options.Timeout
		}
		if config.Options.StepTimeout > 0 {
			fnConfig.StepTimeout = config.Options.StepTimeout
		}
		if config.Options.Mode != "" {
			fnConfig.Mode = config.Options.Mode
		}
		if config.Options.ActorKey != "" {
			fnConfig.ActorKey = config.Options.ActorKey
		}
		if len(config.Options.Secrets) > 0 {
			fnConfig.Secrets = config.Options.Secrets
		}
	}

	// Create adapter handler that converts context and unmarshals event
	handler := func(ctx Context) (any, error) {
		// Unmarshal event data to typed struct
		var event TEvent
		if err := json.Unmarshal(ctx.Event.RawData, &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal event data: %w", err)
		}

		// Build handler context
		handlerCtx := &HandlerContext{
			Event: event,
			EventMeta: HandlerEventMeta{
				ID:             ctx.Event.ID,
				Name:           ctx.Event.Name,
				Version:        ctx.Event.Version,
				Timestamp:      ctx.Event.Timestamp,
				IdempotencyKey: ctx.Event.IdempotencyKey,
				Source:         ctx.Event.Source,
				Metadata:       ctx.Event.Metadata,
			},
			Run:     ctx.Run,
			Secrets: ctx.Secrets,
			Step:    &StepClient{ctx: ctx},
			Logger:  NewLogger(LoggerConfig{Prefix: fmt.Sprintf("[%s]", id)}),
			ctx:     ctx,
		}

		// Call user handler
		return config.Handler(event, handlerCtx)
	}

	return Function{
		Config:  fnConfig,
		Handler: handler,
	}
}

// generateHandlerID creates a handler ID from an event name.
// "order.placed" -> "order-placed-handler"
// "order.*" -> "order-handler"
// "events.>" -> "events-handler"
func generateHandlerID(eventName string) string {
	// Remove wildcards
	name := strings.ReplaceAll(eventName, ".*", "")
	name = strings.ReplaceAll(name, ".>", "")
	name = strings.TrimSuffix(name, ".")

	// Replace dots with dashes
	name = strings.ReplaceAll(name, ".", "-")

	// Clean up any invalid characters
	reg := regexp.MustCompile(`[^a-zA-Z0-9-]`)
	name = reg.ReplaceAllString(name, "-")

	// Remove consecutive dashes
	reg = regexp.MustCompile(`-+`)
	name = reg.ReplaceAllString(name, "-")

	// Trim dashes from ends
	name = strings.Trim(name, "-")

	if name == "" {
		name = "handler"
	}

	return name + "-handler"
}
