# Ironflow Go SDK

Go SDK for [Ironflow](https://github.com/sahina/ironflow) -- an event-driven backend platform with durable workflows, pub/sub, event sourcing, and entity streams.

**Source mirror**: [`github.com/sahina/ironflow-go`](https://github.com/sahina/ironflow-go) (public, read-only). Engine repo is private.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Function Definition](#function-definition)
- [Step Primitives](#step-primitives)
- [Saga Compensation Pattern](#saga-compensation-pattern)
- [HTTP Handler (Push Mode)](#http-handler-push-mode)
- [Worker (Pull Mode)](#worker-pull-mode)
- [Client](#client)
- [Real-Time Subscriptions](#real-time-subscriptions)
- [Entity Streams (Event Sourcing)](#entity-streams-event-sourcing)
- [Projections](#projections)
- [KV Store](#kv-store)
- [Config Management](#config-management)
- [Auth Management](#auth-management)
- [Audit Trail](#audit-trail)
- [Webhooks](#webhooks)
- [Signature Verification](#signature-verification)
- [Event Versioning (Upcasters)](#event-versioning-upcasters)
- [Secrets](#secrets)
- [Testing (ironflowtest)](#testing-ironflowtest)
- [Error Handling](#error-handling)
- [Environment Variables](#environment-variables)
- [Constants](#constants)
- [Logging](#logging)

---

## Installation

```bash
go get github.com/sahina/ironflow-go/ironflow
```

Requires **Go 1.25+**.

---

## Quick Start

```go
package main

import (
    "context"
    "log"
    "os/signal"
    "syscall"

    "github.com/sahina/ironflow-go/ironflow"
)

type OrderData struct {
    OrderID string  `json:"orderId"`
    Email   string  `json:"email"`
    Amount  float64 `json:"amount"`
}

// Define a workflow function
var ProcessOrder = ironflow.CreateFunction(ironflow.FunctionConfig{
    ID:       "process-order",
    Triggers: []ironflow.Trigger{{Event: "order.placed"}},
}, func(ctx ironflow.Context) (any, error) {
    var order OrderData
    if err := ctx.Event.Data(&order); err != nil {
        return nil, err
    }

    // Each step is persisted -- failures resume from last successful step
    validated, err := ironflow.Run[map[string]any](ctx, "validate", func() (map[string]any, error) {
        return map[string]any{"valid": true, "orderId": order.OrderID}, nil
    })
    if err != nil {
        return nil, err
    }

    _, err = ironflow.Run[any](ctx, "notify", func() (any, error) {
        return map[string]any{"sent": true}, nil
    })
    if err != nil {
        return nil, err
    }

    return map[string]any{"status": "completed", "validated": validated}, nil
})

func main() {
    // Start as a worker (Pull mode -- no timeout limits)
    worker := ironflow.NewWorker(ironflow.WorkerConfig{
        Functions: []ironflow.Function{ProcessOrder},
    })

    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
    defer cancel()

    if err := worker.Run(ctx); err != nil {
        log.Fatal(err)
    }
}
```

---

## Function Definition

Use `CreateFunction` to define a workflow function. It panics if the config is invalid (catches misconfigurations at init time).

```go
var MyFunction = ironflow.CreateFunction(ironflow.FunctionConfig{
    // Required
    ID:       "my-function",                                // alphanumeric, hyphens, underscores
    Triggers: []ironflow.Trigger{{Event: "order.placed"}},  // optional — functions without triggers can be called via Invoke/InvokeAsync

    // Optional
    Name:     "My Function",          // display name (defaults to ID)
    Timeout:  5 * time.Minute,        // function timeout (default: 10m)
    StepTimeout: 30 * time.Second,    // default timeout for all steps (overridable per-step)
    Mode:     ironflow.PullMode,      // "push" or "pull" (default: "push")

    Retry: &ironflow.RetryConfig{
        MaxAttempts:   5,             // default: 3
        InitialDelay:  2 * time.Second, // default: 1s
        BackoffFactor: 2.0,           // default: 2.0
        MaxDelay:      10 * time.Minute, // default: 5m
    },

    Concurrency: &ironflow.ConcurrencyConfig{
        Limit: 10,                    // max concurrent executions
        Key:   "event.data.customerId", // grouping key (JSON path)
    },

    ActorKey: "event.data.userId",    // sticky routing JSON path
    Secrets:  []string{"STRIPE_KEY"}, // secrets injected at runtime
    Recording:          true,         // enable audit recording
    RecordingRetention: "30d",        // "7d", "30d", "90d", "forever"
}, func(ctx ironflow.Context) (any, error) {
    // ctx.Event  -- triggering event
    // ctx.Run    -- run metadata (ID, FunctionID, Attempt, StartedAt)
    // ctx.Secrets -- resolved secrets
    return nil, nil
})
```

### Trigger Configuration

```go
Triggers: []ironflow.Trigger{
    {Event: "order.placed"},                          // exact event name
    {Event: "order.*"},                               // wildcard
    {Event: "order.placed", Expression: "data.amount > 100"}, // CEL filter
}
```

### Context Fields

The `Context` passed to handlers provides:

| Field         | Type            | Description                          |
|---------------|-----------------|--------------------------------------|
| `ctx.Event`   | `Event`         | Triggering event (ID, Name, Version, RawData, Timestamp, Source, Metadata) |
| `ctx.Run`     | `RunInfo`       | Run metadata (ID, FunctionID, Attempt, StartedAt) |
| `ctx.Secrets` | `SecretsReader` | Resolved secrets (see [Secrets](#secrets)) |

Use `ctx.Event.Data(&target)` to unmarshal the event payload into a typed struct.

---

## Step Primitives

All steps are memoized. If a run is retried, previously completed steps return their cached result without re-executing.

### Run -- Memoized Step

```go
result, err := ironflow.Run[MyResult](ctx, "step-name", func() (MyResult, error) {
    return doWork()
})
```

With timeout (overrides function-level `StepTimeout`):

```go
result, err := ironflow.Run[MyResult](ctx, "slow-step", func() (MyResult, error) {
    return callExternalAPI()
}, ironflow.WithTimeout(30 * time.Second))
```

### Sleep -- Durable Pause

Survives restarts. Frees worker resources while waiting.

```go
err := ironflow.Sleep(ctx, "wait-24h", 24*time.Hour)
```

### SleepUntil -- Durable Pause Until Time

```go
midnight := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)
err := ironflow.SleepUntil(ctx, "wait-until-midnight", midnight)
```

### WaitForEvent -- Wait for External Event

Pauses durably until a matching event arrives. Default timeout: 7 days.

```go
event, err := ironflow.WaitForEvent[ApprovalEvent](ctx, "wait-approval", ironflow.EventFilter{
    Event:   "order.approved",          // event name to match
    Match:   "data.orderId",            // JSON path for correlation
    Timeout: 7 * 24 * time.Hour,        // default: 7 days
})
if err != nil {
    return nil, err
}

var approval ApprovalEvent
if err := event.Data(&approval); err != nil {
    return nil, err
}
```

### Invoke -- Synchronous Cross-Function Call

Calls another function and waits for its result. Default timeout: 30s.

```go
result, err := ironflow.Invoke[ShippingResult](ctx, "calculate-shipping", map[string]any{
    "items": order.Items,
    "address": order.Address,
})

// With custom timeout
result, err := ironflow.Invoke[ShippingResult](ctx, "calculate-shipping", input,
    ironflow.WithInvokeTimeout(60 * time.Second),
)
```

### InvokeAsync -- Fire-and-Forget

Returns immediately with the child run ID.

```go
result, err := ironflow.InvokeAsync(ctx, "send-notification", map[string]any{
    "userID": order.UserID,
    "message": "Order confirmed",
})
fmt.Println("Child run:", result.RunID)
```

### Parallel -- Concurrent Branch Execution

Execute multiple branches concurrently. Use `RunWithBranch` for steps inside branches.

```go
results, err := ironflow.Parallel(ctx, "checks", []func(*ironflow.BranchContext) (any, error){
    func(b *ironflow.BranchContext) (any, error) {
        return ironflow.RunWithBranch[any](b, "credit-check", func() (any, error) {
            return checkCredit(order.CustomerID)
        })
    },
    func(b *ironflow.BranchContext) (any, error) {
        return ironflow.RunWithBranch[any](b, "inventory-check", func() (any, error) {
            return checkInventory(order.Items)
        })
    },
})
```

With options:

```go
results, err := ironflow.Parallel(ctx, "checks", branches, ironflow.ParallelOptions{
    Concurrency: 5,           // limit concurrent branches (0 = unlimited)
    OnError:     "allSettled", // "failFast" (default) or "allSettled"
})
```

Branch-scoped variants available for all step types:
- `RunWithBranch[T](b, name, fn, opts...)`
- `SleepWithBranch(b, name, duration)`
- `WaitForEventWithBranch(b, name, filter)`
- `CompensateInBranch(b, stepName, fn)`
- `ParallelWithBranch[T](b, name, branches, opts...)` (nested parallel)

### Map -- Parallel Map Over Items

```go
results, err := ironflow.Map(ctx, "process-items", items,
    func(item Item, b *ironflow.BranchContext, index int) (Result, error) {
        return ironflow.RunWithBranch[Result](b, fmt.Sprintf("process-%d", index), func() (Result, error) {
            return processItem(item)
        })
    },
)
```

### Publish -- Pub/Sub from Workflow

Publishes to a developer topic as a durable, memoized step. Unlike `client.Publish()`, this is tracked in step history.

```go
err := ironflow.Publish(ctx, "order.processed", map[string]any{
    "orderId": order.OrderID,
    "total":   order.Total,
})
```

### Compensate -- Saga Rollback

Registers a compensation handler for a previously completed step. Compensations run in reverse order on terminal (non-retryable) failure.

```go
ironflow.Compensate(ctx, "step-name", func() error {
    return undoWork()
})
```

---

## Saga Compensation Pattern

Full example showing the saga pattern with multiple steps and compensations:

```go
var ProcessOrder = ironflow.CreateFunction(ironflow.FunctionConfig{
    ID:       "process-order",
    Triggers: []ironflow.Trigger{{Event: "order.placed"}},
}, func(ctx ironflow.Context) (any, error) {
    // Step 1: Charge payment
    payment, err := ironflow.Run[Payment](ctx, "charge-card", func() (Payment, error) {
        return chargeCard(order.CardID, order.Total)
    })
    if err != nil {
        return nil, err
    }
    // Register compensation: refund if later steps fail
    ironflow.Compensate(ctx, "charge-card", func() error {
        return refundPayment(payment.TransactionID)
    })

    // Step 2: Reserve inventory
    reservation, err := ironflow.Run[Reservation](ctx, "reserve-inventory", func() (Reservation, error) {
        return reserveItems(order.Items)
    })
    if err != nil {
        return nil, err // Triggers charge-card compensation
    }
    ironflow.Compensate(ctx, "reserve-inventory", func() error {
        return releaseReservation(reservation.ID)
    })

    // Step 3: Ship order (if this fails, both compensations run in reverse)
    _, err = ironflow.Run[any](ctx, "ship-order", func() (any, error) {
        return shipOrder(order)
    })
    if err != nil {
        return nil, ironflow.NewNonRetryableError("shipping failed: " + err.Error())
        // Compensations execute: reserve-inventory first, then charge-card
    }

    return map[string]any{"status": "shipped"}, nil
})
```

**Key behavior:**
- Compensations only run on **non-retryable** (terminal) errors.
- Compensations execute in **reverse registration order**.
- Each compensation is recorded as a durable step (`compensate:step-name`).
- Compensation failures are recorded but do not stop remaining compensations.

---

## HTTP Handler (Push Mode)

For serverless/HTTP deployments (Next.js API routes, AWS Lambda, etc.). Functions must complete within HTTP timeout limits.

```go
handler := ironflow.Serve(ironflow.ServeConfig{
    // Required
    Functions: []ironflow.Function{ProcessOrder, SendNotification},

    // Optional
    SigningKey:        os.Getenv("IRONFLOW_SIGNING_KEY"), // verify request signatures
    SkipVerification: false,   // skip signature check (dev only)
    ServerURL:        "http://localhost:9123", // for webhook event emission
    Webhooks:         []ironflow.Webhook{stripeWebhook}, // webhook sources
    Projections:      []ironflow.Projection{orderTotals}, // register projections
    Upcasters:        registry, // event schema upcasting
})

http.Handle("/api/ironflow", handler)
http.ListenAndServe(":3000", nil)
```

The handler accepts POST requests. The Ironflow server sends `PushRequest` payloads; the handler returns `PushResponse`.

Webhook requests are routed to `/webhooks/{provider}` paths under the same handler.

---

## Worker (Pull Mode)

For long-running tasks with no timeout limits. Workers poll the server for jobs via HTTP.

```go
worker := ironflow.NewWorker(ironflow.WorkerConfig{
    // Required
    Functions: []ironflow.Function{GenerateVideo, ProcessImage},

    // Optional
    ServerURL:         "http://localhost:9123", // default: GetServerURL()
    APIKey:            "ifkey_...",                // default: IRONFLOW_API_KEY env
    MaxConcurrentJobs: 4,                       // default: 10
    HeartbeatInterval: 30 * time.Second,        // default: 30s
    ReconnectDelay:    5 * time.Second,          // default: 5s
    Labels:            map[string]string{"gpu": "nvidia-a100"}, // routing labels
    Projections:       []ironflow.Projection{orderTotals},      // run alongside functions
    Upcasters:         registry,                // event schema upcasting
    Logger:            ironflow.NewNoopLogger(), // disable logging
})
```

### Worker Lifecycle

```go
ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
defer cancel()

if err := worker.Run(ctx); err != nil {
    log.Fatal(err)
}
```

- `Run(ctx)` -- starts the worker, blocks until stopped. Auto-reconnects on connection loss.
- `Drain()` -- gracefully stops: finishes active jobs, then stops. Blocks until drained.
- `Stop()` -- immediately stops: cancels active jobs and projection runners.

---

## Client

The `Client` is used for server interaction: emitting events, managing runs, pub/sub, entity streams, auth, and more.

```go
client := ironflow.NewClient(ironflow.ClientConfig{
    ServerURL: ironflow.GetServerURL(),     // default: http://localhost:9123
    APIKey:    "ifkey_...",                     // optional for local dev
    Timeout:   30 * time.Second,            // default: 30s
    HTTPClient: &http.Client{},             // optional custom HTTP client

    Retry: &ironflow.ClientRetryConfig{
        MaxAttempts:          3,               // default: 3 (set to 0 to disable)
        InitialDelay:         100 * time.Millisecond, // default: 100ms
        MaxDelay:             10 * time.Second,       // default: 10s
        BackoffMultiplier:    2.0,                    // default: 2.0
        ConnectionRetryDelay: 2 * time.Second,        // fixed delay for conn errors
        OnRetry: func(event ironflow.RetryEvent) {
            log.Printf("Retry %d/%d: %v", event.Attempt, event.MaxAttempts, event.Error)
        },
    },

    Logger: ironflow.NewNoopLogger(), // disable logging
})
```

### Emit Events

```go
// Async emit (preferred for production)
result, err := client.Emit(ctx, "order.placed", map[string]any{
    "orderId": "123",
    "amount":  99.99,
})
fmt.Println("Event ID:", result.EventID, "Run IDs:", result.RunIDs)

// With options
result, err := client.Emit(ctx, "order.placed", data,
    ironflow.WithEmitIdempotencyKey("order-123"),
    ironflow.WithEmitVersion(2),
    ironflow.WithEmitMetadata(map[string]any{"source": "api"}),
    ironflow.WithEmitNamespace("production"),
)

// Sync emit (waits for completion -- useful for testing)
syncResult, err := client.EmitSync(ctx, "order.placed", data, 30*time.Second)
if syncResult.Status == ironflow.RunStatusCompleted {
    fmt.Printf("Output: %v\n", syncResult.Output)
}
```

### Run Management

```go
// Get a run
run, err := client.GetRun(ctx, runID)
fmt.Println(run.Status, run.Output)

// List runs with filtering
result, err := client.ListRuns(ctx, &ironflow.ListRunsOptions{
    FunctionID: "process-order",
    Status:     ironflow.RunStatusFailed,
    Limit:      50,
    Cursor:     "next-page-cursor",
})
for _, run := range result.Runs {
    fmt.Printf("%s: %s\n", run.ID, run.Status)
}

// Cancel a run
run, err := client.CancelRun(ctx, runID, "no longer needed")

// Retry a failed run (from beginning or specific step)
run, err := client.RetryRun(ctx, runID, "")         // from beginning
run, err := client.RetryRun(ctx, runID, "validate")  // from specific step

// Patch a step's output (hot patching)
err := client.PatchStep(ctx, stepID, map[string]any{"corrected": true}, "fix data")
```

### Scoped Injection

Pause running workflows at step boundaries, inspect and modify step outputs, then resume:

```go
// Pause a running workflow
status, err := client.PauseRun(ctx, "run_abc123")

// Get paused state
state, err := client.GetPausedState(ctx, "run_abc123")
for _, step := range state.Steps {
    fmt.Printf("Step %s: injected=%v\n", step.Name, step.Injected)
}

// Inject modified output
prevOutput, err := client.InjectStepOutput(ctx, "run_abc123", "step_xyz",
    json.RawMessage(`{"corrected": true}`), "Manual fix")

// Resume (fromStep="" to resume from where it paused)
run, err := client.ResumeRun(ctx, "run_abc123", "")
```

### Developer Pub/Sub

```go
// Publish to a topic (does NOT trigger workflow functions)
result, err := client.Publish(ctx, "notifications.email", map[string]any{
    "to":      "user@example.com",
    "subject": "Hello",
})
fmt.Println("Sequence:", result.Sequence)

// With idempotency
result, err := client.Publish(ctx, "notifications.email", data,
    ironflow.WithPublishIdempotencyKey("email-abc"),
)

// List topics
topics, err := client.ListTopics(ctx)
for _, t := range topics {
    fmt.Printf("Topic: %s (%d messages)\n", t.Name, t.MessageCount)
}

// Get topic statistics
stats, err := client.GetTopicStats(ctx, "notifications.email")
fmt.Printf("Messages: %d, Lag: %d\n", stats.MessageCount, stats.Lag)
```

### Server Inspection

```go
// List registered functions
functions, err := client.ListFunctions(ctx)

// List connected workers
workers, err := client.ListWorkers(ctx)

// Health check
status, err := client.Health(ctx) // returns "healthy"

// Server capabilities (transports, features, version)
caps, err := client.GetCapabilities(ctx)
fmt.Println(caps.Transports, caps.Features, caps.Version)

// Auto-detect best subscription transport
transport, err := client.DetectTransport(ctx) // "grpc" or "websocket"
```

### Consumer Groups

```go
// Create a consumer group
group, err := client.CreateConsumerGroup(ctx, ironflow.ConsumerGroupConfig{
    Name:             "order-processors",
    Pattern:          "order.*",
    AckMode:          ironflow.AckModeManual,
    MaxInflight:      50,
    MaxRedeliveries:  3,
    RedeliverDelayMs: 5000,
})

// Join a consumer group (auto-detects transport)
sub, err := client.JoinConsumerGroup(ctx, "order-processors")
defer sub.Unsubscribe()

for event := range sub.Events() {
    if err := processOrder(event); err != nil {
        sub.Nak(event.ID, 10*time.Second)
        continue
    }
    sub.Ack(event.ID)
}

// List/Get/Delete consumer groups
groups, err := client.ListConsumerGroups(ctx)
group, err := client.GetConsumerGroup(ctx, "order-processors")
err := client.DeleteConsumerGroup(ctx, "order-processors")
```

---

## Real-Time Subscriptions

### Subscription Patterns

Use the `Patterns` helper for building NATS-style patterns:

```go
ironflow.Patterns.AllRuns()                // "system.run.>"
ironflow.Patterns.Run("abc123")            // "system.run.abc123.>"
ironflow.Patterns.RunLifecycle("abc123")   // "system.run.abc123.*"
ironflow.Patterns.RunSteps("abc123")       // "system.run.abc123.step.>"
ironflow.Patterns.AllFunctions()           // "system.function.>"
ironflow.Patterns.Function("process-order") // "system.function.process-order.>"
ironflow.Patterns.UserEvent("order.*")     // "events:order.*"
ironflow.Patterns.AllUserEvents()          // "events:>"
ironflow.Patterns.AllSecrets()             // "system.secret.*"
ironflow.Patterns.Secret("API_KEY")        // "system.secret.API_KEY.*"
ironflow.Patterns.SecretAction("updated")  // "system.secret.*.updated"
ironflow.Patterns.Topic("notifications.>") // "topic:notifications.>"
ironflow.Patterns.AllTopics()              // "topic:>"
```

### WebSocket Subscriptions

```go
subClient := client.CreateSubscriptionClient()
if err := subClient.Connect(ctx); err != nil {
    log.Fatal(err)
}
defer subClient.Close()

sub, err := subClient.Subscribe(ctx, "events:order.*", &ironflow.SubscribeOptions{
    Replay:          10,                // replay last 10 events
    IncludeMetadata: true,              // include timestamp/sequence
    Filter:          `data.total > 50`, // CEL filter
    Namespace:       "default",         // event namespace
})
if err != nil {
    log.Fatal(err)
}
defer sub.Unsubscribe()

for {
    select {
    case event := <-sub.Events():
        fmt.Printf("Event: %s on %s\n", event.ID, event.Topic)
    case err := <-sub.Errors():
        fmt.Printf("Error: %s\n", err.Message)
    case <-ctx.Done():
        return
    }
}
```

You can also create a standalone `SubscriptionClient`:

```go
subClient := ironflow.NewSubscriptionClient(ironflow.SubscriptionClientConfig{
    WSURL:             "ws://localhost:9123/ws",
    AutoReconnect:     true,              // default: true
    ReconnectDelay:    time.Second,       // default: 1s
    MaxReconnectDelay: 30 * time.Second,  // default: 30s
    ReconnectBackoff:  1.5,               // default: 1.5
})

subClient.SetConnectionCallback(func(connected bool) {
    fmt.Println("Connected:", connected)
})
```

### gRPC (HTTP Streaming) Subscriptions

```go
grpcClient := client.CreateGrpcSubscriptionClient()
if err := grpcClient.Connect(ctx); err != nil {
    log.Fatal(err)
}
defer grpcClient.Close()

sub, err := grpcClient.Subscribe(ctx, "events:order.*", &ironflow.SubscribeOptions{
    Replay: 10,
    Filter: `data.total > 100`,
})
if err != nil {
    log.Fatal(err)
}
defer sub.Unsubscribe()

for event := range sub.Events() {
    fmt.Printf("Event: %s\n", event.Topic)
}
```

Or standalone:

```go
grpcClient := ironflow.NewGrpcSubscriptionClient(ironflow.GrpcSubscriptionClientConfig{
    ServerURL:     "http://localhost:9123",
    AutoReconnect: true,
})
```

### Consumer Groups with Manual Ack

**WebSocket:**

```go
ackSub, err := subClient.SubscribeAckable(ctx, "events:order.*", &ironflow.SubscribeOptions{
    ConsumerGroup: "order-processors",
})
defer ackSub.Unsubscribe()

for event := range ackSub.Events() {
    if err := processEvent(event); err != nil {
        ackSub.Nak(event.ID, 5*time.Second) // retry after delay
        continue
    }
    ackSub.Ack(event.ID) // success
    // ackSub.Term(event.ID) // permanently reject (no redelivery)
}
```

**gRPC:**

```go
ackSub, err := grpcClient.SubscribeAckable(ctx, "events:order.*", &ironflow.SubscribeOptions{
    ConsumerGroup: "order-processors",
})
```

---

## Entity Streams (Event Sourcing)

Entity streams store domain events per entity with optimistic concurrency control.

```go
// Append an event to an entity stream
result, err := client.AppendStreamEvent(ctx, "order-123", ironflow.AppendEventInput{
    Name:       "item.added",
    Data:       map[string]any{"sku": "WIDGET-1", "qty": 2},
    EntityType: "order",
})
fmt.Println("Version:", result.EntityVersion, "Event ID:", result.EventID)

// With optimistic concurrency (fails if version doesn't match)
result, err := client.AppendStreamEvent(ctx, "order-123", ironflow.AppendEventInput{
    Name:       "item.removed",
    Data:       map[string]any{"sku": "WIDGET-1"},
    EntityType: "order",
},
    ironflow.WithExpectedVersion(3),
    ironflow.WithAppendIdempotencyKey("remove-widget-1"),
    ironflow.WithEventVersion(2), // event schema version
)

// Read events from a stream
events, err := client.ReadStream(ctx, "order-123")
for _, e := range events {
    fmt.Printf("v%d: %s %v\n", e.EntityVersion, e.Name, e.Data)
}

// With read options
events, err := client.ReadStream(ctx, "order-123", ironflow.ReadStreamOpts{
    FromVersion: 5,
    Limit:       10,
    Direction:   "backward", // "forward" (default) or "backward"
})

// Get stream metadata
info, err := client.GetStreamInfo(ctx, "order-123")
fmt.Printf("Entity %s at version %d (%d events)\n", info.EntityID, info.Version, info.EventCount)

// Subscribe to entity stream updates (real-time)
sub, err := subClient.SubscribeEntityStream(ctx, "order-123", ironflow.EntitySubscribeOptions{
    EntityType: "order",
    Replay:     100, // replay last 100 events
    OnEvent: func(e ironflow.StreamEvent) {
        fmt.Printf("Entity event: %s v%d\n", e.Name, e.EntityVersion)
    },
    OnError: func(err error) {
        fmt.Printf("Error: %v\n", err)
    },
})
defer sub.Unsubscribe()
```

---

## Projections

Projections build read models from event streams. Two modes:
- **Managed**: Pure reducer. Maintains state via `InitialState` + handler.
- **External**: Side effects only (send email, call API). No maintained state.

```go
// Managed projection (state is maintained automatically)
var orderTotals = ironflow.CreateProjection(ironflow.ProjectionConfig{
    Name:   "order-totals",
    Events: []string{"order.created", "order.updated"},
    // Mode auto-detected from InitialState (managed if set, external if nil)
    InitialState: func() map[string]any {
        return map[string]any{"total": 0.0, "count": 0}
    },
    Handler: func(state map[string]any, event ironflow.ProjectionEvent, ctx ironflow.ProjectionContext) (map[string]any, error) {
        if event.Name == "order.created" {
            amount, _ := event.Data["amount"].(float64)
            count, _ := state["count"].(float64)
            total, _ := state["total"].(float64)
            return map[string]any{"total": total + amount, "count": count + 1}, nil
        }
        return state, nil
    },
    PartitionKey: "$.data.customerId", // optional: per-partition state
    MaxRetries:   3,                    // default: 3
    BatchSize:    100,                  // default: 100
})

// External projection (side effects, no maintained state)
var emailNotifier = ironflow.CreateProjection(ironflow.ProjectionConfig{
    Name:   "email-notifier",
    Events: []string{"order.completed"},
    // No InitialState = external mode
    Handler: func(state map[string]any, event ironflow.ProjectionEvent, ctx ironflow.ProjectionContext) (map[string]any, error) {
        sendEmail(event.Data["email"].(string), "Order complete!")
        return nil, nil
    },
})

// Run projections alongside functions in a worker
worker := ironflow.NewWorker(ironflow.WorkerConfig{
    Functions:   []ironflow.Function{ProcessOrder},
    Projections: []ironflow.Projection{orderTotals, emailNotifier},
})
```

### Determinism & Idempotence (managed mode)

Managed reducers run under at-least-once delivery. PG-backed rebuild (#486) and the live NATS tail can both invoke the handler for the same event during the overlap window; node failover and retries can replay events at any time. **Correctness depends on the reducer.** Four rules:

- **Deterministic** — same `(state, event)` → same `newState`. No `time.Now()`, `rand.Int*`, `uuid.New()`, `os.Getenv`, file reads. Derive timestamps from `event.Timestamp` and IDs from `event.Data`.
- **Pure** — no network, no DB writes. Side effects require mode `"external"`.
- **Aliasing-safe** — return a fresh `map[string]any`. The Go projection runner deep-copies state via JSON before each invocation (#486 I3) so accidental in-place mutation cannot leak across iterations, but mutating-then-returning is still the wrong pattern.
- **Idempotent** — the same event may be applied multiple times. Prefer keyed-map accumulation (`state["byID"].(map[string]any)[id] = ...`) over counters; key accumulators on `event.ID` when you must accumulate.

See [`docs/explanation/projections.md`](../../../docs/explanation/projections.md#reducer-contract-managed-mode) for examples and rationale.

---

## KV Store

Distributed key-value store backed by NATS JetStream.

```go
kv := client.KV()

// Bucket management
info, err := kv.CreateBucket(ctx, ironflow.BucketConfig{
    Name:         "sessions",
    Description:  "User sessions",
    TTL:          time.Hour,        // auto-expire keys
    MaxValueSize: 1024 * 1024,      // 1MB max value
    MaxBytes:     100 * 1024 * 1024, // 100MB max bucket
    History:      5,                 // keep 5 versions per key
})

buckets, err := kv.ListBuckets(ctx)
bucketInfo, err := kv.GetBucketInfo(ctx, "sessions")
err = kv.DeleteBucket(ctx, "sessions")

// Key operations
bucket := kv.Bucket("sessions")

// Put (unconditional write)
revision, err := bucket.Put(ctx, "user:123", []byte(`{"name":"Alice"}`))

// Get
entry, err := bucket.Get(ctx, "user:123")
fmt.Println(string(entry.Value), entry.Revision) // {"name":"Alice"} 1

// Create (if-not-exists, fails with HTTP 412 if key exists)
revision, err := bucket.Create(ctx, "user:456", []byte(`{"name":"Bob"}`))

// Update (compare-and-swap, fails with HTTP 412 if revision mismatch)
revision, err := bucket.Update(ctx, "user:123",
    []byte(`{"name":"Alice","role":"admin"}`),
    entry.Revision, // must match current revision
)

// List keys (supports wildcard filter)
keys, err := bucket.ListKeys(ctx, "user:*")

// Delete (soft-delete, creates tombstone)
err = bucket.Delete(ctx, "user:456")

// Purge (hard delete, removes all history)
err = bucket.Purge(ctx, "user:456")
```

---

## Config Management

Server-side configuration store with revision tracking.

```go
config := client.Config()

// Set (full document replacement)
result, err := config.Set(ctx, "app-settings", map[string]any{
    "theme":      "dark",
    "maxRetries": 3,
})
fmt.Println("Revision:", result.Revision)

// Get
entry, err := config.Get(ctx, "app-settings")
fmt.Println(entry.Data)     // map[maxRetries:3 theme:dark]
fmt.Println(entry.Revision) // 1

// Patch (shallow merge)
result, err = config.Patch(ctx, "app-settings", map[string]any{
    "maxRetries": 5, // only updates this field
})

// List all configs (names and revisions only, no data)
all, err := config.List(ctx)
for _, c := range all {
    fmt.Printf("%s (rev %d)\n", c.Name, c.Revision)
}

// Delete
err = config.Delete(ctx, "old-config")
```

---

## Auth Management

### API Keys

```go
// Create
key, err := client.CreateAPIKey(ctx, ironflow.CreateAPIKeyInput{
    Name:      "ci-key",
    EnvID:     "env_default",          // optional: scope to environment
    RoleIDs:   []string{"role_admin"}, // optional: assign roles
    ExpiresIn: "90d",                  // optional: expiration
})
fmt.Println("Key:", key.Key) // only available on create

// List
keys, err := client.ListAPIKeys(ctx)

// Get
keyInfo, err := client.GetAPIKey(ctx, key.ID)

// Rotate (creates new key, deletes old)
rotated, err := client.RotateAPIKey(ctx, key.ID)

// Delete
err = client.DeleteAPIKey(ctx, key.ID)
```

### Organizations

```go
org, err := client.CreateOrganization(ctx, ironflow.CreateOrgInput{Name: "Acme Corp"})
orgs, err := client.ListOrganizations(ctx)
org, err = client.GetOrganization(ctx, org.ID)
org, err = client.UpdateOrganization(ctx, org.ID, ironflow.UpdateOrgInput{Name: "Acme Inc"})
err = client.DeleteOrganization(ctx, org.ID)
```

### Roles

```go
role, err := client.CreateRole(ctx, ironflow.CreateRoleInput{
    Name:  "editor",
    OrgID: org.ID, // optional
})
roles, err := client.ListRoles(ctx)
role, err = client.GetRole(ctx, role.ID)
role, err = client.UpdateRole(ctx, role.ID, ironflow.UpdateRoleInput{Name: "content-editor"})
err = client.DeleteRole(ctx, role.ID)

// Assign/remove policies
err = client.AssignPolicyToRole(ctx, role.ID, policyID)
err = client.RemovePolicyFromRole(ctx, role.ID, policyID)
```

### Policies

```go
policy, err := client.CreatePolicy(ctx, ironflow.CreatePolicyInput{
    Name:      "read-only",
    Effect:    "deny",        // writes accept "deny" only (#943, ADR 0016 T2)
    Actions:   "read",         // comma-separated actions
    Resources: "*",            // resource patterns
    Condition: "",             // optional CEL expression
    OrgID:     org.ID,         // optional
})
policies, err := client.ListPolicies(ctx)
policy, err = client.GetPolicy(ctx, policy.ID)
policy, err = client.UpdatePolicy(ctx, policy.ID, ironflow.UpdatePolicyInput{
    Actions: "read,write",
})
err = client.DeletePolicy(ctx, policy.ID)
```

---

## Audit Trail

```go
result, err := client.GetAuditTrail(ctx, runID)
for _, event := range result.Events {
    fmt.Printf("%s: %s (%s)\n", event.CreatedAt, event.EventType, event.FunctionID)
}

// With filtering
result, err := client.GetAuditTrail(ctx, runID, ironflow.GetAuditTrailOpts{
    EventType:     "step.completed",
    FromTimestamp: "2025-01-01T00:00:00Z",
    ToTimestamp:   "2025-12-31T23:59:59Z",
    Limit:         50,
    Cursor:        "next-page-cursor",
})
fmt.Println("Total:", result.TotalCount, "Next:", result.NextCursor)
```

---

## Webhooks

Define webhook sources with verification and transformation. Webhook requests are handled at `/webhooks/{provider}` under the Serve handler.

```go
stripeWebhook := ironflow.CreateWebhook(ironflow.WebhookConfig{
    ID: "stripe",

    // Verify the request signature (optional, return nil to skip)
    Verify: func(req *ironflow.WebhookRequest) error {
        return verifyStripeSignature(req.Body, req.Header.Get("Stripe-Signature"))
    },

    // Transform raw payload into an Ironflow event
    Transform: func(payload []byte) (*ironflow.WebhookEvent, error) {
        var p map[string]any
        json.Unmarshal(payload, &p)
        eventType, _ := p["type"].(string)

        dataBytes, _ := json.Marshal(p["data"])
        return &ironflow.WebhookEvent{
            Name:           "stripe." + eventType,
            Data:           dataBytes,
            IdempotencyKey: p["id"].(string), // deduplication
        }, nil
    },
})

handler := ironflow.Serve(ironflow.ServeConfig{
    Functions: []ironflow.Function{ProcessOrder},
    Webhooks:  []ironflow.Webhook{stripeWebhook},
    ServerURL: "http://localhost:9123", // events are emitted to this server
})
```

The `WebhookRequest` provides:

| Field    | Type          | Description          |
|----------|---------------|----------------------|
| `Body`   | `[]byte`      | Raw request body     |
| `Header` | `http.Header` | HTTP headers         |
| `Method` | `string`      | HTTP method          |
| `URL`    | `string`      | Request URL          |

---

## Signature Verification

HMAC-SHA256 signatures for verifying webhook payloads from the Ironflow server.

```go
// Sign a payload (generates "t=<timestamp>,v1=<signature>")
signature := ironflow.SignPayload(payload, secret)

// Verify a signature (returns nil if valid)
err := ironflow.VerifySignature(payload, signature, secret, ironflow.DefaultSignatureTolerance)

// Quick boolean check
valid := ironflow.IsValidSignature(payload, signature, secret, ironflow.DefaultSignatureTolerance)

// Parse a signature header
params, err := ironflow.ParseSignature(signatureHeader)
fmt.Println(params.Timestamp, params.Signatures["v1"])

// Compute a signature manually
sig := ironflow.ComputeSignature(payload, secret, timestamp)
```

`DefaultSignatureTolerance` is 5 minutes.

---

## Event Versioning (Upcasters)

Transform events from older schema versions to newer ones. Upcasters form a chain: v1 -> v2 -> v3.

```go
registry := ironflow.NewUpcasterRegistry()

// Register: eventName, fromVersion, toVersion, transform function
registry.Register("order.placed", 1, 2, func(data json.RawMessage) (json.RawMessage, error) {
    var m map[string]any
    json.Unmarshal(data, &m)
    if _, ok := m["currency"]; !ok {
        m["currency"] = "USD" // add default currency field
    }
    return json.Marshal(m)
})

registry.Register("order.placed", 2, 3, func(data json.RawMessage) (json.RawMessage, error) {
    var m map[string]any
    json.Unmarshal(data, &m)
    // Rename "amount" to "total"
    if amt, ok := m["amount"]; ok {
        m["total"] = amt
        delete(m, "amount")
    }
    return json.Marshal(m)
})

// Manual usage
upcasted, err := registry.Upcast("order.placed", rawData, 1, 3)  // v1 -> v3
upcasted, err = registry.UpcastToLatest("order.placed", rawData, 1) // v1 -> latest
latest := registry.LatestVersion("order.placed") // 3

// Use with worker or serve (auto-upcasts before passing to handlers)
worker := ironflow.NewWorker(ironflow.WorkerConfig{
    Functions: []ironflow.Function{ProcessOrder},
    Upcasters: registry,
})
```

The chain must be **complete** -- if v2->v3 is registered but not v1->v2, upcasting from v1 fails.

---

## Secrets

Functions can declare required secrets in `FunctionConfig.Secrets`. The Ironflow engine resolves them at execution time and injects them into the context.

```go
var MyFunction = ironflow.CreateFunction(ironflow.FunctionConfig{
    ID:       "my-function",
    Triggers: []ironflow.Trigger{{Event: "payment.process"}},
    Secrets:  []string{"STRIPE_KEY", "WEBHOOK_SECRET"},
}, func(ctx ironflow.Context) (any, error) {
    // Check if a secret exists
    if ctx.Secrets.Has("STRIPE_KEY") {
        // Get a secret value
        var stripeKey string
        if err := ctx.Secrets.Get("STRIPE_KEY", &stripeKey); err != nil {
            return nil, err // "secret "STRIPE_KEY" not found"
        }
        // Use stripeKey...
    }
    return nil, nil
})
```

`SecretsReader` API:

| Method                           | Description                        |
|----------------------------------|------------------------------------|
| `Get(name string, dest *string)` | Get secret value. Error if missing |
| `Has(name string) bool`          | Check if secret exists             |

---

## Testing (ironflowtest)

Unit test workflow functions without a running server using the `ironflowtest` package.

```go
import (
    "testing"
    "github.com/sahina/ironflow-go/ironflow/ironflowtest"
)

func TestProcessOrder(t *testing.T) {
    tc := ironflowtest.NewClient(t, ironflowtest.Config{
        Functions: []ironflow.Function{ProcessOrder},
    })

    // Mock step.Run() calls by step name
    tc.MockStep("validate", func() (any, error) {
        return map[string]any{"valid": true}, nil
    })
    tc.MockStep("charge-card", func() (any, error) {
        return map[string]any{"chargeId": "ch_123"}, nil
    })

    // Mock step.Invoke() calls by function ID
    tc.MockInvoke("payment-service", func(input any) (any, error) {
        return map[string]any{"txId": "tx_123"}, nil
    })

    // Pre-register events for WaitForEvent steps
    tc.SendEvent("order.approved", map[string]any{"approved": true})

    // Execute the function
    run := tc.Emit(t, "order.placed", map[string]any{"orderId": "123"})

    // Assert results
    if run.Status != "completed" {
        t.Fatalf("expected completed, got %s: %v", run.Status, run.Error)
    }

    // Check function output
    output := run.Output.(map[string]any)

    // Check individual step outputs
    stepOut := run.StepOutput("validate")

    // Check all executed steps
    for _, step := range run.Steps {
        fmt.Printf("Step: %s (%s) -> %v\n", step.Name, step.Type, step.Output)
    }

    // Check compensations ran (on failure)
    if len(run.CompensationsRan) > 0 {
        fmt.Println("Compensations:", run.CompensationsRan)
    }
}
```

### Test Behavior

| Behavior | Description |
|----------|-------------|
| **Sleep** | Resolves immediately (no actual waiting) |
| **WaitForEvent** | Consumes from pre-registered queue (`SendEvent`). Fails if no event registered. |
| **Invoke** | Uses `MockInvoke` handler. Fails if no mock registered. |
| **InvokeAsync** | Uses `MockInvoke` handler, returns synthetic run ID. |
| **Run** | Uses `MockStep` handler. Fails if no mock registered. |
| **Compensate** | Tracked. On failure, compensations run in reverse. `run.CompensationsRan` lists them. |
| **Parallel** | Each branch's `RunWithBranch` steps use `MockStep`. |

### TestRun Fields

| Field              | Type         | Description                                      |
|--------------------|-------------|--------------------------------------------------|
| `Status`           | `string`    | `"completed"` or `"failed"`                      |
| `Output`           | `any`       | Function return value                            |
| `Error`            | `error`     | Function error (nil on success)                  |
| `Steps`            | `[]TestStep`| All executed steps in order                      |
| `CompensationsRan` | `[]string`  | Compensation step names in execution order       |

### TestStep Fields

| Field    | Type     | Description                                           |
|----------|---------|-------------------------------------------------------|
| `Name`   | `string`| Step name                                             |
| `Type`   | `string`| `"run"`, `"invoke"`, `"sleep"`, `"waitForEvent"`, `"compensate"` |
| `Output` | `any`   | Step output                                           |
| `Error`  | `error` | Step error                                            |

---

## Error Handling

### Sentinel Errors

```go
import "errors"

errors.Is(err, ironflow.ErrFunctionNotFound)
errors.Is(err, ironflow.ErrRunNotFound)
errors.Is(err, ironflow.ErrTimeout)
errors.Is(err, ironflow.ErrUnauthorized)           // HTTP 401
errors.Is(err, ironflow.ErrForbidden)               // HTTP 403
errors.Is(err, ironflow.ErrEnterpriseLicenseRequired) // HTTP 402
errors.Is(err, ironflow.ErrInvalidSignature)
errors.Is(err, ironflow.ErrSignatureExpired)
errors.Is(err, ironflow.ErrMissingSignature)
errors.Is(err, ironflow.ErrValidation)
```

### Retryability

```go
// Check if an error is retryable (unknown errors default to retryable)
if ironflow.IsRetryable(err) {
    // Safe to retry
}

// Mark an error as non-retryable (triggers compensations)
return nil, ironflow.NewNonRetryableError("invalid input: missing field")

// Wrap an existing error as non-retryable
return nil, ironflow.WrapNonRetryable(err)
```

### Error Types

| Type                  | Description                                    |
|-----------------------|------------------------------------------------|
| `*IronflowError`      | Base error with Message, Code, Retryable, Cause, Details, RetryAfter |
| `*StepError`          | Step failure with StepID, StepName             |
| `*StepTimeoutError`   | Step timeout with StepName, Timeout            |
| `*NonRetryableError`  | Wraps error as non-retryable                   |
| `*InvokeError`        | Invoke failure with FunctionID, ChildRunID, Cause |
| `*SubscriptionError`  | Subscription error with Code, Message, Retrying |

### Creating Errors

```go
// General error
err := ironflow.NewError("something failed", "MY_CODE", true)

// Wrap an existing error
err := ironflow.WrapError(cause, "context message", "MY_CODE", true)

// Step error
err := ironflow.NewStepError("step failed", stepID, stepName, true, cause)

// Step timeout error
err := ironflow.NewStepTimeoutError("slow-step", 30*time.Second)
```

---

## Environment Variables

| Variable                | Description                     | Default                    |
|-------------------------|---------------------------------|----------------------------|
| `IRONFLOW_SERVER_URL`   | Server URL                      | `http://localhost:9123`    |
| `IRONFLOW_SIGNING_KEY`  | Webhook signing secret          | (none)                     |
| `IRONFLOW_API_KEY`      | API key for authentication      | (none)                     |
| `IRONFLOW_LOG_LEVEL`    | Log level: debug, info, warn, error, silent | info           |

Helper functions:

```go
url := ironflow.GetServerURL()      // IRONFLOW_SERVER_URL or default
key := ironflow.GetSigningKey()      // IRONFLOW_SIGNING_KEY
apiKey := ironflow.GetAPIKey()       // IRONFLOW_API_KEY
wsURL := ironflow.GetWebSocketURL("") // converts server URL to ws:// + /ws
```

---

## Constants

```go
ironflow.DefaultServerURL     // "http://localhost:9123"
ironflow.DefaultWebSocketURL  // "ws://localhost:9123/ws"
ironflow.DefaultPort          // 9123
ironflow.DefaultHost          // "localhost"

// Timeouts
ironflow.DefaultClientTimeout       // 30s
ironflow.DefaultFunctionTimeout     // 10m
ironflow.DefaultEmitSyncTimeout     // 30s
ironflow.DefaultSignatureTolerance  // 5m

// Function retry defaults
ironflow.DefaultRetryMaxAttempts    // 3
ironflow.DefaultRetryInitialDelay   // 1s
ironflow.DefaultRetryBackoffFactor  // 2.0
ironflow.DefaultRetryMaxDelay       // 5m

// Client retry defaults
ironflow.DefaultClientRetryMaxAttempts       // 3
ironflow.DefaultClientRetryInitialDelay      // 100ms
ironflow.DefaultClientRetryMaxDelay          // 10s
ironflow.DefaultClientRetryBackoffMultiplier // 2.0
ironflow.DefaultClientRetryConnectionDelay   // 2s

// Worker defaults
ironflow.DefaultWorkerMaxConcurrentJobs  // 10
ironflow.DefaultWorkerHeartbeatInterval  // 30s
ironflow.DefaultWorkerReconnectDelay     // 5s

// Execution modes
ironflow.PushMode  // "push"
ironflow.PullMode  // "pull"

// Run statuses
ironflow.RunStatusPending    // "pending" (deprecated: the engine no longer produces this as of #1222)
ironflow.RunStatusRunning    // "running"
ironflow.RunStatusCompleted  // "completed"
ironflow.RunStatusFailed     // "failed"
ironflow.RunStatusCancelled  // "cancelled"
ironflow.RunStatusPaused     // "paused"
// The capacity lifecycle also produces the string statuses "waiting_for_capacity"
// (queued, eligible for a dispatch slot) and "waiting" (queued, backing off) (#1222).

// Ack modes
ironflow.AckModeAuto    // "auto"
ironflow.AckModeManual  // "manual"

// Backpressure modes
ironflow.BackpressureDrop    // "drop"
ironflow.BackpressureBlock   // "block"
ironflow.BackpressureBuffer  // "buffer"

// Projection modes
ironflow.ProjectionModeManaged   // "managed"
ironflow.ProjectionModeExternal  // "external"

// Event sources
ironflow.EventSourceAPI     // "api"
ironflow.EventSourceCron    // "cron"
ironflow.EventSourceWebhook // "webhook"
```

---

## Logging

The SDK uses a `Logger` interface for all output. You can plug in any structured logger.

```go
type Logger interface {
    Debug(msg string, args ...any)
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)
}
```

Usage:

```go
// Default logger (respects IRONFLOW_LOG_LEVEL env)
logger := ironflow.NewLogger(ironflow.LoggerConfig{
    Prefix: "[my-app]",
})

// Disable logging entirely
logger := ironflow.NewNoopLogger()

// Use custom logger in client/worker
client := ironflow.NewClient(ironflow.ClientConfig{
    Logger: myCustomLogger,
})

worker := ironflow.NewWorker(ironflow.WorkerConfig{
    Logger: myCustomLogger,
})
```

---

## Documentation

Full platform documentation: https://github.com/sahina/ironflow/tree/main/docs

## License

LicenseRef-Ironflow-EULA — see repository LICENSE for full terms.
