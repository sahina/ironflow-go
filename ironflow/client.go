package ironflow

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RetryEvent contains information about a retry attempt.
type RetryEvent struct {
	// Attempt is the current attempt number (1-based).
	Attempt int

	// MaxAttempts is the maximum number of attempts configured.
	MaxAttempts int

	// Error is the error that triggered the retry.
	Error error

	// Delay is the duration to wait before the next retry.
	Delay time.Duration
}

// ClientRetryConfig configures retry behavior for HTTP requests.
type ClientRetryConfig struct {
	// MaxAttempts is the maximum number of retry attempts (default: 3).
	MaxAttempts int

	// InitialDelay is the initial delay between retries for server errors (default: 100ms).
	InitialDelay time.Duration

	// MaxDelay is the maximum delay between retries (default: 10s).
	MaxDelay time.Duration

	// BackoffMultiplier is the backoff multiplier for server errors (default: 2.0).
	BackoffMultiplier float64

	// ConnectionRetryDelay is the fixed delay for connection errors (default: 2s).
	// Connection errors use fixed intervals instead of exponential backoff for faster reconnection.
	ConnectionRetryDelay time.Duration

	// OnRetry is called before each retry attempt. Useful for logging.
	OnRetry func(event RetryEvent)
}

// ClientConfig configures the Ironflow client.
type ClientConfig struct {
	// ServerURL is the Ironflow server URL.
	ServerURL string

	// APIKey is the API key for authentication. If empty, falls back to the
	// IRONFLOW_API_KEY env var. Optional for local dev.
	APIKey string

	// Timeout is the request timeout (default: 30s).
	Timeout time.Duration

	// HTTPClient is an optional custom HTTP client.
	HTTPClient *http.Client

	// Retry configures retry behavior. Set to nil to use defaults.
	// Set MaxAttempts to 0 to disable retries.
	Retry *ClientRetryConfig

	// Logger is the logger to use. If nil, uses the default console logger.
	// Set to NewNoopLogger() to disable logging.
	Logger Logger
}

// Client is the Ironflow API client.
type Client struct {
	serverURL   string
	apiKey      string
	timeout     time.Duration
	httpClient  *http.Client
	retryConfig *ClientRetryConfig
	logger      Logger

	// streamHTTPClient is a Timeout:0 HTTP/2-capable client used for
	// server-streaming RPCs (WaitForProjectionStream, future stream
	// methods). Lazily initialized so we don't build an h2c transport
	// for clients that never stream.
	streamHTTPOnce   sync.Once
	streamHTTPClient *http.Client
}

// NewClient creates a new Ironflow client.
//
// Example:
//
//	client := ironflow.NewClient(ironflow.ClientConfig{
//	    ServerURL: "http://localhost:9123",  // or use GetServerURL()
//	})
//
//	result, err := client.Emit(ctx, "order.placed", map[string]any{
//	    "orderId": "123",
//	    "total":   99.99,
//	})
func NewClient(config ClientConfig) *Client {
	PrintBanner()

	serverURL := config.ServerURL
	if serverURL == "" {
		serverURL = GetServerURL()
	}

	apiKey := config.APIKey
	if apiKey == "" {
		apiKey = GetAPIKey()
	}

	timeout := config.Timeout
	if timeout == 0 {
		timeout = DefaultClientTimeout
	}

	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: timeout,
		}
	}

	// Set up retry configuration
	var retryConfig *ClientRetryConfig
	if config.Retry != nil && config.Retry.MaxAttempts == 0 {
		// Retry explicitly disabled
		retryConfig = nil
	} else if config.Retry != nil {
		// Use provided config with defaults for unset fields
		retryConfig = &ClientRetryConfig{
			MaxAttempts:          config.Retry.MaxAttempts,
			InitialDelay:         config.Retry.InitialDelay,
			MaxDelay:             config.Retry.MaxDelay,
			BackoffMultiplier:    config.Retry.BackoffMultiplier,
			ConnectionRetryDelay: config.Retry.ConnectionRetryDelay,
			OnRetry:              config.Retry.OnRetry,
		}
		if retryConfig.MaxAttempts == 0 {
			retryConfig.MaxAttempts = DefaultClientRetryMaxAttempts
		}
		if retryConfig.InitialDelay == 0 {
			retryConfig.InitialDelay = DefaultClientRetryInitialDelay
		}
		if retryConfig.MaxDelay == 0 {
			retryConfig.MaxDelay = DefaultClientRetryMaxDelay
		}
		if retryConfig.BackoffMultiplier == 0 {
			retryConfig.BackoffMultiplier = DefaultClientRetryBackoffMultiplier
		}
		if retryConfig.ConnectionRetryDelay == 0 {
			retryConfig.ConnectionRetryDelay = DefaultClientRetryConnectionDelay
		}
	} else {
		// Use defaults
		retryConfig = &ClientRetryConfig{
			MaxAttempts:          DefaultClientRetryMaxAttempts,
			InitialDelay:         DefaultClientRetryInitialDelay,
			MaxDelay:             DefaultClientRetryMaxDelay,
			BackoffMultiplier:    DefaultClientRetryBackoffMultiplier,
			ConnectionRetryDelay: DefaultClientRetryConnectionDelay,
		}
	}

	// Initialize logger
	logger := config.Logger
	if logger == nil {
		logger = NewLogger(LoggerConfig{Prefix: "[ironflow-client]"})
	}

	return &Client{
		serverURL:   serverURL,
		apiKey:      apiKey,
		timeout:     timeout,
		httpClient:  httpClient,
		retryConfig: retryConfig,
		logger:      logger,
	}
}

// EmitResult is returned from Emit.
type EmitResult struct {
	// RunIDs are the IDs of runs created by this event.
	RunIDs []string

	// EventID is the ID of the stored event.
	EventID string
}

// EmitOption configures an emit request.
type EmitOption func(*emitOptions)

type emitOptions struct {
	idempotencyKey string
	version        int
	metadata       map[string]any
	namespace      string
}

// WithEmitIdempotencyKey sets the idempotency key for the emit.
func WithEmitIdempotencyKey(key string) EmitOption {
	return func(o *emitOptions) {
		o.idempotencyKey = key
	}
}

// WithEmitVersion sets the event schema version for the emit.
func WithEmitVersion(version int) EmitOption {
	return func(o *emitOptions) {
		o.version = version
	}
}

// WithEmitMetadata sets the metadata for the emit.
func WithEmitMetadata(metadata map[string]any) EmitOption {
	return func(o *emitOptions) {
		o.metadata = metadata
	}
}

// WithEmitNamespace sets the namespace for the emit.
func WithEmitNamespace(namespace string) EmitOption {
	return func(o *emitOptions) {
		o.namespace = namespace
	}
}

// Emit publishes an event.
//
// This is the primary method for publishing events. Events are stored
// and delivered to any matching subscribers and functions.
//
// Example:
//
//	result, err := client.Emit(ctx, "order.placed", map[string]any{
//	    "orderId": "123",
//	    "total":   99.99,
//	})
//
//	// With idempotency key
//	result, err := client.Emit(ctx, "payment.processed", data,
//	    ironflow.WithEmitIdempotencyKey("payment-abc"),
//	)
func (c *Client) Emit(ctx context.Context, eventName string, data any, opts ...EmitOption) (*EmitResult, error) {
	options := &emitOptions{
		namespace: "default",
	}
	for _, opt := range opts {
		opt(options)
	}

	req := map[string]any{
		"event":     eventName,
		"data":      data,
		"namespace": options.namespace,
	}

	if options.idempotencyKey != "" {
		req["idempotency_key"] = options.idempotencyKey
	}
	// 0 is "unset" and is omitted; a negative is forwarded so the server can
	// answer with the reason. Dropping it here (the old `> 0`) meant
	// WithEmitVersion(-1) emitted at version 1 and told the caller nothing.
	if options.version != 0 {
		req["version"] = options.version
	}
	if options.metadata != nil {
		req["metadata"] = options.metadata
	}

	var resp struct {
		RunIDs  []string `json:"runIds"`
		EventID string   `json:"eventId"`
	}

	if err := c.request(ctx, "POST", "/ironflow.v1.PubSubService/Emit", req, &resp); err != nil {
		return nil, err
	}

	return &EmitResult{
		RunIDs:  resp.RunIDs,
		EventID: resp.EventID,
	}, nil
}

// EmitSyncResult is one run's outcome from a synchronous call. EmitSync
// returns one per matched function; InvokeSync returns exactly one.
type EmitSyncResult struct {
	RunID      string
	FunctionID string
	Status     RunStatus
	Output     any
	Error      *ErrorInfo
	DurationMs int64
	// WaitTimedOut reports that the call stopped waiting before the durable run
	// reached a terminal state. Status remains the run's last-known state.
	WaitTimedOut bool
}

// runResultWire is the RunResult wire shape shared by TriggerSync and
// InvokeFunctionSync. Field names are protobuf JSON names — Connect emits
// runId/functionId/durationMs, not snake_case (#1920).
type runResultWire struct {
	RunID      string `json:"runId"`
	FunctionID string `json:"functionId"`
	Status     string `json:"status"`
	Output     any    `json:"output"`
	Error      *struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	} `json:"error"`
	DurationMs   int64 `json:"durationMs"`
	WaitTimedOut bool  `json:"waitTimedOut"`
}

func (r *runResultWire) toEmitSyncResult() (*EmitSyncResult, error) {
	status, err := runStatusFromWire(r.Status)
	if err != nil {
		return nil, err
	}

	result := &EmitSyncResult{
		RunID:        r.RunID,
		FunctionID:   r.FunctionID,
		Status:       status,
		Output:       r.Output,
		DurationMs:   r.DurationMs,
		WaitTimedOut: r.WaitTimedOut,
	}
	if r.Error != nil {
		result.Error = &ErrorInfo{
			Message: r.Error.Message,
			Code:    r.Error.Code,
		}
	}
	return result, nil
}

func runStatusFromWire(status string) (RunStatus, error) {
	switch status {
	case "RUN_STATUS_RUNNING":
		return RunStatusRunning, nil
	case "RUN_STATUS_COMPLETED":
		return RunStatusCompleted, nil
	case "RUN_STATUS_FAILED":
		return RunStatusFailed, nil
	case "RUN_STATUS_CANCELLED":
		return RunStatusCancelled, nil
	case "RUN_STATUS_PAUSED":
		return RunStatusPaused, nil
	case "RUN_STATUS_WAITING_FOR_CAPACITY":
		return RunStatusWaitingForCapacity, nil
	case "RUN_STATUS_WAITING":
		return RunStatusWaiting, nil
	default:
		return "", NewError(
			fmt.Sprintf("invalid run status on the wire: %q", status),
			"INVALID_RESPONSE",
			false,
		)
	}
}

// encodeProtoBytes / decodeProtoBytes bridge a proto `bytes` field, which
// protojson carries as a base64 string — NOT as raw JSON. Several scoped-
// injection fields hold a JSON payload inside a bytes field
// (PausedStepInfo.output, InjectStepOutputRequest.new_output,
// InjectStepOutputResponse.previous_output), so the payload has to be
// base64-encoded on the way out and decoded on the way back (#1919).
//
// Sending raw JSON text is rejected by protojson before the handler ever runs
// ("invalid value for bytes field"), and reading the response without decoding
// hands the caller base64 wrapped in a json.RawMessage.
func encodeProtoBytes(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func decodeProtoBytes(encoded string) (json.RawMessage, error) {
	if encoded == "" {
		return nil, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(decoded), nil
}

// runStatusToWire is the inverse of runStatusFromWire. Status filters land on a
// protobuf enum field, so the canonical RUN_STATUS_* name is the only spelling
// that survives the trip. An unmappable value is NOT rejected by the server:
// Connect unmarshals with DiscardUnknown, which drops unrecognized enum values,
// so the field is silently zeroed to RUN_STATUS_UNSPECIFIED and the handler
// reads that as "no filter" — the caller gets unfiltered runs and no error
// (#1919). This switch is the only place that mistake is ever caught.
//
// Written as an explicit switch rather than a string transform on purpose:
// RunStatusPending has no wire equivalent (the proto reserves the name), and a
// transform would mint a plausible-looking value the server discards.
func runStatusToWire(status RunStatus) (string, error) {
	switch status {
	case RunStatusRunning:
		return "RUN_STATUS_RUNNING", nil
	case RunStatusCompleted:
		return "RUN_STATUS_COMPLETED", nil
	case RunStatusFailed:
		return "RUN_STATUS_FAILED", nil
	case RunStatusCancelled:
		return "RUN_STATUS_CANCELLED", nil
	case RunStatusPaused:
		return "RUN_STATUS_PAUSED", nil
	case RunStatusWaitingForCapacity:
		return "RUN_STATUS_WAITING_FOR_CAPACITY", nil
	case RunStatusWaiting:
		return "RUN_STATUS_WAITING", nil
	default:
		return "", NewError(
			fmt.Sprintf("invalid run status filter: %q", string(status)),
			"INVALID_ARGUMENT",
			false,
		)
	}
}

// SyncOption configures a synchronous call. Applies to both EmitSync and
// InvokeSync.
type SyncOption func(*syncOptions)

type syncOptions struct {
	idempotencyKey string
	metadata       map[string]any
	version        int
}

// WithSyncIdempotencyKey sets the idempotency key for EmitSync or InvokeSync.
// A repeat call with the same key returns the original run instead of
// creating a second one.
func WithSyncIdempotencyKey(key string) SyncOption {
	return func(o *syncOptions) {
		o.idempotencyKey = key
	}
}

// WithSyncMetadata sets the metadata stored on the event generated by
// EmitSync or InvokeSync. The synchronous counterpart of WithEmitMetadata.
func WithSyncMetadata(metadata map[string]any) SyncOption {
	return func(o *syncOptions) {
		o.metadata = metadata
	}
}

// WithSyncVersion sets the event schema version on the event EmitSync
// generates. The synchronous counterpart of WithEmitVersion.
//
// No-op on InvokeSync, which shares this option type: invoking a function
// directly generates no event, so there is no schema to select. Given for one
// field, a second option type is the worse trade.
func WithSyncVersion(version int) SyncOption {
	return func(o *syncOptions) {
		o.version = version
	}
}

// syncTransportGrace is how much longer than the server-side wait budget a
// synchronous call keeps its transport open.
const syncTransportGrace = 5 * time.Second

// syncHTTPClient returns an HTTP client whose transport deadline is at least
// budget. http.Client.Timeout is enforced independently of the request
// context, so the client-level default would otherwise undercut a sync call's
// own timeout_ms. For InvokeSync that is not just a lost result: the server
// reads a dead request context as an abandoned caller and cancels the run
// (ADR 0067 / Q19).
func (c *Client) syncHTTPClient(budget time.Duration) *http.Client {
	if c.httpClient.Timeout == 0 || c.httpClient.Timeout >= budget {
		return c.httpClient
	}
	hc := *c.httpClient
	hc.Timeout = budget
	return &hc
}

// EmitSync emits an event and waits for every run it triggers.
//
// One event can match several functions, so this returns one result per run —
// never just the first. Run outcomes are not errors: a failed or cancelled run
// comes back in the slice with its Status and Error populated, and an event
// that matches nothing returns an empty slice. Only transport and protocol
// failures return err.
//
// For production use, prefer Emit() for better throughput.
//
// Example:
//
//	results, err := client.EmitSync(ctx, "order.placed", map[string]any{
//	    "orderId": "123",
//	}, 30*time.Second)
//	if err != nil {
//	    return err
//	}
//	for _, r := range results {
//	    fmt.Printf("%s: %s\n", r.FunctionID, r.Status)
//	}
//
//	// With an idempotency key
//	results, err := client.EmitSync(ctx, "payment.processed", data, 0,
//	    ironflow.WithSyncIdempotencyKey("payment-abc"),
//	)
func (c *Client) EmitSync(ctx context.Context, eventName string, data any, timeout time.Duration, opts ...SyncOption) ([]EmitSyncResult, error) {
	if timeout == 0 {
		timeout = DefaultEmitSyncTimeout
	}

	options := &syncOptions{}
	for _, opt := range opts {
		opt(options)
	}

	req := map[string]any{
		"event":      eventName,
		"data":       data,
		"timeout_ms": timeout.Milliseconds(),
	}
	if options.idempotencyKey != "" {
		req["idempotency_key"] = options.idempotencyKey
	}
	if options.metadata != nil {
		req["metadata"] = options.metadata
	}
	// Matches Emit's guard: 0 is "unset" on the wire, so omit rather than send
	// it. A negative is left to the server, which answers InvalidArgument with
	// the reason rather than having the client silently drop it.
	if options.version != 0 {
		req["version"] = options.version
	}

	var resp struct {
		Results []runResultWire `json:"results"`
	}

	if err := c.doSyncRequest(ctx, "/ironflow.v1.IronflowService/TriggerSync", req, timeout, &resp); err != nil {
		return nil, err
	}

	// An event matching no function yields an empty slice, not an error: the
	// server answers with results: [] deliberately, and a caller that wants
	// loudness checks len(). Browser and Node agree.
	out := make([]EmitSyncResult, len(resp.Results))
	for i := range resp.Results {
		result, err := resp.Results[i].toEmitSyncResult()
		if err != nil {
			return nil, err
		}
		out[i] = *result
	}

	return out, nil
}

// InvokeSync invokes one function by ID and waits for its single run.
//
// The function-keyed sibling of EmitSync. EmitSync is event-keyed and fans out
// to every matching function; this takes a function ID and returns exactly one
// result, so a caller that knows which function it wants can name the run it
// cares about (ADR 0067).
//
// As with EmitSync, a failed or cancelled run is a result, not an err.
//
// Cancelling ctx cancels the run server-side — unlike Emit/EmitSync, an
// InvokeSync run has exactly one consumer, so a caller that goes away leaves
// it with none. An expired timeout is not abandonment: the result comes back
// with WaitTimedOut set and the run keeps going.
//
// Example:
//
//	result, err := client.InvokeSync(ctx, "process-order", map[string]any{
//	    "orderId": "123",
//	}, 30*time.Second)
//	if err != nil {
//	    return err
//	}
//	if result.Status == ironflow.RunStatusCompleted {
//	    fmt.Printf("Order processed: %v\n", result.Output)
//	}
func (c *Client) InvokeSync(ctx context.Context, functionID string, data any, timeout time.Duration, opts ...SyncOption) (*EmitSyncResult, error) {
	if timeout == 0 {
		timeout = DefaultEmitSyncTimeout
	}

	options := &syncOptions{}
	for _, opt := range opts {
		opt(options)
	}

	req := map[string]any{
		"function_id": functionID,
		"data":        data,
		"timeout_ms":  timeout.Milliseconds(),
	}
	if options.idempotencyKey != "" {
		req["idempotency_key"] = options.idempotencyKey
	}
	if options.metadata != nil {
		req["metadata"] = options.metadata
	}

	var resp struct {
		Result *runResultWire `json:"result"`
	}

	if err := c.doSyncRequest(ctx, "/ironflow.v1.IronflowService/InvokeFunctionSync", req, timeout, &resp); err != nil {
		return nil, err
	}

	// InvokeFunctionSyncResponse carries exactly one result. An absent one is
	// a broken server, not an empty fan-out.
	if resp.Result == nil {
		return nil, NewError("sync response carried no result", "INVALID_RESPONSE", false)
	}

	return resp.Result.toEmitSyncResult()
}

// doSyncRequest issues a synchronous-call request, keeping the transport
// deadline longer than the server's own wait budget. See syncHTTPClient.
func (c *Client) doSyncRequest(ctx context.Context, path string, req any, timeout time.Duration, resp any) error {
	budget := timeout + syncTransportGrace

	syncCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	return c.requestWith(syncCtx, c.syncHTTPClient(budget), "POST", path, req, resp)
}

// GetRun gets a run by ID.
func (c *Client) GetRun(ctx context.Context, runID string) (*WorkflowRun, error) {
	var resp runResponse

	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/GetRun", map[string]string{"id": runID}, &resp); err != nil {
		return nil, err
	}

	return mapRunResponse(&resp)
}

// ListRunsOptions configures a list runs request.
type ListRunsOptions struct {
	FunctionID string
	Status     RunStatus
	Limit      int
	Cursor     string
}

// ListRunsResult is returned from ListRuns.
type ListRunsResult struct {
	Runs       []*WorkflowRun
	NextCursor string
	TotalCount int
}

// ListRuns lists runs with filtering.
func (c *Client) ListRuns(ctx context.Context, opts *ListRunsOptions) (*ListRunsResult, error) {
	req := make(map[string]any)
	if opts != nil {
		if opts.FunctionID != "" {
			req["function_id"] = opts.FunctionID
		}
		if opts.Status != "" {
			wire, err := runStatusToWire(opts.Status)
			if err != nil {
				return nil, err
			}
			req["status"] = wire
		}
		if opts.Limit > 0 {
			req["limit"] = opts.Limit
		}
		if opts.Cursor != "" {
			req["cursor"] = opts.Cursor
		}
	}

	var resp struct {
		Runs       []runResponse `json:"runs"`
		NextCursor string        `json:"nextCursor"`
		TotalCount int           `json:"totalCount"`
	}

	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/ListRuns", req, &resp); err != nil {
		return nil, err
	}

	runs := make([]*WorkflowRun, len(resp.Runs))
	for i := range resp.Runs {
		run, err := mapRunResponse(&resp.Runs[i])
		if err != nil {
			// The realistic trigger is a decode-shape mismatch, in which case
			// ID is empty too — fall back to the index so the operator still
			// knows which row to look at.
			where := fmt.Sprintf("run %q", resp.Runs[i].ID)
			if resp.Runs[i].ID == "" {
				where = fmt.Sprintf("run at index %d", i)
			}
			return nil, WrapError(err, where+" in ListRuns response",
				"INVALID_RESPONSE", false)
		}
		runs[i] = run
	}

	return &ListRunsResult{
		Runs:       runs,
		NextCursor: resp.NextCursor,
		TotalCount: resp.TotalCount,
	}, nil
}

// CancelRun cancels a running run.
func (c *Client) CancelRun(ctx context.Context, runID string, reason string) (*WorkflowRun, error) {
	var resp runResponse

	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/CancelRun", map[string]string{
		"id":     runID,
		"reason": reason,
	}, &resp); err != nil {
		return nil, err
	}

	return mapRunResponse(&resp)
}

// PauseRun pauses a running workflow run for scoped injection.
//
// Example:
//
//	status, err := client.PauseRun(ctx, "run_abc123")
//	fmt.Println("Status:", status) // "paused"
func (c *Client) PauseRun(ctx context.Context, runID string) (string, error) {
	var resp struct {
		Status string `json:"status"`
	}

	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/PauseRun", map[string]string{
		"run_id": runID,
	}, &resp); err != nil {
		return "", err
	}

	return resp.Status, nil
}

// GetPausedState returns the state of a paused run, including completed steps
// and a hint for the next step that will execute on resume.
//
// Example:
//
//	state, err := client.GetPausedState(ctx, "run_abc123")
//	for _, step := range state.Steps {
//	    fmt.Printf("Step %s: injected=%v\n", step.Name, step.Injected)
//	}
func (c *Client) GetPausedState(ctx context.Context, runID string) (*PausedState, error) {
	var resp struct {
		Steps []struct {
			ID          string `json:"id"`
			Name        string `json:"name"`
			Output      string `json:"output"`
			Injected    bool   `json:"injected"`
			CompletedAt string `json:"completedAt"`
			StepType    string `json:"stepType"`
			Status      string `json:"status"`
			// Error is a proto bytes field, so it arrives base64-encoded.
			Error string `json:"error"`
		} `json:"steps"`
		NextStepHint string `json:"nextStepHint"`
		PauseReason  string `json:"pauseReason"`
	}

	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/GetPausedState", map[string]string{
		"run_id": runID,
	}, &resp); err != nil {
		return nil, err
	}

	steps := make([]PausedStepInfo, len(resp.Steps))
	for i, s := range resp.Steps {
		output, err := decodeProtoBytes(s.Output)
		if err != nil {
			return nil, WrapError(err,
				fmt.Sprintf("step %q in GetPausedState response", s.ID),
				"INVALID_RESPONSE", false)
		}

		stepErr, err := decodeProtoBytes(s.Error)
		if err != nil {
			return nil, WrapError(err,
				fmt.Sprintf("step %q error in GetPausedState response", s.ID),
				"INVALID_RESPONSE", false)
		}

		steps[i] = PausedStepInfo{
			ID:          s.ID,
			Name:        s.Name,
			Output:      output,
			Injected:    s.Injected,
			CompletedAt: s.CompletedAt,
			StepType:    s.StepType,
			Status:      s.Status,
			Error:       stepErr,
		}
	}

	return &PausedState{
		Steps:        steps,
		NextStepHint: resp.NextStepHint,
		PauseReason:  resp.PauseReason,
	}, nil
}

// InjectStepOutput replaces the output of a step in a paused run.
// Returns the previous output that was replaced.
//
// Example:
//
//	prevOutput, err := client.InjectStepOutput(ctx, "run_abc123", "step_xyz",
//	    json.RawMessage(`{"corrected": true}`), "Manual correction")
func (c *Client) InjectStepOutput(ctx context.Context, runID, stepID string, newOutput json.RawMessage, reason string) (json.RawMessage, error) {
	var resp struct {
		StepID         string `json:"stepId"`
		PreviousOutput string `json:"previousOutput"`
	}

	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/InjectStepOutput", map[string]any{
		"run_id":     runID,
		"step_id":    stepID,
		"new_output": encodeProtoBytes(newOutput),
		"reason":     reason,
	}, &resp); err != nil {
		return nil, err
	}

	previous, err := decodeProtoBytes(resp.PreviousOutput)
	if err != nil {
		return nil, WrapError(err, "previous output in InjectStepOutput response",
			"INVALID_RESPONSE", false)
	}

	return previous, nil
}

// GetRunStateAt returns the reconstructed state of a run at a specific timestamp.
func (c *Client) GetRunStateAt(ctx context.Context, runID string, at time.Time) (*TimeTravelRunStateSnapshot, error) {
	var resp struct {
		Snapshot TimeTravelRunStateSnapshot `json:"snapshot"`
	}
	if err := c.request(ctx, "POST", "/ironflow.v1.TimeTravelService/GetRunStateAt", map[string]any{
		"runId":     runID,
		"timestamp": at.UTC().Format(time.RFC3339Nano),
	}, &resp); err != nil {
		return nil, err
	}
	return &resp.Snapshot, nil
}

// GetRunTimeline returns the timeline of audit events for a run.
func (c *Client) GetRunTimeline(ctx context.Context, runID string) ([]TimeTravelTimelineEvent, error) {
	var resp struct {
		Events []TimeTravelTimelineEvent `json:"events"`
	}
	if err := c.request(ctx, "POST", "/ironflow.v1.TimeTravelService/GetRunTimeline", map[string]string{
		"runId": runID,
	}, &resp); err != nil {
		return nil, err
	}
	return resp.Events, nil
}

// GetStepOutputAt returns the output of a specific step at a specific timestamp.
func (c *Client) GetStepOutputAt(ctx context.Context, runID, stepID string, at time.Time) (*TimeTravelStepOutputSnapshot, error) {
	var resp TimeTravelStepOutputSnapshot
	if err := c.request(ctx, "POST", "/ironflow.v1.TimeTravelService/GetStepOutputAt", map[string]any{
		"runId":     runID,
		"stepId":    stepID,
		"timestamp": at.UTC().Format(time.RFC3339Nano),
	}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ResumeRun resumes a paused or failed workflow run.
// If fromStep is non-empty, execution resumes from that specific step.
//
// Example:
//
//	run, err := client.ResumeRun(ctx, "run_abc123", "")
//	run, err := client.ResumeRun(ctx, "run_abc123", "step_xyz") // from specific step
func (c *Client) ResumeRun(ctx context.Context, runID string, fromStep string) (*WorkflowRun, error) {
	req := map[string]string{"run_id": runID}
	if fromStep != "" {
		req["from_step"] = fromStep
	}

	var resp runResponse

	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/ResumeRun", req, &resp); err != nil {
		return nil, err
	}

	return mapRunResponse(&resp)
}

// RegisterFunction registers a function with the server.
func (c *Client) RegisterFunction(ctx context.Context, fn Function) error {
	metadata := GetFunctionMetadata(fn)

	var resp struct {
		Created bool `json:"created"`
	}

	return c.request(ctx, "POST", "/ironflow.v1.IronflowService/RegisterFunction", metadata, &resp)
}

// Health performs a health check.
func (c *Client) Health(ctx context.Context) (string, error) {
	var resp struct {
		Status string `json:"status"`
	}

	if err := c.request(ctx, "POST", "/ironflow.v1.IronflowService/Health", map[string]any{}, &resp); err != nil {
		return "", err
	}

	return resp.Status, nil
}

// ServerCapabilities describes the server's available capabilities.
type ServerCapabilities struct {
	// Transports lists supported transport protocols.
	Transports []string

	// Features lists supported features.
	Features []string

	// Version is the server version.
	Version string
}

// GetCapabilities returns the server's capabilities.
// Useful for SDK transport auto-detection.
func (c *Client) GetCapabilities(ctx context.Context) (*ServerCapabilities, error) {
	url := c.serverURL + "/api/v1/capabilities"

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, WrapError(err, "failed to create request", "REQUEST_ERROR", true)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, WrapError(err, "request failed", "REQUEST_FAILED", true)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, NewError(fmt.Sprintf("failed to get capabilities: %d", resp.StatusCode), "HTTP_ERROR", false)
	}

	var caps ServerCapabilities
	if err := json.NewDecoder(resp.Body).Decode(&caps); err != nil {
		return nil, WrapError(err, "failed to decode response", "DECODE_ERROR", false)
	}

	return &caps, nil
}

// FunctionInfo describes a registered function.
type FunctionInfo struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Status        string `json:"status"`
	PreferredMode string `json:"preferred_mode"`
	CreatedAt     string `json:"created_at"`
	UpdatedAt     string `json:"updated_at"`
}

// WorkerInfo describes a connected worker.
type WorkerInfo struct {
	ID            string            `json:"id"`
	Hostname      string            `json:"hostname"`
	FunctionIDs   []string          `json:"function_ids"`
	MaxConcurrent int               `json:"max_concurrent"`
	Labels        map[string]string `json:"labels"`
	ActiveJobs    int               `json:"active_jobs"`
	RegisteredAt  string            `json:"registered_at"`
	LastHeartbeat string            `json:"last_heartbeat"`
	Transport     string            `json:"transport"`
}

// PatchStep patches a step's output (hot patching).
func (c *Client) PatchStep(ctx context.Context, stepID string, output map[string]any, reason string) error {
	return c.request(ctx, "POST", "/api/v1/steps/patch", map[string]any{
		"step_id": stepID,
		"output":  output,
		"reason":  reason,
	}, nil)
}

// ListFunctions returns all registered functions.
func (c *Client) ListFunctions(ctx context.Context) ([]FunctionInfo, error) {
	url := c.serverURL + "/api/v1/functions"

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, WrapError(err, "failed to create request", "REQUEST_ERROR", true)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, WrapError(err, "request failed", "REQUEST_FAILED", true)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, NewError(fmt.Sprintf("failed to list functions: %d", resp.StatusCode), "HTTP_ERROR", false)
	}

	var result struct {
		Functions []FunctionInfo `json:"functions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, WrapError(err, "failed to decode response", "DECODE_ERROR", false)
	}

	return result.Functions, nil
}

// ListWorkers returns all connected workers.
func (c *Client) ListWorkers(ctx context.Context) ([]WorkerInfo, error) {
	url := c.serverURL + "/api/v1/workers"

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, WrapError(err, "failed to create request", "REQUEST_ERROR", true)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, WrapError(err, "request failed", "REQUEST_FAILED", true)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, NewError(fmt.Sprintf("failed to list workers: %d", resp.StatusCode), "HTTP_ERROR", false)
	}

	var result struct {
		Workers []WorkerInfo `json:"workers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, WrapError(err, "failed to decode response", "DECODE_ERROR", false)
	}

	return result.Workers, nil
}

// ============================================================================
// Consumer Group Management
// ============================================================================

// AckMode defines the acknowledgment mode for consumer groups.
type AckMode string

const (
	// AckModeAuto automatically acknowledges messages.
	AckModeAuto AckMode = "auto"
	// AckModeManual requires manual acknowledgment.
	AckModeManual AckMode = "manual"
)

// BackpressureMode defines how backpressure is handled.
type BackpressureMode string

const (
	// BackpressureDrop drops messages when buffer is full.
	BackpressureDrop BackpressureMode = "drop"
	// BackpressureBlock blocks until buffer has space.
	BackpressureBlock BackpressureMode = "block"
	// BackpressureBuffer buffers messages (default).
	BackpressureBuffer BackpressureMode = "buffer"
)

// ConsumerGroupStatus defines the status of a consumer group.
type ConsumerGroupStatus string

const (
	// ConsumerGroupStatusActive indicates an active group.
	ConsumerGroupStatusActive ConsumerGroupStatus = "active"
	// ConsumerGroupStatusPaused indicates a paused group.
	ConsumerGroupStatusPaused ConsumerGroupStatus = "paused"
	// ConsumerGroupStatusDeleted indicates a deleted group.
	ConsumerGroupStatusDeleted ConsumerGroupStatus = "deleted"
)

// ConsumerGroupConfig configures a consumer group.
type ConsumerGroupConfig struct {
	// Name is the unique name within namespace.
	Name string

	// Pattern is the event pattern to subscribe to.
	Pattern string

	// Namespace is the namespace (default: "default").
	Namespace string

	// FilterExpr is an optional CEL filter expression.
	FilterExpr string

	// AckMode is the acknowledgment mode (default: auto).
	AckMode AckMode

	// Backpressure is the backpressure handling (default: buffer).
	Backpressure BackpressureMode

	// MaxInflight is max unacknowledged messages per consumer (default: 100).
	MaxInflight int

	// MaxRedeliveries is max redelivery attempts (default: 3).
	MaxRedeliveries int

	// RedeliverDelayMs is delay between redeliveries in ms (default: 5000).
	RedeliverDelayMs int

	// Metadata is custom metadata.
	Metadata map[string]any
}

// ConsumerGroup represents a consumer group.
type ConsumerGroup struct {
	// ID is the consumer group ID.
	ID string

	// Namespace is the namespace.
	Namespace string

	// Name is the human-readable name.
	Name string

	// Pattern is the event pattern.
	Pattern string

	// FilterExpr is the CEL filter expression.
	FilterExpr string

	// AckMode is the acknowledgment mode.
	AckMode AckMode

	// Backpressure is the backpressure handling.
	Backpressure BackpressureMode

	// MaxInflight is max unacknowledged messages per consumer.
	MaxInflight int

	// MaxRedeliveries is max redelivery attempts.
	MaxRedeliveries int

	// RedeliverDelayMs is delay between redeliveries in ms.
	RedeliverDelayMs int

	// Metadata is custom metadata.
	Metadata map[string]any

	// Status is the current status.
	Status ConsumerGroupStatus

	// MemberCount is the number of active members.
	MemberCount int

	// CreatedAt is the creation timestamp.
	CreatedAt time.Time

	// UpdatedAt is the last update timestamp.
	UpdatedAt time.Time
}

// CreateConsumerGroup creates a new consumer group for load-balanced event delivery.
//
// Example:
//
//	group, err := client.CreateConsumerGroup(ctx, ironflow.ConsumerGroupConfig{
//	    Name:        "order-processors",
//	    Pattern:     "order.*",
//	    AckMode:     ironflow.AckModeManual,
//	    MaxInflight: 50,
//	})
func (c *Client) CreateConsumerGroup(ctx context.Context, config ConsumerGroupConfig) (*ConsumerGroup, error) {
	namespace := config.Namespace
	if namespace == "" {
		namespace = "default"
	}

	req := map[string]any{
		"name":      config.Name,
		"namespace": namespace,
		"pattern":   config.Pattern,
	}

	if config.FilterExpr != "" {
		req["filter_expr"] = config.FilterExpr
	}
	if config.AckMode != "" {
		req["ack_mode"] = mapAckModeToProto(config.AckMode)
	}
	if config.Backpressure != "" {
		req["backpressure"] = mapBackpressureToProto(config.Backpressure)
	}
	if config.MaxInflight > 0 {
		req["max_inflight"] = config.MaxInflight
	}
	if config.MaxRedeliveries > 0 {
		req["max_redeliveries"] = config.MaxRedeliveries
	}
	if config.RedeliverDelayMs > 0 {
		req["redeliver_delay_ms"] = config.RedeliverDelayMs
	}
	if config.Metadata != nil {
		req["metadata"] = config.Metadata
	}

	var resp consumerGroupResponse
	if err := c.request(ctx, "POST", "/ironflow.v1.PubSubService/CreateConsumerGroup", req, &resp); err != nil {
		return nil, err
	}

	return mapConsumerGroupResponse(&resp), nil
}

// GetConsumerGroup gets a consumer group by name.
func (c *Client) GetConsumerGroup(ctx context.Context, name string, opts ...ConsumerGroupOption) (*ConsumerGroup, error) {
	options := &consumerGroupOptions{namespace: "default"}
	for _, opt := range opts {
		opt(options)
	}

	req := map[string]string{
		"name":      name,
		"namespace": options.namespace,
	}

	var resp consumerGroupResponse
	if err := c.request(ctx, "POST", "/ironflow.v1.PubSubService/GetConsumerGroup", req, &resp); err != nil {
		return nil, err
	}

	return mapConsumerGroupResponse(&resp), nil
}

// ListConsumerGroups lists consumer groups.
func (c *Client) ListConsumerGroups(ctx context.Context, opts ...ConsumerGroupOption) ([]*ConsumerGroup, error) {
	options := &consumerGroupOptions{}
	for _, opt := range opts {
		opt(options)
	}

	groups := make([]*ConsumerGroup, 0)
	cursor := ""
	for {
		req := map[string]any{
			"namespace": options.namespace,
			"limit":     100,
		}
		if cursor != "" {
			req["cursor"] = cursor
		}

		var resp struct {
			Groups     []consumerGroupResponse `json:"groups"`
			NextCursor string                  `json:"nextCursor"`
		}

		if err := c.request(ctx, "POST", "/ironflow.v1.PubSubService/ListConsumerGroups", req, &resp); err != nil {
			return nil, err
		}
		for i := range resp.Groups {
			groups = append(groups, mapConsumerGroupResponse(&resp.Groups[i]))
		}
		if resp.NextCursor == "" {
			break
		}
		cursor = resp.NextCursor
	}

	return groups, nil
}

// UpdateConsumerGroupInput contains mutable consumer-group fields. Pointer
// fields distinguish "leave unchanged" from an explicit zero value.
type UpdateConsumerGroupInput struct {
	Pattern          *string
	FilterExpr       *string
	AckMode          *AckMode
	Backpressure     *BackpressureMode
	MaxInflight      *int
	MaxRedeliveries  *int
	RedeliverDelayMs *int
	Metadata         *map[string]any
	Status           *ConsumerGroupStatus
}

// UpdateConsumerGroup updates selected fields on a consumer group.
func (c *Client) UpdateConsumerGroup(ctx context.Context, name string, input UpdateConsumerGroupInput, opts ...ConsumerGroupOption) (*ConsumerGroup, error) {
	options := &consumerGroupOptions{namespace: "default"}
	for _, opt := range opts {
		opt(options)
	}

	group := map[string]any{"name": name, "namespace": options.namespace}
	paths := make([]string, 0, 9)
	if input.Pattern != nil {
		group["pattern"] = *input.Pattern
		paths = append(paths, "pattern")
	}
	if input.FilterExpr != nil {
		group["filter_expr"] = *input.FilterExpr
		paths = append(paths, "filter_expr")
	}
	if input.AckMode != nil {
		group["ack_mode"] = mapAckModeToProto(*input.AckMode)
		paths = append(paths, "ack_mode")
	}
	if input.Backpressure != nil {
		group["backpressure"] = mapBackpressureToProto(*input.Backpressure)
		paths = append(paths, "backpressure")
	}
	if input.MaxInflight != nil {
		group["max_inflight"] = *input.MaxInflight
		paths = append(paths, "max_inflight")
	}
	if input.MaxRedeliveries != nil {
		group["max_redeliveries"] = *input.MaxRedeliveries
		paths = append(paths, "max_redeliveries")
	}
	if input.RedeliverDelayMs != nil {
		group["redeliver_delay_ms"] = *input.RedeliverDelayMs
		paths = append(paths, "redeliver_delay_ms")
	}
	if input.Metadata != nil {
		group["metadata"] = *input.Metadata
		paths = append(paths, "metadata")
	}
	if input.Status != nil {
		group["status"] = mapConsumerGroupStatusToProto(*input.Status)
		paths = append(paths, "status")
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("consumer group update requires at least one field")
	}

	var resp consumerGroupResponse
	if err := c.request(ctx, "POST", "/ironflow.v1.PubSubService/UpdateConsumerGroup", map[string]any{
		"group":       group,
		"update_mask": map[string]any{"paths": paths},
	}, &resp); err != nil {
		return nil, err
	}
	return mapConsumerGroupResponse(&resp), nil
}

// DeleteConsumerGroup deletes a consumer group.
func (c *Client) DeleteConsumerGroup(ctx context.Context, name string, opts ...ConsumerGroupOption) error {
	options := &consumerGroupOptions{namespace: "default"}
	for _, opt := range opts {
		opt(options)
	}

	req := map[string]string{
		"name":      name,
		"namespace": options.namespace,
	}

	return c.request(ctx, "POST", "/ironflow.v1.PubSubService/DeleteConsumerGroup", req, nil)
}

// JoinConsumerGroupOptions configures joining a consumer group.
type JoinConsumerGroupOptions struct {
	// Namespace is the namespace (default: "default").
	Namespace string

	// ConsumerID is the optional consumer identifier.
	ConsumerID string

	// Transport is the preferred transport (auto-detected if not set).
	Transport string
}

// JoinConsumerGroupOption configures JoinConsumerGroup.
type JoinConsumerGroupOption func(*JoinConsumerGroupOptions)

// WithJoinNamespace sets the namespace for JoinConsumerGroup.
func WithJoinNamespace(namespace string) JoinConsumerGroupOption {
	return func(o *JoinConsumerGroupOptions) {
		o.Namespace = namespace
	}
}

// WithJoinConsumerID sets the consumer ID for JoinConsumerGroup.
func WithJoinConsumerID(consumerID string) JoinConsumerGroupOption {
	return func(o *JoinConsumerGroupOptions) {
		o.ConsumerID = consumerID
	}
}

// WithJoinTransport sets the transport preference for JoinConsumerGroup.
// Valid values: "websocket", "grpc". If not set, auto-detects.
func WithJoinTransport(transport string) JoinConsumerGroupOption {
	return func(o *JoinConsumerGroupOptions) {
		o.Transport = transport
	}
}

// JoinConsumerGroup connects to a consumer group for load-balanced event delivery.
//
// Returns a subscription client appropriate for the server's capabilities.
// Use the Events() channel to receive events and Ack/Nak/Term for acknowledgment.
//
// Example:
//
//	sub, err := client.JoinConsumerGroup(ctx, "order-processors")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer sub.Unsubscribe()
//
//	for event := range sub.Events() {
//	    if err := processOrder(event); err != nil {
//	        sub.Nak(event.ID, 10*time.Second)
//	        continue
//	    }
//	    sub.Ack(event.ID)
//	}
func (c *Client) JoinConsumerGroup(ctx context.Context, groupName string, opts ...JoinConsumerGroupOption) (*AckableSubscription, error) {
	options := &JoinConsumerGroupOptions{
		Namespace: "default",
	}
	for _, opt := range opts {
		opt(options)
	}

	// Determine transport
	transport := options.Transport
	if transport == "" {
		transport = "websocket" // Default to WebSocket for compatibility

		// Try to auto-detect based on capabilities
		caps, err := c.GetCapabilities(ctx)
		if err == nil {
			for _, t := range caps.Transports {
				if t == "grpc-bidirectional" {
					transport = "grpc"
					break
				}
			}
		}
	}

	if transport == "grpc" {
		return c.joinConsumerGroupGrpc(ctx, groupName, options)
	}

	return c.joinConsumerGroupWebSocket(ctx, groupName, options)
}

// joinConsumerGroupWebSocket joins a consumer group using WebSocket transport.
func (c *Client) joinConsumerGroupWebSocket(ctx context.Context, groupName string, options *JoinConsumerGroupOptions) (*AckableSubscription, error) {
	// Get consumer group to find pattern
	group, err := c.GetConsumerGroup(ctx, groupName, WithConsumerGroupNamespace(options.Namespace))
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer group: %w", err)
	}

	// Create WebSocket subscription client
	subClient := c.CreateSubscriptionClient()
	if err := subClient.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	// Subscribe with consumer group and manual ack
	sub, err := subClient.SubscribeAckable(ctx, group.Pattern, &SubscribeOptions{
		ConsumerGroup: groupName,
		AckMode:       AckModeManual,
		Namespace:     options.Namespace,
	})
	if err != nil {
		subClient.Close()
		return nil, err
	}

	return sub, nil
}

// joinConsumerGroupGrpc joins a consumer group using HTTP streaming transport.
func (c *Client) joinConsumerGroupGrpc(ctx context.Context, groupName string, options *JoinConsumerGroupOptions) (*AckableSubscription, error) {
	// Get consumer group to find pattern
	group, err := c.GetConsumerGroup(ctx, groupName, WithConsumerGroupNamespace(options.Namespace))
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer group: %w", err)
	}

	// Create HTTP streaming subscription client
	grpcClient := c.CreateGrpcSubscriptionClient()
	if err := grpcClient.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	// Subscribe with manual ack mode
	grpcSub, err := grpcClient.SubscribeAckable(ctx, group.Pattern, &SubscribeOptions{
		ConsumerGroup: groupName,
		AckMode:       AckModeManual,
		Namespace:     options.Namespace,
	})
	if err != nil {
		grpcClient.Close()
		return nil, err
	}

	// Wrap in AckableSubscription interface
	bridgeSub := &Subscription{
		ID:      grpcSub.ID,
		Pattern: grpcSub.Pattern,
		events:  grpcSub.events,
		errors:  grpcSub.errors,
		done:    grpcSub.done,
	}

	return &AckableSubscription{
		Subscription: bridgeSub,
	}, nil
}

// DetectTransport auto-detects the best available transport for subscriptions.
//
// Returns "grpc" if the server supports gRPC bidirectional streaming,
// otherwise returns "websocket".
func (c *Client) DetectTransport(ctx context.Context) (string, error) {
	caps, err := c.GetCapabilities(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get server capabilities: %w", err)
	}

	for _, t := range caps.Transports {
		if t == "grpc-bidirectional" {
			return "grpc", nil
		}
	}

	for _, t := range caps.Transports {
		if t == "websocket" {
			return "websocket", nil
		}
	}

	return "websocket", nil // Default fallback
}

// ConsumerGroupOption configures consumer group operations.
type ConsumerGroupOption func(*consumerGroupOptions)

type consumerGroupOptions struct {
	namespace string
}

// WithConsumerGroupNamespace sets the namespace for consumer group operations.
func WithConsumerGroupNamespace(namespace string) ConsumerGroupOption {
	return func(o *consumerGroupOptions) {
		o.namespace = namespace
	}
}

// consumerGroupResponse is the wire format for consumer group responses.
type consumerGroupResponse struct {
	ID               string         `json:"id"`
	Namespace        string         `json:"namespace"`
	Name             string         `json:"name"`
	Pattern          string         `json:"pattern"`
	FilterExpr       string         `json:"filterExpr"`
	AckMode          string         `json:"ackMode"`
	Backpressure     string         `json:"backpressure"`
	MaxInflight      int            `json:"maxInflight"`
	MaxRedeliveries  int            `json:"maxRedeliveries"`
	RedeliverDelayMs int            `json:"redeliverDelayMs"`
	Metadata         map[string]any `json:"metadata"`
	Status           string         `json:"status"`
	MemberCount      int            `json:"memberCount"`
	CreatedAt        string         `json:"createdAt"`
	UpdatedAt        string         `json:"updatedAt"`
}

func mapAckModeToProto(mode AckMode) string {
	switch mode {
	case AckModeManual:
		return "ACK_MODE_MANUAL"
	default:
		return "ACK_MODE_AUTO"
	}
}

func mapBackpressureToProto(mode BackpressureMode) string {
	switch mode {
	case BackpressureDrop:
		return "BACKPRESSURE_MODE_DROP"
	case BackpressureBlock:
		return "BACKPRESSURE_MODE_BLOCK"
	default:
		return "BACKPRESSURE_MODE_BUFFER"
	}
}

func mapAckModeFromProto(mode string) AckMode {
	switch mode {
	case "ACK_MODE_MANUAL":
		return AckModeManual
	default:
		return AckModeAuto
	}
}

func mapBackpressureFromProto(mode string) BackpressureMode {
	switch mode {
	case "BACKPRESSURE_MODE_DROP":
		return BackpressureDrop
	case "BACKPRESSURE_MODE_BLOCK":
		return BackpressureBlock
	default:
		return BackpressureBuffer
	}
}

func mapConsumerGroupStatusToProto(status ConsumerGroupStatus) string {
	switch status {
	case ConsumerGroupStatusPaused:
		return "CONSUMER_GROUP_STATUS_PAUSED"
	case ConsumerGroupStatusDeleted:
		return "CONSUMER_GROUP_STATUS_DELETED"
	default:
		return "CONSUMER_GROUP_STATUS_ACTIVE"
	}
}

func mapConsumerGroupStatusFromProto(status string) ConsumerGroupStatus {
	switch status {
	case "CONSUMER_GROUP_STATUS_PAUSED":
		return ConsumerGroupStatusPaused
	case "CONSUMER_GROUP_STATUS_DELETED":
		return ConsumerGroupStatusDeleted
	default:
		return ConsumerGroupStatusActive
	}
}

func mapConsumerGroupResponse(r *consumerGroupResponse) *ConsumerGroup {
	group := &ConsumerGroup{
		ID:               r.ID,
		Namespace:        r.Namespace,
		Name:             r.Name,
		Pattern:          r.Pattern,
		FilterExpr:       r.FilterExpr,
		AckMode:          mapAckModeFromProto(r.AckMode),
		Backpressure:     mapBackpressureFromProto(r.Backpressure),
		MaxInflight:      r.MaxInflight,
		MaxRedeliveries:  r.MaxRedeliveries,
		RedeliverDelayMs: r.RedeliverDelayMs,
		Metadata:         r.Metadata,
		Status:           mapConsumerGroupStatusFromProto(r.Status),
		MemberCount:      r.MemberCount,
	}

	if r.CreatedAt != "" {
		if t, err := time.Parse(time.RFC3339, r.CreatedAt); err == nil {
			group.CreatedAt = t
		}
	}
	if r.UpdatedAt != "" {
		if t, err := time.Parse(time.RFC3339, r.UpdatedAt); err == nil {
			group.UpdatedAt = t
		}
	}

	return group
}

// ============================================================================
// Entity Stream Methods
// ============================================================================

// AppendStreamEvent appends an event to an entity stream.
//
// Example:
//
//	result, err := client.AppendStreamEvent(ctx, "order-123", ironflow.AppendEventInput{
//	    Name:       "item.added",
//	    Data:       map[string]any{"sku": "WIDGET-1", "qty": 2},
//	    EntityType: "order",
//	})
//
//	// With optimistic concurrency control
//	result, err := client.AppendStreamEvent(ctx, "order-123", input,
//	    ironflow.WithExpectedVersion(5),
//	)
func (c *Client) AppendStreamEvent(ctx context.Context, entityID string, input AppendEventInput, opts ...AppendOption) (*AppendResult, error) {
	cfg := &appendConfig{
		expectedVersion: -1, // default: skip version check
		version:         1,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	body := map[string]any{
		"entity_id":        entityID,
		"entity_type":      input.EntityType,
		"event_name":       input.Name,
		"data":             input.Data,
		"expected_version": cfg.expectedVersion,
		"idempotency_key":  cfg.idempotencyKey,
		"version":          cfg.version,
	}
	if cfg.metadata != nil {
		body["metadata"] = cfg.metadata
	}

	var result AppendResult
	if err := c.request(ctx, "POST", "/ironflow.v1.EntityStreamService/AppendEvent", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ReadStream reads events from an entity stream.
//
// Example:
//
//	events, err := client.ReadStream(ctx, "order-123")
//
//	// With options
//	events, err := client.ReadStream(ctx, "order-123", ironflow.ReadStreamOpts{
//	    FromVersion: 5,
//	    Limit:       10,
//	    Direction:   "backward",
//	})
func (c *Client) ReadStream(ctx context.Context, entityID string, opts ...ReadStreamOpts) ([]StreamEvent, error) {
	body := map[string]any{
		"entity_id": entityID,
	}

	if len(opts) > 0 {
		opt := opts[0]
		if opt.FromVersion > 0 {
			body["from_version"] = opt.FromVersion
		}
		if opt.Limit > 0 {
			body["limit"] = opt.Limit
		}
		if opt.Direction != "" {
			body["direction"] = opt.Direction
		}
	}

	var result struct {
		Events     []StreamEvent `json:"events"`
		TotalCount int           `json:"totalCount"`
	}
	if err := c.request(ctx, "POST", "/ironflow.v1.EntityStreamService/ReadStream", body, &result); err != nil {
		return nil, err
	}
	return result.Events, nil
}

// GetStreamInfo returns computed metadata for an entity stream.
//
// Example:
//
//	info, err := client.GetStreamInfo(ctx, "order-123")
//	fmt.Printf("Entity %s at version %d\n", info.EntityID, info.Version)
func (c *Client) GetStreamInfo(ctx context.Context, entityID string) (*StreamInfo, error) {
	body := map[string]any{
		"entity_id": entityID,
	}

	var result StreamInfo
	if err := c.request(ctx, "POST", "/ironflow.v1.EntityStreamService/GetStreamInfo", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListStreams returns all entity streams.
//
// Example:
//
//	streams, err := client.ListStreams(ctx)
//	for _, s := range streams {
//	    fmt.Printf("Entity %s (%s) at version %d\n", s.EntityID, s.EntityType, s.Version)
//	}
func (c *Client) ListStreams(ctx context.Context) ([]StreamListEntry, error) {
	var resp struct {
		Streams []StreamListEntry `json:"streams"`
	}
	if err := c.restRequest(ctx, "GET", "/api/v1/streams", nil, &resp); err != nil {
		return nil, err
	}
	if resp.Streams == nil {
		return []StreamListEntry{}, nil
	}
	return resp.Streams, nil
}

// GetEntityHistory returns the full event history for an entity.
//
// Example:
//
//	events, err := client.GetEntityHistory(ctx, "order-123")
//	for _, e := range events {
//	    fmt.Printf("Version %d: %s\n", e.Version, e.EventName)
//	}
func (c *Client) GetEntityHistory(ctx context.Context, entityID string) ([]EntityHistoryEntry, error) {
	var resp struct {
		Entries []EntityHistoryEntry `json:"entries"`
	}
	path := fmt.Sprintf("/api/v1/streams/%s/history", url.PathEscape(entityID))
	if err := c.restRequest(ctx, "GET", path, nil, &resp); err != nil {
		return nil, err
	}
	if resp.Entries == nil {
		return []EntityHistoryEntry{}, nil
	}
	return resp.Entries, nil
}

// CreateSnapshot creates a snapshot for an entity stream.
//
// Example:
//
//	snapshot, err := client.CreateSnapshot(ctx, "order-123", ironflow.CreateSnapshotInput{
//	    EntityType:    "order",
//	    EntityVersion: 10,
//	    State:         map[string]any{"status": "shipped"},
//	})
func (c *Client) CreateSnapshot(ctx context.Context, entityID string, input CreateSnapshotInput) (*StreamSnapshot, error) {
	var snapshot StreamSnapshot
	path := fmt.Sprintf("/api/v1/streams/%s/snapshots", url.PathEscape(entityID))
	if err := c.restRequest(ctx, "POST", path, input, &snapshot); err != nil {
		return nil, err
	}
	// The server only returns {"snapshot_id": "..."} on creation.
	// Populate the remaining fields from the input so callers can use them immediately.
	if snapshot.EntityID == "" {
		snapshot.EntityID = entityID
	}
	if snapshot.EntityType == "" {
		snapshot.EntityType = input.EntityType
	}
	if snapshot.EntityVersion == 0 {
		snapshot.EntityVersion = input.EntityVersion
	}
	if snapshot.State == nil {
		snapshot.State = input.State
	}
	return &snapshot, nil
}

// GetSnapshot returns the latest snapshot for an entity stream.
//
// Example:
//
//	snapshot, err := client.GetSnapshot(ctx, "order-123")
//	fmt.Printf("Snapshot at version %d: %v\n", snapshot.EntityVersion, snapshot.State)
func (c *Client) GetSnapshot(ctx context.Context, entityID string) (*StreamSnapshot, error) {
	var snapshot StreamSnapshot
	path := fmt.Sprintf("/api/v1/streams/%s/snapshots", url.PathEscape(entityID))
	if err := c.restRequest(ctx, "GET", path, nil, &snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// ============================================================================
// Developer Pub/Sub Methods
// ============================================================================

// PublishResult is returned from Publish.
type PublishResult struct {
	// EventID is the unique ID assigned to the published message.
	EventID string
	// Sequence is the JetStream sequence number.
	Sequence uint64
}

// Publish sends a message to a developer pub/sub topic.
// Unlike Emit, this does NOT trigger workflow functions.
//
// Example:
//
//	result, err := client.Publish(ctx, "notifications.email", map[string]any{
//	    "to":      "user@example.com",
//	    "subject": "Hello",
//	})
//
//	// With idempotency key
//	result, err := client.Publish(ctx, "notifications.email", data,
//	    ironflow.WithPublishIdempotencyKey("email-abc"),
//	)
func (c *Client) Publish(ctx context.Context, topic string, data any, opts ...PublishOption) (*PublishResult, error) {
	cfg := &publishConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	req := map[string]any{
		"topic": topic,
		"data":  data,
	}

	if cfg.idempotencyKey != "" {
		req["idempotencyKey"] = cfg.idempotencyKey
	}

	var resp struct {
		EventID  string `json:"eventId"`
		Sequence uint64 `json:"sequence,string"`
	}

	if err := c.request(ctx, "POST", "/ironflow.v1.PubSubService/Publish", req, &resp); err != nil {
		return nil, err
	}

	return &PublishResult{
		EventID:  resp.EventID,
		Sequence: resp.Sequence,
	}, nil
}

// ListTopics returns all active developer pub/sub topics.
//
// Example:
//
//	topics, err := client.ListTopics(ctx)
//	for _, t := range topics {
//	    fmt.Printf("Topic: %s (%d messages)\n", t.Name, t.MessageCount)
//	}
func (c *Client) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	var resp struct {
		Topics []TopicInfo `json:"topics"`
	}

	if err := c.request(ctx, "POST", "/ironflow.v1.PubSubService/ListTopics", map[string]any{}, &resp); err != nil {
		return nil, err
	}

	if resp.Topics == nil {
		return []TopicInfo{}, nil
	}
	return resp.Topics, nil
}

// GetTopicStats returns detailed statistics for a topic.
//
// Example:
//
//	stats, err := client.GetTopicStats(ctx, "notifications.email")
//	fmt.Printf("Messages: %d, Lag: %d\n", stats.MessageCount, stats.Lag)
func (c *Client) GetTopicStats(ctx context.Context, topic string) (*TopicStats, error) {
	req := map[string]any{
		"topic": topic,
	}

	var resp TopicStats

	if err := c.request(ctx, "POST", "/ironflow.v1.PubSubService/GetTopicStats", req, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetAuditTrail retrieves the audit trail for a run.
//
// Example:
//
//	result, err := client.GetAuditTrail(ctx, "run-abc-123")
//	for _, event := range result.Events {
//	    fmt.Printf("%s: %s\n", event.EventType, event.CreatedAt)
//	}
//
//	// With filtering options
//	result, err := client.GetAuditTrail(ctx, "run-abc-123", GetAuditTrailOpts{
//	    EventType: "step.completed",
//	    Limit:     50,
//	})
func (c *Client) GetAuditTrail(ctx context.Context, runID string, opts ...GetAuditTrailOpts) (*AuditTrailResult, error) {
	req := map[string]any{
		"run_id": runID,
	}

	if len(opts) > 0 {
		o := opts[0]
		if o.EventType != "" {
			req["event_type"] = o.EventType
		}
		if o.FromTimestamp != "" {
			req["from_timestamp"] = o.FromTimestamp
		}
		if o.ToTimestamp != "" {
			req["to_timestamp"] = o.ToTimestamp
		}
		if o.Limit > 0 {
			req["limit"] = o.Limit
		}
		if o.Cursor != "" {
			req["cursor"] = o.Cursor
		}
	}

	var resp AuditTrailResult

	if err := c.request(ctx, "POST", "/ironflow.v1.AuditService/GetAuditTrail", req, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// RestRequest makes an HTTP request to the server with retry support.
// This is the exported entry point used by generated SDK code (cmd/sdk-gen).
func (c *Client) RestRequest(ctx context.Context, method, path string, body any, result any) error {
	return c.request(ctx, method, path, body, result)
}

// request makes an HTTP request to the server with retry support.
//
//nolint:unparam // method kept for future flexibility
func (c *Client) request(ctx context.Context, method, path string, body any, result any) error {
	return c.requestWith(ctx, c.httpClient, method, path, body, result)
}

// requestWith is request with an explicit HTTP client, so calls that need a
// transport deadline other than the client default can supply one.
func (c *Client) requestWith(ctx context.Context, httpClient *http.Client, method, path string, body any, result any) error {
	url := c.serverURL + path

	// Marshal body once for reuse across retries
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return WrapError(err, "failed to marshal request body", "MARSHAL_ERROR", false)
		}
	}

	// If retry is disabled, execute once
	if c.retryConfig == nil {
		return c.executeRequest(ctx, httpClient, method, url, bodyBytes, result)
	}

	// Execute with retry logic
	var lastErr error
	for attempt := 1; attempt <= c.retryConfig.MaxAttempts; attempt++ {
		err := c.executeRequest(ctx, httpClient, method, url, bodyBytes, result)
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if error is retryable
		ironflowErr, ok := err.(*IronflowError)
		if !ok || !ironflowErr.Retryable {
			return err
		}

		// Don't retry on last attempt
		if attempt == c.retryConfig.MaxAttempts {
			return err
		}

		// Calculate delay based on error type
		var delay time.Duration
		isConnectionError := ironflowErr.Code == "CONNECTION_REFUSED" || isNetworkError(err)

		if isConnectionError {
			// Use fixed delay for connection errors (faster reconnection)
			delay = c.retryConfig.ConnectionRetryDelay
		} else {
			// Use exponential backoff for server errors
			delayFloat := float64(c.retryConfig.InitialDelay)
			for i := 1; i < attempt; i++ {
				delayFloat *= c.retryConfig.BackoffMultiplier
			}
			if delayFloat > float64(c.retryConfig.MaxDelay) {
				delayFloat = float64(c.retryConfig.MaxDelay)
			}
			delay = time.Duration(delayFloat)
		}

		// Check for Retry-After header
		if retryAfter := ironflowErr.RetryAfter; retryAfter > 0 {
			if retryAfter > delay {
				delay = retryAfter
			}
		}

		// Log retry attempt
		c.logger.Debug("Retry attempt",
			"attempt", attempt,
			"maxAttempts", c.retryConfig.MaxAttempts,
			"error", err,
			"delay", delay,
			"isConnectionError", isConnectionError,
		)

		// Call OnRetry callback if provided
		if c.retryConfig.OnRetry != nil {
			c.retryConfig.OnRetry(RetryEvent{
				Attempt:     attempt,
				MaxAttempts: c.retryConfig.MaxAttempts,
				Error:       err,
				Delay:       delay,
			})
		}

		// Wait before retrying
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			// Continue to next attempt
		}
	}

	return lastErr
}

// isNetworkError checks if the error is a network-level connection error.
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}

	// Check the error message for common connection error patterns
	errMsg := err.Error()
	return strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "no such host") ||
		strings.Contains(errMsg, "network is unreachable") ||
		strings.Contains(errMsg, "dial tcp")
}

// executeRequest performs a single HTTP request.
func (c *Client) executeRequest(ctx context.Context, httpClient *http.Client, method, url string, bodyBytes []byte, result any) error {
	var bodyReader io.Reader
	if bodyBytes != nil {
		bodyReader = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return WrapError(err, "failed to create request", "REQUEST_ERROR", true)
	}

	if bodyBytes != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	// Forward the emitting run id (when the context carries one) so events a
	// function emits are attributed to the run for the flow map's learned
	// emit edges (#1262). Matches the JS SDK's run-context propagation.
	if rid := runIDFromContext(ctx); rid != "" {
		req.Header.Set(HeaderRunID, rid)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return WrapError(err, "request failed", "REQUEST_FAILED", true)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return WrapError(err, "failed to read response", "RESPONSE_ERROR", true)
	}

	if resp.StatusCode >= 400 {
		var errResp struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(respBody, &errResp) // best-effort parsing

		ironflowErr := NewError(
			fmt.Sprintf("%s: %s", errResp.Code, errResp.Message),
			errResp.Code,
			resp.StatusCode >= 500,
		)
		ironflowErr.Details = map[string]any{"http_status": resp.StatusCode}

		switch resp.StatusCode {
		case http.StatusUnauthorized:
			ironflowErr.Cause = ErrUnauthorized
			// Name the env var and the key file, not just the status (#1673).
			ironflowErr.Message += " — " + AuthHelp
		case http.StatusPaymentRequired:
			ironflowErr.Cause = ErrEnterpriseLicenseRequired
		case http.StatusForbidden:
			ironflowErr.Cause = ErrForbidden
			ironflowErr.Message += " — " + AuthHelp
		case http.StatusConflict:
			// Without this, a 409 is indistinguishable from any other 4xx and
			// callers have to string-match the message to tell "already in
			// flight, wait" apart from a real failure (#1963).
			ironflowErr.Cause = ErrConflict
		}

		// Parse Retry-After header if present
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			ironflowErr.RetryAfter = parseRetryAfter(retryAfter)
		}

		return ironflowErr
	}

	if result != nil {
		if err := json.Unmarshal(respBody, result); err != nil {
			return WrapError(err, "failed to unmarshal response", "UNMARSHAL_ERROR", false)
		}
	}

	return nil
}

// parseRetryAfter parses the Retry-After header value.
func parseRetryAfter(value string) time.Duration {
	// Try to parse as seconds
	if seconds, err := time.ParseDuration(value + "s"); err == nil {
		return seconds
	}

	// Try to parse as HTTP date
	if t, err := http.ParseTime(value); err == nil {
		return time.Until(t)
	}

	return 0
}

// runResponse is the wire format for run responses. Field names are protobuf
// JSON names — IronflowService runs on Connect's default codec, which emits
// lowerCamel and omits zero-valued fields (#1919).
type runResponse struct {
	ID          string `json:"id"`
	FunctionID  string `json:"functionId"`
	EventID     string `json:"eventId"`
	Status      string `json:"status"`
	Attempt     int    `json:"attempt"`
	MaxAttempts int    `json:"maxAttempts"`
	Input       any    `json:"input"`
	Output      any    `json:"output"`
	Error       *struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	} `json:"error"`
	StartedAt string `json:"startedAt"`
	EndedAt   string `json:"endedAt"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
}

func mapRunResponse(r *runResponse) (*WorkflowRun, error) {
	status, err := runStatusFromWire(r.Status)
	if err != nil {
		return nil, err
	}

	run := &WorkflowRun{
		ID:          r.ID,
		FunctionID:  r.FunctionID,
		EventID:     r.EventID,
		Status:      status,
		Attempt:     r.Attempt,
		MaxAttempts: r.MaxAttempts,
		Input:       r.Input,
		Output:      r.Output,
	}

	if r.Error != nil {
		run.Error = &ErrorInfo{
			Message: r.Error.Message,
			Code:    r.Error.Code,
		}
	}

	if r.StartedAt != "" {
		if t, err := time.Parse(time.RFC3339, r.StartedAt); err == nil {
			run.StartedAt = &t
		}
	}
	if r.EndedAt != "" {
		if t, err := time.Parse(time.RFC3339, r.EndedAt); err == nil {
			run.EndedAt = &t
		}
	}
	if r.CreatedAt != "" {
		if t, err := time.Parse(time.RFC3339, r.CreatedAt); err == nil {
			run.CreatedAt = t
		}
	}
	if r.UpdatedAt != "" {
		if t, err := time.Parse(time.RFC3339, r.UpdatedAt); err == nil {
			run.UpdatedAt = t
		}
	}

	return run, nil
}

// ============================================================================
// Wait for projection catchup (issue #473)
// ============================================================================

// WaitForProjection blocks until a named projection has processed events
// up to opts.MinSeq, or the timeout elapses. Returns CaughtUp=true on
// success, TimedOut=true if the deadline fires with the projection still
// behind.
//
// Read-your-writes pattern:
//
//	res, err := client.AppendStreamEvent(ctx, orderID, input)
//	_, err = client.WaitForProjection(ctx, "order-detail-view", ironflow.WaitForProjectionOpts{
//	    MinSeq:    res.Sequence.Uint64(),
//	    Partition: orderID,       // for partitioned managed projections
//	    Timeout:   5 * time.Second,
//	})
//	// Now safe to read the projection.
//
// Errors: 404 (projection not found), 409 (paused / rebuilding /
// partition-unsupported-for-external), 429 (wait capacity exceeded), 503
// (NATS bridge unavailable).
func (c *Client) WaitForProjection(ctx context.Context, name string, opts WaitForProjectionOpts) (*WaitResult, error) {
	body := map[string]any{
		"name": name,
		// protojson represents uint64/int64 as strings so values exceeding
		// JavaScript's safe-integer range (2^53-1) survive a JS round-trip
		// without silent precision loss. See protobuf JSON mapping spec.
		"minSeq": strconv.FormatUint(opts.MinSeq, 10),
	}
	if opts.Timeout > 0 {
		body["timeout"] = formatDurationForProto(opts.Timeout)
	}
	if opts.Partition != "" {
		body["partition"] = opts.Partition
	}

	var result WaitResult
	if err := c.request(ctx, "POST", "/ironflow.v1.ProjectionService/WaitProjectionCatchup", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// WaitForProjections waits for multiple projections concurrently. All
// items share a single timeout deadline and a single atomic slot
// reservation on the server — if the cap cannot absorb N items, the
// whole batch is rejected with 429 (ErrWaitCapacityExceeded in the error
// chain). Individual-item failures (ErrProjectionNotFound, etc.) are
// returned per-item in the response via WaitItemResult.Error.
//
// Max 16 items per batch.
func (c *Client) WaitForProjections(ctx context.Context, items []WaitItem, timeout time.Duration) ([]WaitItemResult, error) {
	protoItems := make([]map[string]any, 0, len(items))
	for _, it := range items {
		m := map[string]any{
			"name":   it.Name,
			"minSeq": strconv.FormatUint(it.MinSeq, 10),
		}
		if it.Partition != "" {
			m["partition"] = it.Partition
		}
		protoItems = append(protoItems, m)
	}
	body := map[string]any{"items": protoItems}
	if timeout > 0 {
		body["timeout"] = formatDurationForProto(timeout)
	}

	var wire struct {
		Results []WaitItemResult `json:"results"`
	}
	if err := c.request(ctx, "POST", "/ironflow.v1.ProjectionService/WaitProjectionCatchupBatch", body, &wire); err != nil {
		return nil, err
	}
	if wire.Results == nil {
		return []WaitItemResult{}, nil
	}
	return wire.Results, nil
}

// WaitForEvent waits on a specific projection until it has processed
// the event identified by eventID. The server resolves eventID to its
// NATS sequence via events.nats_seq and waits on that.
//
// Returns a 404-class error if eventID is unknown; 409 if the event
// exists but its nats_seq is NULL (pre-migration or publish failed).
// In the 409 case, fall back to WaitForProjection with MinSeq from a
// fresh write.
func (c *Client) WaitForEvent(ctx context.Context, eventID, projection string, opts WaitForProjectionOpts) (*WaitResult, error) {
	body := map[string]any{
		"eventId":    eventID,
		"projection": projection,
	}
	if opts.Timeout > 0 {
		body["timeout"] = formatDurationForProto(opts.Timeout)
	}
	if opts.Partition != "" {
		body["partition"] = opts.Partition
	}

	var result WaitResult
	if err := c.request(ctx, "POST", "/ironflow.v1.ProjectionService/WaitForEvent", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// formatDurationForProto encodes a time.Duration as the JSON shape
// ConnectRPC expects for google.protobuf.Duration: a decimal string
// with "s" suffix (e.g., "5.3s"). ConnectRPC reads durations in this
// JSON form consistently across all handlers.
func formatDurationForProto(d time.Duration) string {
	return fmt.Sprintf("%.9fs", d.Seconds())
}
