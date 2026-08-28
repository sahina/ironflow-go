package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ============================================================================
// Pattern Helpers
// ============================================================================

// Patterns provides helper functions for building subscription patterns.
//
// Patterns use NATS-style wildcards:
//   - * matches a single token
//   - > matches one or more tokens (must be at end)
//
// Example:
//
//	Patterns.AllRuns()             // "system.run.>"
//	Patterns.Run("abc123")         // "system.run.abc123.>"
//	Patterns.UserEvent("order.*")  // "events:order.*"
//	Patterns.AllSecrets()          // "system.secret.*"
//	Patterns.Secret("API_KEY")     // "system.secret.API_KEY.*"
//	Patterns.SecretAction("updated") // "system.secret.*.updated"
var Patterns = struct {
	// AllRuns returns a pattern matching all run events.
	AllRuns func() string
	// Run returns a pattern matching all events for a specific run.
	Run func(runID string) string
	// RunLifecycle returns a pattern matching run lifecycle events only.
	RunLifecycle func(runID string) string
	// RunSteps returns a pattern matching step events for a run.
	RunSteps func(runID string) string
	// AllFunctions returns a pattern matching all function events.
	AllFunctions func() string
	// Function returns a pattern matching events for a specific function.
	Function func(functionID string) string
	// UserEvent returns a pattern for a user event.
	UserEvent func(eventName string) string
	// AllUserEvents returns a pattern matching all user events.
	AllUserEvents func() string
	// AllSecrets returns a pattern matching all secret events.
	AllSecrets func() string
	// Secret returns a pattern matching all events for a specific secret.
	Secret func(name string) string
	// SecretAction returns a pattern matching a specific action across all secrets.
	SecretAction func(action string) string
	// Topic returns a pattern for developer pub/sub topics.
	Topic func(topicPattern string) string
	// AllTopics returns a pattern matching all developer pub/sub topics.
	AllTopics func() string
}{
	AllRuns:       func() string { return "system.run.>" },
	Run:           func(runID string) string { return fmt.Sprintf("system.run.%s.>", runID) },
	RunLifecycle:  func(runID string) string { return fmt.Sprintf("system.run.%s.*", runID) },
	RunSteps:      func(runID string) string { return fmt.Sprintf("system.run.%s.step.>", runID) },
	AllFunctions:  func() string { return "system.function.>" },
	Function:      func(functionID string) string { return fmt.Sprintf("system.function.%s.>", functionID) },
	UserEvent:     func(eventName string) string { return fmt.Sprintf("events:%s", eventName) },
	AllUserEvents: func() string { return "events:>" },
	AllSecrets:    func() string { return "system.secret.*" },
	Secret:        func(name string) string { return fmt.Sprintf("system.secret.%s.*", name) },
	SecretAction:  func(action string) string { return fmt.Sprintf("system.secret.*.%s", action) },
	Topic:         func(topicPattern string) string { return "topic:" + topicPattern },
	AllTopics:     func() string { return "topic:>" },
}

// ============================================================================
// Types
// ============================================================================

// SubscribeOptions configures a subscription.
type SubscribeOptions struct {
	// Replay is the number of historical events to replay (0 = no replay).
	Replay int

	// StartAfterSequence resumes delivery after this global event sequence.
	// A pointer is used because zero is a valid cursor. It cannot be combined
	// with Replay or ConsumerGroup. When set, reconnects resume after the last
	// event delivered to the subscription, providing at-least-once delivery.
	StartAfterSequence *uint64

	// IncludeMetadata includes event metadata (timestamp, sequence).
	IncludeMetadata bool

	// Filter is a CEL expression to filter events.
	Filter string

	// ConsumerGroup is the consumer group to join for load-balanced delivery.
	ConsumerGroup string

	// AckMode is the acknowledgment mode (auto or manual).
	AckMode AckMode

	// Backpressure is the backpressure handling mode.
	Backpressure BackpressureMode

	// Namespace is the event namespace (default: "default").
	Namespace string
}

// SubscriptionEvent represents an event received from a subscription.
type SubscriptionEvent struct {
	// ID is the event ID (used for manual acknowledgment).
	ID string

	// Topic is the event topic (e.g., "system.run.abc123.updated").
	Topic string

	// Data is the raw event data.
	Data json.RawMessage

	// Meta contains event metadata (if IncludeMetadata was true).
	Meta *EventMeta
}

// EventMeta contains event metadata.
type EventMeta struct {
	// Timestamp is the event timestamp in ISO 8601 format.
	Timestamp string `json:"timestamp"`

	// Sequence is the event sequence number.
	Sequence uint64 `json:"sequence,omitempty"`

	// SequenceExact is the decimal uint64 representation used by JavaScript
	// clients when Sequence exceeds their safe integer range.
	SequenceExact string `json:"sequenceExact,omitempty"`
}

// SubscriptionError represents an error from a subscription.
type SubscriptionError struct {
	// SubscriptionID is the ID of the subscription that had an error.
	SubscriptionID string

	// Code is the error code.
	Code string

	// Message is the error message.
	Message string

	// Retrying indicates if the system is automatically retrying.
	Retrying bool
}

func (e *SubscriptionError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Subscription represents an active subscription.
type Subscription struct {
	// ID is the stable handle ID assigned by the first successful subscribe.
	// Use CurrentID to inspect the current server ID after a reconnect.
	ID string

	// Pattern is the subscription pattern.
	Pattern string

	// Events returns a channel receiving events.
	events chan *SubscriptionEvent

	// Errors returns a channel receiving errors.
	errors chan *SubscriptionError

	// client is the parent client
	client *SubscriptionClient

	// options retains the subscription identity for reconnects.
	options *SubscribeOptions

	// done signals subscription closure
	done chan struct{}
	once sync.Once
}

// Events returns a channel for receiving subscription events.
func (s *Subscription) Events() <-chan *SubscriptionEvent {
	return s.events
}

// Errors returns a channel for receiving subscription errors.
func (s *Subscription) Errors() <-chan *SubscriptionError {
	return s.errors
}

// CurrentID returns the server subscription ID currently bound to this handle.
func (s *Subscription) CurrentID() string {
	return s.client.currentSubscriptionID(s.Pattern, s.ID)
}

// Unsubscribe stops the subscription.
func (s *Subscription) Unsubscribe() {
	s.once.Do(func() {
		close(s.done)
		s.client.unsubscribeByPattern(s.Pattern)
	})
}

// AckableSubscription is a subscription that supports manual acknowledgment.
//
// Use this when SubscribeOptions.AckMode is set to AckModeManual.
// Events must be acknowledged, negatively acknowledged, or terminated.
type AckableSubscription struct {
	*Subscription
}

// Ack acknowledges an event, confirming it was processed successfully.
//
// Once acknowledged, the event will not be redelivered.
//
// Example:
//
//	for event := range sub.Events() {
//	    if err := processEvent(event); err != nil {
//	        sub.Nak(event.ID, 5*time.Second) // Retry after 5s
//	        continue
//	    }
//	    sub.Ack(event.ID)
//	}
func (s *AckableSubscription) Ack(eventID string) error {
	req := wsAckRequest{
		Type:    "ack",
		AckType: "ack",
		EventID: eventID,
	}
	return s.client.sendJSON(req)
}

// Nak negatively acknowledges an event, requesting redelivery.
//
// The delay parameter specifies how long to wait before redelivery.
// If delay is 0, the event is redelivered immediately.
func (s *AckableSubscription) Nak(eventID string, delay time.Duration) error {
	req := wsAckRequest{
		Type:           "ack",
		AckType:        "nak",
		EventID:        eventID,
		RedeliverDelay: int(delay.Milliseconds()),
	}
	return s.client.sendJSON(req)
}

// Term terminates an event, preventing any further redelivery.
//
// Use this when an event cannot be processed and should not be retried
// (e.g., permanently invalid data).
func (s *AckableSubscription) Term(eventID string) error {
	req := wsAckRequest{
		Type:    "ack",
		AckType: "term",
		EventID: eventID,
	}
	return s.client.sendJSON(req)
}

// ============================================================================
// Subscription Client
// ============================================================================

// SubscriptionClientConfig configures a subscription client.
type SubscriptionClientConfig struct {
	// WSURL is the WebSocket server URL (e.g., "ws://localhost:9123/ws").
	WSURL string

	// AutoReconnect enables automatic reconnection for subscriptions without a
	// resume cursor. StartAfterSequence opts its subscription into reconnects.
	AutoReconnect bool

	// ReconnectDelay is the initial reconnect delay (default: 1s).
	ReconnectDelay time.Duration

	// MaxReconnectDelay is the maximum reconnect delay (default: 30s).
	MaxReconnectDelay time.Duration

	// ReconnectBackoff is the backoff multiplier (default: 1.5).
	ReconnectBackoff float64

	// Logger is the logger to use. If nil, uses the default console logger.
	// Set to NewNoopLogger() to disable logging.
	Logger Logger
}

// ConnectionState represents the WebSocket connection state.
type ConnectionState string

const (
	StateConnecting   ConnectionState = "connecting"
	StateConnected    ConnectionState = "connected"
	StateDisconnected ConnectionState = "disconnected"
	StateReconnecting ConnectionState = "reconnecting"
)

// SubscriptionClient is a WebSocket client for real-time subscriptions.
//
// Example:
//
//	client := ironflow.NewSubscriptionClient(ironflow.SubscriptionClientConfig{
//	    WSURL: "ws://localhost:9123/ws",
//	})
//
//	if err := client.Connect(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	sub, err := client.Subscribe(ctx, ironflow.Patterns.AllRuns(), nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	for event := range sub.Events() {
//	    fmt.Printf("Event: %s\n", event.Topic)
//	}
type SubscriptionClient struct {
	config SubscriptionClientConfig
	logger Logger
	// apiKey authenticates the /ws upgrade, which is not a public route.
	// Set from the parent Client by CreateSubscriptionClient, otherwise from
	// IRONFLOW_API_KEY. SubscriptionClientConfig has no key field yet, so a
	// standalone client with an explicit key still depends on the env var.
	apiKey string

	mu               sync.RWMutex
	writeMu          sync.Mutex // protects websocket writes
	conn             *websocket.Conn
	state            ConnectionState
	reconnectAttempt int

	// Subscription tracking
	pendingMu     sync.Mutex
	pending       map[string]*pendingSubscribeAttempt // pattern -> in-flight wire request
	subscriptions map[string]*Subscription            // subscriptionID -> subscription
	subByPattern  map[string]string                   // pattern -> subscriptionID

	// Connection callbacks
	onConnectionChange func(connected bool)

	// Channels for coordination
	done      chan struct{}
	closeOnce sync.Once
}

// subscribeResult is the result of a subscribe request
type subscribeResult struct {
	subscriptionID string
	err            error
}

// pendingSubscribeAttempt serializes subscribe requests for the same pattern.
// The WebSocket protocol correlates results by pattern, so allowing two requests
// for one pattern in flight would make their acknowledgments ambiguous.
type pendingSubscribeAttempt struct {
	result      chan subscribeResult
	done        chan struct{}
	canceledCh  chan struct{}
	options     *SubscribeOptions
	resubscribe *Subscription
	canceled    bool
}

// NewSubscriptionClient creates a new subscription client.
func NewSubscriptionClient(config SubscriptionClientConfig) *SubscriptionClient {
	if config.ReconnectDelay == 0 {
		config.ReconnectDelay = time.Second
	}
	if config.MaxReconnectDelay == 0 {
		config.MaxReconnectDelay = 30 * time.Second
	}
	if config.ReconnectBackoff == 0 {
		config.ReconnectBackoff = 1.5
	}

	// Initialize logger
	logger := config.Logger
	if logger == nil {
		logger = NewLogger(LoggerConfig{Prefix: "[ironflow-subscribe]"})
	}

	return &SubscriptionClient{
		config:        config,
		logger:        logger,
		apiKey:        GetAPIKey(),
		state:         StateDisconnected,
		pending:       make(map[string]*pendingSubscribeAttempt),
		subscriptions: make(map[string]*Subscription),
		subByPattern:  make(map[string]string),
		done:          make(chan struct{}),
	}
}

// State returns the current connection state.
func (c *SubscriptionClient) State() ConnectionState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// IsConnected returns true if connected to the server.
func (c *SubscriptionClient) IsConnected() bool {
	return c.State() == StateConnected
}

// SetConnectionCallback sets a callback for connection state changes.
func (c *SubscriptionClient) SetConnectionCallback(callback func(connected bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onConnectionChange = callback
}

// Connect connects to the WebSocket server.
func (c *SubscriptionClient) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.state == StateConnected {
		c.mu.Unlock()
		return nil
	}
	c.state = StateConnecting
	c.mu.Unlock()

	var dialHeader http.Header
	if c.apiKey != "" {
		dialHeader = http.Header{"Authorization": []string{"Bearer " + c.apiKey}}
	}
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, c.config.WSURL, dialHeader)
	if resp != nil && resp.Body != nil {
		defer func() { _ = resp.Body.Close() }()
	}
	if err != nil {
		c.mu.Lock()
		c.state = StateDisconnected
		c.mu.Unlock()
		return fmt.Errorf("failed to connect: %w", err)
	}

	c.mu.Lock()
	wasReconnecting := c.reconnectAttempt > 0
	c.conn = conn
	c.state = StateConnected
	c.reconnectAttempt = 0
	callback := c.onConnectionChange
	c.mu.Unlock()

	if wasReconnecting {
		c.logger.Info("Reconnected to server")
	} else {
		c.logger.Info("Connected to server")
	}

	if callback != nil {
		callback(true)
	}

	// Start message reader (uses c.done for lifecycle, not context)
	go c.readMessages() //nolint:contextcheck

	// Restore accepted subscriptions, then retry initial requests whose result
	// was interrupted by the previous connection.
	c.resubscribeAll()
	if wasReconnecting {
		c.resendPendingInitialSubscriptions()
	}

	return nil
}

// Close closes the WebSocket connection.
func (c *SubscriptionClient) Close() {
	c.closeOnce.Do(func() {
		close(c.done)

		c.mu.Lock()
		conn := c.conn
		c.conn = nil
		c.state = StateDisconnected

		// Close all subscriptions
		for _, sub := range c.subscriptions {
			close(sub.events)
			close(sub.errors)
		}
		c.subscriptions = make(map[string]*Subscription)
		c.subByPattern = make(map[string]string)
		c.mu.Unlock()
		c.clearPendingSubscribeAttempts()

		if conn != nil {
			c.writeMu.Lock()
			_ = conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			_ = conn.Close()
			c.writeMu.Unlock()
		}
	})
}

func cloneSubscribeOptions(opts *SubscribeOptions) *SubscribeOptions {
	if opts == nil {
		return nil
	}

	cloned := *opts
	if opts.StartAfterSequence != nil {
		cursor := *opts.StartAfterSequence
		cloned.StartAfterSequence = &cursor
	}
	return &cloned
}

func reconnectSubscribeOptions(opts *SubscribeOptions) *SubscribeOptions {
	cloned := cloneSubscribeOptions(opts)
	if cloned != nil {
		cloned.Replay = 0
	}
	return cloned
}

func newWSSubscribeRequest(pattern string, opts *SubscribeOptions) wsSubscribeRequest {
	req := wsSubscribeRequest{
		Type: "subscribe",
		Subscription: wsSubscription{
			Pattern: pattern,
		},
	}
	if opts == nil {
		return req
	}

	needsOptions := opts.Replay > 0 || opts.StartAfterSequence != nil ||
		opts.IncludeMetadata || opts.Filter != "" || opts.ConsumerGroup != "" ||
		opts.AckMode != "" || opts.Backpressure != "" || opts.Namespace != ""
	if !needsOptions {
		return req
	}

	req.Subscription.Options = &wsSubscriptionOptions{
		Replay:             opts.Replay,
		StartAfterSequence: opts.StartAfterSequence,
		IncludeMetadata:    opts.IncludeMetadata || opts.StartAfterSequence != nil,
		Filter:             opts.Filter,
		ConsumerGroup:      opts.ConsumerGroup,
		AckMode:            string(opts.AckMode),
		Backpressure:       string(opts.Backpressure),
		Namespace:          opts.Namespace,
	}
	return req
}

// Subscribe subscribes to events matching a pattern.
//
// Returns a Subscription with Events() and Errors() channels.
// The caller should read from Events() in a goroutine.
//
// Example:
//
//	sub, err := client.Subscribe(ctx, "system.run.>", &ironflow.SubscribeOptions{
//	    Replay: 10,
//	})
//	if err != nil {
//	    return err
//	}
//	defer sub.Unsubscribe()
//
//	for {
//	    select {
//	    case event := <-sub.Events():
//	        fmt.Printf("Event: %s\n", event.Topic)
//	    case err := <-sub.Errors():
//	        fmt.Printf("Error: %s\n", err.Message)
//	    case <-ctx.Done():
//	        return ctx.Err()
//	    }
//	}
func (c *SubscriptionClient) Subscribe(ctx context.Context, pattern string, opts *SubscribeOptions) (*Subscription, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected to server")
	}

	requestOptions := cloneSubscribeOptions(opts)
	attempt := &pendingSubscribeAttempt{
		result:     make(chan subscribeResult, 1),
		done:       make(chan struct{}),
		canceledCh: make(chan struct{}),
		options:    requestOptions,
	}
	if err := c.reserveSubscribeAttempt(ctx, pattern, attempt); err != nil {
		return nil, err
	}

	req := newWSSubscribeRequest(pattern, requestOptions)

	// Send request
	if err := c.sendJSON(req); err != nil {
		c.cancelSubscribeAttempt(pattern, attempt)
		return nil, fmt.Errorf("failed to send subscribe request: %w", err)
	}

	// Wait for result
	select {
	case result := <-attempt.result:
		if result.err != nil {
			c.completeSubscribeAttempt(pattern, attempt)
			return nil, result.err
		}

		// Create subscription
		sub := &Subscription{
			ID:      result.subscriptionID,
			Pattern: pattern,
			events:  make(chan *SubscriptionEvent, 100),
			errors:  make(chan *SubscriptionError, 10),
			client:  c,
			options: requestOptions,
			done:    make(chan struct{}),
		}

		c.mu.Lock()
		c.subscriptions[sub.ID] = sub
		c.subByPattern[pattern] = sub.ID
		c.mu.Unlock()
		c.completeSubscribeAttempt(pattern, attempt)

		return sub, nil

	case <-ctx.Done():
		c.cancelSubscribeAttempt(pattern, attempt)
		return nil, ctx.Err()

	case <-c.done:
		c.cancelSubscribeAttempt(pattern, attempt)
		return nil, fmt.Errorf("client closed")
	}
}

func (c *SubscriptionClient) reserveSubscribeAttempt(
	ctx context.Context,
	pattern string,
	attempt *pendingSubscribeAttempt,
) error {
	for {
		c.mu.RLock()
		_, active := c.subByPattern[pattern]
		c.mu.RUnlock()
		if active {
			return fmt.Errorf("already subscribed to pattern: %s", pattern)
		}

		c.pendingMu.Lock()
		previous := c.pending[pattern]
		if previous == nil {
			c.pending[pattern] = attempt
			c.pendingMu.Unlock()
			return nil
		}
		previousDone := previous.done
		c.pendingMu.Unlock()

		select {
		case <-previousDone:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.done:
			return fmt.Errorf("client closed")
		}
	}
}

func (c *SubscriptionClient) completeSubscribeAttempt(
	pattern string,
	attempt *pendingSubscribeAttempt,
) {
	c.pendingMu.Lock()
	if c.pending[pattern] == attempt {
		delete(c.pending, pattern)
		close(attempt.done)
	}
	c.pendingMu.Unlock()
}

func (c *SubscriptionClient) cancelSubscribeAttempt(
	pattern string,
	attempt *pendingSubscribeAttempt,
) {
	c.pendingMu.Lock()
	if c.pending[pattern] == attempt && !attempt.canceled {
		attempt.canceled = true
		close(attempt.canceledCh)
	}
	c.pendingMu.Unlock()
}

// SubscribeAckable subscribes to events with manual acknowledgment support.
//
// This method forces AckMode to manual and returns an AckableSubscription
// with Ack, Nak, and Term methods.
//
// Example:
//
//	sub, err := client.SubscribeAckable(ctx, "order.*", &ironflow.SubscribeOptions{
//	    ConsumerGroup: "order-processors",
//	})
//	if err != nil {
//	    return err
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
func (c *SubscriptionClient) SubscribeAckable(ctx context.Context, pattern string, opts *SubscribeOptions) (*AckableSubscription, error) {
	if opts == nil {
		opts = &SubscribeOptions{}
	}
	opts.AckMode = AckModeManual

	sub, err := c.Subscribe(ctx, pattern, opts)
	if err != nil {
		return nil, err
	}

	return &AckableSubscription{Subscription: sub}, nil
}

// SubscribeEntityStream subscribes to real-time events for a specific entity stream.
// It constructs the pattern entity:{entityType}.{entityID}.> and delegates to Subscribe.
//
// If opts.OnEvent is set, a goroutine is started that reads from the subscription's
// Events() channel, unmarshals each event into a StreamEvent, and calls OnEvent.
// If opts.OnEvent is nil, use the returned Subscription's Events() channel directly.
//
// Example:
//
//	sub, err := client.SubscribeEntityStream(ctx, "order-123", EntitySubscribeOptions{
//	    EntityType: "order",
//	    Replay:     100,
//	    OnEvent:    func(e StreamEvent) { fmt.Println(e.Name, e.EntityVersion) },
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer sub.Unsubscribe()
func (c *SubscriptionClient) SubscribeEntityStream(ctx context.Context, entityID string, opts EntitySubscribeOptions) (*Subscription, error) {
	if opts.EntityType == "" {
		return nil, fmt.Errorf("EntityType is required")
	}

	pattern := "entity:" + opts.EntityType + "." + entityID + ".>"

	subOpts := &SubscribeOptions{}
	if opts.Replay > 0 {
		subOpts.Replay = opts.Replay
	}

	sub, err := c.Subscribe(ctx, pattern, subOpts)
	if err != nil {
		return nil, err
	}

	// If OnEvent is set, start a goroutine to unmarshal and forward events
	if opts.OnEvent != nil {
		go func() {
			for event := range sub.Events() {
				var streamEvt StreamEvent
				if err := json.Unmarshal(event.Data, &streamEvt); err != nil {
					if opts.OnError != nil {
						opts.OnError(fmt.Errorf("failed to unmarshal entity event: %w", err))
					}
					continue
				}
				opts.OnEvent(streamEvt)
			}
		}()
	}

	// If OnError is set, start a goroutine to forward subscription errors
	if opts.OnError != nil {
		go func() {
			for subErr := range sub.Errors() {
				opts.OnError(fmt.Errorf("subscription error: %s", subErr.Message))
			}
		}()
	}

	return sub, nil
}

func (c *SubscriptionClient) currentSubscriptionID(pattern, fallback string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if subscriptionID := c.subByPattern[pattern]; subscriptionID != "" {
		return subscriptionID
	}
	return fallback
}

// unsubscribeByPattern resolves and removes the current server subscription ID
// atomically with respect to reconnect rebinding.
func (c *SubscriptionClient) unsubscribeByPattern(pattern string) {
	c.mu.Lock()
	subscriptionID := c.subByPattern[pattern]
	sub, exists := c.subscriptions[subscriptionID]
	if exists {
		delete(c.subscriptions, subscriptionID)
		delete(c.subByPattern, pattern)
		close(sub.events)
		close(sub.errors)
	}
	conn := c.conn
	c.mu.Unlock()

	if exists && conn != nil {
		req := wsUnsubscribeRequest{
			Type:           "unsubscribe",
			SubscriptionID: subscriptionID,
		}
		_ = c.sendJSON(req)
	}
}

// sendJSON sends a JSON message
func (c *SubscriptionClient) sendJSON(v any) error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if conn == nil {
		return fmt.Errorf("not connected")
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return conn.WriteJSON(v)
}

// readMessages reads messages from the WebSocket
func (c *SubscriptionClient) readMessages() {
	for {
		select {
		case <-c.done:
			return
		default:
		}

		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			return
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-c.done:
				return
			default:
			}

			c.mu.Lock()
			wasConnected := c.state == StateConnected
			c.state = StateDisconnected
			c.conn = nil
			callback := c.onConnectionChange
			autoReconnect := c.config.AutoReconnect
			if !autoReconnect {
				for _, sub := range c.subscriptions {
					if sub.options != nil && sub.options.StartAfterSequence != nil {
						autoReconnect = true
						break
					}
				}
			}
			c.mu.Unlock()
			if !autoReconnect && c.hasPendingCursorSubscription() {
				autoReconnect = true
			}
			if autoReconnect {
				c.discardPendingResubscribeAttempts()
			} else {
				c.failPendingSubscribeAttempts(fmt.Errorf("connection lost: %w", err))
			}

			if wasConnected {
				c.logger.Warn("Connection lost", "error", err)
				if callback != nil {
					callback(false)
				}
			}

			if autoReconnect {
				go c.scheduleReconnect()
			}
			return
		}

		c.handleMessage(data)
	}
}

func (c *SubscriptionClient) failPendingSubscribeAttempts(err error) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	for pattern, attempt := range c.pending {
		if attempt.resubscribe != nil {
			delete(c.pending, pattern)
			close(attempt.done)
			continue
		}
		if attempt.canceled {
			delete(c.pending, pattern)
			close(attempt.done)
			continue
		}
		select {
		case attempt.result <- subscribeResult{err: err}:
		default:
		}
	}
}

func (c *SubscriptionClient) clearPendingSubscribeAttempts() {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	for pattern, attempt := range c.pending {
		delete(c.pending, pattern)
		close(attempt.done)
	}
}

func (c *SubscriptionClient) hasPendingCursorSubscription() bool {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	for _, attempt := range c.pending {
		if attempt.resubscribe == nil && !attempt.canceled &&
			attempt.options != nil &&
			attempt.options.StartAfterSequence != nil {
			return true
		}
	}
	return false
}

func (c *SubscriptionClient) discardPendingResubscribeAttempts() {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()

	for pattern, attempt := range c.pending {
		if attempt.resubscribe != nil || attempt.canceled {
			delete(c.pending, pattern)
			close(attempt.done)
		}
	}
}

func (c *SubscriptionClient) resendPendingInitialSubscriptions() {
	type pendingRequest struct {
		pattern string
		attempt *pendingSubscribeAttempt
	}

	c.pendingMu.Lock()
	requests := make([]pendingRequest, 0, len(c.pending))
	for pattern, attempt := range c.pending {
		if attempt.resubscribe != nil {
			continue
		}
		if attempt.canceled {
			delete(c.pending, pattern)
			close(attempt.done)
			continue
		}
		requests = append(requests, pendingRequest{pattern: pattern, attempt: attempt})
	}
	c.pendingMu.Unlock()

	for _, pending := range requests {
		if err := c.sendJSON(newWSSubscribeRequest(pending.pattern, pending.attempt.options)); err != nil {
			select {
			case pending.attempt.result <- subscribeResult{
				err: fmt.Errorf("failed to resend subscribe request: %w", err),
			}:
			case <-pending.attempt.done:
			case <-c.done:
			default:
			}
		}
	}
}

// handleMessage processes an incoming message
func (c *SubscriptionClient) handleMessage(data []byte) {
	var msg wsMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}

	switch msg.Type {
	case "subscription_result":
		c.handleSubscriptionResult(data)
	case "event":
		c.handleEvent(data)
	case "subscription_error":
		c.handleSubscriptionError(data)
	case "error":
		c.handleError(data)
	}
}

// handleSubscriptionResult handles subscription result messages
func (c *SubscriptionClient) handleSubscriptionResult(data []byte) {
	var msg wsSubscriptionResultMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}

	for _, result := range msg.Results {
		c.pendingMu.Lock()
		attempt := c.pending[result.Pattern]
		c.pendingMu.Unlock()

		if attempt == nil {
			continue
		}

		if attempt.resubscribe == nil {
			c.pendingMu.Lock()
			canceled := attempt.canceled
			c.pendingMu.Unlock()
			if canceled {
				if result.Status == "ok" {
					_ = c.sendJSON(wsUnsubscribeRequest{
						Type:           "unsubscribe",
						SubscriptionID: result.SubscriptionID,
					})
				}
				c.completeSubscribeAttempt(result.Pattern, attempt)
				continue
			}

			var delivery subscribeResult
			if result.Status == "ok" {
				delivery = subscribeResult{subscriptionID: result.SubscriptionID}
			} else {
				delivery = subscribeResult{
					err: fmt.Errorf("[%s] %s", result.Code, result.Message),
				}
			}
			select {
			case attempt.result <- delivery:
			case <-attempt.done:
			case <-c.done:
			default:
			}
			// The reader must not process a following event or connection loss
			// until Subscribe has installed the accepted ID (or abandoned the
			// attempt). This also prevents a reconnect from resending an already
			// accepted initial request during that handoff window.
			select {
			case <-attempt.done:
			case <-attempt.canceledCh:
				if result.Status == "ok" {
					_ = c.sendJSON(wsUnsubscribeRequest{
						Type:           "unsubscribe",
						SubscriptionID: result.SubscriptionID,
					})
				}
				c.completeSubscribeAttempt(result.Pattern, attempt)
			case <-c.done:
			}
			continue
		}

		oldSub := attempt.resubscribe
		if result.Status == "ok" {
			// A reconnect gets a fresh server subscription ID. Rebind it before
			// returning to the reader so the next event cannot arrive against an
			// ID the client does not know yet.
			rebound := false
			c.mu.Lock()
			if previousID := c.subByPattern[result.Pattern]; previousID != "" {
				if c.subscriptions[previousID] == oldSub {
					delete(c.subscriptions, previousID)
					c.subscriptions[result.SubscriptionID] = oldSub
					c.subByPattern[result.Pattern] = result.SubscriptionID
					rebound = true
				}
			}
			c.mu.Unlock()
			if !rebound {
				_ = c.sendJSON(wsUnsubscribeRequest{
					Type:           "unsubscribe",
					SubscriptionID: result.SubscriptionID,
				})
			}
		} else {
			c.terminateWebSocketSubscription(oldSub, &SubscriptionError{
				Code:    "RESUBSCRIBE_FAILED",
				Message: fmt.Sprintf("[%s] %s", result.Code, result.Message),
			})
		}
		c.completeSubscribeAttempt(result.Pattern, attempt)
	}
}

func (c *SubscriptionClient) reportWebSocketSubscriptionError(
	sub *Subscription,
	subErr *SubscriptionError,
) {
	c.mu.RLock()
	currentID := c.subByPattern[sub.Pattern]
	if currentID == "" || c.subscriptions[currentID] != sub {
		c.mu.RUnlock()
		return
	}
	defer c.mu.RUnlock()

	select {
	case sub.errors <- subErr:
	case <-sub.done:
	case <-c.done:
	default:
	}
}

func (c *SubscriptionClient) terminateWebSocketSubscription(
	sub *Subscription,
	subErr *SubscriptionError,
) {
	c.mu.Lock()
	currentID := c.subByPattern[sub.Pattern]
	if currentID == "" || c.subscriptions[currentID] != sub {
		c.mu.Unlock()
		return
	}
	delete(c.subscriptions, currentID)
	delete(c.subByPattern, sub.Pattern)
	subErr.SubscriptionID = currentID
	select {
	case sub.errors <- subErr:
	default:
	}
	sub.once.Do(func() { close(sub.done) })
	close(sub.events)
	close(sub.errors)
	c.mu.Unlock()
}

// handleEvent handles event messages
func (c *SubscriptionClient) handleEvent(data []byte) {
	var msg wsEventMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}

	c.mu.RLock()
	sub, exists := c.subscriptions[msg.SubscriptionID]
	c.mu.RUnlock()

	if !exists {
		return
	}

	event := &SubscriptionEvent{
		ID:    msg.EventID,
		Topic: msg.Topic,
		Data:  msg.Data,
		Meta:  msg.Meta,
	}

	select {
	case sub.events <- event:
		if event.Meta != nil && event.Meta.Sequence > 0 {
			c.mu.Lock()
			if sub.options != nil && sub.options.StartAfterSequence != nil {
				cursor := event.Meta.Sequence
				sub.options.StartAfterSequence = &cursor
				sub.options.Replay = 0
			}
			c.mu.Unlock()
		}
	case <-sub.done:
	default:
		// Channel full, drop event
	}
}

// handleSubscriptionError handles subscription error messages
func (c *SubscriptionClient) handleSubscriptionError(data []byte) {
	var msg wsSubscriptionErrorMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}

	c.mu.RLock()
	sub, exists := c.subscriptions[msg.SubscriptionID]
	c.mu.RUnlock()

	if !exists {
		return
	}

	subErr := &SubscriptionError{
		SubscriptionID: msg.SubscriptionID,
		Code:           msg.Code,
		Message:        msg.Message,
		Retrying:       msg.Retrying,
	}

	select {
	case sub.errors <- subErr:
	case <-sub.done:
	default:
		// Channel full, drop error
	}
}

// handleError handles general error messages
func (c *SubscriptionClient) handleError(data []byte) {
	var msg wsErrorMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}

	subErr := &SubscriptionError{
		Code:    msg.Code,
		Message: msg.Message,
	}

	c.mu.RLock()
	subs := make([]*Subscription, 0, len(c.subscriptions))
	for _, sub := range c.subscriptions {
		subs = append(subs, sub)
	}
	c.mu.RUnlock()

	for _, sub := range subs {
		select {
		case sub.errors <- subErr:
		case <-sub.done:
		default:
		}
	}
}

// scheduleReconnect schedules a reconnection attempt
func (c *SubscriptionClient) scheduleReconnect() {
	c.mu.Lock()
	if c.state == StateReconnecting {
		c.mu.Unlock()
		return
	}
	c.state = StateReconnecting
	c.reconnectAttempt++
	attempt := c.reconnectAttempt
	c.mu.Unlock()

	// Calculate delay with exponential backoff
	delay := float64(c.config.ReconnectDelay)
	for i := 1; i < attempt; i++ {
		delay *= c.config.ReconnectBackoff
	}
	if delay > float64(c.config.MaxReconnectDelay) {
		delay = float64(c.config.MaxReconnectDelay)
	}

	c.logger.Info("Reconnecting...", "delay", time.Duration(delay), "attempt", attempt)

	select {
	case <-time.After(time.Duration(delay)):
	case <-c.done:
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Connect(ctx); err != nil {
		c.logger.Warn("Reconnect failed", "error", err)
		if c.shouldReconnectSubscriptions() {
			go c.scheduleReconnect()
		}
	}
}

func (c *SubscriptionClient) shouldReconnectSubscriptions() bool {
	if c.config.AutoReconnect {
		return true
	}

	c.mu.RLock()
	for _, sub := range c.subscriptions {
		if sub.options != nil && sub.options.StartAfterSequence != nil {
			c.mu.RUnlock()
			return true
		}
	}
	c.mu.RUnlock()
	return c.hasPendingCursorSubscription()
}

// resubscribeAll resubscribes all active subscriptions
func (c *SubscriptionClient) resubscribeAll() {
	c.mu.RLock()
	patterns := make([]string, 0, len(c.subByPattern))
	for pattern := range c.subByPattern {
		patterns = append(patterns, pattern)
	}
	oldSubs := make(map[string]*Subscription)
	maps.Copy(oldSubs, c.subscriptions)
	c.mu.RUnlock()

	for _, pattern := range patterns {
		// Find old subscription
		var oldSub *Subscription
		for _, sub := range oldSubs {
			if sub.Pattern == pattern {
				oldSub = sub
				break
			}
		}
		if oldSub == nil {
			continue
		}

		attempt := &pendingSubscribeAttempt{
			done:        make(chan struct{}),
			resubscribe: oldSub,
		}

		c.pendingMu.Lock()
		if c.pending[pattern] != nil {
			c.pendingMu.Unlock()
			continue
		}
		c.pending[pattern] = attempt
		c.pendingMu.Unlock()

		// Preserve the subscription identity. Replay only positions the initial
		// subscription, so a reconnect must not apply it again.
		c.mu.RLock()
		reconnectOptions := reconnectSubscribeOptions(oldSub.options)
		c.mu.RUnlock()
		req := newWSSubscribeRequest(pattern, reconnectOptions)

		if err := c.sendJSON(req); err != nil {
			c.reportWebSocketSubscriptionError(oldSub, &SubscriptionError{
				Code:    "RESUBSCRIBE_FAILED",
				Message: err.Error(),
			})
			c.completeSubscribeAttempt(pattern, attempt)
			continue
		}
	}
}

// ============================================================================
// WebSocket Protocol Types
// ============================================================================

type wsMessage struct {
	Type string `json:"type"`
}

type wsSubscribeRequest struct {
	Type         string         `json:"type"`
	Subscription wsSubscription `json:"subscription"`
}

type wsSubscription struct {
	Pattern string                 `json:"pattern"`
	Options *wsSubscriptionOptions `json:"options,omitempty"`
}

type wsSubscriptionOptions struct {
	Replay             int     `json:"replay,omitempty"`
	StartAfterSequence *uint64 `json:"startAfterSequence,omitempty"`
	IncludeMetadata    bool    `json:"includeMetadata,omitempty"`
	Filter             string  `json:"filter,omitempty"`
	ConsumerGroup      string  `json:"consumerGroup,omitempty"`
	AckMode            string  `json:"ackMode,omitempty"`
	Backpressure       string  `json:"backpressure,omitempty"`
	Namespace          string  `json:"namespace,omitempty"`
}

type wsUnsubscribeRequest struct {
	Type           string `json:"type"`
	SubscriptionID string `json:"subscriptionId"`
}

// wsAckRequest is the unified acknowledgment request sent to the server.
// The server expects Type="ack" with a separate AckType field indicating
// the acknowledgment type ("ack", "nak", or "term").
type wsAckRequest struct {
	Type           string `json:"type"`                     // Always "ack"
	AckType        string `json:"ackType"`                  // "ack", "nak", or "term"
	EventID        string `json:"eventId"`                  // Event being acknowledged
	RedeliverDelay int    `json:"redeliverDelay,omitempty"` // For nak: delay in ms before redelivery
}

type wsSubscriptionResultMessage struct {
	Type    string `json:"type"`
	Results []struct {
		Pattern        string `json:"pattern"`
		Status         string `json:"status"`
		SubscriptionID string `json:"subscriptionId,omitempty"`
		Code           string `json:"code,omitempty"`
		Message        string `json:"message,omitempty"`
	} `json:"results"`
}

type wsEventMessage struct {
	Type           string          `json:"type"`
	SubscriptionID string          `json:"subscriptionId"`
	EventID        string          `json:"eventId,omitempty"`
	Topic          string          `json:"topic"`
	Data           json.RawMessage `json:"data"`
	Meta           *EventMeta      `json:"meta,omitempty"`
}

type wsSubscriptionErrorMessage struct {
	Type           string `json:"type"`
	SubscriptionID string `json:"subscriptionId"`
	Code           string `json:"code"`
	Message        string `json:"message"`
	Retrying       bool   `json:"retrying"`
}

type wsErrorMessage struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ============================================================================
// Client Helper
// ============================================================================

// CreateSubscriptionClient creates a subscription client from the API client.
//
// This is a convenience method that derives the WebSocket URL from the
// client's server URL.
func (c *Client) CreateSubscriptionClient() *SubscriptionClient {
	wsURL := c.serverURL
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)

	// Parse and add /ws path
	if u, err := url.Parse(wsURL); err == nil {
		u.Path = "/ws"
		wsURL = u.String()
	} else {
		wsURL = wsURL + "/ws"
	}

	sub := NewSubscriptionClient(SubscriptionClientConfig{
		WSURL:         wsURL,
		AutoReconnect: true,
	})
	// /ws requires auth, so a derived client that dropped the parent's key
	// connected as an anonymous caller and 401'd.
	if c.apiKey != "" {
		sub.apiKey = c.apiKey
	}
	return sub
}
