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
	// ID is the subscription ID.
	ID string

	// Pattern is the subscription pattern.
	Pattern string

	// Events returns a channel receiving events.
	events chan *SubscriptionEvent

	// Errors returns a channel receiving errors.
	errors chan *SubscriptionError

	// client is the parent client
	client *SubscriptionClient

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

// Unsubscribe stops the subscription.
func (s *Subscription) Unsubscribe() {
	s.once.Do(func() {
		close(s.done)
		s.client.unsubscribe(s.ID)
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

	// AutoReconnect enables automatic reconnection (default: true).
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
	pending       map[string]chan subscribeResult // pattern -> result channel
	subscriptions map[string]*Subscription        // subscriptionID -> subscription
	subByPattern  map[string]string               // pattern -> subscriptionID

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
		pending:       make(map[string]chan subscribeResult),
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

	// Resubscribe any existing subscriptions
	c.resubscribeAll()

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

		if conn != nil {
			_ = conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			_ = conn.Close()
		}
	})
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

	// Check for duplicate subscription
	c.mu.RLock()
	if _, exists := c.subByPattern[pattern]; exists {
		c.mu.RUnlock()
		return nil, fmt.Errorf("already subscribed to pattern: %s", pattern)
	}
	c.mu.RUnlock()

	// Create result channel
	resultCh := make(chan subscribeResult, 1)

	c.pendingMu.Lock()
	c.pending[pattern] = resultCh
	c.pendingMu.Unlock()

	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, pattern)
		c.pendingMu.Unlock()
	}()

	// Build subscribe request
	req := wsSubscribeRequest{
		Type: "subscribe",
		Subscription: wsSubscription{
			Pattern: pattern,
		},
	}

	if opts != nil {
		needsOptions := opts.Replay > 0 || opts.IncludeMetadata || opts.Filter != "" ||
			opts.ConsumerGroup != "" || opts.AckMode != "" || opts.Backpressure != "" ||
			opts.Namespace != ""
		if needsOptions {
			req.Subscription.Options = &wsSubscriptionOptions{}
			if opts.Replay > 0 {
				req.Subscription.Options.Replay = opts.Replay
			}
			if opts.IncludeMetadata {
				req.Subscription.Options.IncludeMetadata = opts.IncludeMetadata
			}
			if opts.Filter != "" {
				req.Subscription.Options.Filter = opts.Filter
			}
			if opts.ConsumerGroup != "" {
				req.Subscription.Options.ConsumerGroup = opts.ConsumerGroup
			}
			if opts.AckMode != "" {
				req.Subscription.Options.AckMode = string(opts.AckMode)
			}
			if opts.Backpressure != "" {
				req.Subscription.Options.Backpressure = string(opts.Backpressure)
			}
			if opts.Namespace != "" {
				req.Subscription.Options.Namespace = opts.Namespace
			}
		}
	}

	// Send request
	if err := c.sendJSON(req); err != nil {
		return nil, fmt.Errorf("failed to send subscribe request: %w", err)
	}

	// Wait for result
	select {
	case result := <-resultCh:
		if result.err != nil {
			return nil, result.err
		}

		// Create subscription
		sub := &Subscription{
			ID:      result.subscriptionID,
			Pattern: pattern,
			events:  make(chan *SubscriptionEvent, 100),
			errors:  make(chan *SubscriptionError, 10),
			client:  c,
			done:    make(chan struct{}),
		}

		c.mu.Lock()
		c.subscriptions[sub.ID] = sub
		c.subByPattern[pattern] = sub.ID
		c.mu.Unlock()

		return sub, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-c.done:
		return nil, fmt.Errorf("client closed")
	}
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

// unsubscribe removes a subscription
func (c *SubscriptionClient) unsubscribe(subscriptionID string) {
	c.mu.Lock()
	sub, exists := c.subscriptions[subscriptionID]
	if exists {
		delete(c.subscriptions, subscriptionID)
		delete(c.subByPattern, sub.Pattern)
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
			c.mu.Unlock()

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
		resultCh, exists := c.pending[result.Pattern]
		c.pendingMu.Unlock()

		if !exists {
			continue
		}

		if result.Status == "ok" {
			resultCh <- subscribeResult{subscriptionID: result.SubscriptionID}
		} else {
			resultCh <- subscribeResult{
				err: fmt.Errorf("[%s] %s", result.Code, result.Message),
			}
		}
	}
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

	// Errors are handled by the connection callback and readMessages loop
	_ = c.Connect(ctx)
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

	// Clear old subscriptions but keep the Subscription objects
	c.mu.Lock()
	c.subscriptions = make(map[string]*Subscription)
	c.subByPattern = make(map[string]string)
	c.mu.Unlock()

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

		// Create result channel
		resultCh := make(chan subscribeResult, 1)

		c.pendingMu.Lock()
		c.pending[pattern] = resultCh
		c.pendingMu.Unlock()

		// Send subscribe request
		req := wsSubscribeRequest{
			Type: "subscribe",
			Subscription: wsSubscription{
				Pattern: pattern,
			},
		}

		if err := c.sendJSON(req); err != nil {
			oldSub.errors <- &SubscriptionError{
				Code:    "RESUBSCRIBE_FAILED",
				Message: err.Error(),
			}
			c.pendingMu.Lock()
			delete(c.pending, pattern)
			c.pendingMu.Unlock()
			continue
		}

		// Wait for result in background
		go func(pattern string, oldSub *Subscription) {
			defer func() {
				c.pendingMu.Lock()
				delete(c.pending, pattern)
				c.pendingMu.Unlock()
			}()

			select {
			case result := <-resultCh:
				if result.err != nil {
					select {
					case oldSub.errors <- &SubscriptionError{
						Code:    "RESUBSCRIBE_FAILED",
						Message: result.err.Error(),
					}:
					default:
					}
					return
				}

				// Update subscription with new ID
				c.mu.Lock()
				oldSub.ID = result.subscriptionID
				c.subscriptions[result.subscriptionID] = oldSub
				c.subByPattern[pattern] = result.subscriptionID
				c.mu.Unlock()

			case <-c.done:
				return
			}
		}(pattern, oldSub)
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
	Replay          int    `json:"replay,omitempty"`
	IncludeMetadata bool   `json:"includeMetadata,omitempty"`
	Filter          string `json:"filter,omitempty"`
	ConsumerGroup   string `json:"consumerGroup,omitempty"`
	AckMode         string `json:"ackMode,omitempty"`
	Backpressure    string `json:"backpressure,omitempty"`
	Namespace       string `json:"namespace,omitempty"`
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
