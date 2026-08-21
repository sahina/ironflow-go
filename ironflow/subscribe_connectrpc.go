package ironflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
)

// GrpcSubscriptionClientConfig configures a gRPC subscription client.
type GrpcSubscriptionClientConfig struct {
	// ServerURL is the server URL (e.g., "http://localhost:9123").
	ServerURL string

	// AutoReconnect enables automatic reconnection (default: true).
	AutoReconnect bool

	// ReconnectDelay is the initial reconnect delay (default: 1s).
	ReconnectDelay time.Duration

	// MaxReconnectDelay is the maximum reconnect delay (default: 30s).
	MaxReconnectDelay time.Duration

	// ReconnectBackoff is the backoff multiplier (default: 1.5).
	ReconnectBackoff float64

	// Logger is the logger to use. If nil, uses the default console logger.
	Logger Logger
}

// GrpcSubscriptionClient is a client for streaming subscriptions using ConnectRPC.
//
// This uses the generated ConnectRPC PubSubServiceClient for server-streaming
// subscriptions and plain HTTP POST for acknowledgments.
//
// Example:
//
//	client := ironflow.NewGrpcSubscriptionClient(ironflow.GrpcSubscriptionClientConfig{
//	    ServerURL: "http://localhost:9123",
//	})
//
//	if err := client.Connect(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	sub, err := client.Subscribe(ctx, "order.*", &ironflow.SubscribeOptions{
//	    Filter: `data.total > 100`,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	for event := range sub.Events() {
//	    fmt.Printf("Event: %s\n", event.Topic)
//	}
type GrpcSubscriptionClient struct {
	config       GrpcSubscriptionClientConfig
	logger       Logger
	httpClient   *http.Client
	pubsubClient ironflowv1connect.PubSubServiceClient
	// apiKey is what the bearer interceptor was built with. Kept so the
	// resolution is assertable; the interceptor itself closes over a copy.
	apiKey string

	mu               sync.RWMutex
	state            ConnectionState
	reconnectAttempt int

	// Subscription tracking
	subscriptions map[string]*GrpcSubscription
	subIDCounter  int

	// Connection callbacks
	onConnectionChange func(connected bool)

	// Channels for coordination
	done      chan struct{}
	closeOnce sync.Once
}

// GrpcSubscription represents an active ConnectRPC streaming subscription.
type GrpcSubscription struct {
	// ID is the subscription ID.
	ID string

	// Pattern is the subscription pattern.
	Pattern string

	// events channel for receiving events
	events chan *SubscriptionEvent

	// errors channel for receiving errors
	errors chan *SubscriptionError

	// client is the parent client
	client *GrpcSubscriptionClient

	// cancel cancels the subscription context
	cancel context.CancelFunc

	// done signals subscription closure
	done chan struct{}
	once sync.Once
}

// Events returns a channel for receiving subscription events.
func (s *GrpcSubscription) Events() <-chan *SubscriptionEvent {
	return s.events
}

// Errors returns a channel for receiving subscription errors.
func (s *GrpcSubscription) Errors() <-chan *SubscriptionError {
	return s.errors
}

// Unsubscribe stops the subscription.
func (s *GrpcSubscription) Unsubscribe() {
	s.once.Do(func() {
		close(s.done)
		s.cancel()
		s.client.removeSubscription(s.ID)
	})
}

// NewGrpcSubscriptionClient creates a new gRPC subscription client.
//
// ConnectRPC routes require auth. GrpcSubscriptionClientConfig has no key field
// yet, so this constructor authenticates from IRONFLOW_API_KEY;
// Client.CreateGrpcSubscriptionClient passes the parent client's resolved key.
func NewGrpcSubscriptionClient(config GrpcSubscriptionClientConfig) *GrpcSubscriptionClient {
	return newGrpcSubscriptionClient(config, GetAPIKey())
}

func newGrpcSubscriptionClient(config GrpcSubscriptionClientConfig, apiKey string) *GrpcSubscriptionClient {
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
		logger = NewLogger(LoggerConfig{Prefix: "[ironflow-grpc-subscribe]"})
	}

	httpClient := newH2CClient(config.ServerURL)

	opts := []connect.ClientOption{connect.WithProtoJSON()}
	if apiKey != "" {
		opts = append(opts, connect.WithInterceptors(bearerInterceptor(apiKey)))
	}
	pubsubClient := ironflowv1connect.NewPubSubServiceClient(
		httpClient,
		config.ServerURL,
		opts...,
	)

	return &GrpcSubscriptionClient{
		config:        config,
		logger:        logger,
		httpClient:    httpClient,
		pubsubClient:  pubsubClient,
		apiKey:        apiKey,
		state:         StateDisconnected,
		subscriptions: make(map[string]*GrpcSubscription),
		done:          make(chan struct{}),
	}
}

// bearerAuth attaches an Authorization header to every ConnectRPC request.
// A plain connect.UnaryInterceptorFunc would not do: PubSubService.Subscribe is
// server-streaming, and the streaming path needs its own wrapper.
type bearerAuth struct{ key string }

func bearerInterceptor(key string) connect.Interceptor { return bearerAuth{key: key} }

func (b bearerAuth) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set("Authorization", "Bearer "+b.key)
		return next(ctx, req)
	}
}

func (b bearerAuth) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set("Authorization", "Bearer "+b.key)
		return conn
	}
}

func (b bearerAuth) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// State returns the current connection state.
func (c *GrpcSubscriptionClient) State() ConnectionState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// IsConnected returns true if connected to the server.
func (c *GrpcSubscriptionClient) IsConnected() bool {
	return c.State() == StateConnected
}

// SetConnectionCallback sets a callback for connection state changes.
func (c *GrpcSubscriptionClient) SetConnectionCallback(callback func(connected bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onConnectionChange = callback
}

// Connect establishes readiness for subscriptions.
func (c *GrpcSubscriptionClient) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.state == StateConnected {
		c.mu.Unlock()
		return nil
	}
	c.state = StateConnected
	c.reconnectAttempt = 0
	callback := c.onConnectionChange
	c.mu.Unlock()

	c.logger.Info("Ready for ConnectRPC streaming subscriptions")

	if callback != nil {
		callback(true)
	}

	return nil
}

// Close closes all subscriptions.
func (c *GrpcSubscriptionClient) Close() {
	c.closeOnce.Do(func() {
		close(c.done)

		c.mu.Lock()
		c.state = StateDisconnected

		// Close all subscriptions
		for _, sub := range c.subscriptions {
			close(sub.events)
			close(sub.errors)
			sub.cancel()
		}
		c.subscriptions = make(map[string]*GrpcSubscription)
		c.mu.Unlock()
	})
}

// Subscribe subscribes to events matching a pattern using ConnectRPC server streaming.
//
// Example:
//
//	sub, err := client.Subscribe(ctx, "order.*", &ironflow.SubscribeOptions{
//	    Filter: `data.total > 100`,
//	    Replay: 10,
//	})
func (c *GrpcSubscriptionClient) Subscribe(ctx context.Context, pattern string, opts *SubscribeOptions) (*GrpcSubscription, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected to server")
	}

	// Generate subscription ID
	c.mu.Lock()
	c.subIDCounter++
	subID := fmt.Sprintf("grpc-sub-%d", c.subIDCounter)
	c.mu.Unlock()

	// Build proto request
	protoOpts := &ironflowv1.SubscribeOptions{}
	if opts != nil {
		if opts.Replay > 0 {
			protoOpts.Replay = int32(opts.Replay)
		}
		protoOpts.IncludeMetadata = opts.IncludeMetadata
		protoOpts.Filter = opts.Filter
		protoOpts.ConsumerGroup = opts.ConsumerGroup
		protoOpts.Namespace = opts.Namespace

		switch opts.AckMode {
		case AckModeAuto:
			protoOpts.AckMode = ironflowv1.AckMode_ACK_MODE_AUTO
		case AckModeManual:
			protoOpts.AckMode = ironflowv1.AckMode_ACK_MODE_MANUAL
		}

		switch opts.Backpressure {
		case BackpressureDrop:
			protoOpts.Backpressure = ironflowv1.BackpressureMode_BACKPRESSURE_MODE_DROP
		case BackpressureBlock:
			protoOpts.Backpressure = ironflowv1.BackpressureMode_BACKPRESSURE_MODE_BLOCK
		case BackpressureBuffer:
			protoOpts.Backpressure = ironflowv1.BackpressureMode_BACKPRESSURE_MODE_BUFFER
		}
	}

	req := &ironflowv1.SubscribeRequest{
		Pattern: pattern,
		Options: protoOpts,
	}

	// Create cancellable context
	subCtx, cancel := context.WithCancel(ctx)

	// Create subscription
	sub := &GrpcSubscription{
		ID:      subID,
		Pattern: pattern,
		events:  make(chan *SubscriptionEvent, 100),
		errors:  make(chan *SubscriptionError, 10),
		client:  c,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	c.mu.Lock()
	c.subscriptions[subID] = sub
	c.mu.Unlock()

	// Start streaming in background
	go c.streamSubscription(subCtx, sub, req)

	return sub, nil
}

// streamSubscription handles the ConnectRPC server-streaming connection.
func (c *GrpcSubscriptionClient) streamSubscription(ctx context.Context, sub *GrpcSubscription, req *ironflowv1.SubscribeRequest) {
	defer func() {
		sub.Unsubscribe()
	}()

	// Open server stream via ConnectRPC
	stream, err := c.pubsubClient.Subscribe(ctx, connect.NewRequest(req))
	if err != nil {
		sub.errors <- &SubscriptionError{
			SubscriptionID: sub.ID,
			Code:           "CONNECTION_ERROR",
			Message:        err.Error(),
		}
		return
	}
	defer func() { _ = stream.Close() }()

	for {
		select {
		case <-sub.done:
			return
		case <-c.done:
			return
		default:
		}

		if !stream.Receive() {
			if err := stream.Err(); err != nil {
				select {
				case sub.errors <- &SubscriptionError{
					SubscriptionID: sub.ID,
					Code:           "STREAM_ERROR",
					Message:        err.Error(),
				}:
				case <-sub.done:
				default:
				}
			}
			return
		}

		msg := stream.Msg()

		// Convert proto Struct data to json.RawMessage
		var dataJSON json.RawMessage
		if msg.GetData() != nil {
			b, err := protojson.Marshal(msg.GetData())
			if err != nil {
				continue // Skip malformed messages
			}
			dataJSON = b
		}

		event := &SubscriptionEvent{
			ID:    msg.GetEventId(),
			Topic: msg.GetTopic(),
			Data:  dataJSON,
		}

		if meta := msg.GetMetadata(); meta != nil {
			event.Meta = &EventMeta{
				Sequence: msg.GetSequence(),
			}
			if ts := meta.GetTimestamp(); ts != nil {
				event.Meta.Timestamp = ts.AsTime().Format(time.RFC3339Nano)
			}
		}

		select {
		case sub.events <- event:
		case <-sub.done:
			return
		default:
			// Channel full, drop event
		}
	}
}

// removeSubscription removes a subscription from tracking.
func (c *GrpcSubscriptionClient) removeSubscription(subID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subscriptions, subID)
}

// GrpcAckableSubscription is a subscription that supports manual acknowledgment.
type GrpcAckableSubscription struct {
	*GrpcSubscription

	// serverURL for sending ack requests
	serverURL string
}

// Ack acknowledges an event.
func (s *GrpcAckableSubscription) Ack(eventID string) error {
	return s.sendAck(eventID, "ACK_TYPE_ACK", 0)
}

// Nak negatively acknowledges an event, requesting redelivery.
func (s *GrpcAckableSubscription) Nak(eventID string, delay time.Duration) error {
	return s.sendAck(eventID, "ACK_TYPE_NAK", int(delay.Milliseconds()))
}

// Term terminates an event, preventing further redelivery.
func (s *GrpcAckableSubscription) Term(eventID string) error {
	return s.sendAck(eventID, "ACK_TYPE_TERM", 0)
}

// sendAck sends an acknowledgment to the server via HTTP POST.
func (s *GrpcAckableSubscription) sendAck(eventID, ackType string, redeliverDelay int) error {
	req := map[string]any{
		"eventId": eventID,
		"type":    ackType,
	}
	if redeliverDelay > 0 {
		req["redeliverDelay"] = redeliverDelay
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	url := s.serverURL + "/ironflow.v1.PubSubService/Ack"
	httpReq, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ack failed: %s", string(bodyBytes))
	}

	return nil
}

// SubscribeAckable subscribes with manual acknowledgment support.
//
// Example:
//
//	sub, err := client.SubscribeAckable(ctx, "order.*", &ironflow.SubscribeOptions{
//	    ConsumerGroup: "order-processors",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	for event := range sub.Events() {
//	    if err := processOrder(event); err != nil {
//	        sub.Nak(event.ID, 10*time.Second)
//	        continue
//	    }
//	    sub.Ack(event.ID)
//	}
func (c *GrpcSubscriptionClient) SubscribeAckable(ctx context.Context, pattern string, opts *SubscribeOptions) (*GrpcAckableSubscription, error) {
	if opts == nil {
		opts = &SubscribeOptions{}
	}
	opts.AckMode = AckModeManual

	sub, err := c.Subscribe(ctx, pattern, opts)
	if err != nil {
		return nil, err
	}

	return &GrpcAckableSubscription{
		GrpcSubscription: sub,
		serverURL:        c.config.ServerURL,
	}, nil
}

// CreateGrpcSubscriptionClient creates a gRPC subscription client from the API client.
//
// This is a convenience method that uses the client's server URL.
func (c *Client) CreateGrpcSubscriptionClient() *GrpcSubscriptionClient {
	// Carries the parent client's resolved key: ConnectRPC routes require auth,
	// so a derived client that kept only the URL 401'd on every Subscribe.
	return newGrpcSubscriptionClient(GrpcSubscriptionClientConfig{
		ServerURL:     c.serverURL,
		AutoReconnect: true,
	}, c.apiKey)
}
