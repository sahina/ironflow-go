package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	ironflowv1 "github.com/sahina/ironflow-go/api/ironflow/v1"
	"github.com/sahina/ironflow-go/api/ironflow/v1/ironflowv1connect"
	"github.com/sahina/ironflow-go/ironflow/discovery"
)

// GrpcSubscriptionClientConfig configures a gRPC subscription client.
type GrpcSubscriptionClientConfig struct {
	// ServerURL is the server URL (e.g., "http://localhost:9123").
	ServerURL string

	// AutoReconnect is retained for source compatibility. Cursor-enabled
	// subscriptions reconnect automatically; unpositioned streams still end.
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
// subscriptions and the AckEvent RPC for consumer-group acknowledgments.
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

	// sendMu is held across every send on events. Close closes the client's
	// done first, then takes sendMu to close events, so a send never meets a
	// closed channel.
	sendMu sync.Mutex

	// consumerID is the server-minted subscription_id of a consumer-group
	// stream: the only identity AckEvent accepts for its events (ADR 0094).
	consumerMu sync.Mutex
	consumerID string
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

func (s *GrpcSubscription) setConsumerID(id string) {
	s.consumerMu.Lock()
	s.consumerID = id
	s.consumerMu.Unlock()
}

func (s *GrpcSubscription) currentConsumerID() string {
	s.consumerMu.Lock()
	defer s.consumerMu.Unlock()
	return s.consumerID
}

// NewGrpcSubscriptionClient creates a new gRPC subscription client.
//
// ConnectRPC routes require auth. GrpcSubscriptionClientConfig has no key field
// yet, so this constructor authenticates from IRONFLOW_API_KEY;
// Client.CreateGrpcSubscriptionClient passes the parent client's resolved key.
func NewGrpcSubscriptionClient(config GrpcSubscriptionClientConfig) *GrpcSubscriptionClient {
	discovery.Hydrate()
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

// bearerAuth attaches the Authorization and environment headers to every
// ConnectRPC request. A plain connect.UnaryInterceptorFunc would not do:
// PubSubService.Subscribe is server-streaming, and the streaming path needs
// its own wrapper.
type bearerAuth struct{ key, env string }

func bearerInterceptor(key string) connect.Interceptor { return bearerAuth{key: key} }

// interceptor authenticates and scopes the ConnectRPC clients built from c.
func (c *Client) interceptor() connect.Interceptor {
	return bearerAuth{key: c.apiKey, env: c.environment}
}

func (b bearerAuth) apply(h http.Header) {
	if b.key != "" {
		h.Set("Authorization", "Bearer "+b.key)
	}
	if b.env != "" {
		h.Set(HeaderEnvironmentID, b.env)
	}
}

func (b bearerAuth) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		b.apply(req.Header())
		return next(ctx, req)
	}
}

func (b bearerAuth) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		b.apply(conn.RequestHeader())
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
			sub.sendMu.Lock()
			close(sub.events)
			sub.sendMu.Unlock()
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
		if opts.StartAfterSequence != nil {
			cursor := *opts.StartAfterSequence
			protoOpts.StartAfterSequence = &cursor
		}
		protoOpts.IncludeMetadata = opts.IncludeMetadata || opts.StartAfterSequence != nil
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
	defer sub.Unsubscribe()

	cursorEnabled := req.GetOptions() != nil && req.GetOptions().StartAfterSequence != nil
	var cursor uint64
	if cursorEnabled {
		cursor = req.GetOptions().GetStartAfterSequence()
	}
	accepted := false
	delay := c.config.ReconnectDelay

	for {
		attemptReq := proto.CloneOf(req)
		if accepted && attemptReq.GetOptions() != nil {
			attemptReq.Options.Replay = 0
		}
		if cursorEnabled {
			attemptReq.Options.StartAfterSequence = new(cursor)
		}

		// sdkcoverage: POST /ironflow.v1.PubSubService/Subscribe
		stream, err := c.pubsubClient.Subscribe(ctx, connect.NewRequest(attemptReq))
		if err == nil {
			for stream.Receive() {
				msg := stream.Msg()
				accepted = true
				if id := msg.GetSubscriptionId(); id != "" {
					sub.setConsumerID(id)
				}

				event, convErr := subscriptionEventFromProto(msg)
				if convErr != nil {
					continue // Skip malformed messages.
				}

				// Without a cursor a full channel drops the event. With one it
				// waits: dropping this event and delivering the next would move
				// the cursor past it. The stream is this subscription's own, so
				// waiting holds back nothing else; if the server's buffer for it
				// fills, the server ends the stream with Unavailable to resume.
				delivered, ended := false, false
				sub.sendMu.Lock()
				if cursorEnabled {
					select {
					case sub.events <- event:
						delivered = true
					case <-sub.done:
						ended = true
					case <-c.done:
						ended = true
					case <-ctx.Done():
						ended = true
					}
				} else {
					select {
					case sub.events <- event:
						delivered = true
					case <-sub.done:
						ended = true
					case <-c.done:
						ended = true
					default:
					}
				}
				sub.sendMu.Unlock()
				if ended {
					_ = stream.Close()
					return
				}

				if delivered && cursorEnabled && msg.GetSequence() > 0 {
					cursor = msg.GetSequence()
					delay = c.config.ReconnectDelay
				}
			}
			err = stream.Err()
			_ = stream.Close()
		}

		if err == nil {
			return
		}
		if !cursorEnabled || !isRetryableSubscriptionError(err) {
			c.reportSubscriptionError(sub, err)
			return
		}

		if !waitForSubscriptionReconnect(ctx, sub.done, c.done, delay) {
			return
		}
		delay = nextSubscriptionReconnectDelay(delay, c.config.MaxReconnectDelay, c.config.ReconnectBackoff)
	}
}

// subscriptionEventFromProto converts a streamed proto event to the SDK type.
// The Subscribe and SubscribeBidirectional streams share it.
func subscriptionEventFromProto(msg *ironflowv1.SubscriptionEvent) (*SubscriptionEvent, error) {
	// data carries a JSON object; data_value carries any other payload.
	var dataJSON json.RawMessage
	var err error
	switch {
	case msg.GetData() != nil:
		dataJSON, err = protojson.Marshal(msg.GetData())
	case msg.GetDataValue() != nil:
		dataJSON, err = protojson.Marshal(msg.GetDataValue())
	}
	if err != nil {
		return nil, err
	}

	event := &SubscriptionEvent{
		ID:    msg.GetEventId(),
		Topic: msg.GetTopic(),
		Data:  dataJSON,
	}

	if meta := msg.GetMetadata(); meta != nil {
		event.Meta = &EventMeta{
			Sequence:      msg.GetSequence(),
			SequenceExact: strconv.FormatUint(msg.GetSequence(), 10),
		}
		if ts := meta.GetTimestamp(); ts != nil {
			event.Meta.Timestamp = ts.AsTime().Format(time.RFC3339Nano)
		}
	}
	return event, nil
}

func (c *GrpcSubscriptionClient) reportSubscriptionError(sub *GrpcSubscription, err error) {
	// Close removes subscriptions and closes their channels while holding c.mu.
	// Keep a read lock through the send so it cannot close this channel under us.
	c.mu.RLock()
	if c.subscriptions[sub.ID] != sub {
		c.mu.RUnlock()
		return
	}
	defer c.mu.RUnlock()

	code := connect.CodeOf(err).String()
	select {
	case sub.errors <- &SubscriptionError{
		SubscriptionID: sub.ID,
		Code:           code,
		Message:        err.Error(),
	}:
	case <-sub.done:
	case <-c.done:
	default:
	}
}

func isRetryableSubscriptionError(err error) bool {
	switch connect.CodeOf(err) {
	case connect.CodeUnknown,
		connect.CodeDeadlineExceeded,
		connect.CodeResourceExhausted,
		connect.CodeAborted,
		connect.CodeInternal,
		connect.CodeUnavailable:
		return true
	default:
		return false
	}
}

func waitForSubscriptionReconnect(ctx context.Context, subDone, clientDone <-chan struct{}, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
	case <-subDone:
	case <-clientDone:
	}
	return false
}

func nextSubscriptionReconnectDelay(current, maximum time.Duration, backoff float64) time.Duration {
	next := time.Duration(float64(current) * backoff)
	if next <= current && current < maximum {
		next = current + time.Millisecond
	}
	if next > maximum {
		return maximum
	}
	return next
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

	group     string
	namespace string
}

// Ack acknowledges an event.
func (s *GrpcAckableSubscription) Ack(eventID string) error {
	return s.sendAck(eventID, ironflowv1.AckType_ACK_TYPE_ACK, 0)
}

// Nak negatively acknowledges an event, requesting redelivery.
func (s *GrpcAckableSubscription) Nak(eventID string, delay time.Duration) error {
	return s.sendAck(eventID, ironflowv1.AckType_ACK_TYPE_NAK, delay)
}

// Term terminates an event, preventing further redelivery.
func (s *GrpcAckableSubscription) Term(eventID string) error {
	return s.sendAck(eventID, ironflowv1.AckType_ACK_TYPE_TERM, 0)
}

// ackTimeout bounds one ack: the stream's HTTP client has no timeout. A
// variable so tests can shorten it.
var ackTimeout = 30 * time.Second

// nakDelayMs converts a NAK delay to the int32 milliseconds on the wire. A
// delay out of range is refused, not wrapped.
func nakDelayMs(eventID string, delay time.Duration) (int32, error) {
	ms := delay.Milliseconds()
	if ms < 0 || ms > math.MaxInt32 {
		return 0, &AckError{EventID: eventID, Code: "invalid_argument",
			Message: fmt.Sprintf("redeliver delay %s is outside 0 to %d ms", delay, math.MaxInt32)}
	}
	return int32(ms), nil
}

func (s *GrpcAckableSubscription) sendAck(eventID string, ackType ironflowv1.AckType, delay time.Duration) error {
	delayMs, err := nakDelayMs(eventID, delay)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()
	// sdkcoverage: POST /ironflow.v1.PubSubService/AckEvent
	_, err = s.client.pubsubClient.AckEvent(ctx, connect.NewRequest(&ironflowv1.AckEventRequest{
		Namespace:        s.namespace,
		GroupName:        s.group,
		ConsumerId:       s.currentConsumerID(),
		EventId:          eventID,
		AckType:          ackType,
		RedeliverDelayMs: delayMs,
	}))
	return err
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
		group:            opts.ConsumerGroup,
		namespace:        opts.Namespace,
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
