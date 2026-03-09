package zmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/pebbe/zmq4"
)

// Router manages the ZMQ ROUTER socket for incoming events.
type Router struct {
	bindAddress     string
	pubAddress      string
	publisher       domain.EventPublisher
	deliveryTimeout time.Duration
	now             func() time.Time

	routerSocket *zmq4.Socket
	pubSocket    *zmq4.Socket

	codec      domain.Codec
	compressor domain.Compressor

	mu               sync.Mutex
	directSessions   map[string]*consumerSession
	inflight         map[string]*inflightMessage
	completed        map[string]deliveryStatus
	nextSessionID    uint64
	metrics          DeliveryMetrics
	maxDirectRetries int
	directSender     func(identity []byte, topic string, payload []byte) error
}

type deliveryStatus string

const (
	statusDispatched   deliveryStatus = "dispatched"
	statusAcked        deliveryStatus = "acked"
	statusNacked       deliveryStatus = "nacked"
	statusRetry        deliveryStatus = "retry_scheduled"
	statusDeadLettered deliveryStatus = "dead_lettered"

	controlTopic = "_control"
)

type consumerSession struct {
	SessionID      string
	ConsumerID     string
	SocketIdentity []byte
	Subscriptions  map[string]struct{}
	SupportsAck    bool
	MaxInflight    int
	InflightCount  int
	ConnectedAt    time.Time
	LastHeartbeat  time.Time
}

type inflightMessage struct {
	MessageID       string
	ConsumerID      string
	SessionID       string
	Topic           string
	Payload         []byte
	DeliveryAttempt int
	Status          deliveryStatus
	DispatchedAt    time.Time
}

// DeliveryMetrics captures direct-delivery lifecycle counters.
type DeliveryMetrics struct {
	Dispatched        uint64
	Acked             uint64
	Nacked            uint64
	Retried           uint64
	DeadLettered      uint64
	DeliveryTimeout   uint64
	RetryDueToTimeout uint64
}

type controlMessage struct {
	Type          string   `json:"type"`
	ConsumerID    string   `json:"consumer_id"`
	MessageID     string   `json:"message_id"`
	Status        string   `json:"status"`
	Mode          string   `json:"mode"`
	Subscriptions []string `json:"subscriptions"`
	Capabilities  struct {
		SupportsAck bool `json:"supports_ack"`
		MaxInflight int  `json:"max_inflight"`
	} `json:"capabilities"`
}

// NewRouter creates a new ZMQ Router.
func NewRouter(bindAddress, pubAddress string, publisher domain.EventPublisher, codec domain.Codec, compressor domain.Compressor) *Router {
	return NewRouterWithOptions(bindAddress, pubAddress, publisher, codec, compressor, 3, 30*time.Second)
}

// NewRouterWithOptions creates a router with configurable retry and timeout behavior.
func NewRouterWithOptions(bindAddress, pubAddress string, publisher domain.EventPublisher, codec domain.Codec, compressor domain.Compressor, maxDirectRetries int, deliveryTimeout time.Duration) *Router {
	if maxDirectRetries <= 0 {
		maxDirectRetries = 3
	}
	if deliveryTimeout <= 0 {
		deliveryTimeout = 30 * time.Second
	}

	return &Router{
		bindAddress:      bindAddress,
		pubAddress:       pubAddress,
		publisher:        publisher,
		codec:            codec,
		compressor:       compressor,
		directSessions:   map[string]*consumerSession{},
		inflight:         map[string]*inflightMessage{},
		completed:        map[string]deliveryStatus{},
		maxDirectRetries: maxDirectRetries,
		deliveryTimeout:  deliveryTimeout,
		now:              func() time.Time { return time.Now().UTC() },
	}
}

// Start initializes and runs the ZMQ ROUTER socket loop.
func (r *Router) Start(ctx context.Context) error {
	routerSocket, err := zmq4.NewSocket(zmq4.ROUTER)
	if err != nil {
		return fmt.Errorf("failed to create router socket: %w", err)
	}
	r.routerSocket = routerSocket

	pubSocket, err := zmq4.NewSocket(zmq4.PUB)
	if err != nil {
		return fmt.Errorf("failed to create pub socket: %w", err)
	}
	r.pubSocket = pubSocket

	if err := r.routerSocket.Bind(r.bindAddress); err != nil {
		return fmt.Errorf("failed to bind router socket: %w", err)
	}
	if err := r.pubSocket.Bind(r.pubAddress); err != nil {
		return fmt.Errorf("failed to bind pub socket: %w", err)
	}

	fmt.Println("ZMQ Router started")

	r.directSender = func(identity []byte, topic string, payload []byte) error {
		_, err := r.routerSocket.SendMessage(identity, "", topic, payload)
		return err
	}

	go r.loop(ctx)

	return nil
}

// Stop gracefully closes the ZMQ sockets.
func (r *Router) Stop() {
	if r.routerSocket != nil {
		_ = r.routerSocket.Close()
	}
	if r.pubSocket != nil {
		_ = r.pubSocket.Close()
	}
	fmt.Println("ZMQ Router stopped")
}

func (r *Router) loop(ctx context.Context) {
	defer r.Stop()

	poller := zmq4.NewPoller()
	poller.Add(r.routerSocket, zmq4.POLLIN)

	for {
		sockets, err := poller.Poll(250 * time.Millisecond)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}

		r.processInflightTimeouts()

		if len(sockets) > 0 {
			msg, err := r.routerSocket.RecvMessageBytes(0)
			if err != nil {
				continue
			}

			clientID, topic, rawEvent, err := parseFrames(msg)
			if err != nil {
				continue
			}

			if err := validateTopic(topic); err != nil {
				fmt.Printf("invalid topic %q: %v\n", topic, err)
				continue
			}

			decompressedEvent, err := r.compressor.Decompress(rawEvent)
			if err != nil {
				fmt.Printf("failed to decompress event: %v\n", err)
				continue
			}

			if topic == controlTopic {
				r.handleControl(clientID, decompressedEvent)
				continue
			}

			var event domain.Event
			if err := r.codec.Decode(decompressedEvent, &event); err != nil {
				fmt.Printf("failed to decode event: %v\n", err)
				continue
			}

			event.Topic = topic

			envelope := domain.Envelope{
				ClientID: clientID,
				Event:    event,
			}

			if err := r.publisher.Publish(ctx, envelope); err != nil {
				fmt.Printf("failed to publish event: %v\n", err)
				continue
			}

			if _, err := r.pubSocket.SendMessage(event.Topic, decompressedEvent); err != nil {
				fmt.Printf("failed to fan out event on PUB socket: %v\n", err)
			}

			r.dispatchDirect(event.Topic, event.ID, decompressedEvent)
		}

		if ctx.Err() != nil {
			break
		}
	}
}

func (r *Router) processInflightTimeouts() {
	r.mu.Lock()
	deferredRetries := make([]retryDispatch, 0)
	now := r.now()
	for messageID, record := range r.inflight {
		if now.Sub(record.DispatchedAt) < r.deliveryTimeout {
			continue
		}

		r.metrics.DeliveryTimeout++
		if record.DeliveryAttempt < r.maxDirectRetries {
			session, ok := r.directSessions[record.ConsumerID]
			if !ok {
				delete(r.inflight, messageID)
				r.completed[messageID] = statusDeadLettered
				r.metrics.DeadLettered++
				continue
			}

			record.DeliveryAttempt++
			record.Status = statusRetry
			record.DispatchedAt = now
			r.metrics.Retried++
			r.metrics.RetryDueToTimeout++
			r.metrics.Dispatched++
			deferredRetries = append(deferredRetries, retryDispatch{
				identity: append([]byte(nil), session.SocketIdentity...),
				topic:    record.Topic,
				payload:  append([]byte(nil), record.Payload...),
			})
			continue
		}

		if s, ok := r.directSessions[record.ConsumerID]; ok && s.InflightCount > 0 {
			s.InflightCount--
		}
		delete(r.inflight, messageID)
		r.completed[messageID] = statusDeadLettered
		r.metrics.DeadLettered++
	}
	r.mu.Unlock()

	for _, retry := range deferredRetries {
		if r.directSender == nil {
			continue
		}
		if err := r.directSender(retry.identity, retry.topic, retry.payload); err != nil {
			fmt.Printf("failed to retry timed out direct-dispatch event: %v\n", err)
		}
	}
}

type retryDispatch struct {
	identity []byte
	topic    string
	payload  []byte
}

func (r *Router) handleControl(clientID []byte, raw []byte) {
	var msg controlMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		fmt.Printf("failed to decode control message: %v\n", err)
		return
	}

	switch msg.Type {
	case "consumer.register":
		r.registerConsumerSession(clientID, msg)
	case "consumer.heartbeat":
		r.heartbeatConsumer(msg.ConsumerID)
	case "ack":
		r.handleAck(msg.MessageID, msg.ConsumerID)
	case "nack":
		r.handleNack(msg.MessageID, msg.ConsumerID, msg.Status)
	}
}

func (r *Router) registerConsumerSession(clientID []byte, msg controlMessage) {
	if msg.Mode != "direct" || msg.ConsumerID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextSessionID++
	session := &consumerSession{
		SessionID:      fmt.Sprintf("sess_%06d", r.nextSessionID),
		ConsumerID:     msg.ConsumerID,
		SocketIdentity: append([]byte(nil), clientID...),
		SupportsAck:    msg.Capabilities.SupportsAck,
		Subscriptions:  map[string]struct{}{},
		ConnectedAt:    r.now(),
		LastHeartbeat:  r.now(),
		MaxInflight:    msg.Capabilities.MaxInflight,
	}
	if session.MaxInflight <= 0 {
		session.MaxInflight = 1024
	}
	for _, topic := range msg.Subscriptions {
		session.Subscriptions[topic] = struct{}{}
	}
	r.directSessions[msg.ConsumerID] = session
}

func (r *Router) heartbeatConsumer(consumerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if session, ok := r.directSessions[consumerID]; ok {
		session.LastHeartbeat = r.now()
	}
}

func (r *Router) dispatchDirect(topic, messageID string, payload []byte) {
	r.mu.Lock()
	session := r.selectSession(topic)
	if session == nil {
		r.mu.Unlock()
		return
	}
	if session.SupportsAck && messageID != "" {
		r.inflight[messageID] = &inflightMessage{
			MessageID:       messageID,
			ConsumerID:      session.ConsumerID,
			SessionID:       session.SessionID,
			Topic:           topic,
			Payload:         append([]byte(nil), payload...),
			DeliveryAttempt: 1,
			Status:          statusDispatched,
			DispatchedAt:    r.now(),
		}
		session.InflightCount++
	}
	r.metrics.Dispatched++
	identity := append([]byte(nil), session.SocketIdentity...)
	r.mu.Unlock()

	if r.directSender != nil {
		if err := r.directSender(identity, topic, payload); err != nil {
			fmt.Printf("failed to direct-dispatch event: %v\n", err)
		}
	}
}

func (r *Router) selectSession(topic string) *consumerSession {
	if len(r.directSessions) == 0 {
		return nil
	}
	ids := make([]string, 0, len(r.directSessions))
	for id := range r.directSessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		s := r.directSessions[id]
		if _, ok := s.Subscriptions[topic]; !ok {
			continue
		}
		if s.InflightCount >= s.MaxInflight {
			continue
		}
		return s
	}
	return nil
}

func (r *Router) handleAck(messageID, consumerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if prior, ok := r.completed[messageID]; ok && prior == statusAcked {
		return
	}
	record, ok := r.inflight[messageID]
	if !ok || record.ConsumerID != consumerID {
		return
	}
	if s, ok := r.directSessions[record.ConsumerID]; ok && s.InflightCount > 0 {
		s.InflightCount--
	}
	delete(r.inflight, messageID)
	r.completed[messageID] = statusAcked
	r.metrics.Acked++
}

func (r *Router) handleNack(messageID, consumerID, status string) {
	r.mu.Lock()
	record, ok := r.inflight[messageID]
	if !ok || record.ConsumerID != consumerID {
		r.mu.Unlock()
		return
	}
	r.metrics.Nacked++
	isRetryable := status == "retryable_error"
	if isRetryable && record.DeliveryAttempt < r.maxDirectRetries {
		session, ok := r.directSessions[record.ConsumerID]
		if !ok {
			r.mu.Unlock()
			return
		}
		record.DeliveryAttempt++
		record.Status = statusRetry
		record.DispatchedAt = r.now()
		r.metrics.Retried++
		r.metrics.Dispatched++
		identity := append([]byte(nil), session.SocketIdentity...)
		topic := record.Topic
		payload := append([]byte(nil), record.Payload...)
		r.mu.Unlock()
		if r.directSender != nil {
			if err := r.directSender(identity, topic, payload); err != nil {
				fmt.Printf("failed to retry direct-dispatch event: %v\n", err)
			}
		}
		return
	}

	if s, ok := r.directSessions[record.ConsumerID]; ok && s.InflightCount > 0 {
		s.InflightCount--
	}
	delete(r.inflight, messageID)
	if isRetryable {
		r.completed[messageID] = statusNacked
	} else {
		r.completed[messageID] = statusDeadLettered
		r.metrics.DeadLettered++
	}
	r.mu.Unlock()
}

// MetricsSnapshot returns a thread-safe copy of delivery counters.
func (r *Router) MetricsSnapshot() DeliveryMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.metrics
}

func parseFrames(msg [][]byte) ([]byte, string, []byte, error) {
	switch {
	case len(msg) == 3:
		// [ClientID, Topic, Payload]
		return msg[0], string(msg[1]), msg[2], nil
	case len(msg) == 4 && len(msg[1]) == 0:
		// [ClientID, Delimiter, Topic, Payload]
		return msg[0], string(msg[2]), msg[3], nil
	default:
		return nil, "", nil, fmt.Errorf("malformed message: expected 3 frames or 4 frames with delimiter, got %d", len(msg))
	}
}

func validateTopic(topic string) error {
	if topic == "" {
		return fmt.Errorf("topic must not be empty")
	}

	if strings.HasPrefix(topic, ".") || strings.HasSuffix(topic, ".") || strings.Contains(topic, "..") {
		return fmt.Errorf("topic segments must be non-empty")
	}

	for _, r := range topic {
		if unicode.IsSpace(r) {
			return fmt.Errorf("topic must not contain whitespace")
		}
	}

	return nil
}
