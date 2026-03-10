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
	wal             WAL

	routerSocket *zmq4.Socket
	pubSocket    *zmq4.Socket

	codec      domain.Codec
	compressor domain.Compressor

	mu                     sync.Mutex
	directSessions         map[string]*consumerSession
	inflight               map[string]*inflightMessage
	completed              map[string]deliveryStatus
	nextSessionID          uint64
	metrics                DeliveryMetrics
	maxDirectRetries       int
	maxInflightPerConsumer int
	maxPerTopicQueue       int
	maxQueuedDirect        int
	maxGlobalIngress       int
	directQueue            map[string][]queuedDirectMessage
	directSender           func(identity []byte, topic string, payload []byte) error
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
	BacklogCount   uint64
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

type queuedDirectMessage struct {
	MessageID string
	Topic     string
	Payload   []byte
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
	DispatchPaused    uint64
	BacklogQueued     uint64
	WALWritten        uint64
	WALReplayed       uint64
	Deferred          uint64
	Throttled         uint64
	Dropped           uint64
}

// ConsumerBacklogMetrics captures per-consumer direct dispatch pressure.
type ConsumerBacklogMetrics struct {
	Inflight    int
	MaxInflight int
	Backlog     uint64
}

type controlMessage struct {
	Type          string   `json:"type"`
	ConsumerID    string   `json:"consumer_id"`
	SessionID     string   `json:"session_id"`
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
	return NewRouterWithDurability(bindAddress, pubAddress, publisher, codec, compressor, maxDirectRetries, deliveryTimeout, nil)
}

// NewRouterWithDurability creates a router with configurable retry/timeout behavior and optional WAL durability.
func NewRouterWithDurability(bindAddress, pubAddress string, publisher domain.EventPublisher, codec domain.Codec, compressor domain.Compressor, maxDirectRetries int, deliveryTimeout time.Duration, durability WAL) *Router {
	if maxDirectRetries <= 0 {
		maxDirectRetries = 3
	}
	if deliveryTimeout <= 0 {
		deliveryTimeout = 30 * time.Second
	}

	return &Router{
		bindAddress:            bindAddress,
		pubAddress:             pubAddress,
		publisher:              publisher,
		codec:                  codec,
		compressor:             compressor,
		directSessions:         map[string]*consumerSession{},
		inflight:               map[string]*inflightMessage{},
		completed:              map[string]deliveryStatus{},
		maxDirectRetries:       maxDirectRetries,
		maxInflightPerConsumer: 1024,
		maxPerTopicQueue:       256,
		maxQueuedDirect:        4096,
		maxGlobalIngress:       8192,
		directQueue:            map[string][]queuedDirectMessage{},
		deliveryTimeout:        deliveryTimeout,
		now:                    func() time.Time { return time.Now().UTC() },
		wal:                    durability,
	}
}

// SetMaxInflightPerConsumer configures the hard upper bound for a direct consumer inflight window.
func (r *Router) SetMaxInflightPerConsumer(limit int) {
	if limit <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maxInflightPerConsumer = limit
}

// SetQueueBounds configures per-topic and global queued direct-message limits.
func (r *Router) SetQueueBounds(maxPerTopicQueue, maxQueuedDirect int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if maxPerTopicQueue > 0 {
		r.maxPerTopicQueue = maxPerTopicQueue
	}
	if maxQueuedDirect > 0 {
		r.maxQueuedDirect = maxQueuedDirect
	}
}

// SetGlobalIngressLimit configures a hard cap for inflight+queued direct messages.
func (r *Router) SetGlobalIngressLimit(limit int) {
	if limit <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maxGlobalIngress = limit
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

func (r *Router) replayFromWAL(consumerID string) {
	if r.wal == nil {
		return
	}

	entries, err := r.wal.ReplayUnacked()
	if err != nil {
		fmt.Printf("failed to replay wal: %v\n", err)
		return
	}

	r.mu.Lock()
	deferred := make([]retryDispatch, 0, len(entries))
	for _, entry := range entries {
		if consumerID != "" && entry.Consumer != consumerID {
			continue
		}
		session, ok := r.directSessions[entry.Consumer]
		if !ok {
			continue
		}
		if _, exists := r.inflight[entry.MessageID]; exists {
			continue
		}
		if _, done := r.completed[entry.MessageID]; done {
			continue
		}
		attempt := entry.Attempt
		if attempt <= 0 {
			attempt = 1
		}
		r.inflight[entry.MessageID] = &inflightMessage{
			MessageID:       entry.MessageID,
			ConsumerID:      entry.Consumer,
			SessionID:       entry.SessionID,
			Topic:           entry.Topic,
			Payload:         append([]byte(nil), entry.Payload...),
			DeliveryAttempt: attempt,
			Status:          statusDispatched,
			DispatchedAt:    r.now(),
		}
		session.InflightCount++
		r.metrics.WALReplayed++
		deferred = append(deferred, retryDispatch{identity: append([]byte(nil), session.SocketIdentity...), topic: entry.Topic, payload: append([]byte(nil), entry.Payload...)})
	}
	r.mu.Unlock()

	for _, retry := range deferred {
		if r.directSender == nil {
			continue
		}
		if err := r.directSender(retry.identity, retry.topic, retry.payload); err != nil {
			fmt.Printf("failed to replay wal event: %v\n", err)
		}
	}
}

func (r *Router) processInflightTimeouts() {
	r.mu.Lock()
	deferredRetries := make([]retryDispatch, 0)
	deadLetteredIDs := make([]string, 0)
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
				if r.wal != nil {
					deadLetteredIDs = append(deadLetteredIDs, messageID)
				}
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
		if r.wal != nil {
			deadLetteredIDs = append(deadLetteredIDs, messageID)
		}
	}
	r.mu.Unlock()

	for _, messageID := range deadLetteredIDs {
		if err := r.wal.AppendDeadLettered(messageID); err != nil {
			fmt.Printf("failed to append wal dead-letter: %v\n", err)
		}
	}

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
		r.handleAck(msg.MessageID, msg.ConsumerID, msg.SessionID)
	case "nack":
		r.handleNack(msg.MessageID, msg.ConsumerID, msg.SessionID, msg.Status)
	}
}

func (r *Router) registerConsumerSession(clientID []byte, msg controlMessage) {
	if msg.Mode != "direct" || msg.ConsumerID == "" {
		return
	}
	r.mu.Lock()
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
	if session.MaxInflight <= 0 || session.MaxInflight > r.maxInflightPerConsumer {
		session.MaxInflight = r.maxInflightPerConsumer
	}
	for _, topic := range msg.Subscriptions {
		session.Subscriptions[topic] = struct{}{}
	}
	r.directSessions[msg.ConsumerID] = session
	r.mu.Unlock()

	r.replayFromWAL(msg.ConsumerID)
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
	if r.maxGlobalIngress > 0 && r.totalDirectLoadLocked() >= r.maxGlobalIngress {
		r.metrics.Dropped++
		fmt.Printf("{\"event\":\"ingress_drop\",\"topic\":%q,\"message_id\":%q,\"reason\":\"global_ingress_limit\",\"limit\":%d}\n", topic, messageID, r.maxGlobalIngress)
		r.mu.Unlock()
		return
	}
	session := r.selectSession(topic)
	if session == nil {
		if messageID != "" {
			r.enqueueDirectLocked(topic, messageID, payload)
		}
		r.mu.Unlock()
		return
	}
	identity, walEntry, needsWAL := r.prepareDispatchLocked(session, topic, messageID, payload, 1)
	r.mu.Unlock()

	if needsWAL {
		if err := r.wal.AppendDispatched(walEntry); err != nil {
			fmt.Printf("failed to append wal dispatch: %v\n", err)
		} else {
			r.mu.Lock()
			r.metrics.WALWritten++
			r.mu.Unlock()
		}
	}

	if r.directSender != nil {
		if err := r.directSender(identity, topic, payload); err != nil {
			fmt.Printf("failed to direct-dispatch event: %v\n", err)
		}
	}
}

func (r *Router) prepareDispatchLocked(session *consumerSession, topic, messageID string, payload []byte, attempt int) ([]byte, walDispatchedEntry, bool) {
	if session.SupportsAck && messageID != "" {
		r.inflight[messageID] = &inflightMessage{
			MessageID:       messageID,
			ConsumerID:      session.ConsumerID,
			SessionID:       session.SessionID,
			Topic:           topic,
			Payload:         append([]byte(nil), payload...),
			DeliveryAttempt: attempt,
			Status:          statusDispatched,
			DispatchedAt:    r.now(),
		}
		session.InflightCount++
	}
	r.metrics.Dispatched++
	identity := append([]byte(nil), session.SocketIdentity...)
	walEntry := walDispatchedEntry{MessageID: messageID, Consumer: session.ConsumerID, SessionID: session.SessionID, Topic: topic, Payload: append([]byte(nil), payload...), Attempt: attempt}
	needsWAL := r.wal != nil && session.SupportsAck && messageID != ""
	return identity, walEntry, needsWAL
}

func (r *Router) enqueueDirectLocked(topic, messageID string, payload []byte) {
	queue := r.directQueue[topic]
	if r.maxPerTopicQueue > 0 && len(queue) >= r.maxPerTopicQueue {
		r.metrics.Dropped++
		fmt.Printf("{\"event\":\"direct_drop\",\"topic\":%q,\"message_id\":%q,\"reason\":\"topic_queue_full\",\"limit\":%d}\n", topic, messageID, r.maxPerTopicQueue)
		return
	}
	if r.maxQueuedDirect > 0 && r.totalQueuedLocked() >= r.maxQueuedDirect {
		r.metrics.Dropped++
		fmt.Printf("{\"event\":\"direct_drop\",\"topic\":%q,\"message_id\":%q,\"reason\":\"global_direct_queue_full\",\"limit\":%d}\n", topic, messageID, r.maxQueuedDirect)
		return
	}
	r.metrics.Deferred++
	r.metrics.Throttled++
	r.metrics.BacklogQueued++
	r.directQueue[topic] = append(queue, queuedDirectMessage{MessageID: messageID, Topic: topic, Payload: append([]byte(nil), payload...)})
	for _, s := range r.directSessions {
		if _, ok := s.Subscriptions[topic]; ok {
			s.BacklogCount++
		}
	}
	fmt.Printf("{\"event\":\"direct_deferred\",\"topic\":%q,\"message_id\":%q}\n", topic, messageID)
}

func (r *Router) totalQueuedLocked() int {
	total := 0
	for _, queue := range r.directQueue {
		total += len(queue)
	}
	return total
}

func (r *Router) totalDirectLoadLocked() int {
	return len(r.inflight) + r.totalQueuedLocked()
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
			r.metrics.DispatchPaused++
			continue
		}
		return s
	}
	return nil
}

// ConsumerBacklogSnapshot returns per-consumer inflight and backlog counters.
func (r *Router) ConsumerBacklogSnapshot() map[string]ConsumerBacklogMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]ConsumerBacklogMetrics, len(r.directSessions))
	for consumerID, s := range r.directSessions {
		out[consumerID] = ConsumerBacklogMetrics{
			Inflight:    s.InflightCount,
			MaxInflight: s.MaxInflight,
			Backlog:     s.BacklogCount,
		}
	}
	return out
}

func (r *Router) handleAck(messageID, consumerID, sessionID string) {
	r.mu.Lock()
	if prior, ok := r.completed[messageID]; ok && prior == statusAcked {
		r.mu.Unlock()
		return
	}
	record, ok := r.inflight[messageID]
	if !ok || record.ConsumerID != consumerID {
		r.mu.Unlock()
		return
	}
	if sessionID != "" && record.SessionID != sessionID {
		r.mu.Unlock()
		return
	}
	var drain []retryDispatch
	if s, ok := r.directSessions[record.ConsumerID]; ok && s.InflightCount > 0 {
		s.InflightCount--
		drain = r.drainDeferredLocked(record.Topic)
	}
	delete(r.inflight, messageID)
	r.completed[messageID] = statusAcked
	r.metrics.Acked++
	shouldCommit := r.wal != nil
	r.mu.Unlock()

	if shouldCommit {
		if err := r.wal.AppendCommitted(messageID); err != nil {
			fmt.Printf("failed to append wal commit: %v\n", err)
		}
	}
	r.sendDeferred(drain)
}

func (r *Router) handleNack(messageID, consumerID, sessionID, status string) {
	r.mu.Lock()
	record, ok := r.inflight[messageID]
	if !ok || record.ConsumerID != consumerID {
		r.mu.Unlock()
		return
	}
	if sessionID != "" && record.SessionID != sessionID {
		r.mu.Unlock()
		return
	}
	r.metrics.Nacked++
	isRetryable := status == "retryable_error" || status == "retryable"
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
	drain := r.drainDeferredLocked(record.Topic)
	delete(r.inflight, messageID)
	r.completed[messageID] = statusDeadLettered
	r.metrics.DeadLettered++
	shouldDeadLetter := r.wal != nil
	r.mu.Unlock()

	if shouldDeadLetter {
		if err := r.wal.AppendDeadLettered(messageID); err != nil {
			fmt.Printf("failed to append wal dead-letter: %v\n", err)
		}
	}
	r.sendDeferred(drain)
}

func (r *Router) drainDeferredLocked(topic string) []retryDispatch {
	queue := r.directQueue[topic]
	if len(queue) == 0 {
		return nil
	}
	session := r.selectSession(topic)
	if session == nil {
		return nil
	}
	msg := queue[0]
	if len(queue) == 1 {
		delete(r.directQueue, topic)
	} else {
		r.directQueue[topic] = queue[1:]
	}
	if session.BacklogCount > 0 {
		session.BacklogCount--
	}
	identity, walEntry, needsWAL := r.prepareDispatchLocked(session, msg.Topic, msg.MessageID, msg.Payload, 1)
	if needsWAL {
		if err := r.wal.AppendDispatched(walEntry); err != nil {
			fmt.Printf("failed to append wal dispatch: %v\n", err)
		} else {
			r.metrics.WALWritten++
		}
	}
	return []retryDispatch{{identity: identity, topic: msg.Topic, payload: msg.Payload}}
}

func (r *Router) sendDeferred(retries []retryDispatch) {
	for _, retry := range retries {
		if r.directSender == nil {
			continue
		}
		if err := r.directSender(retry.identity, retry.topic, retry.payload); err != nil {
			fmt.Printf("failed to direct-dispatch deferred event: %v\n", err)
		}
	}
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
