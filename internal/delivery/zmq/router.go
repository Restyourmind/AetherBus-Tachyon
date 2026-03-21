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

const maxScheduledDeliveryHorizon = 365 * 24 * time.Hour

const (
	defaultPriorityBoostThreshold = 8
	defaultPriorityBoostOffset    = 1000
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
	nextScheduleSequence   uint64
	nextQueueSequence      uint64
	metrics                DeliveryMetrics
	maxDirectRetries       int
	maxInflightPerConsumer int
	maxPerTopicQueue       int
	maxQueuedDirect        int
	maxGlobalIngress       int
	sessionSnapshotTTL     time.Duration
	directQueue            map[string]*priorityQueue
	scheduledQueue         []scheduledMessage
	directSender           func(identity []byte, topic string, payload []byte) error
	priorityClasses        []string
	priorityRank           map[string]int
	priorityWeights        map[string]int
	priorityBoostThreshold int
	priorityBoostOffset    int
	priorityPreemption     bool
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

type capabilityHints struct {
	SupportsAck         bool
	SupportsCompression []string
	SupportsCodec       []string
	Resumable           bool
}

type consumerSession struct {
	SessionID         string
	ConsumerID        string
	TransportIdentity []byte
	Subscriptions     map[string]struct{}
	Capabilities      capabilityHints
	MaxInflight       int
	InflightCount     int
	BacklogCount      uint64
	ConnectedAt       time.Time
	LastHeartbeat     time.Time
	Live              bool
	ResumablePending  bool
}

type sessionSnapshot struct {
	SessionID           string    `json:"session_id"`
	ConsumerID          string    `json:"consumer_id"`
	Subscriptions       []string  `json:"subscriptions"`
	ConnectedAt         time.Time `json:"connected_at"`
	LastHeartbeat       time.Time `json:"last_heartbeat"`
	MaxInflight         int       `json:"max_inflight"`
	SupportsAck         bool      `json:"supports_ack"`
	SupportsCompression []string  `json:"supports_compression,omitempty"`
	SupportsCodec       []string  `json:"supports_codec,omitempty"`
	Resumable           bool      `json:"resumable"`
	Live                bool      `json:"live"`
	ResumablePending    bool      `json:"resumable_pending"`
}

type inflightMessage struct {
	MessageID       string
	ConsumerID      string
	SessionID       string
	Topic           string
	Payload         []byte
	Priority        string
	DeliveryAttempt int
	Status          deliveryStatus
	DispatchedAt    time.Time
	EnqueueSequence uint64
}

type queuedDirectMessage struct {
	MessageID       string `json:"message_id"`
	Topic           string `json:"topic"`
	Payload         []byte `json:"payload"`
	Priority        string `json:"priority,omitempty"`
	EnqueueSequence uint64 `json:"enqueue_sequence"`
}

type priorityQueue struct {
	Messages []queuedDirectMessage `json:"messages"`
}

type scheduledMessage struct {
	Sequence        uint64    `json:"sequence"`
	MessageID       string    `json:"message_id"`
	Topic           string    `json:"topic"`
	Payload         []byte    `json:"payload"`
	Priority        string    `json:"priority,omitempty"`
	EnqueueSequence uint64    `json:"enqueue_sequence,omitempty"`
	DeliveryAttempt int       `json:"delivery_attempt,omitempty"`
	DeliverAt       time.Time `json:"deliver_at"`
	Reason          string    `json:"reason,omitempty"`
}

// DeliveryMetrics captures direct-delivery lifecycle counters.
type DeliveryMetrics struct {
	Ingress           uint64
	Routed            uint64
	Unroutable        uint64
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
	Scheduled         uint64
	ScheduledPromoted uint64
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
		SupportsAck         bool     `json:"supports_ack"`
		SupportsCompression []string `json:"supports_compression"`
		SupportsCodec       []string `json:"supports_codec"`
		Resumable           bool     `json:"resumable"`
		MaxInflight         int      `json:"max_inflight"`
	} `json:"capabilities"`
}

func NewRouter(bindAddress, pubAddress string, publisher domain.EventPublisher, codec domain.Codec, compressor domain.Compressor) *Router {
	return NewRouterWithOptions(bindAddress, pubAddress, publisher, codec, compressor, 3, 30*time.Second)
}
func NewRouterWithOptions(bindAddress, pubAddress string, publisher domain.EventPublisher, codec domain.Codec, compressor domain.Compressor, maxDirectRetries int, deliveryTimeout time.Duration) *Router {
	return NewRouterWithDurability(bindAddress, pubAddress, publisher, codec, compressor, maxDirectRetries, deliveryTimeout, nil)
}
func NewRouterWithDurability(bindAddress, pubAddress string, publisher domain.EventPublisher, codec domain.Codec, compressor domain.Compressor, maxDirectRetries int, deliveryTimeout time.Duration, durability WAL) *Router {
	if maxDirectRetries <= 0 {
		maxDirectRetries = 3
	}
	if deliveryTimeout <= 0 {
		deliveryTimeout = 30 * time.Second
	}
	router := &Router{
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
		sessionSnapshotTTL:     5 * time.Minute,
		directQueue:            map[string]*priorityQueue{},
		deliveryTimeout:        deliveryTimeout,
		now:                    func() time.Time { return time.Now().UTC() },
		wal:                    durability,
		priorityBoostThreshold: defaultPriorityBoostThreshold,
		priorityBoostOffset:    defaultPriorityBoostOffset,
		priorityPreemption:     true,
	}
	router.SetPriorityPolicy([]string{"urgent", "high", "normal", "low"}, map[string]int{"urgent": 4, "high": 3, "normal": 2, "low": 1}, true, defaultPriorityBoostThreshold, defaultPriorityBoostOffset)
	return router
}

func (r *Router) Start(ctx context.Context) error {
	if err := r.loadSessionSnapshots(); err != nil {
		return fmt.Errorf("failed to load session snapshots: %w", err)
	}
	if err := r.loadScheduledQueue(); err != nil {
		return fmt.Errorf("failed to load delayed queue: %w", err)
	}
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

func (r *Router) Stop() {
	if r.routerSocket != nil {
		_ = r.routerSocket.Close()
	}
	if r.pubSocket != nil {
		_ = r.pubSocket.Close()
	}
	fmt.Println("ZMQ Router stopped")
}
func (r *Router) SetMaxInflightPerConsumer(limit int) {
	if limit <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maxInflightPerConsumer = limit
}
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
func (r *Router) SetGlobalIngressLimit(limit int) {
	if limit <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maxGlobalIngress = limit
}

func (r *Router) SetPriorityPolicy(classes []string, weights map[string]int, preemption bool, boostThreshold, boostOffset int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(classes) == 0 {
		classes = []string{"urgent", "high", "normal", "low"}
	}
	normalizedClasses := make([]string, 0, len(classes))
	seen := map[string]struct{}{}
	for _, class := range classes {
		class = strings.ToLower(strings.TrimSpace(class))
		if class == "" {
			continue
		}
		if _, ok := seen[class]; ok {
			continue
		}
		seen[class] = struct{}{}
		normalizedClasses = append(normalizedClasses, class)
	}
	if len(normalizedClasses) == 0 {
		normalizedClasses = []string{"urgent", "high", "normal", "low"}
	}
	r.priorityClasses = normalizedClasses
	r.priorityRank = make(map[string]int, len(normalizedClasses))
	r.priorityWeights = make(map[string]int, len(normalizedClasses))
	for i, class := range normalizedClasses {
		r.priorityRank[class] = i
		weight := len(normalizedClasses) - i
		if weights != nil {
			if configured, ok := weights[class]; ok && configured > 0 {
				weight = configured
			}
		}
		r.priorityWeights[class] = weight
	}
	r.priorityPreemption = preemption
	if boostThreshold > 0 {
		r.priorityBoostThreshold = boostThreshold
	}
	if boostOffset > 0 {
		r.priorityBoostOffset = boostOffset
	}
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
		r.promoteScheduledDue()
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
			event.Priority = r.normalizePriority(event.Priority)
			normalizedEvent, err := r.codec.Encode(event)
			if err != nil {
				fmt.Printf("failed to re-encode event: %v\n", err)
				continue
			}
			envelope := domain.Envelope{ClientID: clientID, Event: event, DeliverAt: event.DeliverAt, Priority: event.Priority}
			if err := validateScheduleTimestamp(r.now(), envelope.DeliverAt); err != nil {
				fmt.Printf("invalid delivery timestamp for %q: %v\n", event.ID, err)
				r.mu.Lock()
				r.metrics.Dropped++
				r.mu.Unlock()
				continue
			}
			r.mu.Lock()
			r.metrics.Ingress++
			r.mu.Unlock()
			publishResult := domain.PublishResult{Status: domain.RouteStatusRouted, Topic: event.Topic}
			if publisherWithResult, ok := r.publisher.(domain.EventPublisherWithResult); ok {
				resolved, err := publisherWithResult.PublishWithResult(ctx, envelope)
				if err != nil {
					fmt.Printf("failed to publish event: %v\n", err)
					continue
				}
				publishResult = resolved
			} else if r.publisher != nil {
				if err := r.publisher.Publish(ctx, envelope); err != nil {
					fmt.Printf("failed to publish event: %v\n", err)
					continue
				}
			}
			if publishResult.Status == domain.RouteStatusUnroutable {
				r.mu.Lock()
				r.metrics.Unroutable++
				r.mu.Unlock()
				continue
			}
			r.mu.Lock()
			r.metrics.Routed++
			r.mu.Unlock()
			if _, err := r.pubSocket.SendMessage(event.Topic, normalizedEvent); err != nil {
				fmt.Printf("failed to fan out event on PUB socket: %v\n", err)
			}
			if !event.DeliverAt.IsZero() && event.DeliverAt.After(r.now()) {
				r.scheduleDispatch(topic, event.ID, normalizedEvent, event.Priority, 0, 1, event.DeliverAt, "publish")
			} else {
				r.dispatchDirect(topic, event.ID, normalizedEvent, event.Priority)
			}
		}
		if ctx.Err() != nil {
			break
		}
	}
}

func validateScheduleTimestamp(now, deliverAt time.Time) error {
	if deliverAt.IsZero() {
		return nil
	}
	if deliverAt.Before(now) {
		return fmt.Errorf("delivery timestamp %s is in the past", deliverAt.UTC().Format(time.RFC3339Nano))
	}
	if deliverAt.Sub(now) > maxScheduledDeliveryHorizon {
		return fmt.Errorf("delivery timestamp exceeds %s horizon", maxScheduledDeliveryHorizon)
	}
	return nil
}

func (r *Router) loadSessionSnapshots() error {
	if r.wal == nil {
		return nil
	}
	snapshots, err := r.wal.LoadSessionSnapshots()
	if err != nil {
		return err
	}
	now := r.now()
	for _, snapshot := range snapshots {
		if snapshot.ConsumerID == "" {
			continue
		}
		if r.sessionSnapshotTTL > 0 && !snapshot.LastHeartbeat.IsZero() && now.Sub(snapshot.LastHeartbeat) > r.sessionSnapshotTTL {
			if err := r.wal.DeleteSessionSnapshot(snapshot.ConsumerID); err != nil {
				fmt.Printf("failed to delete stale session snapshot: %v\n", err)
			}
			continue
		}
		session := sessionFromSnapshot(snapshot)
		if session.ResumablePending {
			session.Live = false
			session.TransportIdentity = nil
		}
		if existing, ok := r.directSessions[session.ConsumerID]; !ok || existing.LastHeartbeat.Before(session.LastHeartbeat) {
			r.directSessions[session.ConsumerID] = session
			if n := parseSessionOrdinal(session.SessionID); n > r.nextSessionID {
				r.nextSessionID = n
			}
		}
	}
	return nil
}

func (r *Router) loadScheduledQueue() error {
	if r.wal == nil {
		return nil
	}
	entries, err := r.wal.LoadScheduled()
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scheduledQueue = append([]scheduledMessage(nil), entries...)
	for _, entry := range entries {
		if entry.Sequence > r.nextScheduleSequence {
			r.nextScheduleSequence = entry.Sequence
		}
		if entry.EnqueueSequence > r.nextQueueSequence {
			r.nextQueueSequence = entry.EnqueueSequence
		}
	}
	r.sortScheduledLocked()
	return nil
}

func (r *Router) persistScheduledLocked() {
	if r.wal == nil {
		return
	}
	entries := make([]scheduledMessage, len(r.scheduledQueue))
	copy(entries, r.scheduledQueue)
	if err := r.wal.SaveScheduled(entries); err != nil {
		fmt.Printf("failed to persist delayed queue: %v\n", err)
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
		if !ok || !isSessionLive(session) {
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
		r.inflight[entry.MessageID] = &inflightMessage{MessageID: entry.MessageID, ConsumerID: entry.Consumer, SessionID: session.SessionID, Topic: entry.Topic, Payload: append([]byte(nil), entry.Payload...), Priority: r.normalizePriority(entry.Priority), DeliveryAttempt: attempt, EnqueueSequence: entry.EnqueueSequence, Status: statusDispatched, DispatchedAt: r.now()}
		session.InflightCount++
		r.metrics.WALReplayed++
		deferred = append(deferred, retryDispatch{identity: append([]byte(nil), session.TransportIdentity...), topic: entry.Topic, payload: append([]byte(nil), entry.Payload...)})
	}
	r.mu.Unlock()
	r.sendDeferred(deferred)
}

func (r *Router) promoteDeferredForConsumer(consumerID string) {
	r.mu.Lock()
	session, ok := r.directSessions[consumerID]
	if !ok || !isSessionLive(session) {
		r.mu.Unlock()
		return
	}
	topics := make([]string, 0, len(session.Subscriptions))
	for topic := range session.Subscriptions {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	deferred := make([]retryDispatch, 0)
	for session.InflightCount < session.MaxInflight {
		dispatched := false
		for _, topic := range topics {
			queue := r.directQueue[topic]
			if queue == nil || len(queue.Messages) == 0 {
				continue
			}
			msg := queue.Messages[0]
			if len(queue.Messages) == 1 {
				delete(r.directQueue, topic)
			} else {
				queue.Messages = queue.Messages[1:]
				r.directQueue[topic] = queue
			}
			if session.BacklogCount > 0 {
				session.BacklogCount--
			}
			identity, walEntry, needsWAL := r.prepareDispatchLocked(session, msg.Topic, msg.MessageID, msg.Payload, msg.Priority, msg.EnqueueSequence, 1)
			if needsWAL {
				if err := r.wal.AppendDispatched(walEntry); err != nil {
					fmt.Printf("failed to append wal dispatch: %v\n", err)
				} else {
					r.metrics.WALWritten++
				}
			}
			deferred = append(deferred, retryDispatch{identity: identity, topic: msg.Topic, payload: msg.Payload})
			dispatched = true
			break
		}
		if !dispatched {
			break
		}
	}
	r.mu.Unlock()
	r.sendDeferred(deferred)
}

func (r *Router) processInflightTimeouts() {
	r.mu.Lock()
	deadLetteredRecords := make([]DeadLetterRecord, 0)
	now := r.now()
	for messageID, record := range r.inflight {
		if now.Sub(record.DispatchedAt) < r.deliveryTimeout {
			continue
		}
		r.metrics.DeliveryTimeout++
		if record.DeliveryAttempt < r.maxDirectRetries {
			record.DeliveryAttempt++
			record.Status = statusRetry
			delete(r.inflight, messageID)
			if s, ok := r.directSessions[record.ConsumerID]; ok && s.InflightCount > 0 {
				s.InflightCount--
			}
			r.metrics.Retried++
			r.metrics.RetryDueToTimeout++
			r.scheduleDispatchLocked(record.Topic, record.MessageID, record.Payload, record.Priority, record.EnqueueSequence, record.DeliveryAttempt, now.Add(r.deliveryTimeout), "timeout_retry")
			continue
		}
		if s, ok := r.directSessions[record.ConsumerID]; ok && s.InflightCount > 0 {
			s.InflightCount--
		}
		delete(r.inflight, messageID)
		r.completed[messageID] = statusDeadLettered
		r.metrics.DeadLettered++
		if r.wal != nil {
			deadLetteredRecords = append(deadLetteredRecords, r.deadLetterRecord(messageID, record, "delivery_timeout"))
		}
	}
	r.mu.Unlock()
	for _, record := range deadLetteredRecords {
		if err := r.wal.AppendDeadLettered(record); err != nil {
			fmt.Printf("failed to append wal dead-letter: %v\n", err)
		}
	}
}

func (r *Router) deadLetterRecord(messageID string, record *inflightMessage, reason string) DeadLetterRecord {
	if record == nil {
		return DeadLetterRecord{MessageID: messageID, Reason: reason, DeadLetteredAt: r.now()}
	}
	return DeadLetterRecord{
		MessageID:       messageID,
		ConsumerID:      record.ConsumerID,
		SessionID:       record.SessionID,
		Topic:           record.Topic,
		Payload:         append([]byte(nil), record.Payload...),
		Priority:        record.Priority,
		EnqueueSequence: record.EnqueueSequence,
		Attempt:         record.DeliveryAttempt,
		Reason:          reason,
		DeadLetteredAt:  r.now(),
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
	now := r.now()
	r.mu.Lock()
	session := r.upsertSessionLocked(clientID, msg, now)
	r.mu.Unlock()
	if err := r.persistSessionSnapshot(session); err != nil {
		fmt.Printf("failed to persist session snapshot: %v\n", err)
	}
	r.replayFromWAL(msg.ConsumerID)
	r.promoteDeferredForConsumer(msg.ConsumerID)
	r.promoteScheduledDue()
}
func (r *Router) upsertSessionLocked(clientID []byte, msg controlMessage, now time.Time) *consumerSession {
	existing, ok := r.directSessions[msg.ConsumerID]
	if !ok || existing.SessionID == "" {
		r.nextSessionID++
		existing = &consumerSession{SessionID: fmt.Sprintf("sess_%06d", r.nextSessionID), ConsumerID: msg.ConsumerID, ConnectedAt: now}
		r.directSessions[msg.ConsumerID] = existing
	}
	existing.TransportIdentity = append(existing.TransportIdentity[:0], clientID...)
	existing.Subscriptions = make(map[string]struct{}, len(msg.Subscriptions))
	for _, topic := range msg.Subscriptions {
		existing.Subscriptions[topic] = struct{}{}
	}
	existing.Capabilities = capabilityHints{SupportsAck: msg.Capabilities.SupportsAck, SupportsCompression: cloneStrings(msg.Capabilities.SupportsCompression), SupportsCodec: cloneStrings(msg.Capabilities.SupportsCodec), Resumable: msg.Capabilities.Resumable}
	existing.MaxInflight = msg.Capabilities.MaxInflight
	if existing.MaxInflight <= 0 || existing.MaxInflight > r.maxInflightPerConsumer {
		existing.MaxInflight = r.maxInflightPerConsumer
	}
	if existing.ConnectedAt.IsZero() {
		existing.ConnectedAt = now
	}
	existing.LastHeartbeat = now
	existing.Live = true
	existing.ResumablePending = existing.Capabilities.Resumable
	return cloneSession(existing)
}
func (r *Router) heartbeatConsumer(consumerID string) {
	r.mu.Lock()
	var snapshot *consumerSession
	if session, ok := r.directSessions[consumerID]; ok {
		session.LastHeartbeat = r.now()
		snapshot = cloneSession(session)
	}
	r.mu.Unlock()
	if snapshot != nil {
		if err := r.persistSessionSnapshot(snapshot); err != nil {
			fmt.Printf("failed to persist session heartbeat: %v\n", err)
		}
	}
}

func (r *Router) dispatchDirect(topic, messageID string, payload []byte, priority string) {
	retries := r.dispatchDirectAttempt(topic, messageID, payload, priority, 0, 1)
	r.sendDeferred(retries)
}

func (r *Router) dispatchDirectAttempt(topic, messageID string, payload []byte, priority string, enqueueSequence uint64, attempt int) []retryDispatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.maxGlobalIngress > 0 && r.totalDirectLoadLocked() >= r.maxGlobalIngress {
		r.metrics.Dropped++
		fmt.Printf("{\"event\":\"ingress_drop\",\"topic\":%q,\"message_id\":%q,\"reason\":\"global_ingress_limit\",\"limit\":%d}\n", topic, messageID, r.maxGlobalIngress)
		return nil
	}
	session := r.selectSession(topic)
	if session == nil {
		if messageID != "" {
			r.enqueueDirectLocked(topic, messageID, payload, priority, enqueueSequence)
		}
		return nil
	}
	identity, walEntry, needsWAL := r.prepareDispatchLocked(session, topic, messageID, payload, priority, enqueueSequence, attempt)
	if needsWAL {
		if err := r.wal.AppendDispatched(walEntry); err != nil {
			fmt.Printf("failed to append wal dispatch: %v\n", err)
		} else {
			r.metrics.WALWritten++
		}
	}
	return []retryDispatch{{identity: identity, topic: topic, payload: payload}}
}

func (r *Router) scheduleDispatch(topic, messageID string, payload []byte, priority string, enqueueSequence uint64, attempt int, deliverAt time.Time, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scheduleDispatchLocked(topic, messageID, payload, priority, enqueueSequence, attempt, deliverAt, reason)
}
func (r *Router) scheduleDispatchLocked(topic, messageID string, payload []byte, priority string, enqueueSequence uint64, attempt int, deliverAt time.Time, reason string) {
	if messageID == "" {
		return
	}
	priority = r.normalizePriority(priority)
	if enqueueSequence == 0 {
		enqueueSequence = r.nextEnqueueSequenceLocked()
	}
	r.nextScheduleSequence++
	r.scheduledQueue = append(r.scheduledQueue, scheduledMessage{Sequence: r.nextScheduleSequence, MessageID: messageID, Topic: topic, Payload: append([]byte(nil), payload...), Priority: priority, EnqueueSequence: enqueueSequence, DeliveryAttempt: attempt, DeliverAt: deliverAt, Reason: reason})
	r.sortScheduledLocked()
	r.metrics.Scheduled++
	r.persistScheduledLocked()
}
func (r *Router) promoteScheduledDue() {
	var due []scheduledMessage
	r.mu.Lock()
	now := r.now()
	idx := 0
	for idx < len(r.scheduledQueue) && !r.scheduledQueue[idx].DeliverAt.After(now) {
		idx++
	}
	if idx > 0 {
		due = append([]scheduledMessage(nil), r.scheduledQueue[:idx]...)
		r.scheduledQueue = append([]scheduledMessage(nil), r.scheduledQueue[idx:]...)
		r.persistScheduledLocked()
	}
	r.mu.Unlock()
	for _, entry := range due {
		r.mu.Lock()
		r.metrics.ScheduledPromoted++
		r.mu.Unlock()
		r.sendDeferred(r.dispatchDirectAttempt(entry.Topic, entry.MessageID, entry.Payload, entry.Priority, entry.EnqueueSequence, entry.DeliveryAttempt))
	}
}

func (r *Router) prepareDispatchLocked(session *consumerSession, topic, messageID string, payload []byte, priority string, enqueueSequence uint64, attempt int) ([]byte, walDispatchedEntry, bool) {
	priority = r.normalizePriority(priority)
	if enqueueSequence == 0 {
		enqueueSequence = r.nextEnqueueSequenceLocked()
	}
	if session.Capabilities.SupportsAck && messageID != "" {
		r.inflight[messageID] = &inflightMessage{MessageID: messageID, ConsumerID: session.ConsumerID, SessionID: session.SessionID, Topic: topic, Payload: append([]byte(nil), payload...), Priority: priority, DeliveryAttempt: attempt, EnqueueSequence: enqueueSequence, Status: statusDispatched, DispatchedAt: r.now()}
		session.InflightCount++
	}
	r.metrics.Dispatched++
	identity := append([]byte(nil), session.TransportIdentity...)
	walEntry := walDispatchedEntry{MessageID: messageID, Consumer: session.ConsumerID, SessionID: session.SessionID, Topic: topic, Payload: append([]byte(nil), payload...), Priority: priority, EnqueueSequence: enqueueSequence, Attempt: attempt}
	needsWAL := r.wal != nil && session.Capabilities.SupportsAck && messageID != ""
	return identity, walEntry, needsWAL
}
func (r *Router) enqueueDirectLocked(topic, messageID string, payload []byte, priority string, enqueueSequence uint64) {
	queue := r.directQueue[topic]
	if queue == nil {
		queue = &priorityQueue{}
	}
	if r.maxPerTopicQueue > 0 && len(queue.Messages) >= r.maxPerTopicQueue {
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
	if enqueueSequence == 0 {
		enqueueSequence = r.nextEnqueueSequenceLocked()
	}
	queue.Messages = append(queue.Messages, queuedDirectMessage{MessageID: messageID, Topic: topic, Payload: append([]byte(nil), payload...), Priority: r.normalizePriority(priority), EnqueueSequence: enqueueSequence})
	r.sortPriorityQueueLocked(queue)
	r.directQueue[topic] = queue
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
		if queue != nil {
			total += len(queue.Messages)
		}
	}
	return total
}
func (r *Router) totalDirectLoadLocked() int { return len(r.inflight) + r.totalQueuedLocked() }
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
		if !isSessionLive(s) {
			continue
		}
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
func (r *Router) ConsumerBacklogSnapshot() map[string]ConsumerBacklogMetrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]ConsumerBacklogMetrics, len(r.directSessions))
	for consumerID, s := range r.directSessions {
		out[consumerID] = ConsumerBacklogMetrics{Inflight: s.InflightCount, MaxInflight: s.MaxInflight, Backlog: s.BacklogCount}
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
		record.DeliveryAttempt++
		record.Status = statusRetry
		delete(r.inflight, messageID)
		if s, ok := r.directSessions[record.ConsumerID]; ok && s.InflightCount > 0 {
			s.InflightCount--
		}
		r.metrics.Retried++
		r.scheduleDispatchLocked(record.Topic, record.MessageID, record.Payload, record.Priority, record.EnqueueSequence, record.DeliveryAttempt, r.now().Add(r.deliveryTimeout), "nack_retry")
		r.mu.Unlock()
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
		if err := r.wal.AppendDeadLettered(r.deadLetterRecord(messageID, record, status)); err != nil {
			fmt.Printf("failed to append wal dead-letter: %v\n", err)
		}
	}
	r.sendDeferred(drain)
}
func (r *Router) drainDeferredLocked(topic string) []retryDispatch {
	selectedTopic, msg, ok := r.selectDeferredForDispatchLocked(topic)
	if !ok {
		return nil
	}
	session := r.selectSession(selectedTopic)
	if session == nil {
		return nil
	}
	if session.BacklogCount > 0 {
		session.BacklogCount--
	}
	identity, walEntry, needsWAL := r.prepareDispatchLocked(session, msg.Topic, msg.MessageID, msg.Payload, msg.Priority, msg.EnqueueSequence, 1)
	if needsWAL {
		if err := r.wal.AppendDispatched(walEntry); err != nil {
			fmt.Printf("failed to append wal dispatch: %v\n", err)
		} else {
			r.metrics.WALWritten++
		}
	}
	return []retryDispatch{{identity: identity, topic: msg.Topic, payload: msg.Payload}}
}

func (r *Router) selectDeferredForDispatchLocked(preferredTopic string) (string, queuedDirectMessage, bool) {
	candidates := r.subscribedTopicsLocked(preferredTopic)
	if len(candidates) == 0 {
		return "", queuedDirectMessage{}, false
	}
	var (
		bestTopic string
		bestMsg   queuedDirectMessage
		found     bool
		bestScore int
	)
	for _, topic := range candidates {
		queue := r.directQueue[topic]
		if queue == nil || len(queue.Messages) == 0 {
			continue
		}
		msg := queue.Messages[0]
		score := r.priorityScoreLocked(msg)
		if !found || score > bestScore || (score == bestScore && msg.EnqueueSequence < bestMsg.EnqueueSequence) || (score == bestScore && msg.EnqueueSequence == bestMsg.EnqueueSequence && topic < bestTopic) {
			bestTopic, bestMsg, bestScore, found = topic, msg, score, true
		}
	}
	if !found {
		return "", queuedDirectMessage{}, false
	}
	queue := r.directQueue[bestTopic]
	if len(queue.Messages) == 1 {
		delete(r.directQueue, bestTopic)
	} else {
		queue.Messages = queue.Messages[1:]
		r.directQueue[bestTopic] = queue
	}
	return bestTopic, bestMsg, true
}

func (r *Router) subscribedTopicsLocked(preferredTopic string) []string {
	topicSet := map[string]struct{}{}
	if preferredTopic != "" {
		topicSet[preferredTopic] = struct{}{}
	}
	for _, session := range r.directSessions {
		if !isSessionLive(session) || session.InflightCount >= session.MaxInflight {
			continue
		}
		for topic := range session.Subscriptions {
			topicSet[topic] = struct{}{}
		}
	}
	topics := make([]string, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

func (r *Router) nextEnqueueSequenceLocked() uint64 {
	r.nextQueueSequence++
	return r.nextQueueSequence
}

func (r *Router) normalizePriority(priority string) string {
	return r.normalizePriorityLocked(priority)
}

func (r *Router) normalizePriorityLocked(priority string) string {
	priority = strings.ToLower(strings.TrimSpace(priority))
	if priority != "" {
		if _, ok := r.priorityRank[priority]; ok {
			return priority
		}
	}
	if len(r.priorityClasses) == 0 {
		return "normal"
	}
	for _, candidate := range r.priorityClasses {
		if candidate == "normal" {
			return candidate
		}
	}
	return r.priorityClasses[len(r.priorityClasses)/2]
}

func (r *Router) priorityScoreLocked(msg queuedDirectMessage) int {
	priority := r.normalizePriorityLocked(msg.Priority)
	score := r.priorityWeights[priority]
	if !r.priorityPreemption || r.priorityBoostThreshold <= 0 || r.priorityBoostOffset <= 0 {
		return score
	}
	if msg.EnqueueSequence == 0 || r.nextQueueSequence <= msg.EnqueueSequence {
		return score
	}
	age := int((r.nextQueueSequence - msg.EnqueueSequence) / uint64(r.priorityBoostThreshold))
	return score + (age * r.priorityBoostOffset)
}

func (r *Router) sortPriorityQueueLocked(queue *priorityQueue) {
	if queue == nil {
		return
	}
	sort.SliceStable(queue.Messages, func(i, j int) bool {
		left, right := queue.Messages[i], queue.Messages[j]
		leftRank := r.priorityRank[r.normalizePriorityLocked(left.Priority)]
		rightRank := r.priorityRank[r.normalizePriorityLocked(right.Priority)]
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		return left.EnqueueSequence < right.EnqueueSequence
	})
}
func (r *Router) sendDeferred(retries []retryDispatch) {
	for _, retry := range retries {
		if r.directSender == nil || len(retry.identity) == 0 {
			continue
		}
		if err := r.directSender(retry.identity, retry.topic, retry.payload); err != nil {
			fmt.Printf("failed to direct-dispatch deferred event: %v\n", err)
		}
	}
}
func isSessionLive(session *consumerSession) bool {
	if session == nil || len(session.TransportIdentity) == 0 {
		return false
	}
	if session.Live {
		return true
	}
	return !session.ResumablePending
}
func (r *Router) persistSessionSnapshot(session *consumerSession) error {
	if r.wal == nil || session == nil || !session.Capabilities.Resumable {
		return nil
	}
	return r.wal.SaveSessionSnapshot(snapshotFromSession(session))
}
func snapshotFromSession(session *consumerSession) sessionSnapshot {
	subscriptions := make([]string, 0, len(session.Subscriptions))
	for topic := range session.Subscriptions {
		subscriptions = append(subscriptions, topic)
	}
	sort.Strings(subscriptions)
	return sessionSnapshot{SessionID: session.SessionID, ConsumerID: session.ConsumerID, Subscriptions: subscriptions, ConnectedAt: session.ConnectedAt, LastHeartbeat: session.LastHeartbeat, MaxInflight: session.MaxInflight, SupportsAck: session.Capabilities.SupportsAck, SupportsCompression: cloneStrings(session.Capabilities.SupportsCompression), SupportsCodec: cloneStrings(session.Capabilities.SupportsCodec), Resumable: session.Capabilities.Resumable, Live: session.Live, ResumablePending: session.ResumablePending}
}
func sessionFromSnapshot(snapshot sessionSnapshot) *consumerSession {
	subs := make(map[string]struct{}, len(snapshot.Subscriptions))
	for _, topic := range snapshot.Subscriptions {
		subs[topic] = struct{}{}
	}
	return &consumerSession{SessionID: snapshot.SessionID, ConsumerID: snapshot.ConsumerID, Subscriptions: subs, Capabilities: capabilityHints{SupportsAck: snapshot.SupportsAck, SupportsCompression: cloneStrings(snapshot.SupportsCompression), SupportsCodec: cloneStrings(snapshot.SupportsCodec), Resumable: snapshot.Resumable}, MaxInflight: snapshot.MaxInflight, ConnectedAt: snapshot.ConnectedAt, LastHeartbeat: snapshot.LastHeartbeat, Live: false, ResumablePending: snapshot.Resumable}
}
func cloneSession(session *consumerSession) *consumerSession {
	if session == nil {
		return nil
	}
	copy := *session
	copy.TransportIdentity = append([]byte(nil), session.TransportIdentity...)
	copy.Capabilities = capabilityHints{SupportsAck: session.Capabilities.SupportsAck, SupportsCompression: cloneStrings(session.Capabilities.SupportsCompression), SupportsCodec: cloneStrings(session.Capabilities.SupportsCodec), Resumable: session.Capabilities.Resumable}
	copy.Subscriptions = make(map[string]struct{}, len(session.Subscriptions))
	for k := range session.Subscriptions {
		copy.Subscriptions[k] = struct{}{}
	}
	return &copy
}
func cloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
func parseSessionOrdinal(sessionID string) uint64 {
	var n uint64
	_, _ = fmt.Sscanf(sessionID, "sess_%d", &n)
	return n
}
func (r *Router) sortScheduledLocked() {
	sort.SliceStable(r.scheduledQueue, func(i, j int) bool {
		if r.scheduledQueue[i].DeliverAt.Equal(r.scheduledQueue[j].DeliverAt) {
			return r.scheduledQueue[i].Sequence < r.scheduledQueue[j].Sequence
		}
		return r.scheduledQueue[i].DeliverAt.Before(r.scheduledQueue[j].DeliverAt)
	})
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
		return msg[0], string(msg[1]), msg[2], nil
	case len(msg) == 4 && len(msg[1]) == 0:
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
