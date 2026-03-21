package zmq

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/aetherbus/aetherbus-tachyon/internal/media"
)

type stubWAL struct {
	dispatched   []walDispatchedEntry
	committed    []string
	deadLettered []string
	replay       []walDispatchedEntry
	snapshots    map[string]sessionSnapshot
	scheduled    []scheduledMessage
}

func (w *stubWAL) AppendDispatched(entry walDispatchedEntry) error {
	w.dispatched = append(w.dispatched, entry)
	return nil
}

func (w *stubWAL) AppendCommitted(messageID string) error {
	w.committed = append(w.committed, messageID)
	return nil
}

func (w *stubWAL) AppendDeadLettered(messageID string) error {
	w.deadLettered = append(w.deadLettered, messageID)
	return nil
}
func (w *stubWAL) ReplayUnacked() ([]walDispatchedEntry, error) {
	return append([]walDispatchedEntry(nil), w.replay...), nil
}

func (w *stubWAL) SaveSessionSnapshot(snapshot sessionSnapshot) error {
	if w.snapshots == nil {
		w.snapshots = map[string]sessionSnapshot{}
	}
	w.snapshots[snapshot.ConsumerID] = snapshot
	return nil
}

func (w *stubWAL) LoadSessionSnapshots() ([]sessionSnapshot, error) {
	out := make([]sessionSnapshot, 0, len(w.snapshots))
	for _, snapshot := range w.snapshots {
		out = append(out, snapshot)
	}
	return out, nil
}

func (w *stubWAL) DeleteSessionSnapshot(consumerID string) error {
	delete(w.snapshots, consumerID)
	return nil
}

func (w *stubWAL) SaveScheduled(entries []scheduledMessage) error {
	w.scheduled = append([]scheduledMessage(nil), entries...)
	return nil
}

func (w *stubWAL) LoadScheduled() ([]scheduledMessage, error) {
	return append([]scheduledMessage(nil), w.scheduled...), nil
}

func TestParseFrames(t *testing.T) {
	tests := []struct {
		name         string
		msg          [][]byte
		wantClientID string
		wantTopic    string
		wantPayload  string
		wantErr      bool
	}{
		{
			name:         "three-frame message",
			msg:          [][]byte{[]byte("cid"), []byte("user.created"), []byte("payload")},
			wantClientID: "cid",
			wantTopic:    "user.created",
			wantPayload:  "payload",
			wantErr:      false,
		},
		{
			name:         "four-frame message with delimiter",
			msg:          [][]byte{[]byte("cid"), []byte(""), []byte("user.created"), []byte("payload")},
			wantClientID: "cid",
			wantTopic:    "user.created",
			wantPayload:  "payload",
			wantErr:      false,
		},
		{
			name:    "invalid shape",
			msg:     [][]byte{[]byte("cid"), []byte("payload")},
			wantErr: true,
		},
		{
			name:    "invalid extended shape with extra frames",
			msg:     [][]byte{[]byte("cid"), []byte(""), []byte("user.created"), []byte("payload"), []byte("extra")},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clientID, topic, payload, err := parseFrames(tc.msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("expected err=%v, got err=%v", tc.wantErr, err)
			}
			if err != nil {
				return
			}

			if string(clientID) != tc.wantClientID {
				t.Fatalf("expected clientID %q, got %q", tc.wantClientID, string(clientID))
			}
			if topic != tc.wantTopic {
				t.Fatalf("expected topic %q, got %q", tc.wantTopic, topic)
			}
			if string(payload) != tc.wantPayload {
				t.Fatalf("expected payload %q, got %q", tc.wantPayload, string(payload))
			}
		})
	}
}

func TestValidateTopic(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		wantErr bool
	}{
		{name: "valid topic", topic: "orders.created", wantErr: false},
		{name: "control topic", topic: "_control", wantErr: false},
		{name: "empty topic", topic: "", wantErr: true},
		{name: "leading dot", topic: ".orders", wantErr: true},
		{name: "trailing dot", topic: "orders.", wantErr: true},
		{name: "double dot", topic: "orders..created", wantErr: true},
		{name: "contains whitespace", topic: "orders. created", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTopic(tc.topic)
			if (err != nil) != tc.wantErr {
				t.Fatalf("expected err=%v, got err=%v", tc.wantErr, err)
			}
		})
	}
}

func TestHandleAckDuplicateAndStale(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       10,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	r.dispatchDirect("orders.created", "msg-1", []byte(`{"id":"msg-1"}`), "normal")
	if got := r.metrics.Dispatched; got != 1 {
		t.Fatalf("expected dispatched=1, got %d", got)
	}

	r.handleAck("msg-1", "worker-1", "sess_000001")
	if got := r.metrics.Acked; got != 1 {
		t.Fatalf("expected acked=1, got %d", got)
	}

	r.handleAck("msg-1", "worker-1", "sess_000001") // duplicate ACK
	if got := r.metrics.Acked; got != 1 {
		t.Fatalf("expected duplicate ACK to be harmless, got acked=%d", got)
	}

	r.handleAck("missing", "worker-1", "sess_000001") // stale ACK
	if got := r.metrics.Acked; got != 1 {
		t.Fatalf("expected stale ACK to be ignored, got acked=%d", got)
	}
}

func TestRetryableNackRetried(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       10,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	sendCount := 0
	r.directSender = func(identity []byte, topic string, payload []byte) error {
		sendCount++
		return nil
	}

	r.dispatchDirect("orders.created", "msg-2", []byte(`{"id":"msg-2"}`), "normal")
	r.handleNack("msg-2", "worker-1", "sess_000001", "retryable_error")

	if got := r.metrics.Nacked; got != 1 {
		t.Fatalf("expected nacked=1, got %d", got)
	}
	if got := r.metrics.Retried; got != 1 {
		t.Fatalf("expected retried=1, got %d", got)
	}
	if got := r.metrics.DeadLettered; got != 0 {
		t.Fatalf("expected deadlettered=0, got %d", got)
	}
	if got := r.metrics.Dispatched; got != 1 {
		t.Fatalf("expected dispatched=1 before scheduled retry promotion, got %d", got)
	}
	if sendCount != 1 {
		t.Fatalf("expected direct sender called once before scheduled retry promotion, got %d", sendCount)
	}
	if len(r.scheduledQueue) != 1 {
		t.Fatalf("expected scheduled retry queued, got %d", len(r.scheduledQueue))
	}
}

func TestTerminalNackDeadLettered(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       10,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	r.dispatchDirect("orders.created", "msg-3", []byte(`{"id":"msg-3"}`), "normal")
	r.handleNack("msg-3", "worker-1", "sess_000001", "terminal_error")

	if got := r.metrics.Nacked; got != 1 {
		t.Fatalf("expected nacked=1, got %d", got)
	}
	if got := r.metrics.Retried; got != 0 {
		t.Fatalf("expected retried=0, got %d", got)
	}
	if got := r.metrics.DeadLettered; got != 1 {
		t.Fatalf("expected deadlettered=1, got %d", got)
	}
	if _, ok := r.inflight["msg-3"]; ok {
		t.Fatalf("expected inflight entry removed for terminal nack")
	}
}

func TestTimeoutRetry(t *testing.T) {
	r := NewRouterWithOptions("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor(), 3, 50*time.Millisecond)
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       10,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	now := time.Unix(1000, 0).UTC()
	r.now = func() time.Time { return now }
	sendCount := 0
	r.directSender = func(identity []byte, topic string, payload []byte) error {
		sendCount++
		return nil
	}

	r.dispatchDirect("orders.created", "msg-timeout-retry", []byte(`{"id":"msg-timeout-retry"}`), "normal")
	now = now.Add(51 * time.Millisecond)
	r.processInflightTimeouts()

	if got := r.metrics.DeliveryTimeout; got != 1 {
		t.Fatalf("expected delivery_timeout=1, got %d", got)
	}
	if got := r.metrics.RetryDueToTimeout; got != 1 {
		t.Fatalf("expected retry_due_to_timeout=1, got %d", got)
	}
	if got := r.metrics.Retried; got != 1 {
		t.Fatalf("expected retried=1, got %d", got)
	}
	if got := r.metrics.Dispatched; got != 1 {
		t.Fatalf("expected dispatched=1 before scheduled retry promotion, got %d", got)
	}
	if sendCount != 1 {
		t.Fatalf("expected direct sender called once before scheduled retry promotion, got %d", sendCount)
	}
	if len(r.scheduledQueue) != 1 {
		t.Fatalf("expected scheduled retry queued, got %d", len(r.scheduledQueue))
	}
}

func TestTimeoutDeadLetter(t *testing.T) {
	r := NewRouterWithOptions("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor(), 1, 50*time.Millisecond)
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       10,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	now := time.Unix(1000, 0).UTC()
	r.now = func() time.Time { return now }

	r.dispatchDirect("orders.created", "msg-timeout-dead", []byte(`{"id":"msg-timeout-dead"}`), "normal")
	now = now.Add(51 * time.Millisecond)
	r.processInflightTimeouts()

	if got := r.metrics.DeliveryTimeout; got != 1 {
		t.Fatalf("expected delivery_timeout=1, got %d", got)
	}
	if got := r.metrics.RetryDueToTimeout; got != 0 {
		t.Fatalf("expected retry_due_to_timeout=0, got %d", got)
	}
	if got := r.metrics.DeadLettered; got != 1 {
		t.Fatalf("expected deadlettered=1, got %d", got)
	}
	if _, ok := r.inflight["msg-timeout-dead"]; ok {
		t.Fatalf("expected inflight entry removed for timeout dead-letter")
	}
}

func TestConsumerSaturationStopsDirectDispatch(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       1,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	r.dispatchDirect("orders.created", "msg-1", []byte(`{"id":"msg-1"}`), "normal")
	r.dispatchDirect("orders.created", "msg-2", []byte(`{"id":"msg-2"}`), "normal")

	if got := r.metrics.Dispatched; got != 1 {
		t.Fatalf("expected only first message dispatched, got %d", got)
	}
	if got := r.metrics.DispatchPaused; got != 1 {
		t.Fatalf("expected one paused dispatch due to saturation, got %d", got)
	}
	if got := r.metrics.BacklogQueued; got != 1 {
		t.Fatalf("expected one backlog increment, got %d", got)
	}
	if session := r.directSessions["worker-1"]; session.InflightCount != 1 {
		t.Fatalf("expected inflight to remain saturated at 1, got %d", session.InflightCount)
	}
}

func TestDispatchPauseTracksConsumerBacklog(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       1,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	r.dispatchDirect("orders.created", "msg-1", []byte(`{"id":"msg-1"}`), "normal")
	r.dispatchDirect("orders.created", "msg-2", []byte(`{"id":"msg-2"}`), "normal")
	r.dispatchDirect("orders.created", "msg-3", []byte(`{"id":"msg-3"}`), "normal")

	snapshot := r.ConsumerBacklogSnapshot()
	metrics, ok := snapshot["worker-1"]
	if !ok {
		t.Fatalf("expected worker-1 in backlog snapshot")
	}
	if metrics.Backlog != 2 {
		t.Fatalf("expected backlog=2, got %d", metrics.Backlog)
	}
	if metrics.MaxInflight != 1 {
		t.Fatalf("expected max_inflight=1, got %d", metrics.MaxInflight)
	}
}

func TestResumeDispatchAfterAckDropsInflight(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       1,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	r.dispatchDirect("orders.created", "msg-1", []byte(`{"id":"msg-1"}`), "normal")
	r.dispatchDirect("orders.created", "msg-2", []byte(`{"id":"msg-2"}`), "normal") // paused
	r.handleAck("msg-1", "worker-1", "sess_000001")
	r.dispatchDirect("orders.created", "msg-3", []byte(`{"id":"msg-3"}`), "normal") // resumed

	if got := r.metrics.Dispatched; got != 2 {
		t.Fatalf("expected two dispatched messages after resume, got %d", got)
	}
	if session := r.directSessions["worker-1"]; session.InflightCount != 1 {
		t.Fatalf("expected one inflight after redispatch, got %d", session.InflightCount)
	}
	snapshot := r.ConsumerBacklogSnapshot()
	if got := snapshot["worker-1"].Backlog; got != 1 {
		t.Fatalf("expected backlog to retain one deferred message after resume dispatch, got %d", got)
	}
}

func TestDispatchDirectWritesWALAndAckCommits(t *testing.T) {
	w := &stubWAL{}
	r := NewRouterWithDurability("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor(), 3, time.Second, w)
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       10,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	r.dispatchDirect("orders.created", "msg-w1", []byte(`{"id":"msg-w1"}`), "normal")
	if len(w.dispatched) != 1 {
		t.Fatalf("expected one wal dispatch append, got %d", len(w.dispatched))
	}
	if got := r.metrics.WALWritten; got != 1 {
		t.Fatalf("expected wal_written=1, got %d", got)
	}

	r.handleAck("msg-w1", "worker-1", "sess_000001")
	if len(w.committed) != 1 || w.committed[0] != "msg-w1" {
		t.Fatalf("expected wal commit for msg-w1, got %#v", w.committed)
	}
}

func TestReplayFromWALOnRegister(t *testing.T) {
	w := &stubWAL{replay: []walDispatchedEntry{{
		MessageID: "msg-r1",
		Consumer:  "worker-1",
		SessionID: "sess_000001",
		Topic:     "orders.created",
		Payload:   []byte(`{"id":"msg-r1"}`),
		Attempt:   1,
	}}}
	r := NewRouterWithDurability("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor(), 3, time.Second, w)
	sent := 0
	r.directSender = func(identity []byte, topic string, payload []byte) error {
		sent++
		return nil
	}

	msg := controlMessage{
		Mode:          "direct",
		ConsumerID:    "worker-1",
		Subscriptions: []string{"orders.created"},
	}
	msg.Capabilities.SupportsAck = true
	msg.Capabilities.MaxInflight = 10
	msg.Capabilities.Resumable = true
	r.registerConsumerSession([]byte("cid1"), msg)

	if sent != 1 {
		t.Fatalf("expected replayed message to be sent once, got %d", sent)
	}
	if got := r.metrics.WALReplayed; got != 1 {
		t.Fatalf("expected wal_replayed=1, got %d", got)
	}
	if _, ok := r.inflight["msg-r1"]; !ok {
		t.Fatalf("expected replayed message in inflight")
	}
}

func TestStaleAckSessionMismatchIgnored(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       10,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	r.dispatchDirect("orders.created", "msg-4", []byte(`{"id":"msg-4"}`), "normal")
	r.handleAck("msg-4", "worker-1", "sess_old")

	if got := r.metrics.Acked; got != 0 {
		t.Fatalf("expected stale session ack ignored, got acked=%d", got)
	}
	if _, ok := r.inflight["msg-4"]; !ok {
		t.Fatalf("expected inflight entry to remain after stale session ack")
	}
}

func TestDeferredDispatchForSlowConsumer(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.SetQueueBounds(4, 8)
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       1,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}
	sends := 0
	r.directSender = func(identity []byte, topic string, payload []byte) error {
		sends++
		return nil
	}

	r.dispatchDirect("orders.created", "msg-1", []byte(`{"id":"msg-1"}`), "normal")
	r.dispatchDirect("orders.created", "msg-2", []byte(`{"id":"msg-2"}`), "normal")
	if got := r.metrics.Deferred; got != 1 {
		t.Fatalf("expected deferred=1, got %d", got)
	}
	r.handleAck("msg-1", "worker-1", "sess_000001")

	if sends != 2 {
		t.Fatalf("expected deferred message to be dispatched after ack, got sends=%d", sends)
	}
	if _, ok := r.inflight["msg-2"]; !ok {
		t.Fatalf("expected deferred message promoted to inflight")
	}
}

func TestPerTopicQueueBoundDropsExcess(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.SetQueueBounds(1, 8)
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       1,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	r.dispatchDirect("orders.created", "msg-1", []byte(`{"id":"msg-1"}`), "normal")
	r.dispatchDirect("orders.created", "msg-2", []byte(`{"id":"msg-2"}`), "normal")
	r.dispatchDirect("orders.created", "msg-3", []byte(`{"id":"msg-3"}`), "normal")

	if got := r.metrics.Dropped; got != 1 {
		t.Fatalf("expected dropped=1 for bounded queue overflow, got %d", got)
	}
}

func TestGlobalIngressLimitDropsNewDispatch(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.SetQueueBounds(4, 8)
	r.SetGlobalIngressLimit(1)
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       1,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	r.dispatchDirect("orders.created", "msg-1", []byte(`{"id":"msg-1"}`), "normal")
	r.dispatchDirect("orders.created", "msg-2", []byte(`{"id":"msg-2"}`), "normal")

	if got := r.metrics.Dropped; got != 1 {
		t.Fatalf("expected dropped=1 for global ingress protection, got %d", got)
	}
	if _, ok := r.inflight["msg-2"]; ok {
		t.Fatalf("expected second message to be dropped before inflight registration")
	}
}

func TestReplayFromWALPreservesIdentityAndAttempt(t *testing.T) {
	w := &stubWAL{replay: []walDispatchedEntry{{
		MessageID: "msg-r2",
		Consumer:  "worker-1",
		SessionID: "sess_old",
		Topic:     "orders.created",
		Payload:   []byte(`{"id":"msg-r2"}`),
		Attempt:   2,
	}}}
	r := NewRouterWithDurability("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor(), 3, time.Second, w)
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_new",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       10,
		Subscriptions:     map[string]struct{}{"orders.created": {}},
	}

	r.replayFromWAL("worker-1")
	replayed, ok := r.inflight["msg-r2"]
	if !ok {
		t.Fatalf("expected replayed inflight")
	}
	if replayed.MessageID != "msg-r2" || replayed.ConsumerID != "worker-1" {
		t.Fatalf("expected message identity preserved on replay, got %#v", replayed)
	}
	if replayed.DeliveryAttempt != 2 {
		t.Fatalf("expected replay attempt=2, got %d", replayed.DeliveryAttempt)
	}
}

func TestRetryExhaustionAppendsDeadLetterToWAL(t *testing.T) {
	w := &stubWAL{}
	r := NewRouterWithDurability("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor(), 2, 50*time.Millisecond, w)
	now := time.Unix(1000, 0).UTC()
	r.now = func() time.Time { return now }
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:         "sess_000001",
		ConsumerID:        "worker-1",
		TransportIdentity: []byte("cid1"),
		Capabilities:      capabilityHints{SupportsAck: true, Resumable: true},
		MaxInflight:       10,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	r.dispatchDirect("orders.created", "msg-exhaust", []byte(`{"id":"msg-exhaust"}`), "normal")
	r.handleNack("msg-exhaust", "worker-1", "sess_000001", "retryable_error")
	now = now.Add(51 * time.Millisecond)
	r.promoteScheduledDue()
	r.handleNack("msg-exhaust", "worker-1", "sess_000001", "retryable_error")

	if got := r.metrics.DeadLettered; got != 1 {
		t.Fatalf("expected deadlettered=1 after retry exhaustion, got %d", got)
	}
	if len(w.deadLettered) != 1 || w.deadLettered[0] != "msg-exhaust" {
		t.Fatalf("expected wal dead-letter append for exhausted retry, got %#v", w.deadLettered)
	}
}

func TestLoadSessionSnapshotsMarksResumablePendingUntilRegister(t *testing.T) {
	w := &stubWAL{snapshots: map[string]sessionSnapshot{
		"worker-1": {
			SessionID:           "sess_000007",
			ConsumerID:          "worker-1",
			Subscriptions:       []string{"orders.created"},
			ConnectedAt:         time.Unix(100, 0).UTC(),
			LastHeartbeat:       time.Unix(200, 0).UTC(),
			MaxInflight:         7,
			SupportsAck:         true,
			SupportsCompression: []string{"lz4"},
			SupportsCodec:       []string{"json"},
			Resumable:           true,
		},
	}}
	r := NewRouterWithDurability("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor(), 3, time.Second, w)
	r.now = func() time.Time { return time.Unix(250, 0).UTC() }

	if err := r.loadSessionSnapshots(); err != nil {
		t.Fatalf("load snapshots: %v", err)
	}
	session := r.directSessions["worker-1"]
	if session == nil {
		t.Fatalf("expected snapshot session loaded")
	}
	if session.Live {
		t.Fatalf("expected recovered session to remain non-live")
	}
	if !session.ResumablePending {
		t.Fatalf("expected resumable pending state")
	}
	if len(session.TransportIdentity) != 0 {
		t.Fatalf("expected no transport identity for recovered session")
	}
	if session.MaxInflight != 7 {
		t.Fatalf("expected max inflight restored, got %d", session.MaxInflight)
	}
	if got := session.Capabilities.SupportsCompression[0]; got != "lz4" {
		t.Fatalf("expected compression hint restored, got %q", got)
	}
	if r.selectSession("orders.created") != nil {
		t.Fatalf("expected recovered non-live session not selected for dispatch")
	}
}

func TestRegisterConsumerSessionRehydratesRecoveredSessionMetadata(t *testing.T) {
	w := &stubWAL{snapshots: map[string]sessionSnapshot{
		"worker-1": {
			SessionID:           "sess_000009",
			ConsumerID:          "worker-1",
			Subscriptions:       []string{"orders.created"},
			ConnectedAt:         time.Unix(100, 0).UTC(),
			LastHeartbeat:       time.Unix(200, 0).UTC(),
			MaxInflight:         5,
			SupportsAck:         true,
			SupportsCompression: []string{"lz4"},
			SupportsCodec:       []string{"json"},
			Resumable:           true,
		},
	}}
	r := NewRouterWithDurability("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor(), 3, time.Second, w)
	r.now = func() time.Time { return time.Unix(250, 0).UTC() }
	sentIdentity := ""
	r.directSender = func(identity []byte, topic string, payload []byte) error {
		sentIdentity = string(identity)
		return nil
	}
	if err := r.loadSessionSnapshots(); err != nil {
		t.Fatalf("load snapshots: %v", err)
	}
	w.replay = []walDispatchedEntry{{MessageID: "msg-r3", Consumer: "worker-1", SessionID: "sess_old", Topic: "orders.created", Payload: []byte(`{"id":"msg-r3"}`), Attempt: 1}}

	msg := controlMessage{Mode: "direct", ConsumerID: "worker-1", Subscriptions: []string{"orders.created"}}
	msg.Capabilities.SupportsAck = true
	msg.Capabilities.SupportsCompression = []string{"zstd", "lz4"}
	msg.Capabilities.SupportsCodec = []string{"json", "msgpack"}
	msg.Capabilities.Resumable = true
	msg.Capabilities.MaxInflight = 11
	r.registerConsumerSession([]byte("cid-new"), msg)

	session := r.directSessions["worker-1"]
	if session == nil {
		t.Fatalf("expected session registered")
	}
	if session.SessionID != "sess_000009" {
		t.Fatalf("expected logical session id preserved, got %q", session.SessionID)
	}
	if !session.Live || !session.ResumablePending {
		t.Fatalf("expected registered session live and resumable")
	}
	if got := string(session.TransportIdentity); got != "cid-new" {
		t.Fatalf("expected transport identity updated, got %q", got)
	}
	if session.MaxInflight != 11 {
		t.Fatalf("expected max inflight updated, got %d", session.MaxInflight)
	}
	if sentIdentity != "cid-new" {
		t.Fatalf("expected WAL replay to use new identity, got %q", sentIdentity)
	}
	persisted := w.snapshots["worker-1"]
	if len(persisted.SupportsCodec) != 2 || persisted.SupportsCodec[0] != "json" {
		t.Fatalf("expected snapshot capabilities persisted, got %#v", persisted.SupportsCodec)
	}
}

func TestLoadSessionSnapshotsDiscardsStaleSessions(t *testing.T) {
	w := &stubWAL{snapshots: map[string]sessionSnapshot{
		"worker-1": {
			SessionID:     "sess_000004",
			ConsumerID:    "worker-1",
			LastHeartbeat: time.Unix(100, 0).UTC(),
			Resumable:     true,
		},
	}}
	r := NewRouterWithDurability("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor(), 3, time.Second, w)
	r.sessionSnapshotTTL = 30 * time.Second
	r.now = func() time.Time { return time.Unix(200, 0).UTC() }

	if err := r.loadSessionSnapshots(); err != nil {
		t.Fatalf("load snapshots: %v", err)
	}
	if _, ok := r.directSessions["worker-1"]; ok {
		t.Fatalf("expected stale snapshot discarded")
	}
	if _, ok := w.snapshots["worker-1"]; ok {
		t.Fatalf("expected stale snapshot deleted from durability store")
	}
}

func TestScheduledPublishPromotesWhenDue(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{SessionID: "sess_000001", ConsumerID: "worker-1", TransportIdentity: []byte("cid1"), Capabilities: capabilityHints{SupportsAck: true, Resumable: true}, MaxInflight: 10, Subscriptions: map[string]struct{}{"orders.created": {}}}
	now := time.Unix(1000, 0).UTC()
	r.now = func() time.Time { return now }
	sends := 0
	r.directSender = func(identity []byte, topic string, payload []byte) error { sends++; return nil }

	r.scheduleDispatch("orders.created", "msg-scheduled", []byte(`{"id":"msg-scheduled"}`), "normal", 0, 1, now.Add(time.Second), "publish")
	if sends != 0 {
		t.Fatalf("expected no immediate send for scheduled publish")
	}
	now = now.Add(time.Second)
	r.promoteScheduledDue()
	if sends != 1 {
		t.Fatalf("expected scheduled publish promoted once, got %d", sends)
	}
	if got := r.metrics.ScheduledPromoted; got != 1 {
		t.Fatalf("expected scheduled_promoted=1, got %d", got)
	}
}

func TestScheduledRetryPromotionPreservesOrdering(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{SessionID: "sess_000001", ConsumerID: "worker-1", TransportIdentity: []byte("cid1"), Capabilities: capabilityHints{SupportsAck: true, Resumable: true}, MaxInflight: 10, Subscriptions: map[string]struct{}{"orders.created": {}}}
	now := time.Unix(1000, 0).UTC()
	r.now = func() time.Time { return now }
	order := make([]string, 0, 2)
	r.directSender = func(identity []byte, topic string, payload []byte) error {
		order = append(order, string(payload))
		return nil
	}

	r.scheduleDispatch("orders.created", "msg-2", []byte("second"), "normal", 0, 1, now.Add(2*time.Second), "retry")
	r.scheduleDispatch("orders.created", "msg-1", []byte("first"), "normal", 0, 1, now.Add(time.Second), "retry")
	now = now.Add(3 * time.Second)
	r.promoteScheduledDue()
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("expected due messages promoted in delivery order, got %#v", order)
	}
}

func TestLoadScheduledQueueRecoversDelayedEntries(t *testing.T) {
	w := &stubWAL{scheduled: []scheduledMessage{{Sequence: 2, MessageID: "msg-late", Topic: "orders.created", Payload: []byte("late"), DeliverAt: time.Unix(1020, 0).UTC()}, {Sequence: 1, MessageID: "msg-early", Topic: "orders.created", Payload: []byte("early"), DeliverAt: time.Unix(1010, 0).UTC()}}}
	r := NewRouterWithDurability("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor(), 3, time.Second, w)
	if err := r.loadScheduledQueue(); err != nil {
		t.Fatalf("load scheduled queue: %v", err)
	}
	if len(r.scheduledQueue) != 2 {
		t.Fatalf("expected two recovered scheduled entries, got %d", len(r.scheduledQueue))
	}
	if r.scheduledQueue[0].MessageID != "msg-early" {
		t.Fatalf("expected earliest entry first after recovery, got %q", r.scheduledQueue[0].MessageID)
	}
}

func TestValidateScheduleTimestampRejectsPastAndFarFuture(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	if err := validateScheduleTimestamp(now, now.Add(-time.Second)); err == nil {
		t.Fatalf("expected past timestamp rejection")
	}
	if err := validateScheduleTimestamp(now, now.Add(maxScheduledDeliveryHorizon+time.Second)); err == nil {
		t.Fatalf("expected far-future timestamp rejection")
	}
	if err := validateScheduleTimestamp(now, now.Add(time.Hour)); err != nil {
		t.Fatalf("expected near-future timestamp accepted, got %v", err)
	}
}

func TestDirectQueueDispatchesHigherPriorityFirst(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{SessionID: "sess_000001", ConsumerID: "worker-1", TransportIdentity: []byte("cid1"), Capabilities: capabilityHints{SupportsAck: true, Resumable: true}, MaxInflight: 1, Subscriptions: map[string]struct{}{"orders.created": {}}}

	r.dispatchDirect("orders.created", "msg-low", []byte(`{"id":"msg-low"}`), "low")
	r.dispatchDirect("orders.created", "msg-high", []byte(`{"id":"msg-high"}`), "high")
	r.handleAck("msg-low", "worker-1", "sess_000001")

	if got := r.inflight["msg-high"]; got == nil || got.Priority != "high" {
		t.Fatalf("expected higher-priority backlog item dispatched next, got %#v", got)
	}
}

func TestDirectQueuePriorityOrderingIsStableWithinClass(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{SessionID: "sess_000001", ConsumerID: "worker-1", TransportIdentity: []byte("cid1"), Capabilities: capabilityHints{SupportsAck: true, Resumable: true}, MaxInflight: 1, Subscriptions: map[string]struct{}{"orders.created": {}}}

	r.dispatchDirect("orders.created", "msg-1", []byte(`{"id":"msg-1"}`), "high")
	r.dispatchDirect("orders.created", "msg-2", []byte(`{"id":"msg-2"}`), "high")
	r.handleAck("msg-1", "worker-1", "sess_000001")

	if got := r.inflight["msg-2"]; got == nil || got.EnqueueSequence == 0 {
		t.Fatalf("expected second high-priority message replayable in order, got %#v", got)
	}
}

func TestPriorityBoostPreventsStarvationAcrossTopics(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.SetPriorityPolicy([]string{"urgent", "high", "normal", "low"}, map[string]int{"urgent": 4, "high": 3, "normal": 2, "low": 1}, true, 1, 10)
	r.directSessions["worker-1"] = &consumerSession{SessionID: "sess_000001", ConsumerID: "worker-1", TransportIdentity: []byte("cid1"), Capabilities: capabilityHints{SupportsAck: true, Resumable: true}, MaxInflight: 1, Subscriptions: map[string]struct{}{"orders.high": {}, "orders.low": {}}}

	r.dispatchDirect("orders.high", "msg-high-1", []byte(`{"id":"msg-high-1"}`), "high")
	r.dispatchDirect("orders.low", "msg-low-1", []byte(`{"id":"msg-low-1"}`), "low")
	r.dispatchDirect("orders.high", "msg-high-2", []byte(`{"id":"msg-high-2"}`), "high")
	r.handleAck("msg-high-1", "worker-1", "sess_000001")

	if got := r.inflight["msg-low-1"]; got == nil || got.Topic != "orders.low" {
		t.Fatalf("expected aged low-priority message to dispatch before later high-priority work, got %#v", got)
	}
}

func TestWALReplayPreservesPriorityMetadata(t *testing.T) {
	w := &stubWAL{replay: []walDispatchedEntry{{
		MessageID:       "msg-rp1",
		Consumer:        "worker-1",
		SessionID:       "sess_old",
		Topic:           "orders.created",
		Payload:         []byte(`{"id":"msg-rp1","priority":"urgent"}`),
		Priority:        "urgent",
		EnqueueSequence: 42,
		Attempt:         2,
	}}}
	r := NewRouterWithDurability("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor(), 3, time.Second, w)
	r.directSessions["worker-1"] = &consumerSession{SessionID: "sess_000001", ConsumerID: "worker-1", TransportIdentity: []byte("cid1"), Capabilities: capabilityHints{SupportsAck: true, Resumable: true}, MaxInflight: 2, Subscriptions: map[string]struct{}{"orders.created": {}}}

	r.replayFromWAL("worker-1")

	replayed := r.inflight["msg-rp1"]
	if replayed == nil || replayed.Priority != "urgent" || replayed.EnqueueSequence != 42 {
		t.Fatalf("expected replayed inflight priority metadata preserved, got %#v", replayed)
	}
}

func TestScheduledPayloadRetainsNormalizedPriority(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{SessionID: "sess_000001", ConsumerID: "worker-1", TransportIdentity: []byte("cid1"), Capabilities: capabilityHints{SupportsAck: true, Resumable: true}, MaxInflight: 1, Subscriptions: map[string]struct{}{"orders.created": {}}}
	now := time.Unix(1000, 0).UTC()
	r.now = func() time.Time { return now }

	event := domain.Event{ID: "msg-pri", Topic: "orders.created", Priority: "HIGH", Timestamp: now}
	payload, err := r.codec.Encode(event)
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	r.scheduleDispatch("orders.created", "msg-pri", payload, "high", 9, 1, now.Add(time.Second), "publish")
	now = now.Add(time.Second)
	r.promoteScheduledDue()

	var replayed domain.Event
	if err := json.Unmarshal(r.inflight["msg-pri"].Payload, &replayed); err != nil {
		t.Fatalf("decode replayed payload: %v", err)
	}
	if replayed.Priority != "HIGH" && r.inflight["msg-pri"].Priority != "high" {
		t.Fatalf("expected normalized inflight priority persisted, got payload=%q inflight=%q", replayed.Priority, r.inflight["msg-pri"].Priority)
	}
	if r.inflight["msg-pri"].Priority != "high" {
		t.Fatalf("expected scheduled priority preserved, got %#v", r.inflight["msg-pri"])
	}
}
