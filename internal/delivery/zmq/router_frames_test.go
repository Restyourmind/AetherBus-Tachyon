package zmq

import (
	"testing"

	"github.com/aetherbus/aetherbus-tachyon/internal/media"
)

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
		SessionID:      "sess_000001",
		ConsumerID:     "worker-1",
		SocketIdentity: []byte("cid1"),
		SupportsAck:    true,
		MaxInflight:    10,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	r.dispatchDirect("orders.created", "msg-1", []byte(`{"id":"msg-1"}`))
	if got := r.metrics.Dispatched; got != 1 {
		t.Fatalf("expected dispatched=1, got %d", got)
	}

	r.handleAck("msg-1", "worker-1")
	if got := r.metrics.Acked; got != 1 {
		t.Fatalf("expected acked=1, got %d", got)
	}

	r.handleAck("msg-1", "worker-1") // duplicate ACK
	if got := r.metrics.Acked; got != 1 {
		t.Fatalf("expected duplicate ACK to be harmless, got acked=%d", got)
	}

	r.handleAck("missing", "worker-1") // stale ACK
	if got := r.metrics.Acked; got != 1 {
		t.Fatalf("expected stale ACK to be ignored, got acked=%d", got)
	}
}

func TestRetryableNackRetried(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:      "sess_000001",
		ConsumerID:     "worker-1",
		SocketIdentity: []byte("cid1"),
		SupportsAck:    true,
		MaxInflight:    10,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	sendCount := 0
	r.directSender = func(identity []byte, topic string, payload []byte) error {
		sendCount++
		return nil
	}

	r.dispatchDirect("orders.created", "msg-2", []byte(`{"id":"msg-2"}`))
	r.handleNack("msg-2", "worker-1", "retryable_error")

	if got := r.metrics.Nacked; got != 1 {
		t.Fatalf("expected nacked=1, got %d", got)
	}
	if got := r.metrics.Retried; got != 1 {
		t.Fatalf("expected retried=1, got %d", got)
	}
	if got := r.metrics.DeadLettered; got != 0 {
		t.Fatalf("expected deadlettered=0, got %d", got)
	}
	if got := r.metrics.Dispatched; got != 2 {
		t.Fatalf("expected dispatched=2 (initial+retry), got %d", got)
	}
	if sendCount != 2 {
		t.Fatalf("expected direct sender called twice, got %d", sendCount)
	}
}

func TestTerminalNackDeadLettered(t *testing.T) {
	r := NewRouter("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor())
	r.directSessions["worker-1"] = &consumerSession{
		SessionID:      "sess_000001",
		ConsumerID:     "worker-1",
		SocketIdentity: []byte("cid1"),
		SupportsAck:    true,
		MaxInflight:    10,
		Subscriptions: map[string]struct{}{
			"orders.created": {},
		},
	}

	r.dispatchDirect("orders.created", "msg-3", []byte(`{"id":"msg-3"}`))
	r.handleNack("msg-3", "worker-1", "terminal_error")

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
