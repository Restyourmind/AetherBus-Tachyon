package zmq

import (
	"testing"
	"time"

	"github.com/aetherbus/aetherbus-tachyon/internal/exporter"
	"github.com/aetherbus/aetherbus-tachyon/internal/media"
)

type captureExporter struct{ events []exporter.Event }

func (c *captureExporter) Emit(ev exporter.Event) { c.events = append(c.events, ev.Normalize()) }
func (c *captureExporter) Close() error           { return nil }
func (c *captureExporter) Stats() exporter.Stats  { return exporter.Stats{} }

func TestRouterEmitsLifecycleEvents(t *testing.T) {
	r := NewRouterWithOptions("", "", nil, media.NewJSONCodec(), media.NewNoopCompressor(), 2, 10*time.Millisecond)
	cap := &captureExporter{}
	r.SetExporter(cap)
	session := &consumerSession{SessionID: "sess_001", ConsumerID: "consumer-a", TransportIdentity: []byte("id"), Subscriptions: map[string]struct{}{"orders": {}}, Capabilities: capabilityHints{SupportsAck: true, Resumable: true}, MaxInflight: 4, Live: true}
	r.directSessions[session.ConsumerID] = session
	r.prepareDispatchLocked(session, "tenant-a", "orders", "msg-1", []byte("payload"), "high", 12, 1)
	r.handleAck("msg-1", "consumer-a", "sess_001")
	seen := map[string]bool{}
	for _, ev := range cap.events {
		seen[ev.Action] = true
	}
	for _, action := range []string{"dispatch", "ack"} {
		if !seen[action] {
			t.Fatalf("expected action %q in %+v", action, cap.events)
		}
	}
}
