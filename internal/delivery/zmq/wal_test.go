package zmq

import (
	"path/filepath"
	"testing"
)

func TestFileWALAppendReplayAndCommit(t *testing.T) {
	tmp := t.TempDir()
	w := NewFileWAL(filepath.Join(tmp, "delivery.wal"))

	if err := w.AppendDispatched(walDispatchedEntry{MessageID: "msg-1", Consumer: "c1", SessionID: "s1", Topic: "orders.created", Payload: []byte("p1"), Attempt: 1}); err != nil {
		t.Fatalf("append dispatched: %v", err)
	}
	if err := w.AppendDispatched(walDispatchedEntry{MessageID: "msg-2", Consumer: "c1", SessionID: "s1", Topic: "orders.created", Payload: []byte("p2"), Attempt: 1}); err != nil {
		t.Fatalf("append dispatched: %v", err)
	}
	if err := w.AppendCommitted("msg-1"); err != nil {
		t.Fatalf("append committed: %v", err)
	}

	entries, err := w.ReplayUnacked()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one unacked entry, got %d", len(entries))
	}
	if entries[0].MessageID != "msg-2" {
		t.Fatalf("expected msg-2 replayed, got %s", entries[0].MessageID)
	}
}
