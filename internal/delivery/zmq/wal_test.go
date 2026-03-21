package zmq

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFileWALAppendReplayAndCommit(t *testing.T) {
	tmp := t.TempDir()
	w := NewFileWAL(filepath.Join(tmp, "delivery.wal"))

	if err := w.AppendDispatched(walDispatchedEntry{MessageID: "msg-1", Consumer: "c1", SessionID: "s1", Topic: "orders.created", Payload: []byte("p1"), Priority: "normal", EnqueueSequence: 1, Attempt: 1}); err != nil {
		t.Fatalf("append dispatched: %v", err)
	}
	if err := w.AppendDispatched(walDispatchedEntry{MessageID: "msg-2", Consumer: "c1", SessionID: "s1", Topic: "orders.created", Payload: []byte("p2"), Priority: "high", EnqueueSequence: 2, Attempt: 1}); err != nil {
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
	if entries[0].Priority != "high" || entries[0].EnqueueSequence != 2 {
		t.Fatalf("expected priority metadata preserved, got %#v", entries[0])
	}
}

func TestFileWALReplaySkipsDeadLettered(t *testing.T) {
	tmp := t.TempDir()
	w := NewFileWAL(filepath.Join(tmp, "delivery.wal"))

	if err := w.AppendDispatched(walDispatchedEntry{MessageID: "msg-dlq", Consumer: "c1", SessionID: "s1", Topic: "orders.created", Payload: []byte("p1"), Priority: "low", EnqueueSequence: 7, Attempt: 2}); err != nil {
		t.Fatalf("append dispatched: %v", err)
	}
	if err := w.AppendDeadLettered("msg-dlq"); err != nil {
		t.Fatalf("append dead-lettered: %v", err)
	}

	entries, err := w.ReplayUnacked()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no replay entries after dead-letter finalize, got %d", len(entries))
	}
}

func TestFileWALSessionSnapshotsRoundTrip(t *testing.T) {
	tmp := t.TempDir()
	w := NewFileWAL(filepath.Join(tmp, "delivery.wal"))

	if err := w.SaveSessionSnapshot(sessionSnapshot{ConsumerID: "worker-1", SessionID: "sess_1", LastHeartbeat: time.Unix(100, 0).UTC(), MaxInflight: 5, SupportsAck: true, SupportsCompression: []string{"lz4"}, SupportsCodec: []string{"json"}, Resumable: true}); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	snapshots, err := w.LoadSessionSnapshots()
	if err != nil {
		t.Fatalf("load snapshots: %v", err)
	}
	if len(snapshots) != 1 || snapshots[0].ConsumerID != "worker-1" {
		t.Fatalf("expected worker-1 snapshot, got %#v", snapshots)
	}
	if err := w.DeleteSessionSnapshot("worker-1"); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}
	snapshots, err = w.LoadSessionSnapshots()
	if err != nil {
		t.Fatalf("reload snapshots: %v", err)
	}
	if len(snapshots) != 0 {
		t.Fatalf("expected snapshots empty after delete, got %#v", snapshots)
	}
}

func TestFileWALScheduledRoundTrip(t *testing.T) {
	tmp := t.TempDir()
	w := NewFileWAL(filepath.Join(tmp, "delivery.wal"))
	entries := []scheduledMessage{{Sequence: 2, MessageID: "msg-2", Topic: "orders.created", Payload: []byte("later"), DeliverAt: time.Unix(200, 0).UTC()}, {Sequence: 1, MessageID: "msg-1", Topic: "orders.created", Payload: []byte("first"), DeliverAt: time.Unix(100, 0).UTC()}}
	if err := w.SaveScheduled(entries); err != nil {
		t.Fatalf("save scheduled: %v", err)
	}
	loaded, err := w.LoadScheduled()
	if err != nil {
		t.Fatalf("load scheduled: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected 2 scheduled entries, got %d", len(loaded))
	}
	if loaded[0].MessageID != "msg-1" || loaded[1].MessageID != "msg-2" {
		t.Fatalf("expected scheduled queue sorted by deliver_at then sequence, got %#v", loaded)
	}
}
