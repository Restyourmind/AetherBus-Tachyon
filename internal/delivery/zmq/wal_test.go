package zmq

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/aetherbus/aetherbus-tachyon/internal/admin/audit"
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
	if err := w.AppendDeadLettered(DeadLetterRecord{MessageID: "msg-dlq", ConsumerID: "c1", Topic: "orders.created", Payload: []byte("p1"), DeadLetteredAt: time.Unix(300, 0).UTC(), Reason: "retry_exhausted"}); err != nil {
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

func TestFileWALDeadLetterBrowseReplayAndPurge(t *testing.T) {
	tmp := t.TempDir()
	w := NewFileWAL(filepath.Join(tmp, "delivery.wal"))
	now := time.Unix(500, 0).UTC()
	if err := w.AppendDeadLettered(DeadLetterRecord{MessageID: "msg-1", ConsumerID: "worker-1", Topic: "orders.created", Payload: []byte("p1"), DeadLetteredAt: now, Reason: "retry_exhausted"}); err != nil {
		t.Fatalf("append dead-lettered: %v", err)
	}
	if err := w.AppendDeadLettered(DeadLetterRecord{MessageID: "msg-2", ConsumerID: "worker-2", Topic: "orders.failed", Payload: []byte("p2"), DeadLetteredAt: now.Add(time.Second), Reason: "terminal_nack"}); err != nil {
		t.Fatalf("append dead-lettered: %v", err)
	}
	records, err := w.ListDeadLetters(DeadLetterFilter{ConsumerID: "worker-1"})
	if err != nil {
		t.Fatalf("list dead letters: %v", err)
	}
	if len(records) != 1 || records[0].MessageID != "msg-1" {
		t.Fatalf("unexpected filtered records: %#v", records)
	}
	res, err := w.ReplayDeadLetters(DeadLetterReplayRequest{MessageIDs: []string{"msg-1"}, TargetConsumerID: "worker-1", TargetTopic: "orders.created", Confirm: "REPLAY", RequestedAt: now.Add(2 * time.Second), AdminMutationMetadata: AdminMutationMetadata{Actor: "ops@example.com", Reason: "customer replay request"}})
	if err != nil {
		t.Fatalf("replay dead letters: %v", err)
	}
	if res.Replayed != 1 || res.Failed != 0 {
		t.Fatalf("unexpected replay result: %#v", res)
	}
	scheduled, err := w.LoadScheduled()
	if err != nil {
		t.Fatalf("load scheduled: %v", err)
	}
	if len(scheduled) != 1 || scheduled[0].MessageID != "msg-1" {
		t.Fatalf("unexpected replayed schedule: %#v", scheduled)
	}
	_, ok, err := w.GetDeadLetter("msg-1")
	if err != nil {
		t.Fatalf("get dead letter: %v", err)
	}
	if ok {
		t.Fatalf("expected replayed record removed from dlq")
	}
	purge, err := w.PurgeDeadLetters(DeadLetterPurgeRequest{MessageIDs: []string{"msg-2"}, Confirm: "PURGE", AdminMutationMetadata: AdminMutationMetadata{Actor: "ops@example.com", Reason: "retention cleanup"}})
	if err != nil {
		t.Fatalf("purge dead letters: %v", err)
	}
	if purge.Purged != 1 || purge.Failed != 0 {
		t.Fatalf("unexpected purge result: %#v", purge)
	}
}

func TestFileWALAdminAuditTrailRoundTrip(t *testing.T) {
	tmp := t.TempDir()
	w := NewFileWAL(filepath.Join(tmp, "delivery.wal"))
	now := time.Unix(700, 0).UTC()

	record, err := w.ManualDeadLetter(ManualDeadLetterRequest{Record: DeadLetterRecord{MessageID: "msg-audit", ConsumerID: "worker-9", Topic: "orders.audit", Payload: []byte("payload"), Reason: "manual_admin_action", DeadLetteredAt: now}, AdminMutationMetadata: AdminMutationMetadata{Actor: "admin@example.com", Reason: "legal hold"}})
	if err != nil {
		t.Fatalf("manual dead-letter: %v", err)
	}
	if record.MessageID != "msg-audit" {
		t.Fatalf("unexpected manual dead-letter record: %#v", record)
	}
	if _, err := w.ReplayDeadLetters(DeadLetterReplayRequest{MessageIDs: []string{"msg-audit"}, TargetConsumerID: "worker-9", TargetTopic: "orders.audit", Confirm: "REPLAY", RequestedAt: now.Add(time.Minute), AdminMutationMetadata: AdminMutationMetadata{Actor: "admin@example.com", Reason: "requeue after review"}}); err != nil {
		t.Fatalf("replay dead letters: %v", err)
	}
	events, err := w.ListAuditEvents(audit.Query{MessageID: "msg-audit", Actor: "admin@example.com", Start: now.Add(-time.Second), End: now.Add(2 * time.Minute)})
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 audit events, got %#v", events)
	}
	if events[0].Operation != audit.OperationManualDeadLetter || events[1].Operation != audit.OperationReplayDeadLetter {
		t.Fatalf("unexpected audit operations: %#v", events)
	}
	if events[1].PrevHash != events[0].Hash {
		t.Fatalf("expected chained hashes, got prev=%q hash=%q", events[1].PrevHash, events[0].Hash)
	}
}
