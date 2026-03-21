package audit

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStoreAppendAndQuery(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "delivery.audit"))
	first, err := store.Append(Event{Actor: "alice", Timestamp: time.Unix(100, 0).UTC(), Operation: OperationManualDeadLetter, TargetMessageIDs: []string{"msg-1"}, RequestedReason: "manual quarantine", ResultingState: []MessageState{{MessageID: "msg-1", Location: "dead_letter_store"}}})
	if err != nil {
		t.Fatalf("append first audit event: %v", err)
	}
	second, err := store.Append(Event{Actor: "alice", Timestamp: time.Unix(101, 0).UTC(), Operation: OperationReplayDeadLetter, TargetMessageIDs: []string{"msg-1"}, RequestedReason: "approved replay", PriorState: []MessageState{{MessageID: "msg-1", Location: "dead_letter_store"}}, ResultingState: []MessageState{{MessageID: "msg-1", Location: "scheduled_replay"}}})
	if err != nil {
		t.Fatalf("append second audit event: %v", err)
	}
	if second.PrevHash != first.Hash {
		t.Fatalf("expected second event to chain to first: %#v %#v", first, second)
	}
	results, err := store.Query(Query{MessageID: "msg-1", Actor: "alice", Start: time.Unix(99, 0).UTC(), End: time.Unix(102, 0).UTC()})
	if err != nil {
		t.Fatalf("query audit store: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 audit events, got %#v", results)
	}
}

func TestFileStoreRestartUsesHeadSidecar(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delivery.audit")
	store := NewFileStore(path)
	first, err := store.Append(Event{Actor: "alice", Timestamp: time.Unix(100, 0).UTC(), Operation: OperationManualDeadLetter, TargetMessageIDs: []string{"msg-1"}})
	if err != nil {
		t.Fatalf("append first: %v", err)
	}
	secondStore := NewFileStore(path)
	second, err := secondStore.Append(Event{Actor: "bob", Timestamp: time.Unix(101, 0).UTC(), Operation: OperationReplayDeadLetter, TargetMessageIDs: []string{"msg-1"}})
	if err != nil {
		t.Fatalf("append second: %v", err)
	}
	if second.PrevHash != first.Hash {
		t.Fatalf("expected restart append to chain via sidecar, got prev=%q want=%q", second.PrevHash, first.Hash)
	}
}

func TestFileStoreQueryDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delivery.audit")
	store := NewFileStore(path)
	if _, err := store.Append(Event{Actor: "alice", Timestamp: time.Unix(100, 0).UTC(), Operation: OperationManualDeadLetter, TargetMessageIDs: []string{"msg-1"}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := os.WriteFile(path, []byte("{\"event_id\":\"bad\",\"actor\":\"mallory\",\"timestamp\":\"1970-01-01T00:01:41Z\",\"operation\":\"manual_dead_letter\",\"target_message_ids\":[\"msg-1\"],\"prev_hash\":\"tampered\",\"hash\":\"tampered\"}\n"), 0o644); err != nil {
		t.Fatalf("tamper file: %v", err)
	}
	if _, err := store.Query(Query{}); err == nil {
		t.Fatal("expected corruption detection error")
	}
}
