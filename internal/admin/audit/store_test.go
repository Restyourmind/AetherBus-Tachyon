package audit

import (
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
