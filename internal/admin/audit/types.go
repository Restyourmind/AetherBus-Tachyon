package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Operation string

const (
	OperationReplayDeadLetter Operation = "replay_dead_letter"
	OperationPurgeDeadLetter  Operation = "purge_dead_letter"
	OperationManualDeadLetter Operation = "manual_dead_letter"
)

type MessageState struct {
	MessageID        string    `json:"message_id"`
	Location         string    `json:"location"`
	ConsumerID       string    `json:"consumer_id,omitempty"`
	SessionID        string    `json:"session_id,omitempty"`
	Topic            string    `json:"topic,omitempty"`
	Priority         string    `json:"priority,omitempty"`
	EnqueueSequence  uint64    `json:"enqueue_sequence,omitempty"`
	Attempt          int       `json:"attempt,omitempty"`
	Reason           string    `json:"reason,omitempty"`
	DeadLetteredAt   time.Time `json:"dead_lettered_at,omitempty"`
	ReplayCount      int       `json:"replay_count,omitempty"`
	LastReplayAt     time.Time `json:"last_replay_at,omitempty"`
	LastReplayTarget string    `json:"last_replay_target,omitempty"`
}

type Event struct {
	EventID          string         `json:"event_id"`
	Actor            string         `json:"actor"`
	Timestamp        time.Time      `json:"timestamp"`
	Operation        Operation      `json:"operation"`
	TargetMessageIDs []string       `json:"target_message_ids"`
	RequestedReason  string         `json:"requested_reason,omitempty"`
	PriorState       []MessageState `json:"prior_state,omitempty"`
	ResultingState   []MessageState `json:"resulting_state,omitempty"`
	PrevHash         string         `json:"prev_hash,omitempty"`
	Hash             string         `json:"hash"`
}

type Query struct {
	MessageID string
	Actor     string
	Start     time.Time
	End       time.Time
}

func NormalizeMessageIDs(ids []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func FinalizeEvent(event Event, prevHash string) (Event, error) {
	event.TargetMessageIDs = NormalizeMessageIDs(event.TargetMessageIDs)
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	} else {
		event.Timestamp = event.Timestamp.UTC()
	}
	if event.Actor == "" {
		event.Actor = "unknown"
	}
	event.PrevHash = prevHash
	canonical, err := canonicalDigestPayload(event)
	if err != nil {
		return Event{}, err
	}
	sum := sha256.Sum256(canonical)
	event.Hash = hex.EncodeToString(sum[:])
	if event.EventID == "" {
		event.EventID = fmt.Sprintf("audit_%s", event.Hash[:16])
	}
	return event, nil
}

func canonicalDigestPayload(event Event) ([]byte, error) {
	payload := struct {
		Actor            string         `json:"actor"`
		Timestamp        time.Time      `json:"timestamp"`
		Operation        Operation      `json:"operation"`
		TargetMessageIDs []string       `json:"target_message_ids"`
		RequestedReason  string         `json:"requested_reason,omitempty"`
		PriorState       []MessageState `json:"prior_state,omitempty"`
		ResultingState   []MessageState `json:"resulting_state,omitempty"`
		PrevHash         string         `json:"prev_hash,omitempty"`
	}{event.Actor, event.Timestamp, event.Operation, event.TargetMessageIDs, event.RequestedReason, event.PriorState, event.ResultingState, event.PrevHash}
	return json.Marshal(payload)
}
