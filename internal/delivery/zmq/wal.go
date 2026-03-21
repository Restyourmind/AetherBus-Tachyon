package zmq

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aetherbus/aetherbus-tachyon/internal/admin/audit"
)

type WAL interface {
	AppendDispatched(entry walDispatchedEntry) error
	AppendCommitted(messageID string) error
	AppendDeadLettered(record DeadLetterRecord) error
	ReplayUnacked() ([]walDispatchedEntry, error)
	SaveSessionSnapshot(snapshot sessionSnapshot) error
	LoadSessionSnapshots() ([]sessionSnapshot, error)
	DeleteSessionSnapshot(consumerID string) error
	SaveScheduled(entries []scheduledMessage) error
	LoadScheduled() ([]scheduledMessage, error)
	ListDeadLetters(filter DeadLetterFilter) ([]DeadLetterRecord, error)
	GetDeadLetter(messageID string) (DeadLetterRecord, bool, error)
	ReplayDeadLetters(req DeadLetterReplayRequest) (DeadLetterReplayResult, error)
	PurgeDeadLetters(req DeadLetterPurgeRequest) (DeadLetterPurgeResult, error)
	ManualDeadLetter(req ManualDeadLetterRequest) (DeadLetterRecord, error)
	ListAuditEvents(filter audit.Query) ([]audit.Event, error)
	AppendAuditEvent(event audit.Event) (audit.Event, error)
}

type fileWAL struct {
	mu         sync.Mutex
	path       string
	auditStore audit.Store
}

type walRecord struct {
	Type            string `json:"type"`
	MessageID       string `json:"message_id"`
	Consumer        string `json:"consumer_id,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
	TenantID        string `json:"tenant_id,omitempty"`
	Topic           string `json:"topic,omitempty"`
	Payload         string `json:"payload,omitempty"`
	Priority        string `json:"priority,omitempty"`
	EnqueueSequence uint64 `json:"enqueue_sequence,omitempty"`
	Attempt         int    `json:"attempt,omitempty"`
}

type walDispatchedEntry struct {
	MessageID       string
	Consumer        string
	SessionID       string
	TenantID        string
	Topic           string
	Payload         []byte
	Priority        string
	EnqueueSequence uint64
	Attempt         int
}

type DeadLetterRecord struct {
	MessageID        string    `json:"message_id"`
	ConsumerID       string    `json:"consumer_id,omitempty"`
	SessionID        string    `json:"session_id,omitempty"`
	TenantID         string    `json:"tenant_id,omitempty"`
	Topic            string    `json:"topic"`
	Payload          []byte    `json:"payload"`
	Priority         string    `json:"priority,omitempty"`
	EnqueueSequence  uint64    `json:"enqueue_sequence,omitempty"`
	Attempt          int       `json:"attempt,omitempty"`
	Reason           string    `json:"reason,omitempty"`
	DeadLetteredAt   time.Time `json:"dead_lettered_at"`
	ReplayCount      int       `json:"replay_count,omitempty"`
	LastReplayAt     time.Time `json:"last_replay_at,omitempty"`
	LastReplayTarget string    `json:"last_replay_target,omitempty"`
}

type DeadLetterFilter struct {
	MessageID  string
	ConsumerID string
	Topic      string
	Reason     string
}

type DeadLetterReplayRequest struct {
	MessageIDs       []string
	TargetConsumerID string
	TargetTopic      string
	Confirm          string
	RequestedAt      time.Time
	AdminMutationMetadata
}

type AdminMutationMetadata struct {
	Actor  string
	Reason string
}

type ManualDeadLetterRequest struct {
	Record DeadLetterRecord
	AdminMutationMetadata
}

type DeadLetterReplayFailure struct {
	MessageID string `json:"message_id"`
	Error     string `json:"error"`
}

type DeadLetterReplayResult struct {
	Requested int                       `json:"requested"`
	Replayed  int                       `json:"replayed"`
	Failed    int                       `json:"failed"`
	Failures  []DeadLetterReplayFailure `json:"failures,omitempty"`
}

type DeadLetterPurgeRequest struct {
	MessageIDs []string
	Filter     DeadLetterFilter
	Confirm    string
	AdminMutationMetadata
}

type DeadLetterPurgeResult struct {
	Requested int                       `json:"requested"`
	Purged    int                       `json:"purged"`
	Failed    int                       `json:"failed"`
	Failures  []DeadLetterReplayFailure `json:"failures,omitempty"`
}

func NewFileWAL(path string) WAL {
	return &fileWAL{path: path, auditStore: audit.NewFileStore(path + ".audit")}
}

func (w *fileWAL) AppendDispatched(entry walDispatchedEntry) error {
	return w.appendRecord(walRecord{Type: "dispatched", MessageID: entry.MessageID, Consumer: entry.Consumer, SessionID: entry.SessionID, TenantID: entry.TenantID, Topic: entry.Topic, Payload: base64.StdEncoding.EncodeToString(entry.Payload), Priority: entry.Priority, EnqueueSequence: entry.EnqueueSequence, Attempt: entry.Attempt})
}
func (w *fileWAL) AppendCommitted(messageID string) error {
	return w.appendRecord(walRecord{Type: "committed", MessageID: messageID})
}
func (w *fileWAL) AppendDeadLettered(record DeadLetterRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	store, err := w.loadDeadLettersLocked()
	if err != nil {
		return err
	}
	if record.MessageID == "" {
		return errors.New("dead-letter record requires message_id")
	}
	if record.DeadLetteredAt.IsZero() {
		record.DeadLetteredAt = time.Now().UTC()
	}
	store[record.MessageID] = record
	return w.writeDeadLettersLocked(store)
}

func (w *fileWAL) ReplayUnacked() ([]walDispatchedEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f, err := os.Open(w.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	finalized := map[string]struct{}{}
	pending := map[string]walDispatchedEntry{}
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("decode wal line: %w", err)
		}
		switch rec.Type {
		case "committed", "dead_lettered":
			finalized[rec.MessageID] = struct{}{}
			delete(pending, rec.MessageID)
		case "dispatched":
			if rec.MessageID == "" {
				continue
			}
			if _, ok := finalized[rec.MessageID]; ok {
				continue
			}
			payload, err := base64.StdEncoding.DecodeString(rec.Payload)
			if err != nil {
				return nil, fmt.Errorf("decode wal payload: %w", err)
			}
			pending[rec.MessageID] = walDispatchedEntry{MessageID: rec.MessageID, Consumer: rec.Consumer, SessionID: rec.SessionID, TenantID: rec.TenantID, Topic: rec.Topic, Payload: payload, Priority: rec.Priority, EnqueueSequence: rec.EnqueueSequence, Attempt: rec.Attempt}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	out := make([]walDispatchedEntry, 0, len(pending))
	for _, entry := range pending {
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MessageID < out[j].MessageID })
	return out, nil
}

func (w *fileWAL) ListDeadLetters(filter DeadLetterFilter) ([]DeadLetterRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	store, err := w.loadDeadLettersLocked()
	if err != nil {
		return nil, err
	}
	out := make([]DeadLetterRecord, 0, len(store))
	for _, rec := range store {
		if filter.MessageID != "" && rec.MessageID != filter.MessageID {
			continue
		}
		if filter.ConsumerID != "" && rec.ConsumerID != filter.ConsumerID {
			continue
		}
		if filter.Topic != "" && rec.Topic != filter.Topic {
			continue
		}
		if filter.Reason != "" && rec.Reason != filter.Reason {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DeadLetteredAt.Equal(out[j].DeadLetteredAt) {
			return out[i].MessageID < out[j].MessageID
		}
		return out[i].DeadLetteredAt.Before(out[j].DeadLetteredAt)
	})
	return out, nil
}
func (w *fileWAL) GetDeadLetter(messageID string) (DeadLetterRecord, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	store, err := w.loadDeadLettersLocked()
	if err != nil {
		return DeadLetterRecord{}, false, err
	}
	rec, ok := store[messageID]
	return rec, ok, nil
}
func (w *fileWAL) ReplayDeadLetters(req DeadLetterReplayRequest) (DeadLetterReplayResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if req.Confirm != "REPLAY" {
		return DeadLetterReplayResult{}, errors.New("replay requires confirm=REPLAY")
	}
	if req.TargetConsumerID == "" || req.TargetTopic == "" {
		return DeadLetterReplayResult{}, errors.New("replay requires explicit target_consumer_id and target_topic")
	}
	store, err := w.loadDeadLettersLocked()
	if err != nil {
		return DeadLetterReplayResult{}, err
	}
	scheduled, err := w.loadScheduledLocked()
	if err != nil {
		return DeadLetterReplayResult{}, err
	}
	result := DeadLetterReplayResult{Requested: len(req.MessageIDs)}
	now := req.RequestedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	idSet := uniqueIDs(req.MessageIDs)
	priorStates := make([]audit.MessageState, 0, len(idSet))
	resultStates := make([]audit.MessageState, 0, len(idSet))
	var maxSeq uint64
	for _, entry := range scheduled {
		if entry.Sequence > maxSeq {
			maxSeq = entry.Sequence
		}
	}
	for _, id := range idSet {
		rec, ok := store[id]
		if !ok {
			result.Failed++
			result.Failures = append(result.Failures, DeadLetterReplayFailure{MessageID: id, Error: "record not found"})
			continue
		}
		if rec.ConsumerID != req.TargetConsumerID {
			result.Failed++
			result.Failures = append(result.Failures, DeadLetterReplayFailure{MessageID: id, Error: "target consumer does not match original consumer"})
			continue
		}
		if rec.Topic != req.TargetTopic {
			result.Failed++
			result.Failures = append(result.Failures, DeadLetterReplayFailure{MessageID: id, Error: "target topic does not match original topic"})
			continue
		}
		priorStates = append(priorStates, deadLetterAuditState(rec, "dead_letter_store"))
		maxSeq++
		scheduled = append(scheduled, scheduledMessage{Sequence: maxSeq, MessageID: rec.MessageID, TenantID: rec.TenantID, Topic: rec.Topic, Payload: append([]byte(nil), rec.Payload...), Priority: rec.Priority, EnqueueSequence: rec.EnqueueSequence, DeliveryAttempt: 1, DeliverAt: now, Reason: "dlq_replay"})
		rec.ReplayCount++
		rec.LastReplayAt = now
		rec.LastReplayTarget = req.TargetConsumerID + ":" + req.TargetTopic
		resultStates = append(resultStates, audit.MessageState{MessageID: rec.MessageID, Location: "scheduled_replay", ConsumerID: rec.ConsumerID, SessionID: rec.SessionID, Topic: rec.Topic, Priority: rec.Priority, EnqueueSequence: rec.EnqueueSequence, Attempt: 1, Reason: "dlq_replay", ReplayCount: rec.ReplayCount, LastReplayAt: rec.LastReplayAt, LastReplayTarget: rec.LastReplayTarget})
		delete(store, id)
		result.Replayed++
	}
	if err := w.writeScheduledLocked(scheduled); err != nil {
		return DeadLetterReplayResult{}, err
	}
	if err := w.writeDeadLettersLocked(store); err != nil {
		return DeadLetterReplayResult{}, err
	}
	if len(priorStates) > 0 {
		if _, err := w.appendAuditEventLocked(audit.Event{Actor: req.Actor, Timestamp: now, Operation: audit.OperationReplayDeadLetter, TargetMessageIDs: idSet, RequestedReason: req.Reason, PriorState: priorStates, ResultingState: resultStates}); err != nil {
			return DeadLetterReplayResult{}, err
		}
	}
	return result, nil
}
func (w *fileWAL) PurgeDeadLetters(req DeadLetterPurgeRequest) (DeadLetterPurgeResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if req.Confirm != "PURGE" {
		return DeadLetterPurgeResult{}, errors.New("purge requires confirm=PURGE")
	}
	store, err := w.loadDeadLettersLocked()
	if err != nil {
		return DeadLetterPurgeResult{}, err
	}
	result := DeadLetterPurgeResult{}
	var targetIDs []string
	priorStates := make([]audit.MessageState, 0)
	if len(req.MessageIDs) > 0 {
		ids := uniqueIDs(req.MessageIDs)
		result.Requested = len(ids)
		targetIDs = ids
		for _, id := range ids {
			rec, ok := store[id]
			if !ok {
				result.Failed++
				result.Failures = append(result.Failures, DeadLetterReplayFailure{MessageID: id, Error: "record not found"})
				continue
			}
			priorStates = append(priorStates, deadLetterAuditState(rec, "dead_letter_store"))
			delete(store, id)
			result.Purged++
		}
	} else {
		for id, rec := range store {
			if req.Filter.ConsumerID != "" && rec.ConsumerID != req.Filter.ConsumerID {
				continue
			}
			if req.Filter.Topic != "" && rec.Topic != req.Filter.Topic {
				continue
			}
			if req.Filter.Reason != "" && rec.Reason != req.Filter.Reason {
				continue
			}
			if req.Filter.MessageID != "" && rec.MessageID != req.Filter.MessageID {
				continue
			}
			result.Requested++
			targetIDs = append(targetIDs, id)
			priorStates = append(priorStates, deadLetterAuditState(rec, "dead_letter_store"))
			delete(store, id)
			result.Purged++
		}
	}
	if err := w.writeDeadLettersLocked(store); err != nil {
		return DeadLetterPurgeResult{}, err
	}
	if len(priorStates) > 0 {
		now := time.Now().UTC()
		resultStates := make([]audit.MessageState, 0, len(priorStates))
		for _, state := range priorStates {
			resultStates = append(resultStates, audit.MessageState{MessageID: state.MessageID, Location: "purged"})
		}
		if _, err := w.appendAuditEventLocked(audit.Event{Actor: req.Actor, Timestamp: now, Operation: audit.OperationPurgeDeadLetter, TargetMessageIDs: targetIDs, RequestedReason: req.Reason, PriorState: priorStates, ResultingState: resultStates}); err != nil {
			return DeadLetterPurgeResult{}, err
		}
	}
	return result, nil
}

func (w *fileWAL) ManualDeadLetter(req ManualDeadLetterRequest) (DeadLetterRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	record := req.Record
	if record.MessageID == "" {
		return DeadLetterRecord{}, errors.New("manual dead-letter requires message_id")
	}
	if record.DeadLetteredAt.IsZero() {
		record.DeadLetteredAt = time.Now().UTC()
	}
	store, err := w.loadDeadLettersLocked()
	if err != nil {
		return DeadLetterRecord{}, err
	}
	priorStates := make([]audit.MessageState, 0, 1)
	if existing, ok := store[record.MessageID]; ok {
		priorStates = append(priorStates, deadLetterAuditState(existing, "dead_letter_store"))
	}
	store[record.MessageID] = record
	if err := w.writeDeadLettersLocked(store); err != nil {
		return DeadLetterRecord{}, err
	}
	if _, err := w.appendAuditEventLocked(audit.Event{Actor: req.Actor, Timestamp: record.DeadLetteredAt, Operation: audit.OperationManualDeadLetter, TargetMessageIDs: []string{record.MessageID}, RequestedReason: req.Reason, PriorState: priorStates, ResultingState: []audit.MessageState{deadLetterAuditState(record, "dead_letter_store")}}); err != nil {
		return DeadLetterRecord{}, err
	}
	return record, nil
}

func (w *fileWAL) ListAuditEvents(filter audit.Query) ([]audit.Event, error) {
	return w.auditStore.Query(filter)
}

func (w *fileWAL) AppendAuditEvent(event audit.Event) (audit.Event, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendAuditEventLocked(event)
}

func (w *fileWAL) appendAuditEventLocked(event audit.Event) (audit.Event, error) {
	if w.auditStore == nil {
		return audit.Event{}, errors.New("audit store not configured")
	}
	return w.auditStore.Append(event)
}

func deadLetterAuditState(rec DeadLetterRecord, location string) audit.MessageState {
	return audit.MessageState{MessageID: rec.MessageID, Location: location, ConsumerID: rec.ConsumerID, SessionID: rec.SessionID, Topic: rec.Topic, Priority: rec.Priority, EnqueueSequence: rec.EnqueueSequence, Attempt: rec.Attempt, Reason: rec.Reason, DeadLetteredAt: rec.DeadLetteredAt, ReplayCount: rec.ReplayCount, LastReplayAt: rec.LastReplayAt, LastReplayTarget: rec.LastReplayTarget}
}

func (w *fileWAL) SaveSessionSnapshot(snapshot sessionSnapshot) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	snapshots, err := w.loadSnapshotsLocked()
	if err != nil {
		return err
	}
	snapshots[snapshot.ConsumerID] = snapshot
	return w.writeSnapshotsLocked(snapshots)
}
func (w *fileWAL) LoadSessionSnapshots() ([]sessionSnapshot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	snapshots, err := w.loadSnapshotsLocked()
	if err != nil {
		return nil, err
	}
	out := make([]sessionSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, snapshot)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ConsumerID < out[j].ConsumerID })
	return out, nil
}
func (w *fileWAL) DeleteSessionSnapshot(consumerID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	snapshots, err := w.loadSnapshotsLocked()
	if err != nil {
		return err
	}
	delete(snapshots, consumerID)
	return w.writeSnapshotsLocked(snapshots)
}
func (w *fileWAL) appendRecord(rec walRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(w.path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	encoded, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(encoded, '\n')); err != nil {
		return err
	}
	return f.Sync()
}
func (w *fileWAL) snapshotPath() string   { return w.path + ".sessions" }
func (w *fileWAL) scheduledPath() string  { return w.path + ".scheduled" }
func (w *fileWAL) deadLetterPath() string { return w.path + ".dlq" }
func (w *fileWAL) SaveScheduled(entries []scheduledMessage) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeScheduledLocked(entries)
}
func (w *fileWAL) LoadScheduled() ([]scheduledMessage, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.loadScheduledLocked()
}
func (w *fileWAL) loadScheduledLocked() ([]scheduledMessage, error) {
	data, err := os.ReadFile(w.scheduledPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var entries []scheduledMessage
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("decode scheduled queue: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].DeliverAt.Equal(entries[j].DeliverAt) {
			return entries[i].Sequence < entries[j].Sequence
		}
		return entries[i].DeliverAt.Before(entries[j].DeliverAt)
	})
	return entries, nil
}
func (w *fileWAL) writeScheduledLocked(entries []scheduledMessage) error {
	if err := os.MkdirAll(filepath.Dir(w.scheduledPath()), 0o755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	tmp := w.scheduledPath() + ".tmp"
	if err := os.WriteFile(tmp, append(encoded, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, w.scheduledPath())
}
func (w *fileWAL) loadSnapshotsLocked() (map[string]sessionSnapshot, error) {
	path := w.snapshotPath()
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]sessionSnapshot{}, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return map[string]sessionSnapshot{}, nil
	}
	var snapshots map[string]sessionSnapshot
	if err := json.Unmarshal(data, &snapshots); err != nil {
		return nil, fmt.Errorf("decode session snapshots: %w", err)
	}
	if snapshots == nil {
		snapshots = map[string]sessionSnapshot{}
	}
	return snapshots, nil
}
func (w *fileWAL) writeSnapshotsLocked(snapshots map[string]sessionSnapshot) error {
	if err := os.MkdirAll(filepath.Dir(w.snapshotPath()), 0o755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(snapshots, "", "  ")
	if err != nil {
		return err
	}
	tmp := w.snapshotPath() + ".tmp"
	if err := os.WriteFile(tmp, append(encoded, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, w.snapshotPath())
}
func (w *fileWAL) loadDeadLettersLocked() (map[string]DeadLetterRecord, error) {
	data, err := os.ReadFile(w.deadLetterPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]DeadLetterRecord{}, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return map[string]DeadLetterRecord{}, nil
	}
	var recs map[string]DeadLetterRecord
	if err := json.Unmarshal(data, &recs); err != nil {
		return nil, fmt.Errorf("decode dead-letter store: %w", err)
	}
	if recs == nil {
		recs = map[string]DeadLetterRecord{}
	}
	return recs, nil
}
func (w *fileWAL) writeDeadLettersLocked(recs map[string]DeadLetterRecord) error {
	if err := os.MkdirAll(filepath.Dir(w.deadLetterPath()), 0o755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(recs, "", "  ")
	if err != nil {
		return err
	}
	tmp := w.deadLetterPath() + ".tmp"
	if err := os.WriteFile(tmp, append(encoded, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, w.deadLetterPath())
}
func uniqueIDs(ids []string) []string {
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
