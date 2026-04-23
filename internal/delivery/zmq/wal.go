package zmq

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aetherbus/aetherbus-tachyon/internal/admin/audit"
	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/aetherbus/aetherbus-tachyon/internal/statefile"
)

type WAL interface {
	AppendDispatched(entry walDispatchedEntry) error
	AppendCommitted(messageID string) error
	AppendDeadLettered(record DeadLetterRecord) error
	ReplayUnacked() ([]walDispatchedEntry, error)
	SaveSessionSnapshot(snapshot sessionSnapshot) error
	LoadSessionSnapshots() ([]sessionSnapshot, error)
	DeleteSessionSnapshot(tenantID, consumerID string) error
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

type DurabilityMode string

const (
	DurabilityLocalFsync         DurabilityMode = "local_fsync"
	DurabilityReplicated         DurabilityMode = "replicated"
	DurabilityLocalAndReplicated DurabilityMode = "local_and_replicated"

	defaultSegmentMaxBytes int64 = 4 * 1024 * 1024
)

type ReplicationSink interface {
	Replicate(ctx context.Context, replica SegmentReplica) error
}

type SegmentReplica struct {
	SegmentID   string
	Bytes       []byte
	Closed      bool
	GeneratedAt time.Time
}

type WALOptions struct {
	SegmentMaxBytes int64
	DurabilityMode  DurabilityMode
	ReplicationSink ReplicationSink
}

const (
	sessionSnapshotSchemaVersion = 2
	scheduledSchemaVersion       = 2
	deadLetterSchemaVersion      = 2
)

type fileWAL struct {
	mu             sync.Mutex
	path           string
	auditStore     audit.Store
	options        WALOptions
	segments       *segmentLog
	sessionStore   *statefile.FileStateStore[map[string]sessionSnapshot]
	scheduledStore *statefile.FileStateStore[[]scheduledMessage]
	dlqStore       *statefile.FileStateStore[map[string]DeadLetterRecord]
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

type segmentLog struct {
	root         string
	maxBytes     int64
	durability   DurabilityMode
	replication  ReplicationSink
	activeID     string
	activeFile   *os.File
	activeSize   int64
	nextSequence uint64
}

type segmentDescriptor struct {
	id     string
	path   string
	closed bool
}

type MirrorReplicationSink struct {
	Dir string
}

func NewMirrorReplicationSink(dir string) ReplicationSink {
	if strings.TrimSpace(dir) == "" {
		return nil
	}
	return &MirrorReplicationSink{Dir: dir}
}

func (m *MirrorReplicationSink) Replicate(_ context.Context, replica SegmentReplica) error {
	if m == nil || strings.TrimSpace(m.Dir) == "" {
		return nil
	}
	if err := os.MkdirAll(m.Dir, 0o755); err != nil {
		return err
	}
	target := filepath.Join(m.Dir, replica.SegmentID)
	if err := os.WriteFile(target+".tmp", replica.Bytes, 0o644); err != nil {
		return err
	}
	if err := os.Rename(target+".tmp", target); err != nil {
		return err
	}
	if replica.Closed {
		return os.WriteFile(target+".closed", []byte(replica.GeneratedAt.UTC().Format(time.RFC3339Nano)+"\n"), 0o644)
	}
	return nil
}

func NewFileWAL(path string) WAL {
	return NewFileWALWithOptions(path, WALOptions{})
}

func NewFileWALWithOptions(path string, opts WALOptions) WAL {
	if opts.SegmentMaxBytes <= 0 {
		opts.SegmentMaxBytes = defaultSegmentMaxBytes
	}
	if opts.DurabilityMode == "" {
		opts.DurabilityMode = DurabilityLocalFsync
	}
	w := &fileWAL{
		path:           path,
		auditStore:     audit.NewFileStore(path + ".audit"),
		options:        opts,
		sessionStore:   statefile.NewFileStateStore(path+".sessions", sessionSnapshotSchemaVersion, func() map[string]sessionSnapshot { return map[string]sessionSnapshot{} }),
		scheduledStore: statefile.NewFileStateStore(path+".scheduled", scheduledSchemaVersion, func() []scheduledMessage { return nil }),
		dlqStore:       statefile.NewFileStateStore(path+".dlq", deadLetterSchemaVersion, func() map[string]DeadLetterRecord { return map[string]DeadLetterRecord{} }),
	}
	w.segments = &segmentLog{root: w.segmentRoot(), maxBytes: opts.SegmentMaxBytes, durability: opts.DurabilityMode, replication: opts.ReplicationSink}
	return w
}

func (w *fileWAL) AppendDispatched(entry walDispatchedEntry) error {
	return w.appendRecord(walRecord{Type: "dispatched", MessageID: entry.MessageID, Consumer: entry.Consumer, SessionID: entry.SessionID, TenantID: entry.TenantID, Topic: entry.Topic, Payload: base64.StdEncoding.EncodeToString(entry.Payload), Priority: entry.Priority, EnqueueSequence: entry.EnqueueSequence, Attempt: entry.Attempt})
}
func (w *fileWAL) AppendCommitted(messageID string) error {
	return w.appendRecord(walRecord{Type: "committed", MessageID: messageID})
}
func (w *fileWAL) AppendDeadLettered(record DeadLetterRecord) error {
	if err := w.appendRecord(walRecord{Type: "dead_lettered", MessageID: record.MessageID}); err != nil {
		return err
	}
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
	records, err := w.segments.readAllLocked()
	if err != nil {
		return nil, err
	}
	finalized := map[string]struct{}{}
	pending := map[string]walDispatchedEntry{}
	for _, rec := range records {
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
				return nil, fmt.Errorf("decode wal payload for %s: %w", rec.MessageID, err)
			}
			pending[rec.MessageID] = walDispatchedEntry{MessageID: rec.MessageID, Consumer: rec.Consumer, SessionID: rec.SessionID, TenantID: rec.TenantID, Topic: rec.Topic, Payload: payload, Priority: rec.Priority, EnqueueSequence: rec.EnqueueSequence, Attempt: rec.Attempt}
		}
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
		scheduled = append(scheduled, scheduledMessage{Sequence: maxSeq, MessageID: rec.MessageID, TenantID: rec.TenantID, Topic: rec.Topic, DestinationID: rec.ConsumerID, RouteType: domain.RouteTypeDirect, Payload: append([]byte(nil), rec.Payload...), Priority: rec.Priority, EnqueueSequence: rec.EnqueueSequence, DeliveryAttempt: 1, DeliverAt: now, Reason: "dlq_replay"})
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
	snapshots[sessionSnapshotKey(snapshot.TenantID, snapshot.ConsumerID)] = snapshot
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
func (w *fileWAL) DeleteSessionSnapshot(tenantID, consumerID string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	snapshots, err := w.loadSnapshotsLocked()
	if err != nil {
		return err
	}
	for key, snapshot := range snapshots {
		if snapshot.TenantID == tenantID && snapshot.ConsumerID == consumerID {
			delete(snapshots, key)
		}
	}
	return w.writeSnapshotsLocked(snapshots)
}
func (w *fileWAL) appendRecord(rec walRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segments.appendLocked(rec)
}
func (w *fileWAL) snapshotPath() string   { return w.path + ".sessions" }
func (w *fileWAL) scheduledPath() string  { return w.path + ".scheduled" }
func (w *fileWAL) deadLetterPath() string { return w.path + ".dlq" }
func (w *fileWAL) segmentRoot() string    { return w.path + ".segments" }
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
	envelope, err := w.scheduledStore.Load()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	entries := append([]scheduledMessage(nil), envelope.Data...)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].DeliverAt.Equal(entries[j].DeliverAt) {
			return entries[i].Sequence < entries[j].Sequence
		}
		return entries[i].DeliverAt.Before(entries[j].DeliverAt)
	})
	for i := range entries {
		if entries[i].RouteType == "" {
			entries[i].RouteType = domain.RouteTypeDirect
		}
	}
	return entries, nil
}
func (w *fileWAL) writeScheduledLocked(entries []scheduledMessage) error {
	return w.scheduledStore.Save(entries)
}
func (w *fileWAL) loadSnapshotsLocked() (map[string]sessionSnapshot, error) {
	envelope, err := w.sessionStore.Load()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]sessionSnapshot{}, nil
		}
		return nil, err
	}
	snapshots := envelope.Data
	if snapshots == nil {
		snapshots = map[string]sessionSnapshot{}
	}
	return snapshots, nil
}
func (w *fileWAL) writeSnapshotsLocked(snapshots map[string]sessionSnapshot) error {
	if snapshots == nil {
		snapshots = map[string]sessionSnapshot{}
	}
	return w.sessionStore.Save(snapshots)
}
func (w *fileWAL) loadDeadLettersLocked() (map[string]DeadLetterRecord, error) {
	envelope, err := w.dlqStore.Load()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]DeadLetterRecord{}, nil
		}
		return nil, err
	}
	recs := envelope.Data
	if recs == nil {
		recs = map[string]DeadLetterRecord{}
	}
	return recs, nil
}
func (w *fileWAL) writeDeadLettersLocked(recs map[string]DeadLetterRecord) error {
	if recs == nil {
		recs = map[string]DeadLetterRecord{}
	}
	return w.dlqStore.Save(recs)
}

func (s *segmentLog) appendLocked(rec walRecord) error {
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return err
	}
	encoded, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	if err := s.ensureActiveLocked(); err != nil {
		return err
	}
	if s.activeSize > 0 && s.activeSize+int64(len(encoded)) > s.maxBytes {
		if err := s.rollLocked(); err != nil {
			return err
		}
		if err := s.ensureActiveLocked(); err != nil {
			return err
		}
	}
	if _, err := s.activeFile.Write(encoded); err != nil {
		return err
	}
	if err := s.activeFile.Sync(); err != nil {
		return err
	}
	s.activeSize += int64(len(encoded))
	return s.replicateLocked(false)
}

func (s *segmentLog) ensureActiveLocked() error {
	if s.activeFile != nil {
		return nil
	}
	files, err := s.listSegmentsLocked()
	if err != nil {
		return err
	}
	for i := len(files) - 1; i >= 0; i-- {
		if files[i].closed {
			continue
		}
		f, err := os.OpenFile(files[i].path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}
		s.activeID = files[i].id
		s.activeFile = f
		s.activeSize = info.Size()
		s.nextSequence = nextSequenceFromSegmentID(files[i].id) + 1
		return nil
	}
	return s.createActiveLocked()
}

func (s *segmentLog) createActiveLocked() error {
	id := formatSegmentID(s.nextSequence)
	if id == "" {
		id = formatSegmentID(1)
	}
	path := filepath.Join(s.root, id)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.activeID = id
	if next := nextSequenceFromSegmentID(id) + 1; next > s.nextSequence {
		s.nextSequence = next
	}
	s.activeFile = f
	s.activeSize = 0
	return nil
}

func (s *segmentLog) rollLocked() error {
	if s.activeFile == nil {
		return nil
	}
	if err := s.activeFile.Sync(); err != nil {
		return err
	}
	if err := s.replicateLocked(true); err != nil {
		return err
	}
	closedPath := filepath.Join(s.root, s.activeID+".closed")
	if err := os.WriteFile(closedPath, []byte(time.Now().UTC().Format(time.RFC3339Nano)+"\n"), 0o644); err != nil {
		return err
	}
	if err := s.activeFile.Close(); err != nil {
		return err
	}
	s.activeFile = nil
	s.activeID = ""
	s.activeSize = 0
	return nil
}

func (s *segmentLog) replicateLocked(closed bool) error {
	if s.replication == nil {
		if s.durability == DurabilityReplicated || s.durability == DurabilityLocalAndReplicated {
			return errors.New("replication durability requested without replication sink")
		}
		return nil
	}
	if s.durability != DurabilityReplicated && s.durability != DurabilityLocalAndReplicated && !closed {
		return nil
	}
	path := filepath.Join(s.root, s.activeID)
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return s.replication.Replicate(context.Background(), SegmentReplica{SegmentID: s.activeID, Bytes: data, Closed: closed, GeneratedAt: time.Now().UTC()})
}

func (s *segmentLog) readAllLocked() ([]walRecord, error) {
	segments, err := s.listSegmentsLocked()
	if err != nil {
		return nil, err
	}
	var out []walRecord
	for _, seg := range segments {
		records, err := readSegmentRecords(seg.path, seg.closed)
		if err != nil {
			corruptPath := seg.path + ".corrupt"
			_ = os.Rename(seg.path, corruptPath)
			if seg.closed {
				return nil, fmt.Errorf("corrupt closed wal segment %s moved to %s: %w", seg.path, corruptPath, err)
			}
			continue
		}
		out = append(out, records...)
	}
	return out, nil
}

func (s *segmentLog) listSegmentsLocked() ([]segmentDescriptor, error) {
	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	closedSet := map[string]struct{}{}
	segments := make([]segmentDescriptor, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".closed") {
			closedSet[strings.TrimSuffix(name, ".closed")] = struct{}{}
		}
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".wal") || strings.HasSuffix(name, ".corrupt") {
			continue
		}
		_, closed := closedSet[name]
		segments = append(segments, segmentDescriptor{id: name, path: filepath.Join(s.root, name), closed: closed})
		if seq := nextSequenceFromSegmentID(name) + 1; seq > s.nextSequence {
			s.nextSequence = seq
		}
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].id < segments[j].id })
	return segments, nil
}

func readSegmentRecords(path string, closed bool) ([]walRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	var out []walRecord
	for {
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) == 0 {
				return out, nil
			}
			if !closed {
				return out, nil
			}
			return nil, io.ErrUnexpectedEOF
		}
		if err != nil {
			return nil, err
		}
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}
		var rec walRecord
		if err := json.Unmarshal(trimmed, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
}

func formatSegmentID(seq uint64) string {
	if seq == 0 {
		seq = 1
	}
	return "segment-" + fmt.Sprintf("%020d", seq) + ".wal"
}

func nextSequenceFromSegmentID(id string) uint64 {
	base := strings.TrimSuffix(strings.TrimPrefix(id, "segment-"), ".wal")
	seq, _ := strconv.ParseUint(base, 10, 64)
	return seq
}

func sessionSnapshotKey(tenantID, consumerID string) string {
	return tenantID + "\x00" + consumerID
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
