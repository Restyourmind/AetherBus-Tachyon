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
	"sync"
)

type WAL interface {
	AppendDispatched(entry walDispatchedEntry) error
	AppendCommitted(messageID string) error
	AppendDeadLettered(messageID string) error
	ReplayUnacked() ([]walDispatchedEntry, error)
	SaveSessionSnapshot(snapshot sessionSnapshot) error
	LoadSessionSnapshots() ([]sessionSnapshot, error)
	DeleteSessionSnapshot(consumerID string) error
	SaveScheduled(entries []scheduledMessage) error
	LoadScheduled() ([]scheduledMessage, error)
}

type fileWAL struct {
	mu   sync.Mutex
	path string
}

type walRecord struct {
	Type            string `json:"type"`
	MessageID       string `json:"message_id"`
	Consumer        string `json:"consumer_id,omitempty"`
	SessionID       string `json:"session_id,omitempty"`
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
	Topic           string
	Payload         []byte
	Priority        string
	EnqueueSequence uint64
	Attempt         int
}

func NewFileWAL(path string) WAL { return &fileWAL{path: path} }

func (w *fileWAL) AppendDispatched(entry walDispatchedEntry) error {
	return w.appendRecord(walRecord{Type: "dispatched", MessageID: entry.MessageID, Consumer: entry.Consumer, SessionID: entry.SessionID, Topic: entry.Topic, Payload: base64.StdEncoding.EncodeToString(entry.Payload), Priority: entry.Priority, EnqueueSequence: entry.EnqueueSequence, Attempt: entry.Attempt})
}
func (w *fileWAL) AppendCommitted(messageID string) error {
	return w.appendRecord(walRecord{Type: "committed", MessageID: messageID})
}
func (w *fileWAL) AppendDeadLettered(messageID string) error {
	return w.appendRecord(walRecord{Type: "dead_lettered", MessageID: messageID})
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
			pending[rec.MessageID] = walDispatchedEntry{MessageID: rec.MessageID, Consumer: rec.Consumer, SessionID: rec.SessionID, Topic: rec.Topic, Payload: payload, Priority: rec.Priority, EnqueueSequence: rec.EnqueueSequence, Attempt: rec.Attempt}
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

func (w *fileWAL) snapshotPath() string  { return w.path + ".sessions" }
func (w *fileWAL) scheduledPath() string { return w.path + ".scheduled" }

func (w *fileWAL) SaveScheduled(entries []scheduledMessage) error {
	w.mu.Lock()
	defer w.mu.Unlock()
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

func (w *fileWAL) LoadScheduled() ([]scheduledMessage, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
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
