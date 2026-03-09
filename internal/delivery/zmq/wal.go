package zmq

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type WAL interface {
	AppendDispatched(entry walDispatchedEntry) error
	AppendCommitted(messageID string) error
	ReplayUnacked() ([]walDispatchedEntry, error)
}

type fileWAL struct {
	mu   sync.Mutex
	path string
}

type walRecord struct {
	Type      string `json:"type"`
	MessageID string `json:"message_id"`
	Consumer  string `json:"consumer_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	Topic     string `json:"topic,omitempty"`
	Payload   string `json:"payload,omitempty"`
	Attempt   int    `json:"attempt,omitempty"`
}

type walDispatchedEntry struct {
	MessageID string
	Consumer  string
	SessionID string
	Topic     string
	Payload   []byte
	Attempt   int
}

func NewFileWAL(path string) WAL {
	return &fileWAL{path: path}
}

func (w *fileWAL) AppendDispatched(entry walDispatchedEntry) error {
	rec := walRecord{
		Type:      "dispatched",
		MessageID: entry.MessageID,
		Consumer:  entry.Consumer,
		SessionID: entry.SessionID,
		Topic:     entry.Topic,
		Payload:   base64.StdEncoding.EncodeToString(entry.Payload),
		Attempt:   entry.Attempt,
	}
	return w.appendRecord(rec)
}

func (w *fileWAL) AppendCommitted(messageID string) error {
	rec := walRecord{
		Type:      "committed",
		MessageID: messageID,
	}
	return w.appendRecord(rec)
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
	committed := map[string]struct{}{}
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
		case "committed":
			committed[rec.MessageID] = struct{}{}
			delete(pending, rec.MessageID)
		case "dispatched":
			if rec.MessageID == "" {
				continue
			}
			if _, ok := committed[rec.MessageID]; ok {
				continue
			}
			payload, err := base64.StdEncoding.DecodeString(rec.Payload)
			if err != nil {
				return nil, fmt.Errorf("decode wal payload: %w", err)
			}
			pending[rec.MessageID] = walDispatchedEntry{
				MessageID: rec.MessageID,
				Consumer:  rec.Consumer,
				SessionID: rec.SessionID,
				Topic:     rec.Topic,
				Payload:   payload,
				Attempt:   rec.Attempt,
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	out := make([]walDispatchedEntry, 0, len(pending))
	for _, entry := range pending {
		out = append(out, entry)
	}
	return out, nil
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
