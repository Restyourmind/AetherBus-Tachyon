package audit

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/aetherbus/aetherbus-tachyon/internal/statefile"
)

const headSchemaVersion = 1

type Store interface {
	Append(event Event) (Event, error)
	Query(filter Query) ([]Event, error)
}

type FileStore struct {
	mu        sync.Mutex
	path      string
	headStore *statefile.FileStateStore[string]
}

func NewFileStore(path string) Store {
	return &FileStore{path: path, headStore: statefile.NewFileStateStore(path+".head", headSchemaVersion, func() string { return "" })}
}

func (s *FileStore) Append(event Event) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prevHash, err := s.lastHashLocked()
	if err != nil {
		return Event{}, err
	}
	finalized, err := FinalizeEvent(event, prevHash)
	if err != nil {
		return Event{}, err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return Event{}, err
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return Event{}, err
	}
	defer f.Close()
	encoded, err := json.Marshal(finalized)
	if err != nil {
		return Event{}, err
	}
	if _, err := f.Write(append(encoded, '\n')); err != nil {
		return Event{}, err
	}
	if err := f.Sync(); err != nil {
		return Event{}, err
	}
	if err := s.writeHeadLocked(finalized.Hash); err != nil {
		return Event{}, err
	}
	return finalized, nil
}

func (s *FileStore) Query(filter Query) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []Event
	scanner := bufio.NewScanner(f)
	prevHash := ""
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return nil, fmt.Errorf("decode audit event: %w", err)
		}
		if event.PrevHash != prevHash {
			return nil, fmt.Errorf("audit hash chain mismatch for %s", event.EventID)
		}
		finalized, err := FinalizeEvent(Event{EventID: event.EventID, Actor: event.Actor, Timestamp: event.Timestamp, Operation: event.Operation, TargetMessageIDs: event.TargetMessageIDs, RequestedReason: event.RequestedReason, PriorState: event.PriorState, ResultingState: event.ResultingState}, event.PrevHash)
		if err != nil {
			return nil, err
		}
		if finalized.Hash != event.Hash {
			return nil, fmt.Errorf("audit hash mismatch for %s", event.EventID)
		}
		prevHash = event.Hash
		if filter.Actor != "" && event.Actor != filter.Actor {
			continue
		}
		if !filter.Start.IsZero() && event.Timestamp.Before(filter.Start) {
			continue
		}
		if !filter.End.IsZero() && event.Timestamp.After(filter.End) {
			continue
		}
		if filter.MessageID != "" && !containsMessageID(event, filter.MessageID) {
			continue
		}
		out = append(out, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Timestamp.Equal(out[j].Timestamp) {
			return out[i].EventID < out[j].EventID
		}
		return out[i].Timestamp.Before(out[j].Timestamp)
	})
	return out, nil
}

func containsMessageID(event Event, id string) bool {
	for _, candidate := range event.TargetMessageIDs {
		if candidate == id {
			return true
		}
	}
	for _, state := range event.PriorState {
		if state.MessageID == id {
			return true
		}
	}
	for _, state := range event.ResultingState {
		if state.MessageID == id {
			return true
		}
	}
	return false
}

func (s *FileStore) headPath() string { return s.path + ".head" }

func (s *FileStore) lastHashLocked() (string, error) {
	envelope, err := s.headStore.Load()
	if err == nil {
		return strings.TrimSpace(envelope.Data), nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return s.rebuildHeadLocked()
}

func (s *FileStore) rebuildHeadLocked() (string, error) {
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	defer f.Close()
	var last string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return "", fmt.Errorf("decode audit event: %w", err)
		}
		last = event.Hash
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	if err := s.writeHeadLocked(last); err != nil {
		return "", err
	}
	return last, nil
}

func (s *FileStore) writeHeadLocked(hash string) error {
	return s.headStore.Save(strings.TrimSpace(hash))
}
