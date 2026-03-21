package statefile

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type VersionedEnvelope[T any] struct {
	Version int `json:"version"`
	Data    T   `json:"data"`
}

type FileStateStore[T any] struct {
	path             string
	version          int
	empty            func() T
	allowUnversioned bool
}

func NewFileStateStore[T any](path string, version int, empty func() T) *FileStateStore[T] {
	return &FileStateStore[T]{path: path, version: version, empty: empty, allowUnversioned: true}
}

func (s *FileStateStore[T]) Path() string { return s.path }
func (s *FileStateStore[T]) Version() int { return s.version }

func (s *FileStateStore[T]) Load() (VersionedEnvelope[T], error) {
	zero := s.zero()
	if s == nil || s.path == "" {
		return VersionedEnvelope[T]{Version: s.versionOrZero(), Data: zero}, nil
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return VersionedEnvelope[T]{Version: s.version, Data: zero}, os.ErrNotExist
		}
		return VersionedEnvelope[T]{}, fmt.Errorf("read %s: %w", s.path, err)
	}
	if len(data) == 0 {
		return VersionedEnvelope[T]{Version: s.version, Data: zero}, nil
	}
	var env VersionedEnvelope[T]
	if err := json.Unmarshal(data, &env); err == nil && env.Version != 0 {
		return env, nil
	}
	if !s.allowUnversioned {
		return VersionedEnvelope[T]{}, fmt.Errorf("decode %s: missing version envelope", s.path)
	}
	var legacy T
	if err := json.Unmarshal(data, &legacy); err != nil {
		return VersionedEnvelope[T]{}, fmt.Errorf("decode %s: %w", s.path, err)
	}
	return VersionedEnvelope[T]{Version: s.version, Data: legacy}, nil
}

func (s *FileStateStore[T]) Save(data T) error {
	if s == nil || s.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create state dir for %s: %w", s.path, err)
	}
	encoded, err := json.MarshalIndent(VersionedEnvelope[T]{Version: s.version, Data: data}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", s.path, err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, append(encoded, '\n'), 0o644); err != nil {
		return fmt.Errorf("write temp %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace %s: %w", s.path, err)
	}
	return nil
}

func (s *FileStateStore[T]) zero() T {
	if s != nil && s.empty != nil {
		return s.empty()
	}
	var zero T
	return zero
}

func (s *FileStateStore[T]) versionOrZero() int {
	if s == nil {
		return 0
	}
	return s.version
}
