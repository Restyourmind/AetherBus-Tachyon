package exporter

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Event struct {
	SchemaVersion string            `json:"schema_version"`
	EventID       string            `json:"event_id"`
	Kind          string            `json:"kind"`
	Action        string            `json:"action"`
	Source        string            `json:"source"`
	Cursor        string            `json:"cursor,omitempty"`
	OccurredAt    time.Time         `json:"occurred_at"`
	TenantID      string            `json:"tenant_id,omitempty"`
	MessageID     string            `json:"message_id,omitempty"`
	ConsumerID    string            `json:"consumer_id,omitempty"`
	SessionID     string            `json:"session_id,omitempty"`
	Topic         string            `json:"topic,omitempty"`
	RouteKey      string            `json:"route_key,omitempty"`
	DestinationID string            `json:"destination_id,omitempty"`
	Attempt       int               `json:"attempt,omitempty"`
	Status        string            `json:"status,omitempty"`
	Reason        string            `json:"reason,omitempty"`
	DeliverAt     time.Time         `json:"deliver_at,omitempty"`
	EnqueueSeq    uint64            `json:"enqueue_sequence,omitempty"`
	PayloadSize   int               `json:"payload_size,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
}

func (e Event) Normalize() Event {
	e.SchemaVersion = "v1"
	e.Kind = strings.TrimSpace(e.Kind)
	e.Action = strings.TrimSpace(e.Action)
	e.Source = strings.TrimSpace(e.Source)
	if e.OccurredAt.IsZero() {
		e.OccurredAt = time.Now().UTC()
	} else {
		e.OccurredAt = e.OccurredAt.UTC()
	}
	if !e.DeliverAt.IsZero() {
		e.DeliverAt = e.DeliverAt.UTC()
	}
	if e.EventID == "" {
		e.EventID = StableEventID(e)
	}
	return e
}

func StableEventID(e Event) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%s|%s|%s|%s|%s|%s|%s|%d|%s|%d|%s|%d", e.SchemaVersion, e.Kind, e.Action, e.Source, e.Cursor, e.TenantID, e.MessageID, e.ConsumerID, e.SessionID, e.Attempt, e.Status, e.EnqueueSeq, e.RouteKey, e.PayloadSize)
	return hex.EncodeToString(h.Sum(nil))
}

type Sink interface {
	WriteBatch(ctx context.Context, events []Event) error
	LoadCheckpoint(ctx context.Context, source string) (string, error)
	SaveCheckpoint(ctx context.Context, source, cursor string) error
	Close() error
}

type Exporter interface {
	Emit(Event)
	Close() error
	Stats() Stats
}

type Stats struct{ Emitted, Dropped, Flushed uint64 }

type AsyncExporter struct {
	sink        Sink
	ch          chan Event
	wg          sync.WaitGroup
	closeOnce   sync.Once
	stats       Stats
	checkpoints sync.Map
}

func NewAsyncExporter(sink Sink, buffer int) *AsyncExporter {
	if buffer <= 0 {
		buffer = 1024
	}
	ex := &AsyncExporter{sink: sink, ch: make(chan Event, buffer)}
	if sink != nil {
		if fileSink, ok := sink.(*FileSink); ok {
			for source, cursor := range PrimeCheckpoints(context.Background(), fileSink) {
				ex.checkpoints.Store(source, cursor)
			}
		}
	}
	ex.wg.Add(1)
	go ex.loop()
	return ex
}

func (e *AsyncExporter) Emit(ev Event) {
	if e == nil || e.sink == nil {
		return
	}
	ev = ev.Normalize()
	if ev.Cursor != "" {
		if last, ok := e.checkpoints.Load(ev.Source); ok && compareCursor(last.(string), ev.Cursor) >= 0 {
			return
		}
	}
	select {
	case e.ch <- ev:
		atomic.AddUint64(&e.stats.Emitted, 1)
	default:
		atomic.AddUint64(&e.stats.Dropped, 1)
	}
}

func (e *AsyncExporter) loop() {
	defer e.wg.Done()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	batch := make([]Event, 0, 64)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		_ = e.sink.WriteBatch(context.Background(), batch)
		latest := map[string]string{}
		for _, ev := range batch {
			if ev.Cursor != "" {
				latest[ev.Source] = ev.Cursor
			}
		}
		for source, cursor := range latest {
			_ = e.sink.SaveCheckpoint(context.Background(), source, cursor)
			e.checkpoints.Store(source, cursor)
		}
		atomic.AddUint64(&e.stats.Flushed, uint64(len(batch)))
		batch = batch[:0]
	}
	for {
		select {
		case ev, ok := <-e.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, ev)
			if len(batch) >= 64 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (e *AsyncExporter) Close() error {
	if e == nil {
		return nil
	}
	e.closeOnce.Do(func() {
		close(e.ch)
		e.wg.Wait()
	})
	if e.sink != nil {
		return e.sink.Close()
	}
	return nil
}
func (e *AsyncExporter) Stats() Stats {
	return Stats{Emitted: atomic.LoadUint64(&e.stats.Emitted), Dropped: atomic.LoadUint64(&e.stats.Dropped), Flushed: atomic.LoadUint64(&e.stats.Flushed)}
}

func compareCursor(a, b string) int { return strings.Compare(a, b) }

type FileSink struct {
	mu                   sync.Mutex
	path, checkpointPath string
	seen                 map[string]struct{}
	checkpoints          map[string]string
}

func NewFileSink(path string) (*FileSink, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("file sink path required")
	}
	fs := &FileSink{path: path, checkpointPath: path + ".checkpoints", seen: map[string]struct{}{}, checkpoints: map[string]string{}}
	if err := fs.load(); err != nil {
		return nil, err
	}
	return fs, nil
}

func (f *FileSink) load() error {
	if data, err := os.ReadFile(f.checkpointPath); err == nil && len(data) > 0 {
		_ = json.Unmarshal(data, &f.checkpoints)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.Open(f.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var ev Event
		if json.Unmarshal(scanner.Bytes(), &ev) == nil && ev.EventID != "" {
			f.seen[ev.EventID] = struct{}{}
		}
	}
	return scanner.Err()
}

func (f *FileSink) WriteBatch(_ context.Context, events []Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		return err
	}
	fh, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer fh.Close()
	enc := json.NewEncoder(fh)
	for _, ev := range events {
		if _, ok := f.seen[ev.EventID]; ok {
			continue
		}
		if err := enc.Encode(ev); err != nil {
			return err
		}
		f.seen[ev.EventID] = struct{}{}
	}
	return fh.Sync()
}
func (f *FileSink) LoadCheckpoint(_ context.Context, source string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checkpoints[source], nil
}
func (f *FileSink) SaveCheckpoint(_ context.Context, source, cursor string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if source == "" || cursor == "" {
		return nil
	}
	f.checkpoints[source] = cursor
	data, err := json.MarshalIndent(f.checkpoints, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(f.checkpointPath, append(data, '\n'), 0o644)
}
func (f *FileSink) Close() error { return nil }

type BatchFileSink struct{ *FileSink }

func NewBatchFileSink(path string) (*BatchFileSink, error) {
	fs, err := NewFileSink(path)
	if err != nil {
		return nil, err
	}
	return &BatchFileSink{FileSink: fs}, nil
}

type SQLSink struct{ DriverName, DSN, Table string }

func NewPostgresSink(dsn, table string) *SQLSink {
	return &SQLSink{DriverName: "postgres", DSN: dsn, Table: table}
}
func NewClickHouseSink(dsn, table string) *SQLSink {
	return &SQLSink{DriverName: "clickhouse", DSN: dsn, Table: table}
}
func (s *SQLSink) WriteBatch(context.Context, []Event) error {
	return errors.New("sql sink requires driver-specific implementation wiring")
}
func (s *SQLSink) LoadCheckpoint(context.Context, string) (string, error) { return "", nil }
func (s *SQLSink) SaveCheckpoint(context.Context, string, string) error   { return nil }
func (s *SQLSink) Close() error                                           { return nil }

func PrimeCheckpoints(ctx context.Context, sink Sink, sources ...string) map[string]string {
	out := map[string]string{}
	if fileSink, ok := sink.(*FileSink); ok && len(sources) == 0 {
		fileSink.mu.Lock()
		defer fileSink.mu.Unlock()
		for source, cursor := range fileSink.checkpoints {
			out[source] = cursor
		}
		return out
	}
	for _, source := range sources {
		cp, _ := sink.LoadCheckpoint(ctx, source)
		if cp != "" {
			out[source] = cp
		}
	}
	return out
}

func Cursor(parts ...any) string {
	values := make([]string, 0, len(parts))
	for _, p := range parts {
		values = append(values, fmt.Sprint(p))
	}
	return strings.Join(values, "/")
}

func SortEvents(events []Event) {
	sort.Slice(events, func(i, j int) bool {
		if events[i].OccurredAt.Equal(events[j].OccurredAt) {
			return events[i].EventID < events[j].EventID
		}
		return events[i].OccurredAt.Before(events[j].OccurredAt)
	})
}
