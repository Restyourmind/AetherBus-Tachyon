package exporter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileSinkDedupesAndCheckpoints(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewFileSink(filepath.Join(dir, "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	ex := NewAsyncExporter(sink, 8)
	event := Event{Kind: "delivery", Action: "ack", Source: "router", Cursor: Cursor("router", 1), MessageID: "m-1", OccurredAt: time.Unix(1, 0).UTC()}
	ex.Emit(event)
	ex.Emit(event)
	if err := ex.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	if got := len(splitNonEmpty(string(data))); got != 1 {
		t.Fatalf("expected 1 persisted event, got %d", got)
	}
	cp, err := sink.LoadCheckpoint(t.Context(), "router")
	if err != nil {
		t.Fatal(err)
	}
	if cp == "" {
		t.Fatal("expected checkpoint to be saved")
	}
}

func splitNonEmpty(s string) []string {
	out := []string{}
	for _, line := range strings.Split(s, "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}
