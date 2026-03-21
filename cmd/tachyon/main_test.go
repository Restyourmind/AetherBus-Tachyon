package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aetherbus/aetherbus-tachyon/config"
	transportzmq "github.com/aetherbus/aetherbus-tachyon/internal/delivery/zmq"
	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/aetherbus/aetherbus-tachyon/internal/media"
	"github.com/aetherbus/aetherbus-tachyon/internal/repository"
	"github.com/aetherbus/aetherbus-tachyon/internal/usecase"
	"github.com/pebbe/zmq4"
)

func TestMainIntegration(t *testing.T) {
	const routerAddr = "tcp://127.0.0.1:19555"
	const pubAddr = "tcp://127.0.0.1:19556"

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		time.Sleep(300 * time.Millisecond)
	}()

	routeStore := repository.NewART_RouteStore()
	_ = routeStore.AddRoute(domain.RouteKey{Topic: "user.created"}, "node-1")

	codec := media.NewJSONCodec()
	compressor := media.NewLZ4Compressor()
	eventRouter := usecase.NewEventRouter(routeStore)
	router := transportzmq.NewRouter(routerAddr, pubAddr, eventRouter, codec, compressor)

	if err := router.Start(ctx); err != nil {
		t.Fatalf("Failed to start ZMQ router: %v", err)
	}

	subSocket, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		t.Fatalf("Failed to create SUB socket: %v", err)
	}
	defer subSocket.Close()

	if err := subSocket.SetRcvtimeo(2 * time.Second); err != nil {
		t.Fatalf("Failed to set SUB recv timeout: %v", err)
	}
	if err := subSocket.Connect(pubAddr); err != nil {
		t.Fatalf("Failed to connect SUB socket: %v", err)
	}
	if err := subSocket.SetSubscribe("user.created"); err != nil {
		t.Fatalf("Failed to subscribe on SUB socket: %v", err)
	}

	dealerSocket, err := zmq4.NewSocket(zmq4.DEALER)
	if err != nil {
		t.Fatalf("Failed to create DEALER socket: %v", err)
	}
	defer dealerSocket.Close()

	if err := dealerSocket.SetSndtimeo(2 * time.Second); err != nil {
		t.Fatalf("Failed to set DEALER send timeout: %v", err)
	}
	if err := dealerSocket.Connect(routerAddr); err != nil {
		t.Fatalf("Failed to connect DEALER socket: %v", err)
	}

	// Give PUB/SUB time to establish subscription before publishing.
	time.Sleep(200 * time.Millisecond)

	testEvent := domain.Event{
		ID:     "evt-123",
		Source: "test-client",
		Data: map[string]any{
			"id":   "123",
			"name": "test",
			"blob": strings.Repeat("aetherbus-", 256),
		},
		DataContentType: "application/json",
		SpecVersion:     "1.0",
	}

	encodedEvent, err := codec.Encode(testEvent)
	if err != nil {
		t.Fatalf("Failed to encode event: %v", err)
	}

	compressedEvent, err := compressor.Compress(encodedEvent)
	if err != nil {
		t.Fatalf("Failed to compress event: %v", err)
	}
	if len(compressedEvent) == 0 {
		t.Fatal("Compressed event is empty; test payload must be compressible for LZ4 block mode")
	}

	// Send message following the router protocol: [Delimiter, Topic, Payload].
	// DEALER automatically prefixes its identity for ROUTER.
	if _, err := dealerSocket.SendMessage("", "user.created", compressedEvent); err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	msg, err := subSocket.RecvMessageBytes(0)
	if err != nil {
		t.Fatalf("Failed to receive message: %v", err)
	}

	if len(msg) != 2 {
		t.Fatalf("Expected 2 parts in message, got %d", len(msg))
	}

	expectedTopic := "user.created"
	if string(msg[0]) != expectedTopic {
		t.Fatalf("Expected topic %q, got %q", expectedTopic, string(msg[0]))
	}

	var receivedEvent domain.Event
	if err := codec.Decode(msg[1], &receivedEvent); err != nil {
		t.Fatalf("Failed to decode received payload: %v", err)
	}

	if receivedEvent.ID != testEvent.ID {
		t.Fatalf("Expected event ID %q, got %q", testEvent.ID, receivedEvent.ID)
	}
	if receivedEvent.Source != testEvent.Source {
		t.Fatalf("Expected source %q, got %q", testEvent.Source, receivedEvent.Source)
	}
	if receivedEvent.Topic != expectedTopic {
		t.Fatalf("Expected topic %q in event, got %q", expectedTopic, receivedEvent.Topic)
	}
}

func TestDLQCLIReplayRequiresExplicitTarget(t *testing.T) {
	t.Setenv("WAL_ENABLED", "true")
	t.Setenv("WAL_PATH", t.TempDir()+"/delivery.wal")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	wal := transportzmq.NewFileWAL(cfg.WALPath)
	if err := wal.AppendDeadLettered(transportzmq.DeadLetterRecord{MessageID: "msg-1", ConsumerID: "worker-1", Topic: "orders.created", Payload: []byte("p1"), DeadLetteredAt: time.Now().UTC(), Reason: "retry_exhausted"}); err != nil {
		t.Fatalf("append dead letter: %v", err)
	}
	if err := runDLQCLI(cfg, []string{"replay", "--ids", "msg-1", "--confirm", "REPLAY"}); err == nil {
		t.Fatalf("expected replay to require explicit target")
	}
}

func TestDLQCLIAuditQueryParsesFilters(t *testing.T) {
	query, err := parseAuditQuery("msg-1", "ops@example.com", "2026-03-21T00:00:00Z", "2026-03-21T01:00:00Z")
	if err != nil {
		t.Fatalf("parse audit query: %v", err)
	}
	if query.MessageID != "msg-1" || query.Actor != "ops@example.com" {
		t.Fatalf("unexpected query fields: %#v", query)
	}
	if query.Start.IsZero() || query.End.IsZero() {
		t.Fatalf("expected parsed time bounds: %#v", query)
	}
}
