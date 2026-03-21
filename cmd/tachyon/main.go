package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aetherbus/aetherbus-tachyon/config"
	"github.com/aetherbus/aetherbus-tachyon/internal/app"
	"github.com/aetherbus/aetherbus-tachyon/internal/delivery/zmq"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		os.Exit(1)
	}
	if len(os.Args) > 1 && os.Args[1] == "dlq" {
		if err := runDLQCLI(cfg, os.Args[2:]); err != nil {
			fmt.Printf("DLQ command failed: %v\n", err)
			os.Exit(1)
		}
		return
	}
	runServer(cfg)
}

func runServer(cfg *config.Config) {
	fs := flag.NewFlagSet("tachyon", flag.ExitOnError)
	compress := fs.Bool("compress", true, "Enable LZ4 compression in the router runtime")
	_ = fs.Parse(os.Args[1:])
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := app.NewBenchmarkRuntime(cfg, map[string]string{"user.created": "node-1"}, *compress)
	zmqRouter := runtime.Router
	if err := zmqRouter.Start(ctx); err != nil {
		fmt.Printf("Failed to start ZMQ router: %v\n", err)
		os.Exit(1)
	}
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-signalCh:
		fmt.Printf("Received signal: %s. Shutting down...\n", sig)
		cancel()
	case <-ctx.Done():
	}
	fmt.Println("AetherBus-Tachyon server has stopped.")
}

func runDLQCLI(cfg *config.Config, args []string) error {
	wal := zmq.NewFileWAL(cfg.WALPath)
	if len(args) == 0 {
		return fmt.Errorf("expected dlq subcommand: list, inspect, replay, purge")
	}
	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("dlq list", flag.ExitOnError)
		consumer := fs.String("consumer", "", "filter by consumer id")
		topic := fs.String("topic", "", "filter by topic")
		reason := fs.String("reason", "", "filter by reason")
		_ = fs.Parse(args[1:])
		records, err := wal.ListDeadLetters(zmq.DeadLetterFilter{ConsumerID: *consumer, Topic: *topic, Reason: *reason})
		if err != nil {
			return err
		}
		return writeJSON(records)
	case "inspect":
		fs := flag.NewFlagSet("dlq inspect", flag.ExitOnError)
		messageID := fs.String("id", "", "message id")
		_ = fs.Parse(args[1:])
		if *messageID == "" {
			return fmt.Errorf("inspect requires --id")
		}
		record, ok, err := wal.GetDeadLetter(*messageID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("dead-letter record %q not found", *messageID)
		}
		return writeJSON(record)
	case "replay":
		fs := flag.NewFlagSet("dlq replay", flag.ExitOnError)
		ids := fs.String("ids", "", "comma-separated message ids")
		targetConsumer := fs.String("target-consumer", "", "required original consumer id")
		targetTopic := fs.String("target-topic", "", "required original topic")
		confirm := fs.String("confirm", "", "must equal REPLAY")
		_ = fs.Parse(args[1:])
		result, err := wal.ReplayDeadLetters(zmq.DeadLetterReplayRequest{MessageIDs: splitCSV(*ids), TargetConsumerID: *targetConsumer, TargetTopic: *targetTopic, Confirm: *confirm, RequestedAt: time.Now().UTC()})
		if err != nil {
			return err
		}
		return writeJSON(result)
	case "purge":
		fs := flag.NewFlagSet("dlq purge", flag.ExitOnError)
		ids := fs.String("ids", "", "comma-separated message ids")
		consumer := fs.String("consumer", "", "filter by consumer id")
		topic := fs.String("topic", "", "filter by topic")
		reason := fs.String("reason", "", "filter by reason")
		confirm := fs.String("confirm", "", "must equal PURGE")
		_ = fs.Parse(args[1:])
		result, err := wal.PurgeDeadLetters(zmq.DeadLetterPurgeRequest{MessageIDs: splitCSV(*ids), Filter: zmq.DeadLetterFilter{ConsumerID: *consumer, Topic: *topic, Reason: *reason}, Confirm: *confirm})
		if err != nil {
			return err
		}
		return writeJSON(result)
	default:
		return fmt.Errorf("unknown dlq subcommand %q", args[0])
	}
}

func splitCSV(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
func writeJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
