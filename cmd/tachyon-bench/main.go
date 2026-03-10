package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aetherbus/aetherbus-tachyon/internal/delivery/zmq"
	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/aetherbus/aetherbus-tachyon/internal/media"
	"github.com/aetherbus/aetherbus-tachyon/internal/repository"
	"github.com/aetherbus/aetherbus-tachyon/internal/usecase"
	"github.com/pebbe/zmq4"
)

type benchConfig struct {
	Mode         string
	Topic        string
	PayloadSize  int
	Compress     bool
	Duration     time.Duration
	Connections  int
	SendRate     int // total msg/s across all connections; 0 = max
	RouterAddr   string
	PubAddr      string
	LaunchBroker bool
	PprofAddr    string
	Drain        time.Duration
	Scenario     string
}

var pprofOnce sync.Once

type benchData struct {
	Seq          uint64 `json:"seq"`
	SentUnixNano int64  `json:"sent_unix_nano"`
	Payload      string `json:"payload"`
}

type benchEvent struct {
	ID              string    `json:"ID"`
	Topic           string    `json:"Topic"`
	Source          string    `json:"Source"`
	Timestamp       time.Time `json:"Timestamp"`
	Data            benchData `json:"Data"`
	DataContentType string    `json:"DataContentType"`
	DataSchema      string    `json:"DataSchema"`
	SpecVersion     string    `json:"SpecVersion"`
}

type counters struct {
	sentMessages   atomic.Uint64
	recvMessages   atomic.Uint64
	sentWireBytes  atomic.Uint64
	recvRawBytes   atomic.Uint64
	sendErrors     atomic.Uint64
	recvErrors     atomic.Uint64
	decodeErrors   atomic.Uint64
	lastSequenceID atomic.Uint64
}

type benchResult struct {
	cfg            benchConfig
	sentMessages   uint64
	recvMessages   uint64
	sentWireBytes  uint64
	recvRawBytes   uint64
	sendErrors     uint64
	recvErrors     uint64
	decodeErrors   uint64
	latencies      []time.Duration
	elapsed        time.Duration
	totalAlloc     uint64
	mallocs        uint64
	bytesPerOp     float64
	allocsPerOp    float64
	recvBandwidth  float64
	sendBandwidth  float64
	messagesPerSec float64
}

type noOpCompressor struct{}

func (c *noOpCompressor) Compress(data []byte) ([]byte, error)   { return data, nil }
func (c *noOpCompressor) Decompress(data []byte) ([]byte, error) { return data, nil }

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "harness":
		cfg := parseHarnessFlags(os.Args[2:])
		if err := runHarnessAndReport(cfg); err != nil {
			log.Fatal(err)
		}
	case "matrix":
		cfg := parseHarnessFlags(os.Args[2:])
		if err := runBenchmarkMatrix(cfg); err != nil {
			log.Fatal(err)
		}
	case "run":
		cfg := parseRunFlags(os.Args[2:])
		if err := runAndReport(cfg); err != nil {
			log.Fatal(err)
		}
	case "baseline":
		cfg := parseRunFlags(os.Args[2:])
		if err := runBaseline(cfg); err != nil {
			log.Fatal(err)
		}
	case "step-load":
		cfg, startRate, stepRate, maxRate := parseStepLoadFlags(os.Args[2:])
		if err := runStepLoad(cfg, startRate, stepRate, maxRate); err != nil {
			log.Fatal(err)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println(`tachyon-bench

Usage:
  go run ./cmd/tachyon-bench harness [flags]
  go run ./cmd/tachyon-bench matrix [flags]
  go run ./cmd/tachyon-bench run [flags]
  go run ./cmd/tachyon-bench baseline [flags]
  go run ./cmd/tachyon-bench step-load [flags]

Examples:
  go run ./cmd/tachyon-bench harness --mode direct-ack --payload-class small --compress=true --duration 20s
  go run ./cmd/tachyon-bench harness --mode fanout --fanout-subs 8 --payload-class medium --compress=false
  go run ./cmd/tachyon-bench harness --mode mixed --mixed-topics 8 --duration 30s
  go run ./cmd/tachyon-bench matrix --duration 10s --connections 2
  go run ./cmd/tachyon-bench run --scenario A --duration 30s --connections 4 --topic system.heartbeat
  go run ./cmd/tachyon-bench baseline --duration 20s --connections 2 --compress=false
  go run ./cmd/tachyon-bench run --payload-size 1MB --compress=false --duration 30s --connections 2 --topic media.video.chunk
  go run ./cmd/tachyon-bench step-load --start-rate 1000 --step 1000 --max-rate 10000 --payload-size 64KB --duration 15s

Notes:
  - harness implements first-class scenarios: direct-ack, fanout, mixed.
  - matrix runs CI-friendly benchmark sweeps across mode/payload/compression.
  - --launch-broker=true starts an in-process Tachyon broker.
  - --compress=false uses a no-op compressor in the local benchmark broker.
  - baseline runs 64KB / 1MB / 8MB for quick large-payload comparisons.
  - pprof can be enabled with --pprof :6060`)
}

func parseRunFlags(args []string) benchConfig {
	fs := flag.NewFlagSet("run", flag.ExitOnError)

	var payloadSizeStr string
	cfg := benchConfig{
		Topic:        "user.created",
		PayloadSize:  512,
		Compress:     true,
		Duration:     15 * time.Second,
		Connections:  1,
		SendRate:     0,
		RouterAddr:   "tcp://127.0.0.1:19555",
		PubAddr:      "tcp://127.0.0.1:19556",
		LaunchBroker: true,
		PprofAddr:    "",
		Drain:        2 * time.Second,
		Scenario:     "",
	}

	fs.StringVar(&cfg.Scenario, "scenario", "", "preset scenario: A, B, C, D")
	fs.StringVar(&cfg.Topic, "topic", cfg.Topic, "topic to publish")
	fs.StringVar(&payloadSizeStr, "payload-size", "512B", "payload size: 512B, 64KB, 1MB, 8MB")
	fs.BoolVar(&cfg.Compress, "compress", cfg.Compress, "compress payload with LZ4")
	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "benchmark duration")
	fs.IntVar(&cfg.Connections, "connections", cfg.Connections, "number of DEALER connections")
	fs.IntVar(&cfg.SendRate, "rate", cfg.SendRate, "total send rate msg/s across all connections; 0 = max")
	fs.StringVar(&cfg.RouterAddr, "router", cfg.RouterAddr, "ROUTER bind/connect address")
	fs.StringVar(&cfg.PubAddr, "pub", cfg.PubAddr, "PUB bind/connect address")
	fs.BoolVar(&cfg.LaunchBroker, "launch-broker", cfg.LaunchBroker, "launch local benchmark broker")
	fs.StringVar(&cfg.PprofAddr, "pprof", cfg.PprofAddr, "pprof listen address, e.g. :6060")
	fs.DurationVar(&cfg.Drain, "drain", cfg.Drain, "time to keep receiving after send phase stops")

	fs.Parse(args)

	applyScenario(&cfg)
	if payloadSizeStr != "" {
		cfg.PayloadSize = mustParseSize(payloadSizeStr)
	}
	if cfg.Connections <= 0 {
		cfg.Connections = 1
	}
	return cfg
}

func parseStepLoadFlags(args []string) (benchConfig, int, int, int) {
	fs := flag.NewFlagSet("step-load", flag.ExitOnError)

	var payloadSizeStr string
	cfg := benchConfig{
		Topic:        "user.created",
		PayloadSize:  64 * 1024,
		Compress:     true,
		Duration:     10 * time.Second,
		Connections:  4,
		RouterAddr:   "tcp://127.0.0.1:19555",
		PubAddr:      "tcp://127.0.0.1:19556",
		LaunchBroker: true,
		PprofAddr:    "",
		Drain:        2 * time.Second,
	}

	startRate := 1000
	stepRate := 1000
	maxRate := 10000

	fs.StringVar(&cfg.Topic, "topic", cfg.Topic, "topic to publish")
	fs.StringVar(&payloadSizeStr, "payload-size", "64KB", "payload size")
	fs.BoolVar(&cfg.Compress, "compress", cfg.Compress, "compress payload")
	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "duration per step")
	fs.IntVar(&cfg.Connections, "connections", cfg.Connections, "number of DEALER connections")
	fs.IntVar(&startRate, "start-rate", startRate, "starting total send rate msg/s")
	fs.IntVar(&stepRate, "step", stepRate, "rate increment msg/s")
	fs.IntVar(&maxRate, "max-rate", maxRate, "maximum total send rate msg/s")
	fs.StringVar(&cfg.RouterAddr, "router", cfg.RouterAddr, "ROUTER address")
	fs.StringVar(&cfg.PubAddr, "pub", cfg.PubAddr, "PUB address")
	fs.BoolVar(&cfg.LaunchBroker, "launch-broker", cfg.LaunchBroker, "launch local benchmark broker")
	fs.StringVar(&cfg.PprofAddr, "pprof", cfg.PprofAddr, "pprof listen address")
	fs.DurationVar(&cfg.Drain, "drain", cfg.Drain, "drain time after send phase")

	fs.Parse(args)

	cfg.PayloadSize = mustParseSize(payloadSizeStr)
	if cfg.Connections <= 0 {
		cfg.Connections = 1
	}
	return cfg, startRate, stepRate, maxRate
}

func applyScenario(cfg *benchConfig) {
	switch strings.ToUpper(cfg.Scenario) {
	case "A":
		cfg.Topic = "system.heartbeat"
		cfg.PayloadSize = 512
		cfg.Compress = false
	case "B":
		cfg.Topic = "telemetry.sensor.alpha"
		cfg.PayloadSize = 4 * 1024
		cfg.Compress = true
	case "C":
		cfg.Topic = "media.video.chunk"
		cfg.PayloadSize = 1 * 1024 * 1024
		cfg.Compress = false
	case "D":
		cfg.Topic = "media.video.chunk"
		cfg.PayloadSize = 8 * 1024 * 1024
		cfg.Compress = false
	}
}

func runAndReport(cfg benchConfig) error {
	result, err := runBenchmark(cfg)
	if err != nil {
		return err
	}
	printReport(result)
	return nil
}

func runStepLoad(cfg benchConfig, startRate, stepRate, maxRate int) error {
	fmt.Printf("Step load benchmark: payload=%s compress=%v topic=%s duration=%s connections=%d\n",
		humanSize(cfg.PayloadSize), cfg.Compress, cfg.Topic, cfg.Duration, cfg.Connections)

	for rate := startRate; rate <= maxRate; rate += stepRate {
		stepCfg := cfg
		stepCfg.SendRate = rate
		result, err := runBenchmark(stepCfg)
		if err != nil {
			return fmt.Errorf("rate=%d: %w", rate, err)
		}

		p50 := percentile(result.latencies, 50)
		p95 := percentile(result.latencies, 95)
		p99 := percentile(result.latencies, 99)

		fmt.Printf("rate=%7d msg/s | recv=%7d | msgs/sec=%9.2f | recvMB/s=%8.2f | p50=%8s | p95=%8s | p99=%8s | drops=%d\n",
			rate,
			result.recvMessages,
			result.messagesPerSec,
			result.recvBandwidth,
			p50,
			p95,
			p99,
			result.sentMessages-result.recvMessages,
		)
	}
	return nil
}

func runBaseline(cfg benchConfig) error {
	payloads := []int{64 * 1024, 1 * 1024 * 1024, 8 * 1024 * 1024}

	fmt.Printf("Baseline suite: topic=%s compress=%v duration=%s connections=%d\n",
		cfg.Topic, cfg.Compress, cfg.Duration, cfg.Connections)
	for _, payloadSize := range payloads {
		runCfg := cfg
		runCfg.PayloadSize = payloadSize

		result, err := runBenchmark(runCfg)
		if err != nil {
			return fmt.Errorf("payload=%s: %w", humanSize(payloadSize), err)
		}

		fmt.Printf("payload=%8s | recv=%7d | msg/s=%9.2f | recvMB/s=%8.2f | Bytes/Op=%10.2f | Allocs/Op=%8.2f\n",
			humanSize(payloadSize),
			result.recvMessages,
			result.messagesPerSec,
			result.recvBandwidth,
			result.bytesPerOp,
			result.allocsPerOp,
		)
	}

	fmt.Println("Tip: run with --pprof :6060 and capture CPU profile via go tool pprof http://127.0.0.1:6060/debug/pprof/profile?seconds=30")
	return nil
}

func runBenchmark(cfg benchConfig) (*benchResult, error) {
	if cfg.PprofAddr != "" {
		pprofOnce.Do(func() {
			go func() {
				log.Printf("pprof listening on %s", cfg.PprofAddr)
				if err := http.ListenAndServe(cfg.PprofAddr, nil); err != nil {
					log.Printf("pprof stopped: %v", err)
				}
			}()
		})
	}

	var brokerCancel context.CancelFunc = func() {}
	if cfg.LaunchBroker {
		var brokerCtx context.Context
		brokerCtx, brokerCancel = context.WithCancel(context.Background())
		if err := startLocalBroker(brokerCtx, cfg); err != nil {
			return nil, err
		}
		defer brokerCancel()
	}

	time.Sleep(300 * time.Millisecond) // allow bind/startup

	subSocket, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		return nil, fmt.Errorf("create SUB socket: %w", err)
	}
	defer subSocket.Close()

	if err := subSocket.SetSubscribe(cfg.Topic); err != nil {
		return nil, fmt.Errorf("subscribe SUB: %w", err)
	}
	if err := subSocket.SetRcvtimeo(200 * time.Millisecond); err != nil {
		return nil, fmt.Errorf("set SUB timeout: %w", err)
	}
	if err := subSocket.Connect(cfg.PubAddr); err != nil {
		return nil, fmt.Errorf("connect SUB: %w", err)
	}

	// Slow joiner protection.
	time.Sleep(250 * time.Millisecond)

	wireCompressor := chooseWireCompressor(cfg.Compress)

	payload := strings.Repeat("x", cfg.PayloadSize)

	start := time.Now()
	sendUntil := start.Add(cfg.Duration)
	recvUntil := sendUntil.Add(cfg.Drain)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	var ctr counters
	var recvWG sync.WaitGroup
	latencies := make([]time.Duration, 0, 1024)

	recvWG.Add(1)
	go func() {
		defer recvWG.Done()
		for {
			if time.Now().After(recvUntil) {
				return
			}

			msg, err := subSocket.RecvMessageBytes(0)
			if err != nil {
				ctr.recvErrors.Add(1)
				continue
			}
			if len(msg) != 2 {
				ctr.recvErrors.Add(1)
				continue
			}

			var evt benchEvent
			if err := json.Unmarshal(msg[1], &evt); err != nil {
				ctr.decodeErrors.Add(1)
				continue
			}

			ctr.recvMessages.Add(1)
			ctr.recvRawBytes.Add(uint64(len(msg[1]) + len(msg[0])))

			sentAt := time.Unix(0, evt.Data.SentUnixNano)
			latencies = append(latencies, time.Since(sentAt))
		}
	}()

	var sendWG sync.WaitGroup
	perConnRate := 0
	if cfg.SendRate > 0 {
		perConnRate = int(math.Ceil(float64(cfg.SendRate) / float64(cfg.Connections)))
	}

	for i := 0; i < cfg.Connections; i++ {
		sendWG.Add(1)
		go func(id int) {
			defer sendWG.Done()

			dealer, err := zmq4.NewSocket(zmq4.DEALER)
			if err != nil {
				ctr.sendErrors.Add(1)
				return
			}
			defer dealer.Close()

			if err := dealer.SetSndtimeo(2 * time.Second); err != nil {
				ctr.sendErrors.Add(1)
				return
			}
			if err := dealer.Connect(cfg.RouterAddr); err != nil {
				ctr.sendErrors.Add(1)
				return
			}

			var ticker *time.Ticker
			if perConnRate > 0 {
				interval := time.Second / time.Duration(perConnRate)
				if interval <= 0 {
					interval = time.Microsecond
				}
				ticker = time.NewTicker(interval)
				defer ticker.Stop()
			}

			for {
				if time.Now().After(sendUntil) {
					return
				}
				if ticker != nil {
					<-ticker.C
				}

				seq := ctr.lastSequenceID.Add(1)
				now := time.Now().UTC()

				evt := benchEvent{
					ID:        fmt.Sprintf("evt-%d-%d", id, seq),
					Topic:     cfg.Topic,
					Source:    fmt.Sprintf("tachyon-bench-%d", id),
					Timestamp: now,
					Data: benchData{
						Seq:          seq,
						SentUnixNano: now.UnixNano(),
						Payload:      payload,
					},
					DataContentType: "application/json",
					SpecVersion:     "1.0",
				}

				raw, err := json.Marshal(evt)
				if err != nil {
					ctr.sendErrors.Add(1)
					continue
				}
				wire, err := wireCompressor.Compress(raw)
				if err != nil {
					ctr.sendErrors.Add(1)
					continue
				}

				if _, err := dealer.SendMessage(cfg.Topic, wire); err != nil {
					ctr.sendErrors.Add(1)
					continue
				}

				ctr.sentMessages.Add(1)
				ctr.sentWireBytes.Add(uint64(len(wire) + len(cfg.Topic)))
			}
		}(i)
	}

	sendWG.Wait()
	recvWG.Wait()

	elapsed := time.Since(start)
	runtime.GC()
	runtime.ReadMemStats(&after)

	result := &benchResult{
		cfg:            cfg,
		sentMessages:   ctr.sentMessages.Load(),
		recvMessages:   ctr.recvMessages.Load(),
		sentWireBytes:  ctr.sentWireBytes.Load(),
		recvRawBytes:   ctr.recvRawBytes.Load(),
		sendErrors:     ctr.sendErrors.Load(),
		recvErrors:     ctr.recvErrors.Load(),
		decodeErrors:   ctr.decodeErrors.Load(),
		latencies:      latencies,
		elapsed:        elapsed,
		totalAlloc:     after.TotalAlloc - before.TotalAlloc,
		mallocs:        after.Mallocs - before.Mallocs,
		messagesPerSec: float64(ctr.recvMessages.Load()) / elapsed.Seconds(),
		sendBandwidth:  bytesToMB(float64(ctr.sentWireBytes.Load())) / elapsed.Seconds(),
		recvBandwidth:  bytesToMB(float64(ctr.recvRawBytes.Load())) / elapsed.Seconds(),
	}

	if result.recvMessages > 0 {
		result.bytesPerOp = float64(result.totalAlloc) / float64(result.recvMessages)
		result.allocsPerOp = float64(result.mallocs) / float64(result.recvMessages)
	}

	return result, nil
}

func startLocalBroker(ctx context.Context, cfg benchConfig) error {
	routeStore := repository.NewART_RouteStore()
	routeStore.AddRoute(cfg.Topic, "node-1")

	codec := media.NewJSONCodec()
	eventRouter := usecase.NewEventRouter(routeStore)
	router := zmq.NewRouter(
		cfg.RouterAddr,
		cfg.PubAddr,
		eventRouter,
		codec,
		chooseBrokerCompressor(cfg.Compress),
	)

	return router.Start(ctx)
}

func chooseBrokerCompressor(enabled bool) domain.Compressor {
	if enabled {
		return media.NewLZ4Compressor()
	}
	return &noOpCompressor{}
}

func chooseWireCompressor(enabled bool) domain.Compressor {
	if enabled {
		return media.NewLZ4Compressor()
	}
	return &noOpCompressor{}
}

func printReport(r *benchResult) {
	sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })

	fmt.Println("--- AetherBus Tachyon Benchmark Results ---")
	fmt.Printf("Topic:            %s\n", r.cfg.Topic)
	fmt.Printf("Payload:          %s\n", humanSize(r.cfg.PayloadSize))
	fmt.Printf("Compression:      %v\n", r.cfg.Compress)
	fmt.Printf("Connections:      %d\n", r.cfg.Connections)
	if r.cfg.SendRate == 0 {
		fmt.Printf("Rate target:      max\n")
	} else {
		fmt.Printf("Rate target:      %d msg/s\n", r.cfg.SendRate)
	}
	fmt.Printf("Duration:         %s (drain %s)\n", r.cfg.Duration, r.cfg.Drain)
	fmt.Printf("Elapsed:          %s\n", r.elapsed.Round(time.Millisecond))
	fmt.Println()

	fmt.Println("[ Throughput ]")
	fmt.Printf("Sent:             %d msg\n", r.sentMessages)
	fmt.Printf("Received:         %d msg\n", r.recvMessages)
	fmt.Printf("Messages/sec:     %.2f msg/s\n", r.messagesPerSec)
	fmt.Printf("Ingress Wire BW:  %.2f MB/s\n", r.sendBandwidth)
	fmt.Printf("Egress Raw BW:    %.2f MB/s\n", r.recvBandwidth)
	fmt.Printf("Drops:            %d\n", r.sentMessages-r.recvMessages)
	fmt.Println()

	fmt.Println("[ Latency Distribution ]")
	fmt.Printf("Min:              %s\n", percentile(r.latencies, 0))
	fmt.Printf("P50:              %s\n", percentile(r.latencies, 50))
	fmt.Printf("P95:              %s\n", percentile(r.latencies, 95))
	fmt.Printf("P99:              %s\n", percentile(r.latencies, 99))
	fmt.Printf("Max:              %s\n", percentile(r.latencies, 100))
	fmt.Println()

	fmt.Println("[ Resource Cost ]")
	fmt.Printf("TotalAlloc:       %s\n", humanSize(int(r.totalAlloc)))
	fmt.Printf("Mallocs:          %d\n", r.mallocs)
	fmt.Printf("Bytes/Op:         %.2f\n", r.bytesPerOp)
	fmt.Printf("Allocs/Op:        %.2f\n", r.allocsPerOp)
	fmt.Println()

	fmt.Println("[ Errors ]")
	fmt.Printf("Send errors:      %d\n", r.sendErrors)
	fmt.Printf("Recv errors:      %d\n", r.recvErrors)
	fmt.Printf("Decode errors:    %d\n", r.decodeErrors)
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	if p <= 0 {
		return values[0]
	}
	if p >= 100 {
		return values[len(values)-1]
	}
	idx := int(math.Ceil((p/100.0)*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func mustParseSize(s string) int {
	n, ok := parseSize(s)
	if !ok {
		log.Fatalf("invalid size: %q", s)
	}
	return n
}

func parseSize(s string) (int, bool) {
	s = strings.TrimSpace(strings.ToUpper(s))

	multiplier := 1
	switch {
	case strings.HasSuffix(s, "KB"):
		multiplier = 1024
		s = strings.TrimSuffix(s, "KB")
	case strings.HasSuffix(s, "MB"):
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "GB"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "B"):
		multiplier = 1
		s = strings.TrimSuffix(s, "B")
	}

	var v int
	_, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &v)
	if err != nil || v < 0 {
		return 0, false
	}
	return v * multiplier, true
}

func humanSize(n int) string {
	if n >= 1024*1024*1024 {
		return fmt.Sprintf("%.2f GB", float64(n)/1024.0/1024.0/1024.0)
	}
	if n >= 1024*1024 {
		return fmt.Sprintf("%.2f MB", float64(n)/1024.0/1024.0)
	}
	if n >= 1024 {
		return fmt.Sprintf("%.2f KB", float64(n)/1024.0)
	}
	return fmt.Sprintf("%d B", n)
}

func bytesToMB(v float64) float64 {
	return v / 1024.0 / 1024.0
}
