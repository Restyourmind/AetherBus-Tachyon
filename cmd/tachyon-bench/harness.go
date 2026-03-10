package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aetherbus/aetherbus-tachyon/config"
	"github.com/aetherbus/aetherbus-tachyon/internal/app"
	"github.com/aetherbus/aetherbus-tachyon/internal/domain"
	"github.com/pebbe/zmq4"
)

type harnessConfig struct {
	Mode         string
	PayloadClass string
	PayloadSize  int
	Compress     bool
	Duration     time.Duration
	Connections  int
	SendRate     int
	FanoutSubs   int
	MixedTopics  int
	RouterAddr   string
	PubAddr      string
	LaunchBroker bool
	Drain        time.Duration
}

type harnessResult struct {
	cfg            harnessConfig
	recvMessages   uint64
	sentMessages   uint64
	latencies      []time.Duration
	elapsed        time.Duration
	messagesPerSec float64
	throughputMBps float64
	cpuPercent     float64
	memoryRSSBytes uint64
	totalAlloc     uint64
	mallocs        uint64
	bytesPerOp     float64
	allocsPerOp    float64
	errors         uint64
}

type usageSnapshot struct {
	utime       time.Duration
	stime       time.Duration
	maxRSSBytes uint64
}

func parseHarnessFlags(args []string) harnessConfig {
	fs := flag.NewFlagSet("harness", flag.ExitOnError)
	cfg := harnessConfig{
		Mode:         "direct-ack",
		PayloadClass: "small",
		Compress:     true,
		Duration:     15 * time.Second,
		Connections:  2,
		SendRate:     0,
		FanoutSubs:   4,
		MixedTopics:  6,
		RouterAddr:   "tcp://127.0.0.1:19555",
		PubAddr:      "tcp://127.0.0.1:19556",
		LaunchBroker: true,
		Drain:        2 * time.Second,
	}

	fs.StringVar(&cfg.Mode, "mode", cfg.Mode, "benchmark mode: direct-ack, fanout, mixed")
	fs.StringVar(&cfg.PayloadClass, "payload-class", cfg.PayloadClass, "payload class: small, medium, large")
	fs.BoolVar(&cfg.Compress, "compress", cfg.Compress, "compress payload with LZ4")
	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "benchmark duration")
	fs.IntVar(&cfg.Connections, "connections", cfg.Connections, "number of producers")
	fs.IntVar(&cfg.SendRate, "rate", cfg.SendRate, "total send rate msg/s across producers; 0 = max")
	fs.IntVar(&cfg.FanoutSubs, "fanout-subs", cfg.FanoutSubs, "number of fanout subscribers for fanout mode")
	fs.IntVar(&cfg.MixedTopics, "mixed-topics", cfg.MixedTopics, "number of weighted topics for mixed mode")
	fs.StringVar(&cfg.RouterAddr, "router", cfg.RouterAddr, "ROUTER bind/connect address")
	fs.StringVar(&cfg.PubAddr, "pub", cfg.PubAddr, "PUB bind/connect address")
	fs.BoolVar(&cfg.LaunchBroker, "launch-broker", cfg.LaunchBroker, "launch local benchmark broker")
	fs.DurationVar(&cfg.Drain, "drain", cfg.Drain, "time to continue receiving after send stops")
	_ = fs.Parse(args)

	cfg.Mode = strings.ToLower(cfg.Mode)
	cfg.PayloadClass = strings.ToLower(cfg.PayloadClass)
	cfg.PayloadSize = payloadClassSize(cfg.PayloadClass)
	if cfg.Connections <= 0 {
		cfg.Connections = 1
	}
	if cfg.FanoutSubs <= 0 {
		cfg.FanoutSubs = 1
	}
	if cfg.MixedTopics <= 1 {
		cfg.MixedTopics = 2
	}
	return cfg
}

func payloadClassSize(class string) int {
	switch strings.ToLower(class) {
	case "small":
		return 512
	case "medium":
		return 4 * 1024
	case "large":
		return 64 * 1024
	default:
		return 512
	}
}

func runHarnessAndReport(cfg harnessConfig) error {
	res, err := runHarness(cfg)
	if err != nil {
		return err
	}
	printHarnessReport(res)
	return nil
}

func runBenchmarkMatrix(base harnessConfig) error {
	modes := []string{"direct-ack", "fanout", "mixed"}
	payloads := []string{"small", "medium", "large"}
	compressModes := []bool{false, true}

	fmt.Println("Benchmark matrix (CI-friendly)")
	fmt.Println("mode,payload_class,compress,recv_msgs,msgs_per_sec,p50,p95,p99,cpu_percent,memory_rss_mb,allocs_per_op")
	for _, mode := range modes {
		for _, payload := range payloads {
			for _, compress := range compressModes {
				cfg := base
				cfg.Mode = mode
				cfg.PayloadClass = payload
				cfg.PayloadSize = payloadClassSize(payload)
				cfg.Compress = compress
				res, err := runHarness(cfg)
				if err != nil {
					return fmt.Errorf("mode=%s payload=%s compress=%v: %w", mode, payload, compress, err)
				}
				sort.Slice(res.latencies, func(i, j int) bool { return res.latencies[i] < res.latencies[j] })
				fmt.Printf("%s,%s,%v,%d,%.2f,%s,%s,%s,%.2f,%.2f,%.2f\n",
					mode,
					payload,
					compress,
					res.recvMessages,
					res.messagesPerSec,
					percentile(res.latencies, 50),
					percentile(res.latencies, 95),
					percentile(res.latencies, 99),
					res.cpuPercent,
					float64(res.memoryRSSBytes)/1024.0/1024.0,
					res.allocsPerOp,
				)
			}
		}
	}
	return nil
}

func runHarness(cfg harnessConfig) (*harnessResult, error) {
	topics := benchmarkTopics(cfg)
	if cfg.LaunchBroker {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := startHarnessBroker(ctx, cfg, topics); err != nil {
			return nil, err
		}
		defer cancel()
		time.Sleep(300 * time.Millisecond)
	}

	payload := strings.Repeat("x", cfg.PayloadSize)
	wireComp := chooseWireCompressor(cfg.Compress)
	startUsage := readUsageSnapshot()
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	start := time.Now()

	var sent atomic.Uint64
	var recv atomic.Uint64
	var errors atomic.Uint64
	latMu := sync.Mutex{}
	latencies := make([]time.Duration, 0, 1024)

	sendUntil := start.Add(cfg.Duration)
	recvUntil := sendUntil.Add(cfg.Drain)

	var receiverWG sync.WaitGroup
	switch cfg.Mode {
	case "direct-ack":
		receiverWG.Add(1)
		go func() {
			defer receiverWG.Done()
			if err := runDirectAckReceiver(cfg, wireComp, recvUntil, &recv, &errors, &latMu, &latencies); err != nil {
				errors.Add(1)
			}
		}()
	case "fanout":
		for i := 0; i < cfg.FanoutSubs; i++ {
			receiverWG.Add(1)
			go func() {
				defer receiverWG.Done()
				if err := runFanoutReceiver(cfg, recvUntil, &recv, &errors, &latMu, &latencies); err != nil {
					errors.Add(1)
				}
			}()
		}
	default:
		receiverWG.Add(1)
		go func() {
			defer receiverWG.Done()
			if err := runMixedReceiver(cfg, recvUntil, &recv, &errors, &latMu, &latencies); err != nil {
				errors.Add(1)
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	sendWG := sync.WaitGroup{}
	seq := atomic.Uint64{}
	perConnRate := 0
	if cfg.SendRate > 0 {
		perConnRate = cfg.SendRate / cfg.Connections
		if perConnRate == 0 {
			perConnRate = 1
		}
	}

	for i := 0; i < cfg.Connections; i++ {
		sendWG.Add(1)
		go func(senderID int) {
			defer sendWG.Done()
			s, err := zmq4.NewSocket(zmq4.DEALER)
			if err != nil {
				errors.Add(1)
				return
			}
			defer s.Close()
			if err := s.Connect(cfg.RouterAddr); err != nil {
				errors.Add(1)
				return
			}

			var ticker *time.Ticker
			if perConnRate > 0 {
				ticker = time.NewTicker(time.Second / time.Duration(perConnRate))
				defer ticker.Stop()
			}

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(senderID)))
			for time.Now().Before(sendUntil) {
				if ticker != nil {
					<-ticker.C
				}
				current := seq.Add(1)
				now := time.Now().UTC()
				topic := selectTopic(cfg, topics, rng)
				evt := benchEvent{
					ID:              fmt.Sprintf("evt-%d-%d", senderID, current),
					Topic:           topic,
					Source:          fmt.Sprintf("bench-%d", senderID),
					Timestamp:       now,
					Data:            benchData{Seq: current, SentUnixNano: now.UnixNano(), Payload: payload},
					DataContentType: "application/json",
					SpecVersion:     "1.0",
				}
				raw, err := json.Marshal(evt)
				if err != nil {
					errors.Add(1)
					continue
				}
				wire, err := wireComp.Compress(raw)
				if err != nil {
					errors.Add(1)
					continue
				}
				if _, err := s.SendMessage(topic, wire); err != nil {
					errors.Add(1)
					continue
				}
				sent.Add(1)
			}
		}(i)
	}

	sendWG.Wait()
	receiverWG.Wait()
	elapsed := time.Since(start)
	runtime.GC()
	runtime.ReadMemStats(&memAfter)
	endUsage := readUsageSnapshot()

	result := &harnessResult{
		cfg:            cfg,
		recvMessages:   recv.Load(),
		sentMessages:   sent.Load(),
		latencies:      latencies,
		elapsed:        elapsed,
		messagesPerSec: float64(recv.Load()) / elapsed.Seconds(),
		throughputMBps: bytesToMB(float64(recv.Load()*uint64(cfg.PayloadSize))) / elapsed.Seconds(),
		cpuPercent:     cpuPercent(startUsage, endUsage, elapsed),
		memoryRSSBytes: endUsage.maxRSSBytes,
		totalAlloc:     memAfter.TotalAlloc - memBefore.TotalAlloc,
		mallocs:        memAfter.Mallocs - memBefore.Mallocs,
		errors:         errors.Load(),
	}
	if result.recvMessages > 0 {
		result.bytesPerOp = float64(result.totalAlloc) / float64(result.recvMessages)
		result.allocsPerOp = float64(result.mallocs) / float64(result.recvMessages)
	}
	sort.Slice(result.latencies, func(i, j int) bool { return result.latencies[i] < result.latencies[j] })
	return result, nil
}

func benchmarkTopics(cfg harnessConfig) []string {
	switch cfg.Mode {
	case "mixed":
		topics := make([]string, 0, cfg.MixedTopics)
		for i := 0; i < cfg.MixedTopics; i++ {
			topics = append(topics, fmt.Sprintf("bench.topic.%02d", i))
		}
		return topics
	default:
		return []string{"bench.topic.00"}
	}
}

func selectTopic(cfg harnessConfig, topics []string, rng *rand.Rand) string {
	if cfg.Mode != "mixed" || len(topics) == 1 {
		return topics[0]
	}
	if rng.Intn(100) < 70 {
		return topics[0]
	}
	return topics[1+rng.Intn(len(topics)-1)]
}

func runFanoutReceiver(cfg harnessConfig, recvUntil time.Time, recv *atomic.Uint64, errs *atomic.Uint64, latMu *sync.Mutex, latencies *[]time.Duration) error {
	sub, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		return err
	}
	defer sub.Close()
	if err := sub.SetSubscribe("bench.topic."); err != nil {
		return err
	}
	if err := sub.SetRcvtimeo(100 * time.Millisecond); err != nil {
		return err
	}
	if err := sub.Connect(cfg.PubAddr); err != nil {
		return err
	}
	for time.Now().Before(recvUntil) {
		msg, err := sub.RecvMessageBytes(0)
		if err != nil {
			continue
		}
		if len(msg) != 2 {
			errs.Add(1)
			continue
		}
		var evt benchEvent
		if err := json.Unmarshal(msg[1], &evt); err != nil {
			errs.Add(1)
			continue
		}
		recv.Add(1)
		latMu.Lock()
		*latencies = append(*latencies, time.Since(time.Unix(0, evt.Data.SentUnixNano)))
		latMu.Unlock()
	}
	return nil
}

func runMixedReceiver(cfg harnessConfig, recvUntil time.Time, recv *atomic.Uint64, errs *atomic.Uint64, latMu *sync.Mutex, latencies *[]time.Duration) error {
	return runFanoutReceiver(cfg, recvUntil, recv, errs, latMu, latencies)
}

func runDirectAckReceiver(cfg harnessConfig, wireCompressor domain.Compressor, recvUntil time.Time, recv *atomic.Uint64, errs *atomic.Uint64, latMu *sync.Mutex, latencies *[]time.Duration) error {
	dealer, err := zmq4.NewSocket(zmq4.DEALER)
	if err != nil {
		return err
	}
	defer dealer.Close()
	if err := dealer.SetRcvtimeo(100 * time.Millisecond); err != nil {
		return err
	}
	if err := dealer.Connect(cfg.RouterAddr); err != nil {
		return err
	}

	consumerID := "bench-consumer-1"
	register := map[string]any{
		"type":          "consumer.register",
		"consumer_id":   consumerID,
		"mode":          "direct",
		"subscriptions": []string{"bench.topic.00"},
		"capabilities": map[string]any{
			"supports_ack": true,
			"max_inflight": 1024,
		},
	}
	rawReg, _ := json.Marshal(register)
	wireReg, err := wireCompressor.Compress(rawReg)
	if err != nil {
		return err
	}
	if _, err := dealer.SendMessage("_control", wireReg); err != nil {
		return err
	}

	for time.Now().Before(recvUntil) {
		msg, err := dealer.RecvMessageBytes(0)
		if err != nil {
			continue
		}
		if len(msg) < 2 {
			errs.Add(1)
			continue
		}
		topic := string(msg[len(msg)-2])
		if topic == "_control" {
			continue
		}
		payload := msg[len(msg)-1]
		var evt benchEvent
		if err := json.Unmarshal(payload, &evt); err != nil {
			errs.Add(1)
			continue
		}
		recv.Add(1)
		latMu.Lock()
		*latencies = append(*latencies, time.Since(time.Unix(0, evt.Data.SentUnixNano)))
		latMu.Unlock()

		ack := map[string]any{
			"type":        "ack",
			"consumer_id": consumerID,
			"message_id":  evt.ID,
		}
		rawAck, _ := json.Marshal(ack)
		wireAck, err := wireCompressor.Compress(rawAck)
		if err != nil {
			errs.Add(1)
			continue
		}
		if _, err := dealer.SendMessage("_control", wireAck); err != nil {
			errs.Add(1)
		}
	}
	return nil
}

func startHarnessBroker(ctx context.Context, cfg harnessConfig, topics []string) error {
	c := &appConfigShim{router: cfg.RouterAddr, pub: cfg.PubAddr}
	routes := make(map[string]string, len(topics))
	for _, topic := range topics {
		routes[topic] = "node-1"
	}
	runtime := app.NewBenchmarkRuntime(c.toConfig(), routes, cfg.Compress)
	return runtime.Router.Start(ctx)
}

type appConfigShim struct {
	router string
	pub    string
}

func (s *appConfigShim) toConfig() *config.Config {
	return &config.Config{
		ZmqBindAddress:         s.router,
		ZmqPubAddress:          s.pub,
		WALEnabled:             false,
		DeliveryTimeoutMS:      30000,
		MaxInflightPerConsumer: 1024,
		MaxPerTopicQueue:       256,
		MaxQueuedDirect:        4096,
		MaxGlobalIngress:       8192,
	}
}

func readUsageSnapshot() usageSnapshot {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return usageSnapshot{
		utime:       time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond,
		stime:       time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond,
		maxRSSBytes: uint64(ru.Maxrss) * 1024,
	}
}

func cpuPercent(start, end usageSnapshot, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	cpuUsed := (end.utime + end.stime) - (start.utime + start.stime)
	return (float64(cpuUsed) / float64(elapsed)) * 100.0
}

func printHarnessReport(r *harnessResult) {
	fmt.Println("--- AetherBus Tachyon Benchmark Harness ---")
	fmt.Printf("mode=%s payload_class=%s payload=%s compress=%v\n", r.cfg.Mode, r.cfg.PayloadClass, humanSize(r.cfg.PayloadSize), r.cfg.Compress)
	fmt.Printf("duration=%s connections=%d fanout_subs=%d mixed_topics=%d\n", r.cfg.Duration, r.cfg.Connections, r.cfg.FanoutSubs, r.cfg.MixedTopics)
	fmt.Printf("sent=%d recv=%d errors=%d elapsed=%s\n", r.sentMessages, r.recvMessages, r.errors, r.elapsed.Round(time.Millisecond))
	fmt.Printf("throughput_msgs_sec=%.2f throughput_mb_sec=%.2f\n", r.messagesPerSec, r.throughputMBps)
	fmt.Printf("latency_p50=%s latency_p95=%s latency_p99=%s\n", percentile(r.latencies, 50), percentile(r.latencies, 95), percentile(r.latencies, 99))
	fmt.Printf("cpu_percent=%.2f memory_rss=%s alloc_bytes_total=%s allocs_total=%d bytes_per_op=%.2f allocs_per_op=%.2f\n",
		r.cpuPercent,
		humanSize(int(r.memoryRSSBytes)),
		humanSize(int(r.totalAlloc)),
		r.mallocs,
		r.bytesPerOp,
		r.allocsPerOp,
	)
}
