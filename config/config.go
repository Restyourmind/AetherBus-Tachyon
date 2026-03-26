package config

import (
	"os"
	"sort"
	"strconv"
	"strings"
)

// QueueLimitPolicyConfig defines runtime tuning controls for direct queue limits.
type QueueLimitPolicyConfig struct {
	Enabled                     bool
	EvaluationIntervalMS        int
	MinHoldTimeMS               int
	MemoryLimitBytes            uint64
	RetryRateHighWatermark      float64
	QueueGrowthHighWatermark    float64
	ConsumerLagHighWatermark    int
	MemoryPressureHighWatermark float64
	InflightStep                int
	QueueStep                   int
	MinInflightPerConsumer      int
	MaxInflightPerConsumer      int
	MinPerTopicQueue            int
	MaxPerTopicQueue            int
	AdaptivePriorityWeights     bool
	AdaptiveStep                int
	AgingBoostAfterMS           int
	ClassCircuitBreaker         bool
	ClassBreakerQueueFraction   float64
}

// Config holds the application configuration.
type Config struct {
	ZmqBindAddress           string
	ZmqPubAddress            string
	DeliveryTimeoutMS        int
	WALSegmentMaxBytes       int
	WALDurabilityMode        string
	WALReplicationMirrorDir  string
	MaxInflightPerConsumer   int
	MaxPerTopicQueue         int
	MaxQueuedDirect          int
	MaxGlobalIngress         int
	WALEnabled               bool
	WALPath                  string
	RouteCatalogPath         string
	FastpathSidecarEnabled   bool
	FastpathSocketPath       string
	FastpathCutoverBytes     int
	FastpathRequire          bool
	FastpathFallbackToGo     bool
	SupportedPriorityClasses []string
	PriorityClassWeights     map[string]int
	PriorityPreemption       bool
	PriorityBoostThreshold   int
	PriorityBoostOffset      int
	QueueLimitPolicy         QueueLimitPolicyConfig
}

// Load reads configuration from environment variables and returns a new Config struct.
func Load() (*Config, error) {
	cfg := &Config{
		ZmqBindAddress:           getenvOrDefault("ZMQ_BIND_ADDRESS", "tcp://127.0.0.1:5555"),
		ZmqPubAddress:            getenvOrDefault("ZMQ_PUB_ADDRESS", "tcp://127.0.0.1:5556"),
		DeliveryTimeoutMS:        getenvIntOrDefault("DELIVERY_TIMEOUT_MS", 30000),
		WALSegmentMaxBytes:       getenvIntOrDefault("WAL_SEGMENT_MAX_BYTES", 4*1024*1024),
		WALDurabilityMode:        getenvOrDefault("WAL_DURABILITY_MODE", "local_fsync"),
		WALReplicationMirrorDir:  getenvOrDefault("WAL_REPLICATION_MIRROR_DIR", ""),
		MaxInflightPerConsumer:   getenvIntOrDefault("MAX_INFLIGHT_PER_CONSUMER", 1024),
		MaxPerTopicQueue:         getenvIntOrDefault("MAX_PER_TOPIC_QUEUE", 256),
		MaxQueuedDirect:          getenvIntOrDefault("MAX_QUEUED_DIRECT", 4096),
		MaxGlobalIngress:         getenvIntOrDefault("MAX_GLOBAL_INGRESS", 8192),
		WALEnabled:               getenvBoolOrDefault("WAL_ENABLED", false),
		WALPath:                  getenvOrDefault("WAL_PATH", "./data/direct_delivery.wal"),
		RouteCatalogPath:         getenvOrDefault("ROUTE_CATALOG_PATH", "./data/routes.catalog.json"),
		FastpathSidecarEnabled:   getenvBoolOrDefault("FASTPATH_SIDECAR_ENABLED", false),
		FastpathSocketPath:       getenvOrDefault("FASTPATH_SOCKET_PATH", "/tmp/tachyon-fastpath.sock"),
		FastpathCutoverBytes:     getenvIntOrDefault("FASTPATH_CUTOVER_BYTES", 256*1024),
		FastpathRequire:          getenvBoolOrDefault("FASTPATH_REQUIRE", false),
		FastpathFallbackToGo:     getenvBoolOrDefault("FASTPATH_FALLBACK_TO_GO", true),
		SupportedPriorityClasses: getenvCSVOrDefault("DIRECT_PRIORITY_CLASSES", []string{"urgent", "high", "normal", "low"}),
		PriorityPreemption:       getenvBoolOrDefault("DIRECT_PRIORITY_PREEMPTION", true),
		PriorityBoostThreshold:   getenvIntOrDefault("DIRECT_PRIORITY_BOOST_THRESHOLD", 8),
		PriorityBoostOffset:      getenvIntOrDefault("DIRECT_PRIORITY_BOOST_OFFSET", 1000),
	}
	cfg.PriorityClassWeights = getenvPriorityWeightsOrDefault("DIRECT_PRIORITY_WEIGHTS", cfg.SupportedPriorityClasses)
	cfg.QueueLimitPolicy = QueueLimitPolicyConfig{
		Enabled:                     getenvBoolOrDefault("QUEUE_POLICY_ENABLED", false),
		EvaluationIntervalMS:        getenvIntOrDefault("QUEUE_POLICY_EVALUATION_INTERVAL_MS", 2000),
		MinHoldTimeMS:               getenvIntOrDefault("QUEUE_POLICY_MIN_HOLD_MS", 5000),
		MemoryLimitBytes:            getenvUint64OrDefault("QUEUE_POLICY_MEMORY_LIMIT_BYTES", 512*1024*1024),
		RetryRateHighWatermark:      getenvFloatOrDefault("QUEUE_POLICY_RETRY_RATE_HIGH_WATERMARK", 4.0),
		QueueGrowthHighWatermark:    getenvFloatOrDefault("QUEUE_POLICY_QUEUE_GROWTH_HIGH_WATERMARK", 32.0),
		ConsumerLagHighWatermark:    getenvIntOrDefault("QUEUE_POLICY_CONSUMER_LAG_HIGH_WATERMARK", 256),
		MemoryPressureHighWatermark: getenvFloatOrDefault("QUEUE_POLICY_MEMORY_PRESSURE_HIGH_WATERMARK", 0.85),
		InflightStep:                getenvIntOrDefault("QUEUE_POLICY_INFLIGHT_STEP", 32),
		QueueStep:                   getenvIntOrDefault("QUEUE_POLICY_QUEUE_STEP", 32),
		MinInflightPerConsumer:      getenvIntOrDefault("QUEUE_POLICY_MIN_INFLIGHT_PER_CONSUMER", 128),
		MaxInflightPerConsumer:      getenvIntOrDefault("QUEUE_POLICY_MAX_INFLIGHT_PER_CONSUMER", cfg.MaxInflightPerConsumer),
		MinPerTopicQueue:            getenvIntOrDefault("QUEUE_POLICY_MIN_PER_TOPIC_QUEUE", 64),
		MaxPerTopicQueue:            getenvIntOrDefault("QUEUE_POLICY_MAX_PER_TOPIC_QUEUE", cfg.MaxPerTopicQueue),
		AdaptivePriorityWeights:     getenvBoolOrDefault("QUEUE_POLICY_ADAPTIVE_PRIORITY_WEIGHTS", true),
		AdaptiveStep:                getenvIntOrDefault("QUEUE_POLICY_ADAPTIVE_STEP", 1),
		AgingBoostAfterMS:           getenvIntOrDefault("QUEUE_POLICY_AGING_BOOST_AFTER_MS", 15000),
		ClassCircuitBreaker:         getenvBoolOrDefault("QUEUE_POLICY_CLASS_CIRCUIT_BREAKER", true),
		ClassBreakerQueueFraction:   getenvFloatOrDefault("QUEUE_POLICY_CLASS_BREAKER_QUEUE_FRACTION", 0.8),
	}

	return cfg, nil
}

func getenvIntOrDefault(key string, defaultValue int) int {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return defaultValue
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return defaultValue
	}

	return parsed
}

func getenvOrDefault(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}

	return defaultValue
}

func getenvBoolOrDefault(key string, defaultValue bool) bool {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return defaultValue
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue
	}

	return parsed
}

func getenvCSVOrDefault(key string, defaultValue []string) []string {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return append([]string(nil), defaultValue...)
	}
	parts := strings.Split(value, ",")
	seen := map[string]struct{}{}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.ToLower(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	if len(out) == 0 {
		return append([]string(nil), defaultValue...)
	}
	return out
}

func getenvPriorityWeightsOrDefault(key string, classes []string) map[string]int {
	defaults := make(map[string]int, len(classes))
	allowed := make(map[string]struct{}, len(classes))
	for i, class := range classes {
		defaults[class] = len(classes) - i
		allowed[class] = struct{}{}
	}
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return defaults
	}
	weights := make(map[string]int, len(defaults))
	for class, weight := range defaults {
		weights[class] = weight
	}
	for _, part := range strings.Split(value, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		class := strings.ToLower(strings.TrimSpace(kv[0]))
		if class == "" {
			continue
		}
		if _, ok := allowed[class]; !ok {
			continue
		}
		weight, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil || weight <= 0 {
			continue
		}
		weights[class] = weight
	}
	keys := make([]string, 0, len(weights))
	for class := range weights {
		keys = append(keys, class)
	}
	sort.Strings(keys)
	normalized := make(map[string]int, len(keys))
	for _, class := range keys {
		normalized[class] = weights[class]
	}
	return normalized
}

func getenvUint64OrDefault(key string, defaultValue uint64) uint64 {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 {
		return defaultValue
	}
	return parsed
}

func getenvFloatOrDefault(key string, defaultValue float64) float64 {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return defaultValue
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 {
		return defaultValue
	}
	return parsed
}
