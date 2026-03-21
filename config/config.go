package config

import (
	"os"
	"sort"
	"strconv"
	"strings"
)

// Config holds the application configuration.
// Values are populated from environment variables.
type Config struct {
	ZmqBindAddress           string
	ZmqPubAddress            string
	DeliveryTimeoutMS        int
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
}

// Load reads configuration from environment variables and returns a new Config struct.
func Load() (*Config, error) {
	cfg := &Config{
		ZmqBindAddress:           getenvOrDefault("ZMQ_BIND_ADDRESS", "tcp://127.0.0.1:5555"),
		ZmqPubAddress:            getenvOrDefault("ZMQ_PUB_ADDRESS", "tcp://127.0.0.1:5556"),
		DeliveryTimeoutMS:        getenvIntOrDefault("DELIVERY_TIMEOUT_MS", 30000),
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
	for i, class := range classes {
		defaults[class] = len(classes) - i
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
