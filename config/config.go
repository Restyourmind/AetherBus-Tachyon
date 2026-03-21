package config

import "os"
import "strconv"

// Config holds the application configuration.
// Values are populated from environment variables.
type Config struct {
	ZmqBindAddress         string
	ZmqPubAddress          string
	DeliveryTimeoutMS      int
	MaxInflightPerConsumer int
	MaxPerTopicQueue       int
	MaxQueuedDirect        int
	MaxGlobalIngress       int
	WALEnabled             bool
	WALPath                string
	RouteCatalogPath       string
	FastpathSidecarEnabled bool
	FastpathSocketPath     string
	FastpathCutoverBytes   int
	FastpathRequire        bool
	FastpathFallbackToGo   bool
}

// Load reads configuration from environment variables and returns a new Config struct.
func Load() (*Config, error) {
	cfg := &Config{
		ZmqBindAddress:         getenvOrDefault("ZMQ_BIND_ADDRESS", "tcp://127.0.0.1:5555"),
		ZmqPubAddress:          getenvOrDefault("ZMQ_PUB_ADDRESS", "tcp://127.0.0.1:5556"),
		DeliveryTimeoutMS:      getenvIntOrDefault("DELIVERY_TIMEOUT_MS", 30000),
		MaxInflightPerConsumer: getenvIntOrDefault("MAX_INFLIGHT_PER_CONSUMER", 1024),
		MaxPerTopicQueue:       getenvIntOrDefault("MAX_PER_TOPIC_QUEUE", 256),
		MaxQueuedDirect:        getenvIntOrDefault("MAX_QUEUED_DIRECT", 4096),
		MaxGlobalIngress:       getenvIntOrDefault("MAX_GLOBAL_INGRESS", 8192),
		WALEnabled:             getenvBoolOrDefault("WAL_ENABLED", false),
		WALPath:                getenvOrDefault("WAL_PATH", "./data/direct_delivery.wal"),
		RouteCatalogPath:       getenvOrDefault("ROUTE_CATALOG_PATH", "./data/routes.catalog.json"),
		FastpathSidecarEnabled: getenvBoolOrDefault("FASTPATH_SIDECAR_ENABLED", false),
		FastpathSocketPath:     getenvOrDefault("FASTPATH_SOCKET_PATH", "/tmp/tachyon-fastpath.sock"),
		FastpathCutoverBytes:   getenvIntOrDefault("FASTPATH_CUTOVER_BYTES", 256*1024),
		FastpathRequire:        getenvBoolOrDefault("FASTPATH_REQUIRE", false),
		FastpathFallbackToGo:   getenvBoolOrDefault("FASTPATH_FALLBACK_TO_GO", true),
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
