package config

import "os"
import "strconv"

// Config holds the application configuration.
// Values are populated from environment variables.
type Config struct {
	ZmqBindAddress    string
	ZmqPubAddress     string
	DeliveryTimeoutMS int
}

// Load reads configuration from environment variables and returns a new Config struct.
func Load() (*Config, error) {
	cfg := &Config{
		ZmqBindAddress:    getenvOrDefault("ZMQ_BIND_ADDRESS", "tcp://127.0.0.1:5555"),
		ZmqPubAddress:     getenvOrDefault("ZMQ_PUB_ADDRESS", "tcp://127.0.0.1:5556"),
		DeliveryTimeoutMS: getenvIntOrDefault("DELIVERY_TIMEOUT_MS", 30000),
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
