package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// KafkaConfig holds Kafka connection and consumer/producer settings.
type KafkaConfig struct {
	Brokers       []string
	RawTopic      string
	ConsumerGroup string
}

// MinioConfig holds MinIO object store configuration.
type MinioConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	BucketName      string
	UseSSL          bool
}

// Enabled reports whether MinIO endpoint is configured.
func (m *MinioConfig) Enabled() bool {
	return m != nil && strings.TrimSpace(m.Endpoint) != ""
}

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Port string
}

// Config represents the complete application configuration for ingestion-service.
type Config struct {
	Kafka        KafkaConfig
	Minio        MinioConfig
	Server       ServerConfig
	ContractsDir string
}

// Load loads configuration from .env file (if available) and system environment variables.
// If explicit envFiles are provided, they are loaded in order.
// If no files are passed, it checks default candidate paths (.env, ../.env, ../../.env).
func Load(envFiles ...string) (*Config, error) {
	if len(envFiles) > 0 {
		if err := godotenv.Load(envFiles...); err != nil {
			return nil, fmt.Errorf("loading env file: %w", err)
		}
	} else {
		candidates := []string{".env", "../.env", "../../.env"}
		for _, path := range candidates {
			if fi, err := os.Stat(path); err == nil && !fi.IsDir() {
				if err := godotenv.Load(path); err == nil {
					log.Printf("[config] Loaded environment from %s", path)
					break
				}
			}
		}
	}

	return NewFromEnv()
}

// NewFromEnv constructs a Config struct from the current environment variables.
func NewFromEnv() (*Config, error) {
	brokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	var brokers []string
	for b := range strings.SplitSeq(brokersStr, ",") {
		if trimmed := strings.TrimSpace(b); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}

	useSSL, _ := strconv.ParseBool(getEnv("MINIO_USE_SSL", "false"))

	contractsDir, err := resolveContractsDir()
	if err != nil {
		return nil, fmt.Errorf("resolving contracts directory: %w", err)
	}

	return &Config{
		Kafka: KafkaConfig{
			Brokers:       brokers,
			RawTopic:      getEnv("KAFKA_RAW_TOPIC", "social.engagement.raw"),
			ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "ingestion-service-group"),
		},
		Minio: MinioConfig{
			Endpoint:        getEnv("MINIO_ENDPOINT", "localhost:9000"),
			AccessKeyID:     getEnv("MINIO_ACCESS_KEY", "minioadmin"),
			SecretAccessKey: getEnv("MINIO_SECRET_KEY", "minioadmin"),
			BucketName:      getEnv("MINIO_BUCKET", "socialtrend-datalake"),
			UseSSL:          useSSL,
		},
		Server: ServerConfig{
			Port: getEnv("HTTP_PORT", "8081"),
		},
		ContractsDir: contractsDir,
	}, nil
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func resolveContractsDir() (string, error) {
	candidates := []string{
		os.Getenv("CONTRACTS_KAFKA_DIR"),
		"../../../../contracts/kafka",
		"../../../contracts/kafka",
		"../../contracts/kafka",
		"../contracts/kafka",
		"contracts/kafka",
		"/contracts/kafka",
	}

	for _, c := range candidates {
		if c == "" {
			continue
		}
		if fi, err := os.Stat(c); err == nil && fi.IsDir() {
			return c, nil
		}
	}

	return "", fmt.Errorf("contracts directory not found in candidates: %v", candidates)
}
