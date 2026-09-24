package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	// Set CONTRACTS_KAFKA_DIR to repo contracts directory for test resolution
	repoContractsDir := filepath.Join("..", "..", "..", "..", "contracts", "kafka")
	t.Setenv("CONTRACTS_KAFKA_DIR", repoContractsDir)
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_RAW_TOPIC", "")
	t.Setenv("KAFKA_CONSUMER_GROUP", "")
	t.Setenv("MINIO_ENDPOINT", "")
	t.Setenv("HTTP_PORT", "")

	cfg, err := NewFromEnv()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Brokers[0] != "localhost:9092" {
		t.Errorf("expected default brokers [localhost:9092], got %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.RawTopic != "social.engagement.raw" {
		t.Errorf("expected default raw topic social.engagement.raw, got %s", cfg.Kafka.RawTopic)
	}
	if cfg.Kafka.ConsumerGroup != "ingestion-service-group" {
		t.Errorf("expected default consumer group ingestion-service-group, got %s", cfg.Kafka.ConsumerGroup)
	}
	if cfg.Minio.Endpoint != "localhost:9000" {
		t.Errorf("expected default minio endpoint localhost:9000, got %s", cfg.Minio.Endpoint)
	}
	if !cfg.Minio.Enabled() {
		t.Errorf("expected minio to be enabled by default")
	}
	if cfg.Server.Port != "8081" {
		t.Errorf("expected default port 8081, got %s", cfg.Server.Port)
	}
}

func TestLoad_CustomEnv(t *testing.T) {
	repoContractsDir := filepath.Join("..", "..", "..", "..", "contracts", "kafka")
	t.Setenv("CONTRACTS_KAFKA_DIR", repoContractsDir)
	t.Setenv("KAFKA_BROKERS", "broker1:9092, broker2:9092")
	t.Setenv("KAFKA_RAW_TOPIC", "custom.raw")
	t.Setenv("KAFKA_CONSUMER_GROUP", "custom-group")
	t.Setenv("MINIO_ENDPOINT", "minio.local:9000")
	t.Setenv("MINIO_ACCESS_KEY", "customaccess")
	t.Setenv("MINIO_SECRET_KEY", "customsecret")
	t.Setenv("MINIO_BUCKET", "custom-bucket")
	t.Setenv("MINIO_USE_SSL", "true")
	t.Setenv("HTTP_PORT", "9999")

	cfg, err := NewFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(cfg.Kafka.Brokers) != 2 || cfg.Kafka.Brokers[0] != "broker1:9092" || cfg.Kafka.Brokers[1] != "broker2:9092" {
		t.Errorf("unexpected brokers: %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.RawTopic != "custom.raw" {
		t.Errorf("unexpected raw topic: %s", cfg.Kafka.RawTopic)
	}
	if !cfg.Minio.Enabled() {
		t.Errorf("expected minio to be enabled")
	}
	if cfg.Minio.Endpoint != "minio.local:9000" {
		t.Errorf("unexpected endpoint: %s", cfg.Minio.Endpoint)
	}
	if !cfg.Minio.UseSSL {
		t.Errorf("expected UseSSL true")
	}
	if cfg.Server.Port != "9999" {
		t.Errorf("unexpected port: %s", cfg.Server.Port)
	}
}

func TestLoad_EnvFile(t *testing.T) {
	repoContractsDir := filepath.Join("..", "..", "..", "..", "contracts", "kafka")
	tmpDir := t.TempDir()
	envPath := filepath.Join(tmpDir, ".env")

	envContent := `
CONTRACTS_KAFKA_DIR=` + repoContractsDir + `
KAFKA_BROKERS=kafka-env:9092
MINIO_ENDPOINT=minio-env:9000
MINIO_BUCKET=bucket-from-env
HTTP_PORT=7777
`
	if err := os.WriteFile(envPath, []byte(envContent), 0644); err != nil {
		t.Fatalf("writing temp env file: %v", err)
	}

	cfg, err := Load(envPath)
	if err != nil {
		t.Fatalf("loading env file failed: %v", err)
	}

	if cfg.Kafka.Brokers[0] != "kafka-env:9092" {
		t.Errorf("expected broker kafka-env:9092, got %s", cfg.Kafka.Brokers[0])
	}
	if cfg.Minio.Endpoint != "minio-env:9000" {
		t.Errorf("expected minio endpoint minio-env:9000, got %s", cfg.Minio.Endpoint)
	}
	if cfg.Minio.BucketName != "bucket-from-env" {
		t.Errorf("expected bucket bucket-from-env, got %s", cfg.Minio.BucketName)
	}
	if cfg.Server.Port != "7777" {
		t.Errorf("expected port 7777, got %s", cfg.Server.Port)
	}
}
