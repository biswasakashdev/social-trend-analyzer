package datalake

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/config"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/model"
)

func TestMinioDataLakeStore_Store(t *testing.T) {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:9000"
	}

	cfg := MinioConfig{
		Endpoint:        endpoint,
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
		BucketName:      "test-datalake",
		UseSSL:          false,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	store, err := NewMinioDataLakeStore(ctx, cfg)
	if err != nil {
		t.Skipf("skipping MinIO test, could not connect to %s: %v", endpoint, err)
		return
	}

	collectedAt := time.Date(2026, 9, 24, 14, 30, 0, 0, time.UTC)
	event := &model.RawEvent{
		EventID:        "evt-minio-test-1",
		CollectedAt:    collectedAt,
		SourcePlatform: "instagram",
		AccountSource:  "client-unit-test",
		ContentType:    "image",
	}

	rawJSON := []byte(`{"event_id":"evt-minio-test-1","source_platform":"instagram","account_source":"client-unit-test","content_type":"image"}`)

	s3URI, err := store.Store(ctx, event, rawJSON)
	if err != nil {
		t.Fatalf("failed to store in MinIO: %v", err)
	}

	expectedURI := "s3://test-datalake/instagram/2026-09-24/evt-minio-test-1.json"
	if s3URI != expectedURI {
		t.Errorf("expected S3 URI %s, got %s", expectedURI, s3URI)
	}
}

func TestNewDataLakeStore_WithAppConfig(t *testing.T) {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:9000"
	}

	appCfg := &config.Config{
		Minio: config.MinioConfig{
			Endpoint:        endpoint,
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
			BucketName:      "test-datalake",
			UseSSL:          false,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	store, err := NewDataLakeStore(ctx, appCfg)
	if err != nil {
		t.Skipf("skipping MinIO test, could not connect to %s: %v", endpoint, err)
		return
	}

	if store == nil {
		t.Fatal("expected non-nil DataLakeStore")
	}
}

func TestNewDataLakeStore_MissingEndpoint(t *testing.T) {
	appCfg := &config.Config{
		Minio: config.MinioConfig{
			Endpoint: "",
		},
	}

	_, err := NewDataLakeStore(context.Background(), appCfg)
	if err == nil {
		t.Fatal("expected error when MinIO endpoint is empty")
	}
}
