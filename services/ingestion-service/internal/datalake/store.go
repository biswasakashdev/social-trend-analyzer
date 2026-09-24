package datalake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/config"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/model"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// DataLakeStore defines operations for preserving raw events in the data lake.
type DataLakeStore interface {
	Store(ctx context.Context, event *model.RawEvent, rawJSON []byte) (string, error)
}

// MinioConfig holds configuration for the MinIO object store.
type MinioConfig struct {
	Endpoint        string // e.g. "minio:9000" or "localhost:9000"
	AccessKeyID     string
	SecretAccessKey string
	BucketName      string
	UseSSL          bool
}

// MinioDataLakeStore stores raw events into MinIO object storage.
// Object Key pattern: {source_platform}/{YYYY-MM-DD}/{event_id}.json
type MinioDataLakeStore struct {
	client     *minio.Client
	bucketName string
}

// NewDataLakeStore initializes a DataLakeStore backed exclusively by MinIO object storage.
func NewDataLakeStore(ctx context.Context, cfg *config.Config) (DataLakeStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}

	if cfg.Minio.Endpoint == "" {
		return nil, fmt.Errorf("minio endpoint is required for data lake storage")
	}

	return NewMinioDataLakeStore(ctx, MinioConfig{
		Endpoint:        cfg.Minio.Endpoint,
		AccessKeyID:     cfg.Minio.AccessKeyID,
		SecretAccessKey: cfg.Minio.SecretAccessKey,
		BucketName:      cfg.Minio.BucketName,
		UseSSL:          cfg.Minio.UseSSL,
	})
}

// NewMinioDataLakeStoreWithConfig initializes MinioDataLakeStore using the MinioConfig pointer.
func NewMinioDataLakeStoreWithConfig(ctx context.Context, cfg *config.MinioConfig) (*MinioDataLakeStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("minio config is nil")
	}
	return NewMinioDataLakeStore(ctx, MinioConfig{
		Endpoint:        cfg.Endpoint,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		BucketName:      cfg.BucketName,
		UseSSL:          cfg.UseSSL,
	})
}

// NewMinioDataLakeStore initializes a MinIO client and creates the target bucket if needed.
func NewMinioDataLakeStore(ctx context.Context, cfg MinioConfig) (*MinioDataLakeStore, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("minio endpoint cannot be empty")
	}

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("initializing minio client for endpoint %s: %w", cfg.Endpoint, err)
	}

	bucket := cfg.BucketName
	if bucket == "" {
		bucket = "socialtrend-datalake"
	}

	// Check if bucket exists, create if not
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		log.Printf("[datalake] Warning: checking minio bucket %s: %v", bucket, err)
	} else if !exists {
		err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("creating minio bucket %s: %w", bucket, err)
		}
		log.Printf("[datalake] Created MinIO bucket: %s", bucket)
	} else {
		log.Printf("[datalake] Connected to existing MinIO bucket: %s", bucket)
	}

	return &MinioDataLakeStore{
		client:     client,
		bucketName: bucket,
	}, nil
}

// Store writes the raw event JSON into the MinIO bucket.
func (m *MinioDataLakeStore) Store(ctx context.Context, event *model.RawEvent, rawJSON []byte) (string, error) {
	if event == nil {
		return "", fmt.Errorf("event is nil")
	}

	dateStr := event.CollectedAt.Format("2006-01-02")
	if event.CollectedAt.IsZero() {
		dateStr = time.Now().UTC().Format("2006-01-02")
	}

	platform := event.SourcePlatform
	if platform == "" {
		platform = "unknown"
	}

	objectKey := fmt.Sprintf("%s/%s/%s.json", platform, dateStr, event.EventID)

	data := rawJSON
	if len(data) == 0 {
		var err error
		data, err = json.MarshalIndent(event, "", "  ")
		if err != nil {
			return "", fmt.Errorf("marshaling raw event: %w", err)
		}
	}

	reader := bytes.NewReader(data)
	_, err := m.client.PutObject(ctx, m.bucketName, objectKey, reader, int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return "", fmt.Errorf("uploading %s to minio bucket %s: %w", objectKey, m.bucketName, err)
	}

	return fmt.Sprintf("s3://%s/%s", m.bucketName, objectKey), nil
}
