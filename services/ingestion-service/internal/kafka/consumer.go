package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/config"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/datalake"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/model"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/validator"
	kafka "github.com/segmentio/kafka-go"
)

// Consumer consumes raw events from Kafka and stores them in the data lake.
type Consumer struct {
	reader    *kafka.Reader
	validator *validator.SchemaValidator
	dataLake  datalake.DataLakeStore
}

// ConsumerConfig holds configuration for the Kafka consumer.
type ConsumerConfig struct {
	Brokers   []string
	Topic     string
	GroupID   string
	Validator *validator.SchemaValidator
	DataLake  datalake.DataLakeStore
}

// NewConsumerWithConfig creates a new Kafka Consumer directly using the application Config pointer.
func NewConsumerWithConfig(cfg *config.Config, schemaValidator *validator.SchemaValidator, dataLake datalake.DataLakeStore) *Consumer {
	return NewConsumer(ConsumerConfig{
		Brokers:   cfg.Kafka.Brokers,
		Topic:     cfg.Kafka.RawTopic,
		GroupID:   cfg.Kafka.ConsumerGroup,
		Validator: schemaValidator,
		DataLake:  dataLake,
	})
}

// NewConsumer creates a new Kafka Consumer.
func NewConsumer(cfg ConsumerConfig) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6, // 10MB
		MaxWait:     50 * time.Millisecond,
	})

	return &Consumer{
		reader:    reader,
		validator: cfg.Validator,
		dataLake:  cfg.DataLake,
	}
}

// Start runs the consumer loop until context is canceled.
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("[consumer] Starting raw event consumer loop...")
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				log.Printf("[consumer] Context canceled, stopping raw event consumer loop")
				return nil
			}
			return fmt.Errorf("fetching message from raw topic: %w", err)
		}

		if err := c.processMessage(ctx, msg.Value); err != nil {
			log.Printf("[consumer] Error processing raw message (offset=%d): %v", msg.Offset, err)
			// Commit offset anyway to avoid blocking on bad/poison messages in POC
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("[consumer] Warning: failed to commit message offset %d: %v", msg.Offset, err)
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, data []byte) error {
	// 1. Validate against raw schema
	if c.validator != nil {
		if err := c.validator.ValidateRaw(data); err != nil {
			return fmt.Errorf("validating raw event schema: %w", err)
		}
	}

	// 2. Unmarshal into RawEvent
	var raw model.RawEvent
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("unmarshaling raw event: %w", err)
	}

	// 3. Store raw event in data lake for preservation and downstream processing
	savedPath, err := c.dataLake.Store(ctx, &raw, data)
	if err != nil {
		return fmt.Errorf("storing raw event in data lake (event_id=%s): %w", raw.EventID, err)
	}

	log.Printf("[consumer] Stored raw event in data lake: id=%s platform=%s path=%s", raw.EventID, raw.SourcePlatform, savedPath)
	return nil
}

// Close closes the underlying Kafka reader.
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("closing kafka reader: %w", err)
	}
	return nil
}
