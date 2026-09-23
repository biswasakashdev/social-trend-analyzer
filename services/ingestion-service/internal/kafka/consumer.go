package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

	"time"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/model"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/normalize"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/validator"
	kafka "github.com/segmentio/kafka-go"
)

// Consumer consumes raw events, validates, normalizes, and publishes them.
type Consumer struct {
	reader     *kafka.Reader
	normalizer *normalize.Normalizer
	validator  *validator.SchemaValidator
	producer   *Producer
}

// ConsumerConfig holds configuration for the Kafka consumer.
type ConsumerConfig struct {
	Brokers    []string
	Topic      string
	GroupID    string
	Normalizer *normalize.Normalizer
	Validator  *validator.SchemaValidator
	Producer   *Producer
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
		reader:     reader,
		normalizer: cfg.Normalizer,
		validator:  cfg.Validator,
		producer:   cfg.Producer,
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

	// 3. Normalize
	norm, err := c.normalizer.Normalize(&raw)
	if err != nil {
		return fmt.Errorf("normalizing raw event (event_id=%s): %w", raw.EventID, err)
	}

	// 4. Publish normalized event (PublishNormalized performs validation against normalized schema)
	if err := c.producer.PublishNormalized(ctx, norm); err != nil {
		return fmt.Errorf("publishing normalized event: %w", err)
	}

	return nil
}

// Close closes the underlying Kafka reader.
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("closing kafka reader: %w", err)
	}
	return nil
}
