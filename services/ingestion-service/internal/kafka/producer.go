package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/config"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/model"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/validator"
	kafka "github.com/segmentio/kafka-go"
)

// Producer handles publishing raw events to the raw Kafka topic (for REST fallback).
type Producer struct {
	rawWriter *kafka.Writer
	validator *validator.SchemaValidator
}

// ProducerConfig holds configuration for the Kafka producer.
type ProducerConfig struct {
	Brokers         []string
	RawTopic        string
	SchemaValidator *validator.SchemaValidator
}

// NewProducerWithConfig constructs a Kafka Producer directly using the application Config pointer.
func NewProducerWithConfig(cfg *config.Config, schemaValidator *validator.SchemaValidator) *Producer {
	return NewProducer(ProducerConfig{
		Brokers:         cfg.Kafka.Brokers,
		RawTopic:        cfg.Kafka.RawTopic,
		SchemaValidator: schemaValidator,
	})
}

// NewProducer constructs a new Kafka Producer instance.
func NewProducer(cfg ProducerConfig) *Producer {
	rawWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.RawTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10,
		Async:        false,
	}

	return &Producer{
		rawWriter: rawWriter,
		validator: cfg.SchemaValidator,
	}
}

// PublishRaw validates against the raw schema and produces to social.engagement.raw (used by REST fallback).
func (p *Producer) PublishRaw(ctx context.Context, event *model.RawEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling raw event: %w", err)
	}

	if p.validator != nil {
		if err := p.validator.ValidateRaw(payload); err != nil {
			return fmt.Errorf("pre-publish schema validation failed for raw event (event_id=%s): %w", event.EventID, err)
		}
	}

	msg := kafka.Message{
		Key:   []byte(event.EventID),
		Value: payload,
	}

	if err := p.rawWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("writing message to raw topic: %w", err)
	}

	log.Printf("[producer] Published raw event: id=%s platform=%s", event.EventID, event.SourcePlatform)
	return nil
}

// Close closes the underlying Kafka writer.
func (p *Producer) Close() error {
	if err := p.rawWriter.Close(); err != nil {
		return fmt.Errorf("closing raw writer: %w", err)
	}
	return nil
}
