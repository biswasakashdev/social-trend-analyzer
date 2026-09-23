package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/model"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/validator"
	kafka "github.com/segmentio/kafka-go"
)

// Producer handles publishing validated events to Kafka topics.
type Producer struct {
	normalizedWriter *kafka.Writer
	rawWriter        *kafka.Writer
	validator        *validator.SchemaValidator
}

// ProducerConfig holds configuration for the Kafka producer.
type ProducerConfig struct {
	Brokers          []string
	NormalizedTopic  string
	RawTopic         string
	SchemaValidator  *validator.SchemaValidator
}

// NewProducer constructs a new Kafka Producer instance.
func NewProducer(cfg ProducerConfig) *Producer {
	normalizedWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.NormalizedTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10,
		Async:        false,
	}

	rawWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.RawTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10,
		Async:        false,
	}

	return &Producer{
		normalizedWriter: normalizedWriter,
		rawWriter:        rawWriter,
		validator:        cfg.SchemaValidator,
	}
}

// PublishNormalized validates against the normalized schema and produces to social.engagement.normalized.
func (p *Producer) PublishNormalized(ctx context.Context, event *model.NormalizedEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling normalized event: %w", err)
	}

	// Hard constraint Rule 22: Every Kafka message must validate against its schema before being sent
	if p.validator != nil {
		if err := p.validator.ValidateNormalized(payload); err != nil {
			return fmt.Errorf("pre-publish schema validation failed for normalized event (event_id=%s): %w", event.EventID, err)
		}
	}

	msg := kafka.Message{
		Key:   []byte(event.EventID),
		Value: payload,
	}

	if err := p.normalizedWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("writing message to normalized topic: %w", err)
	}

	log.Printf("[producer] Published normalized event: id=%s source=%s raw_id=%s", event.EventID, event.Source, event.RawEventID)
	return nil
}

// PublishRaw validates against the raw schema and produces to social.engagement.raw (used by REST fallback).
func (p *Producer) PublishRaw(ctx context.Context, event *model.RawEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling raw event: %w", err)
	}

	// Hard constraint Rule 22: Every Kafka message must validate against its schema before being sent
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

	log.Printf("[producer] Published raw event: id=%s source=%s", event.EventID, event.Source)
	return nil
}

// Close closes the underlying Kafka writers.
func (p *Producer) Close() error {
	var firstErr error
	if err := p.normalizedWriter.Close(); err != nil {
		firstErr = fmt.Errorf("closing normalized writer: %w", err)
	}
	if err := p.rawWriter.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("closing raw writer: %w", err)
	}
	return firstErr
}
