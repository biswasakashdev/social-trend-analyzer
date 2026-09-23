package validator_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/model"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/validator"
)

func TestSchemaValidator_RawEvent(t *testing.T) {
	v, err := validator.NewSchemaValidator("../../../../contracts/kafka")
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	validRaw := model.RawEvent{
		EventID:    "raw-123",
		Source:     "reddit",
		SourceType: "public_api",
		Timestamp:  time.Now().UTC(),
		Payload:    json.RawMessage(`{"title":"hello world","ups":10,"num_comments":2}`),
	}
	validBytes, err := json.Marshal(validRaw)
	if err != nil {
		t.Fatalf("marshaling valid raw event: %v", err)
	}

	if err := v.ValidateRaw(validBytes); err != nil {
		t.Errorf("expected valid raw event to pass validation, got: %v", err)
	}

	// Missing required source_type
	invalidRaw := map[string]any{
		"event_id":  "raw-123",
		"source":    "reddit",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   map[string]any{"title": "hello"},
	}
	invalidBytes, _ := json.Marshal(invalidRaw)
	if err := v.ValidateRaw(invalidBytes); err == nil {
		t.Errorf("expected validation to fail for missing source_type")
	}

	// Invalid source enum value
	invalidSource := map[string]any{
		"event_id":    "raw-123",
		"source":      "unsupported_network",
		"source_type": "public_api",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"payload":     map[string]any{"title": "hello"},
	}
	invalidSourceBytes, _ := json.Marshal(invalidSource)
	if err := v.ValidateRaw(invalidSourceBytes); err == nil {
		t.Errorf("expected validation to fail for invalid source enum")
	}
}

func TestSchemaValidator_NormalizedEvent(t *testing.T) {
	v, err := validator.NewSchemaValidator("../../../../contracts/kafka")
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	validNorm := model.NormalizedEvent{
		EventID:    "norm-456",
		RawEventID: "raw-123",
		Source:     "reddit",
		SourceType: "public_api",
		Author:     "tester",
		Text:       "Clean trench coat styling",
		EngagementCounts: model.EngagementCounts{
			Likes:    100,
			Comments: 15,
			Shares:   5,
		},
		Timestamp:    time.Now().UTC(),
		NormalizedAt: time.Now().UTC(),
	}
	validBytes, err := json.Marshal(validNorm)
	if err != nil {
		t.Fatalf("marshaling valid normalized event: %v", err)
	}

	if err := v.ValidateNormalized(validBytes); err != nil {
		t.Errorf("expected valid normalized event to pass, got: %v", err)
	}

	// Missing text
	invalidNorm := map[string]any{
		"event_id":     "norm-456",
		"raw_event_id": "raw-123",
		"source":       "reddit",
		"source_type":  "public_api",
		"author":       "tester",
		"engagement_counts": map[string]any{
			"likes":    10,
			"comments": 2,
		},
		"timestamp":     time.Now().UTC().Format(time.RFC3339),
		"normalized_at": time.Now().UTC().Format(time.RFC3339),
	}
	invalidBytes, _ := json.Marshal(invalidNorm)
	if err := v.ValidateNormalized(invalidBytes); err == nil {
		t.Errorf("expected validation to fail for missing text")
	}
}
