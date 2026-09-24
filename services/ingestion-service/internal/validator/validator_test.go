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
		EventID:        "raw-123",
		CollectedAt:    time.Now().UTC(),
		SourcePlatform: "instagram",
		AccountSource:  "client-trend-app",
		ContentType:    "image",
		Payload:        json.RawMessage(`{"extra":"value"}`),
	}
	validBytes, err := json.Marshal(validRaw)
	if err != nil {
		t.Fatalf("marshaling valid raw event: %v", err)
	}

	if err := v.ValidateRaw(validBytes); err != nil {
		t.Errorf("expected valid raw event to pass validation, got: %v", err)
	}

	// Missing required content_type
	invalidRaw := map[string]any{
		"event_id":        "raw-123",
		"collected_at":    time.Now().UTC().Format(time.RFC3339),
		"source_platform": "instagram",
		"account_source":  "client-1",
	}
	invalidBytes, _ := json.Marshal(invalidRaw)
	if err := v.ValidateRaw(invalidBytes); err == nil {
		t.Errorf("expected validation to fail for missing content_type")
	}

	// Missing required account_source
	missingAccount := map[string]any{
		"event_id":        "raw-123",
		"collected_at":    time.Now().UTC().Format(time.RFC3339),
		"source_platform": "instagram",
		"content_type":    "video",
	}
	missingAccountBytes, _ := json.Marshal(missingAccount)
	if err := v.ValidateRaw(missingAccountBytes); err == nil {
		t.Errorf("expected validation to fail for missing account_source")
	}
}
