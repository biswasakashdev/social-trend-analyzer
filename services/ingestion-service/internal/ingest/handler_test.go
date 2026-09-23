package ingest_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/ingest"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/model"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/validator"
)

func TestHandler_Health(t *testing.T) {
	h := ingest.NewHandler(nil, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte(`"status":"ok"`)) {
		t.Errorf("expected status ok in body, got %s", rec.Body.String())
	}
}

func TestHandler_IngestEvents_Valid(t *testing.T) {
	v, err := validator.NewSchemaValidator("../../../../contracts/kafka")
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	h := ingest.NewHandler(v, nil) // nil producer for unit testing handler logic
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	raw := model.RawEvent{
		EventID:    "test-event-1",
		Source:     model.SourceReddit,
		SourceType: model.SourceTypePublicAPI,
		Timestamp:  time.Now().UTC(),
		Payload:    json.RawMessage(`{"title":"Sample Post","ups":10,"num_comments":2}`),
	}
	rawBytes, _ := json.Marshal(raw)

	req := httptest.NewRequest(http.MethodPost, "/ingest/events", bytes.NewReader(rawBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshaling response: %v", err)
	}
	if resp["status"] != "accepted" {
		t.Errorf("expected status=accepted, got %v", resp["status"])
	}
	if resp["event_id"] != "test-event-1" {
		t.Errorf("expected event_id=test-event-1, got %v", resp["event_id"])
	}
}

func TestHandler_IngestEvents_Invalid(t *testing.T) {
	v, err := validator.NewSchemaValidator("../../../../contracts/kafka")
	if err != nil {
		t.Fatalf("failed to create validator: %v", err)
	}

	h := ingest.NewHandler(v, nil)
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	// Missing required source_type
	invalid := map[string]any{
		"event_id":  "test-bad",
		"source":    "reddit",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   map[string]any{"title": "Missing source type"},
	}
	invalidBytes, _ := json.Marshal(invalid)

	req := httptest.NewRequest(http.MethodPost, "/ingest/events", bytes.NewReader(invalidBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 for invalid schema, got %d: %s", rec.Code, rec.Body.String())
	}
}
