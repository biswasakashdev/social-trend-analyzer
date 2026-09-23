package ingest

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/kafka"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/model"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/validator"
	"github.com/google/uuid"
)

// Handler handles HTTP ingestion fallback requests.
type Handler struct {
	validator *validator.SchemaValidator
	producer  *kafka.Producer
}

// NewHandler creates a new HTTP ingestion fallback handler.
func NewHandler(v *validator.SchemaValidator, p *kafka.Producer) *Handler {
	return &Handler{
		validator: v,
		producer:  p,
	}
}

// RegisterRoutes registers the ingestion HTTP routes on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /ingest/events", h.handleIngestEvents)
	mux.HandleFunc("GET /health", h.handleHealth)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (h *Handler) handleIngestEvents(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("reading request body: %v", err))
		return
	}
	defer r.Body.Close()

	if len(body) == 0 {
		writeError(w, http.StatusBadRequest, "request body cannot be empty")
		return
	}

	// 1. Validate payload against raw JSON schema
	if h.validator != nil {
		if err := h.validator.ValidateRaw(body); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("schema validation failed: %v", err))
			return
		}
	}

	// 2. Unmarshal into RawEvent
	var raw model.RawEvent
	if err := json.Unmarshal(body, &raw); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON payload: %v", err))
		return
	}

	if raw.EventID == "" {
		raw.EventID = uuid.New().String()
	}
	if raw.Timestamp.IsZero() {
		raw.Timestamp = time.Now().UTC()
	}

	// 3. Publish onto raw Kafka topic
	if h.producer != nil {
		if err := h.producer.PublishRaw(r.Context(), &raw); err != nil {
			log.Printf("[ingest] Failed to publish raw event (event_id=%s): %v", raw.EventID, err)
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("publishing raw event: %v", err))
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":   "accepted",
		"event_id": raw.EventID,
		"source":   raw.Source,
	})
}

func writeError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}
