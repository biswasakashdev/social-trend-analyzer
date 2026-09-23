package normalize

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/model"
	"github.com/google/uuid"
)

// Normalizer transforms raw social events into canonical normalized events.
type Normalizer struct{}

// NewNormalizer creates a new Normalizer instance.
func NewNormalizer() *Normalizer {
	return &Normalizer{}
}

// Normalize processes a raw event into a canonical NormalizedEvent.
func (n *Normalizer) Normalize(raw *model.RawEvent) (*model.NormalizedEvent, error) {
	if raw == nil {
		return nil, fmt.Errorf("normalizing event: raw event cannot be nil")
	}

	norm := &model.NormalizedEvent{
		EventID:      uuid.New().String(),
		RawEventID:   raw.EventID,
		Source:       raw.Source,
		SourceType:   raw.SourceType,
		AgentJobID:   raw.AgentJobID,
		Timestamp:    raw.Timestamp,
		NormalizedAt: time.Now().UTC(),
	}

	switch raw.Source {
	case model.SourceReddit:
		if err := normalizeReddit(raw.Payload, norm); err != nil {
			return nil, fmt.Errorf("normalizing reddit event (raw_event_id=%s): %w", raw.EventID, err)
		}
	case model.SourceInstagram:
		if err := normalizeInstagram(raw.Payload, norm); err != nil {
			return nil, fmt.Errorf("normalizing instagram event (raw_event_id=%s): %w", raw.EventID, err)
		}
	case model.SourceExternal:
		if err := normalizeExternal(raw.Payload, norm); err != nil {
			return nil, fmt.Errorf("normalizing external event (raw_event_id=%s): %w", raw.EventID, err)
		}
	default:
		// Attempt generic normalization
		if err := normalizeGeneric(raw.Payload, norm); err != nil {
			return nil, fmt.Errorf("normalizing unknown source %q (raw_event_id=%s): %w", raw.Source, raw.EventID, err)
		}
	}

	if norm.Author == "" {
		norm.Author = "anonymous"
	}

	return norm, nil
}

func normalizeReddit(payloadBytes []byte, target *model.NormalizedEvent) error {
	var p model.RedditPostPayload
	if err := json.Unmarshal(payloadBytes, &p); err != nil {
		return fmt.Errorf("unmarshaling reddit payload: %w", err)
	}

	target.Author = p.Author
	if strings.TrimSpace(p.Body) != "" {
		target.Text = fmt.Sprintf("%s\n\n%s", strings.TrimSpace(p.Title), strings.TrimSpace(p.Body))
	} else {
		target.Text = strings.TrimSpace(p.Title)
	}

	if p.ImageURL != "" {
		img := p.ImageURL
		target.ImageRef = &img
	} else if isImageURL(p.URL) {
		img := p.URL
		target.ImageRef = &img
	}

	target.EngagementCounts = model.EngagementCounts{
		Likes:    max(0, p.Ups),
		Comments: max(0, p.NumComments),
		Shares:   0,
	}

	return nil
}

func normalizeInstagram(payloadBytes []byte, target *model.NormalizedEvent) error {
	var p model.InstagramPostPayload
	if err := json.Unmarshal(payloadBytes, &p); err != nil {
		return fmt.Errorf("unmarshaling instagram payload: %w", err)
	}

	target.Author = p.Username
	target.Text = strings.TrimSpace(p.Caption)

	if p.MediaURL != "" {
		img := p.MediaURL
		target.ImageRef = &img
	}

	target.EngagementCounts = model.EngagementCounts{
		Likes:    max(0, p.LikeCount),
		Comments: max(0, p.CommentsCount),
		Shares:   max(0, p.SharesCount),
	}

	return nil
}

func normalizeExternal(payloadBytes []byte, target *model.NormalizedEvent) error {
	var p model.ExternalEventPayload
	if err := json.Unmarshal(payloadBytes, &p); err != nil {
		return fmt.Errorf("unmarshaling external payload: %w", err)
	}

	target.Author = p.Author
	target.Text = strings.TrimSpace(p.Text)

	if p.ImageURL != "" {
		img := p.ImageURL
		target.ImageRef = &img
	}

	target.EngagementCounts = model.EngagementCounts{
		Likes:    max(0, p.Likes),
		Comments: max(0, p.Comments),
		Shares:   max(0, p.Shares),
	}

	return nil
}

func normalizeGeneric(payloadBytes []byte, target *model.NormalizedEvent) error {
	var m map[string]any
	if err := json.Unmarshal(payloadBytes, &m); err != nil {
		return fmt.Errorf("unmarshaling generic payload: %w", err)
	}

	if author, ok := m["author"].(string); ok {
		target.Author = author
	}
	if text, ok := m["text"].(string); ok {
		target.Text = text
	} else if title, ok := m["title"].(string); ok {
		target.Text = title
	}
	if img, ok := m["image_ref"].(string); ok && img != "" {
		target.ImageRef = &img
	} else if img, ok := m["image_url"].(string); ok && img != "" {
		target.ImageRef = &img
	}

	var likes, comments, shares int
	if l, ok := m["likes"].(float64); ok {
		likes = int(l)
	}
	if c, ok := m["comments"].(float64); ok {
		comments = int(c)
	}
	if s, ok := m["shares"].(float64); ok {
		shares = int(s)
	}

	target.EngagementCounts = model.EngagementCounts{
		Likes:    max(0, likes),
		Comments: max(0, comments),
		Shares:   max(0, shares),
	}

	return nil
}

func isImageURL(u string) bool {
	lower := strings.ToLower(u)
	return strings.HasSuffix(lower, ".jpg") ||
		strings.HasSuffix(lower, ".jpeg") ||
		strings.HasSuffix(lower, ".png") ||
		strings.HasSuffix(lower, ".webp")
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
