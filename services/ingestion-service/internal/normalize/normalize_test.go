package normalize_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/model"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/normalize"
)

func TestNormalizer_Reddit(t *testing.T) {
	n := normalize.NewNormalizer()

	redditPayload, _ := json.Marshal(model.RedditPostPayload{
		ID:          "reddit-01",
		Subreddit:   "fashion",
		Author:      "fashionista",
		Title:       "Winter Trench Coat",
		Body:        "Check out this warm oversized coat",
		URL:         "https://example.com/coat.jpg",
		ImageURL:    "https://example.com/coat.jpg",
		Ups:         250,
		NumComments: 30,
	})

	raw := &model.RawEvent{
		EventID:    "raw-reddit-1",
		Source:     model.SourceReddit,
		SourceType: model.SourceTypePublicAPI,
		Timestamp:  time.Now().UTC(),
		Payload:    redditPayload,
	}

	norm, err := n.Normalize(raw)
	if err != nil {
		t.Fatalf("unexpected error normalizing reddit event: %v", err)
	}

	if norm.RawEventID != raw.EventID {
		t.Errorf("expected RawEventID=%s, got %s", raw.EventID, norm.RawEventID)
	}
	if norm.Author != "fashionista" {
		t.Errorf("expected Author=fashionista, got %s", norm.Author)
	}
	if norm.Text != "Winter Trench Coat\n\nCheck out this warm oversized coat" {
		t.Errorf("expected combined text, got %q", norm.Text)
	}
	if norm.ImageRef == nil || *norm.ImageRef != "https://example.com/coat.jpg" {
		t.Errorf("expected image ref set, got %v", norm.ImageRef)
	}
	if norm.EngagementCounts.Likes != 250 || norm.EngagementCounts.Comments != 30 {
		t.Errorf("unexpected engagement counts: %+v", norm.EngagementCounts)
	}
}

func TestNormalizer_Instagram(t *testing.T) {
	n := normalize.NewNormalizer()

	instaPayload, _ := json.Marshal(model.InstagramPostPayload{
		ID:            "insta-01",
		Username:      "tokyo_vibes",
		Caption:       "Street style Tokyo 2026 #tokyo",
		MediaURL:      "https://example.com/street.jpg",
		LikeCount:     1200,
		CommentsCount: 88,
		SharesCount:   20,
	})

	raw := &model.RawEvent{
		EventID:    "raw-insta-1",
		Source:     model.SourceInstagram,
		SourceType: model.SourceTypePublicAPI,
		Timestamp:  time.Now().UTC(),
		Payload:    instaPayload,
	}

	norm, err := n.Normalize(raw)
	if err != nil {
		t.Fatalf("unexpected error normalizing instagram event: %v", err)
	}

	if norm.Author != "tokyo_vibes" {
		t.Errorf("expected Author=tokyo_vibes, got %s", norm.Author)
	}
	if norm.Text != "Street style Tokyo 2026 #tokyo" {
		t.Errorf("expected caption as text, got %q", norm.Text)
	}
	if norm.EngagementCounts.Likes != 1200 || norm.EngagementCounts.Shares != 20 {
		t.Errorf("unexpected engagement counts: %+v", norm.EngagementCounts)
	}
}

func TestNormalizer_External(t *testing.T) {
	n := normalize.NewNormalizer()

	extPayload, _ := json.Marshal(model.ExternalEventPayload{
		Author:      "feed_observer",
		Text:        "High engagement on linen shirts",
		ImageURL:    "https://example.com/linen.jpg",
		Likes:       45,
		Comments:    5,
		Shares:      2,
		WatchTimeMs: 12000,
	})

	raw := &model.RawEvent{
		EventID:    "raw-ext-1",
		Source:     model.SourceExternal,
		SourceType: model.SourceTypeExternalIngested,
		Timestamp:  time.Now().UTC(),
		Payload:    extPayload,
	}

	norm, err := n.Normalize(raw)
	if err != nil {
		t.Fatalf("unexpected error normalizing external event: %v", err)
	}

	if norm.Author != "feed_observer" {
		t.Errorf("expected Author=feed_observer, got %s", norm.Author)
	}
	if norm.EngagementCounts.Likes != 45 {
		t.Errorf("expected 45 likes, got %d", norm.EngagementCounts.Likes)
	}
}
