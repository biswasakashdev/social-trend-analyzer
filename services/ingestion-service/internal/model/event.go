package model

import (
	"encoding/json"
	"time"
)

// RawEvent is a source-agnostic representation of a single piece of content
// and its engagement signals, as published by any producer (a platform
// adapter, a client agent, or a manual import) onto the raw ingestion topic.
// No field assumes a specific platform. Anything not present at collection
// time is nil/empty — normalization and categorization happen downstream,
// not here.
type RawEvent struct {
	EventID     string     `json:"event_id"`
	CollectedAt time.Time  `json:"collected_at"`          // when we ingested it
	OccurredAt  *time.Time `json:"occurred_at,omitempty"` // when the content was actually posted, if known

	// --- Provenance ---
	SourcePlatform string  `json:"source_platform"`   // e.g. "instagram", "youtube", "manual", "agent:trend-scanner" — free-form, not an enum
	AccountSource  string  `json:"account_source"`    // which of OUR app's users/agents/clients published this event
	Creator        *string `json:"creator,omitempty"` // the content's original author/owner, if known

	// --- Content identity ---
	ContentURL  *string  `json:"content_url,omitempty"`
	ContentType string   `json:"content_type"`         // "image" | "video" | "reel" | "carousel" | "text" | "audio" | ...
	Captions    *string  `json:"captions,omitempty"`
	Hashtags    []string `json:"hashtags,omitempty"`
	MediaURLs   []string `json:"media_urls,omitempty"` // supports carousels / multi-image posts

	// --- Engagement signals (all optional — not every source has all of these) ---
	LikesCount     *int64           `json:"likes_count,omitempty"`
	CommentsCount  *int64           `json:"comments_count,omitempty"`
	SharesCount    *int64           `json:"shares_count,omitempty"`    // reshares/reposts
	ViewsCount     *int64           `json:"views_count,omitempty"`
	ReactionCounts map[string]int64 `json:"reaction_counts,omitempty"` // e.g. {"like":120,"sad":4,"angry":1,"laugh":9}
	Comments       []RawComment     `json:"comments,omitempty"`        // present only if the source exposes comment text

	// --- Attention signals (rare — most sources won't provide these) ---
	AvgWatchTimeSeconds  *float64 `json:"avg_watch_time_seconds,omitempty"`
	ContentLengthSeconds *float64 `json:"content_length_seconds,omitempty"` // duration, for video/audio

	// --- Escape hatch ---
	Payload json.RawMessage `json:"payload,omitempty"` // raw source-specific fields that don't map cleanly above
}

// RawComment represents comment details when provided by a source.
type RawComment struct {
	Author string `json:"author,omitempty"`
	Text   string `json:"text"`
	Likes  *int64 `json:"likes,omitempty"`
}
