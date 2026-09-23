package model

import (
	"encoding/json"
	"time"
)

// Source types defined in docs/project-requirements-doc.md
const (
	SourceReddit    = "reddit"
	SourceInstagram = "instagram"
	SourceExternal  = "external"

	SourceTypePublicAPI         = "public_api"
	SourceTypeExternalIngested  = "external_ingested"
	SourceTypeAccountAutomation = "account_automation"
)

// RawEvent represents an incoming un-normalized event on social.engagement.raw.
type RawEvent struct {
	EventID    string          `json:"event_id"`
	Source     string          `json:"source"`
	SourceType string          `json:"source_type"`
	AgentJobID *string         `json:"agent_job_id,omitempty"`
	ClientID   *string         `json:"client_id,omitempty"`
	Timestamp  time.Time       `json:"timestamp"`
	Payload    json.RawMessage `json:"payload"`
}

// RedditPostPayload represents payload structure from Reddit collections.
type RedditPostPayload struct {
	ID          string `json:"id"`
	Subreddit   string `json:"subreddit"`
	Author      string `json:"author"`
	Title       string `json:"title"`
	Body        string `json:"body"`
	URL         string `json:"url"`
	ImageURL    string `json:"image_url,omitempty"`
	Ups         int    `json:"ups"`
	NumComments int    `json:"num_comments"`
	CreatedUTC  int64  `json:"created_utc,omitempty"`
}

// InstagramPostPayload represents payload structure from Instagram collections.
type InstagramPostPayload struct {
	ID            string `json:"id"`
	Username      string `json:"username"`
	Caption       string `json:"caption"`
	MediaURL      string `json:"media_url,omitempty"`
	Permalink     string `json:"permalink,omitempty"`
	LikeCount     int    `json:"like_count"`
	CommentsCount int    `json:"comments_count"`
	SharesCount   int    `json:"shares_count,omitempty"`
}

// ExternalEventPayload represents payload structure from external ingestion feeds.
type ExternalEventPayload struct {
	Author      string `json:"author"`
	Text        string `json:"text"`
	ImageURL    string `json:"image_url,omitempty"`
	Likes       int    `json:"likes"`
	Comments    int    `json:"comments"`
	Shares      int    `json:"shares,omitempty"`
	WatchTimeMs int    `json:"watch_time_ms,omitempty"`
}

// EngagementCounts contains canonical engagement metrics.
type EngagementCounts struct {
	Likes    int `json:"likes"`
	Comments int `json:"comments"`
	Shares   int `json:"shares,omitempty"`
}

// NormalizedEvent is the canonical event structure published to social.engagement.normalized.
type NormalizedEvent struct {
	EventID          string           `json:"event_id"`
	RawEventID       string           `json:"raw_event_id"`
	Source           string           `json:"source"`
	SourceType       string           `json:"source_type"`
	AgentJobID       *string          `json:"agent_job_id"`
	Author           string           `json:"author"`
	Text             string           `json:"text"`
	ImageRef         *string          `json:"image_ref"`
	EngagementCounts EngagementCounts `json:"engagement_counts"`
	Timestamp        time.Time        `json:"timestamp"`
	NormalizedAt     time.Time        `json:"normalized_at"`
}
