package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/model"
	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

func main() {
	mode := flag.String("mode", "kafka", "Publish mode: 'kafka' (direct to topic) or 'rest' (via POST /ingest/events)")
	brokersFlag := flag.String("brokers", "localhost:9092", "Kafka brokers comma-separated")
	rawTopic := flag.String("raw-topic", "social.engagement.raw", "Raw Kafka topic")
	normalizedTopic := flag.String("norm-topic", "social.engagement.normalized", "Normalized Kafka topic to verify from")
	restURL := flag.String("rest-url", "http://localhost:8081/ingest/events", "REST fallback endpoint URL")
	sourceFlag := flag.String("source", "all", "Sample source to publish: 'reddit', 'instagram', 'external', or 'all'")
	countFlag := flag.Int("count", 1, "Number of times to publish the sample batch")
	verifyFlag := flag.Bool("verify", false, "Wait and verify that published events appear on the normalized topic")
	flag.Parse()

	log.Printf("[publisher] Initializing test publisher (mode=%s, count=%d)...", *mode, *countFlag)

	samples := generateSamples(*sourceFlag)
	if len(samples) == 0 {
		log.Fatalf("[publisher] No samples matched source %q", *sourceFlag)
	}

	var publishedEventIDs []string

	for i := 0; i < *countFlag; i++ {
		for _, s := range samples {
			// Give each published sample a unique event ID and fresh timestamp
			event := s
			event.EventID = fmt.Sprintf("test-%s-%s", event.Source, uuid.New().String()[:8])
			event.Timestamp = time.Now().UTC()

			if *mode == "rest" {
				if err := publishViaREST(*restURL, event); err != nil {
					log.Fatalf("[publisher] Failed to publish via REST: %v", err)
				}
			} else {
				brokers := strings.Split(*brokersFlag, ",")
				if err := publishViaKafka(brokers, *rawTopic, event); err != nil {
					log.Fatalf("[publisher] Failed to publish via Kafka: %v", err)
				}
			}
			publishedEventIDs = append(publishedEventIDs, event.EventID)
			log.Printf("[publisher] Successfully published raw event: id=%s source=%s", event.EventID, event.Source)
		}
	}

	log.Printf("[publisher] Finished publishing %d events", len(publishedEventIDs))

	if *verifyFlag {
		brokers := strings.Split(*brokersFlag, ",")
		log.Printf("[publisher] Verifying that published events appear on topic %q...", *normalizedTopic)
		if err := verifyNormalizedEvents(brokers, *normalizedTopic, publishedEventIDs, 15*time.Second); err != nil {
			log.Fatalf("[publisher] Verification failed: %v", err)
		}
		log.Println("[publisher] Verification SUCCEEDED! All events verified on normalized topic.")
	}
}

func publishViaKafka(brokers []string, topic string, event model.RawEvent) error {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling event: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.EventID),
		Value: payload,
	})
}

func publishViaREST(url string, event model.RawEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling event: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("creating HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("executing HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func verifyNormalizedEvents(brokers []string, topic string, expectedRawIDs []string, timeout time.Duration) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     fmt.Sprintf("verifier-%s", uuid.New().String()),
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	remaining := make(map[string]bool)
	for _, id := range expectedRawIDs {
		remaining[id] = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for len(remaining) > 0 {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("reading message while waiting for %d events: %w", len(remaining), err)
		}

		var norm model.NormalizedEvent
		if err := json.Unmarshal(msg.Value, &norm); err != nil {
			continue
		}

		if remaining[norm.RawEventID] {
			log.Printf("[verifier] Verified normalized event received! ID=%s (RawID=%s, Source=%s, Likes=%d, Comments=%d, ImageRef=%v)",
				norm.EventID, norm.RawEventID, norm.Source, norm.EngagementCounts.Likes, norm.EngagementCounts.Comments, derefString(norm.ImageRef))
			delete(remaining, norm.RawEventID)
		}
	}

	return nil
}

func derefString(s *string) string {
	if s == nil {
		return "<none>"
	}
	return *s
}

func generateSamples(source string) []model.RawEvent {
	jobID := "job-fashion-demo"
	clientID := "collector-client-01"

	redditPayload, _ := json.Marshal(model.RedditPostPayload{
		ID:          "reddit-post-987",
		Subreddit:   "streetwear",
		Author:      "style_curator",
		Title:       "Oversized earth-tone trench coats are taking over this autumn",
		Body:        "Seeing massive engagement around neutral wool silhouettes and layered textures.",
		URL:         "https://example.com/media/trench-coat-autumn.jpg",
		ImageURL:    "https://example.com/media/trench-coat-autumn.jpg",
		Ups:         482,
		NumComments: 63,
		CreatedUTC:  time.Now().Unix(),
	})

	instagramPayload, _ := json.Marshal(model.InstagramPostPayload{
		ID:            "insta-media-554",
		Username:      "tokyo_fashion_pulse",
		Caption:       "Minimalist monochrome looks dominating Shibuya this week. What are your thoughts? #streetstyle #monochrome",
		MediaURL:      "https://example.com/media/shibuya-monochrome.jpg",
		Permalink:     "https://instagram.com/p/C987654321",
		LikeCount:     1540,
		CommentsCount: 112,
		SharesCount:   45,
	})

	externalPayload, _ := json.Marshal(model.ExternalEventPayload{
		Author:      "feed_watcher_agent",
		Text:        "High dwell time recorded on sustainable linen summer dress collections across lifestyle feeds.",
		ImageURL:    "https://example.com/media/sustainable-linen-dress.jpg",
		Likes:       320,
		Comments:    28,
		Shares:      14,
		WatchTimeMs: 45000,
	})

	all := []model.RawEvent{
		{
			EventID:    "sample-reddit",
			Source:     model.SourceReddit,
			SourceType: model.SourceTypePublicAPI,
			AgentJobID: &jobID,
			Timestamp:  time.Now().UTC(),
			Payload:    redditPayload,
		},
		{
			EventID:    "sample-instagram",
			Source:     model.SourceInstagram,
			SourceType: model.SourceTypePublicAPI,
			AgentJobID: &jobID,
			Timestamp:  time.Now().UTC(),
			Payload:    instagramPayload,
		},
		{
			EventID:    "sample-external",
			Source:     model.SourceExternal,
			SourceType: model.SourceTypeExternalIngested,
			AgentJobID: &jobID,
			ClientID:   &clientID,
			Timestamp:  time.Now().UTC(),
			Payload:    externalPayload,
		},
	}

	if source == "all" {
		return all
	}

	var filtered []model.RawEvent
	for _, s := range all {
		if s.Source == source {
			filtered = append(filtered, s)
		}
	}
	return filtered
}
