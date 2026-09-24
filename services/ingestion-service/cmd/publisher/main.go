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
	restURL := flag.String("rest-url", "http://localhost:8081/ingest/events", "REST fallback endpoint URL")
	sourceFlag := flag.String("source", "all", "Sample source to publish: 'instagram', 'manual', or 'all'")
	countFlag := flag.Int("count", 1, "Number of times to publish the sample batch")
	flag.Parse()

	log.Printf("[publisher] Initializing test publisher (mode=%s, count=%d)...", *mode, *countFlag)

	samples := generateSamples(*sourceFlag)
	if len(samples) == 0 {
		log.Fatalf("[publisher] No samples matched source %q", *sourceFlag)
	}

	var publishedEventIDs []string

	for i := 0; i < *countFlag; i++ {
		for _, s := range samples {
			event := s
			event.EventID = fmt.Sprintf("test-%s-%s", event.SourcePlatform, uuid.New().String()[:8])
			event.CollectedAt = time.Now().UTC()

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
			log.Printf("[publisher] Successfully published raw event: id=%s platform=%s", event.EventID, event.SourcePlatform)
		}
	}

	log.Printf("[publisher] Finished publishing %d events", len(publishedEventIDs))
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

func generateSamples(source string) []model.RawEvent {
	caption1 := "Minimalist monochrome looks dominating Shibuya this week. What are your thoughts? #streetstyle #monochrome"
	caption2 := "Silk slip dress styled with chunky gold hoops for summer evenings #slipdress #fashion"
	creator1 := "tokyo_fashion_pulse"
	creator2 := "style_curator"
	url1 := "https://example.com/p/C987654321"
	url2 := "https://example.com/p/C123456789"
	likes1 := int64(1540)
	likes2 := int64(340)
	commentsCount1 := int64(112)
	commentsCount2 := int64(45)

	all := []model.RawEvent{
		{
			EventID:        "sample-instagram-1",
			CollectedAt:    time.Now().UTC(),
			SourcePlatform: "instagram",
			AccountSource:  "hashtag-search-adapter",
			Creator:        &creator1,
			ContentURL:     &url1,
			ContentType:    "reel",
			Captions:       &caption1,
			Hashtags:       []string{"streetstyle", "monochrome"},
			MediaURLs:      []string{"https://example.com/media/shibuya-monochrome.mp4"},
			LikesCount:     &likes1,
			CommentsCount:  &commentsCount1,
			Comments: []model.RawComment{
				{Author: "user_a", Text: "Love this clean look!"},
			},
		},
		{
			EventID:        "sample-instagram-2",
			CollectedAt:    time.Now().UTC(),
			SourcePlatform: "instagram",
			AccountSource:  "business-discovery-adapter",
			Creator:        &creator2,
			ContentURL:     &url2,
			ContentType:    "image",
			Captions:       &caption2,
			Hashtags:       []string{"slipdress", "fashion"},
			MediaURLs:      []string{"https://example.com/media/silk-slip.jpg"},
			LikesCount:     &likes2,
			CommentsCount:  &commentsCount2,
		},
		{
			EventID:        "sample-manual-1",
			CollectedAt:    time.Now().UTC(),
			SourcePlatform: "manual",
			AccountSource:  "curator-admin",
			ContentType:    "text",
			Captions:       &caption2,
		},
	}

	if source == "all" {
		return all
	}

	var filtered []model.RawEvent
	for _, s := range all {
		if s.SourcePlatform == source {
			filtered = append(filtered, s)
		}
	}
	return filtered
}
