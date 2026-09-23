package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/ingest"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/kafka"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/normalize"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/validator"
)

func main() {
	log.Println("[main] Starting ingestion-service...")

	brokersEnv := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersEnv, ",")
	rawTopic := getEnv("KAFKA_RAW_TOPIC", "social.engagement.raw")
	normalizedTopic := getEnv("KAFKA_NORMALIZED_TOPIC", "social.engagement.normalized")
	consumerGroup := getEnv("KAFKA_CONSUMER_GROUP", "ingestion-service-group")
	httpPort := getEnv("HTTP_PORT", "8081")

	contractsDir, err := resolveContractsDir()
	if err != nil {
		log.Fatalf("[main] Failed to resolve contracts directory: %v", err)
	}
	log.Printf("[main] Using contracts directory: %s", contractsDir)

	// 1. Initialize schema validator
	schemaValidator, err := validator.NewSchemaValidator(contractsDir)
	if err != nil {
		log.Fatalf("[main] Failed to initialize schema validator: %v", err)
	}
	log.Println("[main] JSON Schema validator loaded successfully")

	// 2. Initialize normalizer
	normalizer := normalize.NewNormalizer()

	// 3. Initialize Kafka producer
	producer := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:         brokers,
		NormalizedTopic: normalizedTopic,
		RawTopic:        rawTopic,
		SchemaValidator: schemaValidator,
	})
	defer producer.Close()

	// 4. Initialize Kafka consumer
	consumer := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:    brokers,
		Topic:      rawTopic,
		GroupID:    consumerGroup,
		Normalizer: normalizer,
		Validator:  schemaValidator,
		Producer:   producer,
	})
	defer consumer.Close()

	// 5. Context with cancellation on SIGINT/SIGTERM
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 6. Start consumer loop in background
	consumerErrCh := make(chan error, 1)
	go func() {
		if err := consumer.Start(ctx); err != nil {
			consumerErrCh <- fmt.Errorf("consumer terminated with error: %w", err)
		}
	}()

	// 7. Setup REST fallback HTTP server
	mux := http.NewServeMux()
	ingestHandler := ingest.NewHandler(schemaValidator, producer)
	ingestHandler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:         ":" + httpPort,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	httpErrCh := make(chan error, 1)
	go func() {
		log.Printf("[main] REST fallback server listening on :%s", httpPort)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErrCh <- fmt.Errorf("HTTP server error: %w", err)
		}
	}()

	// Wait for shutdown signal or fatal error
	select {
	case <-ctx.Done():
		log.Println("[main] Shutdown signal received, initiating graceful shutdown...")
	case err := <-consumerErrCh:
		log.Printf("[main] Fatal consumer error: %v", err)
		cancel()
	case err := <-httpErrCh:
		log.Printf("[main] Fatal HTTP error: %v", err)
		cancel()
	}

	// Graceful HTTP server shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[main] HTTP server shutdown error: %v", err)
	}

	log.Println("[main] ingestion-service stopped cleanly.")
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func resolveContractsDir() (string, error) {
	candidates := []string{
		os.Getenv("CONTRACTS_KAFKA_DIR"),
		"../../contracts/kafka",
		"../contracts/kafka",
		"contracts/kafka",
		"/contracts/kafka",
	}

	for _, c := range candidates {
		if c == "" {
			continue
		}
		if fi, err := os.Stat(c); err == nil && fi.IsDir() {
			return c, nil
		}
	}

	return "", fmt.Errorf("contracts directory not found in candidates: %v", candidates)
}
