package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/config"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/datalake"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/ingest"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/kafka"
	"github.com/biswasakashdev/social-trends/services/ingestion-service/internal/validator"
)

func main() {
	log.Println("[main] Starting ingestion-service...")

	// Load application configuration (from .env if present and system environment)
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[main] Failed to load configuration: %v", err)
	}

	// Context with cancellation on SIGINT/SIGTERM
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("[main] Using contracts directory: %s", cfg.ContractsDir)

	// 1. Initialize schema validator for raw events
	schemaValidator, err := validator.NewSchemaValidator(cfg.ContractsDir)
	if err != nil {
		log.Fatalf("[main] Failed to initialize schema validator: %v", err)
	}
	log.Println("[main] Raw JSON Schema validator loaded successfully")

	// 2. Initialize Data Lake store using config pointer (MinIO S3 object store)
	dataLakeStore, err := datalake.NewDataLakeStore(ctx, cfg)
	if err != nil {
		log.Fatalf("[main] Failed to initialize MinIO data lake store: %v", err)
	}
	log.Printf("[main] Connected to MinIO Data Lake at %s (bucket: %s)", cfg.Minio.Endpoint, cfg.Minio.BucketName)

	// 3. Initialize Kafka producer using config pointer (used exclusively by REST fallback to publish to raw topic)
	producer := kafka.NewProducerWithConfig(cfg, schemaValidator)
	defer producer.Close()

	// 4. Initialize Kafka consumer using config pointer (collects raw data and writes to Data Lake)
	consumer := kafka.NewConsumerWithConfig(cfg, schemaValidator, dataLakeStore)
	defer consumer.Close()

	// 5. Start consumer loop in background
	consumerErrCh := make(chan error, 1)
	go func() {
		if err := consumer.Start(ctx); err != nil {
			consumerErrCh <- fmt.Errorf("consumer terminated with error: %w", err)
		}
	}()

	// 6. Setup REST fallback HTTP server
	mux := http.NewServeMux()
	ingestHandler := ingest.NewHandler(schemaValidator, producer)
	ingestHandler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	httpErrCh := make(chan error, 1)
	go func() {
		log.Printf("[main] REST fallback server listening on :%s", cfg.Server.Port)
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
