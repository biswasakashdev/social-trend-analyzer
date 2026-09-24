# ingestion-service — Context for Agents

## Purpose
The ingestion worker — Go. Its sole purpose is to consume raw engagement events from the raw Kafka topic (`social.engagement.raw`), validate them against the raw JSON schema, and store them directly into a Data Lake for preservation and downstream processing. It does **NOT** publish data into multiple Kafka topics.

## Reads
- Kafka topic `social.engagement.raw`
  — schema: `/contracts/kafka/social.engagement.raw.schema.json`
- REST fallback: `POST /ingest/events` — for upstream sources that cannot publish to Kafka directly. Validates incoming payloads against the raw schema and republishes onto `social.engagement.raw`, ensuring a unified ingestion flow.

## Writes
- **Data Lake Storage**: Preserves raw events exclusively into MinIO S3 object storage (bucket: `socialtrend-datalake`, key: `{source_platform}/{YYYY-MM-DD}/{event_id}.json`).
- Kafka topic `social.engagement.raw` (write access used exclusively by the REST fallback handler).
- Does **NOT** publish to normalized or downstream Kafka topics.

## Environment Variables
- `KAFKA_BROKERS`: Kafka broker list (default: `localhost:9092`)
- `KAFKA_RAW_TOPIC`: Raw engagement topic (default: `social.engagement.raw`)
- `KAFKA_CONSUMER_GROUP`: Consumer group ID (default: `ingestion-service-group`)
- `MINIO_ENDPOINT`: MinIO S3 API host:port (default: `localhost:9000` or `minio:9000`)
- `MINIO_ACCESS_KEY`: MinIO access key (default: `minioadmin`)
- `MINIO_SECRET_KEY`: MinIO secret key (default: `minioadmin`)
- `MINIO_BUCKET`: MinIO bucket name (default: `socialtrend-datalake`)
- `MINIO_USE_SSL`: Whether to use SSL for MinIO (default: `false`)
- `HTTP_PORT`: REST fallback server port (default: `8081`)

## Does NOT own
- Publishing events to multiple Kafka topics or intermediate messaging queues.
- Cleaning, category classification, or vision/sentiment interpretation — those are downstream tasks performed by `ai-service` reading from MinIO.
- Trend aggregation or agent job orchestration — that's `core-api-service`.
- Operating real upstream collectors or scrapers — it only ingests data published to `social.engagement.raw` or `POST /ingest/events`.

## Project Layout
```
cmd/
├── ingestion/main.go      # entrypoint (wires consumer, MinIO datalake, REST fallback)
└── publisher/main.go      # test fixture publishing sample raw events
internal/
├── config/                # config.go (loads .env and env vars into Config struct), config_test.go
├── datalake/              # store.go (MinioDataLakeStore), store_test.go
├── ingest/                # handler.go (REST fallback endpoint POST /ingest/events)
├── kafka/                 # consumer.go (reads raw, stores in datalake), producer.go (publishes raw for REST)
├── model/                 # event.go (platform-agnostic RawEvent struct)
└── validator/             # validator.go (validates against /contracts/kafka/social.engagement.raw.schema.json)
```

See `docs/rules.md` §3 for Go conventions: wrapped errors
(`fmt.Errorf("...: %w", err)`), no global mutable state, every goroutine
stoppable via `context.Context` cancellation.

## Run Standalone
```bash
docker-compose up kafka minio   # from repo root
cd services/ingestion-service
go run ./cmd/ingestion
```

## Test
```
cd services/ingestion-service
go test ./...
```
