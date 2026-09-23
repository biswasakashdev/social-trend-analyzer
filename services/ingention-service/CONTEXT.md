# ingestion-service — Context for Agents

## Purpose
The ingestion/normalization worker — Go. Consumes raw engagement events from
whatever upstream source publishes them, validates and normalizes them into
the platform's common schema, and republishes them for `ai-service` to
analyze. Stateless — holds no data of its own beyond what's in-flight.

## Reads
- Kafka topic `social.engagement.raw`
  — schema: `/contracts/kafka/social.engagement.raw.schema.json`
- REST fallback: `POST /ingest/events` — for upstream sources that can't
  publish to Kafka directly. Accepted requests are validated against the same
  `raw` schema and republished onto the `raw` topic, so everything downstream
  of validation is a single path regardless of how data arrived.

## Writes
- Kafka topic `social.engagement.normalized`
  — schema: `/contracts/kafka/social.engagement.normalized.schema.json`

## Does NOT own
- Sentiment or image/vision analysis — that's `ai-service`. This service only
  normalizes shape (field names, types, required fields); it does not
  interpret content.
- Trend aggregation, agent jobs, or content suggestions — that's
  `core-api-service`.
- **Building or operating any real upstream collector.** This project
  consumes data via the `raw` topic / `POST /ingest/events`; it does not
  build the app or scraper that produces that data (see
  `docs/requirements.md` §8, Out of Scope). For local development and
  demoing, a small standalone script publishes sample events onto `raw` —
  that script is a test fixture, not a real collector, and should stay
  clearly labeled as such if it lives in this repo.

## Project Layout
```
cmd/ingestion/main.go     # entrypoint
internal/
├── kafka/                # consumer.go (reads raw), producer.go (writes normalized)
├── normalize/            # maps varied upstream shapes → the normalized schema
├── ingest/               # REST fallback handler
└── model/                # types matching /contracts/kafka schemas
```
See `docs/rules.md` §3 for Go conventions: wrapped errors
(`fmt.Errorf("...: %w", err)`), no global mutable state, every goroutine
stoppable via `context.Context` cancellation.

## Functional Requirements This Service Implements
FR1–FR3 in `docs/requirements.md` §5 (Kafka consumption, REST fallback,
normalization/validation).

## Run Standalone
```
docker-compose up kafka   # from repo root
cd services/ingestion-service
go run ./cmd/ingestion
```

## Test
```
cd services/ingestion-service
go test ./...
```
