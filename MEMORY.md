# Project Memory — Social Media Trend Intelligence Platform

> **Notice for AI Agents**: This file serves as a persistent cross-session status tracker and memory log for all AI coding agents working on this repository (Antigravity, Cursor, Claude Code, etc.). Always read [AGENTS.md](AGENTS.md), [docs/rules.md](docs/rules.md), and [docs/phases.md](docs/phases.md) before proposing or implementing changes.

---

## 1. Project Phase Tracker

| Phase | Description | Owner Service / Stack | Status | Exit Criterion Met |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 0** | Foundation & Contracts | Monorepo / Multi-service | **Completed** | Kafka, Postgres, MinIO, service skeletons, and contracts in place |
| **Phase 1** | Ingestion Skeleton & MinIO Lake | `ingestion-service` (Go) | **Completed** | Raw events consumed from `social.engagement.raw`, validated, and preserved into MinIO Data Lake |
| **Phase 2** | AI Service Core | `ai-service` (Python FastAPI) | **Pending / Up Next** | Raw events read from MinIO, cleaned/categorized, sentiment + vision attributes extracted, `/generate` endpoint ready |
| **Phase 3** | Core API Foundations | `core-api-service` (Java / Spring) | Pending | AgentJob CRUD + TrendSnapshot persistence in Postgres |
| **Phase 4** | Trend Aggregation & Suggestions | `core-api-service` / `ai-service` | Pending | Trend ranking computed and suggestion generated via `/generate` |
| **Phase 5** | Frontend | `frontend` (Next.js / React) | Pending | Full UI flow operational without hitting API manually |
| **Phase 6** | End-to-End Demo | All services | Pending | Complete single-workflow demo from seed data to UI review |
| **Phase 7** | Expansion & Multi-job | Multi-service | Pending | Multiple concurrent jobs & additional data providers |

---

## 2. Completed Jobs & Milestones

### 1. Phase 1 Completed (`ingestion-service`)
- **Service**: [services/ingestion-service](services/ingestion-service) (Go 1.24+)
- **Functional Requirements Implemented**: `FR1`, `FR2`, `FR3`
- **Core Components**:
  - **Kafka Consumer**: [services/ingestion-service/internal/kafka/consumer.go](services/ingestion-service/internal/kafka/consumer.go) consumes `social.engagement.raw` with clean context shutdown.
  - **Schema Validation**: [services/ingestion-service/internal/validator/validator.go](services/ingestion-service/internal/validator/validator.go) validates incoming payloads against [contracts/kafka/social.engagement.raw.schema.json](contracts/kafka/social.engagement.raw.schema.json).
  - **Data Lake Store**: [services/ingestion-service/internal/datalake/store.go](services/ingestion-service/internal/datalake/store.go) exclusively preserves raw event records into MinIO S3 object storage (`MinioDataLakeStore`).
  - **Event Models**: [services/ingestion-service/internal/model/event.go](services/ingestion-service/internal/model/event.go) defines platform-agnostic `RawEvent` and `RawComment`.
  - **REST Fallback Endpoint**: [services/ingestion-service/internal/ingest/handler.go](services/ingestion-service/internal/ingest/handler.go) exposes `POST /ingest/events` on port `8081` and republishes incoming payloads to `social.engagement.raw`.
  - **Producer**: [services/ingestion-service/internal/kafka/producer.go](services/ingestion-service/internal/kafka/producer.go) publishes raw events onto `social.engagement.raw` exclusively for REST fallback.
  - **Synthetic Test Publisher**: [services/ingestion-service/cmd/publisher/main.go](services/ingestion-service/cmd/publisher/main.go) provides a standalone tool to emit synthetic raw events to Kafka for testing pipeline flow.
  - **Automated Tests**: Unit test suite covering validator, datalake store (including live MinIO integration), and REST handler (`go test ./...`) all pass.
  - **Dockerfile**: [services/ingestion-service/Dockerfile](services/ingestion-service/Dockerfile) configured for multi-stage building.

### 2. Kafka Raw Schema Aligned with Model
- **Contract**: [contracts/kafka/social.engagement.raw.schema.json](contracts/kafka/social.engagement.raw.schema.json)
- Matches `model.RawEvent` in [services/ingestion-service/internal/model/event.go](services/ingestion-service/internal/model/event.go) exactly:
  - Required fields: `event_id`, `collected_at`, `source_platform`, `account_source`, `content_type`.
  - Nullable/optional fields: `occurred_at`, `content_url`, `author_id`, `creator`, `hashtags`, `media_urls`, `metrics`, `comments`, and escape hatch `payload`.
- Validator unit tests pass against representative events.

### 3. MinIO S3 Data Lake Container & Integration
- **Container**: `socialtrend-minio` running via `quay.io/minio/minio:latest`.
- **Ports**: `9000` (S3 API), `9001` (Web Console).
- **Default Bucket**: `socialtrend-datalake` (auto-created on startup).
- **Key Partition Layout**: `{source_platform}/{YYYY-MM-DD}/{event_id}.json`.
- **Client**: `github.com/minio/minio-go/v7` integrated into [services/ingestion-service/internal/datalake/store.go](services/ingestion-service/internal/datalake/store.go).
- **Service Wires**: [services/ingestion-service/cmd/ingestion/main.go](services/ingestion-service/cmd/ingestion/main.go) connects to MinIO using `MINIO_ENDPOINT` (defaults to `localhost:9000` or `minio:9000`).

### 4. Docker Compose & Root Makefile
- **Compose**: [docker-compose.yml](docker-compose.yml) includes:
  - `minio` (quay.io/minio/minio:latest on `9000:9000`, `9001:9001` with volume `minio-data` and healthcheck)
  - `kafka` (Apache Kafka 3.8.0 on `9092:9092` with healthcheck)
  - `postgres` (Postgres 16 Alpine on `5432:5432` with healthcheck and volume `postgres-data`)
  - `ingestion-service` (configured with MinIO and Kafka dependencies and env vars)
- **Makefile**: [Makefile](Makefile)
  - `ingestion-build-image`: Builds the ingestion container image from the repository root tagged `biswasakash/socialtrend-ingestion:latest`.

### 5. Centralized Configuration Package (`internal/config`)
- **Package**: [services/ingestion-service/internal/config](services/ingestion-service/internal/config)
- **Features**:
  - Employs `github.com/joho/godotenv` to load `.env` files with candidate fallback search.
  - Defines `Config` struct aggregating all configuration sub-domains: `KafkaConfig`, `MinioConfig`, `ServerConfig`, and `ContractsDir`.
  - Application components utilize `*config.Config` pointer directly for instantiation:
    - `datalake.NewDataLakeStore(ctx, cfg)`
    - `kafka.NewProducerWithConfig(cfg, validator)`
    - `kafka.NewConsumerWithConfig(cfg, validator, dataLakeStore)`
  - Added [.env.example](services/ingestion-service/.env.example) for local development setup.
  - Comprehensive unit test coverage in [services/ingestion-service/internal/config/config_test.go](services/ingestion-service/internal/config/config_test.go).

---

## 3. Key Architecture & Operational Context

### Kafka Topics & Contracts
- `social.engagement.raw`: [contracts/kafka/social.engagement.raw.schema.json](contracts/kafka/social.engagement.raw.schema.json) (only raw data topic contract lives in `contracts/kafka/`).

### Data Lake (MinIO)
- **Provider**: Self-hosted MinIO running in Docker container `socialtrend-minio`.
- **Endpoint**: `localhost:9000` (external/host) / `minio:9000` (Docker internal network).
- **Console UI**: `http://localhost:9001` (User: `minioadmin`, Pass: `minioadmin`).
- **Bucket**: `socialtrend-datalake`.
- **Object Key Convention**: `{source_platform}/{YYYY-MM-DD}/{event_id}.json`.

### Design Evolution Post-Phase 1
- **Platform-Agnostic Raw Layer**: `RawEvent` is fully source-agnostic with `source_platform`, `account_source`, `creator`, and `payload` escape hatch, defined in [services/ingestion-service/internal/model/event.go](services/ingestion-service/internal/model/event.go).
- **Ingestion Service Purpose**: Consumes raw data from `social.engagement.raw` and stores directly into the MinIO Data Lake for preservation. Does **not** publish to multiple Kafka topics.
- **Downstream Processing**: Downstream worker `ai-service` (Python) reads raw event JSON objects from MinIO Data Lake (`socialtrend-datalake`) for cleaning, categorization, sentiment analysis, and multimodal style tagging.

### Service Ports
| Service | Internal Port | Host Port | Protocol / Purpose |
| :--- | :--- | :--- | :--- |
| `minio` | `9000` | `9000` | S3 API (Data Lake) |
| `minio-console` | `9001` | `9001` | MinIO Web Console |
| `ingestion-service` | `8081` | `8081` | HTTP / Kafka Client |
| `kafka` | `9092` | `9092` | PLAINTEXT |
| `postgres` | `5432` | `5432` | PostgreSQL |
| `ai-service` *(planned)* | `8000` | `8000` | HTTP / MinIO Client |
| `core-api-service` *(planned)* | `8080` | `8080` | HTTP / Kafka Client |
| `frontend` *(planned)* | `3000` | `3000` | HTTP |

---

## 4. Next Phase: Phase 2 — AI Service Core

When starting **Phase 2**, the assigned agent must:
1. Review [services/ai-service/CONTEXT.md](services/ai-service/CONTEXT.md) and [docs/rules.md](docs/rules.md) §3.
2. Read raw event JSON records from the MinIO Data Lake (`socialtrend-datalake`).
3. Implement cleaning, deduplication, and fashion categorization.
4. Integrate free Hugging Face models for:
   - Sentiment analysis (on post text and comments)
   - Fashion / image attribute extraction (CLIP/BLIP-class mapping to the controlled vocabulary)
5. Implement `/generate` endpoint according to [contracts/http/ai-service.openapi.yaml](contracts/http/ai-service.openapi.yaml).
6. **Phase 2 Exit Criterion**: A raw event retrieved from MinIO with image media and comment text produces extracted sentiment, fashion category attributes, and is ready for draft generation.
