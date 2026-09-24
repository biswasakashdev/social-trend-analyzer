# Project Memory — Social Media Trend Intelligence Platform

> **Notice for AI Agents**: This file serves as a persistent cross-session status tracker and memory log for all AI coding agents working on this repository (Antigravity, Cursor, Claude Code, etc.). Always read [AGENTS.md](file:///Users/akash/Work/Projects/social-trend-analyzer/AGENTS.md), [docs/rules.md](file:///Users/akash/Work/Projects/social-trend-analyzer/docs/rules.md), and [docs/phases.md](file:///Users/akash/Work/Projects/social-trend-analyzer/docs/phases.md) before proposing or implementing changes.

---

## 1. Project Phase Tracker

| Phase | Description | Owner Service / Stack | Status | Exit Criterion Met |
| :--- | :--- | :--- | :--- | :--- |
| **Phase 0** | Foundation & Contracts | Monorepo / Multi-service | **Completed** | Kafka, Postgres, service skeletons, and contracts in place |
| **Phase 1** | Ingestion Skeleton | `ingestion-service` (Go) | **Completed** | Raw events consumed, validated, normalized, and published to normalized topic |
| **Phase 2** | AI Service Core | `ai-service` (Python FastAPI) | **Pending / Up Next** | Sentiment + vision attributes extracted & published to enriched topic |
| **Phase 3** | Core API Foundations | `core-api-service` (Java / Spring) | Pending | AgentJob CRUD + Enriched event persistence in Postgres |
| **Phase 4** | Trend Aggregation & Suggestions | `core-api-service` / `ai-service` | Pending | Trend ranking computed and suggestion generated |
| **Phase 5** | Frontend | `frontend` (Next.js / React) | Pending | Full UI flow operational without hitting API manually |
| **Phase 6** | End-to-End Demo | All services | Pending | Complete single-workflow demo from seed data to UI review |
| **Phase 7** | Expansion & Multi-job | Multi-service | Pending | Multiple concurrent jobs & additional data providers |

---

## 2. Completed Jobs & Milestones

### 1. Phase 1 Completed (`ingestion-service`)
- **Service**: [services/ingestion-service](file:///Users/akash/Work/Projects/social-trend-analyzer/services/ingestion-service) (Go 1.24+)
- **Functional Requirements Implemented**: `FR1`, `FR2`, `FR3`
- **Core Components**:
  - **Kafka Consumer**: [internal/kafka/consumer.go](file:///Users/akash/Work/Projects/social-trend-analyzer/services/ingestion-service/internal/kafka/consumer.go) consumes `social.engagement.raw` with clean context shutdown.
  - **Schema Validation**: [internal/validator/validator.go](file:///Users/akash/Work/Projects/social-trend-analyzer/services/ingestion-service/internal/validator/validator.go) validates incoming payloads against [contracts/kafka/social.engagement.raw.schema.json](file:///Users/akash/Work/Projects/social-trend-analyzer/contracts/kafka/social.engagement.raw.schema.json).
  - **Normalization Engine**: [internal/normalize/normalize.go](file:///Users/akash/Work/Projects/social-trend-analyzer/services/ingestion-service/internal/normalize/normalize.go) maps varied platform formats into common normalized structure.
  - **Kafka Producer**: [internal/kafka/producer.go](file:///Users/akash/Work/Projects/social-trend-analyzer/services/ingestion-service/internal/kafka/producer.go) publishes normalized events to `social.engagement.normalized` with schema pre-validation.
  - **REST Fallback Endpoint**: [internal/ingest/handler.go](file:///Users/akash/Work/Projects/social-trend-analyzer/services/ingestion-service/internal/ingest/handler.go) exposes `POST /ingest/events` on port `8081` and republishes incoming payloads to `social.engagement.raw`.
  - **Synthetic Test Publisher**: [cmd/publisher/main.go](file:///Users/akash/Work/Projects/social-trend-analyzer/services/ingestion-service/cmd/publisher/main.go) provides a standalone tool to emit synthetic raw events to Kafka for testing pipeline flow.
  - **Automated Tests**: Unit test suite covering validator, normalizer, and REST handler (`go test ./...`) all pass.
  - **Dockerfile**: [services/ingestion-service/Dockerfile](file:///Users/akash/Work/Projects/social-trend-analyzer/services/ingestion-service/Dockerfile) configured for multi-stage building.

### 2. Ingestion Service Added to Docker Compose
- **File**: [docker-compose.yml](file:///Users/akash/Work/Projects/social-trend-analyzer/docker-compose.yml)
- **Container Entry**: Added `ingestion-service` using image `biswasakash/socialtrend-ingestion:0.1.0`.
- **Port Mapping**: `8081:8081`
- **Environment Variables**:
  - `KAFKA_BROKERS: kafka:9092`
  - `KAFKA_RAW_TOPIC: social.engagement.raw`
  - `KAFKA_NORMALIZED_TOPIC: social.engagement.normalized`
  - `KAFKA_CONSUMER_GROUP: ingestion-service-group`
  - `HTTP_PORT: 8081`
- **Dependencies**: Depends on `kafka` with condition `service_healthy`.
- **Infrastructure Services**:
  - `kafka` (Apache Kafka 3.8.0 on `9092:9092` with healthcheck)
  - `postgres` (Postgres 16 Alpine on `5432:5432` with healthcheck and persistent volume `postgres-data`)

### 3. Repository Root Makefile Initialized
- **File**: [Makefile](file:///Users/akash/Work/Projects/social-trend-analyzer/Makefile)
- **Targets**:
  - `ingestion-build-image`: Builds the ingestion container image from the repository root using the multi-stage Dockerfile and tags it:
    - `biswasakash/socialtrend-ingestion:latest`

---

## 3. Key Architecture & Operational Context

### Kafka Topics & Contracts
- `social.engagement.raw`: [contracts/kafka/social.engagement.raw.schema.json](file:///Users/akash/Work/Projects/social-trend-analyzer/contracts/kafka/social.engagement.raw.schema.json)
- `social.engagement.normalized`: [contracts/kafka/social.engagement.normalized.schema.json](file:///Users/akash/Work/Projects/social-trend-analyzer/contracts/kafka/social.engagement.normalized.schema.json)
- `social.engagement.enriched`: [contracts/kafka/social.engagement.enriched.schema.json](file:///Users/akash/Work/Projects/social-trend-analyzer/contracts/kafka/social.engagement.enriched.schema.json)

### Service Ports
| Service | Internal Port | Host Port | Protocol |
| :--- | :--- | :--- | :--- |
| `ingestion-service` | `8081` | `8081` | HTTP / Kafka Client |
| `kafka` | `9092` | `9092` | PLAINTEXT |
| `postgres` | `5432` | `5432` | PostgreSQL |
| `ai-service` *(planned)* | `8000` | `8000` | HTTP / Kafka Client |
| `core-api-service` *(planned)* | `8080` | `8080` | HTTP / Kafka Client |
| `frontend` *(planned)* | `3000` | `3000` | HTTP |

---

## 4. Next Phase: Phase 2 — AI Service Core

When starting **Phase 2**, the assigned agent must:
1. Review [services/ai-service/CONTEXT.md](file:///Users/akash/Work/Projects/social-trend-analyzer/services/ai-service/CONTEXT.md) and [docs/rules.md](file:///Users/akash/Work/Projects/social-trend-analyzer/docs/rules.md) §3.
2. Consume from `social.engagement.normalized`.
3. Integrate free Hugging Face models for:
   - Sentiment analysis
   - Fashion / image attribute extraction (CLIP/BLIP-class)
4. Publish output to `social.engagement.enriched` following [contracts/kafka/social.engagement.enriched.schema.json](file:///Users/akash/Work/Projects/social-trend-analyzer/contracts/kafka/social.engagement.enriched.schema.json).
5. **Phase 2 Exit Criterion**: A normalized event with an image and comment text produces an enriched event with both sentiment and image attributes attached.
