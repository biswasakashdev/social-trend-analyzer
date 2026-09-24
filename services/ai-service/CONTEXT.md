# ai-service — Context for Agents

## Purpose
ML and domain inference worker — Python (FastAPI). It consumes preserved raw event JSON records from the MinIO Data Lake (`socialtrend-datalake`), cleans/deduplicates content, classifies content into categories (fashion vertical first), extracts visual style attributes using open-source Hugging Face models (CLIP/BLIP), runs sentiment analysis on captions/comments, and exposes a `/generate` HTTP endpoint for draft suggestions.

## Reads
- **Data Lake (MinIO)**: Consumes preserved raw JSON objects from MinIO bucket `socialtrend-datalake` (partitioned: `{source_platform}/{YYYY-MM-DD}/{event_id}.json`).
- **HTTP Contract**: Implements `/generate` endpoint defined in `contracts/http/ai-service.openapi.yaml`.

## Writes / Exposes
- **HTTP Endpoint**: `POST /generate` implementing `contracts/http/ai-service.openapi.yaml` (called by `core-api-service` for content suggestions).
- Cleaned and enriched domain features for trend aggregation in `core-api-service`.

## Does NOT own
- Raw stream ingestion or Kafka consumer management (that's `ingestion-service`).
- Relational database management or `AgentJob` persistence (that's `core-api-service` with Postgres).
- Frontend UI rendering (that's `frontend`).

## Environment Variables
- `MINIO_ENDPOINT`: MinIO host and port (default: `localhost:9000` or `minio:9000`)
- `MINIO_ACCESS_KEY`: MinIO access key (default: `minioadmin`)
- `MINIO_SECRET_KEY`: MinIO secret key (default: `minioadmin`)
- `MINIO_BUCKET`: MinIO bucket name (default: `socialtrend-datalake`)
- `PORT`: Service port (default: `8000`)

## Run Standalone
```bash
docker-compose up minio   # from repo root
cd services/ai-service
uv run uvicorn app.main:app --reload --port 8000
```

## Test
```bash
cd services/ai-service
uv run pytest
```
