# Social Media Trend Intelligence & Content Automation
## Requirements Document (v0.5 — Generic Raw Layer & Fashion Trend POC Scope)

---

## 1. Overview & Vision

This platform is an intelligence and automation engine designed to understand what content users are engaging with, generating, or asking about across any social or digital content source. 

The core architecture consists of two distinct tiers:
1. **Platform Core — Generic Multi-Source Raw & Categorization Layer**: A fully source-agnostic event pipeline that ingests raw engagement data from any producer (platform adapters, client agents, manual imports, third-party feeds), cleans the stream, and classifies content into broad categories (fashion/style, tech, food, meme, lifestyle, user-inquiries, etc.).
2. **First Use Case — Fashion Style-Trend Detection**: The first concrete vertical built on top of the generic platform. It detects emerging fashion style trends (dresses, jewellery, purses, hairstyles) from public Instagram content, confirms trend spikes via trailing volume and influencer weighting, and generates informed content suggestions.

> **Scope Note**: Fashion trend detection is the **first use case, not the whole purpose of the platform**. The system is designed so that subsequent verticals (e.g., "what technical topics are users asking about" or "trending meme formats") can be added as new categorization branches and domain projections without redesigning or modifying the underlying `RawEvent` contract or ingestion pipeline.

This is a **portfolio and proof-of-concept (POC) system**, not a production deployment. Depth and correctness across one complete path are prioritized over breadth.

---

## 2. Goals

### Platform Core Goals
- Provide a unified, platform-agnostic ingestion contract (`RawEvent`) that any producer can publish to via Kafka or REST fallback.
- Implement an explicit **Cleaning & Categorization** pipeline stage to deduplicate, sanitize, and classify events into content categories before routing to domain-specific handlers.
- Support extensible category projections without polluting generic event envelopes with domain-specific fields.

### Fashion Use Case Goals (First Vertical)
- Ingest public fashion content exclusively from Instagram (Reels and image posts) via official free-tier APIs:
  - **Hashtag Search API**: Curated list of style hashtags (subject to the platform limit of max 30 unique hashtags per rolling 7-day window per connected Business account).
  - **Business Discovery API**: Curated list of target influencers and style creators.
- Extract granular visual style attributes (garment type, silhouette, color palette, material, accessory) using an open-source vision model and a centrally maintained, versioned vocabulary.
- Confirm style trends using trailing average spike detection, unique creator validation, and influencer weighting.
- Generate high-quality post drafts (captions + hashtag recommendations) tailored to an Agent Job's niche using a self-hosted generation model.
- Provide a clean review dashboard for inspecting detected trend snapshots and approving or rejecting generated suggestions.

---

## 3. Engineering Posture: POC vs. Production

This project validates the architecture and end-to-end intelligence loop. It is intentionally engineered with a pragmatic POC posture:

- **In Scope**: One complete path working end-to-end: pull data → clean → categorize → (for fashion) tag styles → detect trend spikes → generate draft content → review in UI. Logic that is most critical to correctness (normalization, category classification, and spike scoring) must be verified with focused unit tests.
- **Decision Heuristic**: Whenever a design choice presents a "quick POC way" versus a "proper production way," default to the **POC way** (e.g., a scheduled batch job for spike detection rather than a distributed streaming aggregator) and document the production alternative in a concise comment.
- **Comments Fetching Policy**: 
  - **Eager**: If a source adapter receives comments for free within the primary content response payload, it populates `Comments` eagerly at collection time.
  - **Lazy**: If fetching comments requires an additional API round-trip, fetching is deferred until after the event survives categorization and is routed to a domain handler that explicitly requests comment text.

### Explicitly Deferred (Post-POC Conscious Decisions)
The following capabilities are recognized production requirements that are consciously deferred post-POC:
- **Resilience**: Retry and exponential backoff beyond basic immediate retry; circuit breakers.
- **Scalability**: Horizontal scaling, cluster auto-scaling, and load testing.
- **Queue Management**: Complex rate-limit-aware queuing beyond respecting hard external platform rate caps.
- **Security & Secrets**: Production secret management (Vault/AWS Secrets Manager), OAuth token refresh flows, and multi-tenant authorization; basic environment variables are sufficient for the POC.
- **Observability**: Distributed tracing, APM, and metrics dashboards (structured stdout/stderr logging is sufficient).
- **Test Coverage**: Broad end-to-end matrix testing (focused contract and unit tests on normalization, categorization, and scoring logic are sufficient).
- **Administration**: Multi-tenant admin tooling, user management, and billing.

---

## 4. System Components & Pipeline Stages

```
[Producers]
  ├── Instagram Hashtag Search Adapter
  ├── Instagram Business Discovery Adapter
  ├── External Client Agents / Apps
  └── Manual Import / REST Fallback (POST /ingest/events)
              │
              ▼
   Kafka: social.engagement.raw (Generic RawEvent Contract)
              │
              ▼
    [ingestion-service (Go)]
      - Consumes raw events
      - Validates against contracts/kafka/social.engagement.raw.schema.json
      - Stores raw JSON into MinIO Data Lake storage (bucket: socialtrend-datalake)
      * Does NOT publish to multiple Kafka topics
              │
              ▼
    [MinIO Data Lake Storage (Preserved Raw Records in Docker)]
              │
              ▼
    [Downstream Processing (ai-service / core-api-service)]
      - Reads raw JSON from MinIO Data Lake
      - Cleaning & Deduplication
      - Categorization (fashion, tech, food, lifestyle, etc.)
      - Multimodal Style Tagging (CLIP/BLIP) on fashion category
      - Trend Spike Aggregation & Content Suggestion
```

### 4.1 Ingestion Layer & Data Lake Preservation
- **Generic Ingestion Endpoint**: A REST fallback endpoint (`POST /ingest/events`) accepting generic `RawEvent` payloads for producers unable to publish directly to Kafka.
- **Kafka Raw Topic**: `social.engagement.raw` acts as the shared ingestion buffer.
- **Data Lake Provider (MinIO)**: Self-hosted S3-compatible object storage (MinIO) running in Docker container `socialtrend-minio` on port `9000` (API) and `9001` (Console). Bucket: `socialtrend-datalake`. Partition key format: `{source_platform}/{YYYY-MM-DD}/{event_id}.json`.
- **Ingestion Worker**: `ingestion-service` consumes raw events from `social.engagement.raw`, validates each against the raw JSON schema, and persists them directly into the MinIO Data Lake. It does not publish to multiple Kafka topics.
- **Producers**: Independent adapters map their source payloads to the generic `RawEvent`:
  - `source_platform`: Free-form string identifying the origin platform (e.g., `"instagram"`, `"youtube"`, `"manual"`).
  - `account_source`: Identifies which application client, agent, or tenant ingested the record.
  - `creator`: The original author/owner on the source platform.

### 4.2 Downstream Cleaning & Categorization
Downstream consumers (`ai-service` in Python) process preserved raw data fetched from the MinIO Data Lake:
1. **Cleaning**:
   - Deduplicate events across rolling windows using `ContentURL`.
   - Discard invalid events missing both `ContentURL` and `Payload`.
   - Sanitize empty strings and normalize nullable timestamp structures (`occurred_at`, `collected_at`).
2. **Categorization**:
   - Classify cleaned content into broad categories: `fashion`, `tech`, `food`, `meme`, `lifestyle`, etc.
   - Performed via a lightweight classification model or prompt against post captions, hashtags, and media previews.

### 4.3 Fashion Trend Vertical (First Domain Pipeline)
Events marked with category `fashion` are consumed by the fashion domain pipeline:
1. **Multimodal Style Tagging**:
   - Uses an open-source vision model (CLIP/BLIP-class) to evaluate `MediaURLs` / `ImageRef`.
   - Tags items against a **centrally maintained, versioned vocabulary** spanning four core sub-categories:
     - `dress` (silhouette, length, neckline, pattern)
     - `jewellery` (metal type, gemstone, style)
     - `purse` (tote, clutch, crossbody, shoulder bag, material)
     - `hairstyle` (cut, braid, color, styling)
2. **Trend Confirmation & Spike Scoring**:
   - Trends are aggregated per style tag over rolling evaluation windows.
   - A style is confirmed as **trending** when all three conditions are satisfied:
     1. **Volume Spike**: `spike_score = volume(tag, day) / 7_day_trailing_avg_volume(tag)` exceeds a configurable threshold (e.g., >= 2.5x).
     2. **Unique Creator Growth**: The count of distinct creators `unique_accounts(tag, day)` is rising concurrently, verifying that the spike is not an artifact of a single viral post.
     3. **Influencer Signal Weighting**: Recent adoption by a curated list of tracked influencers (via Business Discovery) is weighted as an earlier and stronger signal than organic volume alone.

### 4.4 Trend Aggregation & Orchestration (`core-api-service`)
- Manages `AgentJob` lifecycle and configurations.
- Periodically computes and persists `TrendSnapshot` records containing top trending style tags and exemplar content.
- Calls `ai-service` to generate creative post suggestions based on recent trend snapshots.

### 4.5 Content Suggestion & Review
- Generates post drafts (engaging caption, suggested format, and target hashtags) using a self-hosted generation model.
- Stores `ContentSuggestion` records in status `pending` for operator review via the frontend dashboard.

---

## 5. Permanent Platform Limitations & Non-Goals

### 5.1 Watch Time & Retention on Unowned Content
- **Permanent Platform Limitation**: Instagram's Graph API strictly restricts video watch time, average view duration, and replay counts to media owned by the authenticated Business account via Graph Insights. Public media queried through Hashtag Search or Business Discovery **never** provides watch-time metrics.
- `RawEvent.AvgWatchTimeSeconds` exists in the generic contract for sources that legitimately supply attention metrics (e.g., first-party video players, future platforms), but Instagram adapters will permanently leave this field `nil`. This is a documented platform boundary, not a feature gap.

### 5.2 Instagram API Quotas & Restrictions
- **Hashtag Search Quota**: Maximum of **30 unique hashtags per rolling 7-day window** per connected Instagram Business account. The fashion adapter must maintain a curated, rate-budgeted hashtag list.
- **Unauthenticated / Scraped Data**: No credential-based scraping or automated browser session pooling against unowned accounts. All data is gathered through documented APIs or authorized external feeds.

---

## 6. Functional Requirements Summary

| # | Requirement | Scope Tier |
|---|---|---|
| **FR1** | System shall provide a generic, platform-agnostic `RawEvent` contract for Kafka and REST ingestion (`POST /ingest/events`). | Platform Core |
| **FR2** | System shall clean raw events by deduplicating `ContentURL`, dropping malformed records, and normalizing timestamps. | Platform Core |
| **FR3** | System shall classify cleaned events into high-level content categories (fashion, tech, food, etc.) and publish generic `NormalizedEvent` records. | Platform Core |
| **FR4** | System shall ingest public Instagram Reels and image posts via Hashtag Search API (respecting the 30 hashtags / 7-day limit) and Business Discovery API. | Fashion Vertical |
| **FR5** | System shall tag fashion media against a versioned, controlled style vocabulary across dresses, jewellery, purses, and hairstyles using an open vision model. | Fashion Vertical |
| **FR6** | System shall compute trend spike scores using trailing 7-day volume, require rising unique account counts, and apply influencer weighting. | Fashion Vertical |
| **FR7** | System shall generate post suggestions (caption + hashtags) using a self-hosted open-source LLM informed by confirmed trend snapshots. | Fashion Vertical |
| **FR8** | User shall be able to configure `AgentJob` instances specifying target category, style sub-categories, and autonomy preferences. | Orchestration |
| **FR9** | User shall be able to inspect trend snapshots and review, approve, or reject pending content suggestions via a web UI. | Frontend |
| **FR10** | System shall demonstrate the complete pipeline working end-to-end for one curated fashion style trend. | POC Milestone |

---

## 7. Data Model (Core Entities)

- `RawEvent`: Source-agnostic ingestion envelope (`event_id`, `collected_at`, `occurred_at`, `source_platform`, `account_source`, `creator`, `content_url`, `content_type`, `captions`, `hashtags`, `media_urls`, engagement counts, `payload`).
- `NormalizedEvent`: Sanitized, generic classified envelope (`event_id`, `raw_event_id`, `category`, `category_confidence`, cleaned content, engagement counts, `occurred_at`, `cleaned_at`).
- `FashionTrendEvent`: Category-specific projection (`normalized_event_id`, `sub_category`, `style_tags`, `is_tracked_influencer`, `media_product_type`).
- `AgentJob`: Configuration entity (`id`, `name`, `target_category`, `sub_categories`, `autonomy_mode`, `created_at`).
- `TrendSnapshot`: Computed trend intelligence (`id`, `agent_job_id`, `category`, `top_style_tags`, `spike_score`, `time_window`, `created_at`).
- `ContentSuggestion`: Generated content draft (`id`, `agent_job_id`, `trend_snapshot_id`, `draft_text`, `suggested_hashtags`, `status: pending | approved | rejected`).
