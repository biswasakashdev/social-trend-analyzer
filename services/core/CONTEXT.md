# core-api-service — Context for Agents

## Purpose
The main orchestration service — Java + Spring Boot. Owns `AgentJob`,
`BusinessProfile`, `TrendSnapshot`, and `ContentSuggestion`. This is the
system's primary REST API and the only service with a relational database
(Postgres). It's the service the resume story leads with.

## Reads
- Kafka topic `social.engagement.enriched`
  — schema: `/contracts/kafka/social.engagement.enriched.schema.json`
  — consumed by `trend/EnrichedEventConsumer.java`, persisted as
    `CollectedItem` + related `ImageAttributes`/`SentimentResult` rows.

## Writes / Exposes
- Public REST API — contract: `/contracts/http/core-api-service.openapi.yaml`
  - `AgentJob` CRUD (create/list/update) — `agentjob/`
  - `BusinessProfile` create/read, required before a business-tracking
    `AgentJob` can run — `businessprofile/`
  - `TrendSnapshot` read (per agent job) — `trend/`
  - `ContentSuggestion` list/review/approve — `suggestion/`
- Calls **ai-service**'s `POST /generate` to produce a `ContentSuggestion`
  from a `TrendSnapshot` — contract: `/contracts/http/ai-service.openapi.yaml`
  — client: `suggestion/AiServiceClient.java`.

## Does NOT own
- Raw data ingestion or normalization — that's `ingestion-service`. This
  service never reads `social.engagement.raw` or `social.engagement.normalized`.
- Sentiment classification or image/vision analysis, or the generation model
  itself — that's `ai-service`. This service only calls its `/generate`
  contract; it does not implement any model logic.
- Any UI rendering — that's `frontend`.

## Package Layout (package-by-feature)
```
com.socialtrend.core/
├── agentjob/          # AgentJob entity, controller, service, repository
├── businessprofile/   # BusinessProfile entity, service
├── trend/             # TrendSnapshot, TrendAggregationService, EnrichedEventConsumer
├── suggestion/         # ContentSuggestion, controller, orchestration service, AiServiceClient
└── config/            # KafkaConfig, SecurityConfig
```
Each feature folder owns its own controller + service + repository — never a
repo-wide `controllers/` folder. See `docs/rules.md` §3 for the full
convention list (constructor injection only, Flyway migrations, contract-first
endpoint changes).

## Functional Requirements This Service Implements
FR4 (trend aggregation), FR6 (agent job creation, multi-niche), FR7/FR8
(business-profile gating), FR9 (calls generation), FR10 (manual-review vs.
auto-suggest mode), and it's the anchor for FR11 (end-to-end demo path).
See `docs/requirements.md` §5 for full FR text.

## Run Standalone
```
docker-compose up kafka postgres   # from repo root
cd services/core-api-service
./mvnw spring-boot:run
```

## Test
```
cd services/core-api-service
./mvnw test
```
