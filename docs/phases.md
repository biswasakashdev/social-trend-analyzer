# Build Phases — Social Media Trend Intelligence Platform
*(belongs at `docs/phases.md` in the repo)*

Each phase has a clear exit criterion — a thing that's demoably true when
it's done — so progress is always checkable, and the project is demoable
even if it stops partway through. Phases are ordered so the pipeline proves
itself early (with mocked data) before real integrations are layered in.

---

## Phase 0 — Foundation
**Goal**: the repo exists and is runnable, before any real logic is written.

- Scaffold `services/{core-api-service,ingestion-service,ai-service,frontend}`
  with empty-but-buildable projects (Spring Boot skeleton, Go module, FastAPI
  skeleton, React app).
- Write `/contracts/kafka/*.schema.json` for the three topics (`raw`,
  `normalized`, `enriched`) — even in draft form, before any service reads or
  writes them.
- Write `/contracts/http/*.openapi.yaml` for `ai-service` (`/generate`) and
  `core-api-service` (public API) — draft is fine; refine per phase.
- Root `docker-compose.yml` brings up Kafka + Postgres (+ Redis if already
  planned) — services themselves can still be stubs at this point.
- Each service gets a `CONTEXT.md` (Purpose / Reads / Writes / Does NOT own /
  Run / Test), even before there's much to say in "Reads/Writes" yet.

**Exit criterion**: `docker-compose up` starts Kafka + Postgres and all four
service stubs without crashing.

---

## Phase 1 — Ingestion Skeleton (`ingestion-service`, Go)
**Goal**: prove the Kafka consume → normalize → publish shape, using a fake
publisher standing in for any real upstream source.

- A small script/CLI publishes sample events onto `social.engagement.raw`
  (stands in for a real upstream collector, which is out of scope — see
  `docs/requirements.md` Section 8).
- `ingestion-service` consumes `raw`, validates against
  `/contracts/kafka/social.engagement.raw.schema.json`, normalizes, and
  publishes to `social.engagement.normalized`.
- REST fallback endpoint (`POST /ingest/events`) exists and republishes onto
  the same `raw` topic — doesn't need a real external caller yet.

**Exit criterion**: a sample raw event, published by the test script, shows
up correctly shaped on the `normalized` topic.

---

## Phase 2 — AI Service Core (`ai-service`, Python)
**Goal**: sentiment + vision analysis work in isolation, feeding the
`enriched` topic.

- Consume `social.engagement.normalized`.
- Sentiment analysis via a free Hugging Face pipeline.
- Image/fashion attribute extraction via a free Hugging Face vision model
  (CLIP/BLIP-class).
- Publish results to `social.engagement.enriched`, matching that schema.

**Exit criterion**: a normalized event with an image and some comment text
produces an enriched event with both sentiment and image attributes attached.

---

## Phase 3 — Core API Foundations (`core-api-service`, Java)
**Goal**: the Java service can consume enriched events and manage agent jobs
— the two pieces everything else in this service builds on.

- `AgentJob` entity + CRUD endpoints (create/list/update), matching
  `contracts/http/core-api-service.openapi.yaml`.
- `EnrichedEventConsumer` reads `social.engagement.enriched` and persists
  `CollectedItem` + related `ImageAttributes`/`SentimentResult` rows.
- Flyway migrations for the schema so far.

**Exit criterion**: creating an `AgentJob` via the API and running the
pipeline from Phase 1–2 results in `CollectedItem` rows visible in Postgres.

---

## Phase 4 — Trend Aggregation & Content Suggestion
**Goal**: the platform can say "here's what's trending" and turn that into a
draft suggestion.

- `TrendAggregationService` computes a simple ranking (Section 4.4 of
  `docs/requirements.md`) over recent `CollectedItem`s for one `AgentJob`.
- `SuggestionOrchestrationService` calls `ai-service`'s `/generate` endpoint
  with the trend snapshot as context, using the self-hosted generation model.
- `ContentSuggestion` is persisted with status `pending`.

**Exit criterion**: one `AgentJob`, run end-to-end, produces a
`ContentSuggestion` in the database that's visibly informed by the ingested
sample data (not a generic/static output).

---

## Phase 5 — Frontend
**Goal**: the workflow is viewable and operable without hitting the API
directly.

- Agent job creation/config screen.
- Trend dashboard (shows the current `TrendSnapshot` for a job).
- Suggestion review screen (approve/reject a pending `ContentSuggestion`).
- All calls go through `src/api/coreApiClient.ts`, typed against
  `core-api-service.openapi.yaml`.

**Exit criterion**: a person with no API client can create a job, watch a
trend snapshot appear, and review a generated suggestion, entirely in the UI.

---

## Phase 6 — Single Workflow, End-to-End Demo
**Goal**: everything from Phase 0–5 runs as one coherent demo, not just
individually-tested pieces.

- Wire the Phase 1 test publisher (or a first real free-tier source, e.g.
  Reddit) as the actual input.
- Run the full path: event in → normalized → enriched → trend snapshot →
  suggestion → reviewed in the UI.
- Write the top-level `README.md`: what it is, the architecture diagram (from
  `docs/architecture.md`), and exact steps to run the demo locally.

**Exit criterion**: `docker-compose up`, run one script/command to seed data,
and the full pipeline is visibly demoable start to finish — this is the
version you'd show in an interview.

---

## Phase 7 — Expand Beyond the Single Workflow
**Goal**: broaden coverage once the core story is solid — do this only after
Phase 6 is genuinely working end-to-end.

- Multiple concurrent `AgentJob`s with independent niches.
- `BusinessProfile` requirement enforced for business-tracking jobs (FR8).
- Auto-suggest mode alongside manual-review mode (FR10).
- Instagram official-API collection added as a second real input source
  alongside Reddit.
- REST ingestion fallback exercised by an actual second caller, not just the
  Kafka path.

**Exit criterion**: at least one additional agent job type (e.g., the
business-tracking one) is demoable alongside the original Phase 6 workflow,
without having broken it.

---

## Notes on Sequencing

- Phases 1 and 2 can be built in parallel once Phase 0's contracts are
  drafted — they only depend on the `raw`→`normalized`→`enriched` schemas,
  not on each other's code.
- Don't start Phase 7 work early "while you're in there" — Phase 6 being a
  complete, demoable slice is the whole point of this ordering; expanding
  scope before that's true risks ending up with several unfinished pieces
  instead of one finished demo.
