# Social Media Trend Intelligence — Architecture & Tech Stack
## v0.2 — Java-primary polyglot build, services/ layout with shared contracts

---

## 1. Framing

This project is built primarily to strengthen a **Java/Spring Boot backend
developer** resume — that service is the one you should be able to talk about
in depth in an interview. The other languages are used only where they're the
*honest* right tool for that piece (not just to check a box), which is also a
better interview story than "I used everything everywhere":

| Layer | Language/Stack | Why |
|---|---|---|
| **Core API & orchestration** | **Java + Spring Boot** | The centerpiece service — agent job management, trend aggregation, orchestration, REST API. This is what you lead with on your resume. |
| **Ingestion / stream consumer** | **Go** | High-throughput Kafka consumer + normalization. Go's goroutines/channels are a natural fit for a lightweight, concurrent event-processing worker — and it's already one of your listed strengths. |
| **AI/ML service** | **Python** | Sentiment analysis, Hugging Face vision model inference, and the self-hosted content-generation model. Python isn't a stylistic choice here — the Hugging Face/Transformers ecosystem is Python-native, so this is the honest tool for this job. |
| **Frontend** | **Next.js** (React) | Agent job configuration UI, trend dashboard, suggestion review screen. Talks directly to the Java core API — no separate Node backend-for-frontend needed at POC scale. Single-user for now; laid out so multi-tenancy (multiple logins, each with their own agent jobs) can be added later without a rewrite — see `services/frontend/CONTEXT.md`. |

**Not used**: a separate Node/Express service. Next.js talks straight to the
Spring Boot API from its own server/client code; adding a separate Node BFF
here would be technology for its own sake, not because the architecture needs
it. (If you specifically want a distinct Node.js line on your resume from
this project, see the note in Section 6.)

This is a deliberate **polyglot microservices** design — not because a POC
needs four languages, but because it mirrors how real event-driven platforms
actually split responsibilities, and it gives you a defensible answer when an
interviewer asks "why Go here and Python there?"

---

## 2. Data Flow

```
                 ┌─────────────────────┐
 Upstream data → │  ingestion-service   │  (Go)
 (Kafka / REST)  │  consume → validate  │
                 │  → normalize         │
                 └──────────┬──────────┘
                             │ publishes: social.engagement.normalized
                             ▼
                 ┌─────────────────────┐
                 │     ai-service       │  (Python / FastAPI)
                 │  sentiment analysis  │
                 │  vision analysis     │
                 └──────────┬──────────┘
                             │ publishes: social.engagement.enriched
                             ▼
                 ┌─────────────────────┐
                 │  core-api-service    │  (Java / Spring Boot)
                 │  consumes enriched   │
                 │  events → trend      │
                 │  aggregation → agent │
                 │  job orchestration   │
                 │  → calls ai-service  │
                 │    /generate         │
                 └──────────┬──────────┘
                             │ REST API
                             ▼
                 ┌─────────────────────┐
                 │      frontend        │  (Next.js)
                 │  job config, trend   │
                 │  dashboard, review   │
                 └─────────────────────┘
```

**Kafka topics** (single Kafka cluster shared by all services — this is the
integration boundary between languages, so each service only needs to know
the topic schema, not the internals of the service before/after it):

- `social.engagement.raw` — upstream sources publish here (or via the
  ingestion-service's REST fallback, which republishes to this topic)
- `social.engagement.normalized` — output of ingestion-service
- `social.engagement.enriched` — output of ai-service (sentiment + vision
  attributes attached)

core-api-service is the only service with a full relational store (Postgres)
— it owns `AgentJob`, `BusinessProfile`, `TrendSnapshot`, and
`ContentSuggestion`. ingestion-service and ai-service are intentionally
stateless workers between Kafka topics; this keeps the two "attach a new
service to the pipeline" cases (new ingestion source, new ML step) cheap.

---

## 3. Repository Structure (monorepo)

A single repo is the right call for a portfolio project — one link, one
README, one `docker-compose up` to run the whole thing locally.

Services live under `services/`; everything else (docs, shared contracts,
infra, the compose file) lives at repo root. The key idea driving this layout:
**a service's own folder plus `/contracts` is everything an agent (or you)
needs to work on that service — nothing else in the repo should ever need to
be opened.**

```
social-trend-platform/
├── README.md                        # what it is, architecture diagram, how to run it
├── docker-compose.yml                # Kafka, Postgres, Redis, and all 4 services for local dev
│
├── docs/
│   ├── requirements.md               # your existing PRD
│   └── architecture.md               # this file
│
├── contracts/                        # ★ the ONLY thing services depend on outside their own folder
│   ├── kafka/
│   │   ├── social.engagement.raw.schema.json
│   │   ├── social.engagement.normalized.schema.json
│   │   └── social.engagement.enriched.schema.json
│   └── http/
│       ├── ai-service.openapi.yaml         # ai-service's /generate contract (used by core-api-service)
│       └── core-api-service.openapi.yaml   # core-api's public contract (used by frontend)
│
├── infra/
│   ├── kafka/                        # topic/partition setup, not schemas (schemas live in /contracts)
│   └── k8s/                          # optional — only if you want to demonstrate this too
│
└── services/
    │
    ├── core-api-service/              # ★ Java + Spring Boot — primary showcase service
    │   ├── CONTEXT.md                 # agent-facing: purpose, reads/writes, how to run/test
    │   ├── pom.xml
    │   ├── src/main/java/com/socialtrend/core/
    │   │   ├── CoreApiApplication.java
    │   │   ├── agentjob/
    │   │   │   ├── AgentJob.java
    │   │   │   ├── AgentJobController.java
    │   │   │   ├── AgentJobService.java
    │   │   │   └── AgentJobRepository.java
    │   │   ├── businessprofile/
    │   │   │   ├── BusinessProfile.java
    │   │   │   └── BusinessProfileService.java
    │   │   ├── trend/
    │   │   │   ├── TrendSnapshot.java
    │   │   │   ├── TrendAggregationService.java
    │   │   │   └── EnrichedEventConsumer.java   # Kafka listener on social.engagement.enriched
    │   │   ├── suggestion/
    │   │   │   ├── ContentSuggestion.java
    │   │   │   ├── SuggestionController.java
    │   │   │   ├── SuggestionOrchestrationService.java  # calls ai-service /generate
    │   │   │   └── AiServiceClient.java          # REST client generated from contracts/http/ai-service.openapi.yaml
    │   │   └── config/
    │   │       ├── KafkaConfig.java
    │   │       └── SecurityConfig.java
    │   ├── src/main/resources/
    │   │   ├── application.yml
    │   │   └── db/migration/                     # Flyway migrations
    │   └── src/test/java/com/socialtrend/core/... # unit + integration tests
    │
    ├── ingestion-service/              # Go — Kafka consumer + normalization
    │   ├── CONTEXT.md
    │   ├── go.mod
    │   ├── cmd/ingestion/main.go
    │   ├── internal/
    │   │   ├── kafka/
    │   │   │   ├── consumer.go            # reads social.engagement.raw
    │   │   │   └── producer.go            # writes social.engagement.normalized
    │   │   ├── normalize/
    │   │   │   └── normalize.go           # maps varied upstream shapes → schema in /contracts/kafka
    │   │   ├── ingest/
    │   │   │   └── handler.go             # REST fallback: POST /ingest/events
    │   │   └── model/
    │   │       └── event.go               # generated/hand-mapped from /contracts/kafka schemas
    │   ├── Dockerfile
    │   └── ingestion_test.go
    │
    ├── ai-service/                     # Python (FastAPI) — sentiment, vision, generation
    │   ├── CONTEXT.md
    │   ├── pyproject.toml
    │   ├── app/
    │   │   ├── main.py
    │   │   ├── kafka/
    │   │   │   ├── consumer.py            # reads social.engagement.normalized
    │   │   │   └── producer.py            # writes social.engagement.enriched
    │   │   ├── sentiment/
    │   │   │   └── analyzer.py            # HF sentiment-analysis pipeline
    │   │   ├── vision/
    │   │   │   └── image_attributes.py    # HF CLIP/BLIP-based model
    │   │   └── generation/
    │   │       ├── model_loader.py        # loads self-hosted fine-tuned model
    │   │       └── generate.py            # implements contracts/http/ai-service.openapi.yaml
    │   ├── Dockerfile
    │   └── tests/
    │
    └── frontend/                       # Next.js (App Router), pnpm
        ├── CONTEXT.md
        ├── package.json
        ├── pnpm-lock.yaml
        ├── app/
        │   ├── layout.tsx
        │   ├── page.tsx
        │   ├── agent-jobs/
        │   │   ├── page.tsx
        │   │   └── [jobId]/page.tsx
        │   ├── suggestions/
        │   │   └── page.tsx
        │   └── (auth)/                 # reserved for future login — see CONTEXT.md
        ├── components/
        ├── lib/
        │   ├── api/
        │   │   └── coreApiClient.ts    # typed from contracts/http/core-api-service.openapi.yaml
        │   └── auth/
        │       └── currentUser.ts      # stub single-user today; swapped in when multi-tenancy lands
        ├── middleware.ts               # reserved for future route protection
        └── Dockerfile
```

---

## 4. How Agents Should Work In This Repo

The rule that makes this repo agent-friendly: **an agent working inside
`services/<name>/` should never need to open another service's folder.**
Everything it needs to know about the outside world — what data looks like
coming in, what its own API is supposed to return — is defined once in
`/contracts`, not discovered by reading someone else's source.

- **Every service has a `CONTEXT.md` at its root.** This is the first file an
  agent should read before touching that service, and should cover:
  - **Purpose** — what this service does, in 2–3 sentences.
  - **Reads / writes** — which Kafka topics it consumes/produces, or which
    HTTP contract it implements/calls, each pointing at the specific file
    under `/contracts` (not a description of another service's internals).
  - **How to run it standalone** — the one or two commands to run and test
    this service in isolation (assuming Kafka/Postgres are up via the root
    `docker-compose.yml`).
  - **What it explicitly does NOT own** — e.g., core-api-service's CONTEXT.md
    should say "does not read raw upstream data directly — only consumes
    `social.engagement.enriched`, defined in `/contracts/kafka/`."
- **`/contracts` is the only cross-service dependency, ever.** Kafka event
  shapes live in `contracts/kafka/*.schema.json`; HTTP contracts between
  services (ai-service's `/generate`, core-api-service's public API) live in
  `contracts/http/*.openapi.yaml`. If a change requires editing another
  service's actual source to understand it, that's a sign the contract file
  is stale or incomplete — fix the contract, not the workaround of reading
  the code.
- **`core-api-service` follows standard Spring Boot layering**
  (controller → service → repository) **per feature package**, not per layer
  — i.e., `agentjob/` contains its own controller+service+repository, rather
  than one giant `controllers/` folder across features. This also keeps an
  agent's working set small when it's only asked to touch one feature.
  Package name to use: `com.socialtrend.core.<feature>`, matching the
  directory example in Section 3.
- **Every service is independently runnable** via `docker-compose up` at the
  repo root, which brings up Kafka + Postgres + all four services for local
  demoing — but each service also has its own local run/test instructions in
  its `CONTEXT.md` for working on it in isolation.
- **Commit messages/PRs scoped per service** where possible (e.g.,
  `core-api: add agent job endpoint`) — mirrors how this would actually be
  worked on by a team, and keeps an agent's diff contained to the one folder
  it's supposed to be touching.

---

## 5. Example `CONTEXT.md` (core-api-service)

To make the pattern concrete rather than just described:

```markdown
# core-api-service — Context for Agents

## Purpose
Owns agent job configuration, trend aggregation, and content-suggestion
orchestration. This is the system's main REST API and the only service with
a relational database.

## Reads
- Kafka topic `social.engagement.enriched`
  — schema: /contracts/kafka/social.engagement.enriched.schema.json

## Writes / Exposes
- Public REST API — contract: /contracts/http/core-api-service.openapi.yaml
- Calls ai-service's `/generate` endpoint
  — contract: /contracts/http/ai-service.openapi.yaml

## Does NOT own
- Raw data ingestion or normalization (see ingestion-service — not needed to
  work on this service).
- Sentiment/vision analysis or model inference (see ai-service — not needed
  to work on this service).

## Run standalone
docker-compose up kafka postgres   # from repo root
cd services/core-api-service && ./mvnw spring-boot:run

## Test
cd services/core-api-service && ./mvnw test
```

Every service's `CONTEXT.md` should follow this shape — Purpose / Reads /
Writes / Does NOT own / Run / Test — so an agent gets a consistent, minimal
briefing regardless of which service it opens first.

---

## 6. How This Maps to Your Resume

For your **Java resume**, lead with `core-api-service`: "Designed and built
the orchestration core of a polyglot, event-driven trend-analysis platform in
Spring Boot — consuming enriched Kafka events, aggregating trend data, and
coordinating content generation across a Python ML service, with a Go-based
high-throughput ingestion layer feeding the pipeline." That one sentence
demonstrates Spring Boot, Kafka, service orchestration, and system design
judgment (knowing *why* each language was chosen) all at once.

If you later want a dedicated **Go** or **Python** resume variant, the same
repo supports that — you'd just lead with `ingestion-service` or `ai-service`
instead and describe `core-api-service` as "the downstream Java service it
feeds." That's a real advantage of the polyglot monorepo: one build, multiple
resume framings.

If you specifically want a **Node.js** line item from this project, the
natural (non-forced) place to add it is a thin BFF between `frontend` and
`core-api-service` — but I'd only add that if a specific job posting asks for
Node, since it doesn't earn its place architecturally otherwise.
