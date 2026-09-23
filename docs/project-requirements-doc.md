# Social Media Trend Intelligence & Content Automation
## Requirements Document (v0.4 — POC Scope)

---

## 1. Overview

A proof-of-concept system focused on a single component:

**Trend & Content Platform** — monitors social media trends (free-tier
sources plus optional external/behavioral feeds), analyzes what content
performs well, and uses that intelligence to generate and optionally
auto-publish content, organized around user-defined "agent jobs" (e.g., one
agent for LinkedIn tech posts, one for fashion, one for a specific
business/product).

This version drops the standalone "Data Collector Companion App" as something
this project builds. Instead, the platform is designed to **receive**
behavioral/personalized trend data from any external collector (via Kafka or
a REST ingestion API) if and when such an app exists, and/or to **pull** extra
data itself via a server-side browser-automation collector, described below.

**Scope note**: this is a portfolio/demo project, not a production system.
Depth is prioritized over breadth — one working end-to-end workflow, built
well, beats several half-built features. Everything below is deliberately kept
to what's needed to demonstrate the concept credibly.

---

## 2. Goals

- Collect public trend/engagement data from free-tier APIs (Instagram, Reddit).
- Accept an optional external behavioral/personalized data feed (e.g., from a
  separately-built watch-behavior collector app) over Kafka or a REST
  ingestion endpoint, without this project needing to build that collector.
- Optionally collect additional topic-specific data via a server-side
  browser-automation collector that operates a pool of browser instances
  signed into users' own social accounts on their behalf (see 4.1b — this
  carries real platform-ToS and account-risk considerations, noted where it
  applies).
- Analyze image content (e.g., fashion attributes) using an existing free
  vision model — no training from scratch.
- Generate content using a self-hosted, fine-tuned generation model.
- Let a user define multiple independent "agent jobs," each scoped to a niche
  (topic, platform, tone) and a set autonomy level (manual review vs. auto-post).
- For business-tracking agents, require the user to define their product,
  preferences, and desired insights up front, so the agent's output is targeted
  rather than generic.
- Demonstrate exactly **one** full workflow end-to-end (data in → analysis →
  generated content → posted or queued for approval). Design should make a
  second workflow easy to add later, but only one needs to be built for the POC.

---

## 3. Actors

| Actor | Description |
|---|---|
| **User** | Connects a social account, creates one or more agent jobs, reviews/approves or enables auto-mode per job. May optionally link a personal Instagram account for the browser-automation collector, and/or use an external collector app that feeds this platform's ingestion API. |
| **Agent Job** | A configured, independent unit of work — has a niche, a target platform, an autonomy setting, and (for business-tracking jobs) a defined product/preference/insight profile. |
| **System (Collector Bots + Pipeline)** | Collects data, runs analysis, and generates content on behalf of each active agent job. |
| **External Client App** | *(out of scope to build here)* Any third-party or separately-built app — such as a watch-behavior collector — authorized to push data into this platform via the open ingestion API/Kafka topic, using the shared schema. |
| **Automation Worker** | A server-side browser-automation process, one of a pool, signed into a specific user's own linked account, used to fetch topic-specific content that user has asked the system to track. |

---

## 4. System Components

### 4.1 Data Collection (Free-Tier APIs)
- **Instagram**: public data available via Instagram's official Graph API /
  Basic Display API within free rate limits (hashtag/media lookups are
  restricted — scope collection to what the free tier actually permits; this
  should be confirmed during implementation, not assumed).
- **Reddit**: Reddit's free API (via PRAW or direct REST) for posts, comments,
  and engagement (upvotes, comment sentiment) on relevant subreddits.
- Official-API collection stays within each platform's rate limits and terms
  wherever it's used as the collection path.

### 4.1a External Ingestion (Kafka / REST API)
Rather than building a companion app, this platform exposes a consumer-side
contract for one:
- **Ingestion endpoint**: a documented endpoint (`POST /ingest/watch-events`
  or similar) and/or a Kafka topic that any authenticated external app can
  publish watch-event-style records to, using a shared schema.
- **Client registration**: each external app registers for an API key/client
  ID so ingested events can be attributed to the right user/app pair and basic
  rate limiting/abuse checks can be applied.
- **This project's scope stops at consuming this feed.** Building, deploying,
  or operating the app that produces this data (e.g., a background
  watch-behavior collector and any credit/reward mechanism for its users) is
  a separate, external effort and is not part of this requirements doc.

### 4.1b Browser-Automation Collector (Optional, Higher-Risk Path)
As an alternative or supplement to the official Instagram API's limited free
scope, the platform can run a pool of server-side browser instances, each
authenticated with a specific user's own personal Instagram account (with
that user's explicit login and consent), to browse and extract data on topics
the user has asked their agent job to track.

- **Per-user, consented linking only**: an instance only ever acts under a
  given user's own account, for that user's own agent jobs — never a shared
  or scraped-on-behalf-of-someone-else account.
- **Important risk note**: automating a personal account through a browser
  session like this falls outside Instagram's normal API terms of service for
  most platforms, and carries a real risk of the linked account being rate
  limited, flagged, or suspended. This should be called out explicitly to the
  user at link-time (not buried in fine print), and treated in the README as
  a known limitation/risk rather than a fully solved problem — the POC should
  not assume this path is risk-free just because the user personally
  consented to it.
- **Scope for the POC**: keep this path optional and off by default; the core
  demoable workflow (Section 4.7) should not depend on it working.

### 4.2 Data Processing Pipeline
- Normalizes collected posts/comments into a common schema (source, source
  type, author, text, image ref, engagement counts, timestamp).
- Sentiment classification of comments/reactions using a free pretrained model
  (e.g., a Hugging Face sentiment-analysis pipeline — no custom training needed).
- Basic engagement scoring (likes/upvotes + comments, normalized where possible).
- All three data channels (official API, external ingestion, browser
  automation) normalize into the same `CollectedItem` schema, tagged by
  `source_type`, so downstream analysis doesn't need to know where an item
  came from.

### 4.3 Image/Fashion Analysis
- Uses an existing free Hugging Face vision model for detailed image analysis —
  e.g., an image-captioning or visual-attribute model (such as a CLIP-based or
  BLIP-based model) to extract descriptive attributes (garment type, color,
  style, setting) from a post's image.
- Output feeds into the trend layer as structured attributes, not just "this
  image did well" but "a green dress performed well."

### 4.4 Trend Analysis (Lightweight, for POC)
- Aggregates collected + analyzed data to answer: which topics/attributes/
  content types are currently getting the most positive engagement, within the
  scope of a single agent job's niche.
- POC-level scope: simple ranking/aggregation over a recent time window is
  sufficient — no need for a full trend-lifecycle model unless time allows.

### 4.5 Content Generation Engine
- **Self-hosted, fine-tuned generation model** — an open-source LLM (e.g., a
  small Llama/Mistral-class model) fine-tuned or prompt-tuned per agent job's
  niche and tone, run locally/self-hosted rather than called via a third-party
  API.
- Each **agent job** has its own generation context: niche, target platform,
  tone, and (for business jobs) the user's product/preference/insight profile.
- Generates a post draft (caption + suggested hashtags/format) informed by the
  current trend analysis output for that job's niche.

### 4.6 Agent Job Configuration
- User creates an agent job by specifying:
  - Niche/topic (e.g., "LinkedIn tech content," "fashion," or a specific
    business like "my shoe shop")
  - Target platform
  - Optional data-source preferences: official API only, plus external
    ingestion feed if available, and/or the browser-automation collector for
    that user's linked account
  - For **business-tracking jobs specifically**: the user's product, stated
    preferences, and the specific insights they want (e.g., "track competitor
    pricing and trending shoe styles") — required before the job can run, so
    output stays targeted rather than generic
  - Autonomy setting: **manual review required** (default) or **auto-post
    enabled**
- Multiple agent jobs can exist independently and run in parallel, each with
  its own data scope and generation context.

### 4.7 Publishing
- POC scope: demonstrate posting to **one** platform end-to-end (pick whichever
  has the simplest free API path for automated posting, likely Reddit for the
  POC — Instagram's posting API has stricter business-account requirements).
- Manual-mode jobs surface the generated draft for user approval before
  posting; auto-mode jobs post directly.

---

## 5. Functional Requirements Summary

| # | Requirement |
|---|---|
| FR1 | System shall collect posts/comments/images from Instagram and Reddit free-tier APIs. |
| FR2 | System shall classify sentiment of collected comments/reactions using a pretrained model. |
| FR3 | System shall analyze image content using a free Hugging Face vision model to extract descriptive attributes. |
| FR4 | System shall aggregate collected + analyzed data into a simple trend ranking per agent job's niche. |
| FR5 | User shall be able to create one or more independent agent jobs, each with its own niche, platform, and autonomy setting. |
| FR6 | Business-tracking agent jobs shall require the user's product, preferences, and target insights before activation. |
| FR7 | System shall generate a post draft (caption + hashtags/format) via a self-hosted fine-tuned model, using the relevant agent job's trend context. |
| FR8 | System shall support manual-approval mode (default) and auto-post mode, set per agent job. |
| FR9 | System shall demonstrate one full publish path end-to-end (data → analysis → generated draft → posted). |
| FR10 | System shall expose a documented ingestion endpoint (REST and/or Kafka topic) that authenticated external client apps can push watch-event-style data to, without this project building the client app itself. |
| FR11 | System shall support an optional, per-user, consent-linked browser-automation collector that fetches topic-specific data via a user's own Instagram account, clearly disclosing the account-risk trade-off at link time. |
| FR12 | Ingested/automated data (from either channel in FR10/FR11) shall flow into the same `CollectedItem` pipeline as public-API data, tagged by `source_type`. |

---

## 6. Non-Functional Requirements (POC-appropriate)

- **Simplicity over scale**: no need to design for high throughput — this is a
  single-user demo, not a multi-tenant system.
- **Free-tier only**: every external API/model dependency must have a free
  usage path; no paid API keys required to run the demo.
- **Self-hosted generation model**: the content-generation step should not
  depend on a paid third-party LLM API, to keep the POC fully runnable without
  ongoing cost.
- **Clarity over completeness**: prioritize one clean, demoable workflow over
  partial coverage of many features.
- **Consent-first for account-linked collection**: any collection path that
  acts using a user's own account credentials (Section 4.1b) requires
  explicit, visible opt-in, and the linked-account risk should be disclosed,
  not buried.
- **This project does not build the external collector app**: FR10 is a
  consumer contract only; the app that produces that data is out of scope.

---

## 7. Data Model (Draft — POC scope)

- `AgentJob` (id, niche, target_platform, autonomy_mode, business_profile_ref[nullable])
- `BusinessProfile` (product_description, preferences, target_insights) — only
  present for business-tracking jobs
- `CollectedItem` (source_type: `public_api` | `external_ingested` |
  `account_automation`, raw_text, image_ref, engagement_counts, timestamp,
  agent_job_id)
- `ImageAttributes` (collected_item_id, extracted attributes from vision model)
- `SentimentResult` (collected_item_id, sentiment label, score)
- `TrendSnapshot` (agent_job_id, top attributes/topics, time_window, generated_at)
- `GeneratedDraft` (agent_job_id, content, source_trend_snapshot_id, status:
  pending/approved/posted)
- `ClientApp` (id, name, api_key, registered_at) — an external app registered
  to push data through the ingestion endpoint/Kafka topic
- `LinkedAccount` (user_id, platform, account_ref, consent_granted_at, status)
  — a user's own social account linked for the optional browser-automation
  collector

---

## 8. Out of Scope (POC)

- Multi-tenant / production-scale architecture.
- Paid API integrations of any kind.
- Full trend-lifecycle modeling (emerging/peaking/declining) — simple ranking
  is enough for the POC.
- **Building, deploying, or operating any external data-collector app** (e.g.,
  a companion mobile app that observes watch behavior) — this project only
  defines and consumes the ingestion contract (FR10); the producing app, if
  it exists, is a separate project.
- **Full abuse-prevention/fraud detection on the ingestion API** — basic rate
  limiting and API-key auth are enough for a POC; production-grade anti-fraud
  is out of scope but worth a one-line mention in the README as a known
  limitation.
- **Hardening the browser-automation collector against platform detection or
  enforcement** — it's built to work for a demo, not to be resilient against
  a platform actively working to stop this kind of automation; the account-
  suspension risk noted in 4.1b is treated as a known, disclosed limitation,
  not something the POC tries to fully engineer around.
- Training a model from scratch (vision or generation) — both use existing
  free/open models.

---

## 9. Suggested Build Order (single workflow demo)

1. Reddit collector (simplest free API, no business-account requirements) →
   normalize into `CollectedItem`.
2. Sentiment + free Hugging Face image-analysis pipeline on collected items.
3. Simple trend aggregation → `TrendSnapshot` for one agent job.
4. One `AgentJob` hardcoded/configured (e.g., "fashion trend page") with
   manual-approval mode.
5. Self-hosted generation model produces a `GeneratedDraft` from the trend
   snapshot.
6. Manual-approval UI/step → publish to Reddit.
7. Once this path works end-to-end, layer in: multiple agent jobs, the
   business-profile requirement, auto-post mode, and Instagram official-API
   collection.
8. Ingestion path (can be built in parallel once the schema is settled): wire
   up the `POST /ingest/watch-events` endpoint or Kafka consumer, `ClientApp`
   registration, and mapping into `CollectedItem` — this proves the "open,
   pluggable ingestion" idea without this project building the app on the
   other end of it.
9. Browser-automation collector (optional, do last, keep off by default):
   `LinkedAccount` consent flow → a small pool of automated browser sessions
   scoped to one user's own linked account → same `CollectedItem` mapping.
   Treat this as a stretch item — the core demo should already work without it.

This order intentionally front-loads the parts that prove the concept
(collection → analysis → generation → publish) and pushes configurability and
extra sources to the end, so you always have a demoable system even if you stop
partway through.
