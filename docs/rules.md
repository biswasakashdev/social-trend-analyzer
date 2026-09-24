# Agent Rules — Social Media Trend Intelligence Platform
*(belongs at `docs/rules.md` in the repo)*

These are binding rules for any agent (or human) implementing this project.
Where a rule and a specific task instruction conflict, flag the conflict
rather than silently picking one.

---

## 1. Context & Repository Boundaries

1. **Read `services/<name>/CONTEXT.md` in full before touching that service.**
   It's the first file to open, every time — not something to skim after
   starting to code.
2. **Never open another service's source to understand its behavior.** If
   information isn't in that service's `CONTEXT.md` or in `/contracts`,
   that's a documentation gap — fix the doc, don't reverse-engineer from
   someone else's code.
3. **`/contracts` is the only cross-service dependency, ever.** Kafka event
   shapes, Go contract types, and HTTP contracts live there. A service may
   depend on a contract file; it may never depend on another service's
   implementation.
4. **Contract changes come first.** If a Kafka event needs a new field, or an
   HTTP endpoint needs a new response shape, edit the file under `/contracts`
   in the same change (or a change immediately before) implementing it — the
   contract is never a description written after the fact.
5. **Don't introduce a new library/framework into a service without checking
   its `CONTEXT.md` first.** If the need is genuinely new, add the smallest
   dependency that solves it and note it in that service's `CONTEXT.md`.

---

## 2. Scope Discipline & POC Engineering Posture

6. **Nothing from "Out of Scope" in `docs/requirements.md` gets built**, even
   if it would be a small addition while working nearby. Flag it as a future
   idea instead of quietly implementing it.
7. **POC engineering posture: prefer the simplest implementation that produces
   a correct result over the more "proper" one.** Default to the POC way
   (e.g., a scheduled batch job for spike detection instead of an over-engineered
   streaming aggregator) and note the production alternative in a one-line comment.
8. **No unrequested production hardening** — no Kubernetes manifests, no
   autoscaling, no multi-region anything, no complex circuit breakers or
   exponential backoff beyond basic immediate retry — beyond what
   `docs/architecture.md` already describes. This is a single-user demo.
9. **Every change should map to a Functional Requirement.** Reference the
   FR number (from `docs/requirements.md`, Section 6) in the commit/PR
   description. If a change doesn't map to one, question whether it belongs
   in the POC at all.
10. **Free-tier and self-hosted only.** No paid API keys, no paid model
    endpoints, anywhere in the stack — this is a hard constraint, not a
    preference.

---

## 3. Language-Specific Conventions

**Java — `core-api-service`**
11. Package-by-feature, not by layer: `agentjob/` contains its own
    controller, service, and repository — never a repo-wide `controllers/`
    folder.
12. Constructor injection only. No field injection (`@Autowired` on fields).
13. Schema changes go through Flyway migrations under
    `src/main/resources/db/migration/` — never manual/ad-hoc DDL.
14. Every public REST endpoint must match
    `contracts/http/core-api-service.openapi.yaml` — update the contract
    first if the endpoint shape needs to change.

**Go — `ingestion-service`**
15. Standard layout: `cmd/` for entrypoints, `internal/` for everything not
    meant to be imported elsewhere.
16. Wrap errors with context: `fmt.Errorf("consuming raw event: %w", err)`,
    never a bare `return err` across a package boundary.
17. No global mutable state. Every goroutine must have a clear shutdown path
    via `context.Context` cancellation — no goroutine that can't be stopped.

**Python — `ai-service`**
18. FastAPI app structured by feature (`sentiment/`, `vision/`,
    `generation/`, `fashion/`), matching the layout in `docs/architecture.md`.
19. Type hints required on all function signatures.
20. Models load once at process startup, not per-request — a request handler
    should never call a model-loading function.

**Next.js — `frontend`**
21. All API calls go through `lib/api/coreApiClient.ts`, typed against
    `contracts/http/core-api-service.openapi.yaml` — no `fetch()` calls
    scattered inside components or route handlers.
22. No real auth/login/signup work without being explicitly asked to start
    it. Until multi-tenancy work begins, `lib/auth/currentUser.ts` stays a
    stub — route through it, don't bypass it, but don't build out the real
    session logic behind it yet.

---

## 4. Data, Schema & Pipeline Rules

23. **`RawEvent` must never gain a platform-specific top-level field — use
    `Payload` instead.** The raw ingestion layer is strictly platform-agnostic.
    Any platform-peculiar fields that do not fit the common contract belong in
    `Payload json.RawMessage`.
24. **Downstream code must never switch/branch on `SourcePlatform` for business
    logic.** `SourcePlatform` is reserved exclusively for logging, provenance,
    and debugging. Category-based branching (`NormalizedEvent.Category`), post-
    categorization, is where domain-specific pipelines diverge.
25. **Comments fetching default (eager vs. lazy):**
    - **Eager**: If a platform adapter receives comments for free within the
      primary post API response, populate `RawEvent.Comments` at collection time.
    - **Lazy**: If fetching comments requires a separate API round-trip, do not
      fetch eagerly. Defer fetching until an event has survived categorization and
      a downstream domain processor explicitly requests comments.
26. **Fashion style vocabulary is centrally maintained and versioned.** The
    vocabulary for dress silhouettes, jewellery types, purse styles, and hairstyles
    is maintained as a versioned controlled vocabulary within the fashion domain worker (`ai-service`).
    Vision taggers must map against this controlled vocabulary, not infer arbitrary
    free-form strings, so that aggregation and spike detection remain consistent.
27. **Ingestion service preserves raw data to the MinIO Data Lake:** The ingestion
    service's sole responsibility is to consume from `social.engagement.raw` and store
    unaltered raw records directly into the MinIO Data Lake (S3 bucket `socialtrend-datalake`,
    key `{source_platform}/{YYYY-MM-DD}/{event_id}.json`) for downstream preservation and
    consumption by `ai-service`. It does not publish to multiple Kafka topics.
28. Every Kafka message published to `social.engagement.raw` must validate against its
    schema in `/contracts/kafka` before being sent — reject and log, don't publish
    malformed events "to be fixed later."
29. Prefer additive schema changes (new optional fields) so existing
    consumers don't break. A breaking change must update every producing
    service in the same change — never leave a producer on a stale schema.
30. `core-api-service` owns the only relational store. No other service
    writes to Postgres directly; Redis (if used) is cache/ephemeral only.

---

## 5. Platform Quotas, Limits & Privacy

31. **Instagram hashtag quota constraint:** Instagram Graph API limits hashtag
    queries to a maximum of **30 unique hashtags per rolling 7-day window** per
    connected Instagram Business account. Ingestion adapters must maintain a
    curated, rate-budgeted list and never attempt dynamic un-budgeted hashtag queries.
32. **Never attempt to fetch comment text or watch-time for content not owned
    by our account.** Watch time / average view duration on unowned public media
    is a permanent platform limitation of Instagram Graph Insights. Treat
    `RawEvent.AvgWatchTimeSeconds` as nil for Instagram sources — this is a known
    platform constraint, not a feature gap.
33. **No credential-based scraping or account automation.** All social data
    must be acquired via official public APIs, approved feeds, or manual imports.
34. **No auto-publishing to a live platform without explicit operator approval.**
    Manual review (`ContentSuggestion` pending approval) is the default; auto-post
    is an explicit opt-in setting.

---

## 6. Testing & Quality

35. Every new endpoint or Kafka consumer needs at least one test that
    exercises it against its actual contract (OpenAPI schema or JSON schema),
    not just a hand-rolled happy-path fixture.
36. Nothing gets committed without a passing local build/test for that
    service — the exact commands are in that service's `CONTEXT.md`.
37. Focused testing on high-risk logic: normalization, category classification,
    and spike-scoring are the areas most prone to silent errors. Keep unit test
    coverage sharp on these components rather than writing extensive end-to-end mocks.

---

## 7. Change Management

38. `docs/architecture.md` and `docs/requirements.md` change only when Akash
    directs it. An agent implementing a feature should not redesign scope or
    architecture along the way, even if it seems like an improvement — raise
    it instead.
39. Commits/PRs are scoped to one service where possible
    (`core-api: add agent job endpoint`), mirroring how a real team would
    work this repo and keeping any single agent's diff easy to review.
