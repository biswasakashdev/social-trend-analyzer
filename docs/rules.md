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
   shapes and HTTP contracts live there. A service may depend on a contract
   file; it may never depend on another service's implementation.
4. **Contract changes come first.** If a Kafka event needs a new field, or an
   HTTP endpoint needs a new response shape, edit the file under `/contracts`
   in the same change (or a change immediately before) implementing it — the
   contract is never a description written after the fact.
5. **Don't introduce a new library/framework into a service without checking
   its `CONTEXT.md` first.** If the need is genuinely new, add the smallest
   dependency that solves it and note it in that service's `CONTEXT.md`.

---

## 2. Scope Discipline (this is a POC)

6. **Nothing from "Out of Scope" in `docs/requirements.md` gets built**, even
   if it would be a small addition while working nearby. Flag it as a future
   idea instead of quietly implementing it.
7. **No unrequested production hardening** — no Kubernetes manifests, no
   autoscaling, no multi-region anything — beyond what `docs/architecture.md`
   already describes. This is a single-user demo.
8. **Every change should map to a Functional Requirement.** Reference the
   FR number (from `docs/requirements.md`, Section 5) in the commit/PR
   description. If a change doesn't map to one, question whether it belongs
   in the POC at all.
9. **Free-tier and self-hosted only.** No paid API keys, no paid model
   endpoints, anywhere in the stack — this is a hard constraint, not a
   preference.

---

## 3. Language-Specific Conventions

**Java — `core-api-service`**
10. Package-by-feature, not by layer: `agentjob/` contains its own
    controller, service, and repository — never a repo-wide `controllers/`
    folder.
11. Constructor injection only. No field injection (`@Autowired` on fields).
12. Schema changes go through Flyway migrations under
    `src/main/resources/db/migration/` — never manual/ad-hoc DDL.
13. Every public REST endpoint must match
    `contracts/http/core-api-service.openapi.yaml` — update the contract
    first if the endpoint shape needs to change.

**Go — `ingestion-service`**
14. Standard layout: `cmd/` for entrypoints, `internal/` for everything not
    meant to be imported elsewhere.
15. Wrap errors with context: `fmt.Errorf("consuming raw event: %w", err)`,
    never a bare `return err` across a package boundary.
16. No global mutable state. Every goroutine must have a clear shutdown path
    via `context.Context` cancellation — no goroutine that can't be stopped.

**Python — `ai-service`**
17. FastAPI app structured by feature (`sentiment/`, `vision/`,
    `generation/`), matching the layout in `docs/architecture.md`.
18. Type hints required on all function signatures.
19. Models load once at process startup, not per-request — a request handler
    should never call a model-loading function.

**Next.js — `frontend`**
20. All API calls go through `lib/api/coreApiClient.ts`, typed against
    `contracts/http/core-api-service.openapi.yaml` — no `fetch()` calls
    scattered inside components or route handlers.
21. No real auth/login/signup work without being explicitly asked to start
    it. Until multi-tenancy work begins, `lib/auth/currentUser.ts` stays a
    stub — route through it, don't bypass it, but don't build out the real
    session logic behind it yet.

---

## 4. Data & Schema Rules

22. Every Kafka message a service publishes must validate against its schema
    in `/contracts/kafka` before being sent — reject and log, don't publish
    malformed events "to be fixed later."
23. Prefer additive schema changes (new optional fields) so existing
    consumers don't break. A breaking change must update every consuming
    service in the same change — never leave a consumer on a stale schema.
24. `core-api-service` owns the only relational store. No other service
    writes to Postgres directly; Redis (if used) is cache/ephemeral only.

---

## 5. Testing & Quality

25. Every new endpoint or Kafka consumer needs at least one test that
    exercises it against its actual contract (OpenAPI schema or JSON schema),
    not just a hand-rolled happy-path fixture.
26. Nothing gets committed without a passing local build/test for that
    service — the exact commands are in that service's `CONTEXT.md`.
27. Prefer contract fixtures (from `/contracts`) over hand-written mocks when
    testing cross-service behavior, so tests fail if the real contract
    changes underneath them.

---

## 6. Consent, Privacy & Risk

28. No credential-based scraping or account automation unless it's explicitly
    approved as a data source in `docs/requirements.md` — this platform
    currently only *consumes* data via Kafka/REST; it does not collect it.
29. Any feature acting on a user's own account credentials requires an
    explicit, visible consent step — never a silent background action.
30. No auto-publishing to a live platform without the autonomy setting (FR10
    in `docs/requirements.md`) being what's actually being implemented —
    manual review is the default, not an afterthought.

---

## 7. Change Management

31. `docs/architecture.md` and `docs/requirements.md` change only when Akash
    directs it. An agent implementing a feature should not redesign scope or
    architecture along the way, even if it seems like an improvement — raise
    it instead.
32. Commits/PRs are scoped to one service where possible
    (`core-api: add agent job endpoint`), mirroring how a real team would
    work this repo and keeping any single agent's diff easy to review.
