# frontend — Context for Agents

## Purpose
The UI — **Next.js** (App Router), TypeScript. Lets a user create/configure
agent jobs, view the current trend snapshot for a job, and review/approve
generated content suggestions. Contains no business logic of its own — it's a
thin client over `core-api-service`'s API.

Built as a **single-user app for now**, but structured so multi-tenancy
(multiple people logging in, each with their own agent jobs) can be added
later without restructuring the app — see "Future: Multi-Tenancy" below.
Multi-tenancy itself is **not implemented yet**.

## Reads / Calls
- `core-api-service`'s public REST API — contract:
  `/contracts/http/core-api-service.openapi.yaml`, accessed only through
  `lib/api/coreApiClient.ts`.
- Never calls `ai-service` or `ingestion-service` directly — every
  interaction goes through `core-api-service`.

## Does NOT own
- Any business logic, persistence, orchestration, or model calls — all of
  that happens server-side in `core-api-service` (which itself delegates
  analysis/generation to `ai-service`). If a screen needs new data or a new
  action, the fix is a new/changed `core-api-service` endpoint (and contract
  update), not client-side logic standing in for it.
- Real authentication/authorization — not built yet. A stub current-user
  concept stands in until multi-tenancy lands (see below). Don't add a real
  login flow without being asked to start that work specifically.
- User/tenant scoping of data — once accounts exist, deciding which
  `AgentJob`s belong to which user is a `core-api-service` concern, not
  something this app filters client-side.

## Project Layout (App Router)
```
app/
├── layout.tsx
├── page.tsx                    # landing/dashboard
├── agent-jobs/
│   ├── page.tsx                 # list/create agent jobs
│   └── [jobId]/page.tsx         # trend dashboard for one agent job
├── suggestions/
│   └── page.tsx                 # review pending ContentSuggestions
└── (auth)/                      # reserved — not implemented yet, see below
components/
lib/
├── api/
│   └── coreApiClient.ts         # the ONLY place HTTP calls to core-api-service are made
└── auth/
    └── currentUser.ts           # stub now (returns a fixed demo user) — see below
middleware.ts                    # reserved — not implemented yet, see below
```
See `docs/rules.md` §3: all API calls go through `coreApiClient.ts`, typed
against `core-api-service.openapi.yaml` — no `fetch()` calls scattered inside
components.

## Future: Multi-Tenancy (planned, not built yet)
The app is laid out now so this is an additive change later, not a rewrite:

- **Auth**: planned via Auth.js (NextAuth) for login/session handling, with
  routes under `app/(auth)/` (currently an empty reserved folder).
- **Route protection**: `middleware.ts` (currently a reserved empty file)
  will gate authenticated routes once login exists.
- **Current-user indirection already in place**: `lib/auth/currentUser.ts`
  is a stub today — it returns a fixed demo user — but every data-fetching
  call already goes through it rather than assuming a single global dataset.
  Swapping the stub for a real session lookup should not require touching
  `app/agent-jobs/`, `app/suggestions/`, or `coreApiClient.ts`.
- **Backend implication (not this service's work, flagged for awareness)**:
  once real accounts exist, `core-api-service` will need to scope `AgentJob`,
  `BusinessProfile`, `TrendSnapshot`, and `ContentSuggestion` per user —
  that's a `core-api-service`/`docs/requirements.md` data-model change to
  raise separately when multi-tenancy work actually starts, not something to
  pre-build from the frontend side.
- Until that work starts, treat this as a **single implicit user** — don't
  add real login, signup, or per-user data isolation yet.

## Functional Requirements This Service Supports
Surfaces FR6 (agent job creation, including the `BusinessProfile` fields
required for business-tracking jobs), FR10 (manual-review vs. auto-suggest
toggle), and the review step implied by FR9/FR11 in `docs/requirements.md` §5.

## Run Standalone
```
# core-api-service must be running and reachable
cd services/frontend
pnpm install
pnpm dev
```

## Test
```
cd services/frontend
pnpm test
```
