# AGENTS.md — Root Rules for AI Coding Agents
Read by Antigravity, Cursor, and Claude Code. This file is intentionally a
short pointer, not a duplicate — the real rules live in `docs/rules.md`.

## Before doing anything
1. Read `docs/requirements.md`, `docs/architecture.md`, `docs/rules.md`, and
   `docs/phases.md` in full before proposing or making changes.
2. When asked to work inside `services/<name>/`, read that service's
   `services/<name>/CONTEXT.md` first — every service has one.
3. Never read another service's source to understand its behavior. The only
   cross-service dependency is `/contracts` (Kafka schemas, HTTP contracts).
   See `docs/rules.md` §1.

## Hard constraints (see docs/rules.md for the full numbered list)
- This is a POC. Nothing listed under "Out of Scope" in
  `docs/requirements.md` gets built, even incidentally.
- Free-tier and self-hosted only — no paid API keys, no paid model endpoints.
- Every change should map to a Functional Requirement (FR#) from
  `docs/requirements.md` §5 — reference it in the commit/PR description.
- Scope changes to one service at a time where possible.
- Don't start work from a later phase in `docs/phases.md` before the current
  phase's exit criterion is met.

## Current phase
Check `docs/phases.md` for the current phase and its exit criterion before
starting new work — don't assume the next phase is "ready" just because a
prior one is close to done.
