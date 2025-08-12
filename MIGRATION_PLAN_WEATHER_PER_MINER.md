# Migration Plan: Per-Miner, Event-Driven Weather Pipeline

## Overall Goal
- Simplify weather validation into per-miner, event-driven “threads” with independent state and retries.
- Persist state in DB; keep workers stateless except when actively processing.
- Incrementally migrate without breaking current runs; keep code small and readable.

## Current State Summary
- Run orchestration relies on batch loops for verification → day1 → ERA5.
- Per-miner data: `weather_miner_responses` (state, retries), `weather_miner_scores` (scores), `weather_forecast_stats` and `miner_stats` (reporting).
- Jobs: `weather_scoring_jobs` is run-scoped (not per-miner).

## Options and Recommendations
- Keep schema unchanged for MVP; rely on existing tables and indexes.
- Use Postgres as a queue (FOR UPDATE SKIP LOCKED) and asyncio worker pools.
- Incrementally update `weather_forecast_stats`/`miner_stats` on each per-miner step.
- Later consider Dask/Celery once per-miner idempotent steps are stable.

## Phased Migration

### Phase 0: Observability (in progress)
- Ensure stats upserts at every transition.
- Verify indexes/queries for stats.

### Phase 1: Introduce per-miner state machine and scheduler (MVP)
- Add `pipeline/state_machine.py` with states and guards.
- Add `pipeline/scheduler.py` with selectors for next verification/day1/era5 work (SKIP LOCKED aware).
- Add dry-run execution that logs candidates (no side effects) behind a feature flag.

### Phase 2: Replace batch loops with per-miner jobs
- Dispatch per-miner coroutines for verification/day1/era5 steps.
- Idempotent, retryable steps, DB writes per step.
- Minimal run-level coordination remains (seeding only).

### Phase 3: Incremental aggregation and ranks
- Update stats on each step completion; recompute per-run ranks for changed miners only.
- Update `miner_stats` incrementally; optional periodic tidy.

### Phase 4: Optional distributed pool
- Introduce Dask/Celery if needed; reuse same state machine and DB step writes.

## Backwards Compatibility
- Miner protocol unchanged.
- Existing tables remain; new scheduler sits alongside batch loops initially (feature-flagged).

## Success Criteria
- Failures isolated to individual miners/steps.
- Immediate progress visible via `weather_forecast_stats`.
- Reduced batch-induced stalls; improved readability and maintainability.


