## Goals

- Unify and simplify weather stats schema for per-miner, per-run observability and recovery
- Eliminate dual calculations by writing metrics at the source step
- Support fine-grained step/sub-step logging, retries, and idempotency
- Keep miner-facing behavior unchanged; minimize breaking app changes
- Make subnet-wide aggregation efficient for the web server

## Current Relevant Tables (from `gaia/database/validator_schema.py`)

- `weather_forecast_runs`: one row per forecast run (validator-initiated)
- `weather_miner_responses`: one row per miner per run; holds verify/job info, coarse `status`, error
- `weather_miner_scores`: per-miner scores with `score_type`, optional `lead_hours`, `variable_level`, `metrics` JSON
- `weather_forecast_stats`: per-miner per-run rollup; includes many fixed ERA5 columns (`era5_score_24h`..`240h`), combined score, completeness, hosting metrics
- `miner_stats`: per-miner aggregate over all runs
- (Others: ensemble tables, scoring jobs, baseline/geomagnetic/soil moisture — not central here)

## Issues in Current Shape

- Fixed ERA5 columns in `weather_forecast_stats` couple schema to a single cadence; hard to extend (e.g., different lead spacing or forecast types)
- Mixed responsibilities: `weather_forecast_stats` stores both per-lead values (wide) and rollups
- Coarse step/state visibility: most transitions live in `weather_miner_responses.status`, with limited sub-step/error context and retry metadata
- Recovery rules are implicit and scattered; limited idempotency keys
- Aggregations recalc later instead of at the step boundary

## Design Principles

- Single source of truth for per-lead/per-variable scores (narrow, append/update friendly)
- Explicit step/sub-step event log per miner per run with retries, latency, and sanitized errors
- Keep a thin rollup table or materialized view to speed web queries
- Idempotency keys for all writes; permissive yet safe state transitions

## Proposed Schema Changes (Phased)

Phase 1 (Additive; no breaking changes):

- Add `weather_forecast_steps` (event log per miner per run):
  - `id` (PK)
  - `run_id` (FK -> weather_forecast_runs.id)
  - `miner_uid` (FK -> node_table.uid)
  - `miner_hotkey` (text)
  - `step_name` (enum/text): e.g., `verify`, `day1`, `era5`
  - `substep` (text, nullable): e.g., `token_request`, `open_store`, `lead_24`, etc.
  - `lead_hours` (int, nullable) for ERA5 progress granularity
  - `status` (enum/text): `pending`, `in_progress`, `succeeded`, `failed`, `retry_scheduled`
  - `started_at`, `completed_at` (timestamptz)
  - `retry_count` (int, default 0), `next_retry_time` (timestamptz, nullable)
  - `latency_ms` (int, nullable)
  - `error_json` (JSONB, sanitized)
  - `context` (JSONB, nullable) for ancillary metadata
  - Unique constraint: (`run_id`, `miner_uid`, `step_name`, `substep`, `lead_hours`) to support idempotent upserts
  - Indexes: (`run_id`, `miner_uid`), (`step_name`, `status`), (`next_retry_time`) for recovery scanning

- Keep `weather_miner_scores` as the single per-lead/variable score store
  - Rely on this table for all ERA5 (and day1 if per-lead) metrics
  - Add index helpers if needed (e.g., (`run_id`,`miner_uid`,`score_type`,`lead_hours`))

- Retain `weather_forecast_stats` but simplify writes
  - Treat it as a rollup sink updated at the end of each step
  - Compute `era5_combined_score` and `era5_completeness` from `weather_miner_scores` (no need for fixed `era5_score_*` columns)
  - Deprecate writing per-lead columns in `weather_forecast_stats` (see Phase 2)

Phase 2 (Migration/simplification):

- Remove fixed ERA5 columns from `weather_forecast_stats` (`era5_score_24h`..`240h`)
- Option A: Convert `weather_forecast_stats` to a materialized view over `weather_miner_scores` + `weather_forecast_steps` for rollups
- Option B: Keep as a table but only store rollups (`forecast_score_initial`, `era5_combined_score`, `era5_completeness`, hosting metrics); no per-lead columns

Phase 3 (Optional refinements):

- Add `weather_subnet_stats` (materialized view) for subnet snapshots (top/bottom miners, means)
- Add `weather_retry_policies` (configurable per step) to drive recovery scheduling

## Pros / Cons

Pros:
- Clear, modular step/sub-step visibility; easier debugging and metrics
- Narrow score storage scales naturally with different cadences and forecast types
- Idempotent writes via composite keys; simpler localized retries
- Rollups can be recomputed deterministically from events/scores

Cons:
- Requires migration and updating query paths
- Slightly more joins for ad-hoc queries (mitigated by views/materialized views)

## Impacted Code Paths

- Scheduler: can prefer scanning `weather_forecast_steps` where status = `retry_scheduled` per step
- Steps (verify/day1/era5): log sub-steps (`token_request`, `open_store`, `lead_XX`) into `weather_forecast_steps`; write sanitized `error_json`; update retry schedule
- Stats manager: compute rollups by querying `weather_miner_scores` and/or materialized view; stop writing fixed per-lead columns
- Web/API: fetch per-run, per-miner timelines straight from `weather_forecast_steps`

## Example DDL (illustrative)

```sql
CREATE TABLE weather_forecast_steps (
  id BIGSERIAL PRIMARY KEY,
  run_id INTEGER NOT NULL REFERENCES weather_forecast_runs(id) ON DELETE CASCADE,
  miner_uid INTEGER NOT NULL REFERENCES node_table(uid) ON DELETE CASCADE,
  miner_hotkey VARCHAR(255) NOT NULL,
  step_name VARCHAR(32) NOT NULL,
  substep VARCHAR(64),
  lead_hours INTEGER,
  status VARCHAR(32) NOT NULL,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  retry_count INTEGER DEFAULT 0,
  next_retry_time TIMESTAMPTZ,
  latency_ms INTEGER,
  error_json JSONB,
  context JSONB,
  UNIQUE (run_id, miner_uid, step_name, substep, lead_hours)
);

CREATE INDEX idx_wfs_steps_run_miner ON weather_forecast_steps(run_id, miner_uid);
CREATE INDEX idx_wfs_steps_status ON weather_forecast_steps(step_name, status);
CREATE INDEX idx_wfs_steps_retry ON weather_forecast_steps(next_retry_time);
```

## Migration Plan

1) Add `weather_forecast_steps` table; keep existing tables intact
2) Update step implementations to write sub-step events + errors + retries
3) Update `WeatherStatsManager` to compute rollups from `weather_miner_scores` and stop writing fixed ERA5 columns
4) Backfill: derive steps from existing response/status fields where possible
5) After validation, remove fixed ERA5 columns from `weather_forecast_stats` (Phase 2)
6) Optional: add materialized views for subnet stats

## Backward Compatibility

- Phase 1 is non-breaking; existing consumers continue to work
- Phase 2 will remove deprecated columns; we will coordinate update of any consumers beforehand


