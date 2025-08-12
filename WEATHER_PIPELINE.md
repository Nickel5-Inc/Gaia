## Weather Pipeline Overview

This document defines the complete per-run, per-miner pipeline, including sub-steps, retries, and logging.

### Entities
- `weather_forecast_runs`: one row per validator-initiated run.
- `weather_forecast_stats`: one row per miner per run (255 expected), fixed ERA5 columns preserved for plotting.
- `weather_forecast_steps`: event log for sub-steps with status, latency, retries, and sanitized errors.
- `weather_miner_scores`: per-lead/per-variable score records.

### Lifecycle

1) Seed Run (validator-only, once per run)
   - Create `weather_forecast_runs` row.
   - `seed_forecast_run` (code: `steps/seed_step.py:seed_forecast_run`): bulk upsert 255 rows into `weather_forecast_stats` with `forecast_status='seeded'` and insert `verify` pending steps into `weather_forecast_steps`.
   - On reused active runs we backfill any missing miners (idempotent).
   - Sub-steps (implemented):
     - `seed/stats_rows` (idempotent upsert)
     - `seed/steps_rows` (idempotent upsert)
     - `seed/download_gfs` (run-level, decorated `ensure_gfs_reference_available`): ensure GFS analysis + reference are fetched/warmed into cache for `gfs_init` (hindcast-shifted in test mode).

2) Verify (per miner)
   - Sub-steps:
     - `verify/open_store`: token → zarr open (latency), success/failure with retry scheduling. Implemented in `verify_step.run`.
   - On failure: schedule retry with exponential backoff.
   - On success: set response `verification_passed`; stats update with hosting metrics.

3) Day1 Scoring (per miner)
   - One-time per run cache: `day1/clim_cache` (code: `steps/clim_cache.py:ensure_clim_cache`), persisted to disk; workers read it read-only. Cleanup after all miners past day1 (code: `cleanup_clim_cache_if_done`).
   - Sub-steps (decorated in `steps/day1_step.py`):
     - `day1/load_inputs`: fetch GFS analysis & reference; load ERA5 climatology.
     - `day1/score`: compute QC metrics; write `weather_miner_scores` and update `weather_forecast_stats.forecast_score_initial` and status `day1_scored`.
   - On failure: schedule retry.
   - After step: per-miner aggregation and optional cache cleanup when all miners are done/failed.

4) ERA5 Progressive Scoring (per miner)
   - Leads considered ready at `gfs_init + lead_hours + delay_days + buffer_hours` (code: `steps/era5_step.py`).
   - Sub-steps (decorated in `steps/era5_step.py`):
     - `era5/load_truth`: ensure ERA5 truth & climatology datasets available.
     - `era5/lead`: logged upon each completed lead score (inserted as success events when scores exist).
     - `era5/score`: score only ready leads; write `weather_miner_scores`; update `weather_forecast_stats` completeness and combined score. If pending leads remain, schedule next retry at earliest availability.
   - If no leads ready: schedule retry at earliest not-yet-ready lead time.
   - Repeat until all leads complete; then set `status='completed'` for this miner/run.

5) Aggregations
   - Per-step: update `miner_stats` for the affected miner.
   - Periodic: subnet snapshot via `aggregate_step`.

### Scheduler
- Prefers `weather_forecast_steps` with `status='retry_scheduled'` for verify/day1/era5 (implemented in `pipeline/scheduler.py`).
- Falls back to legacy selection while transition completes.

### Error Handling
- All failures are logged to `weather_forecast_steps.error_json` (sanitized) and optionally reflected in `weather_forecast_stats.forecast_error_msg`.
- Retries are configured per sub-step via decorator parameters:
  - `should_retry` (bool), `retry_delay_seconds` (int), `max_retries` (int), `retry_backoff` (`exponential|linear|none`).
  - ERA5 additionally schedules per-lead/date availability retries.

### Database Tracking and Idempotency
- All sub-steps are stored in `weather_forecast_steps` with a unique key `(run_id, miner_uid, step_name, substep, lead_hours)` to prevent duplication.
- Run-level sub-steps use `miner_uid=0, miner_hotkey='coordinator'` to indicate coordinator actions.
- Progress/state transitions are derived from step success events; no in-memory state is relied upon for correctness.

### Current Implementation Map
- Seed: `steps/seed_step.py:seed_forecast_run`
- Verify: `steps/verify_step.py:run`
- Day1: `steps/day1_step.py: _load_inputs (decorated), ensure_clim_cache (run-level), _score_day1 (decorated)`
- ERA5: `steps/era5_step.py: _load_truth (decorated), per-lead success logging, _score_era5 (decorated)`, progressive scheduling
- Aggregation: `steps/aggregate_step.py` and periodic loop in `validator.py`

### Test Mode Behavior
- When `task.test_mode=True`:
  - Sub-step decorators shorten retry base delays to ~5 seconds for fast iteration.
  - GFS init time is shifted backwards by `test_hindcast_days`/`test_hindcast_hours` to form hindcasts; used consistently by Day1 and ERA5 steps.
  - ERA5 progressive scoring uses reduced delays when scheduling the next attempt.
  - Seed/backfill and per-step logging still persist all records for observability.

### Concurrency and Safety
- Read-only datasets can be opened concurrently; ERA5 truth cached per-process.
- Day1 climatology cache persisted to disk; single-flight via atomic lock file.
- Database-level idempotency via unique constraints on steps and stats.


