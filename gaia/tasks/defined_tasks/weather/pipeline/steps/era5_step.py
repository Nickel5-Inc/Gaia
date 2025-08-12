from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, Any, Dict, Tuple

import sqlalchemy as sa

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.pipeline.scheduler import MinerWorkScheduler
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
from gaia.tasks.defined_tasks.weather.processing.weather_logic import (
    calculate_era5_miner_score,
)
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from gaia.tasks.defined_tasks.weather.utils.era5_api import fetch_era5_data
from .step_logger import log_start, log_success, log_failure, schedule_retry
from .substep import substep
from .util_time import get_effective_gfs_init


# Lightweight in-process cache for ERA5 truth per (run_id, leads)
_era5_truth_cache: Dict[Tuple[int, Tuple[int, ...]], Any] = {}


async def _get_era5_truth(task: WeatherTask, run_id: int, gfs_init, leads: list[int]):
    key = (run_id, tuple(leads))
    # Avoid asyncio Lock; concurrent reads are safe. If racing, later write just overwrites same reference.
    ds = _era5_truth_cache.get(key)
    if ds is not None:
        return ds

    target_datetimes = [gfs_init + timedelta(hours=h) for h in leads]
    ds = await fetch_era5_data(
        target_datetimes, cache_dir=task.config.get("era5_cache_dir", "./era5_cache")
    )
    if ds is not None:
        _era5_truth_cache[key] = ds
    return ds


async def run(db: ValidatorDatabaseManager, validator: Optional[Any] = None) -> bool:
    """Claim and process a single ERA5 scoring step for one miner, update stats."""
    sched = MinerWorkScheduler(db)
    item = await sched.claim_era5()
    if not item:
        return False

    # Build WeatherTask for helpers
    task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
    if validator is not None:
        setattr(task, "validator", validator)

    # Pull response details (requires job_id and miner_hotkey)
    resp = await db.fetch_one(
        "SELECT id, run_id, miner_uid, miner_hotkey, job_id FROM weather_miner_responses WHERE id = :rid",
        {"rid": item.response_id},
    )
    if not resp:
        return False

    # Prepare target datetimes for ERA5 based on run's GFS init time and configured leads
    run = await db.fetch_one(
        "SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :rid",
        {"rid": item.run_id},
    )
    if not run or not run.get("gfs_init_time_utc"):
        return False
    gfs_init = get_effective_gfs_init(task, run["gfs_init_time_utc"])
    leads: list[int] = task.config.get(
        "final_scoring_lead_hours", [24, 48, 72, 96, 120, 144, 168, 192, 216, 240]
    )

    # Check existing ERA5 scores to avoid redundant work and compute completeness
    rows = await db.fetch_all(
        sa.text(
            """
            SELECT lead_hours, score
            FROM weather_miner_scores
            WHERE run_id = :rid AND miner_uid = :uid AND score_type = 'era5_rmse'
            """
        ),
        {"rid": item.run_id, "uid": item.miner_uid},
    )
    existing_scores: Dict[int, float] = {}
    for r in rows:
        lh = r.get("lead_hours")
        sc = r.get("score")
        if lh is not None and sc is not None:
            existing_scores[int(lh)] = float(sc)
            # Log per-lead completion
            try:
                await log_success(
                    db,
                    run_id=item.run_id,
                    miner_uid=item.miner_uid,
                    miner_hotkey=item.miner_hotkey,
                    step_name="era5",
                    substep="lead",
                    lead_hours=int(lh),
                )
            except Exception:
                pass

    # If already complete, just update stats and return
    if len([h for h in leads if h in existing_scores]) >= len(leads):
        stats = WeatherStatsManager(
            db,
            validator_hotkey=(
                getattr(getattr(getattr(validator, "validator_wallet", None), "hotkey", None), "ss58_address", None)
                if validator is not None
                else "unknown_validator"
            ),
        )
        await stats.update_forecast_stats(
            run_id=item.run_id,
            miner_uid=item.miner_uid,
            miner_hotkey=item.miner_hotkey,
            status="completed",
            era5_scores=existing_scores,
        )
        return True

    # Progressive availability: filter to leads whose ERA5 should be available now
    delay_days = int(getattr(task.config, "era5_delay_days", task.config.get("era5_delay_days", 5))) if hasattr(task, "config") else 5
    buffer_hours = int(getattr(task.config, "era5_buffer_hours", task.config.get("era5_buffer_hours", 6))) if hasattr(task, "config") else 6
    now_utc = datetime.now(timezone.utc)
    # In test mode, treat short window as immediately available to accelerate
    if getattr(task, "test_mode", False):
        buffer_hours = min(buffer_hours, 1)
    pending_leads = [h for h in leads if h not in existing_scores]
    def needed_time_for_lead(h: int) -> datetime:
        return gfs_init + timedelta(hours=h) + timedelta(days=delay_days) + timedelta(hours=buffer_hours)
    ready_leads = [h for h in pending_leads if now_utc >= needed_time_for_lead(h)]
    if not ready_leads:
        # Schedule next retry at earliest needed time
        if pending_leads:
            next_t = min(needed_time_for_lead(h) for h in pending_leads)
            try:
                await schedule_retry(
                    db,
                    run_id=item.run_id,
                    miner_uid=item.miner_uid,
                    miner_hotkey=item.miner_hotkey,
                    step_name="era5",
                    substep=None,
                    error_json={"type": "era5_not_ready"},
                    retry_count=1,
                    next_retry_time=next_t,
                )
            except Exception:
                pass
        return False

    @substep("era5", "load_truth", should_retry=True, retry_delay_seconds=1800, max_retries=6, retry_backoff="exponential")
    async def _load_truth(db, task: WeatherTask, *, run_id: int, miner_uid: int, miner_hotkey: str):
        truth = await _get_era5_truth(task, run_id, gfs_init, leads)
        clim = await task._get_or_load_era5_climatology()
        if not truth or not clim:
            raise RuntimeError("truth or climatology not available")
        return truth, clim

    try:
        era5_truth_ds, era5_clim = await _load_truth(
            db,
            task,
            run_id=item.run_id,
            miner_uid=item.miner_uid,
            miner_hotkey=item.miner_hotkey,
        )
    except Exception:
        return False

    # Prepare miner record
    miner_record = {
        "id": resp["id"],
        "miner_hotkey": resp["miner_hotkey"],
        "run_id": resp["run_id"],
        "miner_uid": resp["miner_uid"],
        "job_id": resp.get("job_id"),
    }

    # Run ERA5 scoring for this miner
    @substep("era5", "score", should_retry=True, retry_delay_seconds=3600, max_retries=4, retry_backoff="none")
    async def _score_era5(db, task: WeatherTask, *, run_id: int, miner_uid: int, miner_hotkey: str):
        return await calculate_era5_miner_score(
            task_instance=task,
            miner_response_rec=miner_record,
            target_datetimes=[gfs_init + timedelta(hours=h) for h in ready_leads],
            era5_truth_ds=era5_truth_ds,
            era5_climatology_ds=era5_clim,
        )
    import time
    t0 = time.perf_counter()
    # Only score ready leads to progress incrementally
    try:
        ok = await _score_era5(
            db,
            task,
            run_id=item.run_id,
            miner_uid=item.miner_uid,
            miner_hotkey=item.miner_hotkey,
        )
    except Exception:
        ok = False
    if not ok:
        await log_failure(
            db,
            run_id=item.run_id,
            miner_uid=item.miner_uid,
            miner_hotkey=item.miner_hotkey,
            step_name="era5",
            substep="score",
            error_json={"type": "scoring_failed", "message": "calculate_era5_miner_score returned False"},
        )
        # schedule retry
        try:
            from gaia.tasks.defined_tasks.weather.pipeline.retry_policy import next_retry_time as _nrt
            nrt = _nrt("era5", 1)
            await schedule_retry(
                db,
                run_id=item.run_id,
                miner_uid=item.miner_uid,
                miner_hotkey=item.miner_hotkey,
                step_name="era5",
                substep="score",
                error_json={"type": "scoring_failed"},
                retry_count=1,
                next_retry_time=nrt,
            )
        except Exception:
            pass
        return False
    latency_ms = int((time.perf_counter() - t0) * 1000)

    # After scoring, aggregate ERA5 scores for this miner/run and upsert stats
    try:
        rows = await db.fetch_all(
            sa.text(
                """
                SELECT lead_hours, score
                FROM weather_miner_scores
                WHERE run_id = :rid AND miner_uid = :uid AND score_type = 'era5_rmse'
                """
            ),
            {"rid": item.run_id, "uid": item.miner_uid},
        )
        era5_scores = {}
        for r in rows:
            lh = r.get("lead_hours")
            sc = r.get("score")
            if lh is not None and sc is not None:
                era5_scores[int(lh)] = float(sc)

        completed_now = len([h for h in leads if h in era5_scores]) >= len(leads)
        status = "completed" if completed_now else "era5_scoring"
        stats = WeatherStatsManager(
            db,
            validator_hotkey=(
                getattr(getattr(getattr(validator, "validator_wallet", None), "hotkey", None), "ss58_address", None)
                if validator is not None
                else "unknown_validator"
            ),
        )
        await stats.update_forecast_stats(
            run_id=item.run_id,
            miner_uid=item.miner_uid,
            miner_hotkey=item.miner_hotkey,
            status=status,
            era5_scores=era5_scores,
        )
        await log_success(
            db,
            run_id=item.run_id,
            miner_uid=item.miner_uid,
            miner_hotkey=item.miner_hotkey,
            step_name="era5",
            substep="score",
            latency_ms=latency_ms,
        )
        # If not complete, schedule next retry at earliest not-yet-ready lead
        if not completed_now:
            remaining = [h for h in leads if h not in era5_scores]
            if remaining:
                next_t = min(needed_time_for_lead(h) for h in remaining)
                try:
                    await schedule_retry(
                        db,
                        run_id=item.run_id,
                        miner_uid=item.miner_uid,
                        miner_hotkey=item.miner_hotkey,
                        step_name="era5",
                        substep=None,
                        error_json={"type": "era5_more_leads_pending"},
                        retry_count=1,
                        next_retry_time=next_t,
                    )
                except Exception:
                    pass
        # Per-step aggregation: update miner's aggregates now that ERA5 advanced
        try:
            await stats.aggregate_miner_stats(miner_uid=item.miner_uid)
        except Exception:
            pass

        # Run completion check after potential finalization
        from gaia.tasks.defined_tasks.weather.processing.weather_logic import _check_run_completion
        try:
            await _check_run_completion(task, item.run_id)
        except Exception:
            pass
    except Exception:
        pass

    return True


