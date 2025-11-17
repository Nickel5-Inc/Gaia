from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
import numpy as np
from pathlib import Path
from typing import Optional, Any, Dict, Tuple

from gaia.utils.custom_logger import get_logger
logger = get_logger(__name__)

import sqlalchemy as sa

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
# REMOVED: MinerWorkScheduler import - no longer needed since run() method was removed
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
from gaia.tasks.defined_tasks.weather.processing.weather_logic import (
    calculate_era5_miner_score,
)
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from gaia.tasks.defined_tasks.weather.utils.era5_api import fetch_era5_data
from .step_logger import log_start, log_success, log_failure, schedule_retry
from .substep import substep
from .util_time import get_effective_gfs_init




class DataNotReadyError(Exception):
    """Exception raised when ERA5 data is not yet available for scoring."""
    def __init__(self, message: str, next_ready_at: Optional[datetime] = None):
        super().__init__(message)
        self.next_ready_at = next_ready_at


# Lightweight in-process cache for ERA5 truth per (run_id, leads)
_era5_truth_cache: Dict[Tuple[int, Tuple[int, ...]], Any] = {}


async def _get_era5_truth(task: WeatherTask, run_id: int, gfs_init, leads: list[int], *, prefer_cache_only: bool = False):
    key = (run_id, tuple(leads))
    # Avoid asyncio Lock; concurrent reads are safe. If racing, later write just overwrites same reference.
    ds = _era5_truth_cache.get(key)
    if ds is not None:
        return ds

    have_lock = False
    # Single-worker guard: only acquire advisory lock when doing heavy fetches
    if not prefer_cache_only:
        lock_key = (0x45524135 ^ int(run_id))  # prefix 'ERA5' xor run_id
        try:
            db = task.db_manager
            row = await db.fetch_one("SELECT pg_try_advisory_lock(:key) AS ok", {"key": lock_key})
            have_lock = bool(row and row.get("ok"))
        except Exception:
            have_lock = False

    target_datetimes = [gfs_init + timedelta(hours=h) for h in leads]
    ds = None
    try:
        cache_dir_str = task.config.get("era5_cache_dir", "./era5_cache")
        # FAST PATH: cache-only read; no lock, no wait
        if prefer_cache_only:
            try:
                use_progressive_fetch = task.config.get("progressive_era5_fetch", True)
                if use_progressive_fetch:
                    from ...data.acquisition import fetch_era5_data_progressive
                    ds = await fetch_era5_data_progressive(
                        target_datetimes, cache_dir=Path(cache_dir_str), cache_only=True
                    )
                else:
                    ds = None
            except Exception:
                ds = None
        elif have_lock:
            # Use progressive fetch for consistency with finalize worker and better caching
            use_progressive_fetch = task.config.get("progressive_era5_fetch", True)
            if use_progressive_fetch:
                from gaia.tasks.defined_tasks.weather.utils.era5_api import fetch_era5_data_progressive
                ds = await fetch_era5_data_progressive(
                    target_datetimes, cache_dir=Path(cache_dir_str), cache_only=False
                )
            else:
                ds = await fetch_era5_data(
                    target_datetimes, cache_dir=Path(cache_dir_str)
                )
        else:
            # Non-lock holders: do not hit API; attempt cache-only load without delay
            try:
                use_progressive_fetch = task.config.get("progressive_era5_fetch", True)
                if use_progressive_fetch:
                    from ...data.acquisition import fetch_era5_data_progressive
                    ds = await fetch_era5_data_progressive(
                        target_datetimes, cache_dir=Path(cache_dir_str), cache_only=True
                    )
                else:
                    # Non-progressive path has no cache-only mode; skip to None
                    ds = None
            except Exception:
                ds = None
    finally:
        if have_lock:
            try:
                await task.db_manager.execute("SELECT pg_advisory_unlock(:key)", {"key": lock_key})
            except Exception:
                pass
    if ds is not None:
        _era5_truth_cache[key] = ds
    return ds


# REMOVED: Unused run() method that was never called in current execution paths
# All ERA5 scoring now goes through run_item() method which is called by workers.py


async def run_item(
    db: ValidatorDatabaseManager,
    *,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    response_id: int,
    validator: Optional[Any] = None,
) -> bool:
    """Process ERA5 for a specific miner/run item (used by generic queue dispatcher)."""
    # Reuse a single WeatherTask per worker via validator-carried singleton
    task = None
    if validator is not None:
        task = getattr(validator, "weather_task_singleton", None)
    if task is None:
        task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
        if validator is not None:
            try:
                setattr(validator, "weather_task_singleton", task)
            except Exception:
                pass
    if validator is not None:
        setattr(task, "validator", validator)
    # Pull response details
    resp = await db.fetch_one(
        "SELECT id, run_id, miner_uid, miner_hotkey, job_id, verification_passed, status FROM weather_miner_responses WHERE id = :rid",
        {"rid": response_id},
    )
    if not resp:
        return False
    
    # CRITICAL: Validate miner workflow isolation - prevent any crossover between miners
    if resp["run_id"] != run_id:
        logger.error(
            f"[ISOLATION VIOLATION] Response {response_id} belongs to run {resp['run_id']} "
            f"but job specifies run {run_id}. Aborting to prevent crossover."
        )
        return False
    
    if resp["miner_uid"] != miner_uid:
        logger.error(
            f"[ISOLATION VIOLATION] Response {response_id} belongs to miner UID {resp['miner_uid']} "
            f"but job specifies miner UID {miner_uid}. Aborting to prevent crossover."
        )
        return False
    # Gate ERA5 scoring on verified manifest to prevent TOCTOU swaps
    try:
        if not bool(resp.get("verification_passed")) or resp.get("status") != "verified_manifest_store_opened":
            logger.warning(
                f"[ERA5] Run {run_id} Miner {miner_uid}: Skipping ERA5 scoring - manifest not verified (verification_passed={resp.get('verification_passed')}, status={resp.get('status')})"
            )
            return False
    except Exception:
        return False

    run = await db.fetch_one(
        "SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :rid",
        {"rid": run_id},
    )
    if not run or not run.get("gfs_init_time_utc"):
        return False
    gfs_init = run["gfs_init_time_utc"]
    leads: list[int] = task.config.get(
        "final_scoring_lead_hours", [24, 48, 72, 96, 120, 144, 168, 192, 216, 240]
    )
    # OPTIONAL OVERRIDE: Use per-lead scheduling from steps table if present
    try:
        override_rows = await db.fetch_all(
            sa.text(
                """
                SELECT DISTINCT lead_hours
                FROM weather_forecast_steps
                WHERE run_id = :rid AND miner_uid = :uid
                  AND step_name = 'era5' AND substep = 'score'
                  AND lead_hours IS NOT NULL
                  AND status IN ('pending', 'retry_scheduled', 'queued', 'in_progress')
                ORDER BY lead_hours
                """
            ),
            {"rid": run_id, "uid": miner_uid},
        )
        override_leads = [int(r.get("lead_hours")) for r in override_rows if r and r.get("lead_hours") is not None]
        if override_leads:
            allowed = set(leads)
            leads = sorted([h for h in override_leads if h in allowed])
            if not leads:
                # If override produced no valid leads, fail fast to avoid useless attempts
                logger.info(f"[ERA5] Run {run_id} Miner {miner_uid}: override leads present but none valid; skipping")
                return False
    except Exception:
        pass

    # If this step row is waiting_for_truth, avoid any API access and return DataNotReady to complete job
    try:
        st = await db.fetch_one(
            sa.text(
                """
                SELECT status FROM weather_forecast_steps
                WHERE run_id = :rid AND miner_uid = :uid
                  AND step_name = 'era5' AND substep = 'score'
                LIMIT 1
                """
            ),
            {"rid": run_id, "uid": miner_uid},
        )
        if st and st.get("status") == "waiting_for_truth":
            raise DataNotReadyError("waiting_for_truth step row")
    except DataNotReadyError:
        raise
    except Exception:
        pass
    # Existing score check
    rows = await db.fetch_all(
        sa.text(
            """
            SELECT lead_hours, score
            FROM weather_miner_scores
            WHERE run_id = :rid
              AND miner_uid = :uid
              AND response_id = :resp_id
              AND score_type LIKE 'era5_rmse_%'
              AND lead_hours IS NOT NULL
            """
        ),
        {"rid": run_id, "uid": miner_uid, "resp_id": resp["id"]},
    )
    existing_scores: Dict[int, float] = {}
    for r in rows:
        lh = r.get("lead_hours")
        sc = r.get("score")
        if lh is not None and sc is not None:
            existing_scores[int(lh)] = float(sc)
    if len([h for h in leads if h in existing_scores]) >= len(leads):
        # Metrics already exist for all requested leads. Compute/write normalized per-lead and overall via aggregator,
        # rather than writing raw RMSE into stats.
        try:
            from gaia.tasks.defined_tasks.weather.processing.weather_logic import _calculate_and_store_aggregated_era5_score
            vars_levels = task.config.get(
                "final_scoring_variables_levels",
                task.config.get("day1_variables_levels_to_score", []),
            )
            await _calculate_and_store_aggregated_era5_score(
                task_instance=task,
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                response_id=resp["id"],
                lead_hours_scored=leads,
                vars_levels_scored=vars_levels,
            )
        except Exception as agg_err:
            logger.debug(f"[ERA5Step] Skipped normalized aggregate write due to error: {agg_err}")

        # Update status to completed without overwriting normalized scores
        stats = WeatherStatsManager(
            db,
            validator_hotkey=(
                getattr(getattr(getattr(validator, "validator_wallet", None), "hotkey", None), "ss58_address", None)
                if validator is not None
                else "unknown_validator"
            ),
        )
        await stats.update_forecast_stats(
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            status="completed",
        )
        return True
    delay_days = int(getattr(task.config, "era5_delay_days", task.config.get("era5_delay_days", 5))) if hasattr(task, "config") else 5
    buffer_hours = int(getattr(task.config, "era5_buffer_hours", task.config.get("era5_buffer_hours", 6))) if hasattr(task, "config") else 6
    now_utc = datetime.now(timezone.utc)
    if getattr(task, "test_mode", False):
        # In test mode, run immediately after Day1 completes
        delay_days = 0
        buffer_hours = 0
    pending_leads = [h for h in leads if h not in existing_scores]
    def needed_time_for_lead(h: int) -> datetime:
        return gfs_init + timedelta(hours=h) + timedelta(days=delay_days) + timedelta(hours=buffer_hours)
    
    # Group leads by day for efficient daily scoring
    from collections import defaultdict
    leads_by_day = defaultdict(list)
    for h in pending_leads:
        forecast_time = gfs_init + timedelta(hours=h)
        day_key = forecast_time.strftime("%Y-%m-%d")
        leads_by_day[day_key].append(h)
    
    # PROGRESSIVE SCHEDULING: Find ALL ready lead times, not just one day
    ready_leads = []
    ready_days = []
    for day_key, day_leads in leads_by_day.items():
        # Check if any lead from this day is ready
        day_ready_leads = [h for h in day_leads if now_utc >= needed_time_for_lead(h)]
        if day_ready_leads:
            ready_leads.extend(day_ready_leads)
            ready_days.append(day_key)
    
    # Sort ready leads for consistent processing order
    ready_leads = sorted(ready_leads)
    
    # Dependency rule: do not attempt higher leads if an earlier lead exhausted retries
    try:
        min_expected = min(pending_leads + ready_leads) if (pending_leads or ready_leads) else None
        if min_expected is not None:
            from sqlalchemy import text as _text
            row_block = await db.fetch_one(
                _text(
                    """
                    SELECT 1
                    FROM weather_forecast_steps
                    WHERE run_id = :rid AND miner_uid = :uid AND step_name = 'era5' AND substep = 'score'
                      AND lead_hours < :lh
                      AND retry_count >= 3
                      AND status = 'failed'
                    LIMIT 1
                    """
                ),
                {"rid": run_id, "uid": miner_uid, "lh": int(min_expected)},
            )
            if row_block:
                logger.warning(f"[ERA5] Run {run_id} Miner {miner_uid}: Blocking higher leads due to exhausted retries on earlier lead")
                return False
    except Exception:
        pass

    if not ready_leads:
        # Calculate when the earliest lead will be ready
        next_ready_time = min(needed_time_for_lead(h) for h in pending_leads)
        hours_until_ready = (next_ready_time - now_utc).total_seconds() / 3600
        logger.debug(
            f"[ERA5] Run {run_id} Miner {miner_uid}: No ERA5 data ready yet. "
            f"Next lead ready in {hours_until_ready:.1f} hours at {next_ready_time}"
        )
        # Move step to waiting_for_truth and schedule next retry based on min(predicted_ready, now+poll_interval)
        try:
            poll_minutes = int(getattr(task.config, "era5_poll_interval_minutes", task.config.get("era5_poll_interval_minutes", 60))) if hasattr(task, "config") else 60
            from datetime import timedelta as _td
            next_poll_time = now_utc + _td(minutes=poll_minutes)
            nrt = next_ready_time if next_ready_time <= next_poll_time else next_poll_time
            await db.execute(
                """
                UPDATE weather_forecast_steps
                SET status = 'waiting_for_truth', next_retry_time = :nrt
                WHERE run_id = :rid AND miner_uid = :uid AND step_name = 'era5' AND substep = 'score'
                """,
                {"rid": run_id, "uid": miner_uid, "nrt": nrt},
            )
        except Exception:
            pass
        # Signal not-ready to caller
        raise DataNotReadyError(
            f"ERA5 data not ready for {hours_until_ready:.1f} hours",
            next_ready_at=next_ready_time,
        )
    logger.info(f"[ERA5] Run {run_id} Miner {miner_uid}: Progressive scoring {len(ready_days)} days with {len(ready_leads)} lead times: {ready_leads}")
    try:
        logger.debug(f"[ERA5] Run {run_id} Miner {miner_uid}: Fetching ERA5 truth for {len(ready_leads)} targets")
    except Exception:
        pass
    
    # Load truth/climatology for only the ready leads (one day at a time)
    # Try cache-only first (fast path). If missing, attempt guarded fetch to populate cache.
    truth = await _get_era5_truth(task, run_id, gfs_init, ready_leads, prefer_cache_only=True)
    if truth is None:
        try:
            truth = await _get_era5_truth(task, run_id, gfs_init, ready_leads, prefer_cache_only=False)
        except Exception as e_truth:
            try:
                import traceback as _tb
                logger.error(
                    f"[ERA5] Truth fetch failed for run {run_id}, miner {miner_uid}: {type(e_truth).__name__}: {e_truth}\n{''.join(_tb.format_exc(limit=5))}"
                )
            except Exception:
                pass
            truth = None
    try:
        n_times = 0
        if truth is not None:
            try:
                n_times = len(getattr(truth, "indexes", None) and truth.indexes.get("time") or getattr(truth, "time", []))
            except Exception:
                n_times = 0
        logger.debug(f"[ERA5] Run {run_id} Miner {miner_uid}: Truth dataset ready (time_count={n_times})")
    except Exception:
        pass
    try:
        logger.debug(f"[ERA5] Run {run_id} Miner {miner_uid}: Loading ERA5 climatology")
    except Exception:
        pass
    clim = await task._get_or_load_era5_climatology()
    try:
        logger.debug(f"[ERA5] Run {run_id} Miner {miner_uid}: Climatology loaded: {bool(clim)}")
    except Exception:
        pass
    if not truth or not clim:
        # Treat this as "truth not ready" rather than a hard failure; schedule retry
        try:
            poll_minutes = int(getattr(task.config, "era5_poll_interval_minutes", task.config.get("era5_poll_interval_minutes", 60))) if hasattr(task, "config") else 60
            from datetime import timedelta as _td
            nrt = datetime.now(timezone.utc) + _td(minutes=poll_minutes)
            for _lh in ready_leads:
                try:
                    await db.execute(
                        """
                        UPDATE weather_forecast_steps
                        SET status = 'waiting_for_truth', next_retry_time = :nrt
                        WHERE run_id = :rid AND miner_uid = :uid AND step_name = 'era5' AND substep = 'score' AND lead_hours = :lh
                        """,
                        {"rid": run_id, "uid": miner_uid, "lh": int(_lh), "nrt": nrt},
                    )
                except Exception:
                    pass
        except Exception:
            pass
        return False

    # Validate truth actually contains requested times; refine ready_leads accordingly
    try:
        ds_times = getattr(truth, "indexes", None) and truth.indexes.get("time")
        if ds_times is None:
            ds_times = getattr(truth, "time", None) and truth["time"].values
        def _has_time(dt: datetime) -> bool:
            if ds_times is None:
                return False
            try:
                # Convert to numpy datetime64 matching seconds precision; drop tz for compare
                dt64 = np.datetime64(dt.replace(tzinfo=None))
            except Exception:
                return False
            try:
                arr = np.array(ds_times)
            except Exception:
                arr = ds_times
            try:
                # Exact match check
                return np.any(arr == dt64)
            except Exception:
                # Fallback: iterate with tolerance of 60s
                try:
                    for t in np.array(arr):
                        try:
                            delta = np.abs((dt64 - t).astype('timedelta64[s]').astype(int))
                            if delta <= 60:
                                return True
                        except Exception:
                            continue
                except Exception:
                    pass
                return False

        original_ready = list(ready_leads)
        confirmed_ready_leads = []
        for h in ready_leads:
            target_dt = gfs_init + timedelta(hours=h)
            if _has_time(target_dt):
                confirmed_ready_leads.append(h)
        if not confirmed_ready_leads:
            # No matching times; mark failed rather than mislabel as truth gate
            try:
                from gaia.tasks.defined_tasks.weather.pipeline.steps.step_logger import _upsert as _sl_upsert
                for _lh in ready_leads:
                    try:
                        await _sl_upsert(
                            db,
                            run_id=run_id,
                            miner_uid=miner_uid,
                            miner_hotkey=miner_hotkey,
                            step_name="era5",
                            substep="score",
                            lead_hours=int(_lh),
                            status="failed",
                            completed_at=datetime.now(timezone.utc),
                            error_json={"type": "truth_times_missing"},
                        )
                    except Exception:
                        pass
            except Exception:
                pass
            return False
        # Use refined list
        ready_leads = sorted(confirmed_ready_leads)
    except DataNotReadyError:
        raise
    except Exception:
        # If any error during refinement, proceed with original but expect scoring to guard internally
        pass
    miner_record = {
        "id": resp["id"],
        "miner_hotkey": miner_hotkey,
        "run_id": run_id,
        "miner_uid": miner_uid,
        "job_id": resp.get("job_id"),
    }
    # Mark per-lead steps as in_progress now (ensures we don't re-enqueue them)
    try:
        from gaia.tasks.defined_tasks.weather.pipeline.steps.step_logger import _upsert as _sl_upsert
        now_ts = datetime.now(timezone.utc)
        for _lh in ready_leads:
            try:
                await _sl_upsert(
                    db,
                    run_id=run_id,
                    miner_uid=miner_uid,
                    miner_hotkey=miner_hotkey,
                    step_name="era5",
                    substep="score",
                    lead_hours=int(_lh),
                    status="in_progress",
                    started_at=now_ts,
                )
            except Exception:
                pass
    except Exception:
        pass
    @substep("era5", "score", should_retry=False)
    async def _score_era5_item(db, task: WeatherTask, *, run_id: int, miner_uid: int, miner_hotkey: str):
        return await calculate_era5_miner_score(
            task_instance=task,
            miner_response_rec=miner_record,
            target_datetimes=[gfs_init + timedelta(hours=h) for h in ready_leads],
            era5_truth_ds=truth,
            era5_climatology_ds=clim,
        )
    import time
    t0 = time.perf_counter()
    try:
        # Execute scoring without an overall timeout; rely on internal guards and error handling
        ok = await _score_era5_item(
            db,
            task,
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
        )
    except Exception as e:
        import traceback as _tb
        tb_text = _tb.format_exc()
        logger.exception(
            f"[ERA5] Exception during scoring for run {run_id} miner {miner_uid} leads={ready_leads}: {e}\nTRACEBACK:\n{tb_text}"
        )
        ok = False
    if not ok:
        # Build descriptive error message from latest response/error state
        detailed_msg = "ERA5 scoring failed"
        detailed_reason = "scoring_failed"
        try:
            resp_state = await db.fetch_one(
                "SELECT status, error_message FROM weather_miner_responses WHERE id = :rid",
                {"rid": resp["id"]},
            )
            # Check for last verified open error
            try:
                from gaia.tasks.defined_tasks.weather.utils.remote_access import get_last_verified_open_error as _get_last_open_err
                last_open_err = _get_last_open_err(resp.get("job_id"))
            except Exception:
                last_open_err = None
            if resp_state and resp_state.get("error_message"):
                detailed_msg = str(resp_state.get("error_message"))
            elif last_open_err:
                detailed_msg = f"Verified-open error: {last_open_err}"
            # Set a more precise reason when possible
            if resp_state and str(resp_state.get("status", "")).startswith("verification"):
                detailed_reason = "verification_failed"
            elif "verification" in detailed_msg.lower() or "chunk" in detailed_msg.lower():
                detailed_reason = "verification_failed"
        except Exception:
            pass
        # Mark per-lead steps as failed to prevent re-enqueue
        try:
            from gaia.tasks.defined_tasks.weather.pipeline.steps.step_logger import log_failure
            for _lh in ready_leads:
                try:
                    await log_failure(
                        db,
                        run_id=run_id,
                        miner_uid=miner_uid,
                        miner_hotkey=miner_hotkey,
                        step_name="era5",
                        substep="score",
                        lead_hours=int(_lh),
                        error_json={
                            "type": "scoring_failed",
                            "reason": detailed_reason,
                            "message": detailed_msg,
                            "context": {"run_id": run_id, "miner_uid": miner_uid, "lead_hours": int(_lh)},
                        },
                    )
                    try:
                        await db.execute(
                            "UPDATE weather_forecast_steps SET job_id = NULL WHERE run_id = :rid AND miner_uid = :uid AND step_name = 'era5' AND substep = 'score' AND lead_hours = :lh",
                            {"rid": run_id, "uid": miner_uid, "lh": int(_lh)},
                        )
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            pass
        await log_failure(
            db,
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            step_name="era5",
            substep="score",
            error_json={
                "type": "scoring_failed",
                "reason": detailed_reason,
                "message": detailed_msg,
                "context": {"run_id": run_id, "miner_uid": miner_uid, "ready_leads": ready_leads},
            },
        )
        # Ensure no reschedule by returning True (job completed) and leaving step rows in failed state
        return True
    latency_ms = int((time.perf_counter() - t0) * 1000)
    try:
        logger.info(
            f"[ERA5] Run {run_id} Miner {miner_uid}: Scoring complete for leads {ready_leads} in {latency_ms}ms"
        )
    except Exception:
        pass

    # After scoring, compute normalized per-lead scores and overall (writes to weather_forecast_stats)
    try:
        from gaia.tasks.defined_tasks.weather.processing.weather_logic import _calculate_and_store_aggregated_era5_score
        # Use task.config variables list; fallback to day1 list if unset
        vars_levels = task.config.get(
            "final_scoring_variables_levels",
            task.config.get("day1_variables_levels_to_score", []),
        )
        # Call aggregator to compute per-lead normalized scores and blended overall score
        await _calculate_and_store_aggregated_era5_score(
            task_instance=task,
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            response_id=resp["id"],
            lead_hours_scored=ready_leads,
            vars_levels_scored=vars_levels,
        )
    except Exception as agg_err:
        logger.debug(f"[ERA5Step] Skipped normalized aggregate write due to error: {agg_err}")
    # Aggregate/update stats
    try:
        completed_now = len(ready_leads) >= len(leads)
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
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            status=status,
            # Per-lead normalized scores are written by processing.weather_logic during final scoring; avoid overwriting here
            hosting_status=None,
            hosting_latency_ms=None,
            avg_rmse=None,
            avg_acc=None,
            avg_skill_score=None,
        )
        
        # PIPELINE TIMING: Record ERA5 completion time and calculate total duration
        if completed_now:  # Only when fully completed
            await db.execute(
                """
                UPDATE weather_miner_responses 
                SET era5_scoring_completed_at = NOW(),
                    total_pipeline_duration_seconds = EXTRACT(EPOCH FROM (NOW() - job_accepted_at))::INTEGER
                WHERE run_id = :run_id AND miner_uid = :miner_uid
                """,
                {"run_id": run_id, "miner_uid": miner_uid}
            )
            logger.info(f"[ERA5Step] Recorded ERA5 completion and total pipeline duration for miner {miner_uid}")
        # Mark per-lead steps as succeeded
        try:
            from gaia.tasks.defined_tasks.weather.pipeline.steps.step_logger import log_success
            for _lh in ready_leads:
                try:
                    await log_success(
                        db,
                        run_id=run_id,
                        miner_uid=miner_uid,
                        miner_hotkey=miner_hotkey,
                        step_name="era5",
                        substep="score",
                        lead_hours=int(_lh),
                        latency_ms=latency_ms,
                    )
                except Exception:
                    pass
        except Exception:
            pass
        await log_success(
            db,
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            step_name="era5",
            substep="score",
            latency_ms=latency_ms,
        )
        # NEW: Update combined final score row in score_table when ERA5 is computed
        try:
            combined_task = getattr(validator, "weather_task_singleton", None)
            if combined_task is None:
                combined_task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
                try:
                    setattr(validator, "weather_task_singleton", combined_task)
                except Exception:
                    pass
            await combined_task.update_combined_weather_scores(
                run_id_trigger=run_id, force_phase="final"
            )
            logger.info(
                f"[ERA5Step] Updated score_table final row for run {run_id}"
            )
        except Exception as e_comb:
            logger.warning(
                f"[ERA5Step] Failed to update combined final score row for run {run_id}: {e_comb}"
            )
    except Exception:
        pass
    return True

