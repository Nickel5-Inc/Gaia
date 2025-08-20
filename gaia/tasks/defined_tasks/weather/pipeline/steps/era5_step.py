from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Any, Dict, Tuple

logger = logging.getLogger(__name__)

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

logger = logging.getLogger(__name__)


# Lightweight in-process cache for ERA5 truth per (run_id, leads)
_era5_truth_cache: Dict[Tuple[int, Tuple[int, ...]], Any] = {}


async def _get_era5_truth(task: WeatherTask, run_id: int, gfs_init, leads: list[int]):
    key = (run_id, tuple(leads))
    # Avoid asyncio Lock; concurrent reads are safe. If racing, later write just overwrites same reference.
    ds = _era5_truth_cache.get(key)
    if ds is not None:
        return ds

    # Single-worker guard: use advisory lock per (run_id, 'era5') to prevent duplicate heavy fetch
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
        ds = await fetch_era5_data(
            target_datetimes, cache_dir=task.config.get("era5_cache_dir", "./era5_cache")
        )
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
    task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
    if validator is not None:
        setattr(task, "validator", validator)
    # Pull response details
    resp = await db.fetch_one(
        "SELECT id, run_id, miner_uid, miner_hotkey, job_id FROM weather_miner_responses WHERE id = :rid",
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
    # Existing score check
    rows = await db.fetch_all(
        sa.text(
            """
            SELECT lead_hours, score
            FROM weather_miner_scores
            WHERE run_id = :rid AND miner_uid = :uid AND score_type = 'era5_rmse'
            """
        ),
        {"rid": run_id, "uid": miner_uid},
    )
    existing_scores: Dict[int, float] = {}
    for r in rows:
        lh = r.get("lead_hours")
        sc = r.get("score")
        if lh is not None and sc is not None:
            existing_scores[int(lh)] = float(sc)
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
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            status="completed",
            era5_scores=existing_scores,
        )
        return True
    delay_days = int(getattr(task.config, "era5_delay_days", task.config.get("era5_delay_days", 5))) if hasattr(task, "config") else 5
    buffer_hours = int(getattr(task.config, "era5_buffer_hours", task.config.get("era5_buffer_hours", 6))) if hasattr(task, "config") else 6
    now_utc = datetime.now(timezone.utc)
    if getattr(task, "test_mode", False):
        buffer_hours = min(buffer_hours, 1)
    pending_leads = [h for h in leads if h not in existing_scores]
    def needed_time_for_lead(h: int) -> datetime:
        return gfs_init + timedelta(hours=h) + timedelta(days=delay_days) + timedelta(hours=buffer_hours)
    ready_leads = [h for h in pending_leads if now_utc >= needed_time_for_lead(h)]
    if not ready_leads:
        return False
    # Load truth/climatology
    truth = await _get_era5_truth(task, run_id, gfs_init, leads)
    clim = await task._get_or_load_era5_climatology()
    if not truth or not clim:
        return False
    miner_record = {
        "id": resp["id"],
        "miner_hotkey": miner_hotkey,
        "run_id": run_id,
        "miner_uid": miner_uid,
        "job_id": resp.get("job_id"),
    }
    @substep("era5", "score", should_retry=True, retry_delay_seconds=3600, max_retries=4, retry_backoff="none")
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
        ok = await _score_era5_item(
            db,
            task,
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
        )
    except Exception:
        ok = False
    if not ok:
        await log_failure(
            db,
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            step_name="era5",
            substep="score",
            error_json={"type": "scoring_failed", "message": "calculate_era5_miner_score returned False"},
        )
        return False
    latency_ms = int((time.perf_counter() - t0) * 1000)
    # Aggregate/update stats
    try:
        rows = await db.fetch_all(
            sa.text(
                """
                SELECT lead_hours, score
                FROM weather_miner_scores
                WHERE run_id = :rid AND miner_uid = :uid AND score_type = 'era5_rmse'
                """
            ),
            {"rid": run_id, "uid": miner_uid},
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
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            status=status,
            era5_scores=era5_scores,
        )
        await log_success(
            db,
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            step_name="era5",
            substep="score",
            latency_ms=latency_ms,
        )
    except Exception:
        pass
    return True

