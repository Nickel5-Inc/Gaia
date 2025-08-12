from __future__ import annotations

import time
from typing import Optional, Any

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.pipeline.scheduler import MinerWorkScheduler
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
from gaia.tasks.defined_tasks.weather.processing.weather_logic import (
    verify_miner_response,
)
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
import sqlalchemy as sa
from .step_logger import log_start, log_success, log_failure, schedule_retry


def _derive_hosting_status(verification_passed: bool, status: Optional[str]) -> str:
    if verification_passed:
        return "accessible"
    if status == "verification_timeout":
        return "timeout"
    if status == "verification_error":
        return "error"
    # awaiting_miner_completion, retry_scheduled, others
    return "inaccessible"


async def run(db: ValidatorDatabaseManager, validator: Optional[Any] = None) -> bool:
    """Claim and process a single verify step, and upsert hosting metrics to stats."""
    sched = MinerWorkScheduler(db)
    item = await sched.claim_to_verify()
    if not item:
        return False

    run_details = await db.fetch_one(
        "SELECT id, gfs_init_time_utc, status FROM weather_forecast_runs WHERE id = :rid",
        {"rid": item.run_id},
    )
    if not run_details:
        return False

    response_details = await db.fetch_one(
        "SELECT id, miner_hotkey, job_id FROM weather_miner_responses WHERE id = :rid",
        {"rid": item.response_id},
    )
    if not response_details:
        return False

    task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
    if validator is not None:
        setattr(task, "validator", validator)

    t0 = time.perf_counter()
    await log_start(
        db,
        run_id=item.run_id,
        miner_uid=item.miner_uid,
        miner_hotkey=item.miner_hotkey,
        step_name="verify",
        substep="open_store",
    )
    await verify_miner_response(task, run_details, response_details)
    elapsed_ms = int((time.perf_counter() - t0) * 1000)

    # Re-fetch verify outcome and error message to populate stats
    post = await db.fetch_one(
        """
        SELECT verification_passed, status, error_message
        FROM weather_miner_responses
        WHERE id = :rid
        """,
        {"rid": item.response_id},
    )
    if not post:
        return True  # DB updated by verify; nothing else we can do

    hosting_status = _derive_hosting_status(bool(post.get("verification_passed")), post.get("status"))
    err_msg = post.get("error_message")

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
        status=post.get("status") or "verify_complete",
        error_msg=err_msg,
        hosting_status=hosting_status,
        hosting_latency_ms=elapsed_ms,
    )
    # Log sub-step outcome and potential retry schedule
    if post.get("verification_passed"):
        await log_success(
            db,
            run_id=item.run_id,
            miner_uid=item.miner_uid,
            miner_hotkey=item.miner_hotkey,
            step_name="verify",
            substep="open_store",
            latency_ms=elapsed_ms,
        )
    else:
        # fetch potential retry info
        retry_row = await db.fetch_one(
            sa.text("SELECT retry_count, next_retry_time FROM weather_miner_responses WHERE id = :rid"),
            {"rid": item.response_id},
        )
        retry_count = retry_row and retry_row.get("retry_count")
        next_retry_time = retry_row and retry_row.get("next_retry_time")
        from gaia.validator.stats.weather_stats_manager import WeatherStatsManager as _WSM
        sanitized = _WSM.sanitize_error_message(err_msg) if err_msg else None
        # if no legacy scheduling, compute next retry
        if next_retry_time is None:
            try:
                from gaia.tasks.defined_tasks.weather.pipeline.retry_policy import next_retry_time as _nrt
                nrt = _nrt("verify", (retry_count or 0) + 1)
                retry_count = (retry_count or 0) + 1
                next_retry_time = nrt
            except Exception:
                pass
        await log_failure(
            db,
            run_id=item.run_id,
            miner_uid=item.miner_uid,
            miner_hotkey=item.miner_hotkey,
            step_name="verify",
            substep="open_store",
            latency_ms=elapsed_ms,
            error_json=sanitized,
            retry_count=retry_count,
            next_retry_time=next_retry_time,
        )
        if next_retry_time is not None:
            await schedule_retry(
                db,
                run_id=item.run_id,
                miner_uid=item.miner_uid,
                miner_hotkey=item.miner_hotkey,
                step_name="verify",
                substep="open_store",
                error_json=sanitized,
                retry_count=retry_count or 1,
                next_retry_time=next_retry_time,
            )

    # Per-step aggregation hook: keep miner_stats fresh for this miner
    try:
        await stats.aggregate_miner_stats(miner_uid=item.miner_uid)
    except Exception:
        pass

    return True


