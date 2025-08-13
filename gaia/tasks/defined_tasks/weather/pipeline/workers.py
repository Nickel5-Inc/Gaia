from __future__ import annotations

import asyncio
from typing import Optional, Dict, Any
import sqlalchemy as sa

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.pipeline.scheduler import MinerWorkScheduler
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
from gaia.tasks.defined_tasks.weather.pipeline.steps import verify_step, day1_step, era5_step
from gaia.tasks.defined_tasks.weather.weather_scoring_mechanism import (
    evaluate_miner_forecast_day1,
)
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from gaia.tasks.defined_tasks.weather.utils.gfs_api import (
    fetch_gfs_analysis_data,
    fetch_gfs_data,
)


async def process_verify_one(db: ValidatorDatabaseManager, validator: Optional[Any] = None) -> bool:
    """Pick one verify candidate and run verify using existing logic. Returns True if processed."""
    return await verify_step.run(db, validator=validator)


async def process_day1_one(db: ValidatorDatabaseManager, validator: Optional[Any] = None) -> bool:
    """Process one day1 scoring candidate end-to-end for a single miner."""
    return await day1_step.run(db, validator=validator)


async def process_era5_one(db: ValidatorDatabaseManager, validator: Optional[Any] = None) -> bool:
    """Process one ERA5 scoring candidate for a single miner and update stats incrementally."""
    return await era5_step.run(db, validator=validator)


async def process_one(db: ValidatorDatabaseManager, validator: Optional[Any] = None) -> bool:
    """Unified worker: claims the next available work, preferring generic queue, then per-miner fallback."""
    # First ensure step jobs are enqueued into the generic queue
    try:
        await db.enqueue_weather_step_jobs(limit=200)
    except Exception:
        pass
    # Prefer generic queue if available
    job = await db.claim_validator_job(worker_name="weather-w/1", job_type_prefix="weather.")
    if job:
        try:
            jtype = job.get("job_type")
            payload = job.get("payload") or {}
            if isinstance(payload, str):
                import json as _json

                try:
                    payload = _json.loads(payload)
                except Exception:
                    payload = {}
            if jtype == "weather.verify":
                # Use exact payload if provided
                if all(k in payload for k in ("run_id", "miner_uid", "response_id")):
                    # If miner_hotkey missing, fetch from DB
                    miner_hotkey = payload.get("miner_hotkey")
                    if not miner_hotkey:
                        row = await db.fetch_one(
                            "SELECT miner_hotkey FROM weather_miner_responses WHERE id = :rid",
                            {"rid": payload["response_id"]},
                        )
                        miner_hotkey = row and row.get("miner_hotkey")
                    ok = await verify_step.run_item(
                        db,
                        run_id=payload["run_id"],
                        miner_uid=payload["miner_uid"],
                        miner_hotkey=miner_hotkey or "",
                        response_id=payload["response_id"],
                        validator=validator,
                    )
                else:
                    ok = await process_verify_one(db, validator=validator)
            elif jtype == "weather.day1":
                if all(k in payload for k in ("run_id", "miner_uid", "response_id")):
                    miner_hotkey = payload.get("miner_hotkey")
                    if not miner_hotkey:
                        row = await db.fetch_one(
                            "SELECT miner_hotkey FROM weather_miner_responses WHERE id = :rid",
                            {"rid": payload["response_id"]},
                        )
                        miner_hotkey = row and row.get("miner_hotkey")
                    ok = await day1_step.run_item(
                        db,
                        run_id=payload["run_id"],
                        miner_uid=payload["miner_uid"],
                        miner_hotkey=miner_hotkey or "",
                        response_id=payload["response_id"],
                        validator=validator,
                    )
                else:
                    ok = await process_day1_one(db, validator=validator)
            elif jtype == "weather.era5":
                if all(k in payload for k in ("run_id", "miner_uid", "response_id")):
                    miner_hotkey = payload.get("miner_hotkey")
                    if not miner_hotkey:
                        row = await db.fetch_one(
                            "SELECT miner_hotkey FROM weather_miner_responses WHERE id = :rid",
                            {"rid": payload["response_id"]},
                        )
                        miner_hotkey = row and row.get("miner_hotkey")
                    ok = await era5_step.run_item(
                        db,
                        run_id=payload["run_id"],
                        miner_uid=payload["miner_uid"],
                        miner_hotkey=miner_hotkey or "",
                        response_id=payload["response_id"],
                        validator=validator,
                    )
                else:
                    ok = await process_era5_one(db, validator=validator)
            elif jtype == "weather.scoring.day1_qc":
                # Kick off per-miner day1 by ensuring jobs are enqueued
                try:
                    await db.execute(
                        "UPDATE weather_forecast_runs SET status = 'day1_scoring_started' WHERE id = :rid",
                        {"rid": payload.get("run_id")},
                    )
                except Exception:
                    pass
                try:
                    await db.enqueue_weather_step_jobs(limit=500)
                except Exception:
                    pass
                ok = True
            elif jtype == "weather.scoring.era5_final":
                # Signal final scoring attempted; per-miner ERA5 steps will run via scheduler
                try:
                    await db.execute(
                        "UPDATE weather_forecast_runs SET final_scoring_attempted_time = NOW() WHERE id = :rid",
                        {"rid": payload.get("run_id")},
                    )
                except Exception:
                    pass
                ok = True
            else:
                ok = False
            if ok:
                await db.complete_validator_job(job["id"], result={"ok": True})
            else:
                await db.fail_validator_job(job["id"], "step returned False", schedule_retry_in_seconds=900)
            return ok
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    # Try non-weather utility queues next: stats → metagraph → miners → era5 → ops
    job = await db.claim_validator_job(worker_name="weather-w/1", job_type_prefix="stats.")
    if job:
        try:
            j = job.get("job_type")
            if j == "stats.aggregate":
                from gaia.tasks.defined_tasks.weather.pipeline.steps.aggregate_step import run_miner_aggregation

                ok = await run_miner_aggregation(db, validator=validator)
            elif j == "stats.subnet_snapshot":
                from gaia.tasks.defined_tasks.weather.pipeline.steps.aggregate_step import compute_subnet_stats

                _ = await compute_subnet_stats(db)
                ok = True
            else:
                ok = False
            if ok:
                await db.complete_validator_job(job["id"], result={"ok": True})
            else:
                await db.fail_validator_job(job["id"], "unknown stats job")
            return ok
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    job = await db.claim_validator_job(worker_name="weather-w/1", job_type_prefix="metagraph.")
    if job:
        try:
            # Placeholder: actual metagraph sync handled elsewhere; mark completed
            await db.complete_validator_job(job["id"], result={"ok": True})
            return True
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    job = await db.claim_validator_job(worker_name="weather-w/1", job_type_prefix="miners.")
    if job:
        try:
            # Placeholder: implement specific miners ops later
            await db.complete_validator_job(job["id"], result={"ok": True})
            return True
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    job = await db.claim_validator_job(worker_name="weather-w/1", job_type_prefix="era5.")
    if job:
        try:
            # Placeholder: era5 token refresh, etc.
            await db.complete_validator_job(job["id"], result={"ok": True})
            return True
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    job = await db.claim_validator_job(worker_name="weather-w/1", job_type_prefix="ops.")
    if job:
        try:
            # Placeholder ops (status snapshot, db monitor, plots)
            await db.complete_validator_job(job["id"], result={"ok": True})
            return True
        except Exception as e:
            await db.fail_validator_job(job["id"], f"exception: {e}")
            return False

    # Fallback to per-miner scheduler selection
    sched = MinerWorkScheduler(db)
    item = await sched.claim_next()
    if not item:
        return False
    if item.step == "verify":
        return await process_verify_one(db, validator=validator)
    if item.step == "day1":
        return await process_day1_one(db, validator=validator)
    if item.step == "era5":
        return await process_era5_one(db, validator=validator)
    return False


