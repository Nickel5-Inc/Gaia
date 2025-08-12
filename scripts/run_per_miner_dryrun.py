#!/usr/bin/env python3
"""
Dry-run executor to validate per-miner scheduler and verification step end-to-end.
Does not alter batch flow. Intended for manual runs.
"""
import asyncio
import os
from datetime import timezone

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.pipeline import MinerWorkScheduler
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
from gaia.tasks.defined_tasks.weather.processing.weather_logic import (
    verify_miner_response,
)


async def run_verify_one(db: ValidatorDatabaseManager) -> None:
    sched = MinerWorkScheduler(db)
    candidates = await sched.next_to_verify(limit=1)
    if not candidates:
        print("[dryrun] No candidates to verify")
        return

    item = candidates[0]
    print(f"[dryrun] verify candidate: run_id={item.run_id} uid={item.miner_uid} resp_id={item.response_id}")

    run_details = await db.fetch_one(
        "SELECT id, gfs_init_time_utc, status FROM weather_forecast_runs WHERE id = :rid",
        {"rid": item.run_id},
    )
    if not run_details:
        print("[dryrun] run_details not found; skipping")
        return

    response_details = await db.fetch_one(
        "SELECT id, miner_hotkey, job_id FROM weather_miner_responses WHERE id = :rid",
        {"rid": item.response_id},
    )
    if not response_details:
        print("[dryrun] response_details not found; skipping")
        return

    # Instantiate a minimal WeatherTask for using existing functions
    task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)

    try:
        await verify_miner_response(task, run_details, response_details)
        print("[dryrun] verify_miner_response completed")
    except Exception as e:
        print(f"[dryrun] verify_miner_response error: {e}")


async def main():
    db = ValidatorDatabaseManager()
    await db.initialize_database()
    try:
        await run_verify_one(db)
    finally:
        await db.close_all_connections()


if __name__ == "__main__":
    # Allow PYTHONPATH=. python3 scripts/run_per_miner_dryrun.py
    asyncio.run(main())


