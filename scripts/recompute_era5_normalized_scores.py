#!/usr/bin/env python3

import asyncio
import os
from typing import Dict, List

import sqlalchemy as sa

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.processing.weather_logic import WeatherTask  # type: ignore


async def recompute_for_run(db: ValidatorDatabaseManager, run_id: int) -> int:
    # Find miners for run
    miners = await db.fetch_all(
        "SELECT DISTINCT miner_uid, miner_hotkey FROM weather_miner_scores WHERE run_id = :rid",
        {"rid": run_id},
    )
    updated = 0
    # Use processing logic function to rewrite normalized scores and overall
    from gaia.tasks.defined_tasks.weather.processing.weather_logic import _calculate_and_store_aggregated_era5_score
    task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)

    # Determine lead hours expected
    expected_leads = task.config.get("final_scoring_lead_hours", [24,48,72,96,120,144,168,192,216,240])

    for m in miners:
        uid = int(m["miner_uid"])
        # Fetch response_id
        resp = await db.fetch_one(
            "SELECT id FROM weather_miner_responses WHERE run_id = :rid AND miner_uid = :uid ORDER BY id DESC LIMIT 1",
            {"rid": run_id, "uid": uid},
        )
        if not resp:
            continue
        response_id = int(resp["id"])
        # Vars config
        vars_levels = task.config.get(
            "final_scoring_variables_levels",
            task.config.get("day1_variables_levels_to_score", []),
        )
        await _calculate_and_store_aggregated_era5_score(
            task_instance=task,
            run_id=run_id,
            miner_uid=uid,
            miner_hotkey=m.get("miner_hotkey") or "unknown",
            response_id=response_id,
            lead_hours_scored=expected_leads,
            vars_levels_scored=vars_levels,
        )
        updated += 1
    return updated


async def main():
    db = ValidatorDatabaseManager(node_type="validator")
    await db.initialize_database()
    # Scope: last N days runs or all unfinished
    runs = await db.fetch_all(
        sa.text(
            """
            SELECT id
            FROM weather_forecast_runs
            WHERE created_at >= NOW() - INTERVAL '30 days'
            ORDER BY id
            """
        )
    )
    total = 0
    for r in runs:
        rid = int(r["id"])
        count = await recompute_for_run(db, rid)
        total += count
    print(f"Recomputed normalized ERA5 scores for {total} miner-run records across {len(runs)} runs")


if __name__ == "__main__":
    asyncio.run(main())
