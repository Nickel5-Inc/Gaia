#!/usr/bin/env python3
import asyncio
from typing import Dict, Any, List

import sqlalchemy as sa

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask


async def list_runs(db: ValidatorDatabaseManager) -> List[int]:
    rows = await db.fetch_all("SELECT DISTINCT run_id FROM weather_forecast_stats ORDER BY run_id")
    return [int(r["run_id"]) for r in rows]


async def backfill_overall_for_run(
    db: ValidatorDatabaseManager, task: WeatherTask, run_id: int
) -> int:
    cfg = task.config
    qc_threshold = float(cfg.get("day1_binary_threshold", 0.1))
    Wd = float(cfg.get("weather_score_day1_weight", 0.05))
    We = float(cfg.get("weather_score_era5_weight", 0.95))
    s = Wd + We
    if s > 0:
        Wd /= s
        We /= s

    # Read needed fields for this run
    rows = await db.fetch_all(
        sa.text(
            """
            SELECT miner_uid,
                   forecast_score_initial AS day1,
                   era5_score_24h, era5_score_48h, era5_score_72h, era5_score_96h,
                   era5_score_120h, era5_score_144h, era5_score_168h, era5_score_192h,
                   era5_score_216h, era5_score_240h
            FROM weather_forecast_stats
            WHERE run_id = :rid
            """
        ),
        {"rid": run_id},
    )

    updates = 0
    for r in rows:
        uid = int(r["miner_uid"])
        day1 = r.get("day1")
        # Compute ERA5_norm_avg with completeness penalty across the full 10 expected leads
        leads = [
            r.get("era5_score_24h"),
            r.get("era5_score_48h"),
            r.get("era5_score_72h"),
            r.get("era5_score_96h"),
            r.get("era5_score_120h"),
            r.get("era5_score_144h"),
            r.get("era5_score_168h"),
            r.get("era5_score_192h"),
            r.get("era5_score_216h"),
            r.get("era5_score_240h"),
        ]
        vals = [float(x) for x in leads if x is not None]
        # Divide by full horizon (10) so missing leads count as zeros
        era5_norm_avg = (sum(vals) / 10.0) if vals else 0.0

        day1_pass = 1.0 if (day1 is not None and float(day1) >= qc_threshold) else 0.0
        overall = We * era5_norm_avg + Wd * day1_pass

        await db.execute(
            sa.text(
                """
                UPDATE weather_forecast_stats
                SET overall_forecast_score = :overall,
                    era5_combined_score = :era5_combined,
                    era5_completeness = :era5_completeness
                WHERE run_id = :rid AND miner_uid = :uid
                """
            ),
            {
                "overall": overall,
                "era5_combined": era5_norm_avg,
                "era5_completeness": (len(vals) / 10.0),
                "rid": run_id,
                "uid": uid,
            },
        )
        updates += 1

    # Recompute combined rollups and ranks
    mgr = WeatherStatsManager(db, validator_hotkey="backfill")
    await mgr.recompute_era5_rollups_for_run(run_id)
    await mgr.update_miner_ranks_for_run(run_id)
    # Refresh combined score row in score_table for this run
    try:
        await task.update_combined_weather_scores(run_id_trigger=run_id, force_phase="final")
    except Exception:
        pass
    return updates


async def main():
    db = ValidatorDatabaseManager()
    task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
    runs = await list_runs(db)
    total = 0
    for rid in runs:
        n = await backfill_overall_for_run(db, task, rid)
        print({"run": rid, "updated_miners": n})
        total += n
    print({"total_updates": total})


if __name__ == "__main__":
    asyncio.run(main())


