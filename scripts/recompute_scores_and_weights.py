#!/usr/bin/env python3
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask


async def list_runs(db: ValidatorDatabaseManager) -> List[int]:
    rows = await db.fetch_all("SELECT id FROM weather_forecast_runs ORDER BY id ASC")
    return [int(r["id"]) for r in rows]


async def recompute_overall_for_run(db: ValidatorDatabaseManager, run_id: int) -> int:
    # Recompute overall_forecast_score as 0.95 * ERA5_norm_avg + 0.05 * day1_pass with completeness penalty
    cfg_task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
    qc_threshold = float(cfg_task.config.get("day1_binary_threshold", 0.1))
    Wd, We = 0.05, 0.95

    rows = await db.fetch_all(
        """
        SELECT miner_uid,
               forecast_score_initial AS day1,
               era5_score_24h, era5_score_48h, era5_score_72h, era5_score_96h,
               era5_score_120h, era5_score_144h, era5_score_168h, era5_score_192h,
               era5_score_216h, era5_score_240h
        FROM weather_forecast_stats
        WHERE run_id = :rid
        """,
        {"rid": run_id},
    )

    updates = 0
    for r in rows:
        uid = int(r["miner_uid"]) if r.get("miner_uid") is not None else None
        if uid is None:
            continue
        day1 = r.get("day1")
        leads = [
            r.get("era5_score_24h"), r.get("era5_score_48h"), r.get("era5_score_72h"), r.get("era5_score_96h"),
            r.get("era5_score_120h"), r.get("era5_score_144h"), r.get("era5_score_168h"), r.get("era5_score_192h"),
            r.get("era5_score_216h"), r.get("era5_score_240h"),
        ]
        vals = [float(x) for x in leads if x is not None]
        era5_norm_avg = (sum(vals) / 10.0) if vals else 0.0
        day1_pass = 1.0 if (day1 is not None and float(day1) >= qc_threshold) else 0.0
        overall = We * era5_norm_avg + Wd * day1_pass
        await db.execute(
            """
            UPDATE weather_forecast_stats
            SET overall_forecast_score = :overall,
                era5_combined_score = :era5_combined,
                era5_completeness = :era5_completeness,
                updated_at = NOW()
            WHERE run_id = :rid AND miner_uid = :uid
            """,
            {
                "overall": overall,
                "era5_combined": era5_norm_avg,
                "era5_completeness": (len(vals) / 10.0),
                "rid": run_id,
                "uid": uid,
            },
        )
        updates += 1
    return updates


async def rebuild_score_table_from_stats(db: ValidatorDatabaseManager, run_id: int) -> None:
    # Use WeatherTask.update_combined_weather_scores with run trigger; it now reads stats.overall_forecast_score
    task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
    await task.update_combined_weather_scores(run_id_trigger=run_id, force_phase="final")


async def main():
    db = ValidatorDatabaseManager()
    await db.initialize_database()

    runs = await list_runs(db)
    total_updates = 0
    for rid in runs:
        u = await recompute_overall_for_run(db, rid)
        await rebuild_score_table_from_stats(db, rid)
        print({"run": rid, "miners_updated": u})
        total_updates += u
    print({"total_miners_updated": total_updates, "completed_at": datetime.now(timezone.utc).isoformat()})


if __name__ == "__main__":
    asyncio.run(main())
