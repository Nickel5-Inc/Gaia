#!/usr/bin/env python3
import asyncio
import os
import sys
from typing import List

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager


def parse_run_ids(argv: List[str]) -> List[int]:
    if len(argv) > 1:
        return [int(a) for a in argv[1:]]
    env_ids = os.getenv("RUN_IDS")
    if env_ids:
        return [int(x.strip()) for x in env_ids.split(",") if x.strip()]
    return [1, 2, 3]


async def reset_runs(run_ids: List[int]):
    db = ValidatorDatabaseManager()
    await db.initialize_database()

    in_clause = ",".join(str(r) for r in run_ids)

    # 1) Remove prior ERA5 metrics
    await db.execute(
        f"""
        DELETE FROM weather_miner_scores
        WHERE run_id IN ({in_clause}) AND score_type LIKE 'era5_%'
        """
    )
    await db.execute(
        f"""
        DELETE FROM weather_forecast_component_scores
        WHERE run_id IN ({in_clause}) AND score_type = 'era5'
        """
    )

    # 2) Clear normalized per-lead and overall in stats
    await db.execute(
        f"""
        UPDATE weather_forecast_stats
        SET 
          era5_score_24h = NULL,
          era5_score_48h = NULL,
          era5_score_72h = NULL,
          era5_score_96h = NULL,
          era5_score_120h = NULL,
          era5_score_144h = NULL,
          era5_score_168h = NULL,
          era5_score_192h = NULL,
          era5_score_216h = NULL,
          era5_score_240h = NULL,
          avg_rmse = NULL,
          avg_acc = NULL,
          avg_skill_score = NULL,
          overall_forecast_score = NULL,
          era5_combined_score = NULL,
          era5_completeness = NULL
        WHERE run_id IN ({in_clause})
        """
    )

    # 3) Remove per-lead step rows (guard will rebuild)
    await db.execute(
        f"""
        DELETE FROM weather_forecast_steps
        WHERE run_id IN ({in_clause}) AND step_name = 'era5' AND substep = 'score'
        """
    )

    # 4) Cancel any outstanding weather.era5 jobs for these runs
    await db.execute(
        f"""
        UPDATE validator_jobs
        SET status = 'completed', completed_at = NOW(), result = '{{"cancelled":"reset_runs"}}'
        WHERE job_type = 'weather.era5'
          AND run_id IN ({in_clause})
          AND status IN ('pending','in_progress','retry_scheduled')
        """
    )

    # 5) Reset ERA5 timing fields on miner responses (preserve day1 fields)
    await db.execute(
        f"""
        UPDATE weather_miner_responses
        SET era5_scoring_completed_at = NULL,
            total_pipeline_duration_seconds = NULL
        WHERE run_id IN ({in_clause})
        """
    )

    # 6) Remove combined score_table rows for these runs (task_id format: weather_scores_{run}_{GFSZ})
    for rid in run_ids:
        await db.execute(
            f"""
            DELETE FROM score_table
            WHERE task_name = 'weather'
              AND task_id LIKE 'weather_scores_{rid}_%'
            """
        )

    # 7) Enqueue global guard to repopulate steps
    await db.enqueue_singleton_job(
        singleton_key="era5_guard_all",
        job_type="era5.era5_truth_guard_all",
        payload={},
        priority=30,
    )
    print(f"Reset ERA5 scoring state (stats, component scores, miner timings, steps, jobs, score_table) for runs {run_ids} and enqueued guard")


if __name__ == "__main__":
    runs = parse_run_ids(sys.argv)
    asyncio.run(reset_runs(runs))
 