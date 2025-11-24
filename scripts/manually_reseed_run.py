#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def reseed_run(*, run_id: int) -> None:
    # Hardcoded defaults
    db_url = "postgresql+asyncpg://postgres:postgres@localhost/validator_db"
    validator_hotkey = "unknown_validator"
    engine = create_async_engine(db_url, future=True)
    async with engine.begin() as conn:
        # Validate run exists
        res = await conn.execute(text("SELECT id, status FROM weather_forecast_runs WHERE id = :rid"), {"rid": run_id})
        row = res.mappings().first()
        if not row:
            raise SystemExit(f"Run {run_id} not found in weather_forecast_runs")

        # --- Soft reset (minimal) ---
        # 1) Cancel active/scheduled jobs that could block singleton re-enqueue
        await conn.execute(
            text(
                """
                UPDATE validator_jobs
                SET status = 'failed', completed_at = NOW(), lease_expires_at = NULL
                WHERE run_id = :rid
                  AND job_type LIKE 'weather.%'
                  AND status IN ('pending','in_progress','retry_scheduled')
                """
            ),
            {"rid": run_id},
        )
        # 1a) Allow re-enqueue of seed by neutralizing prior completed seed jobs (singleton gate includes 'completed')
        await conn.execute(
            text(
                """
                UPDATE validator_jobs
                SET status = 'failed', completed_at = NOW(), lease_expires_at = NULL
                WHERE run_id = :rid
                  AND job_type = 'weather.seed'
                  AND status = 'completed'
                """
            ),
            {"rid": run_id},
        )
        # 1b) Reset seed step row to pending so step enqueuer can recreate the seed job
        await conn.execute(
            text(
                """
                UPDATE weather_forecast_steps
                SET status = 'pending', job_id = NULL, next_retry_time = NULL
                WHERE run_id = :rid
                  AND step_name = 'seed'
                  AND substep = 'download_gfs'
                """
            ),
            {"rid": run_id},
        )
        # 2) Clear per-miner responses (prevents skip logic and lets poll/enqueue restart)
        await conn.execute(text("DELETE FROM weather_miner_responses WHERE run_id = :rid"), {"rid": run_id})
        # 3) Reset run state and move freeze window anchor to now
        await conn.execute(
            text(
                """
                UPDATE weather_forecast_runs
                SET status = 'created',
                    run_initiation_time = NOW(),
                    completion_time = NULL,
                    final_scoring_attempted_time = NULL,
                    error_message = NULL
                WHERE id = :rid
                """
            ),
            {"rid": run_id},
        )

    # Enqueue orchestration job after cleanup commit (always enqueue)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO validator_jobs (job_type, priority, payload, run_id)
                VALUES (
                  'weather.run.orchestrate',
                  :pri,
                  jsonb_build_object('run_id', (:rid)::int, 'validator_hotkey', (:vhk)::text),
                  :rid
                )
                """
            ),
            {"pri": 50, "rid": run_id, "vhk": validator_hotkey},
        )

    await engine.dispose()
    print(f"Re-seeded run {run_id}. Enqueued orchestrate: True")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Manually re-seed a weather forecast run (irreversible).")
    p.add_argument("run_id", type=int, help="Run ID to reset and re-seed")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(
        reseed_run(run_id=args.run_id)
    )


if __name__ == "__main__":
    main()


