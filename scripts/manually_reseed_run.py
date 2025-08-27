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

        # Order matters due to FKs/cascades
        await conn.execute(text("DELETE FROM validator_jobs WHERE run_id = :rid"), {"rid": run_id})
        await conn.execute(text("DELETE FROM weather_forecast_steps WHERE run_id = :rid"), {"rid": run_id})
        await conn.execute(text("DELETE FROM weather_miner_responses WHERE run_id = :rid"), {"rid": run_id})
        await conn.execute(text("DELETE FROM weather_scoring_jobs WHERE run_id = :rid"), {"rid": run_id})
        await conn.execute(text("DELETE FROM weather_forecast_stats WHERE run_id = :rid"), {"rid": run_id})
        # Ensembles
        await conn.execute(
            text(
                """
                DELETE FROM weather_ensemble_components USING weather_ensemble_forecasts wef
                WHERE weather_ensemble_components.ensemble_id = wef.id AND wef.forecast_run_id = :rid
                """
            ),
            {"rid": run_id},
        )
        await conn.execute(text("DELETE FROM weather_ensemble_forecasts WHERE forecast_run_id = :rid"), {"rid": run_id})
        # Historical weights
        await conn.execute(text("DELETE FROM weather_historical_weights WHERE run_id = :rid"), {"rid": run_id})
        # Combined score rows for this run (task_id format: weather_scores_{run_id}_{GFSZ})
        await conn.execute(
            text("DELETE FROM score_table WHERE task_name = 'weather' AND task_id LIKE ('weather_scores_'||:rid_text||'_%')"),
            {"rid_text": str(run_id)},
        )
        # Reset run state
        await conn.execute(
            text(
                """
                UPDATE weather_forecast_runs
                SET status = 'created', completion_time = NULL, final_scoring_attempted_time = NULL, error_message = NULL
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


