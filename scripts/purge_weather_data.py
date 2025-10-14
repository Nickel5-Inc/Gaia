#!/usr/bin/env python3
from __future__ import annotations

import asyncio
from typing import List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def purge_all_weather_data() -> None:
    # Mirror DB URL used by manually_reseed_run.py
    db_url = "postgresql+asyncpg://postgres:postgres@localhost/validator_db"
    engine = create_async_engine(db_url, future=True)

    async with engine.begin() as conn:
        # Disable triggers for faster deletes if needed (optional)
        # await conn.execute(text("SET session_replication_role = 'replica'"))

        # 1) Remove queued/in-progress/completed jobs related to weather/miners/era5 to avoid re-enqueue collisions
        await conn.execute(
            text(
                """
                DELETE FROM validator_jobs
                WHERE job_type LIKE 'weather.%'
                   OR job_type LIKE 'era5.%'
                   OR job_type LIKE 'miners.%'
                """
            )
        )

        # 2) Delete weather step/event logs and scoring job trackers
        await conn.execute(text("DELETE FROM weather_forecast_steps"))
        await conn.execute(text("DELETE FROM weather_scoring_jobs"))

        # 3) Delete stats and historical weights
        await conn.execute(text("DELETE FROM weather_forecast_stats"))
        await conn.execute(text("DELETE FROM weather_historical_weights"))

        # 4) Delete ensembles (components then forecasts)
        await conn.execute(
            text(
                """
                DELETE FROM weather_ensemble_components USING weather_ensemble_forecasts wef
                WHERE weather_ensemble_components.ensemble_id = wef.id
                """
            )
        )
        await conn.execute(text("DELETE FROM weather_ensemble_forecasts"))

        # 5) Delete component scores and miner scores (explicit, though responses CASCADE also clears these)
        await conn.execute(text("DELETE FROM weather_forecast_component_scores"))
        await conn.execute(text("DELETE FROM weather_miner_scores"))

        # 6) Delete miner responses (CASCADE clears dependent rows)
        await conn.execute(text("DELETE FROM weather_miner_responses"))

    async with engine.begin() as conn:
        # 7) Remove combined aggregated score_table entries for weather tasks
        await conn.execute(
            text("DELETE FROM score_table WHERE task_name = 'weather'")
        )

    await engine.dispose()
    print("Purged all weather-related data (kept weather_forecast_runs).")


def main() -> None:
    asyncio.run(purge_all_weather_data())


if __name__ == "__main__":
    main()


