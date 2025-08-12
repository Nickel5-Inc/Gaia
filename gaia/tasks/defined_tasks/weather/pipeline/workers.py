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
    """Unified worker: claims the next available item and dispatches to the correct step."""
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


