from __future__ import annotations

from typing import Optional, Any, Dict

import sqlalchemy as sa

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from gaia.database.validator_schema import (
    weather_forecast_stats_table,
    miner_stats_table,
)


async def run_miner_aggregation(db: ValidatorDatabaseManager, validator: Optional[Any] = None, miner_uid: Optional[int] = None) -> bool:
    """Aggregate per-miner stats into miner_stats. Optionally a single miner.

    Returns True on success.
    """
    manager = WeatherStatsManager(
        db,
        validator_hotkey=(
            getattr(getattr(getattr(validator, "validator_wallet", None), "hotkey", None), "ss58_address", None)
            if validator is not None
            else "unknown_validator"
        ),
    )
    ok = await manager.aggregate_miner_stats(miner_uid=miner_uid)
    if ok:
        # Update overall ranks after aggregation
        try:
            await manager.get_overall_miner_ranks()
        except Exception:
            pass
    return ok


async def compute_subnet_stats(db: ValidatorDatabaseManager) -> Dict[str, Any]:
    """Compute subnet-wide stats on the fly without persisting (no schema changes)."""
    from gaia.utils.custom_logger import get_logger
    logger = get_logger(__name__)
    
    stats: Dict[str, Any] = {}

    try:
        # Active miners recently seen in forecasts
        q_active = sa.select(sa.func.count(sa.func.distinct(weather_forecast_stats_table.c.miner_uid)))
        result = await db.fetch_one(q_active)
        stats["active_miners"] = result[0] if result and isinstance(result, (tuple, list)) else (result if isinstance(result, int) else 0)
        logger.debug(f"Active miners count: {stats['active_miners']}")
    except Exception as e:
        logger.error(f"Failed to query active miners: {e}")
        stats["active_miners"] = 0

    try:
        # Averages from miner_stats
        q_avgs = sa.select(
            sa.func.avg(miner_stats_table.c.avg_forecast_score).label("avg_forecast_score"),
            sa.func.avg(miner_stats_table.c.avg_day1_score).label("avg_day1_score"),
            sa.func.avg(miner_stats_table.c.avg_era5_completeness).label("avg_era5_completeness"),
            sa.func.avg(miner_stats_table.c.avg_hosting_latency_ms).label("avg_hosting_latency_ms"),
            sa.func.avg(miner_stats_table.c.host_reliability_ratio).label("avg_host_reliability_ratio"),
        )
        row = await db.fetch_one(q_avgs)
        stats.update({k: row[k] if row and row[k] is not None else None for k in [
            "avg_forecast_score",
            "avg_day1_score",
            "avg_era5_completeness",
            "avg_hosting_latency_ms",
            "avg_host_reliability_ratio",
        ]})
        logger.debug(f"Average stats computed: forecast_score={stats.get('avg_forecast_score')}")
    except Exception as e:
        logger.error(f"Failed to query average stats: {e}")
        stats.update({
            "avg_forecast_score": None,
            "avg_day1_score": None,
            "avg_era5_completeness": None,
            "avg_hosting_latency_ms": None,
            "avg_host_reliability_ratio": None,
        })

    try:
        # Distribution sample: top/bottom scores
        q_top = sa.text("""
            SELECT miner_uid, miner_hotkey, avg_forecast_score
            FROM miner_stats
            WHERE avg_forecast_score IS NOT NULL
            ORDER BY avg_forecast_score DESC NULLS LAST
            LIMIT 10
        """)
        stats["top_miners"] = await db.fetch_all(q_top)
        logger.debug(f"Top miners count: {len(stats.get('top_miners', []))}")
    except Exception as e:
        logger.error(f"Failed to query top miners: {e}")
        stats["top_miners"] = []

    try:
        q_bottom = sa.text("""
            SELECT miner_uid, miner_hotkey, avg_forecast_score
            FROM miner_stats
            WHERE avg_forecast_score IS NOT NULL
            ORDER BY avg_forecast_score ASC NULLS LAST
            LIMIT 10
        """)
        stats["bottom_miners"] = await db.fetch_all(q_bottom)
        logger.debug(f"Bottom miners count: {len(stats.get('bottom_miners', []))}")
    except Exception as e:
        logger.error(f"Failed to query bottom miners: {e}")
        stats["bottom_miners"] = []

    logger.info(f"Subnet stats computation completed: {len(stats)} metrics collected")
    return stats


