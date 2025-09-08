from __future__ import annotations

from typing import Any, Dict, Optional

import sqlalchemy as sa

from gaia.database.validator_schema import (miner_stats_table,
                                            weather_forecast_stats_table)
from gaia.validator.database.validator_database_manager import \
    ValidatorDatabaseManager
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager


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
    stats: Dict[str, Any] = {}

    # Active miners recently seen in forecasts
    q_active = sa.select(sa.func.count(sa.func.distinct(weather_forecast_stats_table.c.miner_uid)))
    stats["active_miners"] = (await db.fetch_one(q_active)) or 0

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

    # Distribution sample: top/bottom scores
    q_top = sa.text("""
        SELECT miner_uid, miner_hotkey, avg_forecast_score
        FROM miner_stats
        WHERE avg_forecast_score IS NOT NULL
        ORDER BY avg_forecast_score DESC NULLS LAST
        LIMIT 10
    """)
    q_bottom = sa.text("""
        SELECT miner_uid, miner_hotkey, avg_forecast_score
        FROM miner_stats
        WHERE avg_forecast_score IS NOT NULL
        ORDER BY avg_forecast_score ASC NULLS LAST
        LIMIT 10
    """)
    stats["top_miners"] = await db.fetch_all(q_top)
    stats["bottom_miners"] = await db.fetch_all(q_bottom)

    return stats


