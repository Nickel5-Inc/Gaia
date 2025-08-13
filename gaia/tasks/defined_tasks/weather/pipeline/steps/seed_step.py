from __future__ import annotations

from typing import Optional, Any, List, Dict

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.database.validator_schema import (
    node_table,
    weather_forecast_runs_table,
    weather_forecast_stats_table,
    weather_forecast_steps_table,
)
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask  # type: ignore
from .substep import substep
from .util_time import get_effective_gfs_init


async def seed_forecast_run(
    db: ValidatorDatabaseManager,
    run_id: int,
    validator_hotkey: str = "unknown_validator",
) -> int:
    """Create per-miner seed rows for a forecast run.

    Returns number of miners seeded (inserted or already present).
    Idempotent via (miner_uid, forecast_run_id) uniqueness.
    """
    # Fetch run info for deterministic forecast_run_id and init time
    run = await db.fetch_one(
        sa.select(
            weather_forecast_runs_table.c.gfs_init_time_utc,
            weather_forecast_runs_table.c.target_forecast_time_utc,
        ).where(weather_forecast_runs_table.c.id == run_id)
    )
    if not run:
        return 0

    gfs_init = run["gfs_init_time_utc"]
    target_time = run["target_forecast_time_utc"]

    # Compute forecast_run_id once, using effective gfs_init for test mode
    # (Target time remains for run identity; gfs_init shifting is only for data alignment)
    task_like = type("_T", (), {"test_mode": False, "config": {}})()
    eff_gfs_init = get_effective_gfs_init(task_like, gfs_init)
    forecast_run_id = WeatherStatsManager.generate_forecast_run_id(eff_gfs_init, target_time)

    # Pull miners (UID/hotkey). Keep within 0..255 and require non-null hotkey
    miners: List[Dict[str, Any]] = await db.fetch_all(
        sa.text(
            """
            SELECT uid as miner_uid, hotkey as miner_hotkey
            FROM node_table
            WHERE uid BETWEEN 0 AND 255 AND hotkey IS NOT NULL
            ORDER BY uid ASC
            """
        )
    )
    if not miners:
        return 0

    # Prepare bulk upsert into weather_forecast_stats
    stats_rows = [
        {
            "miner_uid": m["miner_uid"],
            "miner_hotkey": m["miner_hotkey"],
            "forecast_run_id": forecast_run_id,
            "run_id": run_id,
            "forecast_init_time": gfs_init,
            "forecast_status": "seeded",
            "validator_hotkey": validator_hotkey,
        }
        for m in miners
    ]
    if stats_rows:
        stmt = insert(weather_forecast_stats_table).values(stats_rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                weather_forecast_stats_table.c.miner_uid,
                weather_forecast_stats_table.c.forecast_run_id,
            ],
            set_={
                "run_id": stmt.excluded.run_id,
                "forecast_init_time": stmt.excluded.forecast_init_time,
                "forecast_status": stmt.excluded.forecast_status,
                "validator_hotkey": stmt.excluded.validator_hotkey,
            },
        )
        await db.execute(stmt)

    # Seed steps: set day1 step to pending for each miner (no separate verification phase)
    step_rows = [
        {
            "run_id": run_id,
            "miner_uid": m["miner_uid"],
            "miner_hotkey": m["miner_hotkey"],
            "step_name": "day1",
            "substep": None,
            "lead_hours": None,
            "status": "pending",
        }
        for m in miners
    ]
    if step_rows:
        st = insert(weather_forecast_steps_table).values(step_rows)
        st = st.on_conflict_do_nothing(
            index_elements=[
                weather_forecast_steps_table.c.run_id,
                weather_forecast_steps_table.c.miner_uid,
                weather_forecast_steps_table.c.step_name,
                weather_forecast_steps_table.c.substep,
                weather_forecast_steps_table.c.lead_hours,
            ]
        )
        await db.execute(st)

    # Enqueue generic jobs for these steps (including the run-level seed substep for workers)
    try:
        # Ensure a run-level seed step exists so workers can claim GFS warmup
        # Use an existing miner_uid/hotkey to satisfy FK; this is only a coordinator tag
        first_node = await db.fetch_one(
            "SELECT uid, hotkey FROM node_table ORDER BY uid ASC LIMIT 1"
        )
        if first_node and first_node.get("uid") is not None:
            await db.execute(
                """
                INSERT INTO weather_forecast_steps (run_id, miner_uid, miner_hotkey, step_name, substep, lead_hours, status)
                VALUES (:rid, :uid, :hk, 'seed', 'download_gfs', NULL, 'pending')
                ON CONFLICT (run_id, miner_uid, step_name, substep, lead_hours) DO NOTHING
                """,
                {"rid": run_id, "uid": int(first_node["uid"]), "hk": first_node.get("hotkey") or "coordinator"},
            )
        await db.enqueue_weather_step_jobs(limit=1000)
        # Also ensure polling jobs are present for responses that are starting
        await db.enqueue_miner_poll_jobs(limit=1000)
    except Exception:
        pass

    return len(miners)


@substep("seed", "download_gfs", should_retry=True, retry_delay_seconds=300, max_retries=6, retry_backoff="exponential")
async def ensure_gfs_reference_available(db: ValidatorDatabaseManager, task, *, run_id: int, miner_uid: int = -1, miner_hotkey: str = "coordinator"):
    """Ensure GFS reference for this run is fetched or cached.

    Uses the same logic as day1 load_inputs but at run level, and persists locally.
    """
    # Acquire a cluster-wide advisory lock so only one worker performs the heavy fetch per run
    lock_key = 0x47505310 ^ int(run_id)  # deterministic small int key per run (prefix 'GPS\x10')
    try:
        row = await db.fetch_one("SELECT pg_try_advisory_lock(:key) AS ok", {"key": lock_key})
        if not row or not row.get("ok"):
            # Another worker is already doing this; skip gracefully
            return True
    except Exception:
        # If lock acquisition fails, skip to avoid duplicate heavy work
        return True
    run = await db.fetch_one(
        sa.select(weather_forecast_runs_table.c.gfs_init_time_utc).where(weather_forecast_runs_table.c.id == run_id)
    )
    if not run:
        raise RuntimeError("run not found")
    # Use the run's stored GFS init time directly. It is already shifted in test mode at run creation.
    gfs_init = run["gfs_init_time_utc"]
    from gaia.tasks.defined_tasks.weather.utils.gfs_api import (
        fetch_gfs_analysis_data,
        fetch_gfs_data,
    )
    # Day1 leads minimal to warm cache
    leads = task.config.get("initial_scoring_lead_hours", [6, 12]) if hasattr(task, "config") else [6, 12]
    from pathlib import Path
    cache_dir = Path(
        task.config.get("gfs_analysis_cache_dir", "./gfs_analysis_cache")
        if hasattr(task, "config")
        else "./gfs_analysis_cache"
    )
    gfs_analysis_ds = await fetch_gfs_analysis_data([gfs_init], cache_dir=cache_dir)
    if not gfs_analysis_ds:
        raise RuntimeError("fetch_gfs_analysis_data returned None")
    _ = await fetch_gfs_data(
        run_time=gfs_init,
        lead_hours=leads,
        output_dir=str(cache_dir),
    )
    # Ensure lock releases even on early returns
    try:
        await db.execute("SELECT pg_advisory_unlock(:key)", {"key": lock_key})
    except Exception:
        pass
    return True


async def run_item(
    db: ValidatorDatabaseManager,
    *,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    validator: Optional[Any] = None,
) -> bool:
    """Process the run-level seed step (download_gfs) via generic job dispatch."""
    task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
    if validator is not None:
        setattr(task, "validator", validator)
    try:
        await ensure_gfs_reference_available(
            db,
            task,
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
        )
        return True
    except Exception:
        return False

