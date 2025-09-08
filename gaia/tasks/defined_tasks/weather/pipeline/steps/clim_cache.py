from __future__ import annotations

import os
import pickle
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import sqlalchemy as sa

from gaia.tasks.defined_tasks.weather.weather_scoring_mechanism import \
    precompute_climatology_cache
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
from gaia.validator.database.validator_database_manager import \
    ValidatorDatabaseManager

from .step_logger import log_failure, log_start, log_success


def _cache_paths(task: WeatherTask, run_id: int) -> Dict[str, Path]:
    base = Path(task.config.get("clim_cache_dir", "./clim_cache"))
    base.mkdir(parents=True, exist_ok=True)
    run_dir = base / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    data_path = run_dir / "clim_cache.pkl"
    lock_path = run_dir / ".lock"
    return {"dir": run_dir, "data": data_path, "lock": lock_path}


async def ensure_clim_cache(
    db: ValidatorDatabaseManager,
    task: WeatherTask,
    *,
    run_id: int,
    gfs_analysis_sample_ds,
    gfs_init,
    day1_cfg: Dict[str, Any],
    era5_climatology_ds,
) -> Optional[Dict[str, Any]]:
    paths = _cache_paths(task, run_id)
    data_path = paths["data"]
    lock_path = paths["lock"]

    if data_path.exists():
        try:
            with open(data_path, "rb") as f:
                return pickle.load(f)
        except Exception:
            # Corrupt cache; proceed to recompute
            pass

    # Attempt to acquire lock (atomic create)
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        lock_acquired = True
    except FileExistsError:
        lock_acquired = False

    if lock_acquired:
        await log_start(
            db,
            run_id=run_id,
            miner_uid=0,  # coordinator tag; informational only
            miner_hotkey="coordinator",
            step_name="day1",
            substep="clim_cache",
        )
        try:
            # Times to evaluate based on config
            leads = day1_cfg.get(
                "lead_times_hours", task.config.get("initial_scoring_lead_hours", [6, 12])
            )
            times_to_evaluate = [gfs_init + timedelta(hours=h) for h in leads]

            # Pick a sample grid from analysis dataset
            sample_var = None
            for var_name in ["2t", "msl", "z", "t"]:
                if var_name in gfs_analysis_sample_ds.data_vars:
                    sample_var = gfs_analysis_sample_ds[var_name]
                    break
            if sample_var is None:
                sample_var = next(iter(gfs_analysis_sample_ds.data_vars.values()))
            sample_grid = sample_var.isel(time=0) if "time" in sample_var.dims else sample_var

            cache = await precompute_climatology_cache(
                era5_climatology=era5_climatology_ds,
                day1_scoring_config={"variables_levels_to_score": day1_cfg.get("variables", [])},
                times_to_evaluate=times_to_evaluate,
                sample_target_grid=sample_grid,
            )
            with open(data_path, "wb") as f:
                pickle.dump(cache, f)
            await log_success(
                db,
                run_id=run_id,
                miner_uid=0,
                miner_hotkey="coordinator",
                step_name="day1",
                substep="clim_cache",
            )
            return cache
        except Exception as e:
            await log_failure(
                db,
                run_id=run_id,
                miner_uid=0,
                miner_hotkey="coordinator",
                step_name="day1",
                substep="clim_cache",
                error_json={"type": "clim_cache_failed", "message": str(e)},
            )
            return None
        finally:
            try:
                os.remove(lock_path)
            except Exception:
                pass
    else:
        # Another worker is computing; avoid busy wait by returning None and letting caller retry via step scheduler
        return None


async def cleanup_clim_cache_if_done(db: ValidatorDatabaseManager, task: WeatherTask, *, run_id: int) -> None:
    # Check if all miners have completed day1 or failed
    done_row = await db.fetch_one(
        sa.text(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN forecast_status IN ('day1_scored','era5_scoring','completed','failed') THEN 1 ELSE 0 END) AS done
            FROM weather_forecast_stats
            WHERE run_id = :rid
            """
        ),
        {"rid": run_id},
    )
    if not done_row:
        return
    total = int(done_row.get("total") or 0)
    done = int(done_row.get("done") or 0)
    if total > 0 and done >= total:
        # delete cache file
        paths = _cache_paths(task, run_id)
        try:
            if paths["data"].exists():
                paths["data"].unlink()
        except Exception:
            pass


