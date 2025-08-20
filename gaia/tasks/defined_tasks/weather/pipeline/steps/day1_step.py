from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Any

logger = logging.getLogger(__name__)

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
# REMOVED: MinerWorkScheduler import - no longer needed since run() method was removed
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
from gaia.tasks.defined_tasks.weather.weather_scoring_mechanism import (
    evaluate_miner_forecast_day1,
)
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from .step_logger import log_start, log_success, log_failure
from .clim_cache import ensure_clim_cache, cleanup_clim_cache_if_done
from .substep import substep
from .util_time import get_effective_gfs_init


@substep("day1", "load_inputs", should_retry=True, retry_delay_seconds=600, max_retries=5, retry_backoff="exponential")
async def _load_inputs(db, task: WeatherTask, *, run_id: int, miner_uid: int, miner_hotkey: str, gfs_init):
    from gaia.tasks.defined_tasks.weather.utils.gfs_api import (
        fetch_gfs_analysis_data,
        fetch_gfs_data,
    )
    from pathlib import Path
    from datetime import timedelta
    cache_dir = Path(task.config.get("gfs_analysis_cache_dir", "./gfs_analysis_cache"))
    
    # day1 uses initial scoring leads
    leads = task.config.get("initial_scoring_lead_hours", [6, 12])
    
    # Fetch GFS analysis data at evaluation times as ground truth
    analysis_times = [gfs_init + timedelta(hours=h) for h in leads]
    gfs_analysis_ds = await fetch_gfs_analysis_data(analysis_times, cache_dir=cache_dir)
    if not gfs_analysis_ds:
        raise RuntimeError("fetch_gfs_analysis_data returned None")
    
    # Fetch GFS forecast data for reference comparison
    gfs_ref_ds = await fetch_gfs_data(
        run_time=gfs_init,
        lead_hours=leads,
        output_dir=str(cache_dir),
    )
    if not gfs_ref_ds:
        raise RuntimeError("fetch_gfs_data returned None")
    
    era5_clim = await task._get_or_load_era5_climatology()
    if not era5_clim:
        raise RuntimeError("ERA5 climatology not available")
    
    return gfs_analysis_ds, gfs_ref_ds, era5_clim


@substep("day1", "score", should_retry=True, retry_delay_seconds=900, max_retries=3, retry_backoff="linear")
async def _score_day1(db, task: WeatherTask, *, run_id: int, miner_uid: int, miner_hotkey: str, gfs_init, miner_record: dict, gfs_analysis_ds, gfs_ref_ds, era5_clim, day1_cfg: dict, precomputed_cache=None):
    from gaia.tasks.defined_tasks.weather.weather_scoring_mechanism import (
        evaluate_miner_forecast_day1,
    )
    return await evaluate_miner_forecast_day1(
        task_instance=task,
        miner_response_db_record=miner_record,
        gfs_analysis_data_for_run=gfs_analysis_ds,
        gfs_reference_forecast_for_run=gfs_ref_ds,
        era5_climatology=era5_clim,
        day1_scoring_config=day1_cfg,
        run_gfs_init_time=gfs_init,
        precomputed_climatology_cache=precomputed_cache,
    )
# REMOVED: Duplicated GFS API imports - already imported in _load_inputs function


# REMOVED: Unused run() method that was never called in current execution paths
# All Day1 scoring now goes through run_item() method which is called by workers.py


async def run_item(
    db: ValidatorDatabaseManager,
    *,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    response_id: int,
    validator: Optional[Any] = None,
) -> bool:
    """Process Day1 for a specific miner/run item (used by generic queue dispatcher)."""
    run = await db.fetch_one(
        "SELECT id, gfs_init_time_utc FROM weather_forecast_runs WHERE id = :rid",
        {"rid": run_id},
    )
    if not run:
        return False
    task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
    if validator is not None:
        setattr(task, "validator", validator)
    gfs_init = run["gfs_init_time_utc"]
    resp = await db.fetch_one(
        "SELECT id, run_id, miner_uid, miner_hotkey, job_id FROM weather_miner_responses WHERE id = :rid",
        {"rid": response_id},
    )
    if not resp:
        return False
    
    # CRITICAL: Validate miner workflow isolation - prevent any crossover between miners
    if resp["run_id"] != run_id:
        logger.error(
            f"[ISOLATION VIOLATION] Response {response_id} belongs to run {resp['run_id']} "
            f"but job specifies run {run_id}. Aborting to prevent crossover."
        )
        return False
    
    if resp["miner_uid"] != miner_uid:
        logger.error(
            f"[ISOLATION VIOLATION] Response {response_id} belongs to miner UID {resp['miner_uid']} "
            f"but job specifies miner UID {miner_uid}. Aborting to prevent crossover."
        )
        return False
    # Fetch inputs
    try:
        gfs_analysis_ds, gfs_ref_ds, era5_clim = await _load_inputs(
            db,
            task,
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            gfs_init=gfs_init,
        )
    except Exception:
        return False
    # Use hotkey from database record to ensure manifest verification uses correct signer
    db_miner_hotkey = resp["miner_hotkey"]
    if miner_hotkey != db_miner_hotkey:
        logger.warning(
            f"[Run {run_id}] Hotkey mismatch detected for miner UID {miner_uid}: "
            f"parameter={miner_hotkey[:8]}..., database={db_miner_hotkey[:8]}... "
            f"Using database value for manifest verification."
        )
    
    miner_record = {
        "id": resp["id"],
        "miner_hotkey": db_miner_hotkey,  # Use hotkey from database record, not parameter
        "run_id": run_id,
        "miner_uid": miner_uid,
        "job_id": resp.get("job_id"),
    }
    day1_cfg = {
        "variables_levels_to_score": task.config.get("day1_variables_levels_to_score", []),  # Fix key name
        "pattern_correlation_threshold": task.config.get("day1_pattern_correlation_threshold", 0.3),
        "acc_lower_bound": task.config.get("day1_acc_lower_bound", 0.6),
        "alpha_skill": task.config.get("day1_alpha_skill", 0.6),
        "beta_acc": task.config.get("day1_beta_acc", 0.4),
        "clone_penalty_gamma": task.config.get("day1_clone_penalty_gamma", 1.0),
        "clone_delta_thresholds": task.config.get("day1_clone_delta_thresholds", {}),
        "lead_times_hours": task.config.get("initial_scoring_lead_hours", [6, 12]),  # Add lead times
    }
    
    # CRITICAL: Debug configuration to understand why scoring fails
    logger.info(
        f"[Day1Step] Run {run_id}, Miner {miner_uid}: Configuration:"
        f"\n  Variables: {len(day1_cfg['variables_levels_to_score'])} configured"
        f"\n  Lead times: {day1_cfg['lead_times_hours']}"
        f"\n  Pattern threshold: {day1_cfg['pattern_correlation_threshold']}"
        f"\n  ACC lower bound: {day1_cfg['acc_lower_bound']}"
    )
    
    if not day1_cfg["variables_levels_to_score"]:
        logger.error(
            f"[Day1Step] Run {run_id}, Miner {miner_uid}: "
            f"No variables configured for Day1 scoring! Check day1_variables_levels_to_score in task config."
        )
    
    import time
    t0 = time.perf_counter()
    try:
        result = await _score_day1(
            db,
            task,
            run_id=run_id,
            miner_uid=miner_uid,
            miner_hotkey=miner_hotkey,
            gfs_init=gfs_init,
            miner_record=miner_record,
            gfs_analysis_ds=gfs_analysis_ds,
            gfs_ref_ds=gfs_ref_ds,
            era5_clim=era5_clim,
            day1_cfg=day1_cfg,
        )
    except Exception:
        result = None
    latency_ms = int((time.perf_counter() - t0) * 1000)
    try:
        overall = None
        if isinstance(result, dict):
            overall = result.get("overall_day1_score") or result.get("overall_score")
        if overall is not None:
            stats = WeatherStatsManager(
                db,
                validator_hotkey=(
                    getattr(getattr(getattr(validator, "validator_wallet", None), "hotkey", None), "ss58_address", None)
                    if validator is not None
                    else "unknown_validator"
                ),
            )
            
            # CRITICAL FIX: Record component scores and miner scores like the run() method does
            if isinstance(result, dict) and "lead_time_scores" in result:
                lead_time_scores = result["lead_time_scores"]
                
                # Record component scores for each lead time
                for lead_hours, variables in lead_time_scores.items():
                    if variables:
                        # Convert lead_hours to valid_time
                        valid_time = gfs_init + timedelta(hours=int(lead_hours))
                        
                        # Prepare variable scores for component score recording
                        variable_scores = {}
                        for var_key, var_data in variables.items():
                            if isinstance(var_data, dict):
                                # Extract pressure level if present in var_key (e.g., "t_850")
                                parts = var_key.split('_')
                                var_name = parts[0]
                                pressure_level = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
                                
                                variable_scores[var_name] = {
                                    "pressure_level": pressure_level,
                                    "skill_score": var_data.get("skill_score"),
                                    "acc": var_data.get("acc_score"),
                                    "rmse": var_data.get("rmse"),
                                    "bias": var_data.get("bias"),
                                    "mae": var_data.get("mae"),
                                    "pattern_correlation": var_data.get("pattern_correlation"),
                                    "pattern_correlation_passed": var_data.get("pattern_correlation_passed"),
                                    "climatology_check_passed": var_data.get("climatology_check_passed"),
                                    "clone_penalty": var_data.get("clone_penalty_applied", 0),
                                    "weight": 1.0 / len(variables),
                                    "calculation_duration_ms": latency_ms // len(lead_time_scores),
                                }
                        
                        # Record component scores
                        if variable_scores:
                            await stats.record_component_scores(
                                run_id=run_id,
                                response_id=response_id,
                                miner_uid=miner_uid,
                                miner_hotkey=miner_hotkey,
                                score_type="day1",
                                lead_hours=int(lead_hours),
                                valid_time=valid_time,
                                variable_scores=variable_scores
                            )
                            
                # CRITICAL FIX: Write overall score to weather_miner_scores table
                await db.execute(
                    """
                    INSERT INTO weather_miner_scores 
                    (response_id, run_id, miner_uid, miner_hotkey, score_type, score, calculation_time, variable_level)
                    VALUES (:response_id, :run_id, :miner_uid, :miner_hotkey, :score_type, :score, :calculation_time, :variable_level)
                    ON CONFLICT (response_id, score_type, lead_hours, variable_level, valid_time_utc) 
                    DO UPDATE SET score = EXCLUDED.score, calculation_time = EXCLUDED.calculation_time
                    """,
                    {
                        "response_id": response_id,
                        "run_id": run_id,
                        "miner_uid": miner_uid,
                        "miner_hotkey": miner_hotkey,
                        "score_type": "day1_qc_score",
                        "score": float(overall),
                        "calculation_time": datetime.now(timezone.utc),
                        "variable_level": "overall_day1"
                    }
                )
                logger.info(f"[Day1Step] Recorded overall Day1 score {overall:.4f} to weather_miner_scores for miner {miner_uid}")
            
            await stats.update_forecast_stats(
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                status="day1_scored",
                initial_score=float(overall),
            )
            await log_success(
                db,
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                step_name="day1",
                substep="score",
                latency_ms=latency_ms,
            )
            try:
                await stats.aggregate_miner_stats(miner_uid=miner_uid)
            except Exception:
                pass
        from gaia.tasks.defined_tasks.weather.processing.weather_logic import _check_run_completion
        try:
            await _check_run_completion(task, run_id)
        except Exception:
            pass
        try:
            await cleanup_clim_cache_if_done(db, task, run_id=run_id)
        except Exception:
            pass
    except Exception:
        try:
            from gaia.tasks.defined_tasks.weather.pipeline.retry_policy import next_retry_time as _nrt
            nrt = _nrt("day1", 1)
            await log_failure(
                db,
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                step_name="day1",
                substep="score",
                error_json={"type": "day1_unknown", "message": "exception during scoring"},
                retry_count=1,
                next_retry_time=nrt,
            )
        except Exception:
            pass
    return bool(result)
