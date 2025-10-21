from __future__ import annotations

import logging
import numpy as np
import os
from datetime import datetime, timezone, timedelta
from typing import Optional, Any

from gaia.utils.custom_logger import get_logger
logger = get_logger(__name__)

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
import sqlalchemy as sa
# REMOVED: MinerWorkScheduler import - no longer needed since run() method was removed
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
from gaia.tasks.defined_tasks.weather.weather_scoring_mechanism import (
    evaluate_miner_forecast_day1,
)
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from .step_logger import log_start, log_success, log_failure
from .clim_cache import cleanup_clim_cache_if_done
from .substep import substep
from .util_time import get_effective_gfs_init
from ...weather_scoring.scoring import VARIABLE_WEIGHTS


async def _score_variable_batch(
    task: WeatherTask,
    miner_record: dict,
    gfs_analysis_ds,
    gfs_ref_ds,
    era5_clim,
    day1_cfg: dict,
    gfs_init,
    variable_batch: list,
    precomputed_cache=None
):
    """
    Score a batch of variables for a miner to manage memory usage.
    """
    # Create a modified config with only the variables in this batch
    batch_config = day1_cfg.copy()
    batch_config["variables_levels_to_score"] = variable_batch
    
    return await evaluate_miner_forecast_day1(
        task_instance=task,
        miner_response_db_record=miner_record,
        gfs_analysis_data_for_run=gfs_analysis_ds,
        gfs_reference_forecast_for_run=gfs_ref_ds,
        era5_climatology=era5_clim,
        day1_scoring_config=batch_config,
        run_gfs_init_time=gfs_init,
        precomputed_climatology_cache=precomputed_cache,
    )


@substep("day1", "load_inputs", should_retry=True, retry_delay_seconds=600, max_retries=5, retry_backoff="exponential")
async def _load_inputs(db, task: WeatherTask, *, run_id: int, miner_uid: int, miner_hotkey: str, gfs_init):
    from gaia.tasks.defined_tasks.weather.utils.gfs_api import (
        fetch_gfs_analysis_data,
        fetch_gfs_data,
    )
    from pathlib import Path
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
    """
    Main day1 scoring orchestrator - now creates individual variable-level jobs
    instead of scoring all variables at once.
    """
    run = await db.fetch_one(
        "SELECT id, gfs_init_time_utc FROM weather_forecast_runs WHERE id = :rid",
        {"rid": run_id},
    )
    if not run:
        return False
    # Reuse a single WeatherTask per worker via validator-carried singleton
    task = None
    if validator is not None:
        task = getattr(validator, "weather_task_singleton", None)
    if task is None:
        task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
        if validator is not None:
            try:
                setattr(validator, "weather_task_singleton", task)
            except Exception:
                pass
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
        # Strict clamp configuration
        "strict_clone_clamp_enabled": task.config.get("day1_strict_clone_clamp_enabled", True),
        "strict_clone_clamp_fraction": task.config.get("day1_strict_clone_clamp_fraction", 0.5),
        "strict_clamp_value": task.config.get("day1_strict_clamp_value", 0.05),
    }
    
    # CRITICAL: Debug configuration to understand why scoring fails
    logger.info(
        f"[Day1Step] Run {run_id}, Miner {miner_uid}: Configuration:"
        f"\n  Variables: {len(day1_cfg['variables_levels_to_score'])} configured"
        f"\n  Lead times: {day1_cfg['lead_times_hours']}"
        f"\n  Pattern threshold: {day1_cfg['pattern_correlation_threshold']}"
        f"\n  ACC lower bound: {day1_cfg['acc_lower_bound']}"
        f"\n  Strict clamp: {day1_cfg['strict_clone_clamp_enabled']} at {day1_cfg['strict_clone_clamp_fraction']}x, value={day1_cfg['strict_clamp_value']}"
    )
    
    if not day1_cfg["variables_levels_to_score"]:
        logger.error(
            f"[Day1Step] Run {run_id}, Miner {miner_uid}: "
            f"No variables configured for Day1 scoring! Check day1_variables_levels_to_score in task config."
        )
    
    # Process variables individually to manage memory usage
    all_variables = day1_cfg["variables_levels_to_score"]
    
    logger.info(f"[Day1Step] Run {run_id} Miner {miner_uid}: Processing {len(all_variables)} variables individually")
    
    import time
    t0 = time.perf_counter()
    
    # Load dataset once into shared readonly memory for maximum efficiency
    logger.info(f"[Day1Step] Run {run_id} Miner {miner_uid}: Loading miner dataset into shared memory for {len(all_variables)} variables")
    
    # Load the entire dataset once and share it across all variable processing
    shared_miner_dataset = None
    try:
        # Score all variables at once using shared dataset
        result = await _score_variable_batch(
            task=task,
            miner_record=miner_record,
            gfs_analysis_ds=gfs_analysis_ds,
            gfs_ref_ds=gfs_ref_ds,
            era5_clim=era5_clim,
            day1_cfg=day1_cfg,
            gfs_init=gfs_init,
            variable_batch=all_variables,  # Process all variables with shared dataset
            precomputed_cache=None
        )
        
    except Exception as e:
        logger.error(f"[Day1Step] Error in variable processing: {e}", exc_info=True)
        result = None
    finally:
        # CRITICAL: Aggressive cleanup of shared dataset and all associated caches
        if shared_miner_dataset is not None:
            try:
                # Close the dataset
                shared_miner_dataset.close()
                logger.debug(f"[Day1Step] Closed shared miner dataset for miner {miner_uid}")
            except Exception:
                pass
        
        # Force garbage collection to clear any lingering references
        import gc
        import sys
        
        # Clear any module-level caches that might hold dataset references
        try:
            # Clear xarray caches
            if hasattr(sys.modules.get('xarray', None), '_cache'):
                sys.modules['xarray']._cache.clear()
        except Exception:
            pass
            
        # Force aggressive garbage collection
        collected = gc.collect()
        logger.debug(f"[Day1Step] Aggressive cleanup collected {collected} objects for miner {miner_uid}")
        
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
                        
                        # Prepare component scores with detailed pressure-level breakdown
                        component_scores_to_insert = []
                        for var_key, var_data in variables.items():
                            if isinstance(var_data, dict):
                                # Parse variable name from var_key
                                var_name = var_key
                                
                                # Check if this variable has detailed pressure-level scores
                                pressure_level_scores = var_data.get("pressure_level_scores", {})
                                
                                if pressure_level_scores:
                                    # Create component scores for each pressure level
                                    skill_scores = pressure_level_scores.get("skill", {})
                                    acc_scores = pressure_level_scores.get("acc", {})
                                    rmse_scores = pressure_level_scores.get("rmse", {})
                                    mae_scores = pressure_level_scores.get("mae", {})
                                    bias_scores = pressure_level_scores.get("bias", {})
                                    
                                    # Get all pressure levels that have scores
                                    all_levels = set(skill_scores.keys()) | set(acc_scores.keys()) | set(rmse_scores.keys())
                                    
                                    for pressure_level in all_levels:
                                        component_score = {
                                            "run_id": run_id,
                                            "response_id": response_id,
                                            "miner_uid": miner_uid,
                                            "miner_hotkey": miner_hotkey,
                                            "score_type": "day1",
                                            "lead_hours": int(lead_hours),
                                            "valid_time_utc": valid_time,
                                            "variable_name": var_name,
                                            "pressure_level": pressure_level,
                                            "skill_score": skill_scores.get(pressure_level),
                                            "acc": acc_scores.get(pressure_level),
                                            "rmse": rmse_scores.get(pressure_level),
                                            "mse": (rmse_scores.get(pressure_level) ** 2) if rmse_scores.get(pressure_level) is not None else None,
                                            "bias": bias_scores.get(pressure_level),
                                            "mae": mae_scores.get(pressure_level),
                                            "pattern_correlation": var_data.get("pattern_correlation"),
                                            "pattern_correlation_passed": var_data.get("pattern_correlation_passed"),
                                            "climatology_check_passed": var_data.get("climatology_check_passed"),
                                            "clone_penalty": var_data.get("clone_penalty_applied", 0),
                                            "variable_weight": VARIABLE_WEIGHTS.get(var_name, 0.0),
                                            "calculation_duration_ms": latency_ms // len(lead_time_scores),
                                        }
                                        component_scores_to_insert.append(component_score)
                                else:
                                    # Traditional single-score approach for surface variables
                                    # Parse pressure level if present in var_key
                                    pressure_level = None
                                    if var_key[-4:].isdigit() and len(var_key) > 4:
                                        pressure_level = int(var_key[-4:])
                                        var_name = var_key[:-4]
                                    elif var_key[-3:].isdigit() and len(var_key) > 3:
                                        pressure_level = int(var_key[-3:])
                                        var_name = var_key[:-3]
                                    
                                    # Prefer climatology-referenced skill if present for comparability with ERA5
                                    climatology_skill = var_data.get("skill_score_climatology")
                                    component_score = {
                                        "run_id": run_id,
                                        "response_id": response_id,
                                        "miner_uid": miner_uid,
                                        "miner_hotkey": miner_hotkey,
                                        "score_type": "day1",
                                        "lead_hours": int(lead_hours),
                                        "valid_time_utc": valid_time,
                                        "variable_name": var_name,
                                        "pressure_level": pressure_level,
                                        "skill_score": climatology_skill if climatology_skill is not None else var_data.get("skill_score"),
                                        "acc": var_data.get("acc_score"),
                                        "rmse": var_data.get("rmse"),
                                        "mse": (var_data.get("rmse") ** 2) if var_data.get("rmse") is not None else None,
                                        "bias": var_data.get("bias"),
                                        "mae": var_data.get("mae"),
                                        "pattern_correlation": var_data.get("pattern_correlation"),
                                        "pattern_correlation_passed": var_data.get("pattern_correlation_passed"),
                                        "climatology_check_passed": var_data.get("climatology_check_passed"),
                                        "clone_penalty": var_data.get("clone_penalty_applied", 0),
                                        "variable_weight": VARIABLE_WEIGHTS.get(var_name, 0.0),
                                        "calculation_duration_ms": latency_ms // len(lead_time_scores),
                                    }
                                    component_scores_to_insert.append(component_score)
                        
                        # Record detailed component scores directly using batch insert
                        if component_scores_to_insert:
                            from gaia.tasks.defined_tasks.weather.processing.weather_logic import _batch_insert_component_scores
                            success = await _batch_insert_component_scores(
                                db, component_scores_to_insert, batch_size=50
                            )
                            if success:
                                logger.info(f"[Day1Step] Inserted {len(component_scores_to_insert)} detailed component scores for lead {lead_hours}h")
                            else:
                                logger.warning(f"[Day1Step] Failed to insert some component scores for lead {lead_hours}h")
                            
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
            
            # Compute averages across all Day1 component scores for this run/miner
            try:
                avg_row = await db.fetch_one(
                    sa.text(
                        """
                        SELECT AVG(rmse) AS avg_rmse, AVG(acc) AS avg_acc, AVG(skill_score) AS avg_skill
                        FROM weather_forecast_component_scores
                        WHERE run_id = :rid AND miner_uid = :uid AND score_type = 'day1'
                        """
                    ),
                    {"rid": run_id, "uid": miner_uid},
                )
                avg_rmse = float(avg_row["avg_rmse"]) if avg_row and avg_row["avg_rmse"] is not None else None
                avg_acc = float(avg_row["avg_acc"]) if avg_row and avg_row["avg_acc"] is not None else None
                avg_skill = float(avg_row["avg_skill"]) if avg_row and avg_row["avg_skill"] is not None else None
            except Exception:
                avg_rmse = None
                avg_acc = None
                avg_skill = None

            # Also compute an overall_forecast_score consistent with ERA5 path (95/5 blend with completeness penalty)
            try:
                qc_threshold = float(task.config.get("day1_binary_threshold", 0.1))
                day1_pass = 1.0 if (overall is not None and float(overall) >= qc_threshold) else 0.0
                # Until ERA5 exists, treat completeness as zero leads; overall stays 0.05 max if day1 passes after normalization by full horizon
                W_era5 = 0.95
                W_day1 = 0.05
                # With no ERA5 yet, era5_norm_avg is 0.0
                overall_blended = W_era5 * 0.0 + W_day1 * day1_pass
            except Exception:
                overall_blended = None

            await stats.update_forecast_stats(
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                status="day1_scored",
                initial_score=float(overall),
                avg_rmse=avg_rmse,
                avg_acc=avg_acc,
                avg_skill_score=avg_skill,
                overall_forecast_score=overall_blended,
            )
            
            # PIPELINE TIMING: Record Day1 completion time
            await db.execute(
                """
                UPDATE weather_miner_responses 
                SET day1_scoring_completed_at = NOW()
                WHERE run_id = :run_id AND miner_uid = :miner_uid
                """,
                {"run_id": run_id, "miner_uid": miner_uid}
            )
            logger.debug(f"[Day1Step] Recorded Day1 completion time for miner {miner_uid}")
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
            # NEW: Write/refresh combined initial weather scores into score_table for this run
            try:
                combined_task = getattr(validator, "weather_task_singleton", None)
                if combined_task is None:
                    combined_task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
                    try:
                        setattr(validator, "weather_task_singleton", combined_task)
                    except Exception:
                        pass
                await combined_task.update_combined_weather_scores(
                    run_id_trigger=run_id, force_phase="initial"
                )
                logger.info(
                    f"[Day1Step] Updated score_table initial row for run {run_id}"
                )
            except Exception as e_comb:
                logger.warning(
                    f"[Day1Step] Failed to update combined initial score row for run {run_id}: {e_comb}"
                )
            
            # ERA5 successor job creation removed. Global guard will orchestrate lead-ready steps.
            # TEST MODE FAST-PATH: Directly enqueue per-miner ERA5 jobs with a short delay, but only
            # for miners that have completed Day1 scoring for this run.
            try:
                if getattr(task, "test_mode", False):
                    # Verify this miner has recorded Day1 score for the run
                    row = await db.fetch_one(
                        sa.text(
                            """
                            SELECT 1
                            FROM weather_miner_scores
                            WHERE run_id = :rid AND miner_uid = :uid AND score_type = 'day1_qc_score'
                            LIMIT 1
                            """
                        ),
                        {"rid": run_id, "uid": miner_uid},
                    )
                    if row:
                        # Enqueue the miner-level ERA5 step after ~2 minutes
                        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
                        scheduled = _dt.now(_tz.utc) + _td(minutes=2)
                        await db.enqueue_validator_job(
                            job_type="weather.era5",
                            payload={
                                "run_id": run_id,
                                "miner_uid": miner_uid,
                                "response_id": response_id,
                            },
                            priority=85,
                            scheduled_at=scheduled,
                            run_id=run_id,
                            miner_uid=miner_uid,
                            response_id=response_id,
                        )
                        logger.info(
                            f"[Day1Step] TEST MODE: Scheduled immediate ERA5 job (in ~2m) for run {run_id}, miner {miner_uid}"
                        )
            except Exception as _e_enqueue:
                logger.warning(
                    f"[Day1Step] TEST MODE: Failed to enqueue ERA5 job fast-path for run {run_id}, miner {miner_uid}: {_e_enqueue}"
                )
            
        from gaia.tasks.defined_tasks.weather.processing.weather_logic import _check_run_completion
        try:
            await _check_run_completion(task, run_id)
        except Exception:
            pass
        try:
            await cleanup_clim_cache_if_done(db, task, run_id=run_id)
        except Exception:
            pass
    except Exception as e:
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
                error_json={
                    "type": "day1_unknown",
                    "message": str(e),
                    "exception_type": type(e).__name__,
                },
                retry_count=1,
                next_retry_time=nrt,
            )
        except Exception:
            pass
    return bool(result)
