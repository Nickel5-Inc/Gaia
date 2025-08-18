from __future__ import annotations

from typing import Optional, Any

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.pipeline.scheduler import MinerWorkScheduler
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
    cache_dir = Path(task.config.get("gfs_analysis_cache_dir", "./gfs_analysis_cache"))
    gfs_analysis_ds = await fetch_gfs_analysis_data([gfs_init], cache_dir=cache_dir)
    if not gfs_analysis_ds:
        raise RuntimeError("fetch_gfs_analysis_data returned None")
    # day1 uses initial scoring leads
    leads = task.config.get("initial_scoring_lead_hours", [6, 12])
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
async def _score_day1(db, task: WeatherTask, *, run_id: int, miner_uid: int, miner_hotkey: str, gfs_init, miner_record: dict, gfs_analysis_ds, gfs_ref_ds, era5_clim, day1_cfg: dict):
    from gaia.tasks.defined_tasks.weather.weather_scoring_mechanism import (
        evaluate_miner_forecast_day1,
    )
    return await evaluate_miner_forecast_day1(
        task_instance=task,
        miner_response_db_record=miner_record,
        gfs_analysis_ds_for_run=gfs_analysis_ds,
        gfs_reference_forecast_for_run=gfs_ref_ds,
        era5_climatology=era5_clim,
        day1_scoring_config=day1_cfg,
        run_gfs_init_time=gfs_init,
        precomputed_climatology_cache=None,
    )
from gaia.tasks.defined_tasks.weather.utils.gfs_api import (
    fetch_gfs_analysis_data,
    fetch_gfs_data,
)


async def run(db: ValidatorDatabaseManager, validator: Optional[Any] = None) -> bool:
    """Claim and process a single Day1 scoring step for one miner, update stats."""
    sched = MinerWorkScheduler(db)
    item = await sched.claim_day1()
    if not item:
        return False

    # Pull response and run details
    resp = await db.fetch_one(
        "SELECT id, run_id, miner_uid, miner_hotkey, job_id FROM weather_miner_responses WHERE run_id = :rid AND miner_uid = :uid",
        {"rid": item.run_id, "uid": item.miner_uid},
    )
    if not resp:
        return False

    run = await db.fetch_one(
        "SELECT id, gfs_init_time_utc FROM weather_forecast_runs WHERE id = :rid",
        {"rid": item.run_id},
    )
    if not run:
        return False

    # Minimal WeatherTask for helpers/config
    task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
    if validator is not None:
        setattr(task, "validator", validator)

    # Use stored gfs_init_time_utc directly; it already includes test-mode shift at run creation
    gfs_init = run["gfs_init_time_utc"]

    # Fetch inputs (decorated sub-step with retries)
    try:
        gfs_analysis_ds, gfs_ref_ds, era5_clim = await _load_inputs(
            db,
            task,
            run_id=item.run_id,
            miner_uid=item.miner_uid,
            miner_hotkey=item.miner_hotkey,
            gfs_init=gfs_init,
        )
    except Exception:
        return False

    # Build a record matching evaluate_miner_forecast_day1 signature
    miner_record = {
        "id": resp["id"],
        "miner_hotkey": resp["miner_hotkey"],
        "run_id": resp["run_id"],
        "miner_uid": resp["miner_uid"],
        "job_id": resp.get("job_id"),
    }

    day1_cfg = {
        "variables": task.config.get("day1_variables_levels_to_score", []),
        "pattern_correlation_threshold": task.config.get("day1_pattern_correlation_threshold", 0.3),
        "acc_lower_bound": task.config.get("day1_acc_lower_bound", 0.6),
        "alpha_skill": task.config.get("day1_alpha_skill", 0.6),
        "beta_acc": task.config.get("day1_beta_acc", 0.4),
        "clone_penalty_gamma": task.config.get("day1_clone_penalty_gamma", 1.0),
        "clone_delta_thresholds": task.config.get("day1_clone_delta_thresholds", {}),
    }

    # Ensure shared climatology cache exists on disk (one per run)
    try:
        _ = await ensure_clim_cache(
            db,
            task,
            run_id=item.run_id,
            gfs_analysis_sample_ds=gfs_analysis_ds,
            gfs_init=gfs_init,
            day1_cfg=day1_cfg,
            era5_climatology_ds=era5_clim,
        )
    except Exception:
        pass
    # Score with latency measurement
    import time
    t0 = time.perf_counter()
    try:
        result = await _score_day1(
            db,
            task,
            run_id=item.run_id,
            miner_uid=item.miner_uid,
            miner_hotkey=item.miner_hotkey,
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
            
            # Extract and store component scores
            lead_time_scores = result.get("lead_time_scores", {})
            if lead_time_scores:
                from datetime import timedelta
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
                                    "rmse": var_data.get("rmse"),  # May not be present, will be None
                                    "bias": var_data.get("bias"),  # May not be present
                                    "mae": var_data.get("mae"),  # May not be present
                                    "pattern_correlation": var_data.get("pattern_correlation"),
                                    "pattern_correlation_passed": var_data.get("pattern_correlation_passed"),
                                    "climatology_check_passed": var_data.get("climatology_check_passed"),
                                    "clone_penalty": var_data.get("clone_penalty_applied", 0),
                                    "weight": 1.0 / len(variables),  # Equal weight for now
                                    "calculation_duration_ms": latency_ms // len(lead_time_scores),  # Estimate
                                }
                        
                        # Record component scores
                        if variable_scores:
                            await stats.record_component_scores(
                                run_id=item.run_id,
                                response_id=item.response_id,
                                miner_uid=item.miner_uid,
                                miner_hotkey=item.miner_hotkey,
                                score_type="day1",
                                lead_hours=int(lead_hours),
                                valid_time=valid_time,
                                variable_scores=variable_scores
                            )
            
            # Update forecast stats with overall score
            await stats.update_forecast_stats(
                run_id=item.run_id,
                miner_uid=item.miner_uid,
                miner_hotkey=item.miner_hotkey,
                status="day1_scored",
                initial_score=float(overall),
            )
            await log_success(
                db,
                run_id=item.run_id,
                miner_uid=item.miner_uid,
                miner_hotkey=item.miner_hotkey,
                step_name="day1",
                substep="score",
                latency_ms=latency_ms,
            )
            # Per-step aggregation: update this miner's aggregate stats
            try:
                await stats.aggregate_miner_stats(miner_uid=item.miner_uid)
            except Exception:
                pass
        # Run-level completion check
        from gaia.tasks.defined_tasks.weather.processing.weather_logic import _check_run_completion
        try:
            await _check_run_completion(task, item.run_id)
        except Exception:
            pass
        # Attempt cache cleanup if all miners done/failed for day1
        try:
            await cleanup_clim_cache_if_done(db, task, run_id=item.run_id)
        except Exception:
            pass
    except Exception:
        # schedule retry on failure
        try:
            from gaia.tasks.defined_tasks.weather.pipeline.retry_policy import next_retry_time as _nrt
            nrt = _nrt("day1", 1)
            await log_failure(
                db,
                run_id=item.run_id,
                miner_uid=item.miner_uid,
                miner_hotkey=item.miner_hotkey,
                step_name="day1",
                substep="score",
                error_json={"type": "day1_unknown", "message": "exception during scoring"},
                retry_count=1,
                next_retry_time=nrt,
            )
        except Exception:
            pass

    return bool(result)


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
    miner_record = {
        "id": resp["id"],
        "miner_hotkey": miner_hotkey,
        "run_id": run_id,
        "miner_uid": miner_uid,
        "job_id": resp.get("job_id"),
    }
    day1_cfg = {
        "variables": task.config.get("day1_variables_levels_to_score", []),
        "pattern_correlation_threshold": task.config.get("day1_pattern_correlation_threshold", 0.3),
        "acc_lower_bound": task.config.get("day1_acc_lower_bound", 0.6),
        "alpha_skill": task.config.get("day1_alpha_skill", 0.6),
        "beta_acc": task.config.get("day1_beta_acc", 0.4),
        "clone_penalty_gamma": task.config.get("day1_clone_penalty_gamma", 1.0),
        "clone_delta_thresholds": task.config.get("day1_clone_delta_thresholds", {}),
    }
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
