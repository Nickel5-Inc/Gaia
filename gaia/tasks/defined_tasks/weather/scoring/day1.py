"""
Weather Day-1 Scoring Module

Day-1 scoring for weather forecasts using GFS analysis as truth and GFS forecast as reference.
Calculates bias-corrected skill scores and ACC for specified variables and lead times.

Extracted from the monolithic weather_scoring_mechanism.py (1,838 lines) for better modularity.
"""

import asyncio
import gc
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Dict, List, Optional

import numpy as np
import xarray as xr
from fiber.logging_utils import get_logger

if TYPE_CHECKING:
    from ..core.task import WeatherTask

from ..utils.remote_access import open_verified_remote_zarr_dataset
from ..weather_scoring.metrics import (
    _calculate_latitude_weights,
    calculate_acc,
    calculate_bias_corrected_forecast,
    calculate_mse_skill_score,
    perform_sanity_checks,
)

logger = get_logger(__name__)


async def evaluate_miner_forecast_day1(
    task_instance: "WeatherTask",
    miner_response_db_record: Dict,
    gfs_analysis_data_for_run: xr.Dataset,
    gfs_reference_forecast_for_run: xr.Dataset,
    era5_climatology: xr.Dataset,
    day1_scoring_config: Dict,
    run_gfs_init_time: datetime,
    precomputed_climatology_cache: Optional[Dict] = None,
) -> Dict:
    """
    Performs Day-1 scoring for a single miner's forecast.
    
    Uses GFS analysis as truth and GFS forecast as reference.
    Calculates bias-corrected skill scores and ACC for specified variables and lead times.
    
    Args:
        task_instance: WeatherTask instance
        miner_response_db_record: Database record for miner response
        gfs_analysis_data_for_run: GFS analysis data (truth)
        gfs_reference_forecast_for_run: GFS forecast data (reference)
        era5_climatology: ERA5 climatology dataset
        day1_scoring_config: Day-1 scoring configuration
        run_gfs_init_time: GFS initialization time
        precomputed_climatology_cache: Optional pre-computed climatology cache
        
    Returns:
        Dictionary with Day-1 scoring results
    """
    response_id = miner_response_db_record["id"]
    miner_hotkey = miner_response_db_record["miner_hotkey"]
    job_id = miner_response_db_record.get("job_id")
    run_id = miner_response_db_record.get("run_id")
    miner_uid = miner_response_db_record.get("miner_uid")

    # Start timing for this miner's scoring
    scoring_start_time = time.time()
    logger.info(
        f"[Day1Score] Starting for miner {miner_hotkey} "
        f"(Resp: {response_id}, Run: {run_id}, Job: {job_id}, UID: {miner_uid})"
    )

    day1_results = {
        "response_id": response_id,
        "miner_hotkey": miner_hotkey,
        "miner_uid": miner_uid,
        "run_id": run_id,
        "overall_day1_score": None,
        "qc_passed_all_vars_leads": True,
        "lead_time_scores": {},
        "error_message": None,
    }

    miner_forecast_ds: Optional[xr.Dataset] = None

    try:
        # Get fresh access token for miner's forecast data
        token_data_tuple = await _request_fresh_token(task_instance, miner_hotkey, job_id)
        if token_data_tuple is None:
            raise ValueError(
                f"Failed to get fresh access token/URL/manifest_hash for {miner_hotkey} job {job_id}"
            )

        access_token, zarr_store_url, claimed_manifest_content_hash = token_data_tuple

        if not all([access_token, zarr_store_url, claimed_manifest_content_hash]):
            raise ValueError(
                f"Critical forecast access info missing for {miner_hotkey} (Job: {job_id})"
            )

        # Open verified Zarr dataset
        logger.info(f"[Day1Score] Opening VERIFIED Zarr store for {miner_hotkey}: {zarr_store_url}")
        storage_options = {
            "headers": {"Authorization": f"Bearer {access_token}"},
            "ssl": False,
        }

        verification_timeout_seconds = task_instance.config.verification_timeout_seconds / 2

        miner_forecast_ds = await asyncio.wait_for(
            open_verified_remote_zarr_dataset(
                zarr_store_url=zarr_store_url,
                claimed_manifest_content_hash=claimed_manifest_content_hash,
                miner_hotkey_ss58=miner_hotkey,
                storage_options=storage_options,
                job_id=f"{job_id}_day1_score",
            ),
            timeout=verification_timeout_seconds,
        )

        if miner_forecast_ds is None:
            raise ConnectionError(f"Failed to open verified Zarr dataset for miner {miner_hotkey}")

        # Determine evaluation times
        times_to_evaluate = _get_evaluation_times(
            day1_scoring_config, task_instance, run_gfs_init_time
        )

        variables_to_score: List[Dict] = day1_scoring_config.get("variables_levels_to_score", [])

        # Process all time steps in parallel
        aggregated_skill_scores, aggregated_acc_scores = await _process_timesteps_parallel(
            miner_forecast_ds=miner_forecast_ds,
            gfs_analysis_data_for_run=gfs_analysis_data_for_run,
            gfs_reference_forecast_for_run=gfs_reference_forecast_for_run,
            era5_climatology=era5_climatology,
            times_to_evaluate=times_to_evaluate,
            variables_to_score=variables_to_score,
            day1_scoring_config=day1_scoring_config,
            run_gfs_init_time=run_gfs_init_time,
            miner_hotkey=miner_hotkey,
            day1_results=day1_results,
            precomputed_climatology_cache=precomputed_climatology_cache,
        )

        # Calculate final Day-1 score
        day1_results["overall_day1_score"] = _calculate_final_day1_score(
            aggregated_skill_scores, aggregated_acc_scores, day1_scoring_config, miner_hotkey
        )

    except ConnectionError as e_conn:
        logger.error(f"[Day1Score] Connection error for miner {miner_hotkey} (Job {job_id}): {e_conn}")
        day1_results["error_message"] = f"ConnectionError: {str(e_conn)}"
        day1_results["overall_day1_score"] = -np.inf
        day1_results["qc_passed_all_vars_leads"] = False
    except asyncio.TimeoutError:
        logger.error(f"[Day1Score] Timeout opening dataset for miner {miner_hotkey} (Job {job_id})")
        day1_results["error_message"] = "TimeoutError: Opening dataset timed out."
        day1_results["overall_day1_score"] = -np.inf
        day1_results["qc_passed_all_vars_leads"] = False
    except Exception as e_main:
        logger.error(
            f"[Day1Score] Main error for miner {miner_hotkey} (Resp: {response_id}): {e_main}",
            exc_info=True,
        )
        day1_results["error_message"] = str(e_main)
        day1_results["overall_day1_score"] = -np.inf
        day1_results["qc_passed_all_vars_leads"] = False
    finally:
        # Clean up miner-specific objects
        if miner_forecast_ds:
            try:
                miner_forecast_ds.close()
                logger.debug(f"[Day1Score] Closed miner forecast dataset for {miner_hotkey}")
            except Exception:
                pass

        # Cleanup and garbage collection
        _cleanup_miner_objects(miner_hotkey)

        # Log total scoring time
        total_scoring_time = time.time() - scoring_start_time
        logger.info(
            f"[Day1Score] TIMING: Miner {miner_hotkey} scoring completed in {total_scoring_time:.2f} seconds"
        )

    logger.info(
        f"[Day1Score] Completed for {miner_hotkey}. "
        f"Final score: {day1_results['overall_day1_score']}, "
        f"QC Passed: {day1_results['qc_passed_all_vars_leads']}"
    )
    return day1_results


def _get_evaluation_times(
    day1_scoring_config: Dict, task_instance: "WeatherTask", run_gfs_init_time: datetime
) -> List[datetime]:
    """Get the evaluation times for Day-1 scoring."""
    hardcoded_valid_times: Optional[List[datetime]] = day1_scoring_config.get(
        "hardcoded_valid_times_for_eval"
    )
    if hardcoded_valid_times:
        logger.warning(
            f"[Day1Score] USING HARDCODED VALID TIMES FOR EVALUATION: {hardcoded_valid_times}"
        )
        return hardcoded_valid_times
    else:
        lead_times_to_score_hours: List[int] = day1_scoring_config.get(
            "lead_times_hours", task_instance.config.initial_scoring_lead_hours
        )
        times_to_evaluate = [
            run_gfs_init_time + timedelta(hours=h) for h in lead_times_to_score_hours
        ]
        logger.info(
            f"[Day1Score] Using lead_hours {lead_times_to_score_hours} "
            f"relative to GFS init {run_gfs_init_time} for evaluation."
        )
        return times_to_evaluate


async def _process_timesteps_parallel(
    miner_forecast_ds: xr.Dataset,
    gfs_analysis_data_for_run: xr.Dataset,
    gfs_reference_forecast_for_run: xr.Dataset,
    era5_climatology: xr.Dataset,
    times_to_evaluate: List[datetime],
    variables_to_score: List[Dict],
    day1_scoring_config: Dict,
    run_gfs_init_time: datetime,
    miner_hotkey: str,
    day1_results: Dict,
    precomputed_climatology_cache: Optional[Dict],
) -> tuple:
    """Process all time steps in parallel for major performance improvement."""
    logger.info(
        f"[Day1Score] Miner {miner_hotkey}: Starting PARALLEL processing of {len(times_to_evaluate)} time steps"
    )
    parallel_timesteps_start = time.time()

    # Create parallel tasks for all time steps
    timestep_tasks = []
    for valid_time_dt in times_to_evaluate:
        effective_lead_h = int((valid_time_dt - run_gfs_init_time).total_seconds() / 3600)

        # Normalize timezone for parallel processing
        if valid_time_dt.tzinfo is None or valid_time_dt.tzinfo.utcoffset(valid_time_dt) is None:
            valid_time_dt = valid_time_dt.replace(tzinfo=timezone.utc)
        else:
            valid_time_dt = valid_time_dt.astimezone(timezone.utc)

        # Initialize result structure for this time step
        time_key_for_results = effective_lead_h
        day1_results["lead_time_scores"][time_key_for_results] = {}

        # Create parallel task for this time step
        task = asyncio.create_task(
            _process_single_timestep_parallel(
                valid_time_dt=valid_time_dt,
                effective_lead_h=effective_lead_h,
                variables_to_score=variables_to_score,
                miner_forecast_ds=miner_forecast_ds,
                gfs_analysis_data_for_run=gfs_analysis_data_for_run,
                gfs_reference_forecast_for_run=gfs_reference_forecast_for_run,
                era5_climatology=era5_climatology,
                precomputed_climatology_cache=precomputed_climatology_cache,
                day1_scoring_config=day1_scoring_config,
                run_gfs_init_time=run_gfs_init_time,
                miner_hotkey=miner_hotkey,
            ),
            name=f"timestep_{effective_lead_h}h_{miner_hotkey[:8]}",
        )
        timestep_tasks.append((effective_lead_h, task))

    # Execute all time steps in parallel
    logger.debug(
        f"[Day1Score] Miner {miner_hotkey}: Executing {len(timestep_tasks)} time step tasks in parallel..."
    )

    tasks_only = [task for _, task in timestep_tasks]
    timestep_results = await asyncio.gather(*tasks_only, return_exceptions=True)

    parallel_timesteps_time = time.time() - parallel_timesteps_start
    logger.info(
        f"[Day1Score] Miner {miner_hotkey}: PARALLEL time step processing completed in {parallel_timesteps_time:.2f}s"
    )

    # Process results and aggregate scores
    aggregated_skill_scores = []
    aggregated_acc_scores = []

    for i, (effective_lead_h, _) in enumerate(timestep_tasks):
        result = timestep_results[i]
        time_key_for_results = effective_lead_h

        if isinstance(result, Exception):
            logger.error(
                f"[Day1Score] Miner {miner_hotkey}: PARALLEL time step {effective_lead_h}h failed: {result}",
                exc_info=result,
            )
            day1_results["qc_passed_all_vars_leads"] = False
            continue

        if not isinstance(result, dict):
            logger.error(
                f"[Day1Score] Miner {miner_hotkey}: Invalid result type from parallel time step {effective_lead_h}h: {type(result)}"
            )
            continue

        # Handle time step skips or errors
        if result.get("skip_reason"):
            logger.warning(
                f"[Day1Score] Miner {miner_hotkey}: Time step {effective_lead_h}h skipped - {result['skip_reason']}"
            )
            continue

        if result.get("error_message"):
            logger.error(
                f"[Day1Score] Miner {miner_hotkey}: Time step {effective_lead_h}h error - {result['error_message']}"
            )
            day1_results["qc_passed_all_vars_leads"] = False
            continue

        # Process successful time step results
        if not result.get("qc_passed", True):
            day1_results["qc_passed_all_vars_leads"] = False

        # Copy variable results to the main results structure
        for var_key, var_result in result.get("variables", {}).items():
            day1_results["lead_time_scores"][time_key_for_results][var_key] = var_result

        # Add to aggregated scores from this time step
        timestep_skill_scores = result.get("aggregated_skill_scores", [])
        timestep_acc_scores = result.get("aggregated_acc_scores", [])

        aggregated_skill_scores.extend(timestep_skill_scores)
        aggregated_acc_scores.extend(timestep_acc_scores)

        logger.debug(
            f"[Day1Score] Miner {miner_hotkey}: Time step {effective_lead_h}h contributed "
            f"{len(timestep_skill_scores)} skill scores and {len(timestep_acc_scores)} ACC scores"
        )

    # Clean up async task references
    try:
        for _, task in timestep_tasks:
            if not task.done():
                task.cancel()
        del timestep_tasks, tasks_only, timestep_results
        collected = gc.collect()
        logger.debug(
            f"[Day1Score] Miner {miner_hotkey}: Post-processing cleanup collected {collected} objects"
        )
    except Exception as cleanup_err:
        logger.debug(
            f"[Day1Score] Miner {miner_hotkey}: Post-processing cleanup error: {cleanup_err}"
        )

    return aggregated_skill_scores, aggregated_acc_scores


def _calculate_final_day1_score(
    aggregated_skill_scores: List[float],
    aggregated_acc_scores: List[float],
    day1_scoring_config: Dict,
    miner_hotkey: str,
) -> float:
    """Calculate the final Day-1 score from aggregated skill and ACC scores."""
    clipped_skill_scores = [max(0.0, s) for s in aggregated_skill_scores if np.isfinite(s)]
    scaled_acc_scores = [(a + 1.0) / 2.0 for a in aggregated_acc_scores if np.isfinite(a)]

    avg_clipped_skill = np.mean(clipped_skill_scores) if clipped_skill_scores else 0.0
    avg_scaled_acc = np.mean(scaled_acc_scores) if scaled_acc_scores else 0.0

    if not np.isfinite(avg_clipped_skill):
        avg_clipped_skill = 0.0
    if not np.isfinite(avg_scaled_acc):
        avg_scaled_acc = 0.0

    if not aggregated_skill_scores and not aggregated_acc_scores:
        logger.warning(
            f"[Day1Score] Miner {miner_hotkey}: No valid skill or ACC scores to aggregate. Setting overall score to 0."
        )
        return 0.0
    else:
        alpha = day1_scoring_config.get("alpha_skill", 0.5)
        beta = day1_scoring_config.get("beta_acc", 0.5)

        if not np.isclose(alpha + beta, 1.0):
            logger.warning(
                f"[Day1Score] Miner {miner_hotkey}: Alpha ({alpha}) + Beta ({beta}) does not equal 1. "
                f"Score may not be 0-1 bounded as intended."
            )

        normalized_score = alpha * avg_clipped_skill + beta * avg_scaled_acc
        logger.info(
            f"[Day1Score] Miner {miner_hotkey}: AvgClippedSkill={avg_clipped_skill:.3f}, "
            f"AvgScaledACC={avg_scaled_acc:.3f}, Overall Day1 Score={normalized_score:.3f}"
        )
        return normalized_score


def _cleanup_miner_objects(miner_hotkey: str) -> None:
    """Clean up miner-specific objects to prevent memory leaks."""
    try:
        miner_specific_objects = [
            "miner_forecast_ds",
            "miner_forecast_lead",
            "gfs_analysis_lead",
            "gfs_reference_lead",
        ]

        import sys
        frame = sys._getframe(1)  # Get calling frame
        for obj_name in miner_specific_objects:
            if obj_name in frame.f_locals:
                try:
                    obj = frame.f_locals[obj_name]
                    if (
                        hasattr(obj, "close")
                        and obj_name != "gfs_analysis_lead"
                        and obj_name != "gfs_reference_lead"
                    ):
                        # Don't close slices of shared datasets, just del the reference
                        obj.close()
                    del obj
                except Exception:
                    pass

        # Single garbage collection pass for this miner
        collected = gc.collect()
        if collected > 10:  # Only log if significant cleanup occurred
            logger.debug(f"[Day1Score] Cleanup for {miner_hotkey}: collected {collected} objects")

    except Exception as cleanup_err:
        logger.debug(f"[Day1Score] Error in cleanup for {miner_hotkey}: {cleanup_err}")


async def _request_fresh_token(task_instance: "WeatherTask", miner_hotkey: str, job_id: str):
    """Request fresh token from weather logic module."""
    from ..processing.weather_logic import _request_fresh_token as _request_token
    return await _request_token(task_instance, miner_hotkey, job_id)


# Import the parallel processing function (will be moved to parallel.py)
async def _process_single_timestep_parallel(*args, **kwargs):
    """Placeholder - will be moved to parallel.py module."""
    from .parallel import process_single_timestep_parallel
    return await process_single_timestep_parallel(*args, **kwargs)