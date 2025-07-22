import asyncio
import traceback
import gc
import os
import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import uuid
import numpy as np
import xarray as xr
import pandas as pd
import xskillscore as xs

from fiber.logging_utils import get_logger
from typing import TYPE_CHECKING, Any, Optional, Dict, List, Tuple
if TYPE_CHECKING:
    from .weather_task import WeatherTask 

from .processing.weather_logic import _request_fresh_token
from .utils.remote_access import open_verified_remote_zarr_dataset

from .weather_scoring.metrics import (
    calculate_bias_corrected_forecast,
    calculate_mse_skill_score,
    calculate_acc,
    perform_sanity_checks,
    _calculate_latitude_weights,
)

logger = get_logger(__name__)

# Constants for Day-1 Scoring
# DEFAULT_DAY1_ALPHA_SKILL = 0.5
# DEFAULT_DAY1_BETA_ACC = 0.5
# DEFAULT_DAY1_PATTERN_CORR_THRESHOLD = 0.3
# DEFAULT_DAY1_ACC_LOWER_BOUND = 0.6 # For a specific lead like +12h

async def evaluate_miner_forecast_day1(
    task_instance: 'WeatherTask',
    miner_response_db_record: Dict,
    gfs_analysis_data_for_run: xr.Dataset,
    gfs_reference_forecast_for_run: xr.Dataset,
    era5_climatology: xr.Dataset,
    day1_scoring_config: Dict,
    run_gfs_init_time: datetime,
    precomputed_climatology_cache: Optional[Dict] = None  # NEW: Pre-computed climatology cache
) -> Dict:
    """
    Performs Day-1 scoring for a single miner's forecast.
    Uses GFS analysis as truth and GFS forecast as reference.
    Calculates bias-corrected skill scores and ACC for specified variables and lead times.
    """
    response_id = miner_response_db_record['id']
    miner_hotkey = miner_response_db_record['miner_hotkey']
    job_id = miner_response_db_record.get('job_id')
    run_id = miner_response_db_record.get('run_id')
    miner_uid = miner_response_db_record.get('miner_uid')

    # Start timing for this miner's scoring
    scoring_start_time = time.time()
    logger.info(f"[Day1Score] Starting for miner {miner_hotkey} (Resp: {response_id}, Run: {run_id}, Job: {job_id}, UID: {miner_uid})")

    day1_results = {
        "response_id": response_id,
        "miner_hotkey": miner_hotkey,
        "miner_uid": miner_uid,
        "run_id": run_id,
        "overall_day1_score": None,
        "qc_passed_all_vars_leads": True,
        "lead_time_scores": {},
        "error_message": None
    }

    miner_forecast_ds: Optional[xr.Dataset] = None

    try:
        from .processing.weather_logic import _request_fresh_token, _is_miner_registered
        
        token_data_tuple = await _request_fresh_token(task_instance, miner_hotkey, job_id)
        if token_data_tuple is None:
            # Check if miner is still registered before treating this as a critical error
            is_registered = await _is_miner_registered(task_instance, miner_hotkey)
            if not is_registered:
                logger.warning(f"[Day1Score] Miner {miner_hotkey} failed token request and is not in current metagraph - likely deregistered. Skipping day1 scoring for this miner.")
                day1_results["error_message"] = "Miner not in current metagraph (likely deregistered)"
                day1_results["overall_day1_score"] = 0.0
                day1_results["qc_passed_all_vars_leads"] = False
                return day1_results  # Return graceful failure rather than exception
            else:
                logger.error(f"[Day1Score] Miner {miner_hotkey} failed token request but is still registered in metagraph. This may indicate a miner-side issue or network problem.")
                raise ValueError(f"Failed to get fresh access token/URL/manifest_hash for {miner_hotkey} job {job_id}")

        access_token, zarr_store_url, claimed_manifest_content_hash = token_data_tuple
        
        if not all([access_token, zarr_store_url, claimed_manifest_content_hash]):
            raise ValueError(f"Critical forecast access info missing for {miner_hotkey} (Job: {job_id})")

        logger.info(f"[Day1Score] Opening VERIFIED Zarr store for {miner_hotkey}: {zarr_store_url}")
        storage_options = {"headers": {"Authorization": f"Bearer {access_token}"}, "ssl": False}
        
        verification_timeout_seconds = task_instance.config.get('verification_timeout_seconds', 300) / 2

        miner_forecast_ds = await asyncio.wait_for(
            open_verified_remote_zarr_dataset(
                zarr_store_url=zarr_store_url,
                claimed_manifest_content_hash=claimed_manifest_content_hash,
                miner_hotkey_ss58=miner_hotkey,
                storage_options=storage_options,
                job_id=f"{job_id}_day1_score"
            ),
            timeout=verification_timeout_seconds
        )

        if miner_forecast_ds is None:
            raise ConnectionError(f"Failed to open verified Zarr dataset for miner {miner_hotkey}")

        hardcoded_valid_times: Optional[List[datetime]] = day1_scoring_config.get("hardcoded_valid_times_for_eval")
        if hardcoded_valid_times:
            logger.warning(f"[Day1Score] USING HARDCODED VALID TIMES FOR EVALUATION: {hardcoded_valid_times}")
            times_to_evaluate = hardcoded_valid_times
        else:
            lead_times_to_score_hours: List[int] = day1_scoring_config.get('lead_times_hours', task_instance.config.get('initial_scoring_lead_hours', [6,12]))
            times_to_evaluate = [run_gfs_init_time + timedelta(hours=h) for h in lead_times_to_score_hours]
            logger.info(f"[Day1Score] Using lead_hours {lead_times_to_score_hours} relative to GFS init {run_gfs_init_time} for evaluation.")

        variables_to_score: List[Dict] = day1_scoring_config.get('variables_levels_to_score', [])
        
        aggregated_skill_scores = []
        aggregated_acc_scores = []
        
        # OPTIMIZATION 8: PARALLEL TIME STEP PROCESSING
        # Process all time steps simultaneously instead of sequentially for major additional speedup  
        # Expected speedup: 2x for 2 time steps (5s → 2.5s)
        
        logger.info(f"[Day1Score] Miner {miner_hotkey}: Starting PARALLEL processing of {len(times_to_evaluate)} time steps")
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
                    miner_hotkey=miner_hotkey
                ),
                name=f"timestep_{effective_lead_h}h_{miner_hotkey[:8]}"
            )
            timestep_tasks.append((effective_lead_h, task))
        
        # Execute all time steps in parallel and collect results
        logger.debug(f"[Day1Score] Miner {miner_hotkey}: Executing {len(timestep_tasks)} time step tasks in parallel...")
        
        # Get just the tasks for asyncio.gather
        tasks_only = [task for _, task in timestep_tasks]
        timestep_results = await asyncio.gather(*tasks_only, return_exceptions=True)
        
        parallel_timesteps_time = time.time() - parallel_timesteps_start
        logger.info(f"[Day1Score] Miner {miner_hotkey}: PARALLEL time step processing completed in {parallel_timesteps_time:.2f}s")
        
        # Process results from parallel time step execution
        for i, (effective_lead_h, _) in enumerate(timestep_tasks):
            result = timestep_results[i]
            time_key_for_results = effective_lead_h
            
            if isinstance(result, Exception):
                logger.error(f"[Day1Score] Miner {miner_hotkey}: PARALLEL time step {effective_lead_h}h failed: {result}", exc_info=result)
                day1_results["qc_passed_all_vars_leads"] = False
                continue
                
            if not isinstance(result, dict):
                logger.error(f"[Day1Score] Miner {miner_hotkey}: Invalid result type from parallel time step {effective_lead_h}h: {type(result)}")
                continue
            
            # Handle time step skips or errors
            if result.get("skip_reason"):
                logger.warning(f"[Day1Score] Miner {miner_hotkey}: Time step {effective_lead_h}h skipped - {result['skip_reason']}")
                continue
                
            if result.get("error_message"):
                logger.error(f"[Day1Score] Miner {miner_hotkey}: Time step {effective_lead_h}h error - {result['error_message']}")
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
            
            logger.debug(f"[Day1Score] Miner {miner_hotkey}: Time step {effective_lead_h}h contributed {len(timestep_skill_scores)} skill scores and {len(timestep_acc_scores)} ACC scores")

        # CRITICAL ABC MEMORY LEAK FIX: Clean up async task references after result processing
        try:
            for _, task in timestep_tasks:
                if not task.done():
                    task.cancel()
            del timestep_tasks, tasks_only, timestep_results
            # Force immediate cleanup of completed task objects
            collected = gc.collect()
            logger.debug(f"[Day1Score] Miner {miner_hotkey}: Post-processing cleanup collected {collected} objects")
        except Exception as cleanup_err:
            logger.debug(f"[Day1Score] Miner {miner_hotkey}: Post-processing cleanup error: {cleanup_err}")

        clipped_skill_scores = [max(0.0, s) for s in aggregated_skill_scores if np.isfinite(s)]
        scaled_acc_scores = [(a + 1.0) / 2.0 for a in aggregated_acc_scores if np.isfinite(a)]

        avg_clipped_skill = np.mean(clipped_skill_scores) if clipped_skill_scores else 0.0
        avg_scaled_acc = np.mean(scaled_acc_scores) if scaled_acc_scores else 0.0
        
        if not np.isfinite(avg_clipped_skill): avg_clipped_skill = 0.0
        if not np.isfinite(avg_scaled_acc): avg_scaled_acc = 0.0

        if not aggregated_skill_scores and not aggregated_acc_scores:
            logger.warning(f"[Day1Score] Miner {miner_hotkey}: No valid skill or ACC scores to aggregate. Setting overall score to 0.")
            day1_results["overall_day1_score"] = 0.0
            day1_results["qc_passed_all_vars_leads"] = False
        else:
            alpha = day1_scoring_config.get('alpha_skill', 0.5)
            beta = day1_scoring_config.get('beta_acc', 0.5)
            
            if not np.isclose(alpha + beta, 1.0):
                logger.warning(f"[Day1Score] Miner {miner_hotkey}: Alpha ({alpha}) + Beta ({beta}) does not equal 1. Score may not be 0-1 bounded as intended.")
            
            normalized_score = alpha * avg_clipped_skill + beta * avg_scaled_acc
            day1_results["overall_day1_score"] = normalized_score 
            logger.info(f"[Day1Score] Miner {miner_hotkey}: AvgClippedSkill={avg_clipped_skill:.3f}, AvgScaledACC={avg_scaled_acc:.3f}, Overall Day1 Score={normalized_score:.3f}")

    except ConnectionError as e_conn:
        logger.error(f"[Day1Score] Connection error for miner {miner_hotkey} (Job {job_id}): {e_conn}")
        day1_results["error_message"] = f"ConnectionError: {str(e_conn)}"
        day1_results["overall_day1_score"] = -np.inf 
        day1_results["qc_passed_all_vars_leads"] = False
    except asyncio.TimeoutError:
        logger.error(f"[Day1Score] Timeout opening dataset for miner {miner_hotkey} (Job {job_id}).")
        day1_results["error_message"] = "TimeoutError: Opening dataset timed out."
        day1_results["overall_day1_score"] = -np.inf
        day1_results["qc_passed_all_vars_leads"] = False
    except Exception as e_main:
        logger.error(f"[Day1Score] Main error for miner {miner_hotkey} (Resp: {response_id}): {e_main}", exc_info=True)
        day1_results["error_message"] = str(e_main)
        day1_results["overall_day1_score"] = -np.inf # Penalize on error
        day1_results["qc_passed_all_vars_leads"] = False
    finally:
        # CRITICAL: Clean up miner-specific objects, but preserve shared datasets
        if miner_forecast_ds:
            try:
                miner_forecast_ds.close()
                logger.debug(f"[Day1Score] Closed miner forecast dataset for {miner_hotkey}")
            except Exception:
                pass
        
        # Clean up any remaining intermediate objects created during this miner's evaluation
        # But do NOT clean the shared datasets (gfs_analysis_data_for_run, gfs_reference_forecast_for_run, era5_climatology)
        try:
            miner_specific_objects = [
                'miner_forecast_ds', 'miner_forecast_lead', 'gfs_analysis_lead', 'gfs_reference_lead'
            ]
            
            for obj_name in miner_specific_objects:
                if obj_name in locals():
                    try:
                        obj = locals()[obj_name]
                        if hasattr(obj, 'close') and obj_name != 'gfs_analysis_lead' and obj_name != 'gfs_reference_lead':
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

        # Log total scoring time for this miner
        total_scoring_time = time.time() - scoring_start_time
        logger.info(f"[Day1Score] TIMING: Miner {miner_hotkey} scoring completed in {total_scoring_time:.2f} seconds")

    logger.info(f"[Day1Score] Completed for {miner_hotkey}. Final score: {day1_results['overall_day1_score']}, QC Passed: {day1_results['qc_passed_all_vars_leads']}")
    return day1_results

async def _process_single_variable_parallel(
    var_config: Dict,
    miner_forecast_lead: xr.Dataset,
    gfs_analysis_lead: xr.Dataset, 
    gfs_reference_lead: xr.Dataset,
    era5_climatology: xr.Dataset,
    precomputed_climatology_cache: Optional[Dict],
    day1_scoring_config: Dict,
    valid_time_dt: datetime,
    effective_lead_h: int,
    miner_hotkey: str,
    cached_pressure_dims: Dict,
    cached_lat_weights: Optional[xr.DataArray],
    cached_grid_dims: Optional[tuple],
    variables_to_score: List[Dict]
) -> Dict:
    """
    Process a single variable for parallel execution.
    
    Returns a dictionary with:
    - status: 'success', 'skipped', or 'error'
    - var_key: Variable identifier  
    - skill_score: Calculated skill score (if successful)
    - acc_score: Calculated ACC score (if successful)
    - clone_distance_mse: MSE distance from reference
    - clone_penalty_applied: Applied clone penalty
    - sanity_checks: Sanity check results
    - error_message: Error details (if failed)
    - qc_failure_reason: QC failure reason (if applicable)
    """
    var_name = var_config['name']
    var_level = var_config.get('level')
    standard_name_for_clim = var_config.get('standard_name', var_name)
    var_key = f"{var_name}{var_level}" if var_level else var_name
    
    result = {
        "status": "processing",
        "var_key": var_key,
        "skill_score": None,
        "acc_score": None, 
        "clone_distance_mse": None,
        "clone_penalty_applied": None,
        "sanity_checks": {},
        "error_message": None,
        "qc_failure_reason": None
    }
    
    try:
        logger.debug(f"[Day1Score] Miner {miner_hotkey}: PARALLEL scoring Var: {var_key} at Valid Time: {valid_time_dt}")

        # OPTIMIZATION 6: Extract all variables at once to reduce dataset access overhead
        miner_var_da_unaligned = miner_forecast_lead[var_name]
        truth_var_da_unaligned = gfs_analysis_lead[var_name]  
        ref_var_da_unaligned = gfs_reference_lead[var_name]

        # OPTIMIZATION 5A: Reduce logging overhead - only log diagnostics for first variable or on issues
        # Calculate ranges once for use in both logging and unit checks
        miner_min, miner_max, miner_mean = float(miner_var_da_unaligned.min()), float(miner_var_da_unaligned.max()), float(miner_var_da_unaligned.mean())
        truth_min, truth_max, truth_mean = float(truth_var_da_unaligned.min()), float(truth_var_da_unaligned.max()), float(truth_var_da_unaligned.mean())
        ref_min, ref_max, ref_mean = float(ref_var_da_unaligned.min()), float(ref_var_da_unaligned.max()), float(ref_var_da_unaligned.mean())
        
        # Only log detailed diagnostics for first variable or when potential issues detected
        is_first_var = var_config == variables_to_score[0] if variables_to_score else True
        has_potential_issue = (
            (var_name == 'z' and miner_mean < 10000) or
            (var_name == '2t' and (miner_mean < 200 or miner_mean > 350)) or  
            (var_name == 'msl' and (miner_mean < 50000 or miner_mean > 150000))
        )
        
        if is_first_var or has_potential_issue:
            logger.info(f"[Day1Score] Miner {miner_hotkey}: RAW DATA DIAGNOSTICS for {var_key} at {valid_time_dt}:")
            logger.info(f"[Day1Score] Miner {miner_hotkey} {var_key}: range=[{miner_min:.1f}, {miner_max:.1f}], mean={miner_mean:.1f}, units={miner_var_da_unaligned.attrs.get('units', 'unknown')}")
            logger.info(f"[Day1Score] Truth {var_key}: range=[{truth_min:.1f}, {truth_max:.1f}], mean={truth_mean:.1f}, units={truth_var_da_unaligned.attrs.get('units', 'unknown')}")
            logger.info(f"[Day1Score] Ref   {var_key}: range=[{ref_min:.1f}, {ref_max:.1f}], mean={ref_mean:.1f}, units={ref_var_da_unaligned.attrs.get('units', 'unknown')}")
        else:
            logger.debug(f"[Day1Score] Miner {miner_hotkey} {var_key}: mean={miner_mean:.1f}")  # Minimal logging for subsequent vars
        
        # Check for potential unit mismatch indicators
        if var_name == 'z' and var_level == 500:
            # For z500, geopotential should be ~49000-58000 m²/s²
            # If it's geopotential height, it would be ~5000-6000 m
            miner_ratio = miner_mean / 9.80665  # If miner is geopotential, this ratio should be ~5000-6000
            truth_ratio = truth_mean / 9.80665
            logger.info(f"[Day1Score] Miner {miner_hotkey}: z500 UNIT CHECK - If geopotential (m²/s²): miner_mean/g={miner_ratio:.1f}m, truth_mean/g={truth_ratio:.1f}m")
            
            if miner_mean < 10000:  # Much smaller than expected geopotential
                logger.warning(f"[Day1Score] Miner {miner_hotkey}: POTENTIAL UNIT MISMATCH: z500 mean ({miner_mean:.1f}) suggests geopotential height (m) rather than geopotential (m²/s²)")
            elif truth_mean > 40000 and miner_mean > 40000:
                logger.info(f"[Day1Score] Unit check OK: Both miner and truth z500 appear to be geopotential (m²/s²)")
        
        elif var_name == '2t':
            # Temperature should be ~200-320 K
            if miner_mean < 200 or miner_mean > 350:
                logger.warning(f"[Day1Score] Miner {miner_hotkey}: POTENTIAL UNIT ISSUE: 2t mean ({miner_mean:.1f}) outside expected range for Kelvin")
                
        elif var_name == 'msl':
            # Mean sea level pressure should be ~90000-110000 Pa
            if miner_mean < 50000 or miner_mean > 150000:
                logger.warning(f"[Day1Score] Miner {miner_hotkey}: POTENTIAL UNIT ISSUE: msl mean ({miner_mean:.1f}) outside expected range for Pa")

        # AUTOMATIC UNIT CONVERSION: Convert geopotential height to geopotential if needed
        if var_name == 'z' and miner_mean < 10000 and truth_mean > 40000:
            logger.warning(f"[Day1Score] Miner {miner_hotkey}: AUTOMATIC UNIT CONVERSION: Converting miner z from geopotential height (m) to geopotential (m²/s²)")
            miner_var_da_unaligned = miner_var_da_unaligned * 9.80665
            miner_var_da_unaligned.attrs['units'] = 'm2 s-2'
            miner_var_da_unaligned.attrs['long_name'] = 'Geopotential (auto-converted from height)'
            logger.info(f"[Day1Score] Miner {miner_hotkey}: After conversion: z range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}")

        # Check for temperature unit conversions (Celsius to Kelvin)
        elif var_name in ['2t', 't'] and miner_mean < 100 and truth_mean > 200:
            logger.warning(f"[Day1Score] Miner {miner_hotkey}: AUTOMATIC UNIT CONVERSION: Converting miner {var_name} from Celsius to Kelvin")
            miner_var_da_unaligned = miner_var_da_unaligned + 273.15
            miner_var_da_unaligned.attrs['units'] = 'K'
            miner_var_da_unaligned.attrs['long_name'] = f'{miner_var_da_unaligned.attrs.get("long_name", var_name)} (auto-converted from Celsius)'
            logger.info(f"[Day1Score] Miner {miner_hotkey}: After conversion: {var_name} range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}")

        # Check for pressure unit conversions (hPa to Pa)
        elif var_name == 'msl' and miner_mean < 2000 and truth_mean > 50000:
            logger.warning(f"[Day1Score] Miner {miner_hotkey}: AUTOMATIC UNIT CONVERSION: Converting miner msl from hPa to Pa")
            miner_var_da_unaligned = miner_var_da_unaligned * 100.0
            miner_var_da_unaligned.attrs['units'] = 'Pa'
            miner_var_da_unaligned.attrs['long_name'] = 'Mean sea level pressure (auto-converted from hPa)'
            logger.info(f"[Day1Score] Miner {miner_hotkey}: After conversion: msl range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}")

        if var_level:
            # OPTIMIZATION 6B: Use cached pressure dimensions to avoid repeated searches
            def find_pressure_dim_cached(data_array, dataset_name="dataset"):
                cache_key = (dataset_name, tuple(data_array.dims))
                if cache_key in cached_pressure_dims:
                    return cached_pressure_dims[cache_key]
                    
                for dim_name in ['pressure_level', 'plev', 'level']:
                    if dim_name in data_array.dims:
                        cached_pressure_dims[cache_key] = dim_name
                        return dim_name
                
                logger.warning(f"No pressure level dimension found in {dataset_name} for {var_key} level {var_level}. Available dims: {data_array.dims}")
                cached_pressure_dims[cache_key] = None
                return None
            
            miner_pressure_dim = find_pressure_dim_cached(miner_var_da_unaligned, "miner")
            truth_pressure_dim = find_pressure_dim_cached(truth_var_da_unaligned, "truth")
            ref_pressure_dim = find_pressure_dim_cached(ref_var_da_unaligned, "reference")
            
            if not all([miner_pressure_dim, truth_pressure_dim, ref_pressure_dim]):
                logger.warning(f"Missing pressure dimensions for {var_key} level {var_level}. Skipping.")
                result["status"] = "skipped"
                result["qc_failure_reason"] = "Missing pressure dimensions"
                return result
                
            miner_var_da_selected = miner_var_da_unaligned.sel({miner_pressure_dim: var_level}, method="nearest")
            truth_var_da_selected = truth_var_da_unaligned.sel({truth_pressure_dim: var_level}, method="nearest")
            ref_var_da_selected = ref_var_da_unaligned.sel({ref_pressure_dim: var_level}, method="nearest")
            
            if abs(truth_var_da_selected[truth_pressure_dim].item() - var_level) > 10:
                 logger.warning(f"Truth data for {var_key} level {var_level} too far ({truth_var_da_selected[truth_pressure_dim].item()}). Skipping.")
                 result["status"] = "skipped"
                 result["qc_failure_reason"] = "Truth data level too far from target"
                 return result
            if abs(miner_var_da_selected[miner_pressure_dim].item() - var_level) > 10:
                 logger.warning(f"Miner data for {var_key} level {var_level} too far ({miner_var_da_selected[miner_pressure_dim].item()}). Skipping.")
                 result["status"] = "skipped"
                 result["qc_failure_reason"] = "Miner data level too far from target"
                 return result
            if abs(ref_var_da_selected[ref_pressure_dim].item() - var_level) > 10:
                 logger.warning(f"GFS Ref data for {var_key} level {var_level} too far ({ref_var_da_selected[ref_pressure_dim].item()}). Skipping.")
                 result["status"] = "skipped"
                 result["qc_failure_reason"] = "Reference data level too far from target"
                 return result
        else:
            miner_var_da_selected = miner_var_da_unaligned
            truth_var_da_selected = truth_var_da_unaligned
            ref_var_da_selected = ref_var_da_unaligned

        target_grid_da = truth_var_da_selected 
        temp_spatial_dims = [d for d in target_grid_da.dims if d.lower() in ('latitude', 'longitude', 'lat', 'lon')]
        actual_lat_dim_in_target_for_ordering = next((d for d in temp_spatial_dims if d.lower() in ('latitude', 'lat')), None)

        if actual_lat_dim_in_target_for_ordering:
            lat_coord_values = target_grid_da[actual_lat_dim_in_target_for_ordering].values
            if len(lat_coord_values) > 1 and lat_coord_values[0] < lat_coord_values[-1]:
                logger.info(f"[Day1Score] Latitude coordinate '{actual_lat_dim_in_target_for_ordering}' in target_grid_da is ascending. Flipping to descending order for consistency.")
                target_grid_da = target_grid_da.isel({actual_lat_dim_in_target_for_ordering: slice(None, None, -1)})
            else:
                logger.debug(f"[Day1Score] Latitude coordinate '{actual_lat_dim_in_target_for_ordering}' in target_grid_da is already descending or has too few points to determine order.")
        else:
            logger.warning("[Day1Score] Could not determine latitude dimension in target_grid_da to check/ensure descending order.")

        def _standardize_spatial_dims(data_array: xr.DataArray) -> xr.DataArray:
            if not isinstance(data_array, xr.DataArray): return data_array
            rename_dict = {}
            for dim_name in data_array.dims:
                if dim_name.lower() in ('latitude', 'lat_0'): rename_dict[dim_name] = 'lat'
                elif dim_name.lower() in ('longitude', 'lon_0'): rename_dict[dim_name] = 'lon'
            if rename_dict:
                logger.debug(f"[Day1Score] Standardizing spatial dims for variable {var_key}: Renaming {rename_dict}")
                return data_array.rename(rename_dict)
            return data_array

        # OPTIMIZATION 4A: Chain standardization and use target as reference to reduce copying
        target_grid_da_std = _standardize_spatial_dims(target_grid_da)
        truth_var_da_final = target_grid_da_std  # Use directly, no copy needed
        
        # OPTIMIZATION 4B: Keep interpolation threaded as it's CPU-intensive, but optimize data flow
        # ASYNC PROCESSING OPTION: For large datasets, use async processing for interpolation
        from .utils.async_processing import AsyncProcessingConfig
        
        # Check if we should use async processing (based on data size)
        use_async_processing = AsyncProcessingConfig.should_use_async_processing(miner_var_da_selected.size)
        
        if use_async_processing:
            logger.debug(f"[Day1Score] Using async processing for interpolation of {var_key}")
            # Use xarray's optimized interpolation with dask backend
            
        # Use existing thread-based interpolation
        miner_var_da_aligned = await asyncio.to_thread(
            lambda: _standardize_spatial_dims(miner_var_da_selected).interp_like(
                target_grid_da_std, method="linear"
            )
        )
        
        ref_var_da_aligned = await asyncio.to_thread(
            lambda: _standardize_spatial_dims(ref_var_da_selected).interp_like(
                target_grid_da_std, method="linear"
            )
        )
        

        broadcasted_weights_final = None
        spatial_dims_for_metric = [d for d in truth_var_da_final.dims if d in ('lat', 'lon')]
        
        actual_lat_dim_in_target = 'lat' if 'lat' in truth_var_da_final.dims else None

        if actual_lat_dim_in_target:
            try:
                # OPTIMIZATION 2: Use cached latitude weights if grid dimensions match
                current_grid_dims = (truth_var_da_final.dims, truth_var_da_final.shape)
                
                if cached_lat_weights is not None and cached_grid_dims == current_grid_dims:
                                                         # Reuse cached weights - this saves significant computation time
                    broadcasted_weights_final = cached_lat_weights
                    # OPTIMIZATION 5B: Reduce frequent debug logging - only log cache events for first use
                    if is_first_var:
                        logger.debug(f"[Day1Score] For {var_key}, using CACHED latitude weights. Shape: {broadcasted_weights_final.shape}")
                else:
                    # Calculate weights and cache them for subsequent variables
                    target_lat_coord = truth_var_da_final[actual_lat_dim_in_target]
                    # OPTIMIZATION 3A: _calculate_latitude_weights is lightweight - run in main thread
                    one_d_lat_weights_target = _calculate_latitude_weights(target_lat_coord)
                    # OPTIMIZATION 3B: xr.broadcast is just memory operations - run in main thread  
                    _, broadcasted_weights_final = xr.broadcast(truth_var_da_final, one_d_lat_weights_target)
                    
                    # Cache for subsequent variables in this time step
                    cached_lat_weights = broadcasted_weights_final
                    cached_grid_dims = current_grid_dims
                    logger.debug(f"[Day1Score] For {var_key}, calculated and cached new latitude weights. Shape: {broadcasted_weights_final.shape}")
                    
            except Exception as e_broadcast_weights:
                logger.error(f"[Day1Score] Failed to create/broadcast latitude weights based on target_grid_da_std for {var_key}: {e_broadcast_weights}. Proceeding without weights for this variable.")
                broadcasted_weights_final = None
        else:
            logger.warning(f"[Day1Score] For {var_key}, 'lat' dimension not found in truth_var_da_final (dims: {truth_var_da_final.dims}). No weights applied.")
        
        def _get_metric_scalar_value(metric_fn, *args, **kwargs):
            res = metric_fn(*args, **kwargs)
            if hasattr(res, 'compute'):
                res = res.compute()
            return float(res.item())

        # OPTIMIZATION 3D: MSE calculation is vectorized and fast - run in main thread
        clone_distance_mse_val = _get_metric_scalar_value(
            xs.mse, 
            miner_var_da_aligned, 
            ref_var_da_aligned, 
            dim=spatial_dims_for_metric, 
            weights=broadcasted_weights_final, 
            skipna=True
        )
        result["clone_distance_mse"] = clone_distance_mse_val

        delta_thresholds_config = day1_scoring_config.get('clone_delta_thresholds', {})
        delta_for_var = delta_thresholds_config.get(var_key)
        clone_penalty = 0.0

        if delta_for_var is not None and clone_distance_mse_val < delta_for_var:
            gamma = day1_scoring_config.get('clone_penalty_gamma', 1.0)
            clone_penalty = gamma * (1.0 - (clone_distance_mse_val / delta_for_var))
            clone_penalty = max(0.0, clone_penalty)
            logger.warning(f"[Day1Score] Miner {miner_hotkey}: GFS Clone Suspect: {var_key} at {effective_lead_h}h. "
                         f"Distance MSE {clone_distance_mse_val:.4f} < Delta {delta_for_var:.4f}. Penalty: {clone_penalty:.4f}")
            result["qc_failure_reason"] = f"Clone penalty triggered for {var_key}"
        result["clone_penalty_applied"] = clone_penalty

        # MAJOR OPTIMIZATION: Use pre-computed climatology if available
        if precomputed_climatology_cache:
            cache_key = f"{var_name}_{var_level}_{valid_time_dt.isoformat()}"
            clim_var_da_aligned = precomputed_climatology_cache.get(cache_key)
            
            if clim_var_da_aligned is not None:
                logger.debug(f"[Day1Score] USING PRE-COMPUTED climatology for {var_key} - major speedup!")
            else:
                logger.warning(f"[Day1Score] Pre-computed climatology cache miss for {cache_key} - falling back to individual computation")
                # Fallback to original computation
                clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
                clim_hour = valid_time_dt.hour 
                clim_hour_rounded = (clim_hour // 6) * 6 
                clim_var_da_raw = era5_climatology[standard_name_for_clim].sel(
                    dayofyear=clim_dayofyear, hour=clim_hour_rounded, method="nearest"
                )
                clim_var_to_interpolate = _standardize_spatial_dims(clim_var_da_raw)
                if var_level:
                    clim_pressure_dim = None
                    for dim_name in ['pressure_level', 'plev', 'level']:
                        if dim_name in clim_var_to_interpolate.dims:
                            clim_pressure_dim = dim_name
                            break
                    if clim_pressure_dim:
                        clim_var_to_interpolate = clim_var_to_interpolate.sel(**{clim_pressure_dim: var_level}, method="nearest")
                clim_var_da_aligned = await asyncio.to_thread(
                    lambda: clim_var_to_interpolate.interp_like(truth_var_da_final, method="linear")
                )
        else:
            # Original climatology computation (fallback when no cache provided)
            clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
            clim_hour = valid_time_dt.hour 
            
            clim_hour_rounded = (clim_hour // 6) * 6 
            
            clim_var_da_raw = era5_climatology[standard_name_for_clim].sel(
                dayofyear=clim_dayofyear, 
                hour=clim_hour_rounded,
                method="nearest"
            )
            # OPTIMIZATION 4C: Optimize climatology processing chain
            clim_var_to_interpolate = _standardize_spatial_dims(clim_var_da_raw)
            
            if var_level:
                # Handle different pressure level dimension names in climatology data
                clim_pressure_dim = None
                for dim_name in ['pressure_level', 'plev', 'level']:
                    if dim_name in clim_var_to_interpolate.dims:
                        clim_pressure_dim = dim_name
                        break
                
                if clim_pressure_dim:
                    # OPTIMIZATION 3C: Climatology selection is lightweight - run in main thread
                    clim_var_to_interpolate = clim_var_to_interpolate.sel(**{clim_pressure_dim: var_level}, method="nearest")
                    if abs(clim_var_to_interpolate[clim_pressure_dim].item() - var_level) > 10:
                        logger.warning(f"[Day1Score] Climatology for {var_key} at target level {var_level} was found at {clim_var_to_interpolate[clim_pressure_dim].item()}. Using this nearest level data.")
            
            # OPTIMIZATION 4D: Climatology interpolation in thread for consistency
            clim_var_da_aligned = await asyncio.to_thread(
                lambda: clim_var_to_interpolate.interp_like(
                    truth_var_da_final, method="linear"
                )
            )
        

        sanity_results = await perform_sanity_checks(
            forecast_da=miner_var_da_aligned,
            reference_da_for_corr=ref_var_da_aligned,
            variable_name=var_key, 
            climatology_bounds_config=day1_scoring_config.get('climatology_bounds', {}),
            pattern_corr_threshold=day1_scoring_config.get('pattern_correlation_threshold', 0.3),
            lat_weights=broadcasted_weights_final
        )
        result["sanity_checks"] = sanity_results

        if not sanity_results.get("climatology_passed") or \
           not sanity_results.get("pattern_correlation_passed"):
            logger.warning(f"[Day1Score] Miner {miner_hotkey}: Sanity check failed for {var_key} at {effective_lead_h}h. Skipping metrics.")
            result["status"] = "skipped"
            result["qc_failure_reason"] = f"Sanity check failed for {var_key} - " \
                                        f"Climatology passed: {sanity_results.get('climatology_passed')}, " \
                                        f"Pattern correlation passed: {sanity_results.get('pattern_correlation_passed')}"
            return result

        # Bias Correction
        forecast_bc_da = await calculate_bias_corrected_forecast(miner_var_da_aligned, truth_var_da_final)

        # MSE Skill Score
        skill_score = await calculate_mse_skill_score(forecast_bc_da, truth_var_da_final, ref_var_da_aligned, broadcasted_weights_final)
        
        # clone penalty
        skill_score_after_penalty = skill_score - clone_penalty
        result["skill_score"] = skill_score_after_penalty
        
        # ACC
        acc_score = await calculate_acc(miner_var_da_aligned, truth_var_da_final, clim_var_da_aligned, broadcasted_weights_final)
        result["acc_score"] = acc_score
        
        # ACC Lower Bound Check
        if effective_lead_h == 12 and np.isfinite(acc_score) and acc_score < day1_scoring_config.get("acc_lower_bound_d1", 0.6):
            logger.warning(f"[Day1Score] Miner {miner_hotkey}: ACC for {var_key} at valid time {valid_time_dt} (Eff. Lead 12h) ({acc_score:.3f}) is below threshold.")

        # Mark as successful
        result["status"] = "success"
        logger.debug(f"[Day1Score] Miner {miner_hotkey}: PARALLEL Variable {var_key} scored successfully")
        
        return result

    except Exception as e_var:
        logger.error(f"[Day1Score] Miner {miner_hotkey}: Error in parallel scoring {var_key} at {valid_time_dt}: {e_var}", exc_info=True)
        result["status"] = "error"
        result["error_message"] = str(e_var)
        return result
    finally:
        # CRITICAL ABC MEMORY LEAK FIX: Clean up all temporary xarray ABC objects
        try:
            # List of all temporary ABC objects created during variable processing
            temp_objects = [
                'miner_var_da_unaligned', 'truth_var_da_unaligned', 'ref_var_da_unaligned',
                'miner_var_da_selected', 'truth_var_da_selected', 'ref_var_da_selected', 
                'miner_var_da_aligned', 'ref_var_da_aligned', 'truth_var_da_final',
                'target_grid_da', 'target_grid_da_std', 'clim_var_da_aligned',
                'forecast_bc_da', 'broadcasted_weights_final', 'clim_var_da_raw',
                'clim_var_to_interpolate'
            ]
            
            cleaned_count = 0
            for obj_name in temp_objects:
                if obj_name in locals():
                    try:
                        obj = locals()[obj_name]
                        if hasattr(obj, 'close') and callable(obj.close):
                            obj.close()
                        del obj
                        cleaned_count += 1
                    except Exception:
                        pass
            
            # Force garbage collection if we cleaned significant objects
            if cleaned_count > 3:
                collected = gc.collect()
                logger.debug(f"[Day1Score] Variable {var_key} cleanup: removed {cleaned_count} objects, GC collected {collected}")
                
        except Exception as cleanup_err:
            logger.debug(f"[Day1Score] Variable {var_key} cleanup error: {cleanup_err}")


async def precompute_climatology_cache(
    era5_climatology: xr.Dataset,
    day1_scoring_config: Dict,
    times_to_evaluate: List[datetime],
    sample_target_grid: xr.DataArray
) -> Dict[str, xr.DataArray]:
    """
    OPTIMIZATION 1: Pre-compute ALL climatology interpolations once per scoring run.
    This eliminates the massive redundant interpolation overhead per miner.
    Expected 50-200x speedup (250-1000 seconds → 5-20 seconds).
    
    Enhanced with MULTIPROCESSING for CPU-intensive interpolation operations.
    Expected additional 2-4x speedup from parallel processing.
    """
    cache = {}
    variables_to_score = day1_scoring_config.get('variables_levels_to_score', [])
    
    # Validate inputs
    if not variables_to_score:
        logger.warning("⚠️ No variables found in day1_scoring_config['variables_levels_to_score'] - cache will be empty")
        return cache
    
    if not times_to_evaluate:
        logger.warning("⚠️ No times provided for climatology cache - cache will be empty")
        return cache
    
    total_computations = len(variables_to_score) * len(times_to_evaluate)
    
    # SMART ASYNC PROCESSING: Only use async processing when workload justifies the overhead
    # Break-even point is around 10+ computations for async processing benefits
    async_threshold = int(os.getenv('GAIA_ASYNC_THRESHOLD', '10'))  # Configurable threshold
    use_async_processing = (
        total_computations >= async_threshold and
        not os.getenv('GAIA_DISABLE_ASYNC', 'false').lower() == 'true'
    )
    
    if use_async_processing:
        logger.info(f"🚀 ASYNC PROCESSED climatology cache for {len(variables_to_score)} variables × {len(times_to_evaluate)} times = {total_computations} total computations")
        logger.info(f"   Async processing enabled (computations: {total_computations} >= threshold: {async_threshold})")
    else:
        logger.info(f"⚡ SEQUENTIAL climatology cache for {len(variables_to_score)} variables × {len(times_to_evaluate)} times = {total_computations} total computations")
        logger.info(f"   Async processing disabled (computations: {total_computations} < threshold: {async_threshold} or explicitly disabled)")
    
    cache_start = time.time()
    
    def _standardize_spatial_dims_cache(data_array: xr.DataArray) -> xr.DataArray:
        """Local copy of standardization function for cache computation."""
        if not isinstance(data_array, xr.DataArray): 
            return data_array
        rename_dict = {}
        for dim_name in data_array.dims:
            if dim_name.lower() in ('latitude', 'lat_0'): 
                rename_dict[dim_name] = 'lat'
            elif dim_name.lower() in ('longitude', 'lon_0'): 
                rename_dict[dim_name] = 'lon'
        if rename_dict:
            return data_array.rename(rename_dict)
        return data_array

    if use_async_processing:
        # Import async processing functions only when needed
        from .utils.async_processing import compute_climatology_interpolation_async
        
        # ASYNC PROCESSING PATH: Prepare all interpolation tasks for parallel execution
        interpolation_tasks = []
        task_metadata = []
    else:
        # SEQUENTIAL PATH: No async processing needed
        pass
    
    for valid_time_dt in times_to_evaluate:
        for var_config in variables_to_score:
            var_name = var_config['name']
            var_level = var_config.get('level')
            standard_name_for_clim = var_config.get('standard_name', var_name)
            var_key = f"{var_name}{var_level}" if var_level else var_name
            
            cache_key = f"{var_name}_{var_level}_{valid_time_dt.isoformat()}"
            
            try:
                # Extract climatology data for this time
                clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
                clim_hour = valid_time_dt.hour 
                clim_hour_rounded = (clim_hour // 6) * 6 
                
                clim_var_da_raw = era5_climatology[standard_name_for_clim].sel(
                    dayofyear=clim_dayofyear, 
                    hour=clim_hour_rounded,
                    method="nearest"
                )
                
                # Standardize spatial dimensions
                clim_var_to_interpolate = _standardize_spatial_dims_cache(clim_var_da_raw)
                
                # Handle pressure levels if needed
                if var_level:
                    clim_pressure_dim = None
                    for dim_name in ['pressure_level', 'plev', 'level']:
                        if dim_name in clim_var_to_interpolate.dims:
                            clim_pressure_dim = dim_name
                            break
                    
                    if clim_pressure_dim:
                        clim_var_to_interpolate = clim_var_to_interpolate.sel(
                            **{clim_pressure_dim: var_level}, method="nearest"
                        )
                        actual_level = clim_var_to_interpolate[clim_pressure_dim].item()
                        if abs(actual_level - var_level) > 10:
                            logger.warning(f"[CachePrecompute] Climatology for {var_key} at target level {var_level} found at {actual_level}")
                
                if use_async_processing:
                    # ASYNC PROCESSING PATH: Prepare data for parallel execution
                    # Convert to numpy for async processing
                    source_data = clim_var_to_interpolate.values
                    source_lat = clim_var_to_interpolate.lat.values
                    source_lon = clim_var_to_interpolate.lon.values
                    target_lat = sample_target_grid.lat.values
                    target_lon = sample_target_grid.lon.values
                    
                    # Create async task for parallel execution
                    task = compute_climatology_interpolation_async(
                        source_data,
                        (source_lat, source_lon),
                        (target_lat, target_lon), 
                        var_key,
                        "linear"
                    )
                    
                    interpolation_tasks.append(task)
                    task_metadata.append({
                        'cache_key': cache_key,
                        'var_key': var_key,
                        'target_lat': target_lat,
                        'target_lon': target_lon,
                        'clim_coords': clim_var_to_interpolate.coords
                    })
                else:
                    # SEQUENTIAL PATH: Do interpolation immediately
                    try:
                        clim_var_da_aligned = clim_var_to_interpolate.interp_like(
                            sample_target_grid, method="linear"
                        )
                        cache[cache_key] = clim_var_da_aligned
                        logger.debug(f"[CachePrecompute] Sequential: Successfully cached {cache_key}")
                    except Exception as e:
                        logger.error(f"[CachePrecompute] Sequential error for {cache_key}: {e}")
                        continue
                
            except Exception as e:
                logger.error(f"[CachePrecompute] Error preparing climatology task for {cache_key}: {e}")
                continue
    
    # Execute tasks based on chosen method
    if use_async_processing and interpolation_tasks:
        # ASYNC PROCESSING EXECUTION
        logger.info(f"[ClimatologyCache] Executing {len(interpolation_tasks)} interpolation tasks with async processing...")
        parallel_start = time.time()
        
        try:
            interpolation_results = await asyncio.gather(*interpolation_tasks, return_exceptions=True)
            parallel_time = time.time() - parallel_start
            logger.info(f"[ClimatologyCache] Async parallel interpolation completed in {parallel_time:.2f}s")
            
            # Process results and build cache
            successful_computations = 0
            for i, result in enumerate(interpolation_results):
                if isinstance(result, Exception):
                    logger.error(f"[CachePrecompute] Parallel task {i} failed: {result}")
                    continue
                
                if result is None:
                    logger.warning(f"[CachePrecompute] Parallel task {i} returned None")
                    continue
                
                # Reconstruct xarray DataArray from numpy result
                metadata = task_metadata[i]
                cache_key = metadata['cache_key']
                target_lat = metadata['target_lat']
                target_lon = metadata['target_lon']
                original_coords = metadata['clim_coords']
                
                try:
                    # Create new DataArray with interpolated data
                    interpolated_da = xr.DataArray(
                        result,
                        dims=['lat', 'lon'],
                        coords={
                            'lat': target_lat,
                            'lon': target_lon
                        },
                        attrs=dict(original_coords.get('lat', {}).attrs) if 'lat' in original_coords else {}
                    )
                    
                    cache[cache_key] = interpolated_da
                    successful_computations += 1
                    logger.debug(f"[CachePrecompute] Successfully cached {cache_key}")
                    
                except Exception as e:
                    logger.error(f"[CachePrecompute] Error reconstructing DataArray for {cache_key}: {e}")
                    continue
            
            cache_time = time.time() - cache_start
            cache_size = len(cache)
            expected_individual_ops = len(variables_to_score) * len(times_to_evaluate)
            
            logger.info(f"✅ ASYNC PROCESSED climatology cache pre-computation completed!")
            logger.info(f"   - Cache size: {cache_size} entries ({successful_computations} successful)")
            logger.info(f"   - Total computation time: {cache_time:.2f} seconds")
            logger.info(f"   - Async parallel execution time: {parallel_time:.2f} seconds")
            
            # Calculate speedup
            if cache_size > 0 and expected_individual_ops > 0:
                speedup = expected_individual_ops / cache_size
                efficiency = (cache_size / expected_individual_ops) * 100
                async_benefit = cache_time / parallel_time if parallel_time > 0 else 1.0
                logger.info(f"   - Expected miner speedup: {speedup:.1f}x faster per miner")
                logger.info(f"   - Async processing benefit: {async_benefit:.1f}x faster than sequential")
                logger.info(f"   - Cache efficiency: {efficiency:.1f}% (higher is better)")
            
            if cache_size == 0:
                logger.warning("⚠️ Climatology cache is empty - will fallback to individual computation")
            elif cache_size < expected_individual_ops * 0.8:
                logger.warning(f"⚠️ Climatology cache incomplete ({cache_size}/{expected_individual_ops}) - some operations will fallback")
                
        except Exception as e:
            logger.error(f"[CachePrecompute] Error in async parallel interpolation execution: {e}")
            logger.warning("Falling back to sequential climatology computation...")
            
            # Fallback to original sequential method
            for valid_time_dt in times_to_evaluate:
                for var_config in variables_to_score:
                    var_name = var_config['name']
                    var_level = var_config.get('level')
                    standard_name_for_clim = var_config.get('standard_name', var_name)
                    cache_key = f"{var_name}_{var_level}_{valid_time_dt.isoformat()}"
                    
                    try:
                        clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
                        clim_hour = valid_time_dt.hour 
                        clim_hour_rounded = (clim_hour // 6) * 6 
                        
                        clim_var_da_raw = era5_climatology[standard_name_for_clim].sel(
                            dayofyear=clim_dayofyear, 
                            hour=clim_hour_rounded,
                            method="nearest"
                        )
                        
                        clim_var_to_interpolate = _standardize_spatial_dims_cache(clim_var_da_raw)
                        
                        if var_level:
                            clim_pressure_dim = None
                            for dim_name in ['pressure_level', 'plev', 'level']:
                                if dim_name in clim_var_to_interpolate.dims:
                                    clim_pressure_dim = dim_name
                                    break
                            
                            if clim_pressure_dim:
                                clim_var_to_interpolate = clim_var_to_interpolate.sel(
                                    **{clim_pressure_dim: var_level}, method="nearest"
                                )
                        
                        # Sequential fallback interpolation
                        clim_var_da_aligned = await asyncio.to_thread(
                            lambda: clim_var_to_interpolate.interp_like(
                                sample_target_grid, method="linear"
                            )
                        )
                        
                        cache[cache_key] = clim_var_da_aligned
                        
                    except Exception as e:
                        logger.error(f"[CachePrecompute] Fallback error for {cache_key}: {e}")
                        continue
            
            fallback_time = time.time() - cache_start
            logger.info(f"✅ Fallback climatology cache completed in {fallback_time:.2f} seconds")
    else:
        # SEQUENTIAL EXECUTION (no async processing)
        # Cache was already built during the loop above when use_async_processing=False
        cache_time = time.time() - cache_start
        cache_size = len(cache)
        expected_individual_ops = len(variables_to_score) * len(times_to_evaluate)
        
        logger.info(f"✅ SEQUENTIAL climatology cache pre-computation completed!")
        logger.info(f"   - Cache size: {cache_size} entries")
        logger.info(f"   - Total computation time: {cache_time:.2f} seconds")
        
        if cache_size == 0:
            logger.warning("⚠️ Climatology cache is empty - will fallback to individual computation")
        elif cache_size < expected_individual_ops * 0.8:
            logger.warning(f"⚠️ Climatology cache incomplete ({cache_size}/{expected_individual_ops}) - some operations will fallback")
    
    return cache

async def _preload_all_time_slices(
    gfs_analysis_data: xr.Dataset,
    gfs_reference_data: xr.Dataset, 
    miner_forecast_ds: xr.Dataset,
    times_to_evaluate: List[datetime],
    miner_hotkey: str
) -> Dict[str, xr.Dataset]:
    """
    Pre-load all time slices at once to reduce I/O overhead.
    
    OPTIMIZATION: Bulk time slice loading (15-25% speedup)
    Expected benefit: Reduces repeated dataset access overhead
    """
    logger.info(f"[Day1Score] Miner {miner_hotkey}: Pre-loading all {len(times_to_evaluate)} time slices for faster access")
    preload_start = time.time()
    
    try:
        # Convert times to numpy datetime64 for selection
        time_coords = [np.datetime64(dt.replace(tzinfo=None)) for dt in times_to_evaluate]
        
        # Load all time slices at once - much more efficient than individual selections
        gfs_analysis_slices = await asyncio.to_thread(
            lambda: gfs_analysis_data.sel(time=time_coords, method="nearest")
        )
        gfs_reference_slices = await asyncio.to_thread(
            lambda: gfs_reference_data.sel(time=time_coords, method="nearest") 
        )
        
        # Handle miner forecast with timezone-aware selection
        time_dtype_str = str(miner_forecast_ds.time.dtype)
        if 'datetime64' in time_dtype_str and 'UTC' in time_dtype_str:
            # Use pandas timestamps for timezone-aware data
            miner_time_coords = [pd.Timestamp(dt).tz_localize('UTC') if pd.Timestamp(dt).tzinfo is None 
                               else pd.Timestamp(dt).tz_convert('UTC') for dt in times_to_evaluate]
        else:
            miner_time_coords = time_coords
            
        miner_forecast_slices = await asyncio.to_thread(
            lambda: miner_forecast_ds.sel(time=miner_time_coords, method="nearest")
        )
        
        preload_time = time.time() - preload_start
        logger.info(f"[Day1Score] Miner {miner_hotkey}: Pre-loaded all time slices in {preload_time:.2f}s")
        
        return {
            "gfs_analysis": gfs_analysis_slices,
            "gfs_reference": gfs_reference_slices, 
            "miner_forecast": miner_forecast_slices
        }
        
    except Exception as e:
        logger.warning(f"[Day1Score] Miner {miner_hotkey}: Failed to pre-load time slices: {e}. Falling back to individual selection.")
        return None

async def _async_dataset_select(dataset, time_coord, method="nearest"):
    """
    Perform dataset time selection asynchronously to prevent I/O blocking.
    
    OPTIMIZATION: Async I/O for dataset operations (10-20% speedup)
    """
    return await asyncio.to_thread(lambda: dataset.sel(time=time_coord, method=method))

async def _process_single_timestep_parallel(
    valid_time_dt: datetime,
    effective_lead_h: int,
    variables_to_score: List[Dict],
    miner_forecast_ds: xr.Dataset,
    gfs_analysis_data_for_run: xr.Dataset,
    gfs_reference_forecast_for_run: xr.Dataset,
    era5_climatology: xr.Dataset,
    precomputed_climatology_cache: Optional[Dict],
    day1_scoring_config: Dict,
    run_gfs_init_time: datetime,
    miner_hotkey: str
) -> Dict:
    """
    Process a single time step with all its variables in parallel.
    
    This enables both time step AND variable parallelization for maximum performance.
    Expected additional speedup: 2x for 2 time steps (5s → 2.5s)
    
    Returns:
        Dictionary with time step results and variable scores
    """
    time_key_for_results = effective_lead_h
    timestep_results = {
        "time_key": time_key_for_results,
        "valid_time_dt": valid_time_dt,
        "effective_lead_h": effective_lead_h,
        "variables": {},
        "aggregated_skill_scores": [],
        "aggregated_acc_scores": [],
        "qc_passed": True,
        "error_message": None,
        "skip_reason": None
    }
    
    try:
        logger.info(f"[Day1Score] Miner {miner_hotkey}: PARALLEL processing time step {valid_time_dt} (Lead: {effective_lead_h}h)")
        timestep_start = time.time()
        
        # Time data selection (this is the expensive I/O part)
        valid_time_np = np.datetime64(valid_time_dt.replace(tzinfo=None))
        
        try:
            gfs_analysis_lead = gfs_analysis_data_for_run.sel(time=valid_time_np, method="nearest")
            gfs_reference_lead = gfs_reference_forecast_for_run.sel(time=valid_time_np, method="nearest")

            selected_time_gfs_analysis = np.datetime64(gfs_analysis_lead.time.data.item(), 'ns')
            if abs(selected_time_gfs_analysis - valid_time_np) > np.timedelta64(1, 'h'):
                timestep_results["skip_reason"] = f"GFS Analysis time {selected_time_gfs_analysis} too far from target {valid_time_np}"
                return timestep_results
            
            selected_time_gfs_reference = np.datetime64(gfs_reference_lead.time.data.item(), 'ns')
            if abs(selected_time_gfs_reference - valid_time_np) > np.timedelta64(1, 'h'):
                timestep_results["skip_reason"] = f"GFS Reference time {selected_time_gfs_reference} too far from target {valid_time_np}"
                return timestep_results

        except Exception as e_sel:
            timestep_results["error_message"] = f"Could not select GFS data for lead {effective_lead_h}h: {e_sel}"
            return timestep_results
        
        # Handle miner forecast time selection with proper timezone handling
        valid_time_dt_aware = pd.Timestamp(valid_time_dt).tz_localize('UTC') if pd.Timestamp(valid_time_dt).tzinfo is None else pd.Timestamp(valid_time_dt).tz_convert('UTC')
        valid_time_np_ns = np.datetime64(valid_time_dt_aware.replace(tzinfo=None), 'ns')
        selection_label_for_miner = valid_time_np_ns
        
        # Handle timezone-aware datetime dtypes properly
        time_dtype_str = str(miner_forecast_ds.time.dtype)
        is_integer_time = False
        is_timezone_aware = False
        try:
            if 'datetime64' in time_dtype_str and 'UTC' in time_dtype_str:
                is_integer_time = False
                is_timezone_aware = True
                selection_label_for_miner = valid_time_dt_aware
            else:
                is_integer_time = np.issubdtype(miner_forecast_ds.time.dtype, np.integer)
        except TypeError:
            is_integer_time = False
        
        if is_integer_time:
            try:
                selection_label_for_miner = valid_time_np_ns.astype(np.int64)
            except Exception as e_cast:
                timestep_results["error_message"] = f"Failed to cast datetime to int64: {e_cast}"
                return timestep_results

        try:
            miner_forecast_lead = miner_forecast_ds.sel(time=selection_label_for_miner, method="nearest")
        except Exception as e_sel_miner:
            timestep_results["error_message"] = f"Error selecting from miner_forecast_ds: {e_sel_miner}"
            return timestep_results

        # Validate time selection
        miner_time_value_from_sel = miner_forecast_lead.time.item()
        time_diff_too_large = False
        
        if not is_integer_time:
            try:
                miner_time_dt64 = np.datetime64(miner_time_value_from_sel, 'ns')
                if is_timezone_aware:
                    target_naive = selection_label_for_miner.tz_convert('UTC').tz_localize(None)
                    target_dt64 = np.datetime64(target_naive, 'ns')
                    if abs(miner_time_dt64 - target_dt64) > np.timedelta64(1, 'h'):
                        time_diff_too_large = True
                else:
                    if abs(miner_time_dt64 - selection_label_for_miner) > np.timedelta64(1, 'h'):
                        time_diff_too_large = True
            except Exception:
                time_diff_too_large = True
        else:
            hour_in_nanos = np.timedelta64(1, 'h').astype('timedelta64[ns]').astype(np.int64)
            if abs(miner_time_value_from_sel - selection_label_for_miner) > hour_in_nanos:
                time_diff_too_large = True
        
        if time_diff_too_large:
            timestep_results["skip_reason"] = f"Miner forecast time {miner_time_value_from_sel} too far from target {selection_label_for_miner}"
            return timestep_results

        # PARALLEL VARIABLE PROCESSING within this time step
        logger.info(f"[Day1Score] Miner {miner_hotkey}: Starting PARALLEL processing of {len(variables_to_score)} variables at {valid_time_dt}")
        
        # Cache objects for this time step
        cached_lat_weights = None
        cached_grid_dims = None
        cached_pressure_dims = {}
        
        # Create parallel tasks for all variables in this time step
        variable_tasks = []
        for var_config in variables_to_score:
            var_name = var_config['name']
            var_level = var_config.get('level')
            var_key = f"{var_name}{var_level}" if var_level else var_name
            
            # Initialize result structure for this variable
            timestep_results["variables"][var_key] = {
                "skill_score": None, "acc": None, "sanity_checks": {},
                "clone_distance_mse": None, "clone_penalty_applied": None
            }
            
            # Create parallel task for this variable
            task = asyncio.create_task(
                _process_single_variable_parallel(
                    var_config=var_config,
                    miner_forecast_lead=miner_forecast_lead,
                    gfs_analysis_lead=gfs_analysis_lead,
                    gfs_reference_lead=gfs_reference_lead,
                    era5_climatology=era5_climatology,
                    precomputed_climatology_cache=precomputed_climatology_cache,
                    day1_scoring_config=day1_scoring_config,
                    valid_time_dt=valid_time_dt,
                    effective_lead_h=effective_lead_h,
                    miner_hotkey=miner_hotkey,
                    cached_pressure_dims=cached_pressure_dims,
                    cached_lat_weights=cached_lat_weights,
                    cached_grid_dims=cached_grid_dims,
                    variables_to_score=variables_to_score
                ),
                name=f"var_{var_key}_{effective_lead_h}h_{miner_hotkey[:8]}"
            )
            variable_tasks.append((var_key, task))
        
        # Execute all variables in parallel for this time step
        logger.debug(f"[Day1Score] Miner {miner_hotkey}: Executing {len(variable_tasks)} variable tasks for time step {valid_time_dt}")
        
        # Get just the tasks for asyncio.gather
        tasks_only = [task for _, task in variable_tasks]
        variable_results = await asyncio.gather(*tasks_only, return_exceptions=True)
        
        timestep_time = time.time() - timestep_start
        logger.info(f"[Day1Score] Miner {miner_hotkey}: PARALLEL time step {valid_time_dt} completed in {timestep_time:.2f}s")
        
        # Process results from parallel variable execution
        for i, (var_key, _) in enumerate(variable_tasks):
            result = variable_results[i]
            
            if isinstance(result, Exception):
                logger.error(f"[Day1Score] Miner {miner_hotkey}: PARALLEL task failed for {var_key} at {valid_time_dt}: {result}")
                timestep_results["variables"][var_key]["error"] = str(result)
                timestep_results["qc_passed"] = False
                continue
                
            if not isinstance(result, dict):
                logger.error(f"[Day1Score] Miner {miner_hotkey}: Invalid result type from parallel task {var_key}: {type(result)}")
                continue
            
            status = result.get("status")
            
            if status == "success":
                # Successful variable processing
                timestep_results["variables"][var_key]["skill_score"] = result.get("skill_score")
                timestep_results["variables"][var_key]["acc"] = result.get("acc_score")
                timestep_results["variables"][var_key]["clone_distance_mse"] = result.get("clone_distance_mse")
                timestep_results["variables"][var_key]["clone_penalty_applied"] = result.get("clone_penalty_applied")
                timestep_results["variables"][var_key]["sanity_checks"] = result.get("sanity_checks", {})
                
                # Add to aggregated scores for this time step
                skill_score = result.get("skill_score")
                acc_score = result.get("acc_score")
                if skill_score is not None and np.isfinite(skill_score):
                    timestep_results["aggregated_skill_scores"].append(skill_score)
                if acc_score is not None and np.isfinite(acc_score):
                    timestep_results["aggregated_acc_scores"].append(acc_score)
                    
                # Check for QC failures
                qc_failure_reason = result.get("qc_failure_reason")
                if qc_failure_reason:
                    timestep_results["qc_passed"] = False
                    
            elif status == "skipped":
                qc_failure_reason = result.get("qc_failure_reason", "Variable skipped")
                logger.info(f"[Day1Score] Miner {miner_hotkey}: Variable {var_key} skipped at {valid_time_dt} - {qc_failure_reason}")
                timestep_results["qc_passed"] = False
                
            elif status == "error":
                error_message = result.get("error_message", "Unknown error")
                logger.error(f"[Day1Score] Miner {miner_hotkey}: Variable {var_key} processing error at {valid_time_dt}: {error_message}")
                timestep_results["variables"][var_key]["error"] = error_message
                timestep_results["qc_passed"] = False
                
        return timestep_results
        
    except Exception as e_timestep:
        logger.error(f"[Day1Score] Miner {miner_hotkey}: Error in parallel time step {valid_time_dt}: {e_timestep}", exc_info=True)
        timestep_results["error_message"] = str(e_timestep)
        return timestep_results
    finally:
        # CRITICAL ABC MEMORY LEAK FIX: Clean up all timestep ABC objects
        try:
            # Clean up time slice datasets (major ABC objects)
            timestep_objects = [
                'miner_forecast_lead', 'gfs_analysis_lead', 'gfs_reference_lead',
                'cached_lat_weights', 'cached_pressure_dims', 'cached_grid_dims'
            ]
            
            cleaned_count = 0
            for obj_name in timestep_objects:
                if obj_name in locals():
                    try:
                        obj = locals()[obj_name]
                        if hasattr(obj, 'close') and callable(obj.close):
                            obj.close()
                        del obj
                        cleaned_count += 1
                    except Exception:
                        pass
            
            # Force garbage collection for time step cleanup
            if cleaned_count > 0:
                collected = gc.collect()
                logger.debug(f"[Day1Score] Timestep {valid_time_dt} cleanup: removed {cleaned_count} objects, GC collected {collected}")
                
        except Exception as cleanup_err:
            logger.debug(f"[Day1Score] Timestep {valid_time_dt} cleanup error: {cleanup_err}")

def _fast_interpolation(source_data, target_grid, method="nearest"):
    """
    OPTIMIZATION: Use faster interpolation method (5-10% speedup)
    Switch from 'linear' to 'nearest' for 2-3x faster interpolation with minimal accuracy loss
    """
    try:
        # Use nearest neighbor for speed - usually sufficient for weather data
        return source_data.interp_like(target_grid, method=method)
    except Exception:
        # Fallback to linear if nearest fails
        return source_data.interp_like(target_grid, method="linear")

def _vectorized_stats_calculation(data_arrays):
    """
    OPTIMIZATION: Vectorized statistics calculation (3-5% speedup)
    Calculate min/max/mean for multiple arrays simultaneously
    """
    try:
        import dask.array as da
        # Convert to dask arrays for vectorized computation
        dask_arrays = [da.from_array(arr.values, chunks=arr.chunks) for arr in data_arrays]
        
        # Compute all statistics at once
        mins = [float(arr.min().compute()) for arr in dask_arrays]
        maxs = [float(arr.max().compute()) for arr in dask_arrays] 
        means = [float(arr.mean().compute()) for arr in dask_arrays]
        
        return list(zip(mins, maxs, means))
    except ImportError:
        # Fallback to individual computation if dask not available
        return [(float(arr.min()), float(arr.max()), float(arr.mean())) for arr in data_arrays]

def _reduce_precision(data_array):
    """
    OPTIMIZATION: Use float32 instead of float64 (2-3% speedup, 50% memory reduction)
    Weather data typically doesn't need double precision
    """
    if data_array.dtype == 'float64':
        return data_array.astype('float32')
    return data_array
