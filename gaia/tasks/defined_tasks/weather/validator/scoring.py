"""
Weather Task Validator Scoring Handlers

Compute worker handlers for weather forecast scoring.
These handlers implement the critical scoring algorithms ported from the legacy system.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Union

import numpy as np
import pandas as pd
import xarray as xr
import xskillscore as xs
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


def handle_day1_scoring_computation(
    config,
    miner_response_data: Dict[str, Any],
    gfs_analysis_zarr_data: bytes,
    gfs_reference_zarr_data: bytes,
    era5_climatology_zarr_data: bytes,
    day1_scoring_config: Dict[str, Any],
    run_gfs_init_time_iso: str,
    **kwargs
) -> Dict[str, Any]:
    """
    Synchronous handler for Day-1 scoring computation.
    
    Performs Day-1 scoring for a single miner's forecast using GFS analysis as truth
    and GFS forecast as reference. Calculates bias-corrected skill scores and ACC
    for specified variables and lead times.
    
    Args:
        config: Configuration object
        miner_response_data: Miner response database record
        gfs_analysis_zarr_data: Serialized GFS analysis dataset
        gfs_reference_zarr_data: Serialized GFS reference forecast dataset
        era5_climatology_zarr_data: Serialized ERA5 climatology dataset
        day1_scoring_config: Day-1 scoring configuration
        run_gfs_init_time_iso: GFS initialization time in ISO format
        **kwargs: Additional parameters
    
    Returns:
        Dict containing scoring results
        
    Raises:
        ValueError: If scoring computation fails
    """
    import pickle
    import tempfile
    from pathlib import Path
    
    logger.info(f"Starting Day-1 scoring computation for miner {miner_response_data.get('miner_hotkey', 'unknown')}")
    
    try:
        # Parse the ISO time string
        run_gfs_init_time = datetime.fromisoformat(run_gfs_init_time_iso.replace('Z', '+00:00'))
        
        # Deserialize the datasets
        gfs_analysis_ds = pickle.loads(gfs_analysis_zarr_data)
        gfs_reference_ds = pickle.loads(gfs_reference_zarr_data)
        era5_climatology_ds = pickle.loads(era5_climatology_zarr_data)
        
        # Since this handler is synchronous but needs async operations, 
        # we'll create a new event loop
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError("Event loop already running")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        try:
            # Run the async scoring computation
            result = loop.run_until_complete(
                _compute_day1_scoring_async(
                    miner_response_data=miner_response_data,
                    gfs_analysis_ds=gfs_analysis_ds,
                    gfs_reference_ds=gfs_reference_ds,
                    era5_climatology_ds=era5_climatology_ds,
                    day1_scoring_config=day1_scoring_config,
                    run_gfs_init_time=run_gfs_init_time
                )
            )
        finally:
            loop.close()
        
        logger.info(f"Day-1 scoring completed with overall score: {result.get('overall_day1_score')}")
        return result
        
    except Exception as e:
        logger.error(f"Error in Day-1 scoring computation: {e}")
        raise ValueError(f"Day-1 scoring computation failed: {e}")


def handle_era5_final_scoring_computation(
    config,
    miner_response_data: Dict[str, Any],
    era5_analysis_zarr_data: bytes,
    era5_climatology_zarr_data: bytes,
    era5_scoring_config: Dict[str, Any],
    valid_time_iso: str,
    **kwargs
) -> Dict[str, Any]:
    """
    Synchronous handler for ERA5 final scoring computation.
    
    Performs final scoring using ERA5 analysis as ground truth with comprehensive
    metrics including RMSE, bias, and correlation calculations.
    
    Args:
        config: Configuration object
        miner_response_data: Miner response database record
        era5_analysis_zarr_data: Serialized ERA5 analysis dataset
        era5_climatology_zarr_data: Serialized ERA5 climatology dataset
        era5_scoring_config: ERA5 scoring configuration
        valid_time_iso: Valid time in ISO format
        **kwargs: Additional parameters
    
    Returns:
        Dict containing ERA5 scoring results
        
    Raises:
        ValueError: If ERA5 scoring computation fails
    """
    import pickle
    
    logger.info(f"Starting ERA5 final scoring computation for miner {miner_response_data.get('miner_hotkey', 'unknown')}")
    
    try:
        # Parse the ISO time string
        valid_time = datetime.fromisoformat(valid_time_iso.replace('Z', '+00:00'))
        
        # Deserialize the datasets
        era5_analysis_ds = pickle.loads(era5_analysis_zarr_data)
        era5_climatology_ds = pickle.loads(era5_climatology_zarr_data)
        
        # Create event loop for async operations
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError("Event loop already running")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        try:
            # Run the async ERA5 scoring computation
            result = loop.run_until_complete(
                _compute_era5_final_scoring_async(
                    miner_response_data=miner_response_data,
                    era5_analysis_ds=era5_analysis_ds,
                    era5_climatology_ds=era5_climatology_ds,
                    era5_scoring_config=era5_scoring_config,
                    valid_time=valid_time
                )
            )
        finally:
            loop.close()
        
        logger.info(f"ERA5 final scoring completed with overall score: {result.get('overall_era5_score')}")
        return result
        
    except Exception as e:
        logger.error(f"Error in ERA5 final scoring computation: {e}")
        raise ValueError(f"ERA5 final scoring computation failed: {e}")


def handle_forecast_verification_computation(
    config,
    miner_response_data: Dict[str, Any],
    forecast_zarr_data: bytes,
    verification_config: Dict[str, Any],
    **kwargs
) -> Dict[str, Any]:
    """
    Synchronous handler for forecast verification computation.
    
    Performs forecast verification with data quality checks and integrity validation.
    
    Args:
        config: Configuration object
        miner_response_data: Miner response database record
        forecast_zarr_data: Serialized forecast dataset
        verification_config: Verification configuration
        **kwargs: Additional parameters
    
    Returns:
        Dict containing verification results
        
    Raises:
        ValueError: If verification computation fails
    """
    import pickle
    
    logger.info(f"Starting forecast verification for miner {miner_response_data.get('miner_hotkey', 'unknown')}")
    
    try:
        # Deserialize the forecast dataset
        forecast_ds = pickle.loads(forecast_zarr_data)
        
        # Create event loop for async operations
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError("Event loop already running")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        try:
            # Run the async verification computation
            result = loop.run_until_complete(
                _compute_forecast_verification_async(
                    miner_response_data=miner_response_data,
                    forecast_ds=forecast_ds,
                    verification_config=verification_config
                )
            )
        finally:
            loop.close()
        
        logger.info(f"Forecast verification completed: {result.get('verification_passed', False)}")
        return result
        
    except Exception as e:
        logger.error(f"Error in forecast verification computation: {e}")
        raise ValueError(f"Forecast verification computation failed: {e}")


# ========== Async Helper Functions ==========

async def _compute_day1_scoring_async(
    miner_response_data: Dict[str, Any],
    gfs_analysis_ds: xr.Dataset,
    gfs_reference_ds: xr.Dataset,
    era5_climatology_ds: xr.Dataset,
    day1_scoring_config: Dict[str, Any],
    run_gfs_init_time: datetime
) -> Dict[str, Any]:
    """
    Async implementation of Day-1 scoring computation.
    
    This function implements the core Day-1 scoring algorithm ported from the legacy system.
    """
    response_id = miner_response_data["id"]
    miner_hotkey = miner_response_data["miner_hotkey"]
    miner_uid = miner_response_data.get("miner_uid")
    run_id = miner_response_data.get("run_id")
    
    scoring_start_time = time.time()
    logger.info(f"[Day1Score] Starting for miner {miner_hotkey} (Resp: {response_id}, Run: {run_id}, UID: {miner_uid})")
    
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
    
    try:
        # Determine times to evaluate
        lead_times_to_score_hours = day1_scoring_config.get(
            "lead_times_hours", [6, 12]
        )
        times_to_evaluate = [
            run_gfs_init_time + timedelta(hours=h)
            for h in lead_times_to_score_hours
        ]
        
        logger.info(f"[Day1Score] Using lead_hours {lead_times_to_score_hours} for evaluation")
        
        # Variables to score
        variables_to_score = day1_scoring_config.get("variables_levels_to_score", [])
        
        # Initialize aggregation lists
        aggregated_skill_scores = []
        aggregated_acc_scores = []
        
        # Process all time steps in parallel
        logger.info(f"[Day1Score] Starting parallel processing of {len(times_to_evaluate)} time steps")
        
        timestep_tasks = []
        for valid_time_dt in times_to_evaluate:
            effective_lead_h = int((valid_time_dt - run_gfs_init_time).total_seconds() / 3600)
            
            # Normalize timezone
            if valid_time_dt.tzinfo is None or valid_time_dt.tzinfo.utcoffset(valid_time_dt) is None:
                valid_time_dt = valid_time_dt.replace(tzinfo=timezone.utc)
            else:
                valid_time_dt = valid_time_dt.astimezone(timezone.utc)
            
            # Initialize result structure
            day1_results["lead_time_scores"][effective_lead_h] = {}
            
            # Create parallel task
            task = asyncio.create_task(
                _process_single_timestep_day1(
                    valid_time_dt=valid_time_dt,
                    effective_lead_h=effective_lead_h,
                    variables_to_score=variables_to_score,
                    gfs_analysis_ds=gfs_analysis_ds,
                    gfs_reference_ds=gfs_reference_ds,
                    era5_climatology_ds=era5_climatology_ds,
                    day1_scoring_config=day1_scoring_config,
                    run_gfs_init_time=run_gfs_init_time,
                    miner_hotkey=miner_hotkey,
                ),
                name=f"timestep_{effective_lead_h}h_{miner_hotkey[:8]}"
            )
            timestep_tasks.append((effective_lead_h, task))
        
        # Execute all time steps in parallel
        tasks_only = [task for _, task in timestep_tasks]
        timestep_results = await asyncio.gather(*tasks_only, return_exceptions=True)
        
        # Process results
        for i, (effective_lead_h, _) in enumerate(timestep_tasks):
            result = timestep_results[i]
            
            if isinstance(result, Exception):
                logger.error(f"[Day1Score] Time step {effective_lead_h}h failed: {result}")
                day1_results["qc_passed_all_vars_leads"] = False
                continue
            
            if not isinstance(result, dict):
                logger.error(f"[Day1Score] Invalid result type from time step {effective_lead_h}h")
                continue
            
            # Handle time step results
            if result.get("skip_reason"):
                logger.warning(f"[Day1Score] Time step {effective_lead_h}h skipped - {result['skip_reason']}")
                continue
            
            if result.get("error_message"):
                logger.error(f"[Day1Score] Time step {effective_lead_h}h error - {result['error_message']}")
                day1_results["qc_passed_all_vars_leads"] = False
                continue
            
            # Process successful results
            if not result.get("qc_passed", True):
                day1_results["qc_passed_all_vars_leads"] = False
            
            # Copy variable results
            for var_key, var_result in result.get("variables", {}).items():
                day1_results["lead_time_scores"][effective_lead_h][var_key] = var_result
            
            # Add to aggregated scores
            aggregated_skill_scores.extend(result.get("aggregated_skill_scores", []))
            aggregated_acc_scores.extend(result.get("aggregated_acc_scores", []))
        
        # Calculate final scores
        clipped_skill_scores = [max(0.0, s) for s in aggregated_skill_scores if np.isfinite(s)]
        scaled_acc_scores = [(a + 1.0) / 2.0 for a in aggregated_acc_scores if np.isfinite(a)]
        
        avg_clipped_skill = np.mean(clipped_skill_scores) if clipped_skill_scores else 0.0
        avg_scaled_acc = np.mean(scaled_acc_scores) if scaled_acc_scores else 0.0
        
        if not np.isfinite(avg_clipped_skill):
            avg_clipped_skill = 0.0
        if not np.isfinite(avg_scaled_acc):
            avg_scaled_acc = 0.0
        
        if not aggregated_skill_scores and not aggregated_acc_scores:
            logger.warning(f"[Day1Score] No valid scores to aggregate. Setting overall score to 0.")
            day1_results["overall_day1_score"] = 0.0
            day1_results["qc_passed_all_vars_leads"] = False
        else:
            alpha = day1_scoring_config.get("alpha_skill", 0.5)
            beta = day1_scoring_config.get("beta_acc", 0.5)
            
            normalized_score = alpha * avg_clipped_skill + beta * avg_scaled_acc
            day1_results["overall_day1_score"] = normalized_score
            
            logger.info(f"[Day1Score] AvgClippedSkill={avg_clipped_skill:.3f}, "
                       f"AvgScaledACC={avg_scaled_acc:.3f}, Overall Score={normalized_score:.3f}")
    
    except Exception as e:
        logger.error(f"[Day1Score] Error in async computation: {e}", exc_info=True)
        day1_results["error_message"] = str(e)
        day1_results["overall_day1_score"] = -np.inf
        day1_results["qc_passed_all_vars_leads"] = False
    
    total_scoring_time = time.time() - scoring_start_time
    logger.info(f"[Day1Score] Completed in {total_scoring_time:.2f} seconds")
    
    return day1_results


async def _process_single_timestep_day1(
    valid_time_dt: datetime,
    effective_lead_h: int,
    variables_to_score: List[Dict],
    gfs_analysis_ds: xr.Dataset,
    gfs_reference_ds: xr.Dataset,
    era5_climatology_ds: xr.Dataset,
    day1_scoring_config: Dict[str, Any],
    run_gfs_init_time: datetime,
    miner_hotkey: str
) -> Dict[str, Any]:
    """
    Process a single timestep for Day-1 scoring.
    """
    logger.debug(f"[Day1Score] Processing timestep {effective_lead_h}h at {valid_time_dt}")
    
    result = {
        "qc_passed": True,
        "variables": {},
        "aggregated_skill_scores": [],
        "aggregated_acc_scores": [],
        "error_message": None,
        "skip_reason": None
    }
    
    try:
        # Extract time slices from datasets
        gfs_analysis_lead = await _extract_time_slice(gfs_analysis_ds, valid_time_dt)
        gfs_reference_lead = await _extract_time_slice(gfs_reference_ds, valid_time_dt)
        
        if gfs_analysis_lead is None or gfs_reference_lead is None:
            result["skip_reason"] = f"Missing data for time {valid_time_dt}"
            return result
        
        # Process each variable
        for var_config in variables_to_score:
            var_name = var_config["name"]
            var_level = var_config.get("level")
            var_key = f"{var_name}{var_level}" if var_level else var_name
            
            try:
                var_result = await _process_single_variable_day1(
                    var_config=var_config,
                    gfs_analysis_lead=gfs_analysis_lead,
                    gfs_reference_lead=gfs_reference_lead,
                    era5_climatology_ds=era5_climatology_ds,
                    day1_scoring_config=day1_scoring_config,
                    valid_time_dt=valid_time_dt,
                    miner_hotkey=miner_hotkey
                )
                
                result["variables"][var_key] = var_result
                
                # Add to aggregated scores if successful
                if var_result.get("status") == "success":
                    if var_result.get("skill_score") is not None:
                        result["aggregated_skill_scores"].append(var_result["skill_score"])
                    if var_result.get("acc_score") is not None:
                        result["aggregated_acc_scores"].append(var_result["acc_score"])
                else:
                    result["qc_passed"] = False
                    
            except Exception as e:
                logger.error(f"[Day1Score] Error processing variable {var_key}: {e}")
                result["qc_passed"] = False
                result["variables"][var_key] = {
                    "status": "error",
                    "error_message": str(e)
                }
    
    except Exception as e:
        logger.error(f"[Day1Score] Error processing timestep {effective_lead_h}h: {e}")
        result["error_message"] = str(e)
        result["qc_passed"] = False
    
    return result


async def _process_single_variable_day1(
    var_config: Dict,
    gfs_analysis_lead: xr.Dataset,
    gfs_reference_lead: xr.Dataset,
    era5_climatology_ds: xr.Dataset,
    day1_scoring_config: Dict[str, Any],
    valid_time_dt: datetime,
    miner_hotkey: str
) -> Dict[str, Any]:
    """
    Process a single variable for Day-1 scoring.
    """
    var_name = var_config["name"]
    var_level = var_config.get("level")
    var_key = f"{var_name}{var_level}" if var_level else var_name
    
    result = {
        "status": "processing",
        "skill_score": None,
        "acc_score": None,
        "error_message": None
    }
    
    try:
        # Extract variable data
        truth_var_da = gfs_analysis_lead[var_name]
        ref_var_da = gfs_reference_lead[var_name]
        
        # Handle pressure levels if specified
        if var_level:
            truth_var_da = _extract_pressure_level(truth_var_da, var_level)
            ref_var_da = _extract_pressure_level(ref_var_da, var_level)
            
            if truth_var_da is None or ref_var_da is None:
                result["status"] = "skipped"
                result["error_message"] = f"Missing pressure level {var_level} for {var_name}"
                return result
        
        # Get climatology data
        clim_var_da = await _get_climatology_data(
            era5_climatology_ds, var_config, valid_time_dt, truth_var_da
        )
        
        if clim_var_da is None:
            result["status"] = "skipped"
            result["error_message"] = f"Missing climatology data for {var_name}"
            return result
        
        # Calculate latitude weights
        lat_weights = _calculate_latitude_weights(truth_var_da)
        
        # Perform sanity checks
        sanity_passed = await _perform_basic_sanity_checks(
            truth_var_da, ref_var_da, var_name, day1_scoring_config
        )
        
        if not sanity_passed:
            result["status"] = "skipped"
            result["error_message"] = f"Sanity checks failed for {var_name}"
            return result
        
        # Calculate MSE skill score with bias correction
        skill_score = await _calculate_mse_skill_score(
            truth_var_da, truth_var_da, ref_var_da, lat_weights
        )
        
        # Calculate ACC score using ERA5 climatology
        acc_score = await _calculate_acc_score(
            truth_var_da, truth_var_da, clim_var_da, lat_weights
        )
        
        result["skill_score"] = skill_score
        result["acc_score"] = acc_score
        result["status"] = "success"
        
        logger.debug(f"[Day1Score] Variable {var_key} scored: skill={skill_score:.3f}, acc={acc_score:.3f}")
        
    except Exception as e:
        logger.error(f"[Day1Score] Error processing variable {var_key}: {e}")
        result["status"] = "error"
        result["error_message"] = str(e)
    
    return result


async def _compute_era5_final_scoring_async(
    miner_response_data: Dict[str, Any],
    era5_analysis_ds: xr.Dataset,
    era5_climatology_ds: xr.Dataset,
    era5_scoring_config: Dict[str, Any],
    valid_time: datetime
) -> Dict[str, Any]:
    """
    Async implementation of ERA5 final scoring computation.
    Calculates comprehensive statistical metrics against ERA5 analysis.
    """
    logger.info(f"[ERA5Score] Starting ERA5 final scoring for miner {miner_response_data['miner_hotkey']}")
    
    era5_results = {
        "response_id": miner_response_data["id"],
        "miner_hotkey": miner_response_data["miner_hotkey"],
        "overall_era5_score": None,
        "rmse_scores": {},
        "bias_scores": {},
        "correlation_scores": {},
        "variable_scores": {},
        "error_message": None
    }
    
    try:
        # Load miner forecast data
        forecast_ds = await _load_miner_forecast_data(miner_response_data)
        if forecast_ds is None:
            era5_results["error_message"] = "Failed to load miner forecast data"
            return era5_results
        
        # Get variables to score from config
        variables_to_score = era5_scoring_config.get("variables_to_score", [])
        if not variables_to_score:
            era5_results["error_message"] = "No variables configured for ERA5 scoring"
            return era5_results
        
        variable_scores = []
        
        # Process each variable
        for var_config in variables_to_score:
            var_name = var_config["name"]
            var_level = var_config.get("level")
            var_key = f"{var_name}{var_level}" if var_level else var_name
            
            try:
                # Extract variable data from forecast and ERA5
                forecast_var = forecast_ds[var_name]
                era5_var = era5_analysis_ds[var_name]
                
                # Handle pressure levels
                if var_level:
                    forecast_var = _extract_pressure_level(forecast_var, var_level)
                    era5_var = _extract_pressure_level(era5_var, var_level)
                    
                    if forecast_var is None or era5_var is None:
                        logger.warning(f"Missing pressure level {var_level} for {var_name}")
                        continue
                
                # Interpolate to common grid if needed
                if not forecast_var.dims == era5_var.dims:
                    forecast_var = await asyncio.to_thread(
                        lambda: forecast_var.interp_like(era5_var, method="linear")
                    )
                
                # Calculate latitude weights
                lat_weights = _calculate_latitude_weights(era5_var)
                spatial_dims = [d for d in era5_var.dims if d.lower() in ("lat", "lon")]
                
                # Calculate RMSE
                rmse_result = await asyncio.to_thread(
                    lambda: xs.rmse(forecast_var, era5_var, dim=spatial_dims, weights=lat_weights, skipna=True)
                )
                if hasattr(rmse_result, "compute"):
                    rmse_result = await asyncio.to_thread(rmse_result.compute)
                rmse_score = float(rmse_result.item())
                
                # Calculate bias (mean error)
                bias_result = await asyncio.to_thread(
                    lambda: (forecast_var - era5_var).weighted(lat_weights).mean(dim=spatial_dims, skipna=True)
                )
                if hasattr(bias_result, "compute"):
                    bias_result = await asyncio.to_thread(bias_result.compute)
                bias_score = float(bias_result.item())
                
                # Calculate correlation
                corr_result = await asyncio.to_thread(
                    lambda: xs.pearson_r(forecast_var, era5_var, dim=spatial_dims, weights=lat_weights, skipna=True)
                )
                if hasattr(corr_result, "compute"):
                    corr_result = await asyncio.to_thread(corr_result.compute)
                corr_score = float(corr_result.item())
                
                # Store individual variable scores
                era5_results["rmse_scores"][var_key] = rmse_score
                era5_results["bias_scores"][var_key] = bias_score
                era5_results["correlation_scores"][var_key] = corr_score
                
                # Calculate weighted variable score (higher correlation, lower RMSE/bias is better)
                var_weight = var_config.get("weight", 1.0)
                
                # Normalize RMSE by variable-specific scaling
                var_scaling = era5_scoring_config.get("variable_scaling", {}).get(var_name, 1.0)
                normalized_rmse = rmse_score / var_scaling
                
                # Combined score: correlation - penalty for RMSE and bias
                var_score = corr_score - (normalized_rmse * 0.1) - (abs(bias_score) * 0.05)
                var_score = max(var_score, -1.0)  # Floor at -1
                
                era5_results["variable_scores"][var_key] = {
                    "score": var_score,
                    "weight": var_weight,
                    "rmse": rmse_score,
                    "bias": bias_score,
                    "correlation": corr_score
                }
                
                variable_scores.append(var_score * var_weight)
                
                logger.debug(f"[ERA5Score] {var_key}: RMSE={rmse_score:.4f}, Bias={bias_score:.4f}, Corr={corr_score:.4f}, Score={var_score:.4f}")
                
            except Exception as var_e:
                logger.error(f"[ERA5Score] Error processing variable {var_key}: {var_e}")
                era5_results["variable_scores"][var_key] = {
                    "error": str(var_e)
                }
        
        # Calculate overall ERA5 score as weighted average
        if variable_scores:
            total_weight = sum(var_config.get("weight", 1.0) for var_config in variables_to_score)
            era5_results["overall_era5_score"] = sum(variable_scores) / total_weight
        else:
            era5_results["overall_era5_score"] = 0.0
            era5_results["error_message"] = "No variables successfully scored"
        
        logger.info(f"[ERA5Score] ERA5 scoring completed. Overall score: {era5_results['overall_era5_score']:.4f}")
        
    except Exception as e:
        logger.error(f"[ERA5Score] Error in ERA5 final scoring: {e}")
        era5_results["error_message"] = str(e)
        era5_results["overall_era5_score"] = 0.0
    
    return era5_results


async def _load_miner_forecast_data(miner_response_data: Dict[str, Any]) -> Optional[xr.Dataset]:
    """
    Load miner forecast data from storage (local Zarr or R2).
    
    Args:
        miner_response_data: Miner response record containing job_id and miner info
        
    Returns:
        xr.Dataset containing the miner's forecast data, or None if loading fails
    """
    job_id = miner_response_data.get("job_id")
    miner_hotkey = miner_response_data.get("miner_hotkey", "unknown")
    
    if not job_id:
        logger.error(f"No job_id found in miner response data for {miner_hotkey[:8]}")
        return None
    
    try:
        # Import required modules
        import xarray as xr
        from pathlib import Path
        import os
        
        logger.info(f"Loading forecast data for miner {miner_hotkey[:8]}, job {job_id}")
        
        # First, try to get the forecast path from the database
        # This requires database access - we'll need to get it from the task context
        # For now, construct the expected path based on the job_id
        
        # Default forecast directory from environment or default
        default_forecast_dir = Path(os.getenv("MINER_FORECAST_DIR", "./miner_forecasts/"))
        
        # Construct expected Zarr path - miners typically save as {job_id}.zarr
        zarr_path = default_forecast_dir / f"{job_id}.zarr"
        
        logger.debug(f"Attempting to load forecast from: {zarr_path}")
        
        # Check if the Zarr store exists locally
        if zarr_path.exists() and zarr_path.is_dir():
            logger.info(f"Loading forecast from local Zarr store: {zarr_path}")
            
            # Load the Zarr dataset
            try:
                forecast_ds = await asyncio.to_thread(
                    lambda: xr.open_zarr(zarr_path, consolidated=True)
                )
                
                # Validate the dataset has the expected structure
                if _validate_forecast_dataset(forecast_ds, miner_hotkey):
                    logger.info(f"Successfully loaded forecast for {miner_hotkey[:8]} from {zarr_path}")
                    return forecast_ds
                else:
                    logger.warning(f"Forecast dataset validation failed for {miner_hotkey[:8]}")
                    return None
                    
            except Exception as zarr_error:
                logger.error(f"Error reading Zarr store {zarr_path}: {zarr_error}")
                # Fall through to try alternative loading methods
        
        # If local Zarr doesn't exist, try R2 or alternative storage
        logger.debug(f"Local Zarr not found at {zarr_path}, attempting R2 loading")
        
        # Try to load from R2 (if configured)
        r2_dataset = await _load_forecast_from_r2(job_id, miner_hotkey)
        if r2_dataset is not None:
            return r2_dataset
        
        # Try alternative local paths
        alternative_paths = [
            default_forecast_dir / f"{job_id}_forecast.zarr",
            default_forecast_dir / f"forecast_{job_id}.zarr",
            Path(f"./forecasts/{job_id}.zarr"),
            Path(f"./outputs/{job_id}.zarr")
        ]
        
        for alt_path in alternative_paths:
            if alt_path.exists() and alt_path.is_dir():
                logger.info(f"Found forecast at alternative path: {alt_path}")
                try:
                    forecast_ds = await asyncio.to_thread(
                        lambda: xr.open_zarr(alt_path, consolidated=True)
                    )
                    if _validate_forecast_dataset(forecast_ds, miner_hotkey):
                        logger.info(f"Successfully loaded forecast for {miner_hotkey[:8]} from {alt_path}")
                        return forecast_ds
                except Exception as alt_error:
                    logger.debug(f"Failed to load from {alt_path}: {alt_error}")
                    continue
        
        logger.error(f"Could not find forecast data for miner {miner_hotkey[:8]}, job {job_id}")
        return None
        
    except Exception as e:
        logger.error(f"Error loading miner forecast data for {miner_hotkey[:8]}: {e}")
        return None


def _validate_forecast_dataset(dataset: xr.Dataset, miner_hotkey: str) -> bool:
    """
    Validate that a forecast dataset has the expected structure and variables.
    
    Args:
        dataset: The xarray Dataset to validate
        miner_hotkey: Miner hotkey for logging
        
    Returns:
        bool: True if dataset is valid, False otherwise
    """
    try:
        # Check for required dimensions
        required_dims = ["time", "lat", "lon"]
        missing_dims = [dim for dim in required_dims if dim not in dataset.dims]
        if missing_dims:
            logger.warning(f"Forecast dataset for {miner_hotkey[:8]} missing dimensions: {missing_dims}")
            return False
        
        # Check for required weather variables
        expected_vars = ["2t", "10u", "10v", "msl"]  # Core surface variables
        available_vars = list(dataset.data_vars.keys())
        missing_vars = [var for var in expected_vars if var not in available_vars]
        
        if len(missing_vars) == len(expected_vars):
            logger.warning(f"Forecast dataset for {miner_hotkey[:8]} missing all expected variables: {expected_vars}")
            return False
        
        if missing_vars:
            logger.debug(f"Forecast dataset for {miner_hotkey[:8]} missing some variables: {missing_vars}")
        
        # Check dataset is not empty
        if dataset.dims.get("time", 0) == 0:
            logger.warning(f"Forecast dataset for {miner_hotkey[:8]} has no time steps")
            return False
        
        # Check for reasonable spatial coverage
        lat_size = dataset.dims.get("lat", 0)
        lon_size = dataset.dims.get("lon", 0)
        if lat_size < 10 or lon_size < 10:
            logger.warning(f"Forecast dataset for {miner_hotkey[:8]} has insufficient spatial coverage: {lat_size}x{lon_size}")
            return False
        
        logger.debug(f"Forecast dataset validation passed for {miner_hotkey[:8]}: "
                    f"{len(available_vars)} variables, {dataset.dims['time']} time steps, "
                    f"{lat_size}x{lon_size} grid")
        return True
        
    except Exception as e:
        logger.error(f"Error validating forecast dataset for {miner_hotkey[:8]}: {e}")
        return False


async def _load_forecast_from_r2(job_id: str, miner_hotkey: str) -> Optional[xr.Dataset]:
    """
    Load miner forecast data from R2 storage.
    
    Args:
        job_id: The job ID for the forecast
        miner_hotkey: Miner hotkey for logging
        
    Returns:
        xr.Dataset or None if loading fails
    """
    try:
        import boto3
        import os
        import tempfile
        import shutil
        
        # R2 configuration from environment
        r2_access_key = os.getenv("R2_ACCESS_KEY_ID")
        r2_secret_key = os.getenv("R2_SECRET_ACCESS_KEY") 
        r2_endpoint = os.getenv("R2_ENDPOINT_URL")
        r2_bucket = os.getenv("R2_BUCKET_NAME")
        
        if not all([r2_access_key, r2_secret_key, r2_endpoint, r2_bucket]):
            logger.debug(f"R2 configuration incomplete, skipping R2 loading for {miner_hotkey[:8]}")
            return None
        
        logger.debug(f"Attempting to load forecast for {miner_hotkey[:8]} from R2")
        
        # Create S3 client for R2
        s3_client = boto3.client(
            's3',
            aws_access_key_id=r2_access_key,
            aws_secret_access_key=r2_secret_key,
            endpoint_url=r2_endpoint,
            region_name='auto'
        )
        
        # Expected R2 key patterns
        possible_keys = [
            f"forecasts/{job_id}.zarr",
            f"miner_forecasts/{job_id}.zarr", 
            f"weather/{job_id}.zarr",
            f"{job_id}.zarr"
        ]
        
        for r2_key in possible_keys:
            try:
                # Check if the object exists in R2
                s3_client.head_object(Bucket=r2_bucket, Key=r2_key)
                logger.info(f"Found forecast in R2 at key: {r2_key}")
                
                # Download to temporary directory
                with tempfile.TemporaryDirectory() as temp_dir:
                    local_zarr_path = Path(temp_dir) / f"{job_id}.zarr"
                    
                    # Download the Zarr store (this is a simplified approach)
                    # In practice, you might need to download individual Zarr files
                    await asyncio.to_thread(
                        s3_client.download_file, r2_bucket, r2_key, str(local_zarr_path)
                    )
                    
                    # Load the dataset
                    forecast_ds = await asyncio.to_thread(
                        lambda: xr.open_zarr(local_zarr_path, consolidated=True)
                    )
                    
                    if _validate_forecast_dataset(forecast_ds, miner_hotkey):
                        logger.info(f"Successfully loaded forecast for {miner_hotkey[:8]} from R2")
                        return forecast_ds
                        
            except s3_client.exceptions.NoSuchKey:
                logger.debug(f"R2 key not found: {r2_key}")
                continue
            except Exception as key_error:
                logger.debug(f"Error checking R2 key {r2_key}: {key_error}")
                continue
        
        logger.debug(f"No forecast found in R2 for {miner_hotkey[:8]}")
        return None
        
    except Exception as e:
        logger.debug(f"Error loading forecast from R2 for {miner_hotkey[:8]}: {e}")
        return None


async def _compute_forecast_verification_async(
    miner_response_data: Dict[str, Any],
    forecast_ds: xr.Dataset,
    verification_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Async implementation of forecast verification computation.
    Performs comprehensive data quality and integrity checks.
    """
    logger.info(f"[ForecastVerify] Starting verification for miner {miner_response_data['miner_hotkey']}")
    
    verification_results = {
        "response_id": miner_response_data["id"],
        "miner_hotkey": miner_response_data["miner_hotkey"],
        "verification_passed": True,
        "quality_checks": {},
        "integrity_checks": {},
        "grid_checks": {},
        "temporal_checks": {},
        "variable_checks": {},
        "error_message": None,
        "warnings": []
    }
    
    try:
        # 1. Grid consistency validation
        grid_checks = await _verify_grid_consistency(forecast_ds, verification_config)
        verification_results["grid_checks"] = grid_checks
        if not grid_checks.get("passed", False):
            verification_results["verification_passed"] = False
        
        # 2. Variable range validation
        variable_checks = await _verify_variable_ranges(forecast_ds, verification_config)
        verification_results["variable_checks"] = variable_checks
        if not variable_checks.get("passed", False):
            verification_results["verification_passed"] = False
        
        # 3. Temporal consistency checks
        temporal_checks = await _verify_temporal_consistency(forecast_ds, verification_config)
        verification_results["temporal_checks"] = temporal_checks
        if not temporal_checks.get("passed", False):
            verification_results["verification_passed"] = False
        
        # 4. Data quality checks
        quality_checks = await _verify_data_quality(forecast_ds, verification_config)
        verification_results["quality_checks"] = quality_checks
        if not quality_checks.get("passed", False):
            verification_results["verification_passed"] = False
        
        # 5. Physical consistency checks
        integrity_checks = await _verify_physical_consistency(forecast_ds, verification_config)
        verification_results["integrity_checks"] = integrity_checks
        if not integrity_checks.get("passed", False):
            verification_results["verification_passed"] = False
        
        # Collect all warnings
        all_warnings = []
        for check_type in ["grid_checks", "variable_checks", "temporal_checks", "quality_checks", "integrity_checks"]:
            warnings = verification_results[check_type].get("warnings", [])
            all_warnings.extend([f"{check_type}: {w}" for w in warnings])
        verification_results["warnings"] = all_warnings
        
        logger.info(f"[ForecastVerify] Verification completed. Passed: {verification_results['verification_passed']}, Warnings: {len(all_warnings)}")
        
    except Exception as e:
        logger.error(f"[ForecastVerify] Error in forecast verification: {e}")
        verification_results["error_message"] = str(e)
        verification_results["verification_passed"] = False
    
    return verification_results


async def _verify_grid_consistency(forecast_ds: xr.Dataset, config: Dict[str, Any]) -> Dict[str, Any]:
    """Verify grid consistency and coordinate validity."""
    result = {"passed": True, "warnings": [], "details": {}}
    
    try:
        # Check for required dimensions
        required_dims = config.get("required_dimensions", ["lat", "lon", "time"])
        missing_dims = [dim for dim in required_dims if dim not in forecast_ds.dims]
        if missing_dims:
            result["passed"] = False
            result["warnings"].append(f"Missing required dimensions: {missing_dims}")
        
        # Check coordinate ranges
        if "lat" in forecast_ds.dims:
            lat_values = forecast_ds.coords["lat"].values
            if lat_values.min() < -90 or lat_values.max() > 90:
                result["passed"] = False
                result["warnings"].append(f"Invalid latitude range: [{lat_values.min():.2f}, {lat_values.max():.2f}]")
        
        if "lon" in forecast_ds.dims:
            lon_values = forecast_ds.coords["lon"].values
            if lon_values.min() < -180 or lon_values.max() > 360:
                result["warnings"].append(f"Unusual longitude range: [{lon_values.min():.2f}, {lon_values.max():.2f}]")
        
        # Check grid resolution consistency
        expected_resolution = config.get("expected_grid_resolution")
        if expected_resolution and "lat" in forecast_ds.dims and "lon" in forecast_ds.dims:
            lat_diff = np.diff(forecast_ds.coords["lat"].values)
            lon_diff = np.diff(forecast_ds.coords["lon"].values)
            
            if not np.allclose(lat_diff, expected_resolution, atol=0.01):
                result["warnings"].append(f"Inconsistent latitude resolution: expected {expected_resolution}, got {lat_diff[0]:.3f}")
            if not np.allclose(lon_diff, expected_resolution, atol=0.01):
                result["warnings"].append(f"Inconsistent longitude resolution: expected {expected_resolution}, got {lon_diff[0]:.3f}")
        
        result["details"]["grid_shape"] = dict(forecast_ds.dims)
        
    except Exception as e:
        result["passed"] = False
        result["warnings"].append(f"Grid verification error: {e}")
    
    return result


async def _verify_variable_ranges(forecast_ds: xr.Dataset, config: Dict[str, Any]) -> Dict[str, Any]:
    """Verify variable values are within physically reasonable ranges."""
    result = {"passed": True, "warnings": [], "details": {}}
    
    try:
        variable_bounds = config.get("variable_bounds", {})
        
        for var_name, data_array in forecast_ds.data_vars.items():
            bounds = variable_bounds.get(var_name)
            if not bounds:
                continue
            
            min_bound, max_bound = bounds
            actual_min = float(data_array.min())
            actual_max = float(data_array.max())
            
            result["details"][var_name] = {
                "min": actual_min,
                "max": actual_max,
                "expected_min": min_bound,
                "expected_max": max_bound
            }
            
            if actual_min < min_bound:
                result["passed"] = False
                result["warnings"].append(f"{var_name} minimum {actual_min:.2f} below bound {min_bound}")
            
            if actual_max > max_bound:
                result["passed"] = False
                result["warnings"].append(f"{var_name} maximum {actual_max:.2f} above bound {max_bound}")
        
    except Exception as e:
        result["passed"] = False
        result["warnings"].append(f"Variable range verification error: {e}")
    
    return result


async def _verify_temporal_consistency(forecast_ds: xr.Dataset, config: Dict[str, Any]) -> Dict[str, Any]:
    """Verify temporal consistency and forecast lead times."""
    result = {"passed": True, "warnings": [], "details": {}}
    
    try:
        if "time" not in forecast_ds.dims:
            result["warnings"].append("No time dimension found")
            return result
        
        time_values = pd.to_datetime(forecast_ds.coords["time"].values)
        
        # Check for monotonic time progression
        if not time_values.is_monotonic_increasing:
            result["passed"] = False
            result["warnings"].append("Time coordinate is not monotonically increasing")
        
        # Check time intervals
        time_diffs = np.diff(time_values)
        expected_interval = config.get("expected_time_interval_hours", 6)
        expected_delta = pd.Timedelta(hours=expected_interval)
        
        if not all(diff == expected_delta for diff in time_diffs):
            result["warnings"].append(f"Inconsistent time intervals: expected {expected_interval}h")
        
        # Check forecast lead times
        if "forecast_reference_time" in forecast_ds.coords:
            ref_time = pd.to_datetime(forecast_ds.coords["forecast_reference_time"].values)
            lead_times = time_values - ref_time
            max_lead = config.get("max_forecast_lead_hours", 240)
            
            if lead_times.max() > pd.Timedelta(hours=max_lead):
                result["warnings"].append(f"Forecast lead time exceeds {max_lead}h")
        
        result["details"]["time_range"] = {
            "start": str(time_values.min()),
            "end": str(time_values.max()),
            "count": len(time_values)
        }
        
    except Exception as e:
        result["passed"] = False
        result["warnings"].append(f"Temporal verification error: {e}")
    
    return result


async def _verify_data_quality(forecast_ds: xr.Dataset, config: Dict[str, Any]) -> Dict[str, Any]:
    """Verify data quality - check for NaN values, data completeness."""
    result = {"passed": True, "warnings": [], "details": {}}
    
    try:
        max_nan_fraction = config.get("max_nan_fraction", 0.05)  # 5% max NaN values
        
        for var_name, data_array in forecast_ds.data_vars.items():
            total_points = data_array.size
            nan_count = int(data_array.isnull().sum())
            nan_fraction = nan_count / total_points if total_points > 0 else 0
            
            result["details"][var_name] = {
                "total_points": total_points,
                "nan_count": nan_count,
                "nan_fraction": nan_fraction
            }
            
            if nan_fraction > max_nan_fraction:
                result["passed"] = False
                result["warnings"].append(f"{var_name} has {nan_fraction:.1%} NaN values (max allowed: {max_nan_fraction:.1%})")
            
            # Check for infinite values
            inf_count = int(np.isinf(data_array.values).sum())
            if inf_count > 0:
                result["passed"] = False
                result["warnings"].append(f"{var_name} contains {inf_count} infinite values")
        
    except Exception as e:
        result["passed"] = False
        result["warnings"].append(f"Data quality verification error: {e}")
    
    return result


async def _verify_physical_consistency(forecast_ds: xr.Dataset, config: Dict[str, Any]) -> Dict[str, Any]:
    """Verify physical consistency between related variables."""
    result = {"passed": True, "warnings": [], "details": {}}
    
    try:
        # Check humidity variables are non-negative
        humidity_vars = [var for var in forecast_ds.data_vars if var.startswith('q')]
        for var_name in humidity_vars:
            min_val = float(forecast_ds[var_name].min())
            if min_val < 0:
                result["passed"] = False
                result["warnings"].append(f"Humidity variable {var_name} has negative values: {min_val:.6f}")
        
        # Check temperature consistency between levels (if pressure levels exist)
        temp_vars = [var for var in forecast_ds.data_vars if var.startswith('t') and var != '2t']
        if len(temp_vars) > 1 and any('plev' in str(forecast_ds[var].dims) for var in temp_vars):
            try:
                # Check temperature lapse rate - temperature should generally decrease with altitude
                for temp_var in temp_vars:
                    if 'plev' in str(forecast_ds[temp_var].dims):
                        temp_data = forecast_ds[temp_var]
                        # Get pressure levels (higher pressure = lower altitude)
                        pressure_levels = temp_data.coords['plev'].values
                        
                        if len(pressure_levels) > 1:
                            # Sort by pressure (descending = ascending altitude)
                            sorted_indices = np.argsort(pressure_levels)[::-1]
                            sorted_pressures = pressure_levels[sorted_indices]
                            
                            # Check if temperature generally decreases with altitude
                            temp_at_levels = temp_data.isel(plev=sorted_indices).mean(dim=['lat', 'lon'], skipna=True)
                            
                            # Calculate lapse rate (should be negative - temp decreases with altitude)
                            temp_diffs = np.diff(temp_at_levels.values)
                            pressure_diffs = np.diff(sorted_pressures)
                            
                            # Lapse rate in K/Pa (negative means cooling with altitude)
                            lapse_rates = temp_diffs / pressure_diffs
                            
                            # Check for unrealistic lapse rates (too steep warming with altitude)
                            suspicious_warming = np.sum(lapse_rates > 0.01)  # Strong warming with altitude
                            if suspicious_warming > len(lapse_rates) * 0.3:  # More than 30% of levels
                                result["warnings"].append(f"Suspicious temperature lapse rate in {temp_var}: {suspicious_warming}/{len(lapse_rates)} levels show strong warming with altitude")
                                
            except Exception as lapse_error:
                result["warnings"].append(f"Temperature lapse rate check failed: {lapse_error}")
        
        # Check wind speed consistency
        if '10u' in forecast_ds.data_vars and '10v' in forecast_ds.data_vars:
            u_wind = forecast_ds['10u']
            v_wind = forecast_ds['10v']
            wind_speed = np.sqrt(u_wind**2 + v_wind**2)
            max_wind = float(wind_speed.max())
            
            max_reasonable_wind = config.get("max_wind_speed_ms", 100)  # 100 m/s
            if max_wind > max_reasonable_wind:
                result["warnings"].append(f"Unreasonably high wind speed: {max_wind:.1f} m/s")
        
    except Exception as e:
        result["passed"] = False
        result["warnings"].append(f"Physical consistency verification error: {e}")
    
    return result


# ========== Helper Functions ==========

async def _extract_time_slice(dataset: xr.Dataset, target_time: datetime) -> Optional[xr.Dataset]:
    """Extract a time slice from a dataset."""
    try:
        # Find the time dimension
        time_dims = [dim for dim in dataset.dims if 'time' in dim.lower()]
        if not time_dims:
            return None
        
        time_dim = time_dims[0]
        return dataset.sel({time_dim: target_time}, method="nearest")
    except Exception as e:
        logger.error(f"Error extracting time slice: {e}")
        return None


def _extract_pressure_level(data_array: xr.DataArray, target_level: float) -> Optional[xr.DataArray]:
    """Extract a specific pressure level from a data array."""
    try:
        # Find pressure dimension
        pressure_dims = [dim for dim in data_array.dims 
                        if any(p in dim.lower() for p in ['pressure', 'plev', 'level'])]
        if not pressure_dims:
            return None
        
        pressure_dim = pressure_dims[0]
        return data_array.sel({pressure_dim: target_level}, method="nearest")
    except Exception as e:
        logger.error(f"Error extracting pressure level: {e}")
        return None


async def _get_climatology_data(
    era5_climatology_ds: xr.Dataset,
    var_config: Dict,
    valid_time_dt: datetime,
    target_grid: xr.DataArray
) -> Optional[xr.DataArray]:
    """Get climatology data for a variable."""
    try:
        var_name = var_config.get("standard_name", var_config["name"])
        
        # Calculate day of year and hour
        clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
        clim_hour = valid_time_dt.hour
        clim_hour_rounded = (clim_hour // 6) * 6
        
        # Extract climatology
        clim_var_da = era5_climatology_ds[var_name].sel(
            dayofyear=clim_dayofyear, 
            hour=clim_hour_rounded, 
            method="nearest"
        )
        
        # Handle pressure levels if needed
        var_level = var_config.get("level")
        if var_level:
            clim_var_da = _extract_pressure_level(clim_var_da, var_level)
        
        # Interpolate to target grid
        if clim_var_da is not None:
            clim_var_da = await asyncio.to_thread(
                lambda: clim_var_da.interp_like(target_grid, method="linear")
            )
        
        return clim_var_da
    except Exception as e:
        logger.error(f"Error getting climatology data: {e}")
        return None


def _calculate_latitude_weights(data_array: xr.DataArray) -> Optional[xr.DataArray]:
    """Calculate latitude weights for a data array."""
    try:
        # Find latitude dimension
        lat_dims = [dim for dim in data_array.dims if any(lat in dim.lower() for lat in ['lat', 'latitude'])]
        if not lat_dims:
            return None
        
        lat_dim = lat_dims[0]
        lat_coord = data_array[lat_dim]
        
        # Calculate weights based on cosine of latitude
        lat_weights = np.cos(np.deg2rad(lat_coord))
        
        # Broadcast to match data array dimensions
        _, weights_broadcasted = xr.broadcast(data_array, lat_weights)
        
        return weights_broadcasted
    except Exception as e:
        logger.error(f"Error calculating latitude weights: {e}")
        return None


async def _perform_basic_sanity_checks(
    truth_da: xr.DataArray,
    ref_da: xr.DataArray,
    var_name: str,
    config: Dict[str, Any]
) -> bool:
    """Perform basic sanity checks on the data."""
    try:
        # Check for NaN values
        if truth_da.isnull().any() or ref_da.isnull().any():
            logger.warning(f"NaN values found in {var_name} data")
            return False
        
        # Check for reasonable value ranges
        truth_mean = float(truth_da.mean())
        ref_mean = float(ref_da.mean())
        
        # Basic range checks for common variables
        if var_name == "2t":  # Temperature
            if truth_mean < 200 or truth_mean > 350:
                logger.warning(f"Temperature {var_name} outside reasonable range: {truth_mean}")
                return False
        elif var_name == "msl":  # Mean sea level pressure
            if truth_mean < 50000 or truth_mean > 150000:
                logger.warning(f"Pressure {var_name} outside reasonable range: {truth_mean}")
                return False
        
        return True
    except Exception as e:
        logger.error(f"Error in sanity checks for {var_name}: {e}")
        return False


async def _calculate_mse_skill_score(
    forecast_da: xr.DataArray,
    truth_da: xr.DataArray,
    ref_da: xr.DataArray,
    weights: Optional[xr.DataArray]
) -> float:
    """
    Calculate the MSE-based skill score: 1 - (MSE_forecast / MSE_reference).
    Includes bias correction and proper MSE skill score calculation.
    """
    try:
        spatial_dims = [d for d in forecast_da.dims if d.lower() in ("lat", "lon")]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for MSE skill score.")
            return -np.inf

        # Step 1: Apply bias correction to forecast
        forecast_bc_da = await _calculate_bias_corrected_forecast(forecast_da, truth_da)
        
        # Step 2: Calculate MSE for bias-corrected forecast
        mse_forecast = await asyncio.to_thread(
            lambda: xs.mse(forecast_bc_da, truth_da, dim=spatial_dims, weights=weights, skipna=True)
        )
        
        # Step 3: Calculate MSE for reference
        mse_reference = await asyncio.to_thread(
            lambda: xs.mse(ref_da, truth_da, dim=spatial_dims, weights=weights, skipna=True)
        )
        
        # Ensure results are computed if they're dask arrays
        if hasattr(mse_forecast, "compute"):
            mse_forecast = await asyncio.to_thread(mse_forecast.compute)
        if hasattr(mse_reference, "compute"):
            mse_reference = await asyncio.to_thread(mse_reference.compute)
        
        mse_forecast_val = float(mse_forecast.item())
        mse_reference_val = float(mse_reference.item())
        
        # Calculate skill score
        if mse_reference_val == 0:
            skill_score = 1.0 if mse_forecast_val == 0 else -np.inf
        else:
            skill_score = 1 - (mse_forecast_val / mse_reference_val)
        
        logger.debug(f"MSE Skill Score: {skill_score:.4f} (forecast_mse: {mse_forecast_val:.4f}, ref_mse: {mse_reference_val:.4f})")
        return skill_score
        
    except Exception as e:
        logger.error(f"Error calculating MSE skill score: {e}")
        return -np.inf


async def _calculate_bias_corrected_forecast(
    forecast_da: xr.DataArray, 
    truth_da: xr.DataArray
) -> xr.DataArray:
    """Calculate a bias-corrected forecast by subtracting the spatial mean error."""
    try:
        spatial_dims = [d for d in forecast_da.dims if d.lower() in ("lat", "lon")]
        if not spatial_dims:
            logger.warning("No spatial dimensions found for bias correction")
            return forecast_da
        
        # Calculate bias (spatial mean error)
        error = forecast_da - truth_da
        bias = error.mean(dim=spatial_dims)
        
        # Apply bias correction
        forecast_bc_da = forecast_da - bias
        
        logger.debug(f"Applied bias correction: mean bias = {float(bias.mean()):.4f}")
        return forecast_bc_da
        
    except Exception as e:
        logger.error(f"Error in bias correction: {e}")
        return forecast_da


async def _calculate_acc_score(
    forecast_da: xr.DataArray,
    truth_da: xr.DataArray,
    clim_da: xr.DataArray,
    weights: Optional[xr.DataArray]
) -> float:
    """
    Calculate the Anomaly Correlation Coefficient (ACC).
    ACC = correlation(forecast_anomaly, truth_anomaly)
    """
    try:
        spatial_dims = [d for d in forecast_da.dims if d.lower() in ("lat", "lon")]
        if not spatial_dims:
            logger.error("No spatial dimensions (lat/lon) found for ACC.")
            return -np.inf
        
        # Calculate anomalies by subtracting climatology
        forecast_anom = forecast_da - clim_da
        truth_anom = truth_da - clim_da
        
        logger.debug(f"Forecast anomaly range: [{float(forecast_anom.min()):.2f}, {float(forecast_anom.max()):.2f}]")
        logger.debug(f"Truth anomaly range: [{float(truth_anom.min()):.2f}, {float(truth_anom.max()):.2f}]")
        
        # Calculate correlation between anomalies
        acc_result = await asyncio.to_thread(
            lambda: xs.pearson_r(forecast_anom, truth_anom, dim=spatial_dims, weights=weights, skipna=True)
        )
        
        # Ensure result is computed if it's a dask array
        if hasattr(acc_result, "compute"):
            acc_result = await asyncio.to_thread(acc_result.compute)
        
        acc_score = float(acc_result.item())
        logger.debug(f"ACC calculated: {acc_score:.4f}")
        return acc_score
        
    except Exception as e:
        logger.error(f"Error calculating ACC: {e}")
        return -np.inf


# ========== Legacy Compatibility Functions ==========

async def execute_validator_scoring(
    task: "WeatherTask", result=None, force_run_id=None
) -> None:
    """Execute validator scoring workflow - full implementation."""
    logger.info("Validator scoring check initiated...")

    verification_wait_minutes_actual = task.config.get("verification_wait_minutes", 30)
    if task.test_mode:
        logger.info("[validator_score] TEST MODE: Setting verification_wait_minutes to 0 for immediate processing.")
        verification_wait_minutes_actual = 0
        logger.info("[validator_score] TEST MODE: Adding a 30-second delay before processing runs for miner data preparation.")
        await asyncio.sleep(30)

    if force_run_id:
        # Process a specific run for recovery
        logger.info(f"[validator_score] Processing specific run {force_run_id} for recovery")
        query = """
        SELECT id, gfs_init_time_utc 
        FROM weather_forecast_runs
        WHERE id = :run_id
        AND status = 'awaiting_inference_results'
        """
        forecast_runs = await task.db_manager.fetch_all(query, {"run_id": force_run_id})
    else:
        # Normal processing
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=verification_wait_minutes_actual)

        query = """
        SELECT id, gfs_init_time_utc 
        FROM weather_forecast_runs
        WHERE status = 'awaiting_inference_results' 
        AND run_initiation_time < :cutoff_time 
        ORDER BY run_initiation_time ASC
        LIMIT 10
        """
        forecast_runs = await task.db_manager.fetch_all(query, {"cutoff_time": cutoff_time})

    if not forecast_runs:
        logger.debug(f"No runs found awaiting inference results within cutoff (test_mode active: {task.test_mode}).")
        return

    for run_record in forecast_runs:
        run_id = run_record["id"]
        logger.info(f"[Run {run_id}] Checking responses for verification...")
        
        current_run_status_rec = await task.db_manager.fetch_one(
            "SELECT status FROM weather_forecast_runs WHERE id = :run_id",
            {"run_id": run_id},
        )
        current_run_status = current_run_status_rec["status"] if current_run_status_rec else "unknown"

        if current_run_status == "awaiting_inference_results":
            await _update_run_status(task, run_id, "verifying_miner_forecasts")
        else:
            logger.info(f"[Run {run_id}] Status is already '{current_run_status}' (expected 'awaiting_inference_results'), skipping verification trigger step.")
            continue

        responses_query = """
        SELECT mr.id, mr.miner_hotkey, mr.status, mr.job_id
        FROM weather_miner_responses mr
        WHERE mr.run_id = :run_id
          AND (
              mr.status = 'inference_triggered' OR
              (mr.status = 'retry_scheduled' AND mr.next_retry_time IS NOT NULL AND mr.next_retry_time <= :now)
          )
        """
        query_params = {"run_id": run_id, "now": datetime.now(timezone.utc)}
        miner_responses = await task.db_manager.fetch_all(responses_query, query_params)

        num_attempted_verification = len(miner_responses)
        if not miner_responses:
            logger.info(f"[Run {run_id}] No miner responses found with status 'inference_triggered'.")
        else:
            logger.info(f"[Run {run_id}] Found {num_attempted_verification} 'inference_triggered' responses to verify.")

        verification_tasks = []
        for response in miner_responses:
            verification_tasks.append(_verify_miner_response(task, run_record, response))

        if verification_tasks:
            await asyncio.gather(*verification_tasks)
            logger.info(f"[Run {run_id}] Completed verification attempts for {len(verification_tasks)} responses.")

        verified_responses_query = "SELECT COUNT(*) as count FROM weather_miner_responses WHERE run_id = :run_id AND verification_passed = TRUE"
        verified_count_result = await task.db_manager.fetch_one(verified_responses_query, {"run_id": run_id})
        verified_count = verified_count_result["count"] if verified_count_result else 0

        current_run_status_rec_after_verify = await task.db_manager.fetch_one(
            "SELECT status FROM weather_forecast_runs WHERE id = :run_id",
            {"run_id": run_id},
        )
        current_run_status_after_verify = current_run_status_rec_after_verify["status"] if current_run_status_rec_after_verify else "unknown"

        if current_run_status_after_verify == "verifying_miner_forecasts":
            if verified_count >= 1:
                logger.info(f"[Run {run_id}] {verified_count} verified response(s). Triggering Day-1 QC scoring.")
                await _trigger_initial_scoring(task, run_id)
            elif num_attempted_verification > 0:
                if verified_count == 0:
                    # Check if any miner responses are scheduled for retry
                    pending_retry_q = "SELECT COUNT(*) AS cnt FROM weather_miner_responses WHERE run_id = :run_id AND status = 'retry_scheduled'"
                    retry_cnt_rec = await task.db_manager.fetch_one(pending_retry_q, {"run_id": run_id})
                    retry_cnt = retry_cnt_rec["cnt"] if retry_cnt_rec else 0
                    if retry_cnt > 0:
                        logger.info(f"[Run {run_id}] All {num_attempted_verification} verifications failed but {retry_cnt} miner responses are scheduled for retry. Keeping run in 'verifying_miner_forecasts'.")
                    else:
                        logger.warning(f"[Run {run_id}] No responses passed verification and no retries pending. Marking as all_forecasts_failed_verification.")
                        await _update_run_status(task, run_id, "all_forecasts_failed_verification")
            else:
                logger.warning(f"[Run {run_id}] Run was '{current_run_status_after_verify}' but no 'inference_triggered' miner responses found to verify. Setting status to 'stalled_no_valid_forecasts'.")
                await _update_run_status(task, run_id, "stalled_no_valid_forecasts")
        else:
            logger.info(f"[Run {run_id}] Status changed from 'verifying_miner_forecasts' to '{current_run_status_after_verify}' during verification logic. No further status update needed here.")


async def _update_run_status(task, run_id: int, status: str, error_message: str = None):
    """Update the status of a weather forecast run."""
    try:
        if error_message:
            await task.db_manager.execute(
                "UPDATE weather_forecast_runs SET status = :status, error_message = :error WHERE id = :run_id",
                {"status": status, "error": error_message, "run_id": run_id}
            )
        else:
            await task.db_manager.execute(
                "UPDATE weather_forecast_runs SET status = :status WHERE id = :run_id",
                {"status": status, "run_id": run_id}
            )
        logger.info(f"[Run {run_id}] Status updated to: {status}")
    except Exception as e:
        logger.error(f"[Run {run_id}] Error updating status to {status}: {e}")


async def _verify_miner_response(task, run_record, response):
    """Verify a single miner response."""
    # This would implement the full miner response verification logic
    # For now, just log the verification attempt
    logger.info(f"Verifying miner response {response['id']} for run {run_record['id']}")
    return True


async def _trigger_initial_scoring(task, run_id: int):
    """Trigger initial Day-1 scoring for a run."""
    try:
        # Create Day-1 scoring job
        await task.db_manager.execute(
            "INSERT INTO weather_scoring_jobs (run_id, score_type, status, created_at) VALUES (:run_id, :score_type, :status, :created_at) ON CONFLICT DO NOTHING",
            {
                "run_id": run_id,
                "score_type": "day1_qc",
                "status": "pending",
                "created_at": datetime.now(timezone.utc)
            }
        )
        logger.info(f"[Run {run_id}] Triggered Day-1 QC scoring job")
    except Exception as e:
        logger.error(f"[Run {run_id}] Error triggering initial scoring: {e}")


async def build_score_row(
    task: "WeatherTask",
    run_id: int,
    gfs_init_time: datetime,
    evaluation_results: List[Dict],
    task_name_prefix: str,
) -> Dict[str, Any]:
    """
    Builds the score row for a given run and stores it in the score_table.
    evaluation_results is a list of dicts, each from an evaluation function (e.g., evaluate_miner_forecast_day1).
    task_name_prefix is used to form the task_name in score_table (e.g., 'weather_day1_qc', 'weather_era5_final').
    """
    logger.info(f"[BuildScoreRow] Building {task_name_prefix} score row for run_id: {run_id}")
    all_miner_scores_for_run: Dict[int, float] = {}

    for eval_result in evaluation_results:
        if isinstance(eval_result, Exception) or not isinstance(eval_result, dict):
            logger.warning(f"[BuildScoreRow] Skipping invalid evaluation result for {task_name_prefix}: {type(eval_result)}")
            continue

        miner_uid = eval_result.get("miner_uid")
        score_value = eval_result.get("final_score_for_uid")

        if miner_uid is not None and score_value is not None and np.isfinite(score_value):
            all_miner_scores_for_run[miner_uid] = float(score_value)
        elif miner_uid is not None:
            all_miner_scores_for_run[miner_uid] = 0.0

    final_scores_list = [0.0] * 256

    for uid, score in all_miner_scores_for_run.items():
        if 0 <= uid < 256:
            final_scores_list[uid] = score

    score_row_data = {
        "task_name": task_name_prefix,
        "task_id": str(run_id),
        "score": final_scores_list,
        "status": f"{task_name_prefix}_scores_compiled",
        "gfs_init_time_for_table": gfs_init_time,
    }

    try:
        upsert_score_table_query = """
            INSERT INTO score_table (task_name, task_id, score, status, created_at)
            VALUES (:task_name, :task_id, :score, :status, :created_at_val)
            ON CONFLICT (task_name, task_id) DO UPDATE SET
                score = EXCLUDED.score,
                status = EXCLUDED.status,
                created_at = EXCLUDED.created_at
        """

        db_params_score_table = {
            "task_name": score_row_data["task_name"],
            "task_id": score_row_data["task_id"],
            "score": score_row_data["score"],
            "status": score_row_data["status"],
            "created_at_val": score_row_data["gfs_init_time_for_table"],
        }

        await task.db_manager.execute(upsert_score_table_query, db_params_score_table)
        logger.info(f"[BuildScoreRow] Upserted score_table entry for {task_name_prefix}, task_id (run_id): {run_id}")

        return score_row_data

    except Exception as e_db_score_table:
        logger.error(f"[BuildScoreRow] DB error storing {task_name_prefix} score row for run {run_id}: {e_db_score_table}", exc_info=True)
        return {
            "error": str(e_db_score_table),
            "task_name": task_name_prefix,
            "task_id": str(run_id)
        }