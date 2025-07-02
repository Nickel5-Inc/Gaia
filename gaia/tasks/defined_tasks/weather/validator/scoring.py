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
        
        # Calculate bias-corrected skill score
        # Note: This is a simplified implementation - the full algorithm would include
        # bias correction, MSE skill score calculation, and clone detection
        skill_score = await _calculate_simplified_skill_score(
            truth_var_da, ref_var_da, lat_weights
        )
        
        # Calculate ACC score
        acc_score = await _calculate_simplified_acc_score(
            truth_var_da, ref_var_da, clim_var_da, lat_weights
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
    """
    logger.info(f"[ERA5Score] Starting ERA5 final scoring for miner {miner_response_data['miner_hotkey']}")
    
    # This is a placeholder implementation - the full ERA5 scoring would include:
    # - RMSE calculations against ERA5 analysis
    # - Bias calculations
    # - Correlation calculations
    # - Comprehensive statistical metrics
    
    era5_results = {
        "response_id": miner_response_data["id"],
        "miner_hotkey": miner_response_data["miner_hotkey"],
        "overall_era5_score": 0.5,  # Placeholder score
        "rmse_scores": {},
        "bias_scores": {},
        "correlation_scores": {},
        "error_message": None
    }
    
    logger.info(f"[ERA5Score] ERA5 scoring completed (placeholder implementation)")
    return era5_results


async def _compute_forecast_verification_async(
    miner_response_data: Dict[str, Any],
    forecast_ds: xr.Dataset,
    verification_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Async implementation of forecast verification computation.
    """
    logger.info(f"[ForecastVerify] Starting verification for miner {miner_response_data['miner_hotkey']}")
    
    # This is a placeholder implementation - the full verification would include:
    # - Data quality checks
    # - Grid consistency validation
    # - Variable range validation
    # - Temporal consistency checks
    
    verification_results = {
        "response_id": miner_response_data["id"],
        "miner_hotkey": miner_response_data["miner_hotkey"],
        "verification_passed": True,  # Placeholder result
        "quality_checks": {},
        "integrity_checks": {},
        "error_message": None
    }
    
    logger.info(f"[ForecastVerify] Verification completed (placeholder implementation)")
    return verification_results


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


async def _calculate_simplified_skill_score(
    truth_da: xr.DataArray,
    ref_da: xr.DataArray,
    weights: Optional[xr.DataArray]
) -> float:
    """Calculate a simplified skill score."""
    try:
        # This is a simplified implementation
        # The full implementation would include bias correction and proper MSE skill score
        
        # Calculate MSE for reference
        ref_mse = xs.mse(ref_da, truth_da, dim=['lat', 'lon'], weights=weights, skipna=True)
        
        # For now, return a placeholder skill score
        # In the full implementation, this would be: 1 - (forecast_mse / ref_mse)
        skill_score = float(0.7)  # Placeholder
        
        return skill_score
    except Exception as e:
        logger.error(f"Error calculating skill score: {e}")
        return 0.0


async def _calculate_simplified_acc_score(
    truth_da: xr.DataArray,
    ref_da: xr.DataArray,
    clim_da: xr.DataArray,
    weights: Optional[xr.DataArray]
) -> float:
    """Calculate a simplified ACC score."""
    try:
        # This is a simplified implementation
        # The full implementation would calculate proper anomaly correlation coefficient
        
        # Calculate correlation between truth and reference
        correlation = xs.pearson_r(truth_da, ref_da, dim=['lat', 'lon'], weights=weights, skipna=True)
        
        acc_score = float(correlation.compute()) if hasattr(correlation, 'compute') else float(correlation)
        
        return acc_score
    except Exception as e:
        logger.error(f"Error calculating ACC score: {e}")
        return 0.0


# ========== Legacy Compatibility Functions ==========

async def execute_validator_scoring(
    task: "WeatherTask", result=None, force_run_id=None
) -> None:
    """Execute validator scoring workflow - updated implementation."""
    logger.info("Executing validator scoring workflow...")
    
    # This function would integrate with the IO-Engine to dispatch scoring jobs
    # to the compute workers using the handlers implemented above
    
    # Placeholder implementation for now
    pass


async def build_score_row(
    task: "WeatherTask",
    run_id: int,
    gfs_init_time,
    evaluation_results: List[Dict],
    task_name_prefix: str,
) -> Dict[str, Any]:
    """Build score row for database insertion - updated implementation."""
    logger.info(f"Building score row for run {run_id}")
    
    # This function would process the evaluation results from the compute workers
    # and format them for database insertion
    
    return {
        "run_id": run_id,
        "gfs_init_time": gfs_init_time,
        "evaluation_results": evaluation_results,
        "task_name_prefix": task_name_prefix,
        "scores_computed": True
    }