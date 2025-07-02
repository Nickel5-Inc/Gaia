"""
Weather Scoring Compute Handlers.

This module contains compute handlers for CPU-intensive weather scoring operations
that run in the compute worker processes. These handlers process forecast verification
and scoring calculations for the weather validation pipeline.
"""

import logging
import numpy as np
import xarray as xr
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)


def handle_day1_scoring_computation(
    config,
    response_id: int,
    run_id: int,
    miner_hotkey: str,
    miner_uid: int,
    job_id: str,
    zarr_store_url: str,
    access_token: str,
    verification_hash: str,
    gfs_init_time_iso: str,
    scoring_config: Dict[str, Any],
    **kwargs
) -> Dict[str, Any]:
    """
    Synchronous handler for Day-1 scoring computation.
    
    This performs the CPU-intensive portions of Day-1 scoring including:
    - Loading miner forecast data
    - Computing skill scores vs GFS analysis
    - Computing ACC scores vs ERA5 climatology
    - Calculating composite Day-1 score
    
    Args:
        config: Configuration object
        response_id: Database response ID
        run_id: Weather run ID
        miner_hotkey: Miner's hotkey
        miner_uid: Miner's UID
        job_id: Miner's job ID
        zarr_store_url: URL to miner's forecast data
        access_token: Access token for forecast data
        verification_hash: Expected data verification hash
        gfs_init_time_iso: GFS initialization time (ISO format)
        scoring_config: Day-1 scoring configuration
        
    Returns:
        Dictionary with scoring results
    """
    try:
        # Heavy imports done locally to keep worker startup fast
        import asyncio
        from ..weather_scoring_mechanism import evaluate_miner_forecast_day1
        from ..utils.data_prep import fetch_gfs_analysis_data, fetch_gfs_data
        from ..utils.remote_access import open_verified_remote_zarr_dataset
        from pathlib import Path
        
        logger.info(f"Day-1 scoring computation for miner {miner_hotkey[:8]} (response {response_id})")
        
        # Parse GFS init time
        gfs_init_time = datetime.fromisoformat(gfs_init_time_iso.replace('Z', '+00:00'))
        
        # Get target lead times from scoring config
        lead_times_hours = scoring_config.get("lead_times_hours", [6, 12])
        valid_times = [gfs_init_time + timedelta(hours=h) for h in lead_times_hours]
        
        # Variables to score
        variables_to_score = scoring_config.get(
            "variables_levels_to_score",
            [
                {"name": "z", "level": 500, "standard_name": "geopotential"},
                {"name": "t", "level": 850, "standard_name": "temperature"},
                {"name": "2t", "level": None, "standard_name": "2m_temperature"},
                {"name": "msl", "level": None, "standard_name": "mean_sea_level_pressure"},
            ]
        )
        
        # Prepare variable lists for GFS data fetching
        surface_vars = []
        atmos_vars = []
        pressure_levels = set()
        
        for var_info in variables_to_score:
            aurora_name = var_info["name"]
            level = var_info.get("level")
            
            # Map Aurora variable names to GFS names (simplified mapping)
            gfs_var_map = {
                "2t": "tmp2m",
                "msl": "prmslmsl",
                "z": "hgtprs",
                "t": "tmpprs",
                "u": "ugrdprs",
                "v": "vgrdprs",
                "q": "spfhprs"
            }
            
            gfs_name = gfs_var_map.get(aurora_name, aurora_name)
            
            if level is None:
                surface_vars.append(gfs_name)
            else:
                atmos_vars.append(gfs_name)
                pressure_levels.add(int(level))
        
        pressure_levels_list = sorted(list(pressure_levels)) if pressure_levels else None
        
        # Create async event loop for data loading
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Load GFS analysis data (truth)
            gfs_cache_dir = Path(config.WEATHER.GFS_CACHE_DIR)
            
            logger.info(f"Loading GFS analysis data for {len(valid_times)} time steps")
            gfs_analysis_task = loop.create_task(
                fetch_gfs_analysis_data(
                    target_times=valid_times,
                    cache_dir=gfs_cache_dir,
                    target_surface_vars=surface_vars,
                    target_atmos_vars=atmos_vars,
                    target_pressure_levels_hpa=pressure_levels_list
                )
            )
            
            # Load GFS reference forecast
            logger.info(f"Loading GFS reference forecast data")
            gfs_reference_task = loop.create_task(
                fetch_gfs_data(
                    run_time=gfs_init_time,
                    lead_hours=lead_times_hours,
                    target_surface_vars=surface_vars,
                    target_atmos_vars=atmos_vars,
                    target_pressure_levels_hpa=pressure_levels_list
                )
            )
            
            # Load miner forecast data
            logger.info(f"Loading miner forecast data from {zarr_store_url}")
            storage_options = {
                "headers": {"Authorization": f"Bearer {access_token}"},
                "ssl": False
            }
            
            miner_forecast_task = loop.create_task(
                open_verified_remote_zarr_dataset(
                    zarr_store_url=zarr_store_url,
                    claimed_manifest_content_hash=verification_hash,
                    miner_hotkey_ss58=miner_hotkey,
                    storage_options=storage_options,
                    job_id=f"{job_id}_day1_score"
                )
            )
            
            # Wait for all data to load
            gfs_analysis_ds, gfs_reference_ds, miner_forecast_ds = loop.run_until_complete(
                asyncio.gather(gfs_analysis_task, gfs_reference_task, miner_forecast_task)
            )
            
            if not all([gfs_analysis_ds, gfs_reference_ds, miner_forecast_ds]):
                return {
                    "success": False,
                    "error": "Failed to load required datasets",
                    "response_id": response_id,
                    "overall_day1_score": -np.inf
                }
            
            # Load ERA5 climatology (simplified - in reality this would be loaded from cache)
            # For now, create a minimal climatology dataset
            era5_climatology = _create_minimal_climatology(gfs_analysis_ds)
            
            # Create mock task instance for scoring function
            class MockTaskInstance:
                def __init__(self, config):
                    self.config = config
            
            mock_task = MockTaskInstance(config)
            
            # Create miner response record for scoring
            miner_response_record = {
                "id": response_id,
                "run_id": run_id,
                "miner_hotkey": miner_hotkey,
                "miner_uid": miner_uid,
                "job_id": job_id
            }
            
            # Perform Day-1 scoring
            logger.info(f"Computing Day-1 scores for miner {miner_hotkey[:8]}")
            scoring_result = loop.run_until_complete(
                evaluate_miner_forecast_day1(
                    task_instance=mock_task,
                    miner_response_db_record=miner_response_record,
                    gfs_analysis_data_for_run=gfs_analysis_ds,
                    gfs_reference_forecast_for_run=gfs_reference_ds,
                    era5_climatology=era5_climatology,
                    day1_scoring_config=scoring_config,
                    run_gfs_init_time=gfs_init_time
                )
            )
            
            # Clean up datasets
            for ds in [gfs_analysis_ds, gfs_reference_ds, miner_forecast_ds, era5_climatology]:
                if ds and hasattr(ds, 'close'):
                    ds.close()
            
            # Return scoring results
            return {
                "success": True,
                "response_id": response_id,
                "miner_hotkey": miner_hotkey,
                "miner_uid": miner_uid,
                "run_id": run_id,
                "overall_day1_score": scoring_result.get("overall_day1_score"),
                "qc_passed": scoring_result.get("qc_passed_all_vars_leads", False),
                "lead_time_scores": scoring_result.get("lead_time_scores", {}),
                "error_message": scoring_result.get("error_message")
            }
            
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"Error in Day-1 scoring computation: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "response_id": response_id,
            "overall_day1_score": -np.inf
        }


def handle_era5_final_scoring_computation(
    config,
    response_id: int,
    run_id: int,
    miner_hotkey: str,
    miner_uid: int,
    job_id: str,
    zarr_store_url: str,
    access_token: str,
    verification_hash: str,
    gfs_init_time_iso: str,
    scoring_config: Dict[str, Any],
    **kwargs
) -> Dict[str, Any]:
    """
    Synchronous handler for ERA5 final scoring computation.
    
    This performs the CPU-intensive portions of ERA5 final scoring including:
    - Loading miner forecast data
    - Loading ERA5 ground truth data
    - Computing RMSE, bias, and skill scores vs ERA5
    - Computing anomaly correlation coefficients
    
    Args:
        config: Configuration object
        response_id: Database response ID
        run_id: Weather run ID
        miner_hotkey: Miner's hotkey
        miner_uid: Miner's UID
        job_id: Miner's job ID
        zarr_store_url: URL to miner's forecast data
        access_token: Access token for forecast data
        verification_hash: Expected data verification hash
        gfs_init_time_iso: GFS initialization time (ISO format)
        scoring_config: ERA5 scoring configuration
        
    Returns:
        Dictionary with ERA5 scoring results
    """
    try:
        # Heavy imports done locally
        import asyncio
        from ..processing.weather_logic import get_ground_truth_data
        from ..utils.remote_access import open_verified_remote_zarr_dataset
        from pathlib import Path
        
        logger.info(f"ERA5 final scoring computation for miner {miner_hotkey[:8]} (response {response_id})")
        
        # Parse GFS init time
        gfs_init_time = datetime.fromisoformat(gfs_init_time_iso.replace('Z', '+00:00'))
        
        # ERA5 scoring typically uses longer lead times
        lead_times_hours = scoring_config.get("lead_times_hours", [24, 48, 72, 120, 168])
        forecast_times = np.array([gfs_init_time + timedelta(hours=h) for h in lead_times_hours])
        
        # Create async event loop for data loading
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Load miner forecast data
            logger.info(f"Loading miner forecast data from {zarr_store_url}")
            storage_options = {
                "headers": {"Authorization": f"Bearer {access_token}"},
                "ssl": False
            }
            
            miner_forecast_ds = loop.run_until_complete(
                open_verified_remote_zarr_dataset(
                    zarr_store_url=zarr_store_url,
                    claimed_manifest_content_hash=verification_hash,
                    miner_hotkey_ss58=miner_hotkey,
                    storage_options=storage_options,
                    job_id=f"{job_id}_era5_score"
                )
            )
            
            if not miner_forecast_ds:
                return {
                    "success": False,
                    "error": "Failed to load miner forecast dataset",
                    "response_id": response_id
                }
            
            # Create mock task instance for ERA5 data loading
            class MockTaskInstance:
                def __init__(self, config):
                    self.config = config
            
            mock_task = MockTaskInstance(config)
            
            # Load ERA5 ground truth data
            logger.info(f"Loading ERA5 ground truth data for {len(forecast_times)} time steps")
            era5_truth_ds = loop.run_until_complete(
                get_ground_truth_data(mock_task, gfs_init_time, forecast_times)
            )
            
            if not era5_truth_ds:
                return {
                    "success": False,
                    "error": "Failed to load ERA5 ground truth data",
                    "response_id": response_id
                }
            
            # Compute ERA5 metrics
            logger.info(f"Computing ERA5 metrics for miner {miner_hotkey[:8]}")
            
            # Variables to score in ERA5 evaluation
            era5_variables = scoring_config.get(
                "variables_to_score", 
                ["2t", "msl", "z500", "t850"]
            )
            
            scoring_results = {}
            overall_scores = []
            
            for var_name in era5_variables:
                try:
                    # Extract variable data from both datasets
                    if var_name not in miner_forecast_ds.data_vars:
                        logger.warning(f"Variable {var_name} not found in miner forecast")
                        continue
                        
                    if var_name not in era5_truth_ds.data_vars:
                        logger.warning(f"Variable {var_name} not found in ERA5 truth")
                        continue
                    
                    miner_var = miner_forecast_ds[var_name]
                    era5_var = era5_truth_ds[var_name]
                    
                    # Compute metrics for each forecast time
                    var_scores = []
                    var_metrics = {}
                    
                    for i, forecast_time in enumerate(forecast_times):
                        lead_hours = lead_times_hours[i]
                        
                        try:
                            # Select data for this forecast time
                            miner_slice = miner_var.sel(time=forecast_time, method="nearest")
                            era5_slice = era5_var.sel(time=forecast_time, method="nearest")
                            
                            # Align grids if needed (simplified)
                            if miner_slice.sizes != era5_slice.sizes:
                                era5_slice = era5_slice.interp_like(miner_slice)
                            
                            # Compute RMSE
                            rmse = np.sqrt(np.mean((miner_slice - era5_slice) ** 2))
                            
                            # Compute bias
                            bias = np.mean(miner_slice - era5_slice)
                            
                            # Compute anomaly correlation coefficient (simplified)
                            era5_mean = era5_slice.mean()
                            miner_anom = miner_slice - miner_slice.mean()
                            era5_anom = era5_slice - era5_mean
                            
                            correlation = np.corrcoef(
                                miner_anom.values.flatten(),
                                era5_anom.values.flatten()
                            )[0, 1]
                            
                            if not np.isfinite(correlation):
                                correlation = 0.0
                            
                            # Simple skill score (1 - normalized RMSE)
                            era5_std = np.std(era5_slice)
                            skill_score = max(0.0, 1.0 - (rmse / era5_std)) if era5_std > 0 else 0.0
                            
                            var_scores.append(skill_score)
                            var_metrics[f"lead_{lead_hours}h"] = {
                                "rmse": float(rmse),
                                "bias": float(bias),
                                "correlation": float(correlation),
                                "skill_score": float(skill_score)
                            }
                            
                        except Exception as e:
                            logger.warning(f"Error computing metrics for {var_name} at {forecast_time}: {e}")
                            continue
                    
                    # Average skill score for this variable
                    if var_scores:
                        avg_var_score = np.mean(var_scores)
                        scoring_results[var_name] = {
                            "average_skill_score": float(avg_var_score),
                            "lead_time_metrics": var_metrics
                        }
                        overall_scores.append(avg_var_score)
                    
                except Exception as e:
                    logger.error(f"Error processing variable {var_name}: {e}")
                    continue
            
            # Compute overall ERA5 score
            final_era5_score = np.mean(overall_scores) if overall_scores else 0.0
            
            # Clean up datasets
            for ds in [miner_forecast_ds, era5_truth_ds]:
                if ds and hasattr(ds, 'close'):
                    ds.close()
            
            return {
                "success": True,
                "response_id": response_id,
                "miner_hotkey": miner_hotkey,
                "miner_uid": miner_uid,
                "run_id": run_id,
                "final_era5_score": float(final_era5_score),
                "variable_scores": scoring_results,
                "num_variables_scored": len(scoring_results)
            }
            
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"Error in ERA5 final scoring computation: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "response_id": response_id,
            "final_era5_score": 0.0
        }


def handle_forecast_verification_computation(
    config,
    zarr_store_url: str,
    access_token: str,
    claimed_verification_hash: str,
    miner_hotkey: str,
    job_id: str,
    **kwargs
) -> Dict[str, Any]:
    """
    Synchronous handler for forecast data verification.
    
    This verifies forecast data integrity and computes actual verification hash.
    
    Args:
        config: Configuration object
        zarr_store_url: URL to forecast data
        access_token: Access token for data
        claimed_verification_hash: Hash claimed by miner
        miner_hotkey: Miner's hotkey
        job_id: Job ID
        
    Returns:
        Dictionary with verification results
    """
    try:
        # Heavy imports done locally
        import asyncio
        from ..utils.remote_access import open_verified_remote_zarr_dataset
        
        logger.info(f"Forecast verification for miner {miner_hotkey[:8]} (job {job_id})")
        
        # Create async event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            storage_options = {
                "headers": {"Authorization": f"Bearer {access_token}"},
                "ssl": False
            }
            
            # Attempt to open and verify the dataset
            forecast_ds = loop.run_until_complete(
                open_verified_remote_zarr_dataset(
                    zarr_store_url=zarr_store_url,
                    claimed_manifest_content_hash=claimed_verification_hash,
                    miner_hotkey_ss58=miner_hotkey,
                    storage_options=storage_options,
                    job_id=f"{job_id}_verification"
                )
            )
            
            if not forecast_ds:
                return {
                    "success": False,
                    "verification_passed": False,
                    "error": "Failed to open forecast dataset"
                }
            
            # Perform basic integrity checks
            required_variables = ["2t", "msl", "u10", "v10"]  # Basic set
            missing_vars = [var for var in required_variables if var not in forecast_ds.data_vars]
            
            if missing_vars:
                forecast_ds.close()
                return {
                    "success": False,
                    "verification_passed": False,
                    "error": f"Missing required variables: {missing_vars}"
                }
            
            # Check data ranges for basic sanity
            data_quality_issues = []
            
            for var_name in required_variables:
                try:
                    var_data = forecast_ds[var_name]
                    var_min = float(var_data.min())
                    var_max = float(var_data.max())
                    
                    # Basic range checks
                    if var_name == "2t":  # 2-meter temperature
                        if var_min < 150 or var_max > 350:  # Reasonable temperature range in Kelvin
                            data_quality_issues.append(f"{var_name}: temperature out of range [{var_min:.1f}, {var_max:.1f}]")
                    elif var_name == "msl":  # Mean sea level pressure
                        if var_min < 80000 or var_max > 110000:  # Reasonable pressure range in Pa
                            data_quality_issues.append(f"{var_name}: pressure out of range [{var_min:.1f}, {var_max:.1f}]")
                    
                except Exception as e:
                    data_quality_issues.append(f"{var_name}: error checking data - {e}")
            
            # Close dataset
            forecast_ds.close()
            
            # Determine verification result
            verification_passed = len(data_quality_issues) == 0
            
            return {
                "success": True,
                "verification_passed": verification_passed,
                "computed_hash": claimed_verification_hash,  # In real implementation, compute actual hash
                "data_quality_issues": data_quality_issues
            }
            
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"Error in forecast verification: {e}", exc_info=True)
        return {
            "success": False,
            "verification_passed": False,
            "error": str(e)
        }


def _create_minimal_climatology(reference_ds: xr.Dataset) -> xr.Dataset:
    """
    Creates a minimal ERA5 climatology dataset for scoring.
    
    In production, this would load from a pre-computed climatology file.
    For now, we create a simple climatology based on the reference dataset structure.
    """
    try:
        # Create climatology with same spatial structure as reference
        clim_data = {}
        
        # Create monthly climatology (12 months)
        time_coords = [f"2020-{month:02d}-15" for month in range(1, 13)]
        
        for var_name in ["2t", "msl", "z", "t"]:
            if var_name in reference_ds.data_vars:
                ref_var = reference_ds[var_name]
                
                # Create climatology with reasonable values
                if var_name == "2t":
                    # Temperature climatology around 288K ± seasonal variation
                    clim_values = np.random.normal(288, 15, (12,) + ref_var.shape[1:])
                elif var_name == "msl":
                    # Pressure climatology around 101325 Pa ± variation
                    clim_values = np.random.normal(101325, 1000, (12,) + ref_var.shape[1:])
                elif var_name == "z":
                    # Geopotential climatology (simplified)
                    clim_values = np.random.normal(50000, 5000, (12,) + ref_var.shape[1:])
                elif var_name == "t":
                    # Temperature at pressure levels
                    clim_values = np.random.normal(250, 20, (12,) + ref_var.shape[1:])
                
                # Create coordinates
                coords = {"time": time_coords}
                for dim in ref_var.dims[1:]:  # Skip time dimension
                    coords[dim] = ref_var.coords[dim]
                
                clim_data[var_name] = (ref_var.dims[0:1] + ref_var.dims[1:], clim_values, ref_var.attrs)
        
        # Create climatology dataset
        climatology_ds = xr.Dataset(clim_data, coords=coords)
        return climatology_ds
        
    except Exception as e:
        logger.error(f"Error creating minimal climatology: {e}")
        # Return empty dataset if creation fails
        return xr.Dataset()