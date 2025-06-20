import asyncio
import traceback
import gc
import os
import json
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
    run_gfs_init_time: datetime
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
        from .processing.weather_logic import _request_fresh_token
        
        token_data_tuple = await _request_fresh_token(task_instance, miner_hotkey, job_id)
        if token_data_tuple is None:
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
        
        for valid_time_dt in times_to_evaluate:
            effective_lead_h = int((valid_time_dt - run_gfs_init_time).total_seconds() / 3600)

            if valid_time_dt.tzinfo is None or valid_time_dt.tzinfo.utcoffset(valid_time_dt) is None:
                valid_time_dt = valid_time_dt.replace(tzinfo=timezone.utc) 
            else:
                valid_time_dt = valid_time_dt.astimezone(timezone.utc)
            
            valid_time_np = np.datetime64(valid_time_dt.replace(tzinfo=None))
            logger.info(f"[Day1Score] Processing Valid Time: {valid_time_dt} (Effective Lead: {effective_lead_h}h from {run_gfs_init_time})")

            time_key_for_results = effective_lead_h
            day1_results["lead_time_scores"][time_key_for_results] = {}

            try:
                gfs_analysis_lead = gfs_analysis_data_for_run.sel(time=valid_time_np, method="nearest")
                gfs_reference_lead = gfs_reference_forecast_for_run.sel(time=valid_time_np, method="nearest")

                selected_time_gfs_analysis = np.datetime64(gfs_analysis_lead.time.data.item(), 'ns')
                if abs(selected_time_gfs_analysis - valid_time_np) > np.timedelta64(1, 'h'):
                    logger.warning(f"GFS Analysis time {selected_time_gfs_analysis} too far from target {valid_time_np} for lead {effective_lead_h}h. Skipping lead.")
                    continue
                
                selected_time_gfs_reference = np.datetime64(gfs_reference_lead.time.data.item(), 'ns')
                if abs(selected_time_gfs_reference - valid_time_np) > np.timedelta64(1, 'h'):
                    logger.warning(f"GFS Reference time {selected_time_gfs_reference} too far from target {valid_time_np} for lead {effective_lead_h}h. Skipping lead.")
                    continue

            except Exception as e_sel:
                logger.warning(f"Could not select GFS data for lead {effective_lead_h}h (valid time {valid_time_dt}): {e_sel}. Skipping lead.")
                continue
            
            valid_time_dt_aware = pd.Timestamp(valid_time_dt).tz_localize('UTC') if pd.Timestamp(valid_time_dt).tzinfo is None else pd.Timestamp(valid_time_dt).tz_convert('UTC')
            valid_time_np_ns = np.datetime64(valid_time_dt_aware.replace(tzinfo=None), 'ns')

            selection_label_for_miner = valid_time_np_ns
            
            # Handle timezone-aware datetime dtypes properly
            time_dtype_str = str(miner_forecast_ds.time.dtype)
            is_integer_time = False
            is_timezone_aware = False
            try:
                # Try the normal numpy check first, but handle timezone-aware dtypes
                if 'datetime64' in time_dtype_str and 'UTC' in time_dtype_str:
                    # This is a timezone-aware datetime, not an integer
                    is_integer_time = False
                    is_timezone_aware = True
                    logger.debug(f"[Day1Score] Detected timezone-aware datetime dtype: {time_dtype_str}")
                    
                    # For timezone-aware coordinates, use pandas Timestamp directly
                    # Don't convert to numpy datetime64 as it loses timezone information
                    selection_label_for_miner = valid_time_dt_aware
                    logger.debug(f"[Day1Score] Using pandas Timestamp for timezone-aware selection: {selection_label_for_miner} (type: {type(selection_label_for_miner)})")
                else:
                    is_integer_time = np.issubdtype(miner_forecast_ds.time.dtype, np.integer)
            except TypeError as e:
                # Handle the "Cannot interpret 'datetime64[ns, UTC]' as a data type" error
                logger.debug(f"[Day1Score] Cannot check issubdtype for {time_dtype_str}: {e}. Assuming non-integer time.")
                is_integer_time = False
            
            if is_integer_time:
                logger.warning(f"[Day1Score] Miner forecast time coordinate is integer type ({miner_forecast_ds.time.dtype}). Attempting to cast selection label.")
                try:
                    selection_label_for_miner = valid_time_np_ns.astype(np.int64)
                    logger.info(f"[Day1Score] Casting valid_time_np_ns to int64 for miner selection: {selection_label_for_miner}")
                except Exception as e_cast:
                    logger.error(f"[Day1Score] Failed to cast datetime64[ns] to int64 for miner selection: {e_cast}. Skipping lead.")
                    continue
            elif miner_forecast_ds.time.dtype != valid_time_np_ns.dtype:
                 logger.warning(f"[Day1Score] Miner forecast time dtype ({miner_forecast_ds.time.dtype}) differs from selection label dtype ({valid_time_np_ns.dtype}). Proceeding with caution for .sel().")

            try:
                miner_forecast_lead = miner_forecast_ds.sel(time=selection_label_for_miner, method="nearest")
            except TypeError as te_sel_miner:
                logger.error(f"[Day1Score] TypeError during miner_forecast_ds.sel(): {te_sel_miner}. This often indicates incompatible time coordinate types. Miner time dtype: {miner_forecast_ds.time.dtype}, Selection label type: {type(selection_label_for_miner)}, value: {selection_label_for_miner}. Skipping lead.")
                continue
            except Exception as e_sel_miner:
                logger.error(f"[Day1Score] Error selecting from miner_forecast_ds: {e_sel_miner}. Skipping lead.")
                continue

            time_diff_too_large = False
            miner_time_value_from_sel = miner_forecast_lead.time.item()

            # Use the same safe dtype checking as above
            if not is_integer_time:
                try:
                    miner_time_dt64 = np.datetime64(miner_time_value_from_sel, 'ns')
                    
                    # Handle timezone-aware vs naive datetime comparison
                    if is_timezone_aware:
                        # For timezone-aware data, convert both to naive UTC for comparison
                        target_naive = selection_label_for_miner.tz_convert('UTC').tz_localize(None)
                        target_dt64 = np.datetime64(target_naive, 'ns')
                        if abs(miner_time_dt64 - target_dt64) > np.timedelta64(1, 'h'):
                            time_diff_too_large = True
                    else:
                        # For timezone-naive data, use original logic
                        if abs(miner_time_dt64 - selection_label_for_miner) > np.timedelta64(1, 'h'):
                            time_diff_too_large = True
                except Exception as e_conv_dt64:
                    logger.warning(f"[Day1Score] Could not convert/compare miner time {miner_time_value_from_sel} with {selection_label_for_miner}: {e_conv_dt64}. Assuming time difference is too large.")
                    time_diff_too_large = True
            else:
                hour_in_nanos = np.timedelta64(1, 'h').astype('timedelta64[ns]').astype(np.int64)
                if abs(miner_time_value_from_sel - selection_label_for_miner) > hour_in_nanos:
                    time_diff_too_large = True
            
            if time_diff_too_large:
                logger.warning(f"Miner forecast for {valid_time_dt} (selected time value: {miner_time_value_from_sel}, target label: {selection_label_for_miner}) not found or too far. Skipping lead {effective_lead_h}h.")
                continue

            for var_config in variables_to_score:
                var_name = var_config['name']
                var_level = var_config.get('level')
                standard_name_for_clim = var_config.get('standard_name', var_name)
                var_key = f"{var_name}{var_level}" if var_level else var_name
                logger.debug(f"[Day1Score] Scoring Var: {var_key} at Valid Time: {valid_time_dt}")

                day1_results["lead_time_scores"][time_key_for_results][var_key] = {
                    "skill_score": None, "acc": None, "sanity_checks": {},
                    "clone_distance_mse": None, "clone_penalty_applied": None
                }

                try:
                    miner_var_da_unaligned = miner_forecast_lead[var_name]
                    truth_var_da_unaligned = gfs_analysis_lead[var_name]
                    ref_var_da_unaligned = gfs_reference_lead[var_name]

                    # Add detailed diagnostics for potential unit mismatches
                    logger.info(f"[Day1Score] RAW DATA DIAGNOSTICS for {var_key} at {valid_time_dt}:")
                    
                    # Log data ranges before any processing
                    miner_min, miner_max, miner_mean = float(miner_var_da_unaligned.min()), float(miner_var_da_unaligned.max()), float(miner_var_da_unaligned.mean())
                    truth_min, truth_max, truth_mean = float(truth_var_da_unaligned.min()), float(truth_var_da_unaligned.max()), float(truth_var_da_unaligned.mean())
                    ref_min, ref_max, ref_mean = float(ref_var_da_unaligned.min()), float(ref_var_da_unaligned.max()), float(ref_var_da_unaligned.mean())
                    
                    logger.info(f"[Day1Score] Miner {var_key}: range=[{miner_min:.1f}, {miner_max:.1f}], mean={miner_mean:.1f}, units={miner_var_da_unaligned.attrs.get('units', 'unknown')}")
                    logger.info(f"[Day1Score] Truth {var_key}: range=[{truth_min:.1f}, {truth_max:.1f}], mean={truth_mean:.1f}, units={truth_var_da_unaligned.attrs.get('units', 'unknown')}")
                    logger.info(f"[Day1Score] Ref   {var_key}: range=[{ref_min:.1f}, {ref_max:.1f}], mean={ref_mean:.1f}, units={ref_var_da_unaligned.attrs.get('units', 'unknown')}")
                    
                    # Check for potential unit mismatch indicators
                    if var_name == 'z' and var_level == 500:
                        # For z500, geopotential should be ~49000-58000 m²/s²
                        # If it's geopotential height, it would be ~5000-6000 m
                        miner_ratio = miner_mean / 9.80665  # If miner is geopotential, this ratio should be ~5000-6000
                        truth_ratio = truth_mean / 9.80665
                        logger.info(f"[Day1Score] z500 UNIT CHECK - If geopotential (m²/s²): miner_mean/g={miner_ratio:.1f}m, truth_mean/g={truth_ratio:.1f}m")
                        
                        if miner_mean < 10000:  # Much smaller than expected geopotential
                            logger.warning(f"[Day1Score] POTENTIAL UNIT MISMATCH: Miner z500 mean ({miner_mean:.1f}) suggests geopotential height (m) rather than geopotential (m²/s²)")
                        elif truth_mean > 40000 and miner_mean > 40000:
                            logger.info(f"[Day1Score] Unit check OK: Both miner and truth z500 appear to be geopotential (m²/s²)")
                    
                    elif var_name == '2t':
                        # Temperature should be ~200-320 K
                        if miner_mean < 200 or miner_mean > 350:
                            logger.warning(f"[Day1Score] POTENTIAL UNIT ISSUE: Miner 2t mean ({miner_mean:.1f}) outside expected range for Kelvin")
                            
                    elif var_name == 'msl':
                        # Mean sea level pressure should be ~90000-110000 Pa
                        if miner_mean < 50000 or miner_mean > 150000:
                            logger.warning(f"[Day1Score] POTENTIAL UNIT ISSUE: Miner msl mean ({miner_mean:.1f}) outside expected range for Pa")

                    # AUTOMATIC UNIT CONVERSION: Convert geopotential height to geopotential if needed
                    if var_name == 'z' and miner_mean < 10000 and truth_mean > 40000:
                        logger.warning(f"[Day1Score] AUTOMATIC UNIT CONVERSION: Converting miner z from geopotential height (m) to geopotential (m²/s²)")
                        miner_var_da_unaligned = miner_var_da_unaligned * 9.80665
                        miner_var_da_unaligned.attrs['units'] = 'm2 s-2'
                        miner_var_da_unaligned.attrs['long_name'] = 'Geopotential (auto-converted from height)'
                        logger.info(f"[Day1Score] After conversion: miner z range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}")

                    # Check for temperature unit conversions (Celsius to Kelvin)
                    elif var_name in ['2t', 't'] and miner_mean < 100 and truth_mean > 200:
                        logger.warning(f"[Day1Score] AUTOMATIC UNIT CONVERSION: Converting miner {var_name} from Celsius to Kelvin")
                        miner_var_da_unaligned = miner_var_da_unaligned + 273.15
                        miner_var_da_unaligned.attrs['units'] = 'K'
                        miner_var_da_unaligned.attrs['long_name'] = f'{miner_var_da_unaligned.attrs.get("long_name", var_name)} (auto-converted from Celsius)'
                        logger.info(f"[Day1Score] After conversion: miner {var_name} range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}")

                    # Check for pressure unit conversions (hPa to Pa)
                    elif var_name == 'msl' and miner_mean < 2000 and truth_mean > 50000:
                        logger.warning(f"[Day1Score] AUTOMATIC UNIT CONVERSION: Converting miner msl from hPa to Pa")
                        miner_var_da_unaligned = miner_var_da_unaligned * 100.0
                        miner_var_da_unaligned.attrs['units'] = 'Pa'
                        miner_var_da_unaligned.attrs['long_name'] = 'Mean sea level pressure (auto-converted from hPa)'
                        logger.info(f"[Day1Score] After conversion: miner msl range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}")

                    if var_level:
                        # Handle different pressure level dimension names for each dataset
                        def find_pressure_dim(data_array, dataset_name="dataset"):
                            for dim_name in ['pressure_level', 'plev', 'level']:
                                if dim_name in data_array.dims:
                                    return dim_name
                            logger.warning(f"No pressure level dimension found in {dataset_name} for {var_key} level {var_level}. Available dims: {data_array.dims}")
                            return None
                        
                        miner_pressure_dim = find_pressure_dim(miner_var_da_unaligned, "miner")
                        truth_pressure_dim = find_pressure_dim(truth_var_da_unaligned, "truth")
                        ref_pressure_dim = find_pressure_dim(ref_var_da_unaligned, "reference")
                        
                        if not all([miner_pressure_dim, truth_pressure_dim, ref_pressure_dim]):
                            logger.warning(f"Missing pressure dimensions for {var_key} level {var_level}. Skipping.")
                            continue
                            
                        miner_var_da_selected = miner_var_da_unaligned.sel({miner_pressure_dim: var_level}, method="nearest")
                        truth_var_da_selected = truth_var_da_unaligned.sel({truth_pressure_dim: var_level}, method="nearest")
                        ref_var_da_selected = ref_var_da_unaligned.sel({ref_pressure_dim: var_level}, method="nearest")
                        
                        if abs(truth_var_da_selected[truth_pressure_dim].item() - var_level) > 10:
                             logger.warning(f"Truth data for {var_key} level {var_level} too far ({truth_var_da_selected[truth_pressure_dim].item()}). Skipping.")
                             continue
                        if abs(miner_var_da_selected[miner_pressure_dim].item() - var_level) > 10:
                             logger.warning(f"Miner data for {var_key} level {var_level} too far ({miner_var_da_selected[miner_pressure_dim].item()}). Skipping.")
                             continue
                        if abs(ref_var_da_selected[ref_pressure_dim].item() - var_level) > 10:
                             logger.warning(f"GFS Ref data for {var_key} level {var_level} too far ({ref_var_da_selected[ref_pressure_dim].item()}). Skipping.")
                             continue
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

                    miner_var_da_selected_std = _standardize_spatial_dims(miner_var_da_selected)
                    target_grid_da_std = _standardize_spatial_dims(target_grid_da)
                    ref_var_da_selected_std = _standardize_spatial_dims(ref_var_da_selected)


                    miner_var_da_aligned = await asyncio.to_thread(
                        miner_var_da_selected_std.interp_like,
                        target_grid_da_std, method="linear", kwargs={"fill_value": None}
                    )
                    truth_var_da_final = target_grid_da_std
                    
                    ref_var_da_aligned = await asyncio.to_thread(
                        ref_var_da_selected_std.interp_like,
                        target_grid_da_std, method="linear", kwargs={"fill_value": None}
                    )
                    

                    broadcasted_weights_final = None
                    spatial_dims_for_metric = [d for d in truth_var_da_final.dims if d in ('lat', 'lon')]
                    
                    actual_lat_dim_in_target = 'lat' if 'lat' in truth_var_da_final.dims else None


                    if actual_lat_dim_in_target:
                        try:
                            target_lat_coord = truth_var_da_final[actual_lat_dim_in_target]
                            one_d_lat_weights_target = await asyncio.to_thread(_calculate_latitude_weights, target_lat_coord)
                            _, broadcasted_weights_final = await asyncio.to_thread(xr.broadcast, truth_var_da_final, one_d_lat_weights_target)
                            logger.debug(f"[Day1Score] For {var_key}, using target_grid_da_std derived weights. Broadcasted weights dims: {broadcasted_weights_final.dims}, shape: {broadcasted_weights_final.shape}")
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

                    clone_distance_mse_val = await asyncio.to_thread(
                        _get_metric_scalar_value,
                        xs.mse, 
                        miner_var_da_aligned, 
                        ref_var_da_aligned, 
                        dim=spatial_dims_for_metric, 
                        weights=broadcasted_weights_final, 
                        skipna=True
                    )
                    day1_results["lead_time_scores"][time_key_for_results][var_key]["clone_distance_mse"] = clone_distance_mse_val

                    delta_thresholds_config = day1_scoring_config.get('clone_delta_thresholds', {})
                    delta_for_var = delta_thresholds_config.get(var_key)
                    clone_penalty = 0.0

                    if delta_for_var is not None and clone_distance_mse_val < delta_for_var:
                        gamma = day1_scoring_config.get('clone_penalty_gamma', 1.0)
                        clone_penalty = gamma * (1.0 - (clone_distance_mse_val / delta_for_var))
                        clone_penalty = max(0.0, clone_penalty)
                        logger.warning(f"[Day1Score] GFS Clone Suspect: {var_key} at {effective_lead_h}h for {miner_hotkey}. "
                                     f"Distance MSE {clone_distance_mse_val:.4f} < Delta {delta_for_var:.4f}. Penalty: {clone_penalty:.4f}")
                        day1_results["qc_passed_all_vars_leads"] = False
                        logger.info(f"[Day1Score] QC FAILURE REASON: Clone penalty triggered for {var_key}")
                    day1_results["lead_time_scores"][time_key_for_results][var_key]["clone_penalty_applied"] = clone_penalty

                    clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
                    clim_hour = valid_time_dt.hour 
                    
                    clim_hour_rounded = (clim_hour // 6) * 6 
                    
                    clim_var_da_raw = era5_climatology[standard_name_for_clim].sel(
                        dayofyear=clim_dayofyear, 
                        hour=clim_hour_rounded,
                        method="nearest"
                    )
                    clim_var_da_raw_std = _standardize_spatial_dims(clim_var_da_raw)

                    clim_var_to_interpolate = clim_var_da_raw_std
                    if var_level:
                        # Handle different pressure level dimension names in climatology data
                        clim_pressure_dim = None
                        for dim_name in ['pressure_level', 'plev', 'level']:
                            if dim_name in clim_var_to_interpolate.dims:
                                clim_pressure_dim = dim_name
                                break
                        
                        if clim_pressure_dim:
                            clim_var_to_interpolate = await asyncio.to_thread(clim_var_to_interpolate.sel, **{clim_pressure_dim: var_level}, method="nearest")
                            if abs(clim_var_to_interpolate[clim_pressure_dim].item() - var_level) > 10:
                                logger.warning(f"[Day1Score] Climatology for {var_key} at target level {var_level} was found at {clim_var_to_interpolate[clim_pressure_dim].item()}. Using this nearest level data.")
                    
                    clim_var_da_aligned = await asyncio.to_thread(
                        clim_var_to_interpolate.interp_like,
                        truth_var_da_final, method="linear", kwargs={"fill_value": None}
                    )
                    

                    sanity_results = await perform_sanity_checks(
                        forecast_da=miner_var_da_aligned,
                        reference_da_for_corr=ref_var_da_aligned,
                        variable_name=var_key, 
                        climatology_bounds_config=day1_scoring_config.get('climatology_bounds', {}),
                        pattern_corr_threshold=day1_scoring_config.get('pattern_correlation_threshold', 0.3),
                        lat_weights=broadcasted_weights_final
                    )
                    day1_results["lead_time_scores"][time_key_for_results][var_key]["sanity_checks"] = sanity_results

                    if not sanity_results.get("climatology_passed") or \
                       not sanity_results.get("pattern_correlation_passed"):
                        logger.warning(f"[Day1Score] Sanity check failed for {var_key} at {effective_lead_h}h. Skipping metrics.")
                        day1_results["qc_passed_all_vars_leads"] = False
                        logger.info(f"[Day1Score] QC FAILURE REASON: Sanity check failed for {var_key} - "
                                   f"Climatology passed: {sanity_results.get('climatology_passed')}, "
                                   f"Pattern correlation passed: {sanity_results.get('pattern_correlation_passed')}")
                        continue

                    # Bias Correction
                    forecast_bc_da = await calculate_bias_corrected_forecast(miner_var_da_aligned, truth_var_da_final)

                    # MSE Skill Score
                    skill_score = await calculate_mse_skill_score(forecast_bc_da, truth_var_da_final, ref_var_da_aligned, broadcasted_weights_final)
                    
                    # clone penalty
                    skill_score_after_penalty = skill_score - clone_penalty
                    day1_results["lead_time_scores"][time_key_for_results][var_key]["skill_score"] = skill_score_after_penalty

                    if np.isfinite(skill_score_after_penalty): aggregated_skill_scores.append(skill_score_after_penalty)
                    
                    # ACC
                    acc_score = await calculate_acc(miner_var_da_aligned, truth_var_da_final, clim_var_da_aligned, broadcasted_weights_final)
                    day1_results["lead_time_scores"][time_key_for_results][var_key]["acc"] = acc_score
                    if np.isfinite(acc_score): aggregated_acc_scores.append(acc_score)
                    
                    # ACC Lower Bound Check
                    if effective_lead_h == 12 and np.isfinite(acc_score) and acc_score < day1_scoring_config.get("acc_lower_bound_d1", 0.6):
                        logger.warning(f"[Day1Score] ACC for {var_key} at valid time {valid_time_dt} (Eff. Lead 12h) ({acc_score:.3f}) is below threshold.")

                except Exception as e_var:
                    logger.error(f"[Day1Score] Error scoring {var_key} at {valid_time_dt}: {e_var}", exc_info=True)
                    day1_results["lead_time_scores"][time_key_for_results][var_key]["error"] = str(e_var)
                    day1_results["qc_passed_all_vars_leads"] = False

            # Light cleanup after each time step to prevent excessive accumulation
            if len(times_to_evaluate) > 2:  # Only if processing multiple time steps
                gc.collect()

        clipped_skill_scores = [max(0.0, s) for s in aggregated_skill_scores if np.isfinite(s)]
        scaled_acc_scores = [(a + 1.0) / 2.0 for a in aggregated_acc_scores if np.isfinite(a)]

        avg_clipped_skill = np.mean(clipped_skill_scores) if clipped_skill_scores else 0.0
        avg_scaled_acc = np.mean(scaled_acc_scores) if scaled_acc_scores else 0.0
        
        if not np.isfinite(avg_clipped_skill): avg_clipped_skill = 0.0
        if not np.isfinite(avg_scaled_acc): avg_scaled_acc = 0.0

        if not aggregated_skill_scores and not aggregated_acc_scores:
            logger.warning(f"[Day1Score] No valid skill or ACC scores to aggregate for {miner_hotkey}. Setting overall score to 0.")
            day1_results["overall_day1_score"] = 0.0
            day1_results["qc_passed_all_vars_leads"] = False
        else:
            alpha = day1_scoring_config.get('alpha_skill', 0.5)
            beta = day1_scoring_config.get('beta_acc', 0.5)
            
            if not np.isclose(alpha + beta, 1.0):
                logger.warning(f"[Day1Score] Alpha ({alpha}) + Beta ({beta}) does not equal 1. Score may not be 0-1 bounded as intended.")
            
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

    logger.info(f"[Day1Score] Completed for {miner_hotkey}. Final score: {day1_results['overall_day1_score']}, QC Passed: {day1_results['qc_passed_all_vars_leads']}")
    return day1_results
