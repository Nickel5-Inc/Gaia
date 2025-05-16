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

from fiber.logging_utils import get_logger
from typing import TYPE_CHECKING, Any, Optional, Dict, List, Tuple
if TYPE_CHECKING:
    from ..weather_task import WeatherTask 

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
    day1_scoring_config: Dict, # Passed from initial_scoring_worker
    run_gfs_init_time: datetime # GFS 00Z init time for the current run
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
        "qc_passed_all_vars_leads": True, # Assume pass until a failure
        "lead_time_scores": {}, # {lead_hour: {variable_level: {skill, acc, sanity_status}}}
        "error_message": None
    }

    miner_forecast_ds: Optional[xr.Dataset] = None

    try:
        from ..processing.weather_logic import _request_fresh_token
        
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

        # Prepare latitude weights
        lat_coord_name = None
        if 'latitude' in miner_forecast_ds.coords: lat_coord_name = 'latitude'
        elif 'lat' in miner_forecast_ds.coords: lat_coord_name = 'lat'
        
        if not lat_coord_name:
            raise ValueError("Latitude coordinate ('latitude' or 'lat') not found in miner's forecast dataset.")
        lat_weights = _calculate_latitude_weights(miner_forecast_ds[lat_coord_name])

        lead_times_to_score_hours: List[int] = day1_scoring_config.get('lead_times_hours', [6, 12])
        variables_to_score: List[Dict] = day1_scoring_config.get('variables_levels_to_score', [])
        
        aggregated_skill_scores = []
        aggregated_acc_scores = []
        
        for lead_h in lead_times_to_score_hours:
            valid_time_dt = run_gfs_init_time + timedelta(hours=lead_h)
            valid_time_np = np.datetime64(valid_time_dt)
            logger.info(f"[Day1Score] Processing Lead: {lead_h}h (Valid Time: {valid_time_dt})")
            day1_results["lead_time_scores"][lead_h] = {}

            # Select truth and reference data for this lead time
            try:
                gfs_analysis_lead = gfs_analysis_data_for_run.sel(time=valid_time_np, method="nearest")
                gfs_reference_lead = gfs_reference_forecast_for_run.sel(time=valid_time_np, method="nearest")

                if abs(gfs_analysis_lead.time.item() - valid_time_np) > np.timedelta64(1, 'h'):
                    logger.warning(f"GFS Analysis for {valid_time_dt} not found. Skipping lead {lead_h}h.")
                    continue
                if abs(gfs_reference_lead.time.item() - valid_time_np) > np.timedelta64(1, 'h'):
                    logger.warning(f"GFS Reference for {valid_time_dt} not found. Skipping lead {lead_h}h.")
                    continue
            except Exception as e_sel:
                logger.warning(f"Could not select GFS data for lead {lead_h}h (valid time {valid_time_dt}): {e_sel}. Skipping lead.")
                continue
            
            miner_forecast_lead = miner_forecast_ds.sel(time=valid_time_np, method="nearest")
            if abs(miner_forecast_lead.time.item() - valid_time_np) > np.timedelta64(1, 'h'):
                logger.warning(f"Miner forecast for {valid_time_dt} not found. Skipping lead {lead_h}h.")
                continue

            for var_config in variables_to_score:
                var_name = var_config['name']
                level = var_config.get('level')
                standard_name_for_clim = var_config.get('standard_name', var_name)
                var_key = f"{var_name}{level}" if level else var_name
                logger.debug(f"[Day1Score] Scoring Var: {var_key} at Lead: {lead_h}h")

                day1_results["lead_time_scores"][lead_h][var_key] = {
                    "skill_score": None, "acc": None, "sanity_checks": {},
                    "clone_distance_mse": None, "clone_penalty_applied": None
                }

                try:
                    miner_var_da_raw = miner_forecast_lead[var_name]
                    truth_var_da = gfs_analysis_lead[var_name]
                    ref_var_da = gfs_reference_lead[var_name]
                    
                    if level:
                        miner_var_da_raw = miner_var_da_raw.sel(pressure_level=level, method="nearest")
                        truth_var_da = truth_var_da.sel(pressure_level=level, method="nearest")
                        ref_var_da = ref_var_da.sel(pressure_level=level, method="nearest")
                        if abs(truth_var_da.pressure_level.item() - level) > 10:
                             logger.warning(f"Truth data for {var_key} level {level} too far ({truth_var_da.pressure_level.item()}). Skipping.")
                             continue
                        if abs(miner_var_da_raw.pressure_level.item() - level) > 10:
                             logger.warning(f"Miner data for {var_key} level {level} too far ({miner_var_da_raw.pressure_level.item()}). Skipping.")
                             continue
                        if abs(ref_var_da.pressure_level.item() - level) > 10:
                             logger.warning(f"GFS Ref data for {var_key} level {level} too far ({ref_var_da.pressure_level.item()}). Skipping.")
                             continue

                    clone_distance_mse = await asyncio.to_thread(
                        xs.mse, miner_var_da_raw, ref_var_da, dim=[d for d in miner_var_da_raw.dims if d.lower() in ('latitude', 'longitude', 'lat', 'lon')], weights=lat_weights, skipna=True
                    )
                    day1_results["lead_time_scores"][lead_h][var_key]["clone_distance_mse"] = float(clone_distance_mse.item())

                    delta_thresholds_config = day1_scoring_config.get('clone_delta_thresholds', {})
                    delta_for_var = delta_thresholds_config.get(var_key)
                    clone_penalty = 0.0

                    if delta_for_var is not None and clone_distance_mse < delta_for_var:
                        gamma = day1_scoring_config.get('clone_penalty_gamma', 1.0)
                        clone_penalty = gamma * (1.0 - (clone_distance_mse / delta_for_var))
                        clone_penalty = max(0.0, clone_penalty)
                        logger.warning(f"[Day1Score] GFS Clone Suspect: {var_key} at {lead_h}h for {miner_hotkey}. "
                                     f"Distance MSE {clone_distance_mse.item():.4f} < Delta {delta_for_var:.4f}. Penalty: {clone_penalty:.4f}")
                        day1_results["qc_passed_all_vars_leads"] = False
                    day1_results["lead_time_scores"][lead_h][var_key]["clone_penalty_applied"] = clone_penalty

                    # Select climatology for this variable, valid_time dayofyear and hour
                    clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
                    clim_hour = valid_time_dt.hour 
                    
                    clim_hour_rounded = (clim_hour // 6) * 6 
                    
                    clim_var_da = era5_climatology[standard_name_for_clim].sel(
                        dayofyear=clim_dayofyear, 
                        hour=clim_hour_rounded,
                        method="nearest"
                    )
                    if level and 'pressure_level' in clim_var_da.dims:
                        clim_var_da = clim_var_da.sel(pressure_level=level, method="nearest")
                    
                    sanity_results = await perform_sanity_checks(
                        forecast_da=miner_var_da_raw,
                        reference_da_for_corr=ref_var_da,
                        variable_name=var_key, 
                        climatology_bounds_config=day1_scoring_config.get('climatology_bounds', {}),
                        pattern_corr_threshold=day1_scoring_config.get('pattern_correlation_threshold', 0.3),
                        lat_weights=lat_weights
                    )
                    day1_results["lead_time_scores"][lead_h][var_key]["sanity_checks"] = sanity_results

                    if not sanity_results.get("climatology_passed") or \
                       not sanity_results.get("pattern_correlation_passed"):
                        logger.warning(f"[Day1Score] Sanity check failed for {var_key} at {lead_h}h. Skipping metrics.")
                        day1_results["qc_passed_all_vars_leads"] = False
                        continue # Skip scoring this variable if QC fails

                    # Bias Correction
                    forecast_bc_da = await calculate_bias_corrected_forecast(miner_var_da_raw, truth_var_da)

                    # MSE Skill Score (uses bias-corrected forecast)
                    skill_score = await calculate_mse_skill_score(forecast_bc_da, truth_var_da, ref_var_da, lat_weights)
                    
                    # clone penalty
                    skill_score_after_penalty = skill_score - clone_penalty
                    day1_results["lead_time_scores"][lead_h][var_key]["skill_score"] = skill_score_after_penalty

                    if np.isfinite(skill_score_after_penalty): aggregated_skill_scores.append(skill_score_after_penalty)

                    # ACC (uses raw miner data for anomalies)
                    acc_score = await calculate_acc(miner_var_da_raw, truth_var_da, clim_var_da, lat_weights)
                    day1_results["lead_time_scores"][lead_h][var_key]["acc"] = acc_score
                    if np.isfinite(acc_score): aggregated_acc_scores.append(acc_score)
                    
                    # ACC Lower Bound Check
                    if lead_h == 12 and np.isfinite(acc_score) and acc_score < day1_scoring_config.get("acc_lower_bound_d1", 0.6):
                        logger.warning(f"[Day1Score] ACC for {var_key} at 12h ({acc_score:.3f}) is below threshold.")

                except Exception as e_var:
                    logger.error(f"[Day1Score] Error scoring {var_key} at {lead_h}h: {e_var}", exc_info=True)
                    day1_results["lead_time_scores"][lead_h][var_key]["error"] = str(e_var)
                    day1_results["qc_passed_all_vars_leads"] = False


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
        if miner_forecast_ds:
            try:
                miner_forecast_ds.close()
            except Exception:
                pass
        gc.collect()

    logger.info(f"[Day1Score] Completed for {miner_hotkey}. Final score: {day1_results['overall_day1_score']}, QC Passed: {day1_results['qc_passed_all_vars_leads']}")
    return day1_results
