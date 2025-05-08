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

from .weather_scoring.metrics import calculate_rmse, calculate_mae_dict
from .weather_scoring.ensemble import _open_dataset_lazily, ALL_EXPECTED_VARIABLES

logger = get_logger(__name__)

#TODO: Placeholder for future OpenData API
async def fetch_ecmwf_open_data_forecast(target_datetimes: List[datetime]) -> Optional[xr.Dataset]:
    logger.debug("Placeholder: Fetching ECMWF Open Data forecast...")
    await asyncio.sleep(0.1)
    return None

DEFAULT_INITIAL_WEIGHT_FLOOR = 0.1
DEFAULT_INITIAL_WEIGHT_CAP = 1.2
DEFAULT_MAE_THRESHOLD_FOR_PENALTY = 10.0 # Example threshold - needs tuning
TARGET_QC_LEVELS = [850, 500, 250]

async def calculate_gfs_score_and_weight(task_instance: 'WeatherTask', 
                                        response: Dict, 
                                        target_datetimes: List[datetime], 
                                        gfs_analysis_ds: xr.Dataset,
                                        num_verified_miners: int
                                        ) -> Tuple[str, Optional[float], Optional[float]]:
    """
    Calculates initial preliminary weight for a single miner response for the ensemble.
    Uses historical performance (ERA5 > Initial) and QC vs GFS & OpenData.

    Args:
        task_instance: The WeatherTask instance.
        response: DB row containing miner response details.
        target_datetimes: List of specific datetimes to score against.
        gfs_analysis_ds: Pre-loaded xarray Dataset of GFS analysis.
        num_verified_miners: Total number of verified miners in this run (for default weight).

    Returns:
        Tuple: (miner_hotkey, preliminary_unnormalized_weight, gfs_mae_score). 
               Weight and score are None on failure.
    """
    response_id = response['response_id']
    miner_hotkey = response['miner_hotkey']
    kerchunk_url = response.get('kerchunk_json_url')
    job_id = response.get('job_id')
    run_id = response.get('run_id')
    miner_sparse_ds = None
    miner_ds_lazy = None
    gfs_mae_score = None
    prelim_weight = None 

    try:
        logger.debug(f"[InitialWeight] Calculating for miner {miner_hotkey} (Resp: {response_id}, Run: {run_id})")
        
        # Fetch Historical Weight 
        historical_weight = None
        weight_source = "None"
        try:
            # ERA5-based weight primary source, need task runs to populate this
            era5_weight_query = """
                SELECT weight FROM weather_historical_weights
                WHERE miner_hotkey = :hk AND score_type = 'era5_weight' 
                ORDER BY last_updated DESC LIMIT 1
            """ # TODO: Define 'era5_weight' as the type stored by final scoring
            era5_result = await task_instance.db_manager.fetch_one(era5_weight_query, {"hk": miner_hotkey})
            if era5_result and era5_result['weight'] is not None:
                historical_weight = float(era5_result['weight'])
                weight_source = "ERA5 History"
            else:
                # Fallback to previous initial weight if no ERA5 history
                initial_weight_query = """
                    SELECT weight FROM weather_historical_weights
                    WHERE miner_hotkey = :hk AND score_type = 'initial_ensemble_weight'
                    ORDER BY last_updated DESC LIMIT 1
                """
                initial_result = await task_instance.db_manager.fetch_one(initial_weight_query, {"hk": miner_hotkey})
                if initial_result and initial_result['weight'] is not None:
                    historical_weight = float(initial_result['weight'])
                    weight_source = "Initial History"

            if historical_weight is not None:
                logger.debug(f"[InitialWeight] Found historical weight for {miner_hotkey}: {historical_weight:.4f} (Source: {weight_source})")
            else:
                logger.debug(f"[InitialWeight] No historical weight found for {miner_hotkey}. Will use default.")
                weight_source = "Default"
                # Default weight is handled later based on normalization, starting point is 1.0 before QC.

        except Exception as e_hist:
            logger.warning(f"[InitialWeight] Error fetching historical weight for {miner_hotkey}: {e_hist}. Using default.")
            weight_source = "Default (Error)"

        # Miner Forecast & QC References
        if not kerchunk_url or not job_id:
             logger.warning(f"[InitialWeight] Missing kerchunk_url/job_id for {miner_hotkey}. Cannot calculate weight.")
             return miner_hotkey, None, None

        from ..processing.weather_logic import _request_fresh_token
        token_response = await _request_fresh_token(task_instance, miner_hotkey, job_id)
        if not token_response or 'access_token' not in token_response:
            logger.warning(f"[InitialWeight] Could not get token for {miner_hotkey}. Cannot score.")
            return miner_hotkey, None, None
            
        ref_spec = {
            'url': kerchunk_url,
            'protocol': 'http',
            'options': {'headers': {'Authorization': f'Bearer {token_response["access_token"]}'}}
        }
        
        miner_ds_lazy = await _open_dataset_lazily(ref_spec)
        if miner_ds_lazy is None:
             logger.warning(f"[InitialWeight] Failed to open lazy dataset for {miner_hotkey}. Cannot score.")
             return miner_hotkey, None, None
             
        target_times_np = np.array(target_datetimes, dtype='datetime64[ns]')
        miner_sparse_ds = await asyncio.to_thread(
            lambda: miner_ds_lazy.sel(time=target_times_np, method='nearest').load()
        )
        gfs_sparse_ds = gfs_analysis_ds.sel(time=target_times_np, method='nearest')
        
        #TODO: Placeholder for OpenData
        ecmwf_sparse_ds = await fetch_ecmwf_open_data_forecast(target_datetimes)
        if ecmwf_sparse_ds is not None:
            ecmwf_sparse_ds = ecmwf_sparse_ds.sel(time=target_times_np, method='nearest')
            logger.debug(f"[InitialWeight] Placeholder: ECMWF Open Data fetched for {miner_hotkey}.")
        else:
            logger.debug(f"[InitialWeight] Placeholder: ECMWF Open Data not available for {miner_hotkey}.")

        # QC & Calculate Factor
        common_vars_gfs = [v for v in ALL_EXPECTED_VARIABLES if v in miner_sparse_ds and v in gfs_sparse_ds]
        if not common_vars_gfs:
             logger.warning(f"[InitialWeight] Miner {miner_hotkey}: No common vars with GFS. Cannot calculate QC score.")
             return miner_hotkey, None, None
        
        # selecting only target levels for atmospheric vars
        miner_dict_gfs = {}
        gfs_dict = {}
        
        level_coord_name = None
        if 'level' in miner_sparse_ds.coords: level_coord_name = 'level'
        elif 'pressure_level' in miner_sparse_ds.coords: level_coord_name = 'pressure_level'
            
        for var in common_vars_gfs:
            try:
                miner_var_da = miner_sparse_ds[var]
                gfs_var_da = gfs_sparse_ds[var]
                
                is_atmospheric = level_coord_name is not None and level_coord_name in miner_var_da.dims
                
                if is_atmospheric:
                    available_levels_miner = miner_var_da[level_coord_name].values
                    available_levels_gfs = gfs_var_da[level_coord_name].values
                    
                    target_levels_present_miner = [l for l in TARGET_QC_LEVELS if l in available_levels_miner]
                    target_levels_present_gfs = [l for l in TARGET_QC_LEVELS if l in available_levels_gfs]
                    
                    common_target_levels = sorted(list(set(target_levels_present_miner) & set(target_levels_present_gfs)))
                    
                    if not common_target_levels:
                        logger.warning(f"[InitialWeight] Skipping var '{var}' for {miner_hotkey}: No common target QC levels ({TARGET_QC_LEVELS}) found.")
                        continue
                        
                    miner_dict_gfs[var] = miner_var_da.sel({level_coord_name: common_target_levels}).values
                    gfs_dict[var] = gfs_var_da.sel({level_coord_name: common_target_levels}).values
                    logger.debug(f"[InitialWeight] Selected levels {common_target_levels} for var '{var}'")
                else:
                    miner_dict_gfs[var] = miner_var_da.values
                    gfs_dict[var] = gfs_var_da.values
            except Exception as e_var_prep:
                logger.warning(f"[InitialWeight] Error preparing var '{var}' for MAE calculation for {miner_hotkey}: {e_var_prep}")
                if var in miner_dict_gfs: del miner_dict_gfs[var]
                if var in gfs_dict: del gfs_dict[var]
                continue

        if not miner_dict_gfs:
             logger.warning(f"[InitialWeight] Miner {miner_hotkey}: No variables prepared for MAE calculation after level selection/error handling.")
             return miner_hotkey, None, None

        # MAE for QC score vs GFS
        common_keys_for_mae = list(set(miner_dict_gfs.keys()) & set(gfs_dict.keys()))
        if not common_keys_for_mae:
            logger.warning(f"[InitialWeight] Miner {miner_hotkey}: No common variables left after preparation for MAE calculation.")
            return miner_hotkey, None, None
            
        final_miner_dict = {k: miner_dict_gfs[k] for k in common_keys_for_mae}
        final_gfs_dict = {k: gfs_dict[k] for k in common_keys_for_mae}
        
        gfs_mae_score = await calculate_mae_dict(final_miner_dict, final_gfs_dict)
        if gfs_mae_score is None or not np.isfinite(gfs_mae_score):
             logger.warning(f"[InitialWeight] Miner {miner_hotkey}: Invalid MAE score vs GFS ({gfs_mae_score}). Cannot calculate weight factor.")
             return miner_hotkey, None, None

        logger.debug(f"[InitialWeight] Miner {miner_hotkey}: MAE vs GFS = {gfs_mae_score:.4f}")

        #TODO: QC vs OpenData
        qc_vs_opendata_passed = True
        if ecmwf_sparse_ds is not None:
            common_vars_ecmwf = [v for v in ALL_EXPECTED_VARIABLES if v in miner_sparse_ds and v in ecmwf_sparse_ds]
            if common_vars_ecmwf:
                miner_dict_ecmwf = {var: miner_sparse_ds[var].values for var in common_vars_ecmwf}
                ecmwf_dict = {var: ecmwf_sparse_ds[var].values for var in common_vars_ecmwf}
                ecmwf_mae_score = await calculate_mae_dict(miner_dict_ecmwf, ecmwf_dict)
                logger.debug(f"[InitialWeight] Placeholder: Miner {miner_hotkey}: MAE vs OpenData = {ecmwf_mae_score:.4f}")
                # I need to add a logic here to set qc_vs_opendata_passed = False if MAE too high
            else:
                logger.warning(f"[InitialWeight] Miner {miner_hotkey}: No common vars with OpenData.")
                
        # QC Adjustment Factor Logic
        qc_adjustment_factor = 1.0
        mae_penalty_threshold = task_instance.config.get("qc_mae_penalty_threshold", DEFAULT_MAE_THRESHOLD_FOR_PENALTY)
        
        if gfs_mae_score > mae_penalty_threshold:
            logger.warning(f"[InitialWeight] Miner {miner_hotkey}: MAE vs GFS ({gfs_mae_score:.4f}) exceeds threshold ({mae_penalty_threshold:.4f}). Applying penalty.")
            qc_adjustment_factor *= max(0.1, 1.0 - (gfs_mae_score - mae_penalty_threshold) / mae_penalty_threshold) 
            
        if not qc_vs_opendata_passed:
            logger.warning(f"[InitialWeight] Miner {miner_hotkey}: Failed OpenData QC check. Applying penalty.")
            qc_adjustment_factor *= 0.7

        weight_floor = task_instance.config.get("initial_weight_floor", DEFAULT_INITIAL_WEIGHT_FLOOR)
        weight_cap = task_instance.config.get("initial_weight_cap", DEFAULT_INITIAL_WEIGHT_CAP)
        qc_adjustment_factor = max(weight_floor, min(weight_cap, qc_adjustment_factor))
        logger.debug(f"[InitialWeight] Miner {miner_hotkey}: QC Adjustment Factor = {qc_adjustment_factor:.4f}")

        base_weight = 1.0 # Normalization happens in worker
        if weight_source == "ERA5 History" and historical_weight is not None:
            prelim_weight = historical_weight * qc_adjustment_factor 
            logger.debug(f"[InitialWeight] Using ERA5 history ({historical_weight:.4f}) * QC factor ({qc_adjustment_factor:.4f}) = {prelim_weight:.4f}")
        elif weight_source == "Initial History" and historical_weight is not None:
             prelim_weight = historical_weight * qc_adjustment_factor
             logger.debug(f"[InitialWeight] Using Initial history ({historical_weight:.4f}) * QC factor ({qc_adjustment_factor:.4f}) = {prelim_weight:.4f}")
        else:
            prelim_weight = base_weight * qc_adjustment_factor
            logger.debug(f"[InitialWeight] Using Default base ({base_weight:.4f}) * QC factor ({qc_adjustment_factor:.4f}) = {prelim_weight:.4f}")

        prelim_weight = max(0.0, prelim_weight) 

        logger.info(f"[InitialWeight] Miner {miner_hotkey} (Resp: {response_id}): GFS MAE={gfs_mae_score:.4f}, Prelim Weight={prelim_weight:.4f}")
        # (unnormalized) weight and the GFS MAE score
        return miner_hotkey, prelim_weight, gfs_mae_score 
        
    except Exception as e_calc:
        logger.error(f"[InitialWeight] Error calculating initial weight for {miner_hotkey} (Resp: {response_id}): {e_calc}", exc_info=True)
        return miner_hotkey, None, None
    finally:
        if miner_ds_lazy: miner_ds_lazy.close()
        if miner_sparse_ds is not None: del miner_sparse_ds
        if 'ecmwf_sparse_ds' in locals() and ecmwf_sparse_ds is not None and hasattr(ecmwf_sparse_ds, 'close'): 
            try: ecmwf_sparse_ds.close()
            except Exception: pass
        gc.collect()


async def calculate_era5_miner_score(task_instance: 'WeatherTask', 
                                     response: Dict, 
                                     target_datetimes: List[datetime], 
                                     era5_ds: xr.Dataset
                                     ) -> bool:
    """
    Calculates final score (vs ERA5) for a single miner and stores it.

    Args:
        task_instance: The WeatherTask instance.
        response: DB row containing miner response details (response_id, miner_hotkey, kerchunk_json_url, job_id, run_id).
        target_datetimes: List of specific datetimes to score against.
        era5_ds: Pre-loaded xarray Dataset of ERA5 analysis for target_datetimes.

    Returns:
        True if scoring was successful and stored, False otherwise.
    """
    response_id = response['response_id']
    miner_hotkey = response['miner_hotkey']
    kerchunk_url = response.get('kerchunk_json_url')
    job_id = response.get('job_id')
    run_id = response.get('run_id') 
    miner_sparse_ds = None
    miner_ds_lazy = None
    local_score = None

    if run_id is None:
         logger.error(f"[ScoringMech] Missing run_id in response data for miner {miner_hotkey}, resp_id {response_id}. Cannot store score.")
         return False
         
    try:
        logger.debug(f"[ScoringMech] Final ERA5 scoring for miner {miner_hotkey} (Resp: {response_id})")
        if not kerchunk_url or not job_id: 
            logger.warning(f"[ScoringMech] Missing kerchunk_url or job_id for {miner_hotkey} (Resp: {response_id}). Skipping final score.")
            return False
            
        from ..processing.weather_logic import _request_fresh_token
        token_response = await _request_fresh_token(task_instance, miner_hotkey, job_id)
        if not token_response or 'access_token' not in token_response: 
            logger.warning(f"[ScoringMech] Could not get token for {miner_hotkey} (Resp: {response_id}) for final scoring. Skipping.")
            return False
            
        ref_spec = {'url': kerchunk_url, 'protocol': 'http', 'options': {'headers': {'Authorization': f'Bearer {token_response["access_token"]}'}}}
        miner_ds_lazy = await _open_dataset_lazily(ref_spec)
        if miner_ds_lazy is None: return False
             
        target_times_np = np.array(target_datetimes, dtype='datetime64[ns]')
        miner_sparse_ds = await asyncio.to_thread(
            lambda: miner_ds_lazy.sel(time=target_times_np, method='nearest').load()
        )
        era5_sparse_ds = era5_ds.sel(time=target_times_np, method='nearest')

        common_vars = [v for v in ALL_EXPECTED_VARIABLES if v in miner_sparse_ds and v in era5_sparse_ds]
        if not common_vars: 
            logger.warning(f"[ScoringMech] Miner {miner_hotkey} (Resp: {response_id}): No common vars with ERA5. Skipping final score.")
            return False
        
        miner_dict = {var: miner_sparse_ds[var].values for var in common_vars}
        era5_dict = {var: era5_sparse_ds[var].values for var in common_vars}
        
        local_score = await calculate_rmse(miner_dict, gfs_dict=era5_dict) # Use ERA5 here!
        if local_score is None or not np.isfinite(local_score): 
            logger.warning(f"[ScoringMech] Miner {miner_hotkey} (Resp: {response_id}): Invalid ERA5 score calculated ({local_score}).")
            return False
        
        await task_instance.db_manager.execute("""
            INSERT INTO weather_miner_scores (response_id, run_id, score_type, score, calculation_time)
            VALUES (:resp_id, :run_id, 'era5_rmse', :score, :ts)
            ON CONFLICT (response_id, score_type) DO UPDATE SET 
            score = EXCLUDED.score, calculation_time = EXCLUDED.calculation_time
        """, {
            "resp_id": response_id, "run_id": run_id, "score": local_score, "ts": datetime.now(timezone.utc)
        })
        logger.info(f"[ScoringMech] Stored final ERA5 score ({local_score:.4f}) for miner {miner_hotkey} (Resp ID: {response_id})")
        return True
        
    except Exception as e_score:
        logger.error(f"[ScoringMech] Error during final ERA5 scoring for {miner_hotkey} (Resp: {response_id}): {e_score}", exc_info=False)
        logger.debug(traceback.format_exc())
        return False
    finally:
        if miner_ds_lazy: miner_ds_lazy.close()
        if miner_sparse_ds is not None: del miner_sparse_ds
        gc.collect()


async def calculate_era5_ensemble_score(task_instance: 'WeatherTask', 
                                        ensemble_details: Dict, 
                                        target_datetimes: List[datetime], 
                                        ground_truth_ds: xr.Dataset
                                        ) -> Optional[float]:
    """
    Calculates final score (vs ERA5) for a completed ensemble forecast.

    Args:
        task_instance: The WeatherTask instance.
        ensemble_details: DB row with ensemble info (ensemble_path, ensemble_kerchunk_path).
        target_datetimes: List of specific datetimes to score against.
        ground_truth_ds: Pre-loaded xarray Dataset of ERA5 analysis for target_datetimes.

    Returns:
        Calculated score (float) or None if scoring fails.
    """
    ensemble_score = None
    ensemble_ds = None
    
    try:
        ensemble_path = ensemble_details.get('ensemble_path')
        ensemble_kerchunk_path = ensemble_details.get('ensemble_kerchunk_path')
        
        if ensemble_kerchunk_path:
            logger.debug(f"[ScoringMech] Loading ensemble via Kerchunk: {ensemble_kerchunk_path}")
            ref_spec = {'url': ensemble_kerchunk_path, 'protocol': 'file'}
            ensemble_ds = await _open_dataset_lazily(ref_spec)
            if ensemble_ds: ensemble_ds = await asyncio.to_thread(ensemble_ds.load)
        elif ensemble_path and os.path.exists(ensemble_path):
            logger.debug(f"[ScoringMech] Loading ensemble via NetCDF: {ensemble_path}")
            ensemble_ds = await asyncio.to_thread(xr.open_dataset, ensemble_path)
        else:
            logger.warning(f"[ScoringMech] Ensemble file path/kerchunk missing/invalid: Path='{ensemble_path}', Kerchunk='{ensemble_kerchunk_path}'")
            return None

        if ensemble_ds:
            logger.info("[ScoringMech] Calculating final ensemble score vs ERA5...")
            target_times_np = np.array(target_datetimes, dtype='datetime64[ns]')
            common_vars = [v for v in ALL_EXPECTED_VARIABLES if v in ensemble_ds and v in ground_truth_ds]
            
            if not common_vars:
                 logger.warning(f"[ScoringMech] No common vars between ensemble and ERA5.")
                 return None
                 
            ensemble_subset = ensemble_ds[common_vars].sel(time=target_times_np, method='nearest')
            ground_truth_aligned = ground_truth_ds[common_vars].sel(time=target_times_np, method='nearest')
            
            ensemble_dict = {var: ensemble_subset[var].values for var in common_vars}
            ground_truth_dict = {var: ground_truth_aligned[var].values for var in common_vars}
            
            calculated_score = await calculate_rmse(ensemble_dict, ground_truth_dict)
            
            if calculated_score is not None and np.isfinite(calculated_score):
                ensemble_score = float(calculated_score) 
                logger.info(f"[ScoringMech] Calculated final ensemble score (RMSE): {ensemble_score:.4f}")
            else:
                 logger.warning(f"[ScoringMech] Invalid ensemble score calculated ({calculated_score}).")
                 
        else:
            logger.warning("[ScoringMech] Failed to load ensemble dataset.")

    except Exception as score_err:
        logger.error(f"[ScoringMech] Error scoring ensemble: {score_err}", exc_info=True)
    finally:
            if ensemble_ds and hasattr(ensemble_ds, 'close'): ensemble_ds.close()
            gc.collect()
            
    return ensemble_score
