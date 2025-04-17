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

from .weather_scoring.metrics import calculate_rmse
from .weather_scoring.ensemble import _open_dataset_lazily, ALL_EXPECTED_VARIABLES

logger = get_logger(__name__)

async def calculate_gfs_score_and_weight(task_instance: 'WeatherTask', 
                                        response: Dict, 
                                        target_datetimes: List[datetime], 
                                        gfs_analysis_ds: xr.Dataset
                                        ) -> Tuple[str, Optional[float], Optional[float]]:
    """
    Calculates initial score (vs GFS) and weight for a single miner response.
    Handles data loading, alignment, metric calculation.

    Args:
        task_instance: The WeatherTask instance.
        response: DB row containing miner response details (response_id, miner_hotkey, kerchunk_json_url, job_id).
        target_datetimes: List of specific datetimes to score against.
        gfs_analysis_ds: Pre-loaded xarray Dataset of GFS analysis for target_datetimes.

    Returns:
        Tuple: (miner_hotkey, weight, score). Weight and score are None on failure.
    """
    response_id = response['response_id']
    miner_hotkey = response['miner_hotkey']
    kerchunk_url = response.get('kerchunk_json_url')
    job_id = response.get('job_id')
    run_id = response.get('run_id')
    miner_sparse_ds = None
    miner_ds_lazy = None
    local_weight = None
    local_score = None

    try:
        logger.debug(f"[ScoringMech] Initial GFS scoring for miner {miner_hotkey} (Resp: {response_id})")
        
        if not kerchunk_url or not job_id:
             logger.warning(f"[ScoringMech] Missing kerchunk_url or job_id for {miner_hotkey} (Resp: {response_id}). Skipping.")
             return miner_hotkey, None, None
             
        from ..processing.weather_logic import _request_fresh_token
        token_response = await _request_fresh_token(task_instance, miner_hotkey, job_id)
        if not token_response or 'access_token' not in token_response:
            logger.warning(f"[ScoringMech] Could not get token for {miner_hotkey} (Resp: {response_id}). Skipping initial score.")
            return miner_hotkey, None, None
            
        ref_spec = {
            'url': kerchunk_url,
            'protocol': 'http',
            'options': {'headers': {'Authorization': f'Bearer {token_response["access_token"]}'}}
        }
        
        miner_ds_lazy = await _open_dataset_lazily(ref_spec)
        if miner_ds_lazy is None:
             logger.warning(f"[ScoringMech] Failed to open lazy dataset for {miner_hotkey} (Resp: {response_id}). Skipping.")
             return miner_hotkey, None, None
             
        target_times_np = np.array(target_datetimes, dtype='datetime64[ns]')
        miner_sparse_ds = await asyncio.to_thread(
            lambda: miner_ds_lazy.sel(time=target_times_np, method='nearest').load()
        )
        
        gfs_sparse_ds = gfs_analysis_ds.sel(time=target_times_np, method='nearest')

        common_vars = [v for v in ALL_EXPECTED_VARIABLES if v in miner_sparse_ds and v in gfs_sparse_ds]
        if not common_vars:
             logger.warning(f"[ScoringMech] Miner {miner_hotkey} (Resp: {response_id}): No common vars with GFS. Skipping score.")
             return miner_hotkey, None, None
        
        miner_dict = {var: miner_sparse_ds[var].values for var in common_vars}
        gfs_dict = {var: gfs_sparse_ds[var].values for var in common_vars}
        
        local_score = await calculate_rmse(miner_dict, gfs_dict)
        if local_score is None or not np.isfinite(local_score):
             logger.warning(f"[ScoringMech] Miner {miner_hotkey} (Resp: {response_id}): Invalid score calculated ({local_score}). Skipping weight.")
             return miner_hotkey, None, None
             
        local_weight = 1.0 / (1.0 + max(0, local_score))
        
        logger.info(f"[ScoringMech] Miner {miner_hotkey} (Resp: {response_id}): GFS Score (RMSE)={local_score:.4f}, Weight={local_weight:.4f}")
        return miner_hotkey, local_weight, local_score
        
    except Exception as e_score:
        logger.error(f"[ScoringMech] Error during initial GFS scoring for {miner_hotkey} (Resp: {response_id}): {e_score}", exc_info=False)
        logger.debug(traceback.format_exc())
        return miner_hotkey, None, None
    finally:
        if miner_ds_lazy: miner_ds_lazy.close()
        if miner_sparse_ds is not None: del miner_sparse_ds
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
