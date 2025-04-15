import asyncio
import gc
import os
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
import uuid
import numpy as np
import xarray as xr
import pandas as pd
import fsspec
import jwt

from fiber.logging_utils import get_logger
from typing import TYPE_CHECKING, Any, Optional, Dict, List
if TYPE_CHECKING:
    from ..weather_task import WeatherTask
from ..utils.era5_api import fetch_era5_data
from ..scoring.ensemble import _open_dataset_lazily, ALL_EXPECTED_VARIABLES
from ..scoring.metrics import calculate_rmse

logger = get_logger(__name__)
async def _update_run_status(task_instance: 'WeatherTask', run_id: int, status: str, error_message: Optional[str] = None, gfs_metadata: Optional[dict] = None):
    """Helper to update the forecast run status and optionally other fields."""
    logger.debug(f"[Run {run_id}] Updating run status to '{status}'.")
    update_fields = ["status = :status"]
    params = {"run_id": run_id, "status": status}

    if error_message is not None:
        update_fields.append("error_message = :error_msg")
        params["error_msg"] = error_message
    if gfs_metadata is not None:
            update_fields.append("gfs_input_metadata = :gfs_meta")
            params["gfs_meta"] = json.dumps(gfs_metadata, default=str)
    if status in ["completed", "error", "scored", "final_scoring_failed", "ensemble_failed", "initial_scoring_failed", "verification_failed"]:
            update_fields.append("completion_time = :comp_time")
            params["comp_time"] = datetime.now(timezone.utc)

    query = f"""
        UPDATE weather_forecast_runs
        SET {', '.join(update_fields)}
        WHERE id = :run_id
    """
    try:
        await task_instance.db_manager.execute(query, params)
    except Exception as db_err:
        logger.error(f"[Run {run_id}] Failed to update run status to '{status}': {db_err}", exc_info=True)

async def build_score_row(task_instance: 'WeatherTask', forecast_run_id: int):
    """Builds the aggregate score row using FINAL (ERA5) scores."""
    logger.info(f"[build_score_row] Building final score row for forecast run {forecast_run_id}")

    try:
        run_query = "SELECT * FROM weather_forecast_runs WHERE id = :run_id"
        run = await task_instance.db_manager.fetch_one(run_query, {"run_id": forecast_run_id})
        if not run:
            logger.error(f"[build_score_row] Forecast run {forecast_run_id} not found.")
            return

        scores_query = """
        SELECT ms.miner_hotkey, ms.score as final_score -- Fetch the ERA5 score
        FROM weather_miner_scores ms
        JOIN weather_miner_responses mr ON ms.response_id = mr.id
        WHERE mr.run_id = :run_id
            AND mr.verification_passed = TRUE
            AND ms.score_type = 'era5_rmse' -- Specify final score type
        """

        final_scores = await task_instance.db_manager.fetch_all(scores_query, {"run_id": forecast_run_id})

        miner_count = len(final_scores)
        if miner_count == 0:
            logger.warning(f"[build_score_row] No final ERA5 scores found for run {forecast_run_id}. Cannot build score row.")
            return

        avg_score = sum(s['final_score'] for s in final_scores) / miner_count
        max_score = max(s['final_score'] for s in final_scores)
        min_score = min(s['final_score'] for s in final_scores)
        best_miner = min(final_scores, key=lambda s: s['final_score'])
        best_miner_hotkey = best_miner['miner_hotkey']

        ensemble_score = None
        ensemble_details = None
        try:
            ensemble_query = """
            SELECT ef.id, ef.ensemble_path, ef.ensemble_kerchunk_path
            FROM weather_ensemble_forecasts ef
            WHERE ef.forecast_run_id = :run_id AND ef.status = 'completed'
            LIMIT 1
            """
            ensemble_details = await task_instance.db_manager.fetch_one(ensemble_query, {"run_id": forecast_run_id})

            if ensemble_details and (ensemble_details.get('ensemble_path') or ensemble_details.get('ensemble_kerchunk_path')):
                logger.info(f"[build_score_row] Found completed ensemble for run {forecast_run_id}. Scoring vs ERA5.")
                ensemble_ds = None
                ground_truth_ds = None
                try:
                    if ensemble_details.get('ensemble_kerchunk_path'):
                            logger.debug(f"Loading ensemble via Kerchunk: {ensemble_details['ensemble_kerchunk_path']}")
                            ref_spec = {'url': ensemble_details['ensemble_kerchunk_path'], 'protocol': 'file'}
                            ensemble_ds = await _open_dataset_lazily(ref_spec)
                            if ensemble_ds: ensemble_ds = await asyncio.to_thread(ensemble_ds.load)
                    elif ensemble_details.get('ensemble_path') and os.path.exists(ensemble_details['ensemble_path']):
                        logger.debug(f"Loading ensemble via NetCDF: {ensemble_details['ensemble_path']}")
                        ensemble_ds = await asyncio.to_thread(xr.open_dataset, ensemble_details['ensemble_path'])
                    else:
                            logger.warning(f"[build_score_row] Ensemble file path/kerchunk missing/invalid for run {forecast_run_id}.")

                    if ensemble_ds:
                        logger.info("[build_score_row] Fetching ERA5 ground truth for ensemble scoring...")
                        sparse_lead_hours_final = getattr(task_instance.config, 'final_scoring_lead_hours', [120, 168])
                        target_datetimes_final = [run['gfs_init_time_utc'] + timedelta(hours=h) for h in sparse_lead_hours_final]
                        times_in_ensemble_dt = [pd.Timestamp(t).to_pydatetime(warn=False).replace(tzinfo=timezone.utc) for t in ensemble_ds.time.values]
                        target_datetimes_in_ensemble = [t for t in target_datetimes_final if t in times_in_ensemble_dt]

                        if not target_datetimes_in_ensemble:
                            logger.warning(f"[build_score_row] Ensemble dataset for run {forecast_run_id} does not contain the target scoring times. Skipping. Target={target_datetimes_final}, Ensemble={times_in_ensemble_dt}")
                        else:
                            ground_truth_ds = await get_ground_truth_data(task_instance, run['gfs_init_time_utc'], np.array(target_datetimes_in_ensemble, dtype='datetime64[ns]'))

                            if ground_truth_ds:
                                logger.info("[build_score_row] Calculating final ensemble score vs ERA5...")
                                common_vars = [v for v in ALL_EXPECTED_VARIABLES if v in ensemble_ds and v in ground_truth_ds]

                                if not common_vars:
                                        logger.warning(f"[build_score_row] No common vars between ensemble and ERA5 for run {forecast_run_id}.")
                                else:
                                    ensemble_subset = ensemble_ds[common_vars].sel(time=target_datetimes_in_ensemble, method='nearest')
                                    ground_truth_aligned = ground_truth_ds[common_vars].sel(time=target_datetimes_in_ensemble, method='nearest')
                                    ensemble_dict = {var: ensemble_subset[var].values for var in common_vars}
                                    ground_truth_dict = {var: ground_truth_aligned[var].values for var in common_vars}
                                    calculated_score = await calculate_rmse(ensemble_dict, ground_truth_dict)
                                    if calculated_score is not None and np.isfinite(calculated_score):
                                        ensemble_score = float(calculated_score)
                                        logger.info(f"[build_score_row] Calculated final ensemble score (RMSE) for run {forecast_run_id}: {ensemble_score:.4f}")
                                    else:
                                            logger.warning(f"[build_score_row] Invalid ensemble score calculated ({calculated_score}).")
                            else:
                                logger.warning(f"[build_score_row] Could not retrieve ERA5 ground truth for run {forecast_run_id}. Cannot score ensemble.")

                except Exception as score_err:
                    logger.error(f"[build_score_row] Error scoring ensemble for run {forecast_run_id}: {score_err}", exc_info=True)
                finally:
                        if ensemble_ds and hasattr(ensemble_ds, 'close'): ensemble_ds.close()
                        if ground_truth_ds and hasattr(ground_truth_ds, 'close'): ground_truth_ds.close()
                        gc.collect()
            else:
                logger.info(f"[build_score_row] No completed ensemble found for run {forecast_run_id}. Skipping ensemble scoring.")

        except Exception as e_ens_score:
                logger.error(f"[build_score_row] Unexpected error during ensemble scoring setup for run {forecast_run_id}: {e_ens_score}", exc_info=True)

        # Build Final Score Data
        score_data = {
            "task_name": "weather", "subtask_name": "forecast",
            "run_id": str(forecast_run_id), "run_timestamp": run['gfs_init_time_utc'].isoformat(),
            "avg_score": float(avg_score), "max_score": float(max_score), "min_score": float(min_score),
            "miner_count": miner_count, "best_miner": best_miner_hotkey, "ensemble_score": ensemble_score,
            "metadata": {
                "gfs_init_time": run['gfs_init_time_utc'].isoformat(), "final_score_miner_count": miner_count,
                "has_ensemble": ensemble_details is not None, "ensemble_scored": ensemble_score is not None
            }
        }
        
        exists_query = "SELECT id FROM score_table WHERE task_name = 'weather' AND run_id = :run_id"
        existing = await task_instance.db_manager.fetch_one(exists_query, {"run_id": str(forecast_run_id)})
        db_params = {k: v for k, v in score_data.items() if k not in ['task_name', 'subtask_name', 'run_id', 'run_timestamp']}
        db_params["metadata"] = json.dumps(score_data['metadata'])

        if existing:
            update_query = "UPDATE score_table SET avg_score = :avg_score, max_score = :max_score, min_score = :min_score, miner_count = :miner_count, best_miner = :best_miner, ensemble_score = :ensemble_score, metadata = :metadata WHERE id = :id"
            db_params["id"] = existing['id']
            await task_instance.db_manager.execute(update_query, db_params)
            logger.info(f"[build_score_row] Updated final score row for run {forecast_run_id}")
        else:
            insert_query = "INSERT INTO score_table (task_name, subtask_name, run_id, run_timestamp, avg_score, max_score, min_score, miner_count, best_miner, ensemble_score, metadata) VALUES (:task_name, :subtask_name, :run_id, :run_timestamp, :avg_score, :max_score, :min_score, :miner_count, :best_miner, :ensemble_score, :metadata)"
            db_params["task_name"] = score_data['task_name']
            db_params["subtask_name"] = score_data['subtask_name']
            db_params["run_id"] = score_data['run_id']
            db_params["run_timestamp"] = score_data['run_timestamp']
            await task_instance.db_manager.execute(insert_query, db_params)
            logger.info(f"[build_score_row] Inserted final score row for run {forecast_run_id}")

    except Exception as e:
        logger.error(f"[build_score_row] Error building final score row for run {forecast_run_id}: {e}", exc_info=True)

async def get_ground_truth_data(task_instance: 'WeatherTask', init_time: datetime, forecast_times: np.ndarray) -> Optional[xr.Dataset]:
    """
    Fetches ERA5 ground truth data corresponding to the forecast times using the CDS API.

    Args:
        task_instance: The WeatherTask instance.
        init_time: The initialization time of the forecast run (used for logging/context).
        forecast_times: Numpy array of datetime64 objects for the forecast timesteps.

    Returns:
        An xarray.Dataset containing the ERA5 data, or None if retrieval fails.
    """
    logger.info(f"Attempting to fetch ERA5 ground truth for {len(forecast_times)} times starting near {init_time}")
    try:
        target_datetimes = [pd.Timestamp(ts).to_pydatetime(warn=False) for ts in forecast_times]
        target_datetimes = [t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t.astimezone(timezone.utc) for t in target_datetimes]
    except Exception as e:
        logger.error(f"Failed to convert forecast_times to Python datetimes: {e}")
        return None

    era5_cache_dir = Path(getattr(task_instance.config, 'era5_cache_dir', './era5_cache'))
    try:
        ground_truth_ds = await fetch_era5_data(
            target_times=target_datetimes,
            cache_dir=era5_cache_dir
        )
        if ground_truth_ds is None:
            logger.warning("fetch_era5_data returned None. Ground truth unavailable.")
            return None
        else:
            logger.info("Successfully fetched/loaded ERA5 ground truth data.")
            return ground_truth_ds
    except Exception as e:
        logger.error(f"Error occurred during get_ground_truth_data: {e}", exc_info=True)
        return None

async def _trigger_initial_scoring(task_instance: 'WeatherTask', run_id: int):
    """Queues a run for initial scoring based on GFS analysis."""
    if not task_instance.initial_scoring_worker_running:
        logger.warning("Initial scoring worker not running. Cannot queue run.")
        await _update_run_status(task_instance, run_id, "initial_scoring_skipped")
        return
    logger.info(f"Queueing run {run_id} for initial GFS-based scoring.")
    await task_instance.initial_scoring_queue.put(run_id)
    await _update_run_status(task_instance, run_id, "initial_scoring_queued")



async def _request_fresh_token(task_instance: 'WeatherTask', miner_hotkey: str, job_id: str) -> Optional[Dict[str, Any]]:
    """Request a fresh access token for a specific job from a miner."""
    if task_instance.validator is None:
        logger.error("Validator instance not available in WeatherTask. Cannot request token.")
        return None
        
    try:
        kerchunk_request_payload = {
            "nonce": str(uuid.uuid4()),
            "data": {"job_id": job_id}
        }
        
        logger.debug(f"Requesting token for job {job_id} from miner {miner_hotkey}")
        response = await task_instance.validator.query_miner(
            miner_hotkey=miner_hotkey,
            payload=kerchunk_request_payload,
            endpoint="/weather-kerchunk-request"
        )
        
        if response and response.get("status") == "completed" and response.get("access_token"):
            logger.debug(f"Successfully received token for job {job_id} from {miner_hotkey}")
            return {"access_token": response.get("access_token")}
        else:
             logger.warning(f"Failed to get valid token response for job {job_id} from {miner_hotkey}. Response: {response}")
             return None
             
    except Exception as e:
        logger.error(f"Error requesting token for job {job_id} from {miner_hotkey}: {e}", exc_info=True)
        return None

async def _open_forecast_dataset(task_instance: 'WeatherTask', kerchunk_url: str, headers: dict) -> Optional[xr.Dataset]:
    """ (DEPRECATED - Use _open_dataset_lazily from ensemble.py) Opens a forecast dataset using kerchunk JSON."""
    logger.warning("_open_forecast_dataset is deprecated. Use _open_dataset_lazily from weather_scoring.ensemble instead.")
    ref_spec = {
        'url': kerchunk_url,
        'protocol': 'http',
        'options': {'headers': headers}
    }
    return await _open_dataset_lazily(ref_spec)

async def get_job_by_gfs_init_time(task_instance: 'WeatherTask', gfs_init_time_utc: datetime) -> Optional[Dict[str, Any]]:
    """
    Check if a job exists for the given GFS initialization time.
    (Intended for Miner-side usage)
    """
    if task_instance.node_type != 'miner':
        logger.error("get_job_by_gfs_init_time called on non-miner node.")
        return None
        
    try:
        query = """
        SELECT id as job_id, status, target_netcdf_path, kerchunk_json_path 
        FROM weather_miner_jobs
        WHERE gfs_init_time_utc = :gfs_init_time
        ORDER BY id DESC
        LIMIT 1
        """
        if not hasattr(task_instance, 'db_manager') or task_instance.db_manager is None:
             logger.error("DB manager not available in get_job_by_gfs_init_time")
             return None
             
        job = await task_instance.db_manager.fetch_one(query, {"gfs_init_time": gfs_init_time_utc})
        return job
    except Exception as e:
        logger.error(f"Error checking for existing job with GFS init time {gfs_init_time_utc}: {e}")
        return None

async def update_job_status(task_instance: 'WeatherTask', job_id: str, status: str, error_message: Optional[str] = None):
    """
    Update the status of a job in the miner's database.
    (Intended for Miner-side usage)
    """
    if task_instance.node_type != 'miner':
        logger.error("update_job_status called on non-miner node.")
        return False
        
    logger.debug(f"[Job {job_id}] Updating miner job status to '{status}'.")
    try:
        update_fields = ["status = :status", "updated_at = :updated_at"]
        params = {
            "job_id": job_id,
            "status": status,
            "updated_at": datetime.now(timezone.utc)
        }
        
        if status == "processing":
            update_fields.append("processing_start_time = COALESCE(processing_start_time, :proc_start)")
            params["proc_start"] = datetime.now(timezone.utc)
        elif status == "completed":
            update_fields.append("processing_end_time = :proc_end")
            params["proc_end"] = datetime.now(timezone.utc)
        
        if error_message:
            update_fields.append("error_message = :error_msg")
            params["error_msg"] = error_message
            
        query = f"""
        UPDATE weather_miner_jobs
        SET {", ".join(update_fields)}
        WHERE id = :job_id -- Assuming miner job table uses job_id as PK or unique ID
        """
        
        await task_instance.db_manager.execute(query, params)
        logger.info(f"Updated miner job {job_id} status to {status}")
        return True
    except Exception as e:
        logger.error(f"Error updating miner job status for {job_id}: {e}")
        return False

async def update_job_paths(task_instance: 'WeatherTask', job_id: str, target_netcdf_path: str, kerchunk_json_path: str, verification_hash: str):
    """
    Update the file paths and verification hash for a completed job in the miner's database.
    (Intended for Miner-side usage)
    """
    if task_instance.node_type != 'miner':
        logger.error("update_job_paths called on non-miner node.")
        return False
        
    logger.debug(f"[Job {job_id}] Updating miner job paths.")
    try:
        query = """
        UPDATE weather_miner_jobs
        SET target_netcdf_path = :netcdf_path,
            kerchunk_json_path = :kerchunk_path,
            verification_hash = :hash,
            updated_at = :updated_at
        WHERE id = :job_id
        """
        params = {
            "job_id": job_id,
            "netcdf_path": target_netcdf_path,
            "kerchunk_path": kerchunk_json_path,
            "hash": verification_hash,
            "updated_at": datetime.now(timezone.utc)
        }
        await task_instance.db_manager.execute(query, params)
        logger.info(f"Updated miner job {job_id} with file paths and verification hash")
        return True
    except Exception as e:
        logger.error(f"Error updating miner job paths for {job_id}: {e}")
        return False


        