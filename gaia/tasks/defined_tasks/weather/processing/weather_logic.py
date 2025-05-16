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
import traceback
import ipaddress
from typing import TYPE_CHECKING, Any, Optional, Dict, List, Tuple
if TYPE_CHECKING:
    from ..weather_task import WeatherTask
from ..utils.remote_access import open_verified_remote_zarr_dataset
from ..utils.era5_api import fetch_era5_data
from ..utils.gfs_api import fetch_gfs_analysis_data, GFS_SURFACE_VARS, GFS_ATMOS_VARS
from ..utils.hashing import _compute_analysis_profile, ANALYSIS_LOG_DIR
from ..weather_scoring.ensemble import _open_dataset_lazily, ALL_EXPECTED_VARIABLES
from ..weather_scoring.metrics import calculate_rmse
from ..weather_scoring_mechanism import calculate_era5_ensemble_score
from ..schemas.weather_outputs import WeatherKerchunkResponseData
from fiber.logging_utils import get_logger

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

async def build_score_row(task_instance: 'WeatherTask', forecast_run_id: int, ground_truth_ds: Optional[xr.Dataset] = None):
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
                logger.info(f"[build_score_row] Found completed ensemble for run {forecast_run_id}. Attempting to score vs ERA5.")
                local_ground_truth_ds = ground_truth_ds
                close_gt_later = False
                
                try:
                    if local_ground_truth_ds is None:
                        logger.info("[build_score_row] Ground truth not passed, fetching ERA5 for ensemble scoring...")
                        if not run:
                            run = await task_instance.db_manager.fetch_one("SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :run_id", {"run_id": forecast_run_id})
                        
                        if run:
                            sparse_lead_hours_final = task_instance.config.get('final_scoring_lead_hours', [120, 168])
                            target_datetimes_final = [run['gfs_init_time_utc'] + timedelta(hours=h) for h in sparse_lead_hours_final]
                            local_ground_truth_ds = await get_ground_truth_data(task_instance, run['gfs_init_time_utc'], np.array(target_datetimes_final, dtype='datetime64[ns]'))
                            close_gt_later = True
                        else:
                            logger.error(f"[build_score_row] Cannot fetch GT for ensemble, run details missing for {forecast_run_id}")
                    
                    if local_ground_truth_ds:
                        target_datetimes_for_scoring = [pd.Timestamp(t).to_pydatetime(warn=False).replace(tzinfo=timezone.utc) for t in local_ground_truth_ds.time.values]
                        
                        logger.info(f"[build_score_row] Calling calculate_era5_ensemble_score...")
                        ensemble_score = await calculate_era5_ensemble_score(
                            task_instance=task_instance, 
                            ensemble_details=ensemble_details, 
                            target_datetimes=target_datetimes_for_scoring,
                            ground_truth_ds=local_ground_truth_ds
                        )
                        if ensemble_score is not None:
                            logger.info(f"[build_score_row] Received ensemble score: {ensemble_score:.4f}")
                        else:
                            logger.warning(f"[build_score_row] Ensemble scoring failed or returned None.")
                    else:
                        logger.warning(f"[build_score_row] Could not retrieve/receive ground truth data. Cannot score ensemble.")
                            
                except Exception as score_err:
                    logger.error(f"[build_score_row] Error during ensemble scoring call: {score_err}", exc_info=True)
                finally:
                    if close_gt_later and local_ground_truth_ds and hasattr(local_ground_truth_ds, 'close'): 
                        local_ground_truth_ds.close()
                    gc.collect()
            else:
                logger.info(f"[build_score_row] No completed ensemble found for run {forecast_run_id}. Skipping ensemble scoring.")

        except Exception as e_ens_score:
            logger.error(f"[build_score_row] Unexpected error during ensemble scoring setup: {e_ens_score}", exc_info=True)

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

    era5_cache_dir = Path(task_instance.config.get('era5_cache_dir', './era5_cache'))
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

async def _request_fresh_token(task_instance: 'WeatherTask', miner_hotkey: str, job_id: str) -> Optional[Tuple[str, str, str]]:
    """Requests a fresh JWT, Zarr URL, and manifest_content_hash from the miner."""
    logger.info(f"[VerifyLogic] Requesting fresh token/manifest_hash for job {job_id} from miner {miner_hotkey[:12]}...")
    forecast_request_payload = {"nonce": str(uuid.uuid4()), "data": {"job_id": job_id} }
    endpoint_to_call = "/weather-kerchunk-request"
    try:
        all_responses = await task_instance.validator.query_miners(
            payload=forecast_request_payload, endpoint=endpoint_to_call
        )
        response_dict = all_responses.get(miner_hotkey)
        if not response_dict: return None
        if response_dict.get("status_code") == 200:
            miner_response_data = json.loads(response_dict['text'])
            if miner_response_data.get("status") == "completed":
                token = miner_response_data.get("access_token")
                zarr_store_relative_url = miner_response_data.get("zarr_store_url") 
                manifest_content_hash = miner_response_data.get("verification_hash") 
                if token and zarr_store_relative_url and manifest_content_hash:
                    ip_val = response_dict['ip']
                    port_val = response_dict['port']
                    ip_str = str(ipaddress.ip_address(int(ip_val))) if isinstance(ip_val, (str, int)) and str(ip_val).isdigit() else ip_val
                    try: ipaddress.ip_address(ip_str) 
                    except ValueError: logger.warning(f"Miner IP {ip_str} not standard, assuming hostname.")
                    miner_base_url = f"https://{ip_str}:{port_val}"
                    full_zarr_url = miner_base_url.rstrip('/') + "/" + zarr_store_relative_url.lstrip('/')
                    logger.info(f"[VerifyLogic] Success: Token, URL: {full_zarr_url}, ManifestHash: {manifest_content_hash[:10]}...")
                    return token, full_zarr_url, manifest_content_hash
    except Exception as e: logger.error(f"Unhandled exception in _request_fresh_token: {e!r}", exc_info=True)
    return None

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
        SELECT id as job_id, status, target_netcdf_path as zarr_store_path
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
    Now stores the Zarr path in both target_netcdf_path and kerchunk_json_path fields for compatibility.
    (Intended for Miner-side usage)
    """
    if task_instance.node_type != 'miner':
        logger.error("update_job_paths called on non-miner node.")
        return False
        
    logger.debug(f"[Job {job_id}] Updating miner job paths with Zarr store path: {target_netcdf_path}")
    try:
        query = """
        UPDATE weather_miner_jobs
        SET target_netcdf_path = :zarr_path,
            kerchunk_json_path = :zarr_path,
            verification_hash = :hash,
            updated_at = :updated_at
        WHERE id = :job_id
        """
        params = {
            "job_id": job_id,
            "zarr_path": target_netcdf_path,
            "hash": verification_hash,
            "updated_at": datetime.now(timezone.utc)
        }
        await task_instance.db_manager.execute(query, params)
        logger.info(f"Updated miner job {job_id} with Zarr store path and verification hash")
        return True
    except Exception as e:
        logger.error(f"Error updating miner job paths for {job_id}: {e}")
        return False

async def verify_miner_response(task_instance: 'WeatherTask', run_details: Dict, response_details: Dict):
    """Handles fetching Zarr store info and verifying a single miner response's manifest integrity."""
    run_id = run_details['id']
    response_id = response_details['id']
    miner_hotkey = response_details['miner_hotkey'] 
    job_id = response_details.get('job_id') 
    
    if not job_id: 
        logger.error(f"[VerifyLogic, Resp {response_id}] Missing job_id for miner {miner_hotkey}. Cannot verify.")
        await task_instance.db_manager.execute("UPDATE weather_miner_responses SET status = 'verification_error', error_message = 'Internal: Missing job_id' WHERE id = :id", {"id": response_id})
        return 

    logger.info(f"[VerifyLogic] Verifying response {response_id} from {miner_hotkey} (Miner Job ID: {job_id})")
    
    access_token = None
    zarr_store_url = None
    claimed_manifest_content_hash = None 
    verified_dataset: Optional[xr.Dataset] = None
    
    try:
        token_data_tuple = await _request_fresh_token(task_instance, miner_hotkey, job_id)
        if token_data_tuple is None: 
            raise ValueError(f"Failed to get access token/manifest details for {miner_hotkey} job {job_id}")

        access_token, zarr_store_url, claimed_manifest_content_hash = token_data_tuple
        logger.info(f"[VerifyLogic, Resp {response_id}] Unpacked token data. URL: {zarr_store_url}, Manifest Hash: {claimed_manifest_content_hash[:10] if claimed_manifest_content_hash else 'N/A'}...")
            
        if not all([access_token, zarr_store_url, claimed_manifest_content_hash]):
            err_details = f"Token:Set='{bool(access_token)}', URL:'{zarr_store_url}', ManifestHash:Set='{bool(claimed_manifest_content_hash)}'"
            raise ValueError(f"Critical information missing after token request: {err_details}")

        await task_instance.db_manager.execute("""
            UPDATE weather_miner_responses
            SET kerchunk_json_url = :url, verification_hash_claimed = :hash, status = 'verifying_manifest'
            WHERE id = :id
        """, {"id": response_id, "url": zarr_store_url, "hash": claimed_manifest_content_hash})

        logger.info(f"[VerifyLogic, Resp {response_id}] Attempting to open VERIFIED Zarr store: {zarr_store_url}...")
        
        storage_opts = {"headers": {"Authorization": f"Bearer {access_token}"}, "ssl": False}
        verification_timeout_seconds = task_instance.config.get('verification_timeout_seconds', 300) 
        
        verified_dataset = await asyncio.wait_for(
            open_verified_remote_zarr_dataset(
                zarr_store_url=zarr_store_url,
                claimed_manifest_content_hash=claimed_manifest_content_hash, 
                miner_hotkey_ss58=miner_hotkey,
                storage_options=storage_opts,
                job_id=job_id 
            ),
            timeout=verification_timeout_seconds 
        )
        
        verification_succeeded_bool = verified_dataset is not None
        db_status_after_verification = ""
        error_message_for_db = None

        if verification_succeeded_bool:
            db_status_after_verification = "verified_manifest_store_opened"
            logger.info(f"[VerifyLogic, Resp {response_id}] Manifest verified & Zarr store opened with on-read checks.")
            try:
                logger.debug(f"[VerifyLogic, Resp {response_id}] Verified dataset keys: {list(verified_dataset.keys()) if verified_dataset else 'N/A'}")
                
                if verified_dataset:
                    logger.info(f"[VerifyLogic, Resp {response_id}] Running performance profile for verified dataset from miner {miner_hotkey} (Job {job_id}).")
                    
                    profile_metadata = {
                        "time": [run_details.get('gfs_init_time_utc', datetime.now(timezone.utc))], 
                        "source_model": f"miner_verified_{miner_hotkey[:12]}",
                        "resolution": verified_dataset.attrs.get("resolution", 0.25) 
                    }
                    profile_variables = list(verified_dataset.data_vars.keys()) if verified_dataset.data_vars else []

                    profile_job_id = f"resp_{response_id}_job_{job_id}"

                    try:
                        loop = asyncio.get_running_loop()
                        analysis_log_dict = await loop.run_in_executor(
                            None, 
                            _compute_analysis_profile,
                            verified_dataset,
                            profile_metadata,
                            profile_variables,
                            claimed_manifest_content_hash, 
                            profile_job_id, 
                            zarr_store_url 
                        )
                        if analysis_log_dict:
                             logger.info(f"[VerifyLogic, Resp {response_id}] Performance profile completed. Log status: {analysis_log_dict.get('status')}, saved to {ANALYSIS_LOG_DIR}")
                        else:
                             logger.warning(f"[VerifyLogic, Resp {response_id}] Performance profile call returned None or empty.")
                    except Exception as e_profile:
                        logger.error(f"[VerifyLogic, Resp {response_id}] Error during _compute_analysis_profile for {miner_hotkey} (Job {job_id}): {e_profile}", exc_info=True)
            except Exception as e_ds_ops:
                logger.warning(f"[VerifyLogic, Resp {response_id}] Minor error during initial ops on verified dataset or profiling: {e_ds_ops}")
                if error_message_for_db: error_message_for_db += f"; Post-verify op error: {e_ds_ops}"
                else: error_message_for_db = f"Post-manifest op error: {e_ds_ops}"
        else:
            db_status_after_verification = "failed_manifest_verification"
            error_message_for_db = "Manifest verification failed. See verification_logs for details."
            logger.error(f"[VerifyLogic, Resp {response_id}] Manifest verification failed or could not open verified Zarr store.")

        await task_instance.db_manager.execute(""" 
            UPDATE weather_miner_responses 
            SET verification_passed = :verified, status = :new_status, error_message = COALESCE(:err_msg, error_message)
            WHERE id = :id
        """, {"id": response_id, "verified": verification_succeeded_bool, "new_status": db_status_after_verification, "err_msg": error_message_for_db})
        
    except asyncio.TimeoutError:
        logger.error(f"[VerifyLogic, Resp {response_id}] Verified open process timed out for {miner_hotkey} (Job: {job_id}).")
        await task_instance.db_manager.execute("UPDATE weather_miner_responses SET status = 'verification_timeout', error_message = 'Verified open process timed out', verification_passed = FALSE WHERE id = :id", {"id": response_id})
    except Exception as err:
        logger.error(f"[VerifyLogic, Resp {response_id}] Error verifying response from {miner_hotkey} (Job: {job_id}): {err!r}", exc_info=True) 
        await task_instance.db_manager.execute("""
            UPDATE weather_miner_responses SET status = 'verification_error', error_message = :msg, verification_passed = FALSE
            WHERE id = :id
        """, {"id": response_id, "msg": f"Outer verify_miner_response error: {str(err)}"})
    finally:
        if verified_dataset is not None:
            try: verified_dataset.close()
            except Exception: pass
        gc.collect()
        