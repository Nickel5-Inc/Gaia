import asyncio
import gc
import os
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
import xskillscore as xs
from typing import TYPE_CHECKING, Any, Optional, Dict, List, Tuple
if TYPE_CHECKING:
    from ..weather_task import WeatherTask

# High-performance JSON operations for weather data
try:
    from gaia.utils.performance import dumps, loads
except ImportError:
    import json
    def dumps(obj, **kwargs):
        return json.dumps(obj, **kwargs) 
    def loads(s):
        return json.loads(s)
from ..utils.remote_access import open_verified_remote_zarr_dataset
from ..utils.era5_api import fetch_era5_data
from ..utils.gfs_api import fetch_gfs_data, GFS_SURFACE_VARS, GFS_ATMOS_VARS
from ..utils.variable_maps import AURORA_TO_GFS_VAR_MAP
from ..utils.hashing import compute_verification_hash
from ..weather_scoring.metrics import calculate_rmse, calculate_mse_skill_score, calculate_acc, calculate_bias_corrected_forecast, _calculate_latitude_weights, perform_sanity_checks
from fiber.logging_utils import get_logger
from ..weather_scoring.scoring import VARIABLE_WEIGHTS

logger = get_logger(__name__)
async def _update_run_status(task_instance: 'WeatherTask', run_id: int, status: str, error_message: Optional[str] = None, gfs_metadata: Optional[dict] = None):
    """Helper to update the forecast run status and optionally other fields."""
    logger.info(f"[Run {run_id}] Updating run status to '{status}'.")
    update_fields = ["status = :status"]
    params = {"run_id": run_id, "status": status}

    if error_message is not None:
        update_fields.append("error_message = :error_msg")
        params["error_msg"] = error_message
    if gfs_metadata is not None:
            update_fields.append("gfs_input_metadata = :gfs_meta")
            params["gfs_meta"] = dumps(gfs_metadata, default=str)
    if status in ["completed", "error", "scored", "final_scoring_failed", "ensemble_failed", "initial_scoring_failed", "verification_failed"]:
            update_fields.append("completion_time = :comp_time")
            params["comp_time"] = datetime.now(timezone.utc)

    query = f"""
        UPDATE weather_forecast_runs
        SET {', '.join(update_fields)}
        WHERE id = :run_id
    """
    try:
        query_preview = query[:200].replace('\n', ' ')
        logger.info(f"[Run {run_id}] ABOUT TO EXECUTE update status to '{status}'. Query: {query_preview}")
        await task_instance.db_manager.execute(query, params)
        logger.info(f"[Run {run_id}] SUCCESSFULLY EXECUTED update status to '{status}'.")
    except asyncio.CancelledError:
        logger.warning(f"[Run {run_id}] UPDATE RUN STATUS CANCELLED while trying to set status to '{status}'.")
        raise
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
        db_params["metadata"] = dumps(score_data['metadata'])

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
    
    if task_instance.test_mode:
        task_instance.last_test_mode_run_id = run_id
        task_instance.test_mode_run_scored_event.clear()
        logger.info(f"[TestMode] Prepared event for scoring completion of run {run_id}")

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
            payload=forecast_request_payload, 
            endpoint=endpoint_to_call,
            hotkeys=[miner_hotkey]
        )
        response_dict = all_responses.get(miner_hotkey)
        if not response_dict: 
            logger.warning(f"[VerifyLogic] No response received from miner {miner_hotkey[:12]} for job {job_id}")
            return None
        if response_dict.get("status_code") == 200:
            miner_response_data = loads(response_dict['text'])
            miner_status = miner_response_data.get("status")
            
            if miner_status == "completed":
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
                else:
                    logger.warning(f"[VerifyLogic] Miner {miner_hotkey[:12]} responded 'completed' for job {job_id} but missing token/URL/hash")
            elif miner_status == "processing":
                logger.info(f"[VerifyLogic] Miner {miner_hotkey[:12]} job {job_id} still processing: {miner_response_data.get('message', 'No message')}")
                return None  # This is expected, validator should retry later
            elif miner_status == "error":
                logger.warning(f"[VerifyLogic] Miner {miner_hotkey[:12]} job {job_id} failed: {miner_response_data.get('message', 'No error message')}")
                return None
            elif miner_status == "not_found":
                logger.warning(f"[VerifyLogic] Miner {miner_hotkey[:12]} reports job {job_id} not found")
                return None
            else:
                logger.warning(f"[VerifyLogic] Miner {miner_hotkey[:12]} returned unknown status '{miner_status}' for job {job_id}")
        else:
            logger.warning(f"[VerifyLogic] Miner {miner_hotkey[:12]} returned HTTP {response_dict.get('status_code')} for job {job_id}")
    except Exception as e: 
        logger.error(f"Unhandled exception in _request_fresh_token for job {job_id}: {e!r}", exc_info=True)
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
        
    logger.info(f"[Job {job_id}] Updating miner job status to '{status}'.")
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

async def update_job_paths(task_instance: 'WeatherTask', job_id: str, netcdf_path: Optional[str] = None, kerchunk_path: Optional[str] = None, verification_hash: Optional[str] = None) -> None:
    """
    Update the file paths and verification hash for a completed job in the miner's database.
    Now stores the Zarr path in both target_netcdf_path and kerchunk_json_path fields for compatibility.
    (Intended for Miner-side usage)
    """
    if task_instance.node_type != 'miner':
        logger.error("update_job_paths called on non-miner node.")
        return False
        
    logger.info(f"[Job {job_id}] Updating miner job paths with Zarr store path: {netcdf_path}")
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
            "zarr_path": netcdf_path,
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
            # Miner job is still processing or failed - log warning and skip
            logger.warning(f"[VerifyLogic, Resp {response_id}] Miner {miner_hotkey} job {job_id} not ready for verification (still processing or failed). Skipping.")
            await task_instance.db_manager.execute("""
                UPDATE weather_miner_responses 
                SET status = 'awaiting_miner_completion', 
                    error_message = 'Miner job still processing or not available for verification'
                WHERE id = :id
            """, {"id": response_id})
            return  # Exit gracefully, this miner won't be scored for this run

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
                logger.info(f"[VerifyLogic, Resp {response_id}] Verified dataset keys: {list(verified_dataset.keys()) if verified_dataset else 'N/A'}")
                
            except Exception as e_ds_ops:
                logger.warning(f"[VerifyLogic, Resp {response_id}] Minor error during initial ops on verified dataset: {e_ds_ops}")
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
        
async def get_run_gfs_init_time(task_instance: 'WeatherTask', run_id: int) -> Optional[datetime]:
    run_details_query = "SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :run_id"
    run_record = await task_instance.db_manager.fetch_one(run_details_query, {"run_id": run_id})
    if run_record and run_record['gfs_init_time_utc']:
        return run_record['gfs_init_time_utc']
    logger.error(f"Could not retrieve GFS init time for run_id: {run_id}")
    return None

async def calculate_era5_miner_score(
    task_instance: 'WeatherTask',
    miner_response_rec: Dict,
    target_datetimes: List[datetime],
    era5_truth_ds: xr.Dataset,
    era5_climatology_ds: xr.Dataset
) -> bool:
    """
    Calculates final scores for a single miner against ERA5 analysis.
    Metrics: MSE (vs ERA5), Bias (vs ERA5), ACC (vs ERA5 anomalies),
             Skill Score (vs ERA5 Climatology as reference).
    Stores results in weather_miner_scores.
    Returns True if scoring was successful, False otherwise.
    """
    miner_hotkey = miner_response_rec['miner_hotkey']
    miner_uid = miner_response_rec['miner_uid']
    job_id = miner_response_rec['job_id']
    run_id = miner_response_rec['run_id']
    response_id = miner_response_rec['id']

    logger.info(f"[FinalScore] Starting ERA5 scoring for miner {miner_hotkey} (UID: {miner_uid}, Job: {job_id}, Run: {run_id}, RespID: {response_id})")

    gfs_init_time_of_run = await get_run_gfs_init_time(task_instance, run_id)
    if gfs_init_time_of_run is None:
        logger.error(f"[FinalScore] Miner {miner_hotkey}: Could not determine GFS init time for run {run_id}. Cannot calculate lead hours accurately.")
        return False

    final_scoring_config = {
        "variables_levels_to_score": task_instance.config.get('final_scoring_variables_levels', task_instance.config.get('day1_variables_levels_to_score')),
    }

    if era5_climatology_ds is None:
        logger.error(f"[FinalScore] Miner {miner_hotkey}: ERA5 Climatology not available. Cannot calculate ACC or Climatology Skill Score.")
        return False

    miner_forecast_ds: Optional[xr.Dataset] = None
    all_metrics_for_db = []

    try:
        stored_response_details_query = "SELECT kerchunk_json_url, verification_hash_claimed FROM weather_miner_responses WHERE id = :response_id"
        stored_response_data = await task_instance.db_manager.fetch_one(stored_response_details_query, {"response_id": response_id})

        if not stored_response_data or not stored_response_data['verification_hash_claimed'] or not stored_response_data['kerchunk_json_url']:
            logger.error(f"[FinalScore] Miner {miner_hotkey}: Failed to retrieve stored verification_hash_claimed or Zarr URL for response_id {response_id}. Aborting ERA5 scoring.")
            return False
        
        stored_manifest_hash = stored_response_data['verification_hash_claimed']
        token_data_tuple = await _request_fresh_token(task_instance, miner_hotkey, job_id)
        if token_data_tuple is None:
            raise ValueError(f"Failed to get fresh access token for {miner_hotkey} job {job_id}. Cannot ensure Zarr URL is current.")
        
        access_token, current_zarr_store_url, _ = token_data_tuple
        
        logger.info(f"[FinalScore] Miner {miner_hotkey}: Using stored manifest hash: {stored_manifest_hash[:10]}... and current Zarr URL: {current_zarr_store_url}")

        storage_options = {"headers": {"Authorization": f"Bearer {access_token}"}, "ssl": False}
        verification_timeout_seconds = task_instance.config.get('verification_timeout_seconds', 300) / 2

        miner_forecast_ds = await asyncio.wait_for(
            open_verified_remote_zarr_dataset(
                zarr_store_url=current_zarr_store_url,
                claimed_manifest_content_hash=stored_manifest_hash,
                miner_hotkey_ss58=miner_hotkey,
                storage_options=storage_options,
                job_id=f"{job_id}_final_score_reverify"
            ),
            timeout=verification_timeout_seconds
        )
        if miner_forecast_ds is None:
            raise ConnectionError(f"Failed to open verified Zarr dataset for miner {miner_hotkey}")

        gfs_operational_fcst_ds = None
        if gfs_init_time_of_run:
            try:
                unique_lead_hours_for_gfs_fetch = sorted(list(set([
                    int((vt - gfs_init_time_of_run).total_seconds() / 3600) for vt in target_datetimes
                ])))

                gfs_vars_to_request = list(set(AURORA_TO_GFS_VAR_MAP.get(vc['name']) 
                                               for vc in final_scoring_config["variables_levels_to_score"] 
                                               if AURORA_TO_GFS_VAR_MAP.get(vc['name']))) # Get GFS names
                
                gfs_surface_vars_req = [v for v in gfs_vars_to_request if v in GFS_SURFACE_VARS]
                gfs_atmos_vars_req = [v for v in gfs_vars_to_request if v in GFS_ATMOS_VARS]
                pressure_levels_req = list(set(vc.get('level') 
                                              for vc in final_scoring_config["variables_levels_to_score"] 
                                              if vc.get('level') is not None))
                if not pressure_levels_req:
                    pressure_levels_req = None 

                logger.info(f"[FinalScore] Fetching GFS operational reference: Init={gfs_init_time_of_run}, Leads={unique_lead_hours_for_gfs_fetch}, PSurf={gfs_surface_vars_req}, PAtm={gfs_atmos_vars_req}, PLevels={pressure_levels_req}")
                gfs_operational_fcst_ds = await fetch_gfs_data(
                    run_time=gfs_init_time_of_run,
                    lead_hours=unique_lead_hours_for_gfs_fetch,
                    target_surface_vars=gfs_surface_vars_req if gfs_surface_vars_req else None,
                    target_atmos_vars=gfs_atmos_vars_req if gfs_atmos_vars_req else None,
                    target_pressure_levels_hpa=pressure_levels_req
                )
            except Exception as e_fetch_gfs_ref:
                logger.error(f"[FinalScore] Miner {miner_hotkey}: Error fetching GFS operational reference forecast: {e_fetch_gfs_ref}", exc_info=True)
                gfs_operational_fcst_ds = None

        if gfs_operational_fcst_ds is None:
            logger.warning(f"[FinalScore] Miner {miner_hotkey}: Failed to fetch/process GFS operational reference forecast. Skill score vs GFS will not be available.")

        for valid_time_dt in target_datetimes:
            logger.info(f"[FinalScore] Miner {miner_hotkey}: Processing Valid Time: {valid_time_dt}")
            miner_forecast_lead_slice = None
            era5_truth_lead_slice = None

            try:
                # Robust dtype checking for timezone-aware dtypes
                def _is_integer_dtype(dtype):
                    """Check if dtype is integer, handling timezone-aware dtypes."""
                    try:
                        return np.issubdtype(dtype, np.integer)
                    except TypeError:
                        # Handle timezone-aware dtypes that can't be interpreted by numpy
                        return False
                
                def _is_datetime_dtype(dtype):
                    """Check if dtype is datetime, handling timezone-aware dtypes."""
                    dtype_str = str(dtype)
                    return (
                        'datetime64' in dtype_str or
                        dtype_str.startswith('<M8') or
                        'datetime' in dtype_str.lower()
                    )
                
                # Handle miner forecast dataset time dtype with robust checking
                if _is_integer_dtype(miner_forecast_ds.time.dtype):
                    selection_label_miner = int(valid_time_dt.timestamp() * 1_000_000_000)
                elif _is_datetime_dtype(miner_forecast_ds.time.dtype):
                    dtype_str = str(miner_forecast_ds.time.dtype)
                    if 'UTC' in dtype_str or 'tz' in dtype_str.lower():
                        # Timezone-aware - use timezone-aware timestamp
                        selection_label_miner = valid_time_dt
                    else:
                        # Timezone-naive - remove timezone
                        selection_label_miner = valid_time_dt.replace(tzinfo=None)
                else:
                    selection_label_miner = valid_time_dt

                # Handle ERA5 truth dataset time dtype with robust checking
                if _is_integer_dtype(era5_truth_ds.time.dtype):
                    selection_label_era5 = int(valid_time_dt.timestamp() * 1_000_000_000)
                elif _is_datetime_dtype(era5_truth_ds.time.dtype):
                    dtype_str = str(era5_truth_ds.time.dtype)
                    if 'UTC' in dtype_str or 'tz' in dtype_str.lower():
                        # Timezone-aware - use timezone-aware timestamp
                        selection_label_era5 = valid_time_dt
                    else:
                        # Timezone-naive - remove timezone
                        selection_label_era5 = valid_time_dt.replace(tzinfo=None)
                else:
                    selection_label_era5 = valid_time_dt

                logger.info(f"[FinalScore info] Miner time dtype: {miner_forecast_ds.time.dtype}, ERA5 time dtype: {era5_truth_ds.time.dtype}")
                logger.info(f"[FinalScore info] Using selection_label_miner: {selection_label_miner} (type: {type(selection_label_miner)}) for miner_forecast_ds")
                logger.info(f"[FinalScore info] Using selection_label_era5: {selection_label_era5} (type: {type(selection_label_era5)}) for era5_truth_ds")

                miner_forecast_lead_slice = miner_forecast_ds.sel(time=selection_label_miner, method="nearest")
                era5_truth_lead_slice = era5_truth_ds.sel(time=selection_label_era5, method="nearest")

                if miner_forecast_lead_slice is not None:
                    miner_forecast_lead_slice = miner_forecast_lead_slice.squeeze(drop=True)
                if era5_truth_lead_slice is not None:
                    era5_truth_lead_slice = era5_truth_lead_slice.squeeze(drop=True)
                
                miner_time_item = miner_forecast_lead_slice.time.item()
                if isinstance(miner_time_item, (int, float, np.integer, np.floating)):
                    miner_time_actual_utc = pd.Timestamp(miner_time_item, unit='ns', tz='UTC')
                else:
                    miner_time_actual_utc = pd.Timestamp(miner_time_item).tz_localize('UTC') if pd.Timestamp(miner_time_item).tzinfo is None else pd.Timestamp(miner_time_item).tz_convert('UTC')

                era5_time_item = era5_truth_lead_slice.time.item()
                if isinstance(era5_time_item, (int, float, np.integer, np.floating)):
                    era5_time_actual_utc = pd.Timestamp(era5_time_item, unit='ns', tz='UTC')
                else:
                    era5_time_actual_utc = pd.Timestamp(era5_time_item).tz_localize('UTC') if pd.Timestamp(era5_time_item).tzinfo is None else pd.Timestamp(era5_time_item).tz_convert('UTC')

                time_check_failed = False
                if abs((miner_time_actual_utc - valid_time_dt).total_seconds()) > 3600: 
                    logger.warning(f"[FinalScore] Miner {miner_hotkey}: Selected miner forecast time {miner_time_actual_utc} "
                                   f"too far from target {valid_time_dt}. Skipping this valid_time.")
                    time_check_failed = True
                
                # Use more lenient tolerance for ERA5 in test mode where data availability may be limited
                era5_time_tolerance = 21600  # 6 hours for ERA5 (more lenient for test mode)
                if not time_check_failed and abs((era5_time_actual_utc - valid_time_dt).total_seconds()) > era5_time_tolerance:
                    logger.warning(f"[FinalScore] Miner {miner_hotkey}: Selected ERA5 truth time {era5_time_actual_utc} "
                                   f"too far from target {valid_time_dt} (tolerance: {era5_time_tolerance/3600:.1f}h). Skipping this valid_time.")
                    time_check_failed = True
                
                if time_check_failed:
                    continue
            except TypeError as te_sel:
                logger.error(f"[FinalScore] Miner {miner_hotkey}: TypeError during data selection for valid time {valid_time_dt}: {te_sel}. This often indicates incompatible time coordinate types. Skipping this time.", exc_info=True)
                continue
            except Exception as e_sel:
                log_msg = f"[FinalScore] Miner {miner_hotkey}: Error during data selection for valid time {valid_time_dt}: {e_sel}. \
                        Miner slice acquired: {miner_forecast_lead_slice is not None}, ERA5 slice acquired: {era5_truth_lead_slice is not None}. Skipping this time."
                logger.error(log_msg)
                continue

            gfs_op_lead_slice = None
            if gfs_operational_fcst_ds is not None:
                try:
                    selection_label_gfs_op = valid_time_dt
                    if np.issubdtype(gfs_operational_fcst_ds.time.dtype, np.integer):
                        selection_label_gfs_op = int(valid_time_dt.timestamp() * 1_000_000_000)
                    elif str(gfs_operational_fcst_ds.time.dtype) == 'datetime64[ns]':
                        selection_label_gfs_op = valid_time_dt.replace(tzinfo=None)
                    
                    gfs_op_lead_slice = gfs_operational_fcst_ds.sel(time=selection_label_gfs_op, method="nearest").squeeze(drop=True)

                except Exception as e_gfs_sel:
                    logger.warning(f"[FinalScore] Miner {miner_hotkey}: Could not select from GFS operational forecast for {valid_time_dt}: {e_gfs_sel}. Skill vs GFS will be impacted.")
                    gfs_op_lead_slice = None

            for var_config in final_scoring_config["variables_levels_to_score"]:
                var_name = var_config['name']
                var_level = var_config.get('level')
                standard_name_for_clim = var_config.get('standard_name', var_name)
                var_key = f"{var_name}{var_level if var_level else ''}"
                
                lead_hours = int((valid_time_dt - gfs_init_time_of_run).total_seconds() / 3600)

                db_metric_row_base = {
                    "response_id": response_id, "run_id": run_id, "miner_uid": miner_uid, "miner_hotkey": miner_hotkey,
                    "score_type": None, "score": None, "metrics": {}, "calculation_time": datetime.now(timezone.utc), "error_message": None,
                    "lead_hours": lead_hours, "variable_level": var_key, "valid_time_utc": valid_time_dt
                }

                try:
                    logger.info(f"[FinalScore] Miner {miner_hotkey}: Scoring {var_key} at {valid_time_dt} (Lead: {lead_hours}h)")
                    
                    miner_var_da_unaligned = miner_forecast_lead_slice[var_name]
                    truth_var_da_unaligned = era5_truth_lead_slice[var_name]
                    
                    # Add detailed diagnostics for potential unit mismatches
                    logger.info(f"[FinalScore] RAW DATA DIAGNOSTICS for {var_key} at {valid_time_dt}:")
                    
                    # Log data ranges before any processing
                    miner_min, miner_max, miner_mean = float(miner_var_da_unaligned.min()), float(miner_var_da_unaligned.max()), float(miner_var_da_unaligned.mean())
                    truth_min, truth_max, truth_mean = float(truth_var_da_unaligned.min()), float(truth_var_da_unaligned.max()), float(truth_var_da_unaligned.mean())
                    
                    logger.info(f"[FinalScore] Miner {var_key}: range=[{miner_min:.1f}, {miner_max:.1f}], mean={miner_mean:.1f}, units={miner_var_da_unaligned.attrs.get('units', 'unknown')}")
                    logger.info(f"[FinalScore] ERA5  {var_key}: range=[{truth_min:.1f}, {truth_max:.1f}], mean={truth_mean:.1f}, units={truth_var_da_unaligned.attrs.get('units', 'unknown')}")
                    
                    # Check for potential unit mismatch indicators
                    if var_name == 'z' and var_level == 500:
                        # For z500, geopotential should be ~49000-58000 m²/s²
                        # If it's geopotential height, it would be ~5000-6000 m
                        miner_ratio = miner_mean / 9.80665  # If miner is geopotential, this ratio should be ~5000-6000
                        truth_ratio = truth_mean / 9.80665
                        logger.info(f"[FinalScore] z500 UNIT CHECK - If geopotential (m²/s²): miner_mean/g={miner_ratio:.1f}m, truth_mean/g={truth_ratio:.1f}m")
                        
                        if miner_mean < 10000:  # Much smaller than expected geopotential
                            logger.warning(f"[FinalScore] POTENTIAL UNIT MISMATCH: Miner z500 mean ({miner_mean:.1f}) suggests geopotential height (m) rather than geopotential (m²/s²)")
                        elif truth_mean > 40000 and miner_mean > 40000:
                            logger.info(f"[FinalScore] Unit check OK: Both miner and truth z500 appear to be geopotential (m²/s²)")
                    
                    elif var_name == '2t':
                        # Temperature should be ~200-320 K
                        if miner_mean < 200 or miner_mean > 350:
                            logger.warning(f"[FinalScore] POTENTIAL UNIT ISSUE: Miner 2t mean ({miner_mean:.1f}) outside expected range for Kelvin")
                            
                    elif var_name == 'msl':
                        # Mean sea level pressure should be ~90000-110000 Pa
                        if miner_mean < 50000 or miner_mean > 150000:
                            logger.warning(f"[FinalScore] POTENTIAL UNIT ISSUE: Miner msl mean ({miner_mean:.1f}) outside expected range for Pa")
                    
                    # AUTOMATIC UNIT CONVERSION: Convert geopotential height to geopotential if needed
                    if var_name == 'z' and miner_mean < 10000 and truth_mean > 40000:
                        logger.warning(f"[FinalScore] AUTOMATIC UNIT CONVERSION: Converting miner z from geopotential height (m) to geopotential (m²/s²)")
                        miner_var_da_unaligned = miner_var_da_unaligned * 9.80665
                        miner_var_da_unaligned.attrs['units'] = 'm2 s-2'
                        miner_var_da_unaligned.attrs['long_name'] = 'Geopotential (auto-converted from height)'
                        logger.info(f"[FinalScore] After conversion: miner z range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}")

                    # Check for temperature unit conversions (Celsius to Kelvin)
                    elif var_name in ['2t', 't'] and miner_mean < 100 and truth_mean > 200:
                        logger.warning(f"[FinalScore] AUTOMATIC UNIT CONVERSION: Converting miner {var_name} from Celsius to Kelvin")
                        miner_var_da_unaligned = miner_var_da_unaligned + 273.15
                        miner_var_da_unaligned.attrs['units'] = 'K'
                        miner_var_da_unaligned.attrs['long_name'] = f'{miner_var_da_unaligned.attrs.get("long_name", var_name)} (auto-converted from Celsius)'
                        logger.info(f"[FinalScore] After conversion: miner {var_name} range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}")

                    # Check for pressure unit conversions (hPa to Pa)
                    elif var_name == 'msl' and miner_mean < 2000 and truth_mean > 50000:
                        logger.warning(f"[FinalScore] AUTOMATIC UNIT CONVERSION: Converting miner msl from hPa to Pa")
                        miner_var_da_unaligned = miner_var_da_unaligned * 100.0
                        miner_var_da_unaligned.attrs['units'] = 'Pa'
                        miner_var_da_unaligned.attrs['long_name'] = 'Mean sea level pressure (auto-converted from hPa)'
                        logger.info(f"[FinalScore] After conversion: miner msl range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}")

                    clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
                    clim_hour_rounded = (valid_time_dt.hour // 6) * 6
                    climatology_var_da_raw = era5_climatology_ds[standard_name_for_clim].sel(
                        dayofyear=clim_dayofyear, hour=clim_hour_rounded, method="nearest"
                    )

                    if var_level:
                        # Handle different pressure level dimension names robustly
                        def _select_pressure_level(data_array, target_level):
                            """Select pressure level handling multiple possible dimension names."""
                            possible_pressure_dims = ['pressure_level', 'plev', 'level', 'levels', 'p']
                            for dim_name in possible_pressure_dims:
                                if dim_name in data_array.dims:
                                    return data_array.sel(**{dim_name: target_level}, method="nearest").squeeze(drop=True)
                            raise ValueError(f"No pressure level dimension found in {list(data_array.dims)}. Expected one of: {possible_pressure_dims}")
                        
                        miner_var_da_selected = _select_pressure_level(miner_var_da_unaligned, var_level)
                        truth_var_da_selected = _select_pressure_level(truth_var_da_unaligned, var_level)
                        # Apply robust pressure level selection to climatology too
                        clim_pressure_dim = None
                        for dim_name in ['pressure_level', 'plev', 'level', 'levels', 'p']:
                            if dim_name in climatology_var_da_raw.dims:
                                clim_pressure_dim = dim_name
                                break
                        if clim_pressure_dim:
                            climatology_var_da_selected = climatology_var_da_raw.sel(**{clim_pressure_dim: var_level}, method="nearest").squeeze(drop=True)
                        else: 
                            climatology_var_da_selected = climatology_var_da_raw.squeeze(drop=True)
                    else:
                        miner_var_da_selected = miner_var_da_unaligned.squeeze(drop=True)
                        truth_var_da_selected = truth_var_da_unaligned.squeeze(drop=True)
                        climatology_var_da_selected = climatology_var_da_raw.squeeze(drop=True)

                    def _standardize_spatial_dims_final(data_array: xr.DataArray) -> xr.DataArray:
                        if not isinstance(data_array, xr.DataArray): return data_array
                        rename_dict = {}
                        for dim_name in data_array.dims:
                            if dim_name.lower() in ('latitude', 'lat_0'): rename_dict[dim_name] = 'lat'
                            elif dim_name.lower() in ('longitude', 'lon_0'): rename_dict[dim_name] = 'lon'
                        return data_array.rename(rename_dict) if rename_dict else data_array

                    miner_var_da_std = _standardize_spatial_dims_final(miner_var_da_selected)
                    truth_var_da_std = _standardize_spatial_dims_final(truth_var_da_selected)
                    climatology_var_da_std = _standardize_spatial_dims_final(climatology_var_da_selected)
                    
                    target_grid_for_interp = truth_var_da_std
                    if 'lat' in target_grid_for_interp.dims and len(target_grid_for_interp.lat) > 1 and target_grid_for_interp.lat.values[0] < target_grid_for_interp.lat.values[-1]:
                        target_grid_for_interp = await asyncio.to_thread(target_grid_for_interp.isel, lat=slice(None, None, -1))

                    miner_var_da_aligned = await asyncio.to_thread(
                        miner_var_da_std.interp_like, target_grid_for_interp, method="linear", kwargs={"fill_value": None}
                    )
                    truth_var_da_final = target_grid_for_interp
                    
                    clim_var_to_interpolate = climatology_var_da_std
                    # Apply robust pressure level handling to climatology interpolation
                    if var_level:
                        clim_pressure_dim = None
                        for dim_name in ['pressure_level', 'plev', 'level', 'levels', 'p']:
                            if dim_name in clim_var_to_interpolate.dims:
                                clim_pressure_dim = dim_name
                                break
                        # If climatology still has pressure dimensions but truth doesn't, select the level  
                        truth_has_pressure = any(dim in truth_var_da_final.dims for dim in ['pressure_level', 'plev', 'level', 'levels', 'p'])
                        if clim_pressure_dim and not truth_has_pressure:
                            clim_var_to_interpolate = clim_var_to_interpolate.sel(**{clim_pressure_dim: var_level}, method="nearest").squeeze(drop=True)
                    
                    climatology_da_aligned = await asyncio.to_thread(
                        clim_var_to_interpolate.interp_like, truth_var_da_final, method="linear", kwargs={"fill_value": None}
                    )

                    logger.info(f"[FinalScore info Metric Input] Var: {var_key}, Level: {var_level}")
                    logger.info(f"[FinalScore info Metric Input] truth_var_da_final -> Shape: {truth_var_da_final.shape}, Dims: {truth_var_da_final.dims}, Coords: {list(truth_var_da_final.coords.keys())}")
                    logger.info(f"[FinalScore info Metric Input] miner_var_da_aligned -> Shape: {miner_var_da_aligned.shape}, Dims: {miner_var_da_aligned.dims}, Coords: {list(miner_var_da_aligned.coords.keys())}")
                    logger.info(f"[FinalScore info Metric Input] climatology_da_aligned -> Shape: {climatology_da_aligned.shape}, Dims: {climatology_da_aligned.dims}, Coords: {list(climatology_da_aligned.coords.keys())}")

                    lat_weights = None
                    if 'lat' in truth_var_da_final.dims:
                        one_d_lat_weights = await asyncio.to_thread(_calculate_latitude_weights, truth_var_da_final['lat'])
                        _, lat_weights = await asyncio.to_thread(xr.broadcast, truth_var_da_final, one_d_lat_weights)

                    current_metrics = {}
                    bias_corrected_forecast_da = await calculate_bias_corrected_forecast(miner_var_da_aligned, truth_var_da_final)
                    
                    actual_bias_op = (miner_var_da_aligned - truth_var_da_final)
                    
                    def calculate_mean_scalar_threaded(op):
                        computed_mean = op.mean()
                        if hasattr(computed_mean, 'compute'):
                            computed_mean = computed_mean.compute()
                        return float(computed_mean.item())
                    
                    mean_bias_val = await asyncio.to_thread(calculate_mean_scalar_threaded, actual_bias_op)
                    current_metrics["bias"] = mean_bias_val

                    def calculate_raw_mse_scalar_threaded(*args, **kwargs):
                        res = xs.mse(*args, **kwargs)
                        if hasattr(res, 'compute'):
                            res = res.compute()
                        return float(res.item())

                    raw_mse_val = await asyncio.to_thread(
                        calculate_raw_mse_scalar_threaded,
                        miner_var_da_aligned, 
                        truth_var_da_final, 
                        dim=[d for d in truth_var_da_final.dims if d in ('lat', 'lon')], 
                        weights=lat_weights, 
                        skipna=True
                    )
                    current_metrics["mse"] = raw_mse_val
                    current_metrics["rmse"] = np.sqrt(raw_mse_val)
                    
                    rmse_metric_row = {**db_metric_row_base, "metrics": current_metrics.copy(), "score_type": f"era5_rmse_{var_key}_{int(lead_hours)}h", "score": current_metrics["rmse"]}
                    all_metrics_for_db.append(rmse_metric_row)

                    acc_val = await calculate_acc(miner_var_da_aligned, truth_var_da_final, climatology_da_aligned, lat_weights)
                    current_metrics["acc"] = acc_val
                    acc_metric_row = {**db_metric_row_base, "metrics": current_metrics.copy(), "score_type": f"era5_acc_{var_key}_{int(lead_hours)}h", "score": acc_val}
                    all_metrics_for_db.append(acc_metric_row)

                    skill_score_val = None
                    
                    if gfs_op_lead_slice is not None:
                        try:
                            if var_name in gfs_op_lead_slice:
                                gfs_var_da_unaligned = gfs_op_lead_slice[var_name]

                                if var_level:
                                    gfs_pressure_dim = None
                                    for dim_name in ['pressure_level', 'plev', 'level', 'isobaricInhPa']:
                                        if dim_name in gfs_var_da_unaligned.dims:
                                            gfs_pressure_dim = dim_name
                                            break
                                    
                                    if gfs_pressure_dim:
                                        gfs_var_da_selected = gfs_var_da_unaligned.sel({gfs_pressure_dim: var_level}, method="nearest")
                                    else:
                                        logger.warning(f"[FinalScore] No pressure dimension found in GFS data for {var_key}. Using surface data.")
                                        gfs_var_da_selected = gfs_var_da_unaligned
                                else:
                                    gfs_var_da_selected = gfs_var_da_unaligned
                                
                                gfs_var_da_std = _standardize_spatial_dims_final(gfs_var_da_selected)
                                gfs_var_da_aligned = await asyncio.to_thread(
                                    gfs_var_da_std.interp_like, truth_var_da_final, method="linear", kwargs={"fill_value": None}
                                )
                                
                                # bias correction - fresh calculation for each variable to prevent reuse
                                forecast_bc_da = await calculate_bias_corrected_forecast(miner_var_da_aligned, truth_var_da_final)
                                
                                # skill score
                                skill_score_val = await calculate_mse_skill_score(
                                    forecast_bc_da, truth_var_da_final, gfs_var_da_aligned, lat_weights
                                )
                                
                                if np.isfinite(skill_score_val):
                                    logger.info(f"[FinalScore] UID {miner_uid} - Calculated GFS-based skill for {var_key} L{lead_hours}h: {skill_score_val:.4f}")
                                    skill_metric_row = {**db_metric_row_base, 
                                                       "metrics": current_metrics.copy(), 
                                                       "score_type": f"era5_skill_gfs_{var_key}_{int(lead_hours)}h", 
                                                       "score": skill_score_val}
                                    all_metrics_for_db.append(skill_metric_row)
                                else:
                                    logger.warning(f"[FinalScore] UID {miner_uid} - Calculated skill score is non-finite for {var_key} L{lead_hours}h")
                                    skill_score_val = None
                            else:
                                logger.warning(f"[FinalScore] UID {miner_uid} - Variable {var_name} not found in GFS forecast data")
                        except Exception as e_skill:
                            logger.error(f"[FinalScore] UID {miner_uid} - Error calculating skill score for {var_key} L{lead_hours}h: {e_skill}", exc_info=True)
                            skill_score_val = None
                    else:
                        logger.warning(f"[FinalScore] UID {miner_uid} - No GFS forecast data available for skill score calculation")
                    
                    if skill_score_val is None:
                        try:
                            # bias correction - ALWAYS recalculate for each variable (prevent variable reuse bug)
                            forecast_bc_da = await calculate_bias_corrected_forecast(miner_var_da_aligned, truth_var_da_final)
                            
                            # skill score vs climatology
                            skill_score_val = await calculate_mse_skill_score(
                                forecast_bc_da, truth_var_da_final, climatology_da_aligned, lat_weights
                            )
                            
                            if np.isfinite(skill_score_val):
                                logger.info(f"[FinalScore] UID {miner_uid} - Calculated CLIM-based skill for {var_key} L{lead_hours}h: {skill_score_val:.4f}")
                                skill_metric_row = {**db_metric_row_base, 
                                                   "metrics": current_metrics.copy(), 
                                                   "score_type": f"era5_skill_clim_{var_key}_{int(lead_hours)}h", 
                                                   "score": skill_score_val}
                                all_metrics_for_db.append(skill_metric_row)
                            else:
                                logger.warning(f"[FinalScore] UID {miner_uid} - Calculated climatology skill score is non-finite for {var_key} L{lead_hours}h")
                                skill_score_val = None
                        except Exception as e_clim_skill:
                            logger.error(f"[FinalScore] UID {miner_uid} - Error calculating climatology skill score for {var_key} L{lead_hours}h: {e_clim_skill}", exc_info=True)
                            skill_score_val = None
                    
                    if skill_score_val is None:
                        logger.warning(f"[FinalScore] UID {miner_uid} - No valid skill score calculated for {var_key} L{lead_hours}h")
                    
                    skill_score_log_str = f"{skill_score_val:.3f}" if skill_score_val is not None else "N/A"
                    logger.info(f"[FinalScore] Miner {miner_hotkey} V:{var_key} L:{int(lead_hours)}h RMSE:{current_metrics.get('rmse', np.nan):.2f} ACC:{current_metrics.get('acc', np.nan):.3f} SKILL:{skill_score_log_str}")

                except KeyError as ke:
                    logger.error(f"[FinalScore] Miner {miner_hotkey}: KeyError scoring {var_key} at {valid_time_dt}: {ke}. This often means the variable was not found in one of the datasets (miner, truth, or climatology).", exc_info=True)
                    error_metric_row = {**db_metric_row_base, "score_type": f"era5_error_{var_key}_{int(lead_hours)}h", "error_message": f"KeyError: {ke}"}
                    all_metrics_for_db.append(error_metric_row)
                except Exception as e_var_score:
                    logger.error(f"[FinalScore] Miner {miner_hotkey}: Error scoring {var_key} at {valid_time_dt}: {e_var_score}", exc_info=True)
                    error_metric_row = {**db_metric_row_base, "score_type": f"era5_error_{var_key}_{int(lead_hours)}h", "error_message": str(e_var_score)}
                    all_metrics_for_db.append(error_metric_row)
    finally:
        if miner_forecast_ds:
            try: 
                miner_forecast_ds.close()
                logger.debug(f"[FinalScore] Closed miner forecast dataset for {miner_hotkey}")
            except Exception: 
                pass
        
        # CRITICAL: Enhanced cleanup per miner for ERA5 scoring
        try:
            # Clear any ERA5-specific intermediate objects created during this miner's evaluation
            miner_specific_objects = [
                'miner_forecast_ds', 'gfs_operational_fcst_ds', 'miner_forecast_lead_slice', 
                'era5_truth_lead_slice', 'gfs_op_lead_slice'
            ]
            
            for obj_name in miner_specific_objects:
                if obj_name in locals():
                    try:
                        obj = locals()[obj_name]
                        if hasattr(obj, 'close'):
                            obj.close()
                        del obj
                    except Exception:
                        pass
            
            # Force cleanup for this miner's processing
            collected = gc.collect()
            logger.debug(f"[FinalScore] Cleanup for {miner_hotkey}: collected {collected} objects")
            
        except Exception as cleanup_err:
            logger.debug(f"[FinalScore] Cleanup error for {miner_hotkey}: {cleanup_err}")
        
        # Final garbage collection
        gc.collect()
    
    if not all_metrics_for_db:
        logger.warning(f"[FinalScore] Miner {miner_hotkey}: No metrics were calculated. This might be due to selection errors for all valid_times or variables.")
        return False

    insert_query = """
        INSERT INTO weather_miner_scores 
        (response_id, run_id, miner_uid, miner_hotkey, score_type, score, metrics, calculation_time, error_message, lead_hours, variable_level, valid_time_utc)
        VALUES (:response_id, :run_id, :miner_uid, :miner_hotkey, :score_type, :score, :metrics_json, :calculation_time, :error_message, :lead_hours, :variable_level, :valid_time_utc)
        ON CONFLICT (response_id, score_type, lead_hours, variable_level, valid_time_utc) DO UPDATE SET 
        score = EXCLUDED.score, metrics = EXCLUDED.metrics, calculation_time = EXCLUDED.calculation_time, error_message = EXCLUDED.error_message,
        miner_uid = EXCLUDED.miner_uid, miner_hotkey = EXCLUDED.miner_hotkey, run_id = EXCLUDED.run_id 
    """ 
    
    successful_inserts = 0
    for metric_record in all_metrics_for_db:
        params = metric_record.copy()
        params.setdefault('score', None) 
        params.setdefault('metrics', {}) 
        params.setdefault('error_message', None)

        params["metrics_json"] = dumps(params.pop("metrics"), default=str) 
        
        try:
            await task_instance.db_manager.execute(insert_query, params)
            successful_inserts +=1
        except Exception as db_err_indiv:
            logger.error(f"[FinalScore] Miner {miner_hotkey}: DB error storing single ERA5 score record ({params.get('score_type')} for {params.get('variable_level')} at {params.get('lead_hours')}h): {db_err_indiv}", exc_info=False) # exc_info=False to avoid too much noise if many fail

    if successful_inserts > 0:
        logger.info(f"[FinalScore] Miner {miner_hotkey}: Stored/Updated {successful_inserts}/{len(all_metrics_for_db)} ERA5 metric records to DB.")
        return True
    else:
        logger.error(f"[FinalScore] Miner {miner_hotkey}: Failed to store any ERA5 metric records to DB.")
        return False

async def _calculate_and_store_aggregated_era5_score(
    task_instance: 'WeatherTask',
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    response_id: int,
    lead_hours_scored: List[int],
    vars_levels_scored: List[Dict]
) -> Optional[float]:
    """
    Calculates a single aggregated ERA5 score (0-1) for a miner.
    Composition: 50% Skill/Error Component, 50% ACC Component.
    Equal weighting for var/level within each component for now.
    """
    logger.info(f"[AggFinalScore] Calculating aggregated ERA5 score for UID {miner_uid}, Run {run_id}")
    
    skill_error_component_scores = []
    acc_component_scores = []

    for lead_h in lead_hours_scored:
        for var_config in vars_levels_scored:
            var_name = var_config['name']
            var_level = var_config.get('level')
            var_key = f"{var_name}{var_level if var_level else ''}"

            rmse_score_val = None
            skill_score_val = None
            acc_score_val = None

            # Fetch RMSE
            rmse_score_type = f"era5_rmse_{var_key}_{lead_h}h"
            rmse_rec = await task_instance.db_manager.fetch_one(
                "SELECT score FROM weather_miner_scores WHERE response_id = :resp_id AND score_type = :stype",
                {"resp_id": response_id, "stype": rmse_score_type}
            )
            if rmse_rec and rmse_rec['score'] is not None and np.isfinite(rmse_rec['score']):
                rmse_score_val = rmse_rec['score']

            skill_score_val = None
            skill_score_type_gfs = f"era5_skill_gfs_{var_key}_{lead_h}h"
            skill_rec_gfs = await task_instance.db_manager.fetch_one(
                "SELECT score FROM weather_miner_scores WHERE response_id = :resp_id AND score_type = :stype",
                {"resp_id": response_id, "stype": skill_score_type_gfs}
            )
            if skill_rec_gfs and skill_rec_gfs['score'] is not None and np.isfinite(skill_rec_gfs['score']):
                skill_score_val = skill_rec_gfs['score']
                logger.info(f"[AggFinalScore] UID {miner_uid} - Using GFS-based skill for {var_key} L{lead_h}h: {skill_score_val:.4f}")
            else:
                skill_score_type_clim = f"era5_skill_clim_{var_key}_{lead_h}h"
                skill_rec_clim = await task_instance.db_manager.fetch_one(
                    "SELECT score FROM weather_miner_scores WHERE response_id = :resp_id AND score_type = :stype",
                    {"resp_id": response_id, "stype": skill_score_type_clim}
                )
                if skill_rec_clim and skill_rec_clim['score'] is not None and np.isfinite(skill_rec_clim['score']):
                    skill_score_val = skill_rec_clim['score']
                    logger.info(f"[AggFinalScore] UID {miner_uid} - Using CLIM-based skill for {var_key} L{lead_h}h: {skill_score_val:.4f} (GFS-based not found/valid)")
                else:
                    logger.warning(f"[AggFinalScore] UID {miner_uid} - No valid GFS or CLIM skill score found for {var_key} L{lead_h}h.")
            
            acc_score_type = f"era5_acc_{var_key}_{lead_h}h"
            acc_rec = await task_instance.db_manager.fetch_one(
                "SELECT score FROM weather_miner_scores WHERE response_id = :resp_id AND score_type = :stype",
                {"resp_id": response_id, "stype": acc_score_type}
            )
            if acc_rec and acc_rec['score'] is not None and np.isfinite(acc_rec['score']):
                acc_score_val = acc_rec['score']

            
            normalized_skill_error_item = 0.0
            if skill_score_val is not None:
                normalized_skill_error_item = max(0.0, skill_score_val)
            elif rmse_score_val is not None and rmse_score_val >= 0:
                normalized_skill_error_item = 1.0 / (1.0 + rmse_score_val)
            else:
                logger.warning(f"[AggFinalScore] No valid Skill Score or RMSE for {var_key} at lead {lead_h}h for UID {miner_uid}. Skill/Error item will be 0.")
            skill_error_component_scores.append(normalized_skill_error_item)


            normalized_acc_item = 0.0
            if acc_score_val is not None:
                normalized_acc_item = (acc_score_val + 1.0) / 2.0
            else:
                logger.warning(f"[AggFinalScore] No valid ACC for {var_key} at lead {lead_h}h for UID {miner_uid}. ACC item will be 0.")
            acc_component_scores.append(normalized_acc_item)

    avg_skill_error_score = sum(skill_error_component_scores) / len(skill_error_component_scores) if skill_error_component_scores else 0.0
    avg_acc_score = sum(acc_component_scores) / len(acc_component_scores) if acc_component_scores else 0.0

    final_score_val = (0.5 * avg_skill_error_score) + (0.5 * avg_acc_score)
    
    logger.info(f"[AggFinalScore] UID {miner_uid}, Run {run_id}: AvgNormSkill/Error={avg_skill_error_score:.4f} (from {len(skill_error_component_scores)} items), AvgNormACC={avg_acc_score:.4f} (from {len(acc_component_scores)} items) => Final Composite Score: {final_score_val:.4f}")

    agg_score_type = "era5_final_composite_score" 
    metrics_for_agg_score = {
        "avg_normalized_skill_error_component": avg_skill_error_score,
        "avg_normalized_acc_component": avg_acc_score,
        "num_skill_error_components_aggregated": len(skill_error_component_scores),
        "num_acc_components_aggregated": len(acc_component_scores)
    }

    db_params = {
        "response_id": response_id, "run_id": run_id, "miner_uid": miner_uid, "miner_hotkey": miner_hotkey,
        "score_type": agg_score_type, 
        "score": final_score_val if np.isfinite(final_score_val) else 0.0,
        "metrics_json": dumps(metrics_for_agg_score, default=str),
        "calculation_time": datetime.now(timezone.utc),
        "error_message": None,
        "lead_hours": None, 
        "variable_level": "aggregated_final", 
        "valid_time_utc": None 
    }
    
    insert_agg_query = """
        INSERT INTO weather_miner_scores 
        (response_id, run_id, miner_uid, miner_hotkey, score_type, score, metrics, calculation_time, error_message, lead_hours, variable_level, valid_time_utc)
        VALUES (:response_id, :run_id, :miner_uid, :miner_hotkey, :score_type, :score, :metrics_json, :calculation_time, :error_message, :lead_hours, :variable_level, :valid_time_utc)
        ON CONFLICT (response_id, score_type, lead_hours, variable_level, valid_time_utc) DO UPDATE SET 
        score = EXCLUDED.score, metrics = EXCLUDED.metrics, calculation_time = EXCLUDED.calculation_time, error_message = EXCLUDED.error_message,
        run_id = EXCLUDED.run_id, miner_uid = EXCLUDED.miner_uid, miner_hotkey = EXCLUDED.miner_hotkey
    """

    try:
        await task_instance.db_manager.execute(insert_agg_query, db_params)
        logger.info(f"[AggFinalScore] Stored/Updated final composite ERA5 score for UID {miner_uid}, Run {run_id}, Type: {agg_score_type}")
        return final_score_val
    except Exception as e_db:
        logger.error(f"[AggFinalScore] DB error storing final composite ERA5 score for UID {miner_uid}, Run {run_id}: {e_db}", exc_info=True)
        return None
