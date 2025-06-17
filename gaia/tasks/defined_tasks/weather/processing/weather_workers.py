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
import shutil
import time
import zarr
import numcodecs
from typing import Dict, Optional, List, Tuple
import base64
import pickle
import httpx
import gzip
import httpx
import gzip
import psutil
from functools import partial

from fiber.logging_utils import get_logger
from aurora import Batch

from ..utils.data_prep import create_aurora_batch_from_gfs
from ..utils.variable_maps import AURORA_TO_GFS_VAR_MAP

from .weather_logic import (
    _request_fresh_token,
    _update_run_status,
    update_job_status,
    update_job_paths,
    get_ground_truth_data,
    calculate_era5_miner_score,
    _calculate_and_store_aggregated_era5_score
)

from ..utils.gfs_api import fetch_gfs_analysis_data, fetch_gfs_data, GFS_SURFACE_VARS, GFS_ATMOS_VARS
from ..utils.era5_api import fetch_era5_data
from ..utils.hashing import compute_verification_hash, compute_input_data_hash, CANONICAL_VARS_FOR_HASHING
from ..weather_scoring.metrics import calculate_rmse
from ..weather_scoring_mechanism import evaluate_miner_forecast_day1, _execute_scoring_for_miner_process
from ..schemas.weather_outputs import WeatherProgressUpdate

from sqlalchemy import update
from gaia.database.validator_schema import weather_forecast_runs_table

logger = get_logger(__name__)

VALIDATOR_ENSEMBLE_DIR = Path("./validator_ensembles/")
MINER_FORECAST_DIR_BG = Path("./miner_forecasts_background/")
MINER_INPUT_BATCHES_DIR = Path("./miner_input_batches/")
MINER_INPUT_BATCHES_DIR.mkdir(parents=True, exist_ok=True)


def score_miner_in_process_wrapper(
    miner_response_rec,
    gfs_analysis_path,
    gfs_reference_path,
    era5_climatology_path,
    day1_scoring_config,
    gfs_init_time,
    task_config,
    db_manager_config
):
    """
    Synchronous wrapper to run the async scoring function in a new event loop.
    This is the target function for the ProcessPoolExecutor.
    """
    return asyncio.run(_execute_scoring_for_miner_process(
        miner_response_db_record=miner_response_rec,
        gfs_analysis_ds_path=gfs_analysis_path,
        gfs_reference_ds_path=gfs_reference_path,
        era5_climatology_ds_path=era5_climatology_path,
        day1_scoring_config=day1_scoring_config,
        run_gfs_init_time=gfs_init_time,
        task_config=task_config,
        miner_hotkey=miner_response_rec['miner_hotkey'],
        job_id=miner_response_rec['job_id'],
        db_manager_config=db_manager_config
    ))


def _prepare_http_payload_sync(prepared_batch_for_http: Batch) -> bytes:
    logger.debug(f"SYNC: Serializing Aurora Batch for HTTP service...")
    
    # Track memory usage for large batch processing
    pickled_batch = None
    base64_encoded_batch = None
    payload_json_str = None
    
    try:
        pickled_batch = pickle.dumps(prepared_batch_for_http)
        pickled_size_mb = len(pickled_batch) / (1024 * 1024)
        if pickled_size_mb > 50:
            logger.warning(f"Large Aurora batch pickle: {pickled_size_mb:.1f}MB")
        
        base64_encoded_batch = base64.b64encode(pickled_batch).decode('utf-8')
        
        # Clean up pickled data immediately
        del pickled_batch
        pickled_batch = None
        
        payload_json_str = json.dumps({"serialized_aurora_batch": base64_encoded_batch})
        
        # Clean up base64 data immediately
        del base64_encoded_batch
        base64_encoded_batch = None
        
        gzipped_payload = gzip.compress(payload_json_str.encode('utf-8'))
        
        # Clean up JSON string immediately
        del payload_json_str
        payload_json_str = None
        
        # Force garbage collection for large data
        if pickled_size_mb > 50:
            collected = gc.collect()
            logger.info(f"GC collected {collected} objects after large batch serialization ({pickled_size_mb:.1f}MB)")
        
        return gzipped_payload
        
    except Exception as e:
        # Clean up on error
        if pickled_batch:
            del pickled_batch
        if base64_encoded_batch:
            del base64_encoded_batch
        if payload_json_str:
            del payload_json_str
        logger.error(f"Error in _prepare_http_payload_sync: {e}")
        raise


def _prepare_http_payload_sync(prepared_batch_for_http: Batch) -> bytes:
    logger.debug(f"SYNC: Serializing Aurora Batch for HTTP service...")
    pickled_batch = pickle.dumps(prepared_batch_for_http)
    base64_encoded_batch = base64.b64encode(pickled_batch).decode('utf-8')
    payload_json_str = json.dumps({"serialized_aurora_batch": base64_encoded_batch})
    gzipped_payload = gzip.compress(payload_json_str.encode('utf-8'))
    return gzipped_payload

async def run_inference_background(task_instance: 'WeatherTask', job_id: str):
    """
    Background task to run the inference process for a given job_id.
    Uses a semaphore to limit concurrent GPU-intensive operations.
    Handles both local model inference and HTTP service-based inference.
    Includes duplicate checking to prevent redundant inference for the same timestep.
    """
    logger.info(f"[InferenceTask Job {job_id}] Background inference task initiated.")
    
    # Set up memory monitoring for this job
    from ..utils.memory_monitor import get_memory_monitor, log_memory_usage
    memory_monitor = get_memory_monitor()
    log_memory_usage(f"job {job_id} start")

    # Check for duplicates before starting expensive operations
    try:
        # Get job details including GFS timestep
        job_check_query = """
            SELECT status, gfs_init_time_utc FROM weather_miner_jobs WHERE id = :job_id
        """
        job_check_details = await task_instance.db_manager.fetch_one(job_check_query, {"job_id": job_id})
        
        if not job_check_details:
            logger.error(f"[InferenceTask Job {job_id}] Job not found during duplicate check. Aborting.")
            return
            
        current_status = job_check_details['status']
        gfs_init_time = job_check_details['gfs_init_time_utc']
        
        # Check if this job is already in progress or completed
        if current_status in ['in_progress', 'completed']:
            logger.warning(f"[InferenceTask Job {job_id}] Job already in status '{current_status}'. Skipping duplicate inference.")
            return
        
        # Check for other jobs with same timestep that are already in progress or completed
        if gfs_init_time:
            duplicate_check_query = """
                SELECT id, status FROM weather_miner_jobs 
                WHERE gfs_init_time_utc = :gfs_time 
                AND id != :current_job_id 
                AND status IN ('in_progress', 'completed')
                ORDER BY id DESC LIMIT 1
            """
            duplicate_job = await task_instance.db_manager.fetch_one(duplicate_check_query, {
                "gfs_time": gfs_init_time,
                "current_job_id": job_id
            })
            
            if duplicate_job:
                logger.warning(f"[InferenceTask Job {job_id}] Found existing job {duplicate_job['id']} for same timestep {gfs_init_time} with status '{duplicate_job['status']}'. Aborting duplicate inference.")
                await update_job_status(task_instance, job_id, "skipped_duplicate", f"Duplicate of job {duplicate_job['id']}")
                return
                
    except Exception as e:
        logger.error(f"[InferenceTask Job {job_id}] Error during duplicate check: {e}", exc_info=True)
        # Continue with inference if duplicate check fails to avoid blocking valid jobs

    # Initialize variables at the top of the function scope
    prepared_batch: Optional[Batch] = None
    output_steps_datasets: Optional[List[xr.Dataset]] = None # To store the final list of xr.Dataset predictions
    ds_t0: Optional[xr.Dataset] = None
    ds_t_minus_6: Optional[xr.Dataset] = None
    gfs_concat_data_for_batch_prep: Optional[xr.Dataset] = None
    
    local_gfs_cache_dir = Path(task_instance.config.get('gfs_analysis_cache_dir', './gfs_analysis_cache'))
    miner_hotkey_for_filename = "unknown_miner_hk"
    if task_instance.keypair and task_instance.keypair.ss58_address:
        miner_hotkey_for_filename = task_instance.keypair.ss58_address
    else:
        logger.warning(f"[InferenceTask Job {job_id}] Miner keypair not available for filename generation.")

    try:
        await update_job_status(task_instance, job_id, "processing_input")
        logger.info(f"[InferenceTask Job {job_id}] Fetching job details from DB...")
        job_db_details = await task_instance.db_manager.fetch_one(
            "SELECT gfs_init_time_utc, gfs_t_minus_6_time_utc FROM weather_miner_jobs WHERE id = :job_id",
            {"job_id": job_id}
        )
        if not job_db_details:
            logger.error(f"[InferenceTask Job {job_id}] Job details not found in DB. Aborting.")
            await update_job_status(task_instance, job_id, "error", "Job details not found")
            return
        gfs_init_time_utc = job_db_details['gfs_init_time_utc']
        gfs_t_minus_6_time_utc = job_db_details['gfs_t_minus_6_time_utc']

        current_inference_type = task_instance.config.get("weather_inference_type", "local_model").lower()
        logger.info(f"[InferenceTask Job {job_id}] Current inference type for pre-semaphore prep: {current_inference_type}")

        if current_inference_type == "http_service":
            http_service_url_available = task_instance.inference_service_url is not None and task_instance.inference_service_url.strip() != ""
            if not http_service_url_available:
                err_msg = "HTTP service URL not configured in WeatherTask for http_service type."
                logger.error(f"[InferenceTask Job {job_id}] {err_msg}")
                await update_job_status(task_instance, job_id, "error", err_msg)
                return

            logger.info(f"[InferenceTask Job {job_id}] Fetching initial batch path for HTTP service from 'input_batch_pickle_path'.")
            job_details_for_http = await task_instance.db_manager.fetch_one(
                "SELECT input_batch_pickle_path FROM weather_miner_jobs WHERE id = :job_id",
                {"job_id": job_id}
            )
            if not job_details_for_http or not job_details_for_http['input_batch_pickle_path']:
                err_msg = f"Cannot find input_batch_pickle_path for job {job_id} for HTTP inference."
                logger.error(f"[InferenceTask Job {job_id}] {err_msg}")
                await update_job_status(task_instance, job_id, "error", err_msg)
                return
            
            input_batch_file_path = Path(job_details_for_http['input_batch_pickle_path'])
            if not await asyncio.to_thread(input_batch_file_path.exists):
                err_msg = f"Input batch pickle file {input_batch_file_path} not found for HTTP inference."
                logger.error(f"[InferenceTask Job {job_id}] {err_msg}")
                await update_job_status(task_instance, job_id, "error", err_msg)
                return
            
            try:
                logger.info(f"[InferenceTask Job {job_id}] Loading initial batch from {input_batch_file_path} for HTTP service.")
                def _load_pickle_sync(path):
                    with open(path, "rb") as f: return pickle.load(f)
                prepared_batch = await asyncio.to_thread(_load_pickle_sync, input_batch_file_path) # Assign to prepared_batch
                if not prepared_batch: raise ValueError("Loaded pickled batch is None.")
                logger.info(f"[InferenceTask Job {job_id}] Successfully loaded pickled batch for HTTP. Type: {type(prepared_batch)}")
            except Exception as e_load_batch:
                err_msg = f"Failed to load pickled batch from {input_batch_file_path}: {e_load_batch}"
                logger.error(f"[InferenceTask Job {job_id}] {err_msg}", exc_info=True)
                await update_job_status(task_instance, job_id, "error", err_msg)
                return

        elif current_inference_type == "local_model":
            if task_instance.inference_runner is None or not hasattr(task_instance.inference_runner, 'run_multistep_inference'):
                err_msg = "Local inference runner or model not ready for local_model type."
                logger.error(f"[InferenceTask Job {job_id}] {err_msg}")
                await update_job_status(task_instance, job_id, "error", err_msg)
                return

            # DIAGNOSTIC: Add detailed logging to compare data processing paths
            logger.info(f"[InferenceTask Job {job_id}] DIAGNOSTIC - LOCAL MODEL INFERENCE PIPELINE STARTED")

            logger.info(f"[InferenceTask Job {job_id}] Fetching GFS data for local model (T0={gfs_init_time_utc}, T-6={gfs_t_minus_6_time_utc})...")
            ds_t0 = await fetch_gfs_analysis_data([gfs_init_time_utc], cache_dir=local_gfs_cache_dir)
            ds_t_minus_6 = await fetch_gfs_analysis_data([gfs_t_minus_6_time_utc], cache_dir=local_gfs_cache_dir)
            if ds_t0 is None or ds_t_minus_6 is None:
                err_msg = "Failed to fetch/load GFS data from cache for local_model."
                logger.error(f"[InferenceTask Job {job_id}] {err_msg}")
                await update_job_status(task_instance, job_id, "error", err_msg)
                return
            
            logger.info(f"[InferenceTask Job {job_id}] DIAGNOSTIC - LOCAL MODEL - Raw GFS data loaded:")
            logger.info(f"[InferenceTask Job {job_id}]   - ds_t0 variables: {list(ds_t0.data_vars.keys())}")
            logger.info(f"[InferenceTask Job {job_id}]   - ds_t0 dims: {dict(ds_t0.dims)}")
            logger.info(f"[InferenceTask Job {job_id}]   - ds_t_minus_6 variables: {list(ds_t_minus_6.data_vars.keys())}")
            logger.info(f"[InferenceTask Job {job_id}]   - ds_t_minus_6 dims: {dict(ds_t_minus_6.dims)}")
            
            logger.info(f"[InferenceTask Job {job_id}] Preparing Aurora batch from GFS data for local model...")
            gfs_concat_data_for_batch_prep = xr.concat([ds_t0, ds_t_minus_6], dim='time').sortby('time')
            
            # DIAGNOSTIC: Log combined dataset details
            logger.info(f"[InferenceTask Job {job_id}] DIAGNOSTIC - LOCAL MODEL - Combined GFS data:")
            logger.info(f"[InferenceTask Job {job_id}]   - Combined variables: {list(gfs_concat_data_for_batch_prep.data_vars.keys())}")
            logger.info(f"[InferenceTask Job {job_id}]   - Combined dims: {dict(gfs_concat_data_for_batch_prep.dims)}")
            logger.info(f"[InferenceTask Job {job_id}]   - Time values: {gfs_concat_data_for_batch_prep.time.values}")
            if 'lat' in gfs_concat_data_for_batch_prep.coords:
                lat_vals = gfs_concat_data_for_batch_prep.lat.values
                logger.info(f"[InferenceTask Job {job_id}]   - Lat range: [{lat_vals.min():.3f}, {lat_vals.max():.3f}], shape: {lat_vals.shape}")
            if 'lon' in gfs_concat_data_for_batch_prep.coords:
                lon_vals = gfs_concat_data_for_batch_prep.lon.values
                logger.info(f"[InferenceTask Job {job_id}]   - Lon range: [{lon_vals.min():.3f}, {lon_vals.max():.3f}], shape: {lon_vals.shape}")
            
            # Log sample variable data to check for potential issues
            for var_name in ['2t', 'msl', 'z', 't']:
                if var_name in gfs_concat_data_for_batch_prep:
                    var_data = gfs_concat_data_for_batch_prep[var_name]
                    var_min, var_max, var_mean = float(var_data.min()), float(var_data.max()), float(var_data.mean())
                    logger.info(f"[InferenceTask Job {job_id}]     - GFS {var_name}: shape={var_data.shape}, range=[{var_min:.6f}, {var_max:.6f}], mean={var_mean:.6f}")
            
            # Memory check before batch creation (CPU-intensive)
            if not memory_monitor.check_memory_pressure(f"job {job_id} pre-batch-creation"):
                logger.error(f"[InferenceTask Job {job_id}] Aborting due to memory pressure before batch creation")
                await update_job_status(task_instance, job_id, 'failed', "Memory pressure too high before batch creation")
                return
            
            prepared_batch = await asyncio.to_thread(
                create_aurora_batch_from_gfs,
                gfs_data=gfs_concat_data_for_batch_prep,
                resolution='0.25',
                download_dir='./static_data',
                history_steps=2
            )
            
            # Immediate cleanup of intermediate data after batch creation
            try:
                if gfs_concat_data_for_batch_prep is not None:
                    gfs_concat_data_for_batch_prep.close()
                    del gfs_concat_data_for_batch_prep
                if ds_t0 is not None:
                    ds_t0.close()
                    del ds_t0
                if ds_t_minus_6 is not None:
                    ds_t_minus_6.close()
                    del ds_t_minus_6
                gc.collect()
                log_memory_usage(f"job {job_id} post-batch-creation-cleanup")
            except Exception as e_cleanup:
                logger.warning(f"[InferenceTask Job {job_id}] Error during post-batch creation cleanup: {e_cleanup}")
            
            if prepared_batch is None:
                err_msg = "Failed to create Aurora batch for local model from GFS data."
                logger.error(f"[InferenceTask Job {job_id}] {err_msg}")
                await update_job_status(task_instance, job_id, "error", err_msg)
                return
            
            # DIAGNOSTIC: Log batch details to compare with HTTP processing
            try:
                logger.info(f"[InferenceTask Job {job_id}] DIAGNOSTIC - LOCAL MODEL BATCH CREATED:")
                logger.info(f"[InferenceTask Job {job_id}]   - Type: {type(prepared_batch)}")
                if hasattr(prepared_batch, 'metadata'):
                    if hasattr(prepared_batch.metadata, 'time'):
                        logger.info(f"[InferenceTask Job {job_id}]   - Metadata time: {prepared_batch.metadata.time}")
                    if hasattr(prepared_batch.metadata, 'lat'):
                        logger.info(f"[InferenceTask Job {job_id}]   - Lat shape: {prepared_batch.metadata.lat.shape}, range: [{float(prepared_batch.metadata.lat.min()):.3f}, {float(prepared_batch.metadata.lat.max()):.3f}]")
                    if hasattr(prepared_batch.metadata, 'lon'):
                        logger.info(f"[InferenceTask Job {job_id}]   - Lon shape: {prepared_batch.metadata.lon.shape}, range: [{float(prepared_batch.metadata.lon.min()):.3f}, {float(prepared_batch.metadata.lon.max()):.3f}]")
                    if hasattr(prepared_batch.metadata, 'atmos_levels'):
                        logger.info(f"[InferenceTask Job {job_id}]   - Pressure levels: {prepared_batch.metadata.atmos_levels}")
                
                if hasattr(prepared_batch, 'surf_vars'):
                    logger.info(f"[InferenceTask Job {job_id}]   - Surface variables: {list(prepared_batch.surf_vars.keys())}")
                    for var_name, tensor in prepared_batch.surf_vars.items():
                        var_min, var_max, var_mean = float(tensor.min()), float(tensor.max()), float(tensor.mean())
                        logger.info(f"[InferenceTask Job {job_id}]     - {var_name}: shape={tensor.shape}, range=[{var_min:.6f}, {var_max:.6f}], mean={var_mean:.6f}")
                
                if hasattr(prepared_batch, 'atmos_vars'):
                    logger.info(f"[InferenceTask Job {job_id}]   - Atmospheric variables: {list(prepared_batch.atmos_vars.keys())}")
                    for var_name, tensor in prepared_batch.atmos_vars.items():
                        var_min, var_max, var_mean = float(tensor.min()), float(tensor.max()), float(tensor.mean())
                        logger.info(f"[InferenceTask Job {job_id}]     - {var_name}: shape={tensor.shape}, range=[{var_min:.6f}, {var_max:.6f}], mean={var_mean:.6f}")
                
                if hasattr(prepared_batch, 'static_vars'):
                    logger.info(f"[InferenceTask Job {job_id}]   - Static variables: {list(prepared_batch.static_vars.keys())}")
                    for var_name, tensor in prepared_batch.static_vars.items():
                        var_min, var_max, var_mean = float(tensor.min()), float(tensor.max()), float(tensor.mean())
                        logger.info(f"[InferenceTask Job {job_id}]     - {var_name}: shape={tensor.shape}, range=[{var_min:.6f}, {var_max:.6f}], mean={var_mean:.6f}")
                        
            except Exception as e:
                logger.warning(f"[InferenceTask Job {job_id}] Error during local batch diagnostics: {e}")
            
            logger.info(f"[InferenceTask Job {job_id}] Aurora batch prepared for local model. Type: {type(prepared_batch)}")
        
        else:
            err_msg = f"Unknown current_inference_type: '{current_inference_type}'. Aborting."
            logger.error(f"[InferenceTask Job {job_id}] {err_msg}")
            await update_job_status(task_instance, job_id, "error", err_msg)
            return

        # Critical check: prepared_batch must be valid to proceed to semaphore
        if prepared_batch is None:
            err_msg = "CRITICAL: `prepared_batch` is None before entering GPU semaphore. This indicates a flaw in pre-semaphore preparation logic."
            logger.error(f"[InferenceTask Job {job_id}] {err_msg}")
            await update_job_status(task_instance, job_id, "error", err_msg)
            # Ensure GFS datasets are closed if they were loaded for local model path that failed before semaphore
            if ds_t0: ds_t0.close()
            if ds_t_minus_6: ds_t_minus_6.close()
            if gfs_concat_data_for_batch_prep: gfs_concat_data_for_batch_prep.close()
            gc.collect()
            return

        await update_job_status(task_instance, job_id, "running_inference")
        logger.info(f"[InferenceTask Job {job_id}] Waiting for GPU semaphore...")
        
        # Check memory safety before acquiring semaphore
        if not memory_monitor.check_memory_pressure(f"job {job_id} pre-semaphore"):
            logger.error(f"[InferenceTask Job {job_id}] Aborting due to memory pressure before semaphore")
            await update_job_status(task_instance, job_id, 'failed', "Memory pressure too high - preventing OOM")
            return
        
        # Wrap the main inference logic in a try-catch to prevent unhandled ValueErrors
        try:
            async with task_instance.gpu_semaphore:
                # Final memory check after acquiring semaphore
                log_memory_usage(f"job {job_id} semaphore acquired")
                if not memory_monitor.check_memory_pressure(f"job {job_id} pre-inference"):
                    logger.error(f"[InferenceTask Job {job_id}] Aborting due to memory pressure before inference")
                    await update_job_status(task_instance, job_id, 'failed', "Memory pressure too high - preventing OOM")
                    return
                logger.info(f"[InferenceTask Job {job_id}] Acquired GPU semaphore. Running inference...")
                inference_type_for_call = task_instance.config.get("weather_inference_type", "local_model").lower()
                logger.info(f"[InferenceTask Job {job_id}] (Inside Semaphore) Effective inference type for call: {inference_type_for_call}")

                if inference_type_for_call == "local_model":
                    if task_instance.inference_runner and task_instance.inference_runner.model:
                        logger.info(f"[InferenceTask Job {job_id}] (Inside Semaphore) Running local inference using prepared_batch (type: {type(prepared_batch)})...")
                        output_steps_datasets = await asyncio.to_thread( # Assign to output_steps_datasets
                            task_instance.inference_runner.run_multistep_inference,
                            prepared_batch,
                            steps=task_instance.config.get('inference_steps', 40)
                        )
                        logger.info(f"[InferenceTask Job {job_id}] (Inside Semaphore) Local inference completed. Received {len(output_steps_datasets if output_steps_datasets else [])} steps.")
                    else:
                        logger.error(f"[InferenceTask Job {job_id}] (Inside Semaphore) Local model runner/model not available. Cannot run local inference.")
                        output_steps_datasets = None
                
                elif inference_type_for_call == "http_service":
                    logger.info(f"[InferenceTask Job {job_id}] (Inside Semaphore) HTTP inference will use prepared_batch (type: {type(prepared_batch)}). Calling _run_inference_via_http_service...")
                    output_steps_datasets = await task_instance._run_inference_via_http_service( # Assign to output_steps_datasets
                        job_id=job_id,
                        initial_batch=prepared_batch 
                    )
                    logger.info(f"[InferenceTask Job {job_id}] (Inside Semaphore) HTTP inference call completed. Result steps: {len(output_steps_datasets if output_steps_datasets else [])}.")
                
                else:
                    logger.error(f"[InferenceTask Job {job_id}] (Inside Semaphore) Unknown inference type for call: '{inference_type_for_call}'. Skipping inference.")
                    output_steps_datasets = None
            
            logger.info(f"[InferenceTask Job {job_id}] Released GPU semaphore.")

            # selected_predictions_cpu is now output_steps_datasets
            if output_steps_datasets is None:
                error_msg_inference = "Inference process failed or returned None."
                logger.error(f"[InferenceTask Job {job_id}] {error_msg_inference}")
                await update_job_status(task_instance, job_id, "error", error_msg_inference)
                # GFS data cleanup happens in finally block
                return
            
            if not output_steps_datasets: # Empty list
                logger.info(f"[InferenceTask Job {job_id}] Inference resulted in an empty list of predictions (0 steps). This may be an expected outcome.")
                await update_job_status(task_instance, job_id, "completed_no_data", "Inference produced no forecast steps.")
                
                # Immediately cleanup R2 inputs for completed job
                if task_instance.config.get('weather_inference_type') == 'http_service':
                    asyncio.create_task(_immediate_r2_input_cleanup(task_instance, job_id))
                
                # GFS data cleanup happens in finally block
                return

            logger.info(f"[InferenceTask Job {job_id}] Inference successful. Processing {len(output_steps_datasets)} steps for saving...")
            await update_job_status(task_instance, job_id, "processing_output")

            MINER_FORECAST_DIR_BG.mkdir(parents=True, exist_ok=True)

            def _blocking_save_and_process():
                if not output_steps_datasets:
                    raise ValueError("Inference returned no prediction data (output_steps_datasets is None or empty).")

                combined_forecast_ds = None
                base_time = pd.to_datetime(gfs_init_time_utc)

                if isinstance(output_steps_datasets, xr.Dataset):
                    logger.info(f"[InferenceTask Job {job_id}] Processing pre-combined forecast (xr.Dataset) from HTTP service.")
                    combined_forecast_ds = output_steps_datasets

                    if 'time' not in combined_forecast_ds.coords or not pd.api.types.is_datetime64_any_dtype(combined_forecast_ds['time'].dtype):
                        logger.error(f"[InferenceTask Job {job_id}] Dataset from HTTP service is missing a valid 'time' coordinate. Cannot proceed with saving.")
                        raise ValueError("Dataset from HTTP service is missing a valid 'time' coordinate.")

                    if 'lead_time' not in combined_forecast_ds.coords:
                        logger.warning(f"[InferenceTask Job {job_id}] 'lead_time' coordinate not found in dataset from HTTP service. Attempting to derive.")
                        try:
                            derived_lead_times_hours = ((pd.to_datetime(combined_forecast_ds['time'].values) - base_time) / pd.Timedelta(hours=1)).astype(int)
                            combined_forecast_ds = combined_forecast_ds.assign_coords(lead_time=('time', derived_lead_times_hours))
                            logger.info(f"[InferenceTask Job {job_id}] Derived and assigned 'lead_time' coordinate based on 'time' and gfs_init_time_utc.")
                        except Exception as e_derive_lead:
                            logger.error(f"[InferenceTask Job {job_id}] Failed to derive 'lead_time' coordinate: {e_derive_lead}. Hashing might be affected if it relies on 'lead_time'.")
                else:
                    logger.info(f"[InferenceTask Job {job_id}] Processing list of Batch objects from local/Azure inference.")
                    if not isinstance(output_steps_datasets, list) or not output_steps_datasets:
                        raise ValueError("Inference returned no prediction steps or unexpected format for batch list.")
                        
                forecast_datasets = []
                lead_times_hours_list = []

                for i, batch_step in enumerate(output_steps_datasets):
                    forecast_step_h = task_instance.config.get('forecast_step_hours', 6)
                    current_lead_time_hours = (i + 1) * forecast_step_h
                    forecast_time = base_time + timedelta(hours=current_lead_time_hours)
                    
                    # Convert timezone-aware datetime to timezone-naive to avoid zarr serialization issues
                    if hasattr(forecast_time, 'tz_localize'):
                        # If pandas timestamp
                        forecast_time_naive = forecast_time.tz_localize(None)
                    elif hasattr(forecast_time, 'replace'):
                        # If python datetime
                        forecast_time_naive = forecast_time.replace(tzinfo=None)
                    else:
                        forecast_time_naive = forecast_time

                    if not isinstance(batch_step, Batch):
                        logger.warning(f"[InferenceTask Job {job_id}] Step {i} prediction is not an aurora.Batch (type: {type(batch_step)}), skipping.")
                        continue

                    logger.debug(f"Converting prediction Batch step {i+1} (T+{current_lead_time_hours}h) to xarray Dataset...")
                    data_vars = {}
                    for var_name, tensor_data in batch_step.surf_vars.items():
                        try:
                            np_data = tensor_data.squeeze().cpu().numpy()
                            data_vars[var_name] = xr.DataArray(np_data, dims=["lat", "lon"], name=var_name)
                        except Exception as e_surf:
                            logger.error(f"Error processing surface var {var_name} for step {i+1}: {e_surf}")
                    
                    for var_name, tensor_data in batch_step.atmos_vars.items():
                        try:
                            np_data = tensor_data.squeeze().cpu().numpy()
                            data_vars[var_name] = xr.DataArray(np_data, dims=["pressure_level", "lat", "lon"], name=var_name)
                        except Exception as e_atmos:
                            logger.error(f"Error processing atmos var {var_name} for step {i+1}: {e_atmos}")

                    lat_coords = batch_step.metadata.lat.cpu().numpy()
                    lon_coords = batch_step.metadata.lon.cpu().numpy()
                    level_coords = np.array(batch_step.metadata.atmos_levels)
                    
                    ds_step = xr.Dataset(
                        data_vars,
                        coords={
                            "time": ([forecast_time_naive]), # Use timezone-naive datetime
                            "pressure_level": (("pressure_level"), level_coords),
                            "lat": (("lat"), lat_coords),
                            "lon": (("lon"), lon_coords),
                        }
                    )
                    forecast_datasets.append(ds_step)
                    lead_times_hours_list.append(current_lead_time_hours)

                if not forecast_datasets:
                    raise ValueError("No forecast datasets created after processing batch prediction steps.")

                # Memory monitoring for local inference concatenation
                try:
                    import psutil
                    process = psutil.Process()
                    memory_before_local_concat_mb = process.memory_info().rss / (1024 * 1024)
                    logger.info(f"[InferenceTask Job {job_id}] Memory before local forecast concatenation: {memory_before_local_concat_mb:.1f} MB")
                    
                    # Emergency memory check for local inference
                    if memory_before_local_concat_mb > 12000:
                        logger.error(f"[InferenceTask Job {job_id}] 🚨 EMERGENCY: Memory too high before concatenation ({memory_before_local_concat_mb:.1f} MB). Aborting.")
                        # Cleanup forecast datasets before aborting
                        for ds in forecast_datasets:
                            try:
                                ds.close()
                            except:
                                pass
                        del forecast_datasets
                        gc.collect()
                        raise RuntimeError("Memory usage too high - preventing OOM")
                except Exception:
                    pass

                combined_forecast_ds = xr.concat(forecast_datasets, dim='time')
                combined_forecast_ds = combined_forecast_ds.assign_coords(lead_time=('time', lead_times_hours_list))
                
                # Memory monitoring after concatenation
                try:
                    memory_after_local_concat_mb = process.memory_info().rss / (1024 * 1024)
                    memory_used_local_mb = memory_after_local_concat_mb - memory_before_local_concat_mb
                    logger.info(f"[InferenceTask Job {job_id}] Memory after local concatenation: {memory_after_local_concat_mb:.1f} MB (used {memory_used_local_mb:+.1f} MB)")
                    
                    # Warning for high memory usage
                    if memory_after_local_concat_mb > 10000:
                        logger.warning(f"[InferenceTask Job {job_id}] ⚠️  HIGH MEMORY USAGE in local inference: {memory_after_local_concat_mb:.1f} MB")
                except Exception:
                    pass

            if combined_forecast_ds is None:
                raise ValueError("combined_forecast_ds was not properly assigned.")

            logger.info(f"[InferenceTask Job {job_id}] Combined forecast dimensions: {combined_forecast_ds.dims}")

            gfs_time_str = gfs_init_time_utc.strftime('%Y%m%d%H')
            unique_suffix = job_id.split('-')[0]
            
            dirname_zarr = f"weather_forecast_{gfs_time_str}_miner_hk_{miner_hotkey_for_filename[:10]}_{unique_suffix}.zarr"
            output_zarr_path = MINER_FORECAST_DIR_BG / dirname_zarr

            encoding = {}
            for var_name, da in combined_forecast_ds.data_vars.items():
                chunks_for_var = {}
                
                time_dim_in_var = next((d for d in da.dims if d.lower() == 'time'), None)
                level_dim_in_var = next((d for d in da.dims if d.lower() in ('pressure_level', 'level', 'plev', 'isobaricinhpa')), None)
                lat_dim_in_var = next((d for d in da.dims if d.lower() in ('lat', 'latitude')), None)
                lon_dim_in_var = next((d for d in da.dims if d.lower() in ('lon', 'longitude')), None)

                if time_dim_in_var:
                    chunks_for_var[time_dim_in_var] = 1
                if level_dim_in_var:
                    chunks_for_var[level_dim_in_var] = 1 
                if lat_dim_in_var:
                    chunks_for_var[lat_dim_in_var] = combined_forecast_ds.sizes[lat_dim_in_var]
                if lon_dim_in_var:
                    chunks_for_var[lon_dim_in_var] = combined_forecast_ds.sizes[lon_dim_in_var]
                
                ordered_chunks_list = []
                for dim_name_in_da in da.dims:
                    ordered_chunks_list.append(chunks_for_var.get(dim_name_in_da, combined_forecast_ds.sizes[dim_name_in_da]))
                
                encoding[var_name] = {
                    'chunks': tuple(ordered_chunks_list),
                    'compressor': numcodecs.Blosc(cname='zstd', clevel=3, shuffle=numcodecs.Blosc.BITSHUFFLE)
                }
                
                # Add explicit time encoding to ensure consistency between local and HTTP service paths
                for coord_name in combined_forecast_ds.coords:
                    if coord_name.lower() == 'time' and pd.api.types.is_datetime64_any_dtype(combined_forecast_ds.coords[coord_name].dtype):
                        encoding['time'] = {
                            'units': f'hours since {base_time.strftime("%Y-%m-%d %H:%M:%S")}',
                            'calendar': 'standard',
                            'dtype': 'float64'
                        }
                        logger.info(f"[InferenceTask Job {job_id}] Added explicit time encoding for consistency: {encoding['time']}")
                        break
                
                logger.info(f"[InferenceTask Job {job_id}] Saving forecast to Zarr store with chunking. Example encoding for {list(encoding.keys())[0] if encoding else 'N/A'}: {list(encoding.values())[0] if encoding else 'N/A'}")
                
                if os.path.exists(output_zarr_path):
                    shutil.rmtree(output_zarr_path)
                
                # --- Fix for datetime64[ns, UTC] issue ---
                if combined_forecast_ds is not None:
                    logger.debug(f"[InferenceTask Job {job_id}] Checking and converting timezone-aware datetime64 dtypes before saving to Zarr...")
                    for coord_name in list(combined_forecast_ds.coords):
                        coord = combined_forecast_ds.coords[coord_name]
                        if pd.api.types.is_datetime64_any_dtype(coord.dtype) and getattr(coord.dtype, 'tz', None) is not None:
                            logger.info(f"[InferenceTask Job {job_id}] Converting coordinate '{coord_name}' from {coord.dtype} to datetime64[ns].")
                            combined_forecast_ds = combined_forecast_ds.assign_coords(**{coord_name: coord.variable.astype('datetime64[ns]')})
                        elif str(coord.dtype) == 'datetime64[ns, UTC]': # Fallback for direct numpy dtypes if tz attribute is not present
                            logger.info(f"[InferenceTask Job {job_id}] Converting coordinate '{coord_name}' from {coord.dtype} (direct check) to datetime64[ns].")
                            combined_forecast_ds = combined_forecast_ds.assign_coords(**{coord_name: coord.variable.astype('datetime64[ns]')})
                    
                    for var_name in list(combined_forecast_ds.data_vars):
                        data_array = combined_forecast_ds[var_name]
                        if pd.api.types.is_datetime64_any_dtype(data_array.dtype) and getattr(data_array.dtype, 'tz', None) is not None:
                            logger.info(f"[InferenceTask Job {job_id}] Converting data variable '{var_name}' from {data_array.dtype} to datetime64[ns].")
                            combined_forecast_ds[var_name] = data_array.astype('datetime64[ns]')
                        elif str(data_array.dtype) == 'datetime64[ns, UTC]':
                            logger.info(f"[InferenceTask Job {job_id}] Converting data variable '{var_name}' from {data_array.dtype} (direct check) to datetime64[ns].")
                            combined_forecast_ds[var_name] = data_array.astype('datetime64[ns]')
                # --- End fix ---

                combined_forecast_ds.to_zarr(
                    output_zarr_path,
                    encoding=encoding,
                    consolidated=True,
                    compute=True
                )
                
                try:
                    zarr.consolidate_metadata(str(output_zarr_path))
                    logger.info(f"[InferenceTask Job {job_id}] Explicitly consolidated Zarr metadata")
                except Exception as e_consolidate:
                    logger.warning(f"[InferenceTask Job {job_id}] Failed to explicitly consolidate Zarr metadata: {e_consolidate}")
                
                logger.info(f"[InferenceTask Job {job_id}] Successfully saved forecast to Zarr store: {output_zarr_path}")
                output_metadata = {
                    "time": [base_time],
                    "source_model": "aurora",
                    "resolution": 0.25
                }
                
                # Generate manifest and signature for the zarr store
                verification_hash = None
                try:
                    logger.info(f"[{job_id}] 🔐 Generating manifest and signature for Zarr store...")
                    
                    # Get miner keypair for signing
                    miner_keypair = task_instance.keypair if task_instance.keypair else None
                    
                    if miner_keypair:
                        def _generate_manifest_sync():
                            from ..utils.hashing import generate_manifest_and_signature
                            return generate_manifest_and_signature(
                                zarr_store_path=Path(output_zarr_path),
                                miner_hotkey_keypair=miner_keypair,
                                include_zarr_metadata_in_manifest=True,
                                chunk_hash_algo_name="xxh64"
                            )
                        
                        manifest_result = await asyncio.to_thread(_generate_manifest_sync)
                        
                        if manifest_result:
                            _manifest_dict, _signature_bytes, manifest_content_sha256_hash = manifest_result
                            verification_hash = manifest_content_sha256_hash
                            logger.info(f"[{job_id}] ✅ Generated verification hash: {verification_hash[:10]}...")
                        else:
                            logger.warning(f"[{job_id}] ⚠️  Failed to generate manifest and signature.")
                    else:
                        logger.warning(f"[{job_id}] ⚠️  No miner keypair available for manifest signing.")
                        
                except Exception as e_manifest:
                    logger.error(f"[{job_id}] ❌ Error generating manifest: {e_manifest}", exc_info=True)
                    verification_hash = None

                return str(output_zarr_path), verification_hash

            zarr_path, v_hash = await asyncio.to_thread(_blocking_save_and_process)

            await update_job_paths(
                task_instance=task_instance,
                job_id=job_id,
                netcdf_path=zarr_path,
                kerchunk_path=zarr_path,
                verification_hash=v_hash
            )
            await update_job_status(task_instance, job_id, "completed")
            
            # Immediately cleanup R2 inputs for completed job
            if task_instance.config.get('weather_inference_type') == 'http_service':
                asyncio.create_task(_immediate_r2_input_cleanup(task_instance, job_id))
            
            logger.info(f"[InferenceTask Job {job_id}] Background inference task completed successfully.")

        except Exception as inference_err:
            # Catch any unhandled exceptions from the main inference logic (including ValueErrors from save/processing)
            logger.error(f"[InferenceTask Job {job_id}] Unhandled error during inference or processing: {inference_err}", exc_info=True)
            await update_job_status(task_instance, job_id, "error", error_message=f"Inference error: {inference_err}")
            
            # Cleanup R2 inputs for failed job
            if task_instance.config.get('weather_inference_type') == 'http_service':
                asyncio.create_task(_immediate_r2_input_cleanup(task_instance, job_id))

    except Exception as e:
        logger.error(f"[InferenceTask Job {job_id}] Background inference task failed unexpectedly: {e}", exc_info=True)
        try:
             await update_job_status(task_instance, job_id, "error", error_message=f"Unexpected task error: {e}")
        except Exception as final_db_err:
             logger.error(f"[InferenceTask Job {job_id}] Failed to update job status to error after task failure: {final_db_err}")
        
        # Cleanup R2 inputs for failed job
        if task_instance.config.get('weather_inference_type') == 'http_service':
            asyncio.create_task(_immediate_r2_input_cleanup(task_instance, job_id))
    finally:
        # Comprehensive cleanup of large GFS datasets and other objects
        logger.debug(f"[InferenceTask Job {job_id}] Entering finally block for cleanup.")
        if ds_t0 and hasattr(ds_t0, 'close'): 
            try: ds_t0.close(); logger.debug(f"[InferenceTask Job {job_id}] Closed ds_t0.")
            except Exception as e_close: logger.warning(f"[InferenceTask Job {job_id}] Error closing ds_t0: {e_close}")
        if ds_t_minus_6 and hasattr(ds_t_minus_6, 'close'):
            try: ds_t_minus_6.close(); logger.debug(f"[InferenceTask Job {job_id}] Closed ds_t_minus_6.")
            except Exception as e_close: logger.warning(f"[InferenceTask Job {job_id}] Error closing ds_t_minus_6: {e_close}")
        if gfs_concat_data_for_batch_prep and hasattr(gfs_concat_data_for_batch_prep, 'close'):
            try: gfs_concat_data_for_batch_prep.close(); logger.debug(f"[InferenceTask Job {job_id}] Closed gfs_concat_data_for_batch_prep.")
            except Exception as e_close: logger.warning(f"[InferenceTask Job {job_id}] Error closing gfs_concat_data_for_batch_prep: {e_close}")
        
        # prepared_batch is not an xarray dataset usually, so no close(), just delete reference
        if 'prepared_batch' in locals() and prepared_batch:
            del prepared_batch
            logger.debug(f"[InferenceTask Job {job_id}] Cleared reference to prepared_batch.")
        
        if 'output_steps_datasets' in locals() and output_steps_datasets: # list of datasets
            for i, ds_step in enumerate(output_steps_datasets):
                if ds_step and hasattr(ds_step, 'close'):
                    try: ds_step.close(); logger.debug(f"[InferenceTask Job {job_id}] Closed output_steps_datasets step {i}.")
                    except Exception as e_close: logger.warning(f"[InferenceTask Job {job_id}] Error closing output_steps_datasets step {i}: {e_close}")
            del output_steps_datasets
            logger.debug(f"[InferenceTask Job {job_id}] Cleared reference to output_steps_datasets.")

        gc.collect() # Force garbage collection
        logger.info(f"[InferenceTask Job {job_id}] Background inference task finally block completed.")


async def initial_scoring_worker(task_instance: 'WeatherTask'):
    """
    Continuously checks for new forecast runs that need initial scoring (Day-1 QC).
    Uses a ProcessPoolExecutor to parallelize scoring across multiple CPU cores.
    """
    worker_id = str(uuid.uuid4())[:8]
    logger.info(f"[InitialScoringWorker-{worker_id}] Starting up at {datetime.now(timezone.utc).isoformat()}")
    if task_instance.db_manager is None:
        logger.error(f"[InitialScoringWorker-{worker_id}] DB manager not available. Aborting.")
        return
    if task_instance.scoring_executor is None:
        logger.error(f"[InitialScoringWorker-{worker_id}] Scoring executor not available. Aborting.")
        return

    temp_base_dir = Path("./temp_scoring_data")
    temp_base_dir.mkdir(exist_ok=True)

    try:
        while True:
            run_id = None
            gfs_analysis_ds_for_run = None
            gfs_reference_ds_for_run = None
            era5_climatology_ds = None
            temp_run_dir = None
            
            try:
                run_id = await task_instance.initial_scoring_queue.get()
                
                temp_run_dir = temp_base_dir / str(run_id)
                temp_run_dir.mkdir(exist_ok=True)

                logger.info(f"[Day1ScoringWorker] Processing Day-1 QC scores for run {run_id}")
                await _update_run_status(task_instance, run_id, "day1_scoring_started")
                
                run_details_query = "SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :run_id"
                run_record = await task_instance.db_manager.fetch_one(run_details_query, {"run_id": run_id})
                if not run_record:
                    logger.error(f"[Day1ScoringWorker] Run {run_id}: Details not found. Skipping.")
                    task_instance.initial_scoring_queue.task_done()
                    continue

                gfs_init_time = run_record['gfs_init_time_utc']
                logger.info(f"[Day1ScoringWorker] Run {run_id}: gfs_init_time: {gfs_init_time}")
                
                responses_query = """
                SELECT mr.id, mr.miner_hotkey, mr.miner_uid, mr.job_id, mr.run_id,
                       mr.kerchunk_json_url, mr.verification_hash_claimed 
                FROM weather_miner_responses mr
                WHERE mr.run_id = :run_id AND mr.verification_passed = TRUE 
                AND mr.status = 'verified_manifest_store_opened' 
                """
                responses = await task_instance.db_manager.fetch_all(responses_query, {"run_id": run_id})
                
                if not responses:
                    logger.warning(f"[Day1ScoringWorker] Run {run_id}: No verified responses for Day-1 scoring.")
                    await _update_run_status(task_instance, run_id, "day1_scoring_failed", "No verified members")
                    task_instance.initial_scoring_queue.task_done()
                    continue
                    
                logger.info(f"[Day1ScoringWorker] Run {run_id}: Found {len(responses)} verified responses.")
                            
                day1_lead_hours = task_instance.config.get('initial_scoring_lead_hours', [6, 12])
                valid_times_for_gfs = [gfs_init_time + timedelta(hours=h) for h in day1_lead_hours]
                gfs_cache_dir = Path(task_instance.config.get('gfs_analysis_cache_dir', './gfs_analysis_cache'))

                resolved_day1_variables_levels_to_score = task_instance.config.get('day1_variables_levels_to_score', [
                    {"name": "z", "level": 500, "standard_name": "geopotential"},
                    {"name": "t", "level": 850, "standard_name": "temperature"},
                    {"name": "2t", "level": None, "standard_name": "2m_temperature"},
                    {"name": "msl", "level": None, "standard_name": "mean_sea_level_pressure"}
                ])

                target_surface_vars_gfs_day1, target_atmos_vars_gfs_day1 = [], []
                target_pressure_levels_hpa_day1_set = set()

                for var_info in resolved_day1_variables_levels_to_score:
                    gfs_name = AURORA_TO_GFS_VAR_MAP.get(var_info['name'])
                    if not gfs_name: continue
                    if var_info.get('level') is None:
                        if gfs_name not in target_surface_vars_gfs_day1: target_surface_vars_gfs_day1.append(gfs_name)
                    else:
                        if gfs_name not in target_atmos_vars_gfs_day1: target_atmos_vars_gfs_day1.append(gfs_name)
                        target_pressure_levels_hpa_day1_set.add(int(var_info['level']))
                
                target_pressure_levels_list_day1 = sorted(list(target_pressure_levels_hpa_day1_set)) or None
