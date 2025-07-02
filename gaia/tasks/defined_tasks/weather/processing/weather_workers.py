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
from ..weather_scoring_mechanism import evaluate_miner_forecast_day1, precompute_climatology_cache
from ..schemas.weather_outputs import WeatherProgressUpdate

from sqlalchemy import update
from gaia.database.validator_schema import weather_forecast_runs_table

logger = get_logger(__name__)

VALIDATOR_ENSEMBLE_DIR = Path("./validator_ensembles/")
MINER_FORECAST_DIR_BG = Path("./miner_forecasts_background/")
MINER_INPUT_BATCHES_DIR = Path("./miner_input_batches/")
MINER_INPUT_BATCHES_DIR.mkdir(parents=True, exist_ok=True)


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
                base_time = pd.to_datetime(gfs_init_time_utc)
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
            
            # Immediately cleanup R2 inputs for completed job (with task tracking)
            if task_instance.config.get('weather_inference_type') == 'http_service':
                cleanup_task = asyncio.create_task(_immediate_r2_input_cleanup(task_instance, job_id))
                if hasattr(task_instance, 'validator') and hasattr(task_instance.validator, '_track_background_task'):
                    task_instance.validator._track_background_task(cleanup_task, f"r2_cleanup_{job_id}")
            
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
        
        # Cleanup R2 inputs for failed job (with task tracking)
        if task_instance.config.get('weather_inference_type') == 'http_service':
            cleanup_task = asyncio.create_task(_immediate_r2_input_cleanup(task_instance, job_id))
            if hasattr(task_instance, 'validator') and hasattr(task_instance.validator, '_track_background_task'):
                task_instance.validator._track_background_task(cleanup_task, f"r2_cleanup_failed_{job_id}")
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
    """
    worker_id = str(uuid.uuid4())[:8]
    logger.info(f"[InitialScoringWorker-{worker_id}] Starting up at {datetime.now(timezone.utc).isoformat()}")
    if task_instance.db_manager is None:
        logger.error(f"[InitialScoringWorker-{worker_id}] DB manager not available. Aborting.")
        task_instance.initial_scoring_worker_running = False
        return

    try:
        while True:
            run_id = None
            gfs_analysis_ds_for_run = None
            gfs_reference_ds_for_run = None
            era5_climatology_ds = None

            try:
                run_id = await task_instance.initial_scoring_queue.get()
                
                logger.info(f"[Day1ScoringWorker] Processing Day-1 QC scores for run {run_id}")
                # Mark persistent scoring job as started
                await task_instance._start_scoring_job(run_id, 'day1_qc')
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
                SELECT 
                    mr.id, 
                    mr.miner_hotkey, 
                    mr.miner_uid, 
                    mr.job_id,
                    mr.run_id,
                    mr.kerchunk_json_url, 
                    mr.verification_hash_claimed 
                FROM weather_miner_responses mr
                WHERE mr.run_id = :run_id AND mr.verification_passed = TRUE 
                AND mr.status = 'verified_manifest_store_opened' 
                """
                responses = await task_instance.db_manager.fetch_all(responses_query, {"run_id": run_id})
                
                min_members_for_scoring = 1
                if not responses or len(responses) < min_members_for_scoring:
                    logger.warning(f"[Day1ScoringWorker] Run {run_id}: No verified responses found for Day-1 scoring.")
                    await _update_run_status(task_instance, run_id, "day1_scoring_failed", error_message="No verified members with opened stores")
                    # Mark persistent scoring job as failed
                    await task_instance._complete_scoring_job(run_id, 'day1_qc', success=False, error_message="No verified members with opened stores")
                    task_instance.initial_scoring_queue.task_done()
                    continue
                    
                logger.info(f"[Day1ScoringWorker] Run {run_id}: Found {len(responses)} verified responses. Init time: {gfs_init_time}")
                            
                day1_lead_hours = task_instance.config.get('initial_scoring_lead_hours', [6, 12])
                valid_times_for_gfs = [gfs_init_time + timedelta(hours=h) for h in day1_lead_hours]
                gfs_reference_run_time = gfs_init_time
                gfs_reference_lead_hours = day1_lead_hours

                logger.info(f"[Day1ScoringWorker] Run {run_id}: Using Day-1 lead hours {day1_lead_hours} relative to GFS init {gfs_init_time}. Valid times for GFS: {valid_times_for_gfs}")

                gfs_cache_dir = Path(task_instance.config.get('gfs_analysis_cache_dir', './gfs_analysis_cache'))

                resolved_day1_variables_levels_to_score = task_instance.config.get('day1_variables_levels_to_score', [
                    {"name": "z", "level": 500, "standard_name": "geopotential"},
                    {"name": "t", "level": 850, "standard_name": "temperature"},
                    {"name": "2t", "level": None, "standard_name": "2m_temperature"},
                    {"name": "msl", "level": None, "standard_name": "mean_sea_level_pressure"}
                ])

                target_surface_vars_gfs_day1 = []
                target_atmos_vars_gfs_day1 = []
                target_pressure_levels_hpa_day1_set = set()

                for var_info in resolved_day1_variables_levels_to_score:
                    aurora_name = var_info['name']
                    level = var_info.get('level')
                    gfs_name = AURORA_TO_GFS_VAR_MAP.get(aurora_name)

                    if not gfs_name:
                        logger.warning(f"[Day1ScoringWorker] Run {run_id}: Unknown Aurora variable name '{aurora_name}' in day1_variables_levels_to_score. Skipping for GFS fetch.")
                        continue

                    if level is None:
                        if gfs_name not in target_surface_vars_gfs_day1:
                            target_surface_vars_gfs_day1.append(gfs_name)
                    else:
                        if gfs_name not in target_atmos_vars_gfs_day1:
                            target_atmos_vars_gfs_day1.append(gfs_name)
                        target_pressure_levels_hpa_day1_set.add(int(level))
                
                target_pressure_levels_list_day1 = sorted(list(target_pressure_levels_hpa_day1_set)) if target_pressure_levels_hpa_day1_set else None

                logger.info(f"[Day1ScoringWorker] Run {run_id}: Targeting GFS fetch for Day-1. Surface vars: {target_surface_vars_gfs_day1}, Atmos vars: {target_atmos_vars_gfs_day1}, Levels: {target_pressure_levels_list_day1}")


                gfs_analysis_ds_for_run = await fetch_gfs_analysis_data(
                    target_times=valid_times_for_gfs, 
                    cache_dir=gfs_cache_dir,
                    target_surface_vars=target_surface_vars_gfs_day1,
                    target_atmos_vars=target_atmos_vars_gfs_day1,
                    target_pressure_levels_hpa=target_pressure_levels_list_day1
                )
                if gfs_analysis_ds_for_run is None:
                    logger.error(f"[Day1ScoringWorker] Run {run_id}: fetch_gfs_analysis_data returned None.")
                    raise ValueError(f"Failed to fetch GFS analysis data for run {run_id}")
                logger.info(f"[Day1ScoringWorker] Run {run_id}: GFS Analysis data loaded.")

                logger.info(f"[Day1ScoringWorker] Run {run_id}: Attempting to fetch GFS reference forecast...")
                gfs_reference_ds_for_run = await fetch_gfs_data(
                    run_time=gfs_reference_run_time,
                    lead_hours=gfs_reference_lead_hours,
                    target_surface_vars=target_surface_vars_gfs_day1,
                    target_atmos_vars=target_atmos_vars_gfs_day1,
                    target_pressure_levels_hpa=target_pressure_levels_list_day1
                )
                if gfs_reference_ds_for_run is None:
                    logger.error(f"[Day1ScoringWorker] Run {run_id}: fetch_gfs_data for reference forecast returned None.")
                    raise ValueError(f"Failed to fetch GFS reference forecast data for run {run_id}")
                logger.info(f"[Day1ScoringWorker] Run {run_id}: GFS Reference forecast data loaded.")

                logger.info(f"[Day1ScoringWorker] Run {run_id}: Attempting to load ERA5 climatology...")
                era5_climatology_ds = await task_instance._get_or_load_era5_climatology() # Uses to_thread internally
                if era5_climatology_ds is None:
                    logger.error(f"[Day1ScoringWorker] Run {run_id}: _get_or_load_era5_climatology returned None.")
                    raise ValueError(f"Failed to load ERA5 climatology for run {run_id}")
                
                logger.info(f"[Day1ScoringWorker] Run {run_id}: GFS Analysis, GFS Reference, and ERA5 Climatology prepared.")
                
                day1_scoring_config = {
                    "evaluation_valid_times": valid_times_for_gfs,
                    "variables_levels_to_score": resolved_day1_variables_levels_to_score, # Use the consolidated list
                    "climatology_bounds": task_instance.config.get('day1_climatology_bounds', {
                        "2t": (180, 340), "msl": (90000, 110000),
                        "t500": (200, 300), "t850": (220, 320),
                        "z500": (4000, 6000)
                    }),
                    "pattern_correlation_threshold": task_instance.config.get("day1_pattern_correlation_threshold", 0.3),
                    "acc_lower_bound_d1": task_instance.config.get("day1_acc_lower_bound", 0.6),
                    "alpha_skill": task_instance.config.get("day1_alpha_skill", 0.6),
                    "beta_acc": task_instance.config.get("day1_beta_acc", 0.4),
                    "clone_penalty_gamma": task_instance.config.get("day1_clone_penalty_gamma", 1.0),
                    "clone_delta_thresholds": task_instance.config.get("day1_clone_delta_thresholds", {})
                }

                logger.info(f"[Day1ScoringWorker] Run {run_id}: Configured. Starting evaluation for {len(responses)} miners.")

                # MAJOR PERFORMANCE OPTIMIZATION: Pre-compute climatology interpolations once
                # instead of computing them for each miner individually
                logger.info(f"[Day1ScoringWorker] Run {run_id}: Pre-computing climatology cache for massive performance boost...")
                
                # Get a sample target grid from GFS analysis data
                sample_var = None
                for var_name in ['2t', 'msl', 'z', 't']:  # Try common variables
                    if var_name in gfs_analysis_ds_for_run.data_vars:
                        sample_var = gfs_analysis_ds_for_run[var_name]
                        break
                
                if sample_var is None:
                    logger.warning(f"[Day1ScoringWorker] Run {run_id}: Could not find suitable sample variable for target grid template")
                    sample_var = next(iter(gfs_analysis_ds_for_run.data_vars.values()))  # Use first available variable
                
                # Determine times to evaluate (same logic as in evaluate_miner_forecast_day1)
                hardcoded_valid_times = day1_scoring_config.get("hardcoded_valid_times_for_eval")
                if hardcoded_valid_times:
                    times_to_evaluate = hardcoded_valid_times
                    logger.info(f"[Day1ScoringWorker] Run {run_id}: Using hardcoded times for climatology cache: {times_to_evaluate}")
                else:
                    lead_times_to_score_hours = day1_scoring_config.get('lead_times_hours', task_instance.config.get('initial_scoring_lead_hours', [6,12]))
                    times_to_evaluate = [gfs_init_time + timedelta(hours=h) for h in lead_times_to_score_hours]
                    logger.info(f"[Day1ScoringWorker] Run {run_id}: Using lead_hours {lead_times_to_score_hours} for climatology cache")
                
                # Pre-compute climatology cache - this is where the magic happens!
                try:
                    climatology_cache_start_time = time.time()
                    precomputed_climatology = await precompute_climatology_cache(
                        era5_climatology_ds,
                        day1_scoring_config,
                        times_to_evaluate,
                        sample_var.isel(time=0) if 'time' in sample_var.dims else sample_var  # Remove time dimension for template
                    )
                    climatology_cache_time = time.time() - climatology_cache_start_time
                    logger.info(f"[Day1ScoringWorker] Run {run_id}: ✅ Pre-computed climatology cache in {climatology_cache_time:.2f}s - this will dramatically speed up miner scoring!")
                except Exception as e_cache:
                    logger.error(f"[Day1ScoringWorker] Run {run_id}: Failed to pre-compute climatology cache: {e_cache}")
                    logger.warning(f"[Day1ScoringWorker] Run {run_id}: Falling back to individual climatology computation per miner")
                    precomputed_climatology = None

                scoring_tasks = []
                
                for miner_response_rec in responses:
                    logger.debug(f"[Day1ScoringWorker] Run {run_id}: Creating scoring task for miner {miner_response_rec.get('miner_hotkey')}")
                    scoring_tasks.append(
                        evaluate_miner_forecast_day1(
                            task_instance, 
                            miner_response_rec,
                            gfs_analysis_ds_for_run,
                            gfs_reference_ds_for_run,
                            era5_climatology_ds,
                            day1_scoring_config,
                            gfs_init_time,
                            precomputed_climatology  # NEW: Pass the pre-computed climatology cache
                        )
                    )

                    if len(responses) > 10 and len(scoring_tasks) % 10 == 0:
                        await asyncio.sleep(0) 

                    if len(responses) > 10 and len(scoring_tasks) % 10 == 0:
                        await asyncio.sleep(0) 
                
                logger.info(f"[Day1ScoringWorker] Run {run_id}: Created {len(scoring_tasks)} scoring tasks.")
                evaluation_results = await asyncio.gather(*scoring_tasks, return_exceptions=True)
                
                # CRITICAL: Comprehensive cleanup after all miners processed
                logger.info(f"[Day1ScoringWorker] Run {run_id}: Starting comprehensive cleanup after scoring all miners...")
                
                # Check memory before cleanup
                try:
                    process = psutil.Process()
                    memory_before_mb = process.memory_info().rss / (1024 * 1024)
                    logger.info(f"[Day1ScoringWorker] Run {run_id}: Memory before cleanup: {memory_before_mb:.1f} MB")
                except Exception:
                    memory_before_mb = None
                
                # AGGRESSIVE MEMORY CLEANUP inspired by substrate manager approach
                try:
                    import sys
                    
                    # 1. Clear xarray/dask/numpy module-level caches
                    for module_name in list(sys.modules.keys()):
                        if any(pattern in module_name.lower() for pattern in 
                               ['xarray', 'dask', 'numpy', 'pandas', 'fsspec', 'zarr', 'netcdf4', 'h5py', 'scipy']):
                            module = sys.modules.get(module_name)
                            if hasattr(module, '__dict__'):
                                for attr_name in list(module.__dict__.keys()):
                                    if any(cache_pattern in attr_name.lower() for cache_pattern in 
                                           ['cache', 'registry', '_cached', '__pycache__', '_instance_cache']):
                                        try:
                                            cache_obj = getattr(module, attr_name)
                                            if hasattr(cache_obj, 'clear'):
                                                cache_obj.clear()
                                                logger.debug(f"[Day1ScoringWorker] Run {run_id}: Cleared {module_name}.{attr_name}")
                                            elif isinstance(cache_obj, dict):
                                                cache_obj.clear()
                                                logger.debug(f"[Day1ScoringWorker] Run {run_id}: Cleared dict {module_name}.{attr_name}")
                                        except Exception:
                                            pass
                    
                    # 2. Force multiple garbage collection passes (like substrate manager)
                    collected_total = 0
                    for gc_pass in range(5):  # Multiple passes to catch circular references
                        collected = gc.collect()
                        collected_total += collected
                        if collected == 0:
                            break  # No more objects to collect
                        logger.debug(f"[Day1ScoringWorker] Run {run_id}: GC pass {gc_pass + 1} collected {collected} objects")
                    
                    # 3. Clear Python's internal caches
                    try:
                        import sys
                        if hasattr(sys, '_clear_type_cache'):
                            sys._clear_type_cache()
                            logger.debug(f"[Day1ScoringWorker] Run {run_id}: Cleared Python type cache")
                    except Exception:
                        pass
                    
                    # 4. Try to force memory defragmentation
                    try:
                        # Force malloc trim on Linux (returns memory to OS)
                        import ctypes
                        try:
                            libc = ctypes.CDLL("libc.so.6")
                            libc.malloc_trim(0)
                            logger.debug(f"[Day1ScoringWorker] Run {run_id}: Performed malloc_trim")
                        except Exception:
                            pass
                    except Exception:
                        pass
                    
                    # 5. Clear netCDF4/HDF5 caches if possible
                    try:
                        import netCDF4
                        if hasattr(netCDF4, '_default_fillvals'):
                            netCDF4._default_fillvals.clear()
                    except Exception:
                        pass
                    
                    # 6. Clear numpy internal caches
                    try:
                        import numpy as np
                        if hasattr(np, '_NoValue'):
                            # Clear numpy's internal caches
                            for attr in ['_TypeCodes', '_names', '_typestr', '_ctypes']:
                                if hasattr(np, attr):
                                    obj = getattr(np, attr)
                                    if hasattr(obj, 'clear'):
                                        obj.clear()
                    except Exception:
                        pass
                    
                    logger.info(f"[Day1ScoringWorker] Run {run_id}: Aggressive cleanup collected {collected_total} objects")
                    
                except Exception as cleanup_err:
                    logger.debug(f"[Day1ScoringWorker] Run {run_id}: Error in aggressive cleanup: {cleanup_err}")
                
                # Final garbage collection
                gc.collect()
                
                # Check memory after cleanup and report effectiveness
                try:
                    if memory_before_mb is not None:
                        process = psutil.Process()
                        memory_after_mb = process.memory_info().rss / (1024 * 1024)
                        memory_freed_mb = memory_before_mb - memory_after_mb
                        logger.info(f"[Day1ScoringWorker] Run {run_id}: Memory after cleanup: {memory_after_mb:.1f} MB")
                        if memory_freed_mb > 0:
                            logger.info(f"[Day1ScoringWorker] Run {run_id}: ✅ Memory freed: {memory_freed_mb:.1f} MB ({memory_freed_mb/memory_before_mb*100:.1f}%)")
                        else:
                            logger.warning(f"[Day1ScoringWorker] Run {run_id}: ❌ Memory not freed - increased by {abs(memory_freed_mb):.1f} MB")
                            
                        # Check for memory-mapped files that might be holding memory
                        try:
                            open_files = process.open_files()
                            zarr_files = [f for f in open_files if '.zarr' in f.path or 'cache' in f.path]
                            if zarr_files:
                                logger.warning(f"[Day1ScoringWorker] Run {run_id}: Found {len(zarr_files)} open cache/zarr files: {[f.path for f in zarr_files[:3]]}")
                        except Exception:
                            pass
                except Exception:
                    pass

                logger.info(f"[Day1ScoringWorker] Run {run_id}: evaluation_results count: {len(evaluation_results)}")
                successful_scores = 0
                db_update_tasks = []
                
                for i, result in enumerate(evaluation_results):
                    if isinstance(result, Exception):
                        logger.error(f"[Day1ScoringWorker] Run {run_id}: A Day-1 scoring task failed: {result}", exc_info=result)
                        continue
                    
                    if result and isinstance(result, dict):
                        resp_id = result.get("response_id")
                        lead_scores_json = await asyncio.to_thread(json.dumps, result.get("lead_time_scores"), default=str)
                        overall_score = result.get("overall_day1_score")
                        qc_passed = result.get("qc_passed_all_vars_leads")
                        error_msg = result.get("error_message")
                        miner_uid_from_result = result.get("miner_uid")
                        miner_hotkey_from_result = result.get("miner_hotkey")

                        if resp_id is not None:
                            db_params = {
                                "resp_id": resp_id, 
                                "run_id": run_id, 
                                "miner_uid": miner_uid_from_result, 
                                "miner_hotkey": miner_hotkey_from_result, 
                                "score_type": 'day1_qc_score', 
                                "score": overall_score if np.isfinite(overall_score) else -9999.0, 
                                "metrics": lead_scores_json,
                                "ts": datetime.now(timezone.utc),
                                "err": error_msg,
                                "lead_hours_val": None,
                                "variable_level_val": "overall_day1",
                                "valid_time_utc_val": run_record['gfs_init_time_utc'] if run_record else None
                            }
                            db_update_tasks.append(task_instance.db_manager.execute(
                                """INSERT INTO weather_miner_scores 
                                   (response_id, run_id, miner_uid, miner_hotkey, score_type, score, metrics, calculation_time, error_message, lead_hours, variable_level, valid_time_utc)
                                   VALUES (:resp_id, :run_id, :miner_uid, :miner_hotkey, :score_type, :score, :metrics, :ts, :err, :lead_hours_val, :variable_level_val, :valid_time_utc_val)                            
                                   ON CONFLICT (response_id, score_type, lead_hours, variable_level, valid_time_utc) DO UPDATE SET
                                   score = EXCLUDED.score, metrics = EXCLUDED.metrics, miner_uid = EXCLUDED.miner_uid, miner_hotkey = EXCLUDED.miner_hotkey,
                                   calculation_time = EXCLUDED.calculation_time, error_message = EXCLUDED.error_message
                                """,
                                db_params
                            ))
                            if error_msg:
                                 logger.warning(f"[Day1ScoringWorker] Miner {result.get('miner_hotkey')} Day-1 scoring for resp {resp_id} completed with error: {error_msg}")
                            elif overall_score is not None and np.isfinite(overall_score):
                                successful_scores += 1
                                logger.info(f"[Day1ScoringWorker] Miner {result.get('miner_hotkey')} Day-1 Score (Resp {resp_id}): {overall_score:.4f}, QC Overall: {qc_passed}")
                            else:
                                logger.warning(f"[Day1ScoringWorker] Miner {result.get('miner_hotkey')} Day-1 scoring for resp {resp_id} resulted in invalid score: {overall_score}")
                    if len(evaluation_results) > 20 and i % 20 == 19:
                        await asyncio.sleep(0)
                    if len(evaluation_results) > 20 and i % 20 == 19:
                        await asyncio.sleep(0)
                
                if db_update_tasks:
                    try:
                        await asyncio.gather(*db_update_tasks)
                        logger.info(f"[Day1ScoringWorker] Run {run_id}: Stored Day-1 QC scores for {len(db_update_tasks)} responses.")
                    except Exception as db_store_err:
                        logger.error(f"[Day1ScoringWorker] Run {run_id}: Error storing Day-1 QC scores to DB: {db_store_err}", exc_info=True)

                logger.info(f"[Day1ScoringWorker] Run {run_id}: Successfully processed Day-1 QC for {successful_scores}/{len(responses)} miner responses.")
                
                try:
                    logger.info(f"[Day1ScoringWorker] Run {run_id}: Triggering update of combined weather scores.")
                    await task_instance.update_combined_weather_scores(run_id_trigger=run_id)
                except Exception as e_comb_score:
                    logger.error(f"[Day1ScoringWorker] Run {run_id}: Error triggering combined score update: {e_comb_score}", exc_info=True)

                await _update_run_status(task_instance, run_id, "day1_scoring_complete")
                # Mark persistent scoring job as completed successfully
                await task_instance._complete_scoring_job(run_id, 'day1_qc', success=True)
                logger.info(f"[Day1ScoringWorker] Run {run_id}: Marked as day1_scoring_complete.")
                
                task_instance.initial_scoring_queue.task_done()

                if task_instance.test_mode and run_id == task_instance.last_test_mode_run_id:
                    logger.info(f"[Day1ScoringWorker] TEST MODE: Signaling scoring completion for run {run_id}.")
                    task_instance.test_mode_run_scored_event.set()
                
                # CRITICAL: Cleanup after successful run completion (moved from finally block)
                logger.info(f"[Day1ScoringWorker] Run {run_id}: Starting comprehensive cleanup after successful run completion...")
                
                # Close shared datasets immediately after each run
                if gfs_analysis_ds_for_run and hasattr(gfs_analysis_ds_for_run, 'close'):
                    try: 
                        gfs_analysis_ds_for_run.close()
                        logger.debug(f"[Day1ScoringWorker] Run {run_id}: Closed GFS analysis dataset")
                    except Exception: 
                        pass
                if gfs_reference_ds_for_run and hasattr(gfs_reference_ds_for_run, 'close'):
                    try: 
                        gfs_reference_ds_for_run.close()
                        logger.debug(f"[Day1ScoringWorker] Run {run_id}: Closed GFS reference dataset")
                    except Exception: 
                        pass
                
                # Clear large objects from this run
                try:
                    large_objects = [
                        'gfs_analysis_ds_for_run', 'gfs_reference_ds_for_run', 'evaluation_results',
                        'scoring_tasks', 'responses', 'db_update_tasks'
                    ]
                    
                    for obj_name in large_objects:
                        if obj_name in locals():
                            try:
                                obj = locals()[obj_name]
                                if hasattr(obj, 'close') and obj_name != 'era5_climatology_ds':
                                    obj.close()
                                del obj
                                logger.debug(f"[Day1ScoringWorker] Run {run_id}: Cleared {obj_name}")
                            except Exception:
                                pass
                    
                    # Force garbage collection after each run 
                    final_collected = gc.collect()
                    logger.info(f"[Day1ScoringWorker] Run {run_id}: End-of-run cleanup collected {final_collected} objects")
                    
                except Exception as cleanup_err:
                    logger.debug(f"[Day1ScoringWorker] Run {run_id}: Error in end-of-run cleanup: {cleanup_err}")
                
                gc.collect()

            except Exception as e:
                logger.error(f"[Day1ScoringWorker] Unexpected error processing run {run_id}: {e}", exc_info=True)
                if run_id:
                    try:
                        await _update_run_status(task_instance, run_id, "day1_scoring_failed", error_message=f"Day1 Worker error: {e}")
                        # Mark persistent scoring job as failed
                        await task_instance._complete_scoring_job(run_id, 'day1_qc', success=False, error_message=f"Day1 Worker error: {e}")
                    except Exception as db_err:
                        logger.error(f"[Day1ScoringWorker] Failed to update DB status on error: {db_err}")
                if task_instance.initial_scoring_queue._unfinished_tasks > 0:
                    task_instance.initial_scoring_queue.task_done()
            finally:
                # Simplified cleanup for exception cases only (main cleanup happens after successful completion)
                logger.debug(f"[Day1ScoringWorker] Run {run_id}: Emergency cleanup in finally block...")
                
                if gfs_analysis_ds_for_run and hasattr(gfs_analysis_ds_for_run, 'close'):
                    try: 
                        gfs_analysis_ds_for_run.close()
                    except Exception: 
                        pass
                if gfs_reference_ds_for_run and hasattr(gfs_reference_ds_for_run, 'close'):
                    try: 
                        gfs_reference_ds_for_run.close()
                    except Exception: 
                        pass

    except asyncio.CancelledError:
        logger.info("[InitialScoringWorker] Worker cancelled, shutting down gracefully")
        task_instance.initial_scoring_worker_running = False
        raise
    except Exception as e:
        logger.error(f"[InitialScoringWorker] Fatal error in worker: {e}", exc_info=True)
        task_instance.initial_scoring_worker_running = False
        raise

async def finalize_scores_worker(self):
    """Background worker to calculate final scores against ERA5 after delay."""
    CHECK_INTERVAL_SECONDS = 30 if self.test_mode else int(self.config.get('final_scoring_check_interval_seconds', 3600))
    ERA5_DELAY_DAYS = int(self.config.get('era5_delay_days', 5))
    FORECAST_DURATION_HOURS = int(self.config.get('forecast_duration_hours', 240))
    ERA5_BUFFER_HOURS = int(self.config.get('era5_buffer_hours', 6))

    era5_climatology_ds_for_cycle = await self._get_or_load_era5_climatology()
    if era5_climatology_ds_for_cycle is None:
        logger.error("[FinalizeWorker] ERA5 Climatology not available at worker startup. Worker will not be effective. Please check config.")

    try:
        while self.final_scoring_worker_running:
            run_id = None
            era5_ds = None
            processed_run_ids = set()
            work_done = False

            try:
                logger.info("[FinalizeWorker] Checking for runs ready for final ERA5 scoring...")

                if era5_climatology_ds_for_cycle is None:
                    logger.warning("[FinalizeWorker] ERA5 Climatology was not available, attempting to reload it...")
                    era5_climatology_ds_for_cycle = await self._get_or_load_era5_climatology()
                    if era5_climatology_ds_for_cycle is None:
                        logger.error("[FinalizeWorker] ERA5 Climatology still not available after reload attempt. Cannot proceed with this scoring cycle. Will retry.")
                        await asyncio.sleep(CHECK_INTERVAL_SECONDS)
                        continue

                now_utc = datetime.now(timezone.utc)
                sparse_lead_hours_config = self.config.get('final_scoring_lead_hours', [120, 168]) 
                max_final_lead_hour = max(sparse_lead_hours_config) if sparse_lead_hours_config else 0

                if self.test_mode:
                    logger.info("[FinalizeWorker] TEST MODE: Ignoring ERA5 delay, checking all runs for final scoring")
                    runs_to_score_query = """
                    SELECT id, gfs_init_time_utc
                    FROM weather_forecast_runs
                    WHERE status IN ('day1_scoring_complete', 'ensemble_created', 'completed', 'all_forecasts_failed_verification', 'stalled_no_valid_forecasts', 'initial_scoring_failed', 'final_scoring_failed', 'scored')
                    AND final_scoring_attempted_time IS NULL 
                    ORDER BY gfs_init_time_utc ASC
                    LIMIT 10 
                    """
                    ready_runs = await self.db_manager.fetch_all(runs_to_score_query, {})
                else:
                    init_time_cutoff = now_utc - timedelta(days=ERA5_DELAY_DAYS) - timedelta(hours=max_final_lead_hour) - timedelta(hours=ERA5_BUFFER_HOURS)
                    retry_cutoff_time = now_utc - timedelta(hours=6)
                    runs_to_score_query = """
                    SELECT id, gfs_init_time_utc
                    FROM weather_forecast_runs
                    WHERE status IN ('processing_ensemble', 'completed', 'initial_scoring_failed', 'ensemble_failed', 'final_scoring_failed', 'scored', 'day1_scoring_complete') 
                    AND (final_scoring_attempted_time IS NULL OR final_scoring_attempted_time < :retry_cutoff)
                    AND gfs_init_time_utc < :init_time_cutoff 
                    ORDER BY gfs_init_time_utc ASC
                    LIMIT 10 
                    """
                    ready_runs = await self.db_manager.fetch_all(runs_to_score_query, {
                        "init_time_cutoff": init_time_cutoff,
                        "retry_cutoff": retry_cutoff_time
                    })

                if not ready_runs:
                    logger.debug("[FinalizeWorker] No runs ready for final scoring.")
                else:
                    logger.info(f"[FinalizeWorker] Found {len(ready_runs)} runs potentially ready for final scoring.")
                    work_done = True

                for run in ready_runs:
                    run_id = run['id']
                    if run_id in processed_run_ids: continue

                    gfs_init_time = run['gfs_init_time_utc']
                    era5_ds_for_run = None

                    logger.info(f"[FinalizeWorker] Processing final scores for run {run_id} (Init: {gfs_init_time}).")
                    
                    # Start persistent scoring job tracking
                    await self._start_scoring_job(run_id, 'era5_final')
                    
                    await self.db_manager.execute(
                            "UPDATE weather_forecast_runs SET final_scoring_attempted_time = :now WHERE id = :rid",
                            {"now": now_utc, "rid": run_id}
                    )

                    target_datetimes_for_run = [gfs_init_time + timedelta(hours=h) for h in sparse_lead_hours_config]

                    logger.info(f"[FinalizeWorker] Run {run_id}: Fetching ERA5 analysis for final scoring at lead hours: {sparse_lead_hours_config}.")
                    era5_cache = Path(self.config.get('era5_cache_dir', './era5_cache'))
                    era5_ds_for_run = await fetch_era5_data(target_times=target_datetimes_for_run, cache_dir=era5_cache) # Ensure fetch_era5_data is efficient

                    if era5_ds_for_run is None:
                        logger.error(f"[FinalizeWorker] Run {run_id}: Failed to fetch ERA5 data. Aborting final scoring for this run.")
                        await _update_run_status(self, run_id, "final_scoring_failed", error_message="ERA5 fetch failed")
                        # Mark scoring job as failed
                        await self._complete_scoring_job(run_id, 'era5_final', success=False, error_message="ERA5 fetch failed")
                        processed_run_ids.add(run_id)
                        continue

                    logger.info(f"[FinalizeWorker] Run {run_id}: ERA5 data fetched/loaded.")

                    responses_query = """    
                    SELECT mr.id, mr.miner_hotkey, mr.miner_uid, mr.job_id, mr.run_id
                    FROM weather_miner_responses mr
                    WHERE mr.run_id = :run_id AND mr.verification_passed = TRUE
                    """
                    verified_responses_for_run = await self.db_manager.fetch_all(responses_query, {"run_id": run_id})

                    if not verified_responses_for_run:
                        logger.warning(f"[FinalizeWorker] Run {run_id}: No verified responses. Skipping miner scoring.")
                        await _update_run_status(self, run_id, "final_scoring_skipped_no_verified_miners")
                        # Mark scoring job as completed (skipped case)
                        await self._complete_scoring_job(run_id, 'era5_final', success=True, error_message="No verified responses")
                        processed_run_ids.add(run_id)
                        if era5_ds_for_run: era5_ds_for_run.close()
                        continue

                    logger.info(f"[FinalizeWorker] Run {run_id}: Found {len(verified_responses_for_run)} verified miner responses.")
                    scoring_execution_tasks = []
                    
                    async def score_single_miner_with_semaphore(resp_rec):
                        async with self.era5_scoring_semaphore:
                            logger.debug(f"[FinalizeWorker] Acquired semaphore for scoring miner {resp_rec.get('miner_hotkey')}, Run {run_id}")
                            try:
                                return await calculate_era5_miner_score(
                                    self, resp_rec, target_datetimes_for_run, era5_ds_for_run, era5_climatology_ds_for_cycle
                                )
                            finally:
                                logger.debug(f"[FinalizeWorker] Released semaphore for scoring miner {resp_rec.get('miner_hotkey')}, Run {run_id}")

                    for resp_rec in verified_responses_for_run:
                         scoring_execution_tasks.append(score_single_miner_with_semaphore(resp_rec))
                         if len(verified_responses_for_run) > 5 and len(scoring_execution_tasks) % 5 == 0:
                             await asyncio.sleep(0)

                    miner_scoring_results = await asyncio.gather(*scoring_execution_tasks)
                    successful_final_scores_count = 0
                    
                                    # CRITICAL: Memory cleanup after ERA5 scoring - similar to initial scoring
                logger.info(f"[FinalizeWorker] Run {run_id}: Starting comprehensive memory cleanup after ERA5 scoring...")
                
                # Cleanup async processing resources (minimal cleanup needed)
                try:
                    # Async processing cleanup is handled automatically by asyncio/dask
                    logger.debug(f"[FinalizeWorker] Run {run_id}: Async processing cleanup completed")
                except Exception as async_cleanup_err:
                    logger.debug(f"[FinalizeWorker] Run {run_id}: Error during async processing cleanup: {async_cleanup_err}")
                
                # Check memory before cleanup
                try:
                    process = psutil.Process()
                    memory_before_mb = process.memory_info().rss / (1024 * 1024)
                    logger.info(f"[FinalizeWorker] Run {run_id}: Memory before ERA5 cleanup: {memory_before_mb:.1f} MB")
                except Exception:
                    memory_before_mb = None
                    
                    # AGGRESSIVE MEMORY CLEANUP for ERA5 data
                    try:
                        import sys
                        
                        # 1. Clear ERA5/xarray/dask/numpy module-level caches
                        modules_cleared = 0
                        cache_objects_cleared = 0
                        
                        for module_name in list(sys.modules.keys()):
                            if any(pattern in module_name.lower() for pattern in 
                                   ['xarray', 'dask', 'numpy', 'pandas', 'fsspec', 'zarr', 'netcdf4', 'h5py', 'scipy', 'era5']):
                                module = sys.modules.get(module_name)
                                if hasattr(module, '__dict__'):
                                    module_cleared = False
                                    for attr_name in list(module.__dict__.keys()):
                                        if any(cache_pattern in attr_name.lower() for cache_pattern in 
                                               ['cache', 'registry', '_cached', '__pycache__', '_instance_cache', '_buffer', '_memo']):
                                            try:
                                                cache_obj = getattr(module, attr_name)
                                                if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                                    cache_obj.clear()
                                                    cache_objects_cleared += 1
                                                    module_cleared = True
                                                elif isinstance(cache_obj, (dict, list, set)):
                                                    cache_obj.clear()
                                                    cache_objects_cleared += 1
                                                    module_cleared = True
                                            except Exception:
                                                pass
                                    if module_cleared:
                                        modules_cleared += 1
                        
                        logger.info(f"[FinalizeWorker] Run {run_id}: Cleared {cache_objects_cleared} cache objects from {modules_cleared} modules")
                        
                        # 2. Force multiple garbage collection passes
                        collected_total = 0
                        for gc_pass in range(5):  # Multiple passes for circular references
                            collected = gc.collect()
                            collected_total += collected
                            if collected == 0:
                                break
                            logger.debug(f"[FinalizeWorker] Run {run_id}: GC pass {gc_pass + 1} collected {collected} objects")
                        
                        # 3. Clear Python's internal caches
                        try:
                            import sys
                            if hasattr(sys, '_clear_type_cache'):
                                sys._clear_type_cache()
                                logger.debug(f"[FinalizeWorker] Run {run_id}: Cleared Python type cache")
                        except Exception:
                            pass
                        
                        # 4. Force memory defragmentation for large ERA5 datasets
                        try:
                            import ctypes
                            try:
                                libc = ctypes.CDLL("libc.so.6")
                                libc.malloc_trim(0)
                                logger.debug(f"[FinalizeWorker] Run {run_id}: Performed malloc_trim")
                            except Exception:
                                pass
                        except Exception:
                            pass
                        
                        # 5. Clear library-specific caches
                        try:
                            # Clear netCDF4/HDF5 caches (important for ERA5 data)
                            import netCDF4
                            if hasattr(netCDF4, '_default_fillvals'):
                                netCDF4._default_fillvals.clear()
                                
                            # Clear xarray backend caches
                            import xarray as xr
                            if hasattr(xr.backends, 'plugins'):
                                if hasattr(xr.backends.plugins, 'clear'):
                                    xr.backends.plugins.clear()
                                elif isinstance(xr.backends.plugins, dict):
                                    xr.backends.plugins.clear()
                                    
                            # Clear dask caches
                            try:
                                import dask
                                if hasattr(dask, 'base') and hasattr(dask.base, 'tokenize'):
                                    if hasattr(dask.base.tokenize, 'cache'):
                                        dask.base.tokenize.cache.clear()
                            except ImportError:
                                pass
                            
                        except ImportError:
                            pass
                        
                        logger.info(f"[FinalizeWorker] Run {run_id}: ERA5 aggressive cleanup collected {collected_total} objects")
                        
                    except Exception as cleanup_err:
                        logger.debug(f"[FinalizeWorker] Run {run_id}: Error in ERA5 aggressive cleanup: {cleanup_err}")
                    
                    # Final garbage collection
                    gc.collect()
                    
                    # Check memory after cleanup and report effectiveness
                    try:
                        if memory_before_mb is not None:
                            process = psutil.Process()
                            memory_after_mb = process.memory_info().rss / (1024 * 1024)
                            memory_freed_mb = memory_before_mb - memory_after_mb
                            logger.info(f"[FinalizeWorker] Run {run_id}: Memory after ERA5 cleanup: {memory_after_mb:.1f} MB")
                            if memory_freed_mb > 0:
                                logger.info(f"[FinalizeWorker] Run {run_id}: ✅ ERA5 memory freed: {memory_freed_mb:.1f} MB ({memory_freed_mb/memory_before_mb*100:.1f}%)")
                            else:
                                logger.warning(f"[FinalizeWorker] Run {run_id}: ❌ ERA5 memory not freed - increased by {abs(memory_freed_mb):.1f} MB")
                                
                            # Check for lingering ERA5/zarr files
                            try:
                                open_files = process.open_files()
                                era5_files = [f for f in open_files if any(pattern in f.path.lower() for pattern in ['.zarr', 'era5', 'cache', '.nc'])]
                                if era5_files:
                                    logger.warning(f"[FinalizeWorker] Run {run_id}: Found {len(era5_files)} open ERA5/cache files: {[f.path for f in era5_files[:3]]}")
                            except Exception:
                                pass
                    except Exception:
                        pass

                    # CRITICAL: Close large datasets before aggregate calculation 
                    # The aggregate score calculation only needs database records, not the large datasets
                    logger.info(f"[FinalizeWorker] Run {run_id}: Closing large ERA5 datasets before aggregate calculations...")
                    try:
                        if era5_ds_for_run:
                            era5_ds_for_run.close()
                            logger.debug(f"[FinalizeWorker] Run {run_id}: Closed era5_ds_for_run")
                            era5_ds_for_run = None
                            
                        if 'era5_climatology_ds_for_cycle' in locals():
                            era5_climatology_ds_for_cycle.close()
                            logger.debug(f"[FinalizeWorker] Run {run_id}: Closed era5_climatology_ds_for_cycle")
                            era5_climatology_ds_for_cycle = None
                            
                        # Force garbage collection after closing large datasets
                        collected = gc.collect()
                        logger.info(f"[FinalizeWorker] Run {run_id}: 🗑️ Closed large ERA5 datasets, GC collected {collected} objects before aggregate calculations")
                        
                    except Exception as close_err:
                        logger.debug(f"[FinalizeWorker] Run {run_id}: Error closing datasets: {close_err}")

                    for i_resp, miner_score_task_succeeded in enumerate(miner_scoring_results): # Process results
                        resp_rec_inner = verified_responses_for_run[i_resp]
                        if miner_score_task_succeeded: 
                            logger.info(f"[FinalizeWorker] Run {run_id}: Detailed ERA5 metrics stored for UID {resp_rec_inner['miner_uid']}. Calculating aggregated score.")
                            final_vars_levels_cfg = self.config.get('final_scoring_variables_levels', self.config.get('day1_variables_levels_to_score'))
                            
                            agg_score = await _calculate_and_store_aggregated_era5_score(
                                task_instance=self, run_id=run_id, miner_uid=resp_rec_inner['miner_uid'],
                                miner_hotkey=resp_rec_inner['miner_hotkey'], response_id=resp_rec_inner['id'], 
                                lead_hours_scored=sparse_lead_hours_config, vars_levels_scored=final_vars_levels_cfg
                            )
                            if agg_score is not None:
                                successful_final_scores_count += 1
                                logger.info(f"[FinalizeWorker] Run {run_id}: Aggregated ERA5 score for UID {resp_rec_inner['miner_uid']}: {agg_score:.4f}")
                            else:
                                logger.warning(f"[FinalizeWorker] Run {run_id}: Failed to calculate/store aggregated ERA5 score for UID {resp_rec_inner['miner_uid']}.")
                        else:
                            logger.warning(f"[FinalizeWorker] Run {run_id}: Skipping aggregated score for UID {resp_rec_inner['miner_uid']} (detailed scoring failed).")
                        if len(verified_responses_for_run) > 10 and i_resp % 10 == 9:
                            await asyncio.sleep(0)

                    logger.info(f"[FinalizeWorker] Run {run_id}: Completed final scoring for {successful_final_scores_count}/{len(verified_responses_for_run)} miners.")
                    
                    if successful_final_scores_count > 0: 
                        logger.info(f"[FinalizeWorker] Run {run_id}: Marked as 'scored'.")
                        await _update_run_status(self, run_id, "scored")
                        # Mark scoring job as completed successfully
                        await self._complete_scoring_job(run_id, 'era5_final', success=True)
                        try:
                            logger.info(f"[FinalizeWorker] Run {run_id}: Triggering update of combined weather scores.")
                            await self.update_combined_weather_scores(run_id_trigger=run_id)
                        except Exception as e_comb_score:
                            logger.error(f"[FinalizeWorker] Run {run_id}: Error triggering combined score update: {e_comb_score}", exc_info=True)
                    else:
                         logger.warning(f"[FinalizeWorker] Run {run_id}: No miners successfully scored against ERA5. Skipping combined score update.")
                         # Mark scoring job as completed but with limited success
                         await self._complete_scoring_job(run_id, 'era5_final', success=True, error_message="No miners successfully scored")
                    processed_run_ids.add(run_id)
                    
                    # Final cleanup for any remaining dataset references (defensive)
                    logger.debug(f"[FinalizeWorker] Run {run_id}: Final cleanup - ensuring all datasets are closed")
                    for ds_name in ['era5_ds_for_run', 'era5_climatology_ds_for_cycle']:
                        if ds_name in locals() and locals()[ds_name] is not None:
                            try: 
                                locals()[ds_name].close()
                                logger.debug(f"[FinalizeWorker] Run {run_id}: Final cleanup closed {ds_name}")
                            except Exception: 
                                pass

                if work_done:
                    gc.collect()

            except Exception as e:
                logger.error(f"[FinalizeWorker] Unexpected error (Last run_id: {run_id}): {e}", exc_info=True)
                if run_id:
                    try: await _update_run_status(self, run_id, "final_scoring_failed", error_message=f"Worker loop error: {e}")
                    except Exception as db_err:
                        logger.error(f"[FinalizeWorker] Failed to update DB status on error: {db_err}")

            try:
                logger.debug(f"[FinalizeWorker] Sleeping for {CHECK_INTERVAL_SECONDS} seconds...")
                await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                logger.info("[FinalizeWorker] Sleep interrupted, worker stopping.")
                break
                
    except asyncio.CancelledError:
        logger.info("[FinalizeWorker] Worker cancelled, shutting down gracefully")
        self.final_scoring_worker_running = False
        raise
    except Exception as e:
        logger.error(f"[FinalizeWorker] Fatal error in worker: {e}", exc_info=True)
        self.final_scoring_worker_running = False
        raise

async def cleanup_worker(task_instance: 'WeatherTask'):
    """Periodically cleans up old cache files, ensemble files, and DB records."""
    CHECK_INTERVAL_SECONDS = int(task_instance.config.get('cleanup_check_interval_seconds', 6 * 3600))
    GFS_CACHE_RETENTION_DAYS = int(task_instance.config.get('gfs_cache_retention_days', 7))
    ERA5_CACHE_RETENTION_DAYS = int(task_instance.config.get('era5_cache_retention_days', 30))
    ENSEMBLE_RETENTION_DAYS = int(task_instance.config.get('ensemble_retention_days', 14))
    DB_RUN_RETENTION_DAYS = int(task_instance.config.get('db_run_retention_days', 90))
    
    gfs_cache_dir = Path(task_instance.config.get('gfs_analysis_cache_dir', './gfs_analysis_cache'))
    era5_cache_dir = Path(task_instance.config.get('era5_cache_dir', './era5_cache'))
    ensemble_dir = VALIDATOR_ENSEMBLE_DIR
    
    def _blocking_cleanup_directory(dir_path_str: str, retention_days: int, pattern: str, current_time_ts: float):
        dir_path = Path(dir_path_str)
        if not dir_path.is_dir():
            logger.debug(f"[CleanupWorker] Directory not found, skipping: {dir_path}")
            return 0
            
        cutoff_time = current_time_ts - (retention_days * 24 * 3600)
        deleted_count = 0
        try:
            for item_path in dir_path.glob(pattern):
                try:
                    if item_path.is_file():
                        file_mod_time = item_path.stat().st_mtime
                        if file_mod_time < cutoff_time:
                            item_path.unlink()
                            logger.debug(f"[CleanupWorker] Deleted old file: {item_path}")
                            deleted_count += 1
                except FileNotFoundError:
                    continue 
                except Exception as e_file:
                    logger.warning(f"[CleanupWorker] Error processing item {item_path} for deletion: {e_file}")

            if pattern == "*" or "*" in pattern:
                 for item in dir_path.iterdir(): 
                      if item.is_dir():
                           try:
                                if not any(item.iterdir()):
                                     shutil.rmtree(item)
                                     logger.debug(f"[CleanupWorker] Removed empty/old directory: {item}")
                           except OSError as e_dir:
                                logger.warning(f"[CleanupWorker] Error removing dir {item}: {e_dir}")
                                
        except Exception as e_glob:
             logger.error(f"[CleanupWorker] Error processing directory {dir_path} with pattern {pattern}: {e_glob}")
        logger.info(f"[CleanupWorker] Deleted {deleted_count} items older than {retention_days} days from {dir_path} matching '{pattern}'.")
        return deleted_count

    try:
        while task_instance.cleanup_worker_running:
            try:
                now_ts = time.time()
                now_dt_utc = datetime.now(timezone.utc)
                logger.info("[CleanupWorker] Starting cleanup cycle...")

                logger.info("[CleanupWorker] Cleaning up GFS cache...")
                await asyncio.to_thread(_blocking_cleanup_directory, str(gfs_cache_dir), GFS_CACHE_RETENTION_DAYS, "*.nc", now_ts)
                
                logger.info("[CleanupWorker] Cleaning up ERA5 cache...")
                await asyncio.to_thread(_blocking_cleanup_directory, str(era5_cache_dir), ERA5_CACHE_RETENTION_DAYS, "*.nc", now_ts)
                
                logger.info("[CleanupWorker] Cleaning up Ensemble files (NetCDF and JSON)...")
                await asyncio.to_thread(_blocking_cleanup_directory, str(ensemble_dir), ENSEMBLE_RETENTION_DAYS, "*.nc", now_ts)
                await asyncio.to_thread(_blocking_cleanup_directory, str(ensemble_dir), ENSEMBLE_RETENTION_DAYS, "*.json", now_ts)
                
                logger.info("[CleanupWorker] Cleaning up Miner Forecast Zarr stores...")
                await asyncio.to_thread(_blocking_cleanup_directory, str(MINER_FORECAST_DIR_BG), ENSEMBLE_RETENTION_DAYS, "*.zarr", now_ts)

                # Add cleanup for miner input batches
                if task_instance.node_type == "miner":
                    logger.info("[CleanupWorker] Cleaning up Miner Input Batch pickle files...")
                    await asyncio.to_thread(_blocking_cleanup_directory, str(MINER_INPUT_BATCHES_DIR), 
                                          task_instance.config.get('input_batch_retention_days', 3), "*.pkl", now_ts)

                logger.info("[CleanupWorker] Cleaning up old database records...")
                db_cutoff_time = now_dt_utc - timedelta(days=DB_RUN_RETENTION_DAYS)
                try:
                    delete_runs_query = "DELETE FROM weather_forecast_runs WHERE run_initiation_time < :cutoff"
                    result = await task_instance.db_manager.execute(delete_runs_query, {"cutoff": db_cutoff_time})
                    if result and hasattr(result, 'rowcount'):
                        logger.info(f"[CleanupWorker] Deleted {result.rowcount} old runs (and related data via cascade) older than {db_cutoff_time}.")
                    else:
                         logger.info(f"[CleanupWorker] Executed old run deletion query (actual rowcount might not be available from driver).")
                         
                except Exception as e_db:
                    logger.error(f"[CleanupWorker] Error during database cleanup: {e_db}", exc_info=True)
                    
                logger.info("[CleanupWorker] Cleanup cycle finished.")
                
                gc.collect()

            except Exception as e_outer:
                logger.error(f"[CleanupWorker] Unexpected error in main loop: {e_outer}", exc_info=True)
                await asyncio.sleep(60)
                
            try:
                await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                 logger.info("[CleanupWorker] Sleep interrupted, worker stopping.")
                 break
                 
    except asyncio.CancelledError:
        logger.info("[CleanupWorker] Worker cancelled, shutting down gracefully")
        task_instance.cleanup_worker_running = False
        raise
    except Exception as e:
        logger.error(f"[CleanupWorker] Fatal error in worker: {e}", exc_info=True)
        task_instance.cleanup_worker_running = False
        raise


async def fetch_and_hash_gfs_task(
    task_instance: 'WeatherTask',
    job_id: str,
    t0_run_time: datetime,
    t_minus_6_run_time: datetime
):
    """
    Background task for miners: Fetches GFS analysis data for T=0h and T=-6h,
    prepares the initial Aurora Batch, saves it, computes the canonical input hash,
    and updates the job record in the database with the path and hash.
    """
    logger.info(f"[FetchHashTask Job {job_id}] Starting GFS fetch, batch prep, and input hash computation for T0={t0_run_time}, T-6={t_minus_6_run_time}")
    input_batch_pickle_file_path_str = None # Initialize here for broader scope
    initial_batch: Optional[BatchType] = None # Initialize initial_batch to None

    # Set current job for progress tracking
    task_instance.current_job_id = job_id

    # Define progress callback
    async def progress_callback(update_dict):
        update = WeatherProgressUpdate(**update_dict)
        await task_instance._emit_progress(update)

    try:
        await update_job_status(task_instance, job_id, "fetching_gfs")

        cache_dir_str = task_instance.config.get('gfs_analysis_cache_dir', './gfs_analysis_cache')
        gfs_cache_dir = Path(cache_dir_str)
        gfs_cache_dir.mkdir(parents=True, exist_ok=True)

        # --- Fetch GFS Data with Progress Tracking ---
        await task_instance._emit_progress(WeatherProgressUpdate(
            operation="gfs_fetch",
            stage="starting",
            progress=0.0,
            message=f"Starting GFS data fetch for job {job_id[:8]}"
        ))

        logger.info(f"[FetchHashTask Job {job_id}] Fetching GFS T0 data ({t0_run_time})...")
        ds_t0 = await fetch_gfs_analysis_data([t0_run_time], cache_dir=gfs_cache_dir, progress_callback=progress_callback)
        
        logger.info(f"[FetchHashTask Job {job_id}] Fetching GFS T-6 data ({t_minus_6_run_time})...")
        ds_t_minus_6 = await fetch_gfs_analysis_data([t_minus_6_run_time], cache_dir=gfs_cache_dir, progress_callback=progress_callback)

        if ds_t0 is None or ds_t_minus_6 is None:
            logger.error(f"[FetchHashTask Job {job_id}] Failed to fetch GFS data (T0 or T-6 is None).")
            await update_job_status(task_instance, job_id, "fetch_error", "Failed to fetch GFS data for batch preparation.")
            await task_instance._emit_progress(WeatherProgressUpdate(
                operation="gfs_fetch",
                stage="error",
                progress=0.0,
                message="Failed to fetch GFS data"
            ))
            return

        # --- Prepare Aurora Batch with Progress ---
        await task_instance._emit_progress(WeatherProgressUpdate(
            operation="batch_preparation",
            stage="processing",
            progress=0.7,
            message="Preparing Aurora batch from GFS data"
        ))

        logger.info(f"[FetchHashTask Job {job_id}] Preparing Aurora Batch from GFS data...")
        gfs_concat_data_for_batch_prep = xr.concat([ds_t0, ds_t_minus_6], dim='time').sortby('time')
        initial_batch = await asyncio.to_thread(
            create_aurora_batch_from_gfs,
            gfs_data=gfs_concat_data_for_batch_prep,
            resolution='0.25',
            download_dir='./static_data',
            history_steps=2
        )
        if initial_batch is None:
            logger.error(f"[FetchHashTask Job {job_id}] Failed to create Aurora Batch (create_aurora_batch_from_gfs returned None).")
            await update_job_status(task_instance, job_id, "error", "Failed to prepare initial Aurora batch.")
            await task_instance._emit_progress(WeatherProgressUpdate(
                operation="batch_preparation",
                stage="error",
                progress=0.0,
                message="Failed to prepare initial Aurora batch"
            ))
            return
        logger.info(f"[FetchHashTask Job {job_id}] Aurora Batch prepared successfully.")
        await task_instance._emit_progress(WeatherProgressUpdate(
            operation="batch_preparation",
            stage="completed",
            progress=1.0,
            message="Aurora batch prepared successfully"
        ))

        # --- Save Pickled Aurora Batch with Progress ---
        await task_instance._emit_progress(WeatherProgressUpdate(
            operation="batch_saving",
            stage="processing",
            progress=0.8,
            message="Saving Aurora batch to local storage"
        ))

        job_id_prefix = job_id.split('-')[0]
        miner_hotkey_for_filename = "unknown_miner_hk"
        if task_instance.keypair and task_instance.keypair.ss58_address:
            miner_hotkey_for_filename = task_instance.keypair.ss58_address
        input_batch_filename = f"input_batch_{t0_run_time.strftime('%Y%m%d%H')}_miner_hk_{miner_hotkey_for_filename[:10]}_{job_id_prefix}.pkl"
        input_batch_pickle_file_path = MINER_INPUT_BATCHES_DIR / input_batch_filename
        
        try:
            with open(input_batch_pickle_file_path, "wb") as f:
                pickle.dump(initial_batch, f)
            input_batch_pickle_file_path_str = str(input_batch_pickle_file_path)
            logger.info(f"[FetchHashTask Job {job_id}] Aurora Batch saved to: {input_batch_pickle_file_path_str}")
            
            await task_instance._emit_progress(WeatherProgressUpdate(
                operation="batch_saving",
                stage="completed",
                progress=1.0,
                message=f"Aurora batch saved ({input_batch_pickle_file_path.stat().st_size} bytes)",
                bytes_downloaded=input_batch_pickle_file_path.stat().st_size,
                bytes_total=input_batch_pickle_file_path.stat().st_size
            ))
        except Exception as e_save:
            logger.error(f"[FetchHashTask Job {job_id}] Failed to save Aurora Batch pickle: {e_save}", exc_info=True)
            await update_job_status(task_instance, job_id, "error", f"Failed to save Aurora Batch: {e_save}")
            await task_instance._emit_progress(WeatherProgressUpdate(
                operation="batch_saving",
                stage="error",
                progress=0.0,
                message=f"Failed to save Aurora batch: {e_save}"
            ))
            return

        # --- Compute Input Hash with Progress ---
        await task_instance._emit_progress(WeatherProgressUpdate(
            operation="hash_computation",
            stage="processing",
            progress=0.9,
            message="Computing canonical input data hash"
        ))

        await update_job_status(task_instance, job_id, "hashing_input")
        logger.info(f"[FetchHashTask Job {job_id}] Computing canonical input data hash...")
        try:
            canonical_input_hash = await compute_input_data_hash(
                t0_run_time=t0_run_time,
                t_minus_6_run_time=t_minus_6_run_time,
                cache_dir=gfs_cache_dir
            )
            if not canonical_input_hash:
                logger.error(f"[FetchHashTask Job {job_id}] Hash computation returned None/empty.")
                await update_job_status(task_instance, job_id, "error", "Input data hash computation failed.")
                await task_instance._emit_progress(WeatherProgressUpdate(
                    operation="hash_computation",
                    stage="error",
                    progress=0.0,
                    message="Hash computation failed"
                ))
                return

            logger.info(f"[FetchHashTask Job {job_id}] Computed input hash: {canonical_input_hash[:16]}...")
            await task_instance._emit_progress(WeatherProgressUpdate(
                operation="hash_computation",
                stage="completed",
                progress=1.0,
                message=f"Hash computed: {canonical_input_hash[:16]}..."
            ))
        except Exception as e_hash:
            logger.error(f"[FetchHashTask Job {job_id}] Hash computation error: {e_hash}", exc_info=True)
            await update_job_status(task_instance, job_id, "error", f"Hash computation failed: {e_hash}")
            await task_instance._emit_progress(WeatherProgressUpdate(
                operation="hash_computation",
                stage="error",
                progress=0.0,
                message=f"Hash computation error: {e_hash}"
            ))
            return

        if canonical_input_hash:
            logger.info(f"[FetchHashTask Job {job_id}] Successfully computed input hash: {canonical_input_hash[:10]}... Updating DB.")
            update_query = """
                UPDATE weather_miner_jobs
                SET input_data_hash = :hash,
                    input_batch_pickle_path = :pickle_path,
                    status = :status,
                    updated_at = :now
                WHERE id = :job_id
            """
            await task_instance.db_manager.execute(update_query, {
                "job_id": job_id,
                "hash": canonical_input_hash,
                "pickle_path": input_batch_pickle_file_path_str, # Store the path
                "status": "input_hashed_awaiting_validation",
                "now": datetime.now(timezone.utc)
            })
            logger.info(f"[FetchHashTask Job {job_id}] Status updated to input_hashed_awaiting_validation with hash and batch path.")
        else:
            logger.error(f"[FetchHashTask Job {job_id}] compute_input_data_hash failed (returned None). Updating status to fetch_error.")
            await update_job_status(task_instance, job_id, "fetch_error", "Failed to compute input hash.")

    except Exception as e:
        logger.error(f"[FetchHashTask Job {job_id}] Unexpected error: {e}", exc_info=True)
        error_message = f"Unexpected error during fetch/hash: {e}"
        # Attempt to clean up partial pickle file if it exists and an error occurred
        if input_batch_pickle_file_path_str:
            try:
                partial_pickle_path = Path(input_batch_pickle_file_path_str)
                if partial_pickle_path.exists():
                    partial_pickle_path.unlink()
                    logger.info(f"[FetchHashTask Job {job_id}] Cleaned up partial pickle file: {input_batch_pickle_file_path_str}")
            except Exception as cleanup_err:
                logger.warning(f"[FetchHashTask Job {job_id}] Error cleaning up partial pickle file {input_batch_pickle_file_path_str}: {cleanup_err}")
        
        try:
            await update_job_status(task_instance, job_id, "error", error_message)
        except Exception as db_err:
            logger.error(f"[FetchHashTask Job {job_id}] Failed to update status to error after exception: {db_err}")
    finally:
        # Close datasets if they were opened
        if 'ds_t0' in locals() and ds_t0 and hasattr(ds_t0, 'close'):
            try: ds_t0.close()
            except Exception: pass
        if 'ds_t_minus_6' in locals() and ds_t_minus_6 and hasattr(ds_t_minus_6, 'close'):
            try: ds_t_minus_6.close()
            except Exception: pass
        if 'gfs_concat_data_for_batch_prep' in locals() and gfs_concat_data_for_batch_prep is not None and hasattr(gfs_concat_data_for_batch_prep, 'close'):
            try: gfs_concat_data_for_batch_prep.close()
            except Exception: pass
        if initial_batch is not None: # Check if initial_batch was assigned
            del initial_batch # Explicitly delete to free memory sooner
        gc.collect()


    logger.info(f"[FetchHashTask Job {job_id}] Task finished.")

async def r2_cleanup_worker(task_instance: 'WeatherTask'):
    """
    Periodically cleans up old input and forecast objects from the R2 bucket.
    Enhanced aggressive cleanup:
    - Immediately deletes inputs after inference completion
    - Groups outputs by GFS timestep and keeps only the latest one per timestep
    - Cleans up old testing data and junk files
    """
    logger.info("R2 cleanup worker started with enhanced aggressive cleanup.")
    
    while getattr(task_instance, 'r2_cleanup_worker_running', False):
        try:
            cleanup_interval = task_instance.config.get('r2_cleanup_interval_seconds', 1800)  # Reduced to 30 minutes for more frequent cleanup
            max_inputs = 0  # Changed: Keep NO inputs (delete immediately after use)
            max_outputs_per_timestep = 1  # Changed: Keep only 1 output per GFS timestep
            
            logger.info(f"[R2Cleanup] Running ENHANCED cleanup. Keeping {max_inputs} inputs and {max_outputs_per_timestep} output per timestep.")

            s3_client = await task_instance._get_r2_s3_client()
            if not s3_client:
                logger.error("[R2Cleanup] Could not get R2 client. Skipping cleanup cycle.")
                await asyncio.sleep(cleanup_interval)
                continue
            
            bucket_name = task_instance.r2_config.get("r2_bucket_name") if task_instance.r2_config else None
            if not bucket_name:
                logger.error("[R2Cleanup] R2 bucket name not configured. Skipping cleanup cycle.")
                await asyncio.sleep(cleanup_interval)
                continue

            # --- AGGRESSIVE INPUT CLEANUP: Delete ALL inputs immediately ---
            try:
                logger.info(f"[R2Cleanup] AGGRESSIVE: Cleaning up ALL 'inputs/' (keeping max {max_inputs})...")
                
                # Check if we have any completed jobs that no longer need their inputs
                completed_jobs_with_inputs = await task_instance.db_manager.fetch_all("""
                    SELECT DISTINCT id FROM weather_miner_jobs 
                    WHERE status IN ('completed', 'error', 'failed') 
                    AND updated_at < NOW() - INTERVAL '1 hour'  -- Only cleanup inputs for jobs older than 1 hour
                """)
                
                # Get all input objects
                paginator = s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=bucket_name, Prefix='inputs/')
                
                inputs_to_delete = []
                all_input_objects = []
                
                for page in pages:
                    if 'Contents' in page:
                        all_input_objects.extend(page['Contents'])
                
                # Group by job_id and check if job is completed
                inputs_by_job = {}
                for obj in all_input_objects:
                    key = obj['Key']
                    if key.count('/') > 1:
                        job_id = key.split('/')[1]
                        if job_id not in inputs_by_job:
                            inputs_by_job[job_id] = {'keys': [], 'last_modified': None}
                        inputs_by_job[job_id]['keys'].append(key)
                        if inputs_by_job[job_id]['last_modified'] is None or obj['LastModified'] > inputs_by_job[job_id]['last_modified']:
                            inputs_by_job[job_id]['last_modified'] = obj['LastModified']

                # Check each job's status and mark for deletion if completed
                for job_id, data in inputs_by_job.items():
                    try:
                        job_status = await task_instance.db_manager.fetch_one(
                            "SELECT status FROM weather_miner_jobs WHERE id = :job_id",
                            {"job_id": job_id}
                        )
                        
                        # Delete inputs for completed/failed jobs OR if max_inputs is 0 (always delete)
                        if max_inputs == 0 or (job_status and job_status['status'] in ['completed', 'error', 'failed']):
                            inputs_to_delete.extend(data['keys'])
                            status_str = job_status['status'] if job_status else 'unknown'
                            logger.info(f"[R2Cleanup] Marking job {job_id} inputs for deletion (status: {status_str})")
                        
                    except Exception as e_job_check:
                        logger.warning(f"[R2Cleanup] Could not check status for job {job_id}, will keep inputs: {e_job_check}")

                # Also delete inputs older than 7 days regardless of job status (cleanup testing junk)
                cutoff_time = datetime.now(timezone.utc) - timedelta(days=7)
                for job_id, data in inputs_by_job.items():
                    if data['last_modified'] < cutoff_time:
                        inputs_to_delete.extend(data['keys'])
                        logger.info(f"[R2Cleanup] Marking old job {job_id} inputs for deletion (older than 7 days)")

                if inputs_to_delete:
                    # Remove duplicates
                    inputs_to_delete = list(set(inputs_to_delete))
                    logger.info(f"[R2Cleanup] Deleting {len(inputs_to_delete)} input objects from {len(inputs_by_job)} total jobs.")
                    
                    # Delete in batches of 1000 (R2 limit)
                    for i in range(0, len(inputs_to_delete), 1000):
                        chunk = inputs_to_delete[i:i+1000]
                        delete_payload = {'Objects': [{'Key': key} for key in chunk]}
                        await asyncio.to_thread(s3_client.delete_objects, Bucket=bucket_name, Delete=delete_payload)
                    logger.info(f"[R2Cleanup] Successfully deleted {len(inputs_to_delete)} input objects.")
                else:
                    logger.info(f"[R2Cleanup] No input objects to delete (total jobs: {len(inputs_by_job)}).")

            except Exception as e_inputs:
                logger.error(f"[R2Cleanup] Error during enhanced input cleanup: {e_inputs}", exc_info=True)

            # --- ENHANCED OUTPUT CLEANUP: Group by GFS timestep and keep only latest per timestep ---
            try:
                logger.info(f"[R2Cleanup] ENHANCED: Cleaning up 'outputs/' grouped by GFS timestep (keeping {max_outputs_per_timestep} per timestep)...")
                
                # Get all output prefixes (job directories)
                paginator = s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=bucket_name, Prefix='outputs/', Delimiter='/')

                output_prefixes = []
                for page in pages:
                    if 'CommonPrefixes' in page:
                        for prefix_info in page['CommonPrefixes']:
                            output_prefixes.append(prefix_info['Prefix'])

                if not output_prefixes:
                    logger.info("[R2Cleanup] No output prefixes found.")
                else:
                    logger.info(f"[R2Cleanup] Found {len(output_prefixes)} output prefixes to analyze.")
                    
                    # Get job details for timestep grouping
                    prefix_to_timestep = {}
                    prefix_to_modified = {}
                    
                    for prefix in output_prefixes:
                        try:
                            # Extract job_id from prefix (outputs/job_id/)
                            job_id = prefix.rstrip('/').split('/')[-1]
                            
                            # Get GFS timestep for this job
                            job_info = await task_instance.db_manager.fetch_one(
                                "SELECT gfs_init_time_utc FROM weather_miner_jobs WHERE id = :job_id",
                                {"job_id": job_id}
                            )
                            
                            # Get most recent modification time for the prefix
                            resp = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
                            if 'Contents' in resp and resp['Contents']:
                                mod_time = resp['Contents'][0]['LastModified']
                            else:
                                mod_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
                            
                            if job_info and job_info['gfs_init_time_utc']:
                                timestep = job_info['gfs_init_time_utc']
                                prefix_to_timestep[prefix] = timestep
                                prefix_to_modified[prefix] = mod_time
                                logger.debug(f"[R2Cleanup] Mapped prefix {prefix} to timestep {timestep}")
                            else:
                                # If no job info found, treat as testing junk - delete if older than 3 days
                                cutoff_for_junk = datetime.now(timezone.utc) - timedelta(days=3)
                                if mod_time < cutoff_for_junk:
                                    logger.info(f"[R2Cleanup] Marking orphaned/testing prefix for deletion: {prefix} (no job info, older than 3 days)")
                                    prefix_to_timestep[prefix] = None  # Mark for deletion
                                    prefix_to_modified[prefix] = mod_time
                                
                        except Exception as e_prefix:
                            logger.warning(f"[R2Cleanup] Error processing prefix {prefix}: {e_prefix}")
                    
                    # Group prefixes by timestep
                    timestep_groups = {}
                    orphaned_prefixes = []
                    
                    for prefix, timestep in prefix_to_timestep.items():
                        if timestep is None:
                            orphaned_prefixes.append(prefix)
                        else:
                            if timestep not in timestep_groups:
                                timestep_groups[timestep] = []
                            timestep_groups[timestep].append(prefix)
                    
                    # For each timestep, keep only the most recent prefix(es)
                    prefixes_to_delete = []
                    
                    for timestep, prefixes in timestep_groups.items():
                        if len(prefixes) > max_outputs_per_timestep:
                            # Sort by modification time, keep most recent
                            sorted_prefixes = sorted(prefixes, key=lambda p: prefix_to_modified[p], reverse=True)
                            to_keep = sorted_prefixes[:max_outputs_per_timestep]
                            to_delete = sorted_prefixes[max_outputs_per_timestep:]
                            
                            prefixes_to_delete.extend(to_delete)
                            logger.info(f"[R2Cleanup] Timestep {timestep}: keeping {len(to_keep)} most recent, deleting {len(to_delete)} older outputs")
                    
                    # Add orphaned prefixes to deletion list
                    prefixes_to_delete.extend(orphaned_prefixes)
                    if orphaned_prefixes:
                        logger.info(f"[R2Cleanup] Deleting {len(orphaned_prefixes)} orphaned/testing prefixes")
                    
                    # Delete the marked prefixes
                    if prefixes_to_delete:
                        logger.info(f"[R2Cleanup] Deleting {len(prefixes_to_delete)} output prefixes total.")
                        
                        for prefix_to_delete in prefixes_to_delete:
                            logger.info(f"[R2Cleanup] Deleting all objects under prefix: {prefix_to_delete}")
                            
                            # List all objects under the prefix and delete them in batches
                            obj_paginator = s3_client.get_paginator('list_objects_v2')
                            obj_pages = obj_paginator.paginate(Bucket=bucket_name, Prefix=prefix_to_delete)
                            
                            keys_to_delete = []
                            for page in obj_pages:
                                if 'Contents' in page:
                                    keys_to_delete.extend([{'Key': obj['Key']} for obj in page['Contents']])
                            
                            if keys_to_delete:
                                for i in range(0, len(keys_to_delete), 1000):
                                    chunk = keys_to_delete[i:i+1000]
                                    delete_payload = {'Objects': chunk}
                                    await asyncio.to_thread(s3_client.delete_objects, Bucket=bucket_name, Delete=delete_payload)
                                logger.info(f"[R2Cleanup] Deleted {len(keys_to_delete)} objects for prefix {prefix_to_delete}")
                    else:
                        logger.info("[R2Cleanup] No output prefixes need deletion.")

            except Exception as e_outputs:
                logger.error(f"[R2Cleanup] Error during enhanced output cleanup: {e_outputs}", exc_info=True)

            # --- CLEANUP TESTING JUNK: Delete any files not following expected patterns ---
            try:
                logger.info("[R2Cleanup] Cleaning up testing junk and malformed objects...")
                
                # Look for objects not in inputs/ or outputs/ prefixes
                all_objects_paginator = s3_client.get_paginator('list_objects_v2')
                all_pages = all_objects_paginator.paginate(Bucket=bucket_name)
                
                junk_objects = []
                cutoff_for_junk = datetime.now(timezone.utc) - timedelta(days=1)  # Delete junk older than 1 day
                
                for page in all_pages:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            key = obj['Key']
                            # Skip expected patterns
                            if key.startswith('inputs/') or key.startswith('outputs/'):
                                continue
                            
                            # Mark as junk if it doesn't follow expected patterns and is old enough
                            if obj['LastModified'] < cutoff_for_junk:
                                junk_objects.append(key)
                                logger.debug(f"[R2Cleanup] Marking junk object for deletion: {key}")
                
                if junk_objects:
                    logger.info(f"[R2Cleanup] Deleting {len(junk_objects)} junk objects.")
                    for i in range(0, len(junk_objects), 1000):
                        chunk = junk_objects[i:i+1000]
                        delete_payload = {'Objects': [{'Key': key} for key in chunk]}
                        await asyncio.to_thread(s3_client.delete_objects, Bucket=bucket_name, Delete=delete_payload)
                    logger.info(f"[R2Cleanup] Successfully deleted {len(junk_objects)} junk objects.")
                else:
                    logger.info("[R2Cleanup] No junk objects found for cleanup.")
                    
            except Exception as e_junk:
                logger.error(f"[R2Cleanup] Error during junk cleanup: {e_junk}", exc_info=True)

            logger.info(f"[R2Cleanup] Enhanced cleanup cycle finished. Worker sleeping for {cleanup_interval} seconds.")
            await asyncio.sleep(cleanup_interval)

        except asyncio.CancelledError:
            logger.info("R2 cleanup worker has been cancelled.")
            break
        except Exception as e:
            logger.error(f"An unexpected error occurred in the enhanced R2 cleanup worker: {e}", exc_info=True)
            logger.info("R2 cleanup worker will sleep for 60 seconds before retrying.")
            await asyncio.sleep(60)

    logger.info("Enhanced R2 cleanup worker has stopped.")


def get_job_by_gfs_init_time(self, gfs_init_time: datetime) -> Optional[Dict]:
    """
    Checks the database for an existing, non-failed job for a specific GFS initialization time.
    """
    # Implementation of the method
    pass

async def poll_runpod_job_worker(task_instance: 'WeatherTask', job_id: str, runpod_job_id: str):
    """
    Background worker to poll a RunPod job until it completes or fails.
    """
    logger.info(f"[RunPodPoller Job {job_id}] Starting polling for RunPod Job ID: {runpod_job_id}")

    base_url = task_instance.inference_service_url.rsplit('/', 1)[0]
    status_url = f"{base_url}/status/{runpod_job_id}"
    headers = {"Authorization": f"Bearer {task_instance.runpod_api_key}"}

    poll_interval = task_instance.config.get('runpod_poll_interval_seconds', 10)
    max_attempts = task_instance.config.get('runpod_max_poll_attempts', 180) # Default 30 mins

    try:
        async with httpx.AsyncClient(headers=headers, timeout=60.0) as client:
            for attempt in range(max_attempts):
                await asyncio.sleep(poll_interval)
                
                # Only log polling status every 6th attempt (every ~60 seconds if poll_interval=10)
                if (attempt + 1) % 6 == 0 or attempt == 0:
                    logger.debug(f"[RunPodPoller Job {job_id}] Polling status (Attempt {attempt + 1}/{max_attempts})")

                try:
                    response = await client.get(status_url)
                    response.raise_for_status()
                    status_data = response.json()
                    current_status = status_data.get("status")

                    if current_status == "COMPLETED":
                        logger.info(f"[RunPodPoller Job {job_id}] RunPod job COMPLETED.")
                        manifest = status_data.get("output")
                        if not manifest or not isinstance(manifest, dict):
                            raise ValueError(f"RunPod job completed but 'output' is missing or invalid. Full response: {status_data}")

                        output_prefix = manifest.get("output_r2_object_key_prefix")
                        if not output_prefix:
                            raise ValueError("Completed job manifest missing 'output_r2_object_key_prefix'.")
                        
                        logger.info(f"[{job_id}] Inference successful. Output prefix: {output_prefix}.")
                        
                        # Check file serving mode to determine next steps
                        file_serving_mode = task_instance.config.get('file_serving_mode', 'local')
                        
                        if file_serving_mode == 'local':
                            # Download files from R2 to local storage for serving
                            logger.info(f"[{job_id}] File serving mode is 'local'. Downloading forecast files from R2...")
                            download_result = await _download_forecast_from_r2_to_local(task_instance, job_id, output_prefix)
                            
                            if download_result:
                                local_zarr_path, verification_hash = download_result
                                logger.info(f"[{job_id}] Successfully downloaded forecast to local storage: {local_zarr_path}")
                                await update_job_paths(
                                    task_instance, 
                                    job_id, 
                                    netcdf_path=local_zarr_path,
                                    kerchunk_path=local_zarr_path,  # Same as netcdf for zarr
                                    verification_hash=verification_hash
                                )
                            else:
                                error_msg = "Failed to download forecast from R2 to local storage"
                                logger.error(f"[{job_id}] {error_msg}")
                                await update_job_status(task_instance, job_id, 'failed', error_msg)
                                return
                        else:
                            # R2 proxy mode - store the R2 prefix for proxying
                            logger.info(f"[{job_id}] File serving mode is 'r2_proxy'. Storing R2 prefix for proxy serving.")
                            await update_job_paths(task_instance, job_id, netcdf_path=output_prefix)
                        
                        await update_job_status(task_instance, job_id, 'completed')
                        
                        # Immediately cleanup R2 inputs for completed job
                        asyncio.create_task(_immediate_r2_input_cleanup(task_instance, job_id))
                        
                        return # Success, exit worker

                    elif current_status in ["IN_QUEUE", "IN_PROGRESS"]:
                        # Only log status periodically to reduce verbosity  
                        if (attempt + 1) % 6 == 0 or attempt == 0:
                            logger.debug(f"[RunPodPoller Job {job_id}] RunPod status is '{current_status}'. Continuing (attempt {attempt + 1}/{max_attempts}).")
                        # No DB update needed, just continue polling

                    elif current_status in ["FAILED", "CANCELLED", "TIMED_OUT"]:
                        error_output = status_data.get("output", f"No error details provided, but status was {current_status}.")
                        error_msg = f"RunPod job terminated with status '{current_status}'. Details: {error_output}"
                        logger.error(f"[RunPodPoller Job {job_id}] {error_msg}")
                        await update_job_status(task_instance, job_id, 'failed', error_msg)
                        return  # Exit worker gracefully instead of raising

                    else: # Unhandled status
                        logger.warning(f"[RunPodPoller Job {job_id}] Unhandled RunPod status '{current_status}'. Treating as temporary. Response: {status_data}")

                except httpx.HTTPStatusError as e:
                    logger.error(f"[RunPodPoller Job {job_id}] HTTP error polling status: {e.response.status_code} - {e.response.text}")
                    # Continue polling on server errors (5xx), but fail gracefully on client errors (4xx)
                    if 500 <= e.response.status_code < 600:
                        logger.warning(f"[{job_id}] Server error, will retry.")
                    else:
                        error_msg = f"HTTP client error {e.response.status_code}: {e.response.text}"
                        logger.error(f"[RunPodPoller Job {job_id}] {error_msg}")
                        await update_job_status(task_instance, job_id, 'failed', error_msg)
                        return  # Exit worker gracefully instead of raising
                except httpx.RequestError as e:
                    logger.error(f"[RunPodPoller Job {job_id}] Request error polling status: {e}. Will retry.")
                except (json.JSONDecodeError, ValueError) as e:
                    error_msg = f"Error processing RunPod status response: {e}"
                    logger.error(f"[RunPodPoller Job {job_id}] {error_msg}")
                    await update_job_status(task_instance, job_id, 'failed', error_msg)
                    return  # Exit worker gracefully instead of raising

            # If loop finishes, it's a timeout
            timeout_msg = f"Polling timed out after {max_attempts} attempts."
            logger.error(f"[RunPodPoller Job {job_id}] {timeout_msg}")
            await update_job_status(task_instance, job_id, 'failed', timeout_msg)

    except Exception as e:
        error_message = f"RunPod poller failed: {e}"
        logger.error(f"[RunPodPoller Job {job_id}] {error_message}", exc_info=True)
        try:
            await update_job_status(task_instance, job_id, 'failed', error_message)
        except Exception as db_error:
            logger.error(f"[RunPodPoller Job {job_id}] Failed to update job status after error: {db_error}")
    
    logger.info(f"[RunPodPoller Job {job_id}] Polling worker exited gracefully.")

async def weather_job_status_logger(task_instance: 'WeatherTask'):
    """
    Periodic worker that logs a comprehensive status overview of all weather jobs.
    Shows running jobs, queued jobs, validator info, timesteps, and summary statistics.
    """
    logger.info("[JobStatusLogger] Weather job status logger started.")
    
    # Configurable interval (default: 5 minutes)
    status_log_interval = task_instance.config.get('job_status_log_interval_seconds', 300)
    
    while True:
        try:
            await asyncio.sleep(status_log_interval)
            
            if task_instance.node_type != 'miner':
                logger.debug("[JobStatusLogger] Skipping status log (not a miner node).")
                continue
                
            # Query all jobs from the last 24 hours
            query = """
                SELECT 
                    id,
                    status,
                    validator_hotkey,
                    gfs_init_time_utc,
                    gfs_t_minus_6_time_utc,
                    validator_request_time,
                    processing_start_time,
                    processing_end_time,
                    error_message,
                    updated_at
                FROM weather_miner_jobs 
                WHERE validator_request_time >= NOW() - INTERVAL '24 hours'
                ORDER BY validator_request_time DESC
            """
            
            jobs = await task_instance.db_manager.fetch_all(query, {})
            
            if not jobs:
                logger.info("[JobStatusLogger] 📊 No weather jobs found in the last 24 hours.")
                continue
            
            # Categorize jobs by status
            status_groups = {}
            validator_stats = {}
            total_jobs = len(jobs)
            
            for job in jobs:
                status = job['status']
                validator = job['validator_hotkey'] or 'Unknown'
                
                # Group by status
                if status not in status_groups:
                    status_groups[status] = []
                status_groups[status].append(job)
                
                # Count by validator
                if validator not in validator_stats:
                    validator_stats[validator] = {'total': 0, 'completed': 0, 'failed': 0, 'running': 0}
                validator_stats[validator]['total'] += 1
                
                if status == 'completed':
                    validator_stats[validator]['completed'] += 1
                elif status in ['error', 'failed', 'fetch_error', 'input_hash_mismatch']:
                    validator_stats[validator]['failed'] += 1
                elif status in ['processing', 'running_inference', 'processing_input', 'processing_output']:
                    validator_stats[validator]['running'] += 1
            
            # Build status summary
            logger.info("=" * 80)
            logger.info("🌦️  WEATHER JOBS STATUS OVERVIEW")
            logger.info("=" * 80)
            logger.info(f"📈 Total jobs (24h): {total_jobs}")
            
            # Status breakdown
            status_summary = []
            priority_statuses = ['running_inference', 'processing', 'processing_input', 'processing_output', 
                               'fetch_queued', 'fetching_gfs', 'input_hashed_awaiting_validation', 
                               'completed', 'error', 'failed', 'fetch_error']
            
            for status in priority_statuses:
                if status in status_groups:
                    count = len(status_groups[status])
                    status_summary.append(f"{status}: {count}")
            
            # Add any other statuses not in priority list
            for status, jobs_list in status_groups.items():
                if status not in priority_statuses:
                    count = len(jobs_list)
                    status_summary.append(f"{status}: {count}")
            
            logger.info(f"📊 Status breakdown: {' | '.join(status_summary)}")
            
            # Active jobs details
            active_statuses = ['running_inference', 'processing', 'processing_input', 'processing_output', 
                             'fetch_queued', 'fetching_gfs', 'input_hashed_awaiting_validation']
            active_jobs = []
            for status in active_statuses:
                if status in status_groups:
                    active_jobs.extend(status_groups[status])
            
            if active_jobs:
                logger.info(f"🔄 Active jobs ({len(active_jobs)}):")
                for job in active_jobs[:10]:  # Show up to 10 active jobs
                    job_id_short = job['id'][:8]
                    validator_short = (job['validator_hotkey'] or 'Unknown')[:8]
                    gfs_time = job['gfs_init_time_utc'].strftime('%m-%d %H:%M') if job['gfs_init_time_utc'] else 'N/A'
                    duration = ""
                    if job['processing_start_time']:
                        from datetime import datetime, timezone
                        start_time = job['processing_start_time']
                        current_time = datetime.now(timezone.utc)
                        duration_mins = int((current_time - start_time).total_seconds() / 60)
                        duration = f" ({duration_mins}m)"
                    
                    logger.info(f"   • {job_id_short} | {job['status']:<20} | Val: {validator_short} | GFS: {gfs_time}{duration}")
                
                if len(active_jobs) > 10:
                    logger.info(f"   ... and {len(active_jobs) - 10} more active jobs")
            
            # Recent completions
            completed_jobs = status_groups.get('completed', [])
            if completed_jobs:
                recent_completed = completed_jobs[:5]  # Last 5 completed
                logger.info(f"✅ Recent completions ({len(completed_jobs)} total):")
                for job in recent_completed:
                    job_id_short = job['id'][:8]
                    validator_short = (job['validator_hotkey'] or 'Unknown')[:8]
                    gfs_time = job['gfs_init_time_utc'].strftime('%m-%d %H:%M') if job['gfs_init_time_utc'] else 'N/A'
                    completed_time = job['processing_end_time'].strftime('%H:%M') if job['processing_end_time'] else 'N/A'
                    logger.info(f"   • {job_id_short} | Val: {validator_short} | GFS: {gfs_time} | Done: {completed_time}")
            
            # Recent failures
            failed_statuses = ['error', 'failed', 'fetch_error', 'input_hash_mismatch']
            failed_jobs = []
            for status in failed_statuses:
                if status in status_groups:
                    failed_jobs.extend(status_groups[status])
            
            if failed_jobs:
                recent_failed = sorted(failed_jobs, key=lambda x: x['updated_at'] or x['validator_request_time'], reverse=True)[:3]
                logger.info(f"❌ Recent failures ({len(failed_jobs)} total):")
                for job in recent_failed:
                    job_id_short = job['id'][:8]
                    validator_short = (job['validator_hotkey'] or 'Unknown')[:8]
                    gfs_time = job['gfs_init_time_utc'].strftime('%m-%d %H:%M') if job['gfs_init_time_utc'] else 'N/A'
                    error_preview = (job['error_message'] or 'No details')[:50] + ('...' if len(job['error_message'] or '') > 50 else '')
                    logger.info(f"   • {job_id_short} | {job['status']:<12} | Val: {validator_short} | GFS: {gfs_time}")
                    logger.info(f"     └─ {error_preview}")
            
            # Validator statistics
            if validator_stats:
                logger.info("🏗️  Validator performance:")
                # Sort by total jobs
                sorted_validators = sorted(validator_stats.items(), key=lambda x: x[1]['total'], reverse=True)
                for validator, stats in sorted_validators[:5]:  # Top 5 validators
                    validator_short = validator[:12] if validator != 'Unknown' else validator
                    success_rate = (stats['completed'] / stats['total'] * 100) if stats['total'] > 0 else 0
                    logger.info(f"   • {validator_short:<12} | Total: {stats['total']:>2} | ✅ {stats['completed']:>2} | ❌ {stats['failed']:>2} | 🔄 {stats['running']:>2} | Success: {success_rate:.0f}%")
            
            logger.info("=" * 80)
            
        except asyncio.CancelledError:
            logger.info("[JobStatusLogger] Weather job status logger has been cancelled.")
            break
        except Exception as e:
            logger.error(f"[JobStatusLogger] Error in weather job status logger: {e}", exc_info=True)
            logger.info("[JobStatusLogger] Status logger will sleep for 60 seconds before retrying.")
            await asyncio.sleep(60)
    
    logger.info("[JobStatusLogger] Weather job status logger has stopped.")

async def _download_forecast_from_r2_to_local(task_instance: 'WeatherTask', job_id: str, r2_output_prefix: str) -> Optional[Tuple[str, str]]:
    """
    Downloads forecast NetCDF files from R2 and combines them into a local zarr store.
    
    Args:
        task_instance: WeatherTask instance
        job_id: Job identifier
        r2_output_prefix: R2 prefix where forecast files are stored
        
    Returns:
        Tuple of (zarr_path, verification_hash) if successful, None if failed
    """
    try:
        # Get R2 client
        s3_client = await task_instance._get_r2_s3_client()
        if not s3_client:
            logger.error(f"[{job_id}] Cannot download from R2: S3 client not available")
            return None
            
        bucket_name = task_instance.r2_config.get("r2_bucket_name")
        if not bucket_name:
            logger.error(f"[{job_id}] Cannot download from R2: bucket name not configured")
            return None
        
        logger.info(f"[{job_id}] Listing files in R2 prefix: s3://{bucket_name}/{r2_output_prefix}")
        
        # List all NetCDF files in the R2 prefix
        try:
            response = await asyncio.to_thread(
                s3_client.list_objects_v2,
                Bucket=bucket_name,
                Prefix=r2_output_prefix
            )
            
            if 'Contents' not in response:
                logger.error(f"[{job_id}] No files found in R2 prefix: {r2_output_prefix}")
                return None
                
            nc_files = []
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.nc'):
                    nc_files.append(key)
                    
            if not nc_files:
                logger.error(f"[{job_id}] No .nc files found in R2 prefix: {r2_output_prefix}")
                return None
                
            logger.info(f"[{job_id}] Found {len(nc_files)} NetCDF files to download")
            
        except Exception as e_list:
            logger.error(f"[{job_id}] Error listing R2 objects: {e_list}", exc_info=True)
            return None
        
        # Create temporary directory for downloads
        import tempfile
        with tempfile.TemporaryDirectory(prefix=f"forecast_download_{job_id[:8]}_") as temp_dir:
            temp_path = Path(temp_dir)
            downloaded_files = []
            
            # Download all NetCDF files
            logger.info(f"[{job_id}] Downloading {len(nc_files)} files from R2...")
            download_tasks = []
            
            for nc_key in nc_files:
                local_file_path = temp_path / Path(nc_key).name
                download_tasks.append(_download_single_file_from_r2(s3_client, bucket_name, nc_key, local_file_path))
            
            # Execute downloads in parallel with some concurrency control
            semaphore = asyncio.Semaphore(10)  # Limit concurrent downloads
            
            completed_downloads = 0
            total_downloads = len(download_tasks)
            
            async def download_with_semaphore(task, file_index):
                nonlocal completed_downloads
                async with semaphore:
                    result = await task
                    completed_downloads += 1
                    
                    # Log progress every 5 files or on special milestones
                    if completed_downloads % 5 == 0 or completed_downloads in [1, total_downloads]:
                        percent_done = (completed_downloads / total_downloads) * 100
                        logger.info(f"[{job_id}] Download progress: {completed_downloads}/{total_downloads} files ({percent_done:.1f}%) - Latest: {Path(nc_files[file_index]).name}")
                    
                    return result
            
            # Create tasks with file indices for progress reporting
            semaphore_tasks = [download_with_semaphore(task, i) for i, task in enumerate(download_tasks)]
            results = await asyncio.gather(*semaphore_tasks, return_exceptions=True)
            
            # Check results and collect successful downloads
            successful_count = 0
            failed_files = []
            for i, result in enumerate(results):
                if result is True:
                    downloaded_files.append(temp_path / Path(nc_files[i]).name)
                    successful_count += 1
                else:
                    failed_files.append(nc_files[i])
                    logger.error(f"[{job_id}] ❌ DOWNLOAD FAILED: {nc_files[i]} - Error: {result}")
            
            if not downloaded_files:
                logger.error(f"[{job_id}] No files successfully downloaded from R2")
                return None
            
            # Report download summary and fail if incomplete
            if failed_files:
                logger.error(f"[{job_id}] ❌ DOWNLOAD INCOMPLETE: {successful_count}/{total_downloads} files successful, {len(failed_files)} FAILED")
                logger.error(f"[{job_id}] Failed files: {[Path(f).name for f in failed_files]}")
                logger.error(f"[{job_id}] Cannot create valid forecast with missing timesteps. Aborting zarr creation.")
                return None  # Fail instead of proceeding with incomplete data
            else:
                logger.info(f"[{job_id}] ✅ Download complete! Successfully downloaded {successful_count}/{total_downloads} files.")
                
            logger.info(f"[{job_id}] Proceeding with zarr store creation using complete dataset ({successful_count} files)...")
            
            # Load and combine NetCDF files into a single zarr store
            try:
                import xarray as xr
                import pandas as pd
                from pathlib import Path as PathlibPath
                import gc  # For explicit garbage collection
                
                # Create local zarr path first
                gfs_init_time = None
                try:
                    # Get GFS init time from job database
                    job_details = await task_instance.db_manager.fetch_one(
                        "SELECT gfs_init_time_utc FROM weather_miner_jobs WHERE id = :job_id",
                        {"job_id": job_id}
                    )
                    if job_details:
                        gfs_init_time = job_details['gfs_init_time_utc']
                except Exception as e_job_details:
                    logger.warning(f"[{job_id}] Could not get GFS init time from DB: {e_job_details}")
                
                # Generate zarr store path
                if gfs_init_time:
                    gfs_time_str = gfs_init_time.strftime('%Y%m%d%H')
                else:
                    gfs_time_str = "unknown"
                
                miner_hotkey_for_filename = "unknown_miner_hk"
                if task_instance.keypair and task_instance.keypair.ss58_address:
                    miner_hotkey_for_filename = task_instance.keypair.ss58_address
                
                unique_suffix = job_id.split('-')[0]
                dirname_zarr = f"weather_forecast_{gfs_time_str}_miner_hk_{miner_hotkey_for_filename[:10]}_{unique_suffix}.zarr"
                
                # Use the same forecast directory as local inference
                from gaia.tasks.defined_tasks.weather.weather_task import MINER_FORECAST_DIR_BG
                MINER_FORECAST_DIR_BG.mkdir(parents=True, exist_ok=True)
                output_zarr_path = MINER_FORECAST_DIR_BG / dirname_zarr
                
                # Remove existing zarr store if it exists
                if output_zarr_path.exists():
                    logger.info(f"[{job_id}] 🗑️  Removing existing zarr store...")
                    import shutil
                    shutil.rmtree(output_zarr_path)
                
                # MEMORY-OPTIMIZED APPROACH: Process files in smaller batches and create zarr incrementally
                logger.info(f"[{job_id}] 📂 Processing {len(downloaded_files)} NetCDF files in memory-efficient batches...")
                
                # Sort files to ensure proper time ordering
                sorted_files = sorted(downloaded_files)
                
                # First, open one file to get the structure and create zarr template
                logger.info(f"[{job_id}] 🔍 Analyzing first file to determine zarr structure...")
                first_ds = xr.open_dataset(sorted_files[0])
                
                # Configure chunking for zarr
                logger.info(f"[{job_id}] ⚙️  Configuring zarr encoding for {len(first_ds.data_vars)} variables...")
                import numcodecs
                encoding = {}
                for var_name, da in first_ds.data_vars.items():
                    chunks_for_var = {}
                    
                    time_dim_in_var = next((d for d in da.dims if d.lower() == 'time'), None)
                    lat_dim_in_var = next((d for d in da.dims if d.lower() in ('lat', 'latitude')), None)
                    lon_dim_in_var = next((d for d in da.dims if d.lower() in ('lon', 'longitude')), None)
                    level_dim_in_var = next((d for d in da.dims if d.lower() in ('pressure_level', 'level', 'plev', 'isobaricinhpa')), None)

                    if time_dim_in_var:
                        chunks_for_var[time_dim_in_var] = 1
                    if level_dim_in_var:
                        chunks_for_var[level_dim_in_var] = 1 
                    if lat_dim_in_var:
                        chunks_for_var[lat_dim_in_var] = first_ds.sizes[lat_dim_in_var]
                    if lon_dim_in_var:
                        chunks_for_var[lon_dim_in_var] = first_ds.sizes[lon_dim_in_var]
                    
                    ordered_chunks_list = []
                    for dim_name_in_da in da.dims:
                        ordered_chunks_list.append(chunks_for_var.get(dim_name_in_da, first_ds.sizes[dim_name_in_da]))
                    
                    encoding[var_name] = {
                        'chunks': tuple(ordered_chunks_list),
                        'compressor': numcodecs.Blosc(cname='zstd', clevel=3, shuffle=numcodecs.Blosc.BITSHUFFLE)
                    }
                
                # Add explicit time encoding to ensure consistency between local and HTTP service paths
                for coord_name in first_ds.coords:
                    if coord_name.lower() == 'time' and pd.api.types.is_datetime64_any_dtype(first_ds.coords[coord_name].dtype):
                        # Extract base time from the first dataset's time coordinate
                        # Handle both scalar and array time coordinates
                        time_values = first_ds.time.values
                        if time_values.ndim == 0:  # Scalar time coordinate
                            first_time = pd.to_datetime(time_values.item())
                        else:  # Array time coordinate
                            first_time = pd.to_datetime(time_values[0])
                        encoding['time'] = {
                            'units': f'hours since {first_time.strftime("%Y-%m-%d %H:%M:%S")}',
                            'calendar': 'standard',
                            'dtype': 'float64'
                        }
                        logger.info(f"[{job_id}] Added explicit time encoding for HTTP service consistency: {encoding['time']}")
                        break
                
                # Process files in smaller batches to avoid memory exhaustion  
                batch_size = 4  # Reduce from 8 to 4 files at a time for lower memory usage
                all_datasets = []
                
                logger.info(f"[{job_id}] 🔄 Processing files in batches of {batch_size}...")
                for batch_start in range(0, len(sorted_files), batch_size):
                    batch_end = min(batch_start + batch_size, len(sorted_files))
                    batch_files = sorted_files[batch_start:batch_end]
                    
                    logger.info(f"[{job_id}] Loading batch {batch_start//batch_size + 1}/{(len(sorted_files) + batch_size - 1)//batch_size}: files {batch_start+1}-{batch_end}")
                    
                    # Load this batch of files with memory monitoring
                    batch_datasets = []
                    for file_path in batch_files:
                        try:
                            # EMERGENCY CIRCUIT BREAKER: Check memory before loading each file
                            try:
                                import psutil
                                process = psutil.Process()
                                current_memory_mb = process.memory_info().rss / (1024 * 1024)
                                
                                # Emergency memory threshold: 12GB (75% of 16GB system)
                                if current_memory_mb > 12000:
                                    logger.error(f"[{job_id}] 🚨 EMERGENCY: Memory usage too high ({current_memory_mb:.1f} MB). Aborting to prevent OOM.")
                                    # Force cleanup
                                    for ds in batch_datasets:
                                        try:
                                            ds.close()
                                        except:
                                            pass
                                    del batch_datasets
                                    gc.collect()
                                    return None
                            except Exception:
                                pass
                            
                            ds = xr.open_dataset(file_path, chunks={'time': 1})  # Use dask chunks
                            batch_datasets.append(ds)
                        except Exception as e_open:
                            logger.error(f"[{job_id}] Failed to open {file_path}: {e_open}")
                    
                    if batch_datasets:
                        # Concatenate this batch
                        if len(batch_datasets) > 1:
                            batch_combined = xr.concat(batch_datasets, dim='time')
                        else:
                            batch_combined = batch_datasets[0]
                        
                        all_datasets.append(batch_combined)
                        
                        # Close individual datasets to free memory
                        for ds in batch_datasets:
                            if ds != batch_combined:  # Don't close the combined one
                                ds.close()
                        del batch_datasets
                        
                        # Force garbage collection after each batch
                        gc.collect()
                        
                        percent_complete = (batch_end / len(sorted_files)) * 100
                        logger.info(f"[{job_id}] Batch processing: {batch_end}/{len(sorted_files)} files ({percent_complete:.1f}%)")
                
                if not all_datasets:
                    logger.error(f"[{job_id}] No datasets could be loaded from downloaded files")
                    return None
                
                # Final concatenation of all batches
                logger.info(f"[{job_id}] 🔗 Final concatenation of {len(all_datasets)} batches...")
                
                # Check memory before concatenation
                try:
                    import psutil
                    process = psutil.Process()
                    memory_before_concat_mb = process.memory_info().rss / (1024 * 1024)
                    logger.info(f"[{job_id}] Memory before final concatenation: {memory_before_concat_mb:.1f} MB")
                except Exception:
                    pass
                
                combined_ds = xr.concat(all_datasets, dim='time')
                
                # Close batch datasets to free memory IMMEDIATELY
                for ds in all_datasets:
                    ds.close()
                del all_datasets
                
                # Force multiple GC passes to clean up after large concatenation
                for gc_pass in range(3):
                    collected = gc.collect()
                    if collected == 0:
                        break
                    logger.debug(f"[{job_id}] Post-concat GC pass {gc_pass + 1}: collected {collected} objects")
                
                # Check memory after concatenation
                try:
                    memory_after_concat_mb = process.memory_info().rss / (1024 * 1024)
                    memory_used_mb = memory_after_concat_mb - memory_before_concat_mb
                    logger.info(f"[{job_id}] Memory after concatenation: {memory_after_concat_mb:.1f} MB (used {memory_used_mb:+.1f} MB)")
                    
                    # Memory pressure warning
                    if memory_after_concat_mb > 10000:  # 10GB warning threshold
                        logger.warning(f"[{job_id}] ⚠️  HIGH MEMORY USAGE: {memory_after_concat_mb:.1f} MB - Risk of OOM")
                except Exception:
                    pass
                
                logger.info(f"[{job_id}] 📊 Sorting combined dataset by time...")
                combined_ds = combined_ds.sortby('time')
                
                # Save to zarr with memory-efficient approach
                logger.info(f"[{job_id}] 💾 Writing combined dataset to zarr store: {output_zarr_path.name}")
                logger.info(f"[{job_id}] 📈 Dataset shape: {dict(combined_ds.sizes)} | Variables: {list(combined_ds.data_vars.keys())}")
                
                # MEMORY-SAFE: Write to zarr in chunks using dask delayed operations
                try:
                    # Check memory before zarr write
                    try:
                        memory_before_write_mb = process.memory_info().rss / (1024 * 1024)
                        logger.info(f"[{job_id}] Memory before zarr write: {memory_before_write_mb:.1f} MB")
                    except Exception:
                        pass
                    
                    # Configure chunking to minimize memory usage during write
                    rechunked_ds = combined_ds.chunk({
                        'time': 1,  # One time step at a time
                        'lat': -1,  # Full latitude
                        'lon': -1   # Full longitude  
                    })
                    
                    # Use compute=True for immediate write (avoids keeping large dask graph in memory)
                    rechunked_ds.to_zarr(
                        output_zarr_path,
                        encoding=encoding,
                        consolidated=True,
                        compute=True
                    )
                    
                    # Check memory after zarr write
                    try:
                        memory_after_write_mb = process.memory_info().rss / (1024 * 1024)
                        memory_used_write_mb = memory_after_write_mb - memory_before_write_mb
                        logger.info(f"[{job_id}] Memory after zarr write: {memory_after_write_mb:.1f} MB (used {memory_used_write_mb:+.1f} MB)")
                    except Exception:
                        pass
                    
                except Exception as e_zarr_write:
                    logger.error(f"[{job_id}] Failed to write zarr store: {e_zarr_write}", exc_info=True)
                    raise
                finally:
                    # Aggressive cleanup of zarr write objects
                    try:
                        if 'rechunked_ds' in locals():
                            rechunked_ds.close()
                            del rechunked_ds
                    except Exception:
                        pass
                
                # Close and cleanup main dataset
                combined_ds.close()
                del combined_ds
                
                # Force memory cleanup after zarr write
                for gc_pass in range(3):
                    collected = gc.collect()
                    if collected == 0:
                        break
                    logger.debug(f"[{job_id}] Post-zarr-write GC pass {gc_pass + 1}: collected {collected} objects")
                
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
                
            except Exception as e_combine:
                logger.error(f"[{job_id}] Error combining NetCDF files into zarr: {e_combine}", exc_info=True)
                # Cleanup any partial zarr store
                try:
                    if 'output_zarr_path' in locals() and output_zarr_path.exists():
                        import shutil
                        shutil.rmtree(output_zarr_path)
                        logger.info(f"[{job_id}] Cleaned up partial zarr store after error")
                except:
                    pass
                return None
    
    except Exception as e:
        logger.error(f"[{job_id}] Error downloading forecast from R2: {e}", exc_info=True)
        return None


async def _download_single_file_from_r2(s3_client, bucket_name: str, object_key: str, local_path: Path) -> bool:
    """Helper function to download a single file from R2 with retry logic."""
    max_retries = 3
    base_delay = 2.0  # seconds
    
    for attempt in range(max_retries):
        try:
            await asyncio.to_thread(
                s3_client.download_file,
                bucket_name,
                object_key,
                str(local_path)
            )
            
            # Verify file was actually downloaded and has content
            if not local_path.exists():
                error_msg = f"File not found after download: {local_path}"
                if attempt < max_retries - 1:
                    logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {object_key}: {error_msg}")
                    await asyncio.sleep(base_delay * (attempt + 1))
                    continue
                return error_msg
            
            file_size = local_path.stat().st_size
            if file_size == 0:
                error_msg = f"Downloaded file is empty (0 bytes): {object_key}"
                if attempt < max_retries - 1:
                    logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {object_key}: {error_msg}")
                    # Remove empty file before retry
                    try:
                        local_path.unlink()
                    except:
                        pass
                    await asyncio.sleep(base_delay * (attempt + 1))
                    continue
                return error_msg
                
            return True
            
        except Exception as e:
            error_msg = f"Download exception: {type(e).__name__}: {str(e)}"
            if attempt < max_retries - 1:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {object_key}: {error_msg}")
                # Clean up partial file before retry
                try:
                    if local_path.exists():
                        local_path.unlink()
                except:
                    pass
                await asyncio.sleep(base_delay * (attempt + 1))
                continue
            return error_msg
    
    return f"All {max_retries} download attempts failed for {object_key}"

async def _immediate_r2_input_cleanup(task_instance: 'WeatherTask', job_id: str):
    """
    Immediately cleans up input files from R2 after a job completes inference.
    This is called directly from the job completion logic to avoid waiting for periodic cleanup.
    """
    try:
        s3_client = await task_instance._get_r2_s3_client()
        if not s3_client:
            logger.warning(f"[{job_id}] Cannot cleanup R2 inputs: S3 client not available")
            return

        bucket_name = task_instance.r2_config.get("r2_bucket_name") if task_instance.r2_config else None
        if not bucket_name:
            logger.warning(f"[{job_id}] Cannot cleanup R2 inputs: bucket name not configured")
            return

        input_prefix = f"inputs/{job_id}/"
        logger.info(f"[{job_id}] Immediately cleaning up R2 inputs at prefix: {input_prefix}")

        # List all objects under the job's input prefix
        response = await asyncio.to_thread(
            s3_client.list_objects_v2,
            Bucket=bucket_name,
            Prefix=input_prefix
        )

        if 'Contents' not in response:
            logger.info(f"[{job_id}] No input objects found to cleanup")
            return

        # Delete all objects for this job
        keys_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        
        if keys_to_delete:
            delete_payload = {'Objects': keys_to_delete}
            await asyncio.to_thread(s3_client.delete_objects, Bucket=bucket_name, Delete=delete_payload)
            logger.info(f"[{job_id}] Successfully deleted {len(keys_to_delete)} input objects from R2")
        else:
            logger.info(f"[{job_id}] No input objects to delete")

    except Exception as e:
        logger.error(f"[{job_id}] Error during immediate R2 input cleanup: {e}", exc_info=True)
