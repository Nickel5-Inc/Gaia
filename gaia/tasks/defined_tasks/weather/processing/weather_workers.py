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
from typing import Dict, Optional, List
import base64
import pickle
import httpx
import gzip
import httpx
import gzip

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
from ..weather_scoring_mechanism import evaluate_miner_forecast_day1
from ..weather_scoring_mechanism import evaluate_miner_forecast_day1

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
    """
    logger.info(f"[InferenceTask Job {job_id}] Background inference task initiated.")

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

            logger.info(f"[InferenceTask Job {job_id}] Fetching GFS data for local model (T0={gfs_init_time_utc}, T-6={gfs_t_minus_6_time_utc})...")
            ds_t0 = await fetch_gfs_analysis_data([gfs_init_time_utc], cache_dir=local_gfs_cache_dir)
            ds_t_minus_6 = await fetch_gfs_analysis_data([gfs_t_minus_6_time_utc], cache_dir=local_gfs_cache_dir)
            if ds_t0 is None or ds_t_minus_6 is None:
                err_msg = "Failed to fetch/load GFS data from cache for local_model."
                logger.error(f"[InferenceTask Job {job_id}] {err_msg}")
                await update_job_status(task_instance, job_id, "error", err_msg)
                return
            
            logger.info(f"[InferenceTask Job {job_id}] Preparing Aurora batch from GFS data for local model...")
            gfs_concat_data_for_batch_prep = xr.concat([ds_t0, ds_t_minus_6], dim='time').sortby('time')
            prepared_batch = await asyncio.to_thread( # Assign to prepared_batch
                create_aurora_batch_from_gfs, 
                gfs_data=gfs_concat_data_for_batch_prep
            )
            if prepared_batch is None:
                err_msg = "Failed to create Aurora batch for local model from GFS data."
                logger.error(f"[InferenceTask Job {job_id}] {err_msg}")
                await update_job_status(task_instance, job_id, "error", err_msg)
                return
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
        
        async with task_instance.gpu_semaphore:
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
            # GFS data cleanup happens in finally block
            return

        logger.info(f"[InferenceTask Job {job_id}] Inference successful. Processing {len(output_steps_datasets)} steps for saving...")
        await update_job_status(task_instance, job_id, "processing_output")

        try:
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

                    combined_forecast_ds = xr.concat(forecast_datasets, dim='time')
                    combined_forecast_ds = combined_forecast_ds.assign_coords(lead_time=('time', lead_times_hours_list))

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
                    from ..utils.hashing import generate_manifest_and_signature
                    
                    # Get miner keypair for signing
                    miner_keypair = task_instance.keypair if task_instance.keypair else None
                    
                    if miner_keypair:
                        logger.info(f"[InferenceTask Job {job_id}] Generating manifest and signature for Zarr store...")
                        manifest_result = generate_manifest_and_signature(
                            zarr_store_path=Path(output_zarr_path),
                            miner_hotkey_keypair=miner_keypair,
                            include_zarr_metadata_in_manifest=True,
                            chunk_hash_algo_name="xxh64"
                        )
                        
                        if manifest_result:
                            _manifest_dict, _signature_bytes, manifest_content_sha256_hash = manifest_result
                            verification_hash = manifest_content_sha256_hash
                            logger.info(f"[InferenceTask Job {job_id}] Generated verification hash: {verification_hash[:10]}...")
                        else:
                            logger.warning(f"[InferenceTask Job {job_id}] Failed to generate manifest and signature.")
                    else:
                        logger.warning(f"[InferenceTask Job {job_id}] No miner keypair available for manifest signing.")
                        
                except Exception as e_manifest:
                    logger.error(f"[InferenceTask Job {job_id}] Error generating manifest: {e_manifest}", exc_info=True)
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
            logger.info(f"[InferenceTask Job {job_id}] Background inference task completed successfully.")

        except Exception as save_err:
            logger.error(f"[InferenceTask Job {job_id}] Failed to save or process output: {save_err}", exc_info=True)
            await update_job_status(task_instance, job_id, "error", error_message=f"Output processing error: {save_err}")

    except Exception as e:
        logger.error(f"[InferenceTask Job {job_id}] Background inference task failed unexpectedly: {e}", exc_info=True)
        try:
             await update_job_status(task_instance, job_id, "error", error_message=f"Unexpected task error: {e}")
        except Exception as final_db_err:
             logger.error(f"[InferenceTask Job {job_id}] Failed to update job status to error after task failure: {final_db_err}")
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
                            gfs_init_time
                        )
                    )

                    if len(responses) > 10 and len(scoring_tasks) % 10 == 0:
                        await asyncio.sleep(0) 

                    if len(responses) > 10 and len(scoring_tasks) % 10 == 0:
                        await asyncio.sleep(0) 
                
                logger.info(f"[Day1ScoringWorker] Run {run_id}: Created {len(scoring_tasks)} scoring tasks.")
                evaluation_results = await asyncio.gather(*scoring_tasks, return_exceptions=True)
                
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
                logger.info(f"[Day1ScoringWorker] Run {run_id}: Marked as day1_scoring_complete.")
                
                task_instance.initial_scoring_queue.task_done()

                if task_instance.test_mode and run_id == task_instance.last_test_mode_run_id:
                    logger.info(f"[Day1ScoringWorker] TEST MODE: Signaling scoring completion for run {run_id}.")
                    task_instance.test_mode_run_scored_event.set()
                
                gc.collect()
                
            except Exception as e:
                logger.error(f"[Day1ScoringWorker] Unexpected error processing run {run_id}: {e}", exc_info=True)
                if run_id:
                    try:
                        await _update_run_status(task_instance, run_id, "day1_scoring_failed", error_message=f"Day1 Worker error: {e}")
                    except Exception as db_err:
                        logger.error(f"[Day1ScoringWorker] Failed to update DB status on error: {db_err}")
                if task_instance.initial_scoring_queue._unfinished_tasks > 0:
                    task_instance.initial_scoring_queue.task_done()
            finally:
                if gfs_analysis_ds_for_run and hasattr(gfs_analysis_ds_for_run, 'close'):
                    try: gfs_analysis_ds_for_run.close()
                    except Exception: pass
                if gfs_reference_ds_for_run and hasattr(gfs_reference_ds_for_run, 'close'):
                    try: gfs_reference_ds_for_run.close()
                    except Exception: pass
                    
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
                        try:
                            logger.info(f"[FinalizeWorker] Run {run_id}: Triggering update of combined weather scores.")
                            await self.update_combined_weather_scores(run_id_trigger=run_id)
                        except Exception as e_comb_score:
                            logger.error(f"[FinalizeWorker] Run {run_id}: Error triggering combined score update: {e_comb_score}", exc_info=True)
                    else:
                         logger.warning(f"[FinalizeWorker] Run {run_id}: No miners successfully scored against ERA5. Skipping combined score update.")
                    processed_run_ids.add(run_id)
                    
                    if era5_ds_for_run:
                        try: era5_ds_for_run.close()
                        except Exception: pass

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

    try:
        await update_job_status(task_instance, job_id, "fetching_gfs")

        cache_dir_str = task_instance.config.get('gfs_analysis_cache_dir', './gfs_analysis_cache')
        gfs_cache_dir = Path(cache_dir_str)
        gfs_cache_dir.mkdir(parents=True, exist_ok=True)

        # --- Fetch GFS Data ---
        logger.info(f"[FetchHashTask Job {job_id}] Fetching GFS T0 data ({t0_run_time})...")
        ds_t0 = await fetch_gfs_analysis_data([t0_run_time], cache_dir=gfs_cache_dir)
        logger.info(f"[FetchHashTask Job {job_id}] Fetching GFS T-6 data ({t_minus_6_run_time})...")
        ds_t_minus_6 = await fetch_gfs_analysis_data([t_minus_6_run_time], cache_dir=gfs_cache_dir)

        if ds_t0 is None or ds_t_minus_6 is None:
            logger.error(f"[FetchHashTask Job {job_id}] Failed to fetch GFS data (T0 or T-6 is None).")
            await update_job_status(task_instance, job_id, "fetch_error", "Failed to fetch GFS data for batch preparation.")
            return

        # --- Prepare Aurora Batch ---
        logger.info(f"[FetchHashTask Job {job_id}] Preparing Aurora Batch from GFS data...")
        gfs_concat_data_for_batch_prep = await asyncio.to_thread(
            lambda d0, d_minus_6: xr.concat([d_minus_6, d0], dim='time').sortby('time'),
            ds_t0,
            ds_t_minus_6
        )
        initial_batch = await asyncio.to_thread(
            create_aurora_batch_from_gfs,
            gfs_data=gfs_concat_data_for_batch_prep
        )
        if initial_batch is None:
            logger.error(f"[FetchHashTask Job {job_id}] Failed to create Aurora Batch (create_aurora_batch_from_gfs returned None).")
            await update_job_status(task_instance, job_id, "error", "Failed to prepare initial Aurora batch.")
            return
        logger.info(f"[FetchHashTask Job {job_id}] Aurora Batch prepared successfully.")

        # --- Save Pickled Aurora Batch ---
        MINER_INPUT_BATCHES_DIR.mkdir(parents=True, exist_ok=True) # Ensure dir exists
        input_batch_pickle_filename = f"input_batch_{job_id}.pkl"
        input_batch_pickle_file_path = MINER_INPUT_BATCHES_DIR / input_batch_pickle_filename
        input_batch_pickle_file_path_str = str(input_batch_pickle_file_path.resolve())

        logger.info(f"[FetchHashTask Job {job_id}] Saving pickled Aurora Batch to {input_batch_pickle_file_path_str}...")
        def _save_pickle_sync(batch, path):
            with open(path, "wb") as f:
                pickle.dump(batch, f)
        await asyncio.to_thread(_save_pickle_sync, initial_batch, input_batch_pickle_file_path_str)
        logger.info(f"[FetchHashTask Job {job_id}] Successfully saved pickled Aurora Batch.")
        
        # --- Compute Input Hash (using the original datasets, not the batch object) ---
        logger.info(f"[FetchHashTask Job {job_id}] Calling compute_input_data_hash...")
        computed_hash = await compute_input_data_hash(
            t0_run_time=t0_run_time,
            t_minus_6_run_time=t_minus_6_run_time,
            cache_dir=gfs_cache_dir,
            ds_t0_override=ds_t0, # Pass fetched datasets to avoid re-fetch/re-load
            ds_t_minus_6_override=ds_t_minus_6
        )

        if computed_hash:
            logger.info(f"[FetchHashTask Job {job_id}] Successfully computed input hash: {computed_hash[:10]}... Updating DB.")
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
                "hash": computed_hash,
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
        del initial_batch # Explicitly delete to free memory sooner
        gc.collect()


    logger.info(f"[FetchHashTask Job {job_id}] Task finished.")
