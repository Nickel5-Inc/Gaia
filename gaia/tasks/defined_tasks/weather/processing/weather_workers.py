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
from kerchunk.hdf import SingleHdf5ToZarr
import pandas as pd
import shutil
import time

from fiber.logging_utils import get_logger
from aurora import Batch

from ..utils.data_prep import create_aurora_batch_from_gfs

from .weather_logic import (
    _request_fresh_token,
    _update_run_status,
    update_job_status,
    update_job_paths,
    build_score_row,
    get_ground_truth_data
)

from ..utils.gfs_api import fetch_gfs_analysis_data
from ..utils.era5_api import fetch_era5_data
from ..utils.hashing import compute_verification_hash
from ..weather_scoring.ensemble import (
    create_physics_aware_ensemble,
    _open_dataset_lazily,
    ALL_EXPECTED_VARIABLES,
    AURORA_FUNDAMENTAL_SURFACE_VARIABLES,
    AURORA_FUNDAMENTAL_ATMOS_VARIABLES,
    AURORA_DERIVED_VARIABLES
)
from ..weather_scoring.metrics import calculate_rmse
from ..weather_scoring_mechanism import calculate_gfs_score_and_weight, calculate_era5_miner_score
logger = get_logger(__name__)

VALIDATOR_ENSEMBLE_DIR = Path("./validator_ensembles/")
MINER_FORECAST_DIR_BG = Path("./miner_forecasts_background/")


async def ensemble_worker(self):
    """Background worker that processes ensemble tasks using the advanced method."""
    while self.ensemble_worker_running:
        run_id = None
        ensemble_id = None
        try:
            try:
                run_id = await asyncio.wait_for(self.ensemble_task_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue

            logger.info(f"[EnsembleWorker] Processing ensemble for run {run_id}")

            query = """
            SELECT 
                mr.miner_hotkey,
                mr.kerchunk_json_url,
                mr.job_id,  -- Need job_id for token requests
                fr.gfs_init_time_utc,
                ef.id as ensemble_id, 
                COALESCE(
                    (SELECT weight FROM weather_historical_weights 
                        WHERE miner_hotkey = mr.miner_hotkey 
                        ORDER BY last_updated DESC LIMIT 1),
                    0.5 -- Default weight if none found
                ) as miner_weight
            FROM 
                weather_miner_responses mr
            JOIN
                weather_forecast_runs fr ON mr.run_id = fr.id
            JOIN
                weather_ensemble_forecasts ef ON ef.forecast_run_id = fr.id
            WHERE 
                mr.run_id = :run_id
                AND mr.verification_passed = TRUE
                AND mr.status = 'verified'
            """
            responses = await self.db_manager.fetch_all(query, {"run_id": run_id})

            if not responses or len(responses) < 3:
                logger.warning(f"[EnsembleWorker] Not enough verified responses ({len(responses)}) for run {run_id} to create ensemble. Min required: {self.config.get('min_ensemble_members', 3)}")
                await self.db_manager.execute(
                    "UPDATE weather_ensemble_forecasts SET status = 'failed', error_message = :msg WHERE id = :eid",
                    {"eid": ensemble_id, "msg": "Insufficient verified members"}
                )
                self.ensemble_task_queue.task_done()
                continue
                
            ensemble_id = responses[0]['ensemble_id']
            gfs_init_time = responses[0]['gfs_init_time_utc']

            miner_forecast_refs = {}
            miner_weights = {}
            skipped_miners = []

            async def _get_miner_ref(response):
                miner_id = response['miner_hotkey']
                job_id = response['job_id']
                kerchunk_url = response['kerchunk_json_url']
                weight = float(response['miner_weight'])
                
                token_response = await self._request_fresh_token(miner_id, job_id)
                if not token_response or 'access_token' not in token_response:
                    logger.warning(f"[EnsembleWorker] Could not get access token for miner {miner_id}, job {job_id}. Skipping for ensemble.")
                    return None, miner_id
                    
                access_token = token_response['access_token']
                ref_spec = {
                    'url': kerchunk_url,
                    'protocol': 'http',
                    'options': {
                        'headers': {
                            'Authorization': f'Bearer {access_token}'
                        }
                    }
                }
                return (miner_id, ref_spec, weight), None

            tasks = [_get_miner_ref(resp) for resp in responses]
            results = await asyncio.gather(*tasks)

            for result, skipped_miner_id in results:
                    if skipped_miner_id:
                        skipped_miners.append(skipped_miner_id)
                    elif result:
                        miner_id, ref_spec, weight = result
                        miner_forecast_refs[miner_id] = ref_spec
                        miner_weights[miner_id] = weight

            if len(miner_forecast_refs) < self.config.get('min_ensemble_members', 3):
                logger.warning(f"[EnsembleWorker] Not enough valid miners ({len(miner_forecast_refs)}) after token requests for run {run_id}. Min required: {self.config.get('min_ensemble_members', 3)}")
                await self.db_manager.execute(
                    "UPDATE weather_ensemble_forecasts SET status = 'failed', error_message = :msg WHERE id = :eid",
                    {"eid": ensemble_id, "msg": "Insufficient members after token fetch"}
                )
                self.ensemble_task_queue.task_done()
                continue

            top_k = self.config.get('top_k_ensemble', None)
            ensemble_ds = await create_physics_aware_ensemble(
                miner_forecast_refs=miner_forecast_refs,
                miner_weights=miner_weights,
                top_k=top_k
                #variables_to_process=... # optional subset
                #consistency_checks=... # optional override default
            )

            if ensemble_ds:
                logger.info(f"[EnsembleWorker] Successfully created ensemble dataset for run {run_id}")
                
                time_str = gfs_init_time.strftime('%Y%m%d%H')
                ensemble_nc_filename = f"ensemble_run_{run_id}_{time_str}.nc"
                ensemble_path = VALIDATOR_ENSEMBLE_DIR / ensemble_nc_filename
                try:
                    encoding = {var: {'zlib': True, 'complevel': 5} for var in ensemble_ds.data_vars}
                    await asyncio.to_thread(ensemble_ds.to_netcdf, path=str(ensemble_path), encoding=encoding)
                    logger.info(f"[EnsembleWorker] Saved ensemble NetCDF: {ensemble_path}")
                except Exception as e_save:
                        logger.error(f"[EnsembleWorker] Failed to save ensemble NetCDF for run {run_id}: {e_save}", exc_info=True)
                        await self.db_manager.execute(
                        "UPDATE weather_ensemble_forecasts SET status = 'failed', error_message = :msg WHERE id = :eid",
                        {"eid": ensemble_id, "msg": f"Failed to save NetCDF: {e_save}"}
                        )
                        await self._update_run_status(run_id, "ensemble_failed", error_message=f"Failed to save NetCDF")
                        self.ensemble_task_queue.task_done()
                        continue

                kerchunk_filename = f"{os.path.splitext(ensemble_nc_filename)[0]}.json"
                kerchunk_path = VALIDATOR_ENSEMBLE_DIR / kerchunk_filename
                try:
                    h5chunks = SingleHdf5ToZarr(str(ensemble_path), inline_threshold=0)
                    kerchunk_metadata = h5chunks.translate()
                    with open(kerchunk_path, 'w') as f:
                        json.dump(kerchunk_metadata, f)
                    logger.info(f"[EnsembleWorker] Generated ensemble Kerchunk JSON: {kerchunk_path}")
                except Exception as e_kc:
                    logger.error(f"[EnsembleWorker] Failed to generate Kerchunk JSON for run {run_id}: {e_kc}", exc_info=True)
                    kerchunk_path = None

                verification_hash = None
                try:
                    ensemble_metadata_for_hash = {
                        "time": [gfs_init_time],
                        "source_model": "physics_aware_ensemble",
                        "resolution": ensemble_ds.attrs.get('resolution', 0.25)
                    }
                    variables_to_hash = [v for v in ensemble_ds.data_vars if v in ALL_EXPECTED_VARIABLES]
                    
                    data_for_hash = {"surf_vars": {}, "atmos_vars": {}}
                    for var in variables_to_hash:
                        var_data = ensemble_ds[var].values
                        if var in AURORA_FUNDAMENTAL_SURFACE_VARIABLES + AURORA_DERIVED_VARIABLES:
                            data_for_hash["surf_vars"][var] = var_data
                        elif var in AURORA_FUNDAMENTAL_ATMOS_VARIABLES:
                            data_for_hash["atmos_vars"][var] = var_data

                    if not data_for_hash["surf_vars"] and not data_for_hash["atmos_vars"]:
                         logger.warning(f"[EnsembleWorker] No variables categorized for hashing in ensemble for run {run_id}. Hash set to None.")
                         verification_hash = None
                    else:
                        logger.debug(f"[EnsembleWorker] Data prepared for hashing. Computing hash...")
                        verification_hash = compute_verification_hash(
                            data=data_for_hash,
                            metadata=ensemble_metadata_for_hash,
                            variables=list(data_for_hash["surf_vars"].keys()) + list(data_for_hash["atmos_vars"].keys()),
                            timesteps=list(range(len(ensemble_ds.time)))
                        )
                        if verification_hash:
                            logger.info(f"[EnsembleWorker] Computed ensemble verification hash for run {run_id}: {verification_hash[:10]}..."
                        )
                        else:
                            logger.warning(f"[EnsembleWorker] compute_verification_hash returned None for run {run_id}")
                
                except Exception as e_hash:
                     logger.error(f"[EnsembleWorker] Failed during hash preparation/computation for run {run_id}: {e_hash}", exc_info=True)
                     verification_hash = None

                update_params = {
                    "eid": ensemble_id,
                    "status": "completed",
                    "path": str(ensemble_path),
                    "kpath": str(kerchunk_path) if kerchunk_path else None,
                    "hash": verification_hash,
                    "end_time": datetime.now(timezone.utc)
                }
                update_query = """
                UPDATE weather_ensemble_forecasts
                SET status = :status, 
                    ensemble_path = :path,
                    ensemble_kerchunk_path = :kpath,
                    ensemble_verification_hash = :hash,
                    processing_end_time = :end_time,
                    error_message = NULL
                WHERE id = :eid
                """
                await self.db_manager.execute(update_query, update_params)
                await self._update_run_status(run_id, "completed")
                logger.info(f"[EnsembleWorker] Successfully completed and recorded ensemble for run {run_id}")

            else:
                logger.error(f"[EnsembleWorker] create_physics_aware_ensemble failed for run {run_id}")
                await self.db_manager.execute(
                    "UPDATE weather_ensemble_forecasts SET status = 'failed', error_message = :msg WHERE id = :eid",
                    {"eid": ensemble_id, "msg": "Ensemble creation function returned None"}
                )
                await self._update_run_status(run_id, "ensemble_failed", error_message="Ensemble function failed")

            self.ensemble_task_queue.task_done()

        except asyncio.CancelledError:
            logger.info("[EnsembleWorker] Worker cancelled")
            if run_id is not None and self.ensemble_task_queue._unfinished_tasks > 0:
                    self.ensemble_task_queue.task_done()
            break

        except Exception as e:
            logger.error(f"[EnsembleWorker] Unexpected error processing run {run_id}: {e}", exc_info=True)
            if run_id and ensemble_id:
                try:
                    await self.db_manager.execute(
                        "UPDATE weather_ensemble_forecasts SET status = 'error', error_message = :msg WHERE id = :eid",
                        {"eid": ensemble_id, "msg": f"Worker error: {e}"}
                    )
                    await self._update_run_status(run_id, "ensemble_failed", error_message=f"Worker error: {e}")
                except Exception as db_err:
                        logger.error(f"[EnsembleWorker] Failed to update DB on error: {db_err}")
            if run_id is not None and self.ensemble_task_queue._unfinished_tasks > 0:
                    self.ensemble_task_queue.task_done()
            await asyncio.sleep(1)

async def run_inference_background(
    task_instance: 'WeatherTask',
    job_id: str,
):
    """
    Runs the weather forecast inference as a background task.
    Fetches data based on job_id (expects cache hit from fetch_and_hash task)
    and prepares the batch before running the model.
    
    Args:
        task_instance: The WeatherTask instance (miner-side).
        job_id: The unique job identifier.
    """
    logger.info(f"[InferenceTask Job {job_id}] Starting background inference task...")
    if task_instance.inference_runner is None:
        logger.error(f"[InferenceTask Job {job_id}] Inference runner not available. Aborting.")
        await update_job_status(task_instance, job_id, "error", "Inference runner missing")
        return
    if task_instance.db_manager is None:
         logger.error(f"[InferenceTask Job {job_id}] DB manager not available. Aborting.")
         return

    prepared_batch = None
    ds_t0 = None
    ds_t_minus_6 = None
    try:
        logger.info(f"[InferenceTask Job {job_id}] Fetching job details from DB...")
        query = "SELECT status, gfs_init_time_utc, gfs_t_minus_6_time_utc, validator_hotkey, miner_hotkey FROM weather_miner_jobs WHERE id = :job_id"
        job_details = await task_instance.db_manager.fetch_one(query, {"job_id": job_id})

        if not job_details:
            logger.error(f"[InferenceTask Job {job_id}] Job details not found in DB. Aborting.")
            return

        if task_instance.keypair:
             miner_hotkey = task_instance.keypair.ss58_address
        else:
             logger.warning(f"[InferenceTask Job {job_id}] Miner keypair not found in WeatherTask instance. Using placeholder hotkey.")
             miner_hotkey = "miner_hk_missing"

        current_status = job_details['status']
        if current_status != 'inference_queued':
             logger.warning(f"[InferenceTask Job {job_id}] Expected status 'inference_queued' but found '{current_status}'. Proceeding cautiously...")
        
        gfs_init_time_utc = job_details['gfs_init_time_utc']
        gfs_t_minus_6_time_utc = job_details['gfs_t_minus_6_time_utc']
        if not gfs_init_time_utc or not gfs_t_minus_6_time_utc:
            logger.error(f"[InferenceTask Job {job_id}] Missing GFS timestamps in DB record. Aborting.")
            await update_job_status(task_instance, job_id, "error", "Missing GFS timestamps for inference")
            return
        
        await update_job_status(task_instance, job_id, "loading_input")
        logger.info(f"[InferenceTask Job {job_id}] Fetching GFS data (expecting cache hit) for T0={gfs_init_time_utc}, T-6={gfs_t_minus_6_time_utc}")
        gfs_cache_dir = Path(task_instance.config.get('gfs_analysis_cache_dir', './gfs_analysis_cache'))
        ds_t0 = await fetch_gfs_analysis_data([gfs_init_time_utc], cache_dir=gfs_cache_dir)
        ds_t_minus_6 = await fetch_gfs_analysis_data([gfs_t_minus_6_time_utc], cache_dir=gfs_cache_dir)

        if ds_t0 is None or ds_t_minus_6 is None:
            logger.error(f"[InferenceTask Job {job_id}] Failed to fetch/load GFS data from cache. Aborting.")
            await update_job_status(task_instance, job_id, "error", "Failed to load GFS data for inference")
            return

        logger.info(f"[InferenceTask Job {job_id}] Preparing Aurora Batch from fetched datasets...")
        try:
            prepared_batch = await asyncio.to_thread(create_aurora_batch_from_gfs, ds_t0, ds_t_minus_6)

            if prepared_batch is None:
                raise ValueError("Batch preparation function returned None")
            logger.info(f"[InferenceTask Job {job_id}] Aurora Batch prepared successfully.")
        except Exception as batch_prep_err:
            logger.error(f"[InferenceTask Job {job_id}] Failed to prepare Aurora Batch: {batch_prep_err}", exc_info=True)
            await update_job_status(task_instance, job_id, "error", f"Batch preparation failed: {batch_prep_err}")
            return
        finally:
            if ds_t0 and hasattr(ds_t0, 'close'): ds_t0.close()
            if ds_t_minus_6 and hasattr(ds_t_minus_6, 'close'): ds_t_minus_6.close()

        await update_job_status(task_instance, job_id, "running_inference")
        logger.info(f"[InferenceTask Job {job_id}] Waiting for GPU semaphore...")
        async with task_instance.gpu_semaphore:
            logger.info(f"[InferenceTask Job {job_id}] Acquired GPU semaphore, running inference...")
            try:
                selected_predictions_cpu = await asyncio.to_thread(
                    task_instance.inference_runner.run_multistep_inference,
                    prepared_batch,
                    steps=task_instance.config.get('inference_steps', 40)
                )
                logger.info(f"[InferenceTask Job {job_id}] Inference completed with {len(selected_predictions_cpu)} time steps")

            except Exception as infer_err:
                logger.error(f"[InferenceTask Job {job_id}] Inference failed: {infer_err}", exc_info=True)
                await update_job_status(task_instance, job_id, "error", error_message=f"Inference error: {infer_err}")
                return

        await update_job_status(task_instance, job_id, "processing_output")
        try:
            MINER_FORECAST_DIR_BG.mkdir(parents=True, exist_ok=True)

            def _blocking_save_and_process():
                if not selected_predictions_cpu:
                    raise ValueError("Inference returned no prediction steps.")

                forecast_datasets = []
                lead_times_hours = []
                base_time = pd.to_datetime(gfs_init_time_utc)

                for i, batch_step in enumerate(selected_predictions_cpu):
                    lead_time_hours = (i + 1) * task_instance.config.get('forecast_step_hours', 6) # Example: check actual model step logic
                    forecast_time = base_time + timedelta(hours=lead_time_hours)

                    if not isinstance(batch_step, Batch):
                         logger.warning(f"[InferenceTask Job {job_id}] Step {i} prediction is not an aurora.Batch, attempting conversion if possible.")
                         raise TypeError(f"Prediction step {i} has unexpected type: {type(batch_step)}")

                    ds_step = batch_step.to_xarray_dataset()
                    ds_step = ds_step.assign_coords(time=[forecast_time])
                    ds_step = ds_step.expand_dims('time')
                    forecast_datasets.append(ds_step)
                    lead_times_hours.append(lead_time_hours)

                if not forecast_datasets:
                    raise ValueError("No forecast datasets created after processing prediction steps.")

                combined_forecast_ds = xr.concat(forecast_datasets, dim='time')
                combined_forecast_ds = combined_forecast_ds.assign_coords(lead_time=('time', lead_times_hours))
                logger.info(f"[InferenceTask Job {job_id}] Combined forecast dimensions: {combined_forecast_ds.dims}")

                gfs_time_str = gfs_init_time_utc.strftime('%Y%m%d%H')
                unique_suffix = job_id.split('-')[0]
                filename_nc = f"weather_forecast_{gfs_time_str}_{miner_hotkey[:8]}_{unique_suffix}.nc"
                output_nc_path = MINER_FORECAST_DIR_BG / filename_nc

                encoding = {var: {'zlib': True, 'complevel': 4} for var in combined_forecast_ds.data_vars}
                combined_forecast_ds.to_netcdf(output_nc_path, encoding=encoding)
                logger.info(f"[InferenceTask Job {job_id}] Saved forecast to NetCDF: {output_nc_path}")

                filename_json = f"{os.path.splitext(filename_nc)[0]}.json"
                output_json_path = MINER_FORECAST_DIR_BG / filename_json

                h5chunks = SingleHdf5ToZarr(str(output_nc_path), inline_threshold=100)
                kerchunk_metadata = h5chunks.translate()
                with open(output_json_path, 'w') as f:
                    json.dump(kerchunk_metadata, f)
                logger.info(f"[InferenceTask Job {job_id}] Generated Kerchunk JSON: {output_json_path}")

                from gaia.tasks.defined_tasks.weather.utils.hashing import compute_verification_hash
                output_metadata = {
                    "time": [base_time],
                    "source_model": "aurora",
                    "resolution": 0.25
                }
                data_for_hash = {"surf_vars": {}, "atmos_vars": {}}
                for var_name in combined_forecast_ds.data_vars:
                     if var_name in CANONICAL_VARS_FOR_HASHING:
                          is_surface = len(combined_forecast_ds[var_name].dims) == 3 # time, lat, lon
                          is_atmos = len(combined_forecast_ds[var_name].dims) == 4 # time, level, lat, lon
                          if is_surface:
                               data_for_hash["surf_vars"][var_name] = combined_forecast_ds[var_name].values[np.newaxis, ...]
                          elif is_atmos:
                               data_for_hash["atmos_vars"][var_name] = combined_forecast_ds[var_name].values[np.newaxis, ...]

                variables_to_hash = list(data_for_hash["surf_vars"].keys()) + list(data_for_hash["atmos_vars"].keys())
                timesteps_to_hash = list(range(len(combined_forecast_ds.time)))
                
                if not variables_to_hash:
                    logger.warning(f"[InferenceTask Job {job_id}] No variables found/categorized for output hashing!")
                    verification_hash = None
                else:
                    verification_hash = compute_verification_hash(
                        data=data_for_hash,
                        metadata=output_metadata,
                        variables=variables_to_hash,
                        timesteps=timesteps_to_hash
                    )
                    if verification_hash:
                         logger.info(f"[InferenceTask Job {job_id}] Computed output verification hash: {verification_hash[:10]}...")
                    else:
                         logger.warning(f"[InferenceTask Job {job_id}] compute_verification_hash returned None for output.")

                return str(output_nc_path), str(output_json_path), verification_hash

            nc_path, json_path, v_hash = await asyncio.to_thread(_blocking_save_and_process)

            await update_job_paths(
                task_instance=task_instance,
                job_id=job_id,
                target_netcdf_path=nc_path,
                kerchunk_json_path=json_path,
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
        if ds_t0 is not None and hasattr(ds_t0, 'close'):
            try: ds_t0.close() 
            except Exception: pass
        if ds_t_minus_6 is not None and hasattr(ds_t_minus_6, 'close'):
            try: ds_t_minus_6.close()
            except Exception: pass

async def initial_scoring_worker(self):
    """Background worker to calculate initial scores/weights vs GFS analysis."""
    while self.initial_scoring_worker_running:
        run_id = None
        gfs_analysis_ds = None
        try:
            try:
                run_id = await asyncio.wait_for(self.initial_scoring_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                    continue 
                    
            logger.info(f"[InitialScoringWorker] Processing initial scores for run {run_id}")
            await self._update_run_status(run_id, "initial_scoring")
            
            responses_query = """
            SELECT 
                mr.id as response_id, 
                mr.miner_hotkey, 
                mr.kerchunk_json_url, 
                mr.job_id,
                fr.gfs_init_time_utc
            FROM weather_miner_responses mr
            JOIN weather_forecast_runs fr ON mr.run_id = fr.id
            WHERE mr.run_id = :run_id AND mr.verification_passed = TRUE AND mr.status = 'verified'
            """
            responses = await self.db_manager.fetch_all(responses_query, {"run_id": run_id})
            
            min_members = self.config.get('min_ensemble_members', 3)
            if not responses or len(responses) < min_members:
                logger.warning(f"[InitialScoringWorker] Run {run_id}: Insufficient verified responses ({len(responses)} < {min_members}) found for initial scoring.")
                await self._update_run_status(run_id, "initial_scoring_failed", error_message="Insufficient verified members")
                self.initial_scoring_queue.task_done()
                continue
                
            gfs_init_time = responses[0]['gfs_init_time_utc']
            logger.info(f"[InitialScoringWorker] Run {run_id}: Found {len(responses)} verified responses. Init time: {gfs_init_time}")
            
            sparse_lead_hours = self.config.get('initial_scoring_lead_hours', [24, 72])
            target_datetimes = [gfs_init_time + timedelta(hours=h) for h in sparse_lead_hours]
            logger.info(f"[InitialScoringWorker] Run {run_id}: Fetching GFS analysis for initial scoring at lead hours: {sparse_lead_hours}.")
            
            gfs_cache = Path(self.config.get('gfs_analysis_cache_dir', './gfs_analysis_cache'))
            gfs_analysis_ds = await fetch_gfs_analysis_data(target_times=target_datetimes, cache_dir=gfs_cache)
            
            if gfs_analysis_ds is None:
                logger.error(f"[InitialScoringWorker] Run {run_id}: Failed to fetch GFS analysis data. Aborting initial scoring.")
                await self._update_run_status(run_id, "initial_scoring_failed", error_message="GFS analysis fetch failed")
                self.initial_scoring_queue.task_done()
                continue
                    
            logger.info(f"[InitialScoringWorker] Run {run_id}: GFS analysis data fetched/loaded.")
            
            calculated_weights = {}
            scored_miners_count = 0
            tasks = []

            for resp in responses:
                 tasks.append(calculate_gfs_score_and_weight(self, resp, target_datetimes, gfs_analysis_ds))
                     
            scoring_results = await asyncio.gather(*tasks)
            
            for hotkey, weight, score in scoring_results:
                 if weight is not None and score is not None:
                     calculated_weights[hotkey] = weight
                     scored_miners_count += 1
                     try:
                        insert_weight_query = """
                            INSERT INTO weather_historical_weights 
                            (miner_hotkey, run_id, score_type, score, weight, last_updated)
                            VALUES (:hk, :rid, :stype, :score, :weight, :ts)
                            ON CONFLICT (miner_hotkey, run_id, score_type) DO UPDATE SET
                            score = EXCLUDED.score, 
                            weight = EXCLUDED.weight, 
                            last_updated = EXCLUDED.last_updated
                        """
                        await self.db_manager.execute(insert_weight_query, {
                            "hk": hotkey, "rid": run_id, "stype": 'gfs_rmse', 
                            "score": score, "weight": weight, "ts": datetime.now(timezone.utc)
                        })
                        logger.debug(f"[InitialScoringWorker] Stored GFS weight for {hotkey}, run {run_id}")
                     except Exception as db_err:
                          logger.error(f"[InitialScoringWorker] Run {run_id}: Failed to store GFS-based weight for {hotkey}: {db_err}")
            
            logger.info(f"[InitialScoringWorker] Run {run_id}: Successfully processed GFS scores for {scored_miners_count}/{len(responses)} miners.")
            
            if scored_miners_count < min_members:
                logger.warning(f"[InitialScoringWorker] Run {run_id}: Only {scored_miners_count} miners successfully scored (min {min_members}). Cannot proceed to ensemble.")
                await self._update_run_status(run_id, "initial_scoring_failed", error_message=f"Scored {scored_miners_count}/{min_members} needed")
                self.initial_scoring_queue.task_done()
                continue
                
            logger.info(f"[InitialScoringWorker] Run {run_id}: Initial scoring complete. Triggering ensemble creation.")
            await self.ensemble_task_queue.put(run_id)
            await self._update_run_status(run_id, "processing_ensemble")
            
            self.initial_scoring_queue.task_done()
            
        except asyncio.CancelledError:
            logger.info("[InitialScoringWorker] Worker cancelled")
            if run_id:
                    self.initial_scoring_queue.task_done()
            break
            
        except Exception as e:
            logger.error(f"[InitialScoringWorker] Unexpected error processing run {run_id}: {e}", exc_info=True)
            if run_id:
                try:
                    await self._update_run_status(run_id, "initial_scoring_failed", error_message=f"Worker error: {e}")
                except Exception as db_err:
                    logger.error(f"[InitialScoringWorker] Failed to update DB status on error: {db_err}")
                if self.initial_scoring_queue._unfinished_tasks > 0:
                    self.initial_scoring_queue.task_done()
            await asyncio.sleep(1)
        finally:
                if gfs_analysis_ds:
                    try:
                        gfs_analysis_ds.close()
                    except Exception:
                        pass 
                gc.collect()

async def finalize_scores_worker(self):
    """Background worker to calculate final scores against ERA5 after delay."""
    CHECK_INTERVAL_SECONDS = int(self.config.get('final_scoring_check_interval_seconds', 3600)) # Default 1 hour
    ERA5_DELAY_DAYS = int(self.config.get('era5_delay_days', 5))
    FORECAST_DURATION_HOURS = int(self.config.get('forecast_duration_hours', 240)) # 10 days

    while self.final_scoring_worker_running:
        run_id = None
        era5_ds = None
        processed_run_ids = set()

        try:
            logger.info("[FinalizeWorker] Checking for runs ready for final ERA5 scoring...")

            now_utc = datetime.now(timezone.utc)
            forecast_end_cutoff = now_utc - timedelta(days=ERA5_DELAY_DAYS)
            init_time_cutoff = forecast_end_cutoff - timedelta(hours=FORECAST_DURATION_HOURS)

            runs_to_score_query = """
            SELECT id, gfs_init_time_utc
            FROM weather_forecast_runs
            WHERE status IN ('processing_ensemble', 'completed', 'initial_scoring_failed', 'ensemble_failed', 'final_scoring_failed', 'scored') -- Include 'scored' for potential re-scoring needs? Or exclude? Let's exclude for now.
            AND (final_scoring_attempted_time IS NULL OR final_scoring_attempted_time < :retry_cutoff) -- Allow retries after a while
            AND gfs_init_time_utc < :init_time_cutoff
            ORDER BY gfs_init_time_utc ASC
            LIMIT 10 -- Process in batches
            """
            retry_cutoff_time = now_utc - timedelta(hours=6)

            ready_runs = await self.db_manager.fetch_all(runs_to_score_query, {
                "init_time_cutoff": init_time_cutoff,
                "retry_cutoff": retry_cutoff_time
            })

            if not ready_runs:
                logger.debug("[FinalizeWorker] No runs ready for final scoring.")
            else:
                logger.info(f"[FinalizeWorker] Found {len(ready_runs)} runs potentially ready for final scoring.")

            for run in ready_runs:
                run_id = run['id']
                if run_id in processed_run_ids: continue

                gfs_init_time = run['gfs_init_time_utc']
                era5_ds = None

                logger.info(f"[FinalizeWorker] Processing final scores for run {run_id} (Init: {gfs_init_time}).")
                await self.db_manager.execute(
                        "UPDATE weather_forecast_runs SET final_scoring_attempted_time = :now WHERE id = :rid",
                        {"now": now_utc, "rid": run_id}
                )

                sparse_lead_hours = self.config.get('final_scoring_lead_hours', [120, 168]) # Day 5, Day 7 defaults
                target_datetimes = [gfs_init_time + timedelta(hours=h) for h in sparse_lead_hours]
                logger.info(f"[FinalizeWorker] Run {run_id}: Fetching ERA5 analysis for final scoring at lead hours: {sparse_lead_hours}.")

                era5_cache = Path(self.config.get('era5_cache_dir', './era5_cache'))
                era5_ds = await fetch_era5_data(target_times=target_datetimes, cache_dir=era5_cache)

                if era5_ds is None:
                    logger.error(f"[FinalizeWorker] Run {run_id}: Failed to fetch ERA5 data. Aborting final scoring for this run.")
                    await self._update_run_status(run_id, "final_scoring_failed", error_message="ERA5 fetch failed")
                    processed_run_ids.add(run_id)
                    continue

                logger.info(f"[FinalizeWorker] Run {run_id}: ERA5 data fetched/loaded.")

                logger.info(f"[FinalizeWorker] Run {run_id}: Found {len(verified_responses)} verified miner responses for final scoring.")
                tasks = []
                
                for resp in verified_responses:
                     resp_with_run_id = resp.copy()
                     resp_with_run_id['run_id'] = run_id 
                     tasks.append(calculate_era5_miner_score(self, resp_with_run_id, target_datetimes, era5_ds))
                         
                miner_scoring_results = await asyncio.gather(*tasks)
                successful_miner_scores = sum(1 for success in miner_scoring_results if success)
                logger.info(f"[FinalizeWorker] Run {run_id}: Completed final scoring attempts for {successful_miner_scores}/{len(verified_responses)} miners.")
                
                if successful_miner_scores > 0: 
                    logger.info(f"[FinalizeWorker] Run {run_id}: Building final score row using available ERA5 scores..." )
                    await build_score_row(self, run_id, ground_truth_ds=era5_ds)
                    await self._update_run_status(run_id, "scored")
                    logger.info(f"[FinalizeWorker] Run {run_id}: Final scoring process completed.")
                else:
                     logger.warning(f"[FinalizeWorker] Run {run_id}: No miners successfully scored against ERA5. Skipping score row build.")
                     await self._update_run_status(run_id, "final_scoring_failed", error_message="No miners scored vs ERA5")
                processed_run_ids.add(run_id)

        except asyncio.CancelledError:
            logger.info("[FinalizeWorker] Worker cancelled")
            break

        except Exception as e:
            logger.error(f"[FinalizeWorker] Unexpected error in main loop (Last run_id: {run_id}): {e}", exc_info=True)
            if run_id:
                try:
                    await self._update_run_status(run_id, "final_scoring_failed", error_message=f"Worker loop error: {e}")
                except Exception as db_err:
                    logger.error(f"[FinalizeWorker] Failed to update DB status on error: {db_err}")
        finally:
                if era5_ds:
                    try:
                        era5_ds.close()
                    except Exception:
                        pass
                gc.collect()

        try:
            logger.debug(f"[FinalizeWorker] Sleeping for {CHECK_INTERVAL_SECONDS} seconds...")
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
        except asyncio.CancelledError:
                logger.info("[FinalizeWorker] Sleep interrupted, worker stopping.")
                break

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
    
    while task_instance.cleanup_worker_running:
        try:
            now_ts = time.time()
            now_dt_utc = datetime.now(timezone.utc)
            logger.info("[CleanupWorker] Starting cleanup cycle...")

            async def cleanup_directory(dir_path: Path, retention_days: int, pattern: str = "*.nc"):
                if not dir_path.is_dir():
                    logger.debug(f"[CleanupWorker] Directory not found, skipping: {dir_path}")
                    return 0
                    
                cutoff_time = now_ts - (retention_days * 24 * 3600)
                deleted_count = 0
                try:
                    for filepath in dir_path.glob(pattern):
                        try:
                            if filepath.is_file():
                                file_mod_time = filepath.stat().st_mtime
                                if file_mod_time < cutoff_time:
                                    filepath.unlink()
                                    logger.debug(f"[CleanupWorker] Deleted old file: {filepath}")
                                    deleted_count += 1
                        except FileNotFoundError:
                            continue
                        except Exception as e_file:
                            logger.warning(f"[CleanupWorker] Error deleting file {filepath}: {e_file}")
                    if pattern == "*.json":
                         for item in dir_path.iterdir(): 
                              if item.is_dir() and not any(item.iterdir()):
                                   try:
                                        item.rmdir()
                                        logger.debug(f"[CleanupWorker] Removed empty directory: {item}")
                                   except OSError as e_dir:
                                        logger.warning(f"[CleanupWorker] Error removing empty dir {item}: {e_dir}")
                                        
                except Exception as e_glob:
                     logger.error(f"[CleanupWorker] Error processing directory {dir_path}: {e_glob}")
                logger.info(f"[CleanupWorker] Deleted {deleted_count} files older than {retention_days} days from {dir_path} matching {pattern}.")
                return deleted_count

            logger.info("[CleanupWorker] Cleaning up GFS cache...")
            await cleanup_directory(gfs_cache_dir, GFS_CACHE_RETENTION_DAYS, "*.nc")
            logger.info("[CleanupWorker] Cleaning up ERA5 cache...")
            await cleanup_directory(era5_cache_dir, ERA5_CACHE_RETENTION_DAYS, "*.nc")
            logger.info("[CleanupWorker] Cleaning up Ensemble files...")
            await cleanup_directory(ensemble_dir, ENSEMBLE_RETENTION_DAYS, "*.nc")
            await cleanup_directory(ensemble_dir, ENSEMBLE_RETENTION_DAYS, "*.json")

            logger.info("[CleanupWorker] Cleaning up old database records...")
            db_cutoff_time = now_dt_utc - timedelta(days=DB_RUN_RETENTION_DAYS)
            try:
                delete_runs_query = "DELETE FROM weather_forecast_runs WHERE run_initiation_time < :cutoff"
                result = await task_instance.db_manager.execute(delete_runs_query, {"cutoff": db_cutoff_time})
                if result and hasattr(result, 'rowcount'):
                    logger.info(f"[CleanupWorker] Deleted {result.rowcount} old runs (and related data via cascade) older than {db_cutoff_time}.")
                else:
                     logger.info(f"[CleanupWorker] Executed old run deletion query (rowcount not available).")
                     
            except Exception as e_db:
                logger.error(f"[CleanupWorker] Error during database cleanup: {e_db}", exc_info=True)
                
            logger.info("[CleanupWorker] Cleanup cycle finished.")

        except asyncio.CancelledError:
            logger.info("[CleanupWorker] Worker cancelled")
            break
        except Exception as e_outer:
            logger.error(f"[CleanupWorker] Unexpected error in main loop: {e_outer}", exc_info=True)
            await asyncio.sleep(60) 
            
        try:
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
        except asyncio.CancelledError:
             logger.info("[CleanupWorker] Sleep interrupted, worker stopping.")
             break

async def fetch_and_hash_gfs_task(
    task_instance: 'WeatherTask',
    job_id: str,
    t0_run_time: datetime,
    t_minus_6_run_time: datetime
):
    """
    Background task for miners: Fetches GFS analysis data for T=0h and T=-6h,
    computes the canonical input hash, and updates the job record in the database.
    """
    logger.info(f"[FetchHashTask Job {job_id}] Starting GFS fetch and input hash computation for T0={t0_run_time}, T-6={t_minus_6_run_time}")

    try:
        await update_job_status(task_instance, job_id, "fetching_gfs")

        cache_dir_str = task_instance.config.get('gfs_analysis_cache_dir', './gfs_analysis_cache')
        gfs_cache_dir = Path(cache_dir_str)
        gfs_cache_dir.mkdir(parents=True, exist_ok=True) # Ensure it exists

        logger.info(f"[FetchHashTask Job {job_id}] Calling compute_input_data_hash...")
        computed_hash = await compute_input_data_hash(
            t0_run_time=t0_run_time,
            t_minus_6_run_time=t_minus_6_run_time,
            cache_dir=gfs_cache_dir
        )

        if computed_hash:
            logger.info(f"[FetchHashTask Job {job_id}] Successfully computed input hash: {computed_hash[:10]}... Updating DB.")
            update_query = """
                UPDATE weather_miner_jobs
                SET input_data_hash = :hash, status = :status, updated_at = :now
                WHERE id = :job_id
            """
            await task_instance.db_manager.execute(update_query, {
                "job_id": job_id,
                "hash": computed_hash,
                "status": "input_hashed_awaiting_validation",
                "now": datetime.now(timezone.utc)
            })
            logger.info(f"[FetchHashTask Job {job_id}] Status updated to input_hashed_awaiting_validation.")
        else:
            logger.error(f"[FetchHashTask Job {job_id}] compute_input_data_hash failed (returned None). Updating status to fetch_error.")
            await update_job_status(task_instance, job_id, "fetch_error", "Failed to fetch GFS data or compute input hash.")

    except Exception as e:
        logger.error(f"[FetchHashTask Job {job_id}] Unexpected error: {e}", exc_info=True)
        try:
            await update_job_status(task_instance, job_id, "error", f"Unexpected error during fetch/hash: {e}")
        except Exception as db_err:
            logger.error(f"[FetchHashTask Job {job_id}] Failed to update status to error after exception: {db_err}")

    logger.info(f"[FetchHashTask Job {job_id}] Task finished.")