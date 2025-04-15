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

from fiber.logging_utils import get_logger

from typing import TYPE_CHECKING, Any, Optional, Dict, List
if TYPE_CHECKING:
    from ..weather_task import WeatherTask
    try:
        from aurora.core import Batch
    except ImportError:
        Batch = Any
else:
    Batch = Any 

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
from ..scoring.ensemble import (
    create_physics_aware_ensemble,
    _open_dataset_lazily,
    ALL_EXPECTED_VARIABLES,
    AURORA_FUNDAMENTAL_SURFACE_VARIABLES,
    AURORA_FUNDAMENTAL_ATMOS_VARIABLES,
    AURORA_DERIVED_VARIABLES
)
from ..scoring.metrics import calculate_rmse
logger = get_logger(__name__)

VALIDATOR_ENSEMBLE_DIR = Path("./validator_ensembles/")
MINER_FORECAST_DIR_BG = Path("./miner_forecasts_background/")


async def _ensemble_worker(self):
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
                logger.warning(f"[EnsembleWorker] Not enough verified responses ({len(responses)}) for run {run_id} to create ensemble. Min required: {getattr(self.config, 'min_ensemble_members', 3)}")
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

            if len(miner_forecast_refs) < getattr(self.config, 'min_ensemble_members', 3):
                logger.warning(f"[EnsembleWorker] Not enough valid miners ({len(miner_forecast_refs)}) after token requests for run {run_id}. Min required: {getattr(self.config, 'min_ensemble_members', 3)}")
                await self.db_manager.execute(
                    "UPDATE weather_ensemble_forecasts SET status = 'failed', error_message = :msg WHERE id = :eid",
                    {"eid": ensemble_id, "msg": "Insufficient members after token fetch"}
                )
                self.ensemble_task_queue.task_done()
                continue

            top_k = getattr(self.config, 'top_k_ensemble', None)
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
                        if 'time' not in ensemble_ds[var].dims:
                            var_data = np.expand_dims(var_data, axis=0)
                            
                        if var in AURORA_FUNDAMENTAL_SURFACE_VARIABLES + [dv for dv in AURORA_DERIVED_VARIABLES if dv in ensemble_ds]:
                            data_for_hash["surf_vars"][var] = var_data
                        elif var in AURORA_FUNDAMENTAL_ATMOS_VARIABLES:
                            data_for_hash["atmos_vars"][var] = var_data

                    if not data_for_hash["surf_vars"] and not data_for_hash["atmos_vars"]:
                            logger.warning(f"[EnsembleWorker] No variables found for hashing in ensemble for run {run_id}")
                else:
                        verification_hash = compute_verification_hash(
                            data=data_for_hash,
                            metadata=ensemble_metadata_for_hash,
                            variables=variables_to_hash,
                            timesteps=list(range(len(ensemble_ds.time)))
                        )
                        logger.info(f"[EnsembleWorker] Computed ensemble verification hash for run {run_id}: {verification_hash[:10]}..."
                    )
                except Exception as e_hash:
                        logger.error(f"[EnsembleWorker] Failed to compute verification hash for ensemble run {run_id}: {e_hash}", exc_info=True)

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

async def _run_inference_background(self, initial_batch: Batch, job_id: str, gfs_init_time: datetime, miner_hotkey: str):
    """
    Runs the weather forecast inference as a background task.
    
    Args:
        initial_batch: The preprocessed Aurora batch
        job_id: The unique job identifier
        gfs_init_time: The GFS initialization time
        miner_hotkey: The miner's hotkey
    """
    logger.info(f"[Job {job_id}] Starting background inference task...")
    
    try:
        await self.update_job_status(job_id, "processing")
        
        logger.info(f"[Job {job_id}] Waiting for GPU semaphore...")
        async with self.gpu_semaphore:
            logger.info(f"[Job {job_id}] Acquired GPU semaphore, running inference...")
            
            try:
                selected_predictions_cpu = await asyncio.to_thread(
                    self.inference_runner.run_multistep_inference,
                    initial_batch,
                    steps=40  # 40 steps of 6h each = 10 days
                )
                logger.info(f"[Job {job_id}] Inference completed with {len(selected_predictions_cpu)} time steps")
                
            except Exception as infer_err:
                logger.error(f"[Job {job_id}] Inference failed: {infer_err}", exc_info=True)
                await self.update_job_status(job_id, "error", error_message=f"Inference error: {infer_err}")
                return
        
        try:
            MINER_FORECAST_DIR_BG.mkdir(parents=True, exist_ok=True)
            
            def _blocking_save_and_process():
                if not selected_predictions_cpu:
                    raise ValueError("Inference returned no prediction steps.")

                forecast_datasets = []
                lead_times_hours = []
                base_time = pd.to_datetime(initial_batch.metadata.time[0])

                for i, batch in enumerate(selected_predictions_cpu):
                    step_index_original = i * 2 + 1
                    lead_time_hours = (step_index_original + 1) * 6
                    forecast_time = base_time + timedelta(hours=lead_time_hours)

                    ds_step = batch.to_xarray_dataset()
                    ds_step = ds_step.assign_coords(time=[forecast_time])
                    ds_step = ds_step.expand_dims('time')
                    forecast_datasets.append(ds_step)
                    lead_times_hours.append(lead_time_hours)

                combined_forecast_ds = xr.concat(forecast_datasets, dim='time')
                combined_forecast_ds = combined_forecast_ds.assign_coords(lead_time=('time', lead_times_hours))
                logger.info(f"[Job {job_id}] Combined forecast dimensions: {combined_forecast_ds.dims}")

                gfs_time_str = gfs_init_time.strftime('%Y%m%d%H')
                unique_suffix = str(uuid.uuid4())[:8]
                filename_nc = f"weather_forecast_{gfs_time_str}_{miner_hotkey[:8]}_{unique_suffix}.nc"
                output_nc_path = MINER_FORECAST_DIR_BG / filename_nc
                
                combined_forecast_ds.to_netcdf(output_nc_path)
                logger.info(f"[Job {job_id}] Saved forecast to NetCDF: {output_nc_path}")
                
                filename_json = f"{os.path.splitext(filename_nc)[0]}.json"
                output_json_path = MINER_FORECAST_DIR_BG / filename_json
                
                from kerchunk.hdf import SingleHdf5ToZarr
                h5chunks = SingleHdf5ToZarr(str(output_nc_path), inline_threshold=0)
                kerchunk_metadata = h5chunks.translate()
                
                with open(output_json_path, 'w') as f:
                    json.dump(kerchunk_metadata, f)
                logger.info(f"[Job {job_id}] Generated Kerchunk JSON: {output_json_path}")
                
                from gaia.tasks.defined_tasks.weather.utils.hashing import compute_verification_hash
                
                forecast_metadata = {
                    "time": [base_time],
                    "source_model": "aurora",
                    "resolution": 0.25
                }
                
                variables_to_hash = ["2t", "10u", "10v", "msl", "z", "u", "v", "t", "q"]
                timesteps_to_hash = list(range(len(forecast_datasets)))
            
                data_for_hash = {
                    "surf_vars": {},
                    "atmos_vars": {}
                }
                
                for var in combined_forecast_ds.data_vars:
                    if var in ["2t", "10u", "10v", "msl"]:
                        data_for_hash["surf_vars"][var] = combined_forecast_ds[var].values[np.newaxis, :]
                    elif var in ["z", "u", "v", "t", "q"]:
                        data_for_hash["atmos_vars"][var] = combined_forecast_ds[var].values[np.newaxis, :]
                
                verification_hash = compute_verification_hash(
                    data=data_for_hash,
                    metadata=forecast_metadata,
                    variables=[v for v in variables_to_hash if v in combined_forecast_ds.data_vars],
                    timesteps=timesteps_to_hash
                )
                
                logger.info(f"[Job {job_id}] Computed verification hash: {verification_hash}")
                
                return str(output_nc_path), str(output_json_path), verification_hash
            
            nc_path, json_path, v_hash = await asyncio.to_thread(_blocking_save_and_process)
            
            await self.update_job_paths(
                job_id=job_id, 
                target_netcdf_path=nc_path, 
                kerchunk_json_path=json_path, 
                verification_hash=v_hash
            )
            
            await self.update_job_status(job_id, "completed")
            logger.info(f"[Job {job_id}] Background task completed successfully")
            
        except Exception as save_err:
            logger.error(f"[Job {job_id}] Failed to save results: {save_err}", exc_info=True)
            await self.update_job_status(job_id, "error", error_message=f"Processing error: {save_err}")
    
    except Exception as e:
        logger.error(f"[Job {job_id}] Background task failed: {e}", exc_info=True)
        await self.update_job_status(job_id, "error", error_message=f"Task error: {e}")

async def _initial_scoring_worker(self):
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
            
            min_members = getattr(self.config, 'min_ensemble_members', 3)
            if not responses or len(responses) < min_members:
                logger.warning(f"[InitialScoringWorker] Run {run_id}: Insufficient verified responses ({len(responses)} < {min_members}) found for initial scoring.")
                await self._update_run_status(run_id, "initial_scoring_failed", error_message="Insufficient verified members")
                self.initial_scoring_queue.task_done()
                    continue
                
            gfs_init_time = responses[0]['gfs_init_time_utc']
            logger.info(f"[InitialScoringWorker] Run {run_id}: Found {len(responses)} verified responses. Init time: {gfs_init_time}")
            
            sparse_lead_hours = getattr(self.config, 'initial_scoring_lead_hours', [24, 72])
            target_datetimes = [gfs_init_time + timedelta(hours=h) for h in sparse_lead_hours]
            logger.info(f"[InitialScoringWorker] Run {run_id}: Fetching GFS analysis for initial scoring at lead hours: {sparse_lead_hours}.")
            
            gfs_cache = Path(getattr(self.config, 'gfs_analysis_cache_dir', './gfs_analysis_cache'))
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

            async def score_individual_miner(response):
                response_id = response['response_id']
                miner_hotkey = response['miner_hotkey']
                kerchunk_url = response['kerchunk_json_url']
                job_id = response['job_id']
                miner_sparse_ds = None
                miner_ds_lazy = None
                local_weight = None
                local_score = None
                
                try:
                    logger.debug(f"[InitialScoringWorker] Scoring miner {miner_hotkey} for response {response_id}")
                    token_response = await self._request_fresh_token(miner_hotkey, job_id)
                    if not token_response or 'access_token' not in token_response:
                        logger.warning(f"[InitialScoringWorker] Run {run_id}: Could not get token for {miner_hotkey}. Skipping initial score.")
                        return miner_hotkey, None, None
                        
                ref_spec = {
                    'url': kerchunk_url,
                        'protocol': 'http',
                        'options': {'headers': {'Authorization': f'Bearer {token_response["access_token"]}'}}
                    }
                    
                    miner_ds_lazy = await _open_dataset_lazily(ref_spec)
                    if miner_ds_lazy is None:
                            logger.warning(f"[InitialScoringWorker] Run {run_id}: Failed to open lazy dataset for {miner_hotkey}. Skipping.")
                            return miner_hotkey, None, None
                            
                    miner_sparse_ds = await asyncio.to_thread(
                        lambda: miner_ds_lazy.sel(time=target_datetimes, method='nearest').load()
                    )
                    
                    gfs_sparse_ds = gfs_analysis_ds.sel(time=target_datetimes, method='nearest')

                    common_vars = [v for v in ALL_EXPECTED_VARIABLES if v in miner_sparse_ds and v in gfs_sparse_ds]
                    if not common_vars:
                            logger.warning(f"[InitialScoringWorker] Run {run_id}, Miner {miner_hotkey}: No common vars with GFS. Skipping score.")
                            return miner_hotkey, None, None
                    
                    miner_dict = {var: miner_sparse_ds[var].values for var in common_vars}
                    gfs_dict = {var: gfs_sparse_ds[var].values for var in common_vars}
                    
                    local_score = await calculate_rmse(miner_dict, gfs_dict)
                    if local_score is None or not np.isfinite(local_score):
                            logger.warning(f"[InitialScoringWorker] Run {run_id}, Miner {miner_hotkey}: Invalid score calculated ({local_score}). Skipping weight.")
                            return miner_hotkey, None, None
                            
                    local_weight = 1.0 / (1.0 + max(0, local_score))
                    
                    logger.info(f"[InitialScoringWorker] Run {run_id}, Miner {miner_hotkey}: GFS Score (RMSE)={local_score:.4f}, Weight={local_weight:.4f}")
                    return miner_hotkey, local_weight, local_score
                    
                except Exception as e_score:
                    logger.error(f"[InitialScoringWorker] Run {run_id}: Error scoring miner {miner_hotkey}: {e_score}", exc_info=False)
                    logger.debug(traceback.format_exc())
                    return miner_hotkey, None, None
                finally:
                    if miner_ds_lazy: miner_ds_lazy.close()
                    del miner_sparse_ds
                    gc.collect()
                    
            for resp in responses:
                    tasks.append(score_individual_miner(resp))
                    
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
                            "hk": hotkey, 
                            "rid": run_id, 
                            "stype": 'gfs_rmse', # Store score type explicitly
                            "score": score, 
                            "weight": weight, 
                            "ts": datetime.now(timezone.utc)
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

async def _finalize_scores_worker(self):
    """Background worker to calculate final scores against ERA5 after delay."""
    CHECK_INTERVAL_SECONDS = int(getattr(self.config, 'final_scoring_check_interval_seconds', 3600)) # Default 1 hour
    ERA5_DELAY_DAYS = int(getattr(self.config, 'era5_delay_days', 5))
    FORECAST_DURATION_HOURS = int(getattr(self.config, 'forecast_duration_hours', 240)) # 10 days

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

                sparse_lead_hours = getattr(self.config, 'final_scoring_lead_hours', [120, 168]) # Day 5, Day 7 defaults
                target_datetimes = [gfs_init_time + timedelta(hours=h) for h in sparse_lead_hours]
                logger.info(f"[FinalizeWorker] Run {run_id}: Fetching ERA5 analysis for final scoring at lead hours: {sparse_lead_hours}.")

                era5_cache = Path(getattr(self.config, 'era5_cache_dir', './era5_cache'))
                era5_ds = await fetch_era5_data(target_times=target_datetimes, cache_dir=era5_cache)

                if era5_ds is None:
                    logger.error(f"[FinalizeWorker] Run {run_id}: Failed to fetch ERA5 data. Aborting final scoring for this run.")
                    await self._update_run_status(run_id, "final_scoring_failed", error_message="ERA5 fetch failed")
                    processed_run_ids.add(run_id)
                    continue

                logger.info(f"[FinalizeWorker] Run {run_id}: ERA5 data fetched/loaded.")

                # Score Individual Miners (vs ERA5)
                miner_responses_query = """
                SELECT id as response_id, miner_hotkey, kerchunk_json_url, job_id
                FROM weather_miner_responses
                WHERE run_id = :run_id AND verification_passed = TRUE
                """
                verified_responses = await self.db_manager.fetch_all(miner_responses_query, {"run_id": run_id})

                logger.info(f"[FinalizeWorker] Run {run_id}: Found {len(verified_responses)} verified miner responses for final scoring.")
                tasks = []

                async def score_individual_miner_era5(response):
                    response_id = response['response_id']
                    miner_hotkey = response['miner_hotkey']
                    kerchunk_url = response['kerchunk_json_url']
                    job_id = response['job_id']
                    miner_sparse_ds = None
                    miner_ds_lazy = None
                    local_score = None

                    try:
                        logger.debug(f"[FinalizeWorker] Final scoring miner {miner_hotkey} vs ERA5 (Resp ID: {response_id})")
                        if not kerchunk_url:
                            logger.warning(f"[FinalizeWorker] Missing Kerchunk URL for verified response {response_id}. Skipping final score.")
                            return False

                        token_response = await self._request_fresh_token(miner_hotkey, job_id)
                        if not token_response or 'access_token' not in token_response:
                            logger.warning(f"[FinalizeWorker] Run {run_id}: Could not get token for {miner_hotkey} for final scoring. Skipping.")
                            return False

                        ref_spec = {'url': kerchunk_url, 'protocol': 'http', 'options': {'headers': {'Authorization': f'Bearer {token_response["access_token"]}'}}}
                        miner_ds_lazy = await _open_dataset_lazily(ref_spec)
                        if miner_ds_lazy is None: return False

                        miner_sparse_ds = await asyncio.to_thread(
                            lambda: miner_ds_lazy.sel(time=target_datetimes, method='nearest').load()
                        )
                        era5_sparse_ds = era5_ds.sel(time=target_datetimes, method='nearest')

                        common_vars = [v for v in ALL_EXPECTED_VARIABLES if v in miner_sparse_ds and v in era5_sparse_ds]
                        if not common_vars:
                            logger.warning(f"[FinalizeWorker] Miner {miner_hotkey} - No common variables with ERA5 found for scoring.")
                            return False

                        miner_dict = {var: miner_sparse_ds[var].values for var in common_vars}
                        era5_dict = {var: era5_sparse_ds[var].values for var in common_vars}

                        local_score = await calculate_rmse(miner_dict, gfs_dict=era5_dict) # Pass ERA5 as gfs_dict
                        if local_score is None or not np.isfinite(local_score):
                            logger.warning(f"[FinalizeWorker] Miner {miner_hotkey} - Invalid ERA5 score calculated ({local_score}).")
                            return False

                        await self.db_manager.execute("""
                            INSERT INTO weather_miner_scores (response_id, run_id, score_type, score, calculation_time)
                            VALUES (:resp_id, :run_id, 'era5_rmse', :score, :ts)
                            ON CONFLICT (response_id, score_type) DO UPDATE SET
                            score = EXCLUDED.score, calculation_time = EXCLUDED.calculation_time
                        """, {
                            "resp_id": response_id, "run_id": run_id, "score": local_score, "ts": datetime.now(timezone.utc)
                        })
                        logger.debug(f"[FinalizeWorker] Run {run_id}: Stored final ERA5 score ({local_score:.4f}) for miner {miner_hotkey} (Resp ID: {response_id})")
                        return True

                    except Exception as e_score:
                        logger.error(f"[FinalizeWorker] Run {run_id}: Error final scoring miner {miner_hotkey} (Resp ID: {response_id}): {e_score}", exc_info=False)
                        return False
                    finally:
                        if miner_ds_lazy: miner_ds_lazy.close()
                        del miner_sparse_ds; gc.collect()

                for resp in verified_responses:
                        tasks.append(score_individual_miner_era5(resp))
                miner_scoring_results = await asyncio.gather(*tasks)
                successful_miner_scores = sum(1 for success in miner_scoring_results if success)
                logger.info(f"[FinalizeWorker] Run {run_id}: Completed final scoring attempts for {successful_miner_scores}/{len(verified_responses)} miners.")

                if successful_miner_scores > 0:
                    logger.info(f"[FinalizeWorker] Run {run_id}: Building final score row using available ERA5 scores..." )
                    await self.build_score_row(run_id)

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
