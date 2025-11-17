import asyncio
import traceback
import gc
import os
import sys
import json
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import uuid
import ipaddress
import numpy as np
import xarray as xr
import pandas as pd
import xskillscore as xs
from json import dumps

from gaia.utils.custom_logger import get_logger
from typing import TYPE_CHECKING, Any, Optional, Dict, List, Tuple

if TYPE_CHECKING:
    from .weather_task import WeatherTask

from .processing.weather_logic import _request_fresh_token
from .utils.remote_access import open_verified_remote_zarr_dataset, get_last_verified_open_error, open_verified_remote_zarr_variable
from .utils.hashing import get_last_manifest_log

from .weather_scoring.metrics import (
    calculate_bias_corrected_forecast,
    calculate_mse_skill_score,
    calculate_acc,
    perform_sanity_checks,
    _calculate_latitude_weights,
)

# Import memory-aware caching to prevent unlimited cache growth during scoring
from .utils.memory_management import memory_aware_cache

logger = get_logger(__name__)

# Constants for Day-1 Scoring
# DEFAULT_DAY1_ALPHA_SKILL = 0.5
# DEFAULT_DAY1_BETA_ACC = 0.5
# DEFAULT_DAY1_PATTERN_CORR_THRESHOLD = 0.3
# DEFAULT_DAY1_ACC_LOWER_BOUND = 0.6 # For a specific lead like +12h


async def evaluate_miner_forecast_day1(
    task_instance: "WeatherTask",
    miner_response_db_record: Dict,
    gfs_analysis_data_for_run: xr.Dataset,
    gfs_reference_forecast_for_run: xr.Dataset,
    era5_climatology: xr.Dataset,
    day1_scoring_config: Dict,
    run_gfs_init_time: datetime,
    precomputed_climatology_cache: Optional[
        Dict
    ] = None,  # NEW: Pre-computed climatology cache
) -> Dict:
    """
    Performs Day-1 scoring for a single miner's forecast.
    Uses GFS analysis as truth and GFS forecast as reference.
    Calculates bias-corrected skill scores and ACC for specified variables and lead times.
    """
    response_id = miner_response_db_record["id"]
    miner_hotkey = miner_response_db_record["miner_hotkey"]
    job_id = miner_response_db_record.get("job_id")
    run_id = miner_response_db_record.get("run_id")
    miner_uid = miner_response_db_record.get("miner_uid")

    # Start timing for this miner's scoring
    scoring_start_time = time.time()
    logger.info(
        f"[Day1Score] Starting for miner {miner_hotkey[:8]}...{miner_hotkey[-8:]} (Resp: {response_id}, Run: {run_id}, Job: {job_id}, UID: {miner_uid})"
    )
    
    # CRITICAL: Validate job_id ownership across miner records
    if job_id:
        try:
            logger.info(
                f"[Day1Score] Job {job_id} assigned to miner UID {miner_uid} with hotkey {miner_hotkey[:8]}...{miner_hotkey[-8:]}. "
                f"Validating job ownership..."
            )
            
            # CRITICAL: Check if this job_id appears in multiple miner records (data corruption)
            job_ownership_check = await task_instance.db_manager.fetch_all(
                """
                SELECT miner_uid, miner_hotkey, status, response_time 
                FROM weather_miner_responses 
                WHERE job_id = :job_id
                ORDER BY response_time DESC
                """,
                {"job_id": job_id}
            )
            
            if len(job_ownership_check) > 1:
                logger.error(
                    f"[DATA CORRUPTION] Job {job_id} appears in {len(job_ownership_check)} different miner records:"
                )
                for i, record in enumerate(job_ownership_check):
                    logger.error(
                        f"  [{i+1}] UID {record['miner_uid']}, hotkey {record['miner_hotkey'][:8]}...{record['miner_hotkey'][-8:]}, "
                        f"status: {record['status']}, time: {record['response_time']}"
                    )
                logger.error(f"[DATA CORRUPTION] This job should only belong to ONE miner!")
            elif len(job_ownership_check) == 1:
                owner = job_ownership_check[0]
                if owner["miner_uid"] != miner_uid or owner["miner_hotkey"] != miner_hotkey:
                    logger.error(
                        f"[DATA CORRUPTION] Job {job_id} ownership mismatch: "
                        f"Current scoring: UID {miner_uid}, hotkey {miner_hotkey[:8]}...{miner_hotkey[-8:]} "
                        f"Database record: UID {owner['miner_uid']}, hotkey {owner['miner_hotkey'][:8]}...{owner['miner_hotkey'][-8:]}"
                    )
            else:
                logger.info(f"[Day1Score] Job {job_id} ownership validated - single record found")
        except Exception as e:
            logger.warning(f"[Day1Score] Could not validate job ownership: {e}")
    else:
        logger.warning(f"[Day1Score] No job_id provided, cannot validate ownership")

    day1_results = {
        "response_id": response_id,
        "miner_hotkey": miner_hotkey,
        "miner_uid": miner_uid,
        "run_id": run_id,
        "overall_day1_score": None,
        "qc_passed_all_vars_leads": True,
        "lead_time_scores": {},
        "error_message": None,
    }

    miner_forecast_ds: Optional[xr.Dataset] = None

    try:
        from .processing.weather_logic import _request_fresh_token, _is_miner_registered

        token_data_tuple = await _request_fresh_token(
            task_instance, miner_hotkey, job_id
        )
        if token_data_tuple is None:
            # Check if miner is still registered before treating this as a critical error
            is_registered = await _is_miner_registered(task_instance, miner_hotkey)
            if not is_registered:
                logger.warning(
                    f"[Day1Score] Miner {miner_hotkey} failed token request and is not in current metagraph - likely deregistered. Cleaning up all records for this miner."
                )
                # Import the cleanup function
                from .processing.weather_logic import (
                    _cleanup_miner_records,
                    _check_run_completion,
                )

                await _cleanup_miner_records(
                    task_instance, miner_hotkey, miner_response_db_record.get("run_id")
                )
                # Check if this was the last miner in the run
                await _check_run_completion(
                    task_instance, miner_response_db_record.get("run_id")
                )
                day1_results["error_message"] = (
                    "Miner not in current metagraph (likely deregistered)"
                )
                day1_results["overall_day1_score"] = 0.0
                day1_results["qc_passed_all_vars_leads"] = False
                return day1_results  # Return graceful failure rather than exception
            else:
                logger.error(
                    f"[Day1Score] Miner {miner_hotkey} failed token request but is still registered in metagraph. This may indicate a miner-side issue or network problem."
                )
                # Import the cleanup function
                from .processing.weather_logic import (
                    _cleanup_offline_miner_from_run,
                    _check_run_completion,
                )

                await _cleanup_offline_miner_from_run(
                    task_instance, miner_hotkey, miner_response_db_record.get("run_id")
                )
                # Check if this was the last miner in the run
                await _check_run_completion(
                    task_instance, miner_response_db_record.get("run_id")
                )
                day1_results["error_message"] = "Miner offline during scoring"
                day1_results["overall_day1_score"] = 0.0
                day1_results["qc_passed_all_vars_leads"] = False
                return day1_results  # Return graceful failure rather than exception

        access_token, miner_reported_url, miner_reported_manifest_hash = token_data_tuple

        # Use frozen URL/hash from DB when available; if missing (fast-path), fall back to miner-reported and freeze post-verify
        stored_rec = await task_instance.db_manager.fetch_one(
            "SELECT kerchunk_json_url, verification_hash_claimed, kerchunk_json_retrieved, frozen_manifest_files FROM weather_miner_responses WHERE id = :rid",
            {"rid": response_id},
        )
        # Strict: Day1 requires frozen URL/hash/manifest present; do not freeze during scoring
        if (
            not stored_rec
            or not stored_rec.get("kerchunk_json_url")
            or not stored_rec.get("verification_hash_claimed")
            or not stored_rec.get("kerchunk_json_retrieved")
        ):
            logger.error(
                f"[Day1Score] Missing frozen URL/hash/manifest for response {response_id}. Aborting day1 scoring."
            )
            day1_results["error_message"] = "Missing frozen manifest at day1"
            day1_results["overall_day1_score"] = 0.0
            day1_results["qc_passed_all_vars_leads"] = False
            return day1_results

        zarr_store_url = stored_rec["kerchunk_json_url"]
        # Normalize relative Zarr paths to fully-qualified URLs using miner origin
        try:
            if isinstance(zarr_store_url, str) and zarr_store_url.startswith("/"):
                # Build miner base from node_table
                row = await task_instance.db_manager.fetch_one(
                    "SELECT ip, port FROM node_table WHERE hotkey = :hk",
                    {"hk": miner_hotkey},
                )
                if row and row.get("ip") and row.get("port"):
                    ip_val, port_val = row["ip"], row["port"]
                    try:
                        ip_str = (
                            str(ipaddress.ip_address(int(ip_val)))
                            if isinstance(ip_val, (str, int)) and str(ip_val).isdigit()
                            else ip_val
                        )
                    except Exception:
                        ip_str = str(ip_val)
                    miner_base_url = f"https://{ip_str}:{port_val}"
                    normalized_url = miner_base_url.rstrip("/") + "/" + zarr_store_url.lstrip("/")
                    if normalized_url != zarr_store_url:
                        zarr_store_url = normalized_url
                        # Persist normalized URL to avoid future regressions
                        try:
                            await task_instance.db_manager.execute(
                                "UPDATE weather_miner_responses SET kerchunk_json_url = :url WHERE id = :rid",
                                {"url": zarr_store_url, "rid": response_id},
                            )
                        except Exception:
                            pass
        except Exception:
            pass
        claimed_manifest_content_hash = stored_rec["verification_hash_claimed"]
        frozen_manifest_json = stored_rec.get("kerchunk_json_retrieved")

        # Enforce mismatch policy strictly
        if (
            miner_reported_manifest_hash
            and miner_reported_manifest_hash != claimed_manifest_content_hash
        ):
            logger.error(
                f"[Day1Score] Manifest hash mismatch. Stored={claimed_manifest_content_hash[:10]}..., MinerNow={miner_reported_manifest_hash[:10]}..."
            )
            await task_instance.db_manager.execute(
                """
                UPDATE weather_miner_responses
                SET status = 'verification_error', error_message = 'Manifest hash mismatch between stored and miner-reported during day1 scoring'
                WHERE id = :rid
                """,
                {"rid": response_id},
            )
            day1_results["error_message"] = "Manifest hash mismatch (day1)"
            day1_results["overall_day1_score"] = 0.0
            day1_results["qc_passed_all_vars_leads"] = False
            return day1_results

        if not all([access_token, zarr_store_url, claimed_manifest_content_hash]):
            raise ValueError(
                f"Critical forecast access info missing for {miner_hotkey} (Job: {job_id})"
            )

        logger.info(
            f"[Day1Score] Opening VERIFIED Zarr store (FROZEN) for {miner_hotkey}: {zarr_store_url}"
        )
        storage_options = {
            "headers": {"Authorization": f"Bearer {access_token}"},
            "ssl": False,
        }

        verification_timeout_seconds = (
            task_instance.config.get("verification_timeout_seconds", 300) / 2
        )

        # Open dataset with VerifyingChunkMapper for on-demand verification
        miner_forecast_ds = None
        try:
            result_ds = await asyncio.wait_for(
                open_verified_remote_zarr_dataset(
                    zarr_store_url=zarr_store_url,
                    claimed_manifest_content_hash=claimed_manifest_content_hash,
                    miner_hotkey_ss58=miner_hotkey,
                    storage_options=storage_options,
                    job_id=f"{job_id}_day1_score",
                    frozen_manifest=frozen_manifest_json,
                    return_manifest=True,
                ),
                timeout=verification_timeout_seconds,
            )
            if isinstance(result_ds, tuple):
                miner_forecast_ds, _manifest_used = result_ds
            else:
                miner_forecast_ds = result_ds
        except Exception as open_err:
            logger.error(
                f"[Day1Score] Exception during verified open for {miner_hotkey} at {zarr_store_url}: {open_err}",
                exc_info=True,
            )
            miner_forecast_ds = None

        if miner_forecast_ds is None:
            logger.error(
                f"[Day1Score] Failed to open verified Zarr dataset for miner {miner_hotkey} - manifest verification failed\n"
                f"  URL: {zarr_store_url}\n  ClaimedHash: {claimed_manifest_content_hash}"
            )
            try:
                # Log the last manifest verification log for context
                last_log = get_last_manifest_log(f"{job_id}_day1_score")
                if last_log:
                    logger.error(f"[Day1Score] Last manifest verify log: {json.dumps(last_log, default=str)[:2000]}")
                # Capture last verified open error
                last_open_err = get_last_verified_open_error(f"{job_id}_day1_score")
                if last_open_err:
                    logger.error(f"[Day1Score] Last verified open error: {last_open_err}")
            except Exception:
                pass
            # Import the cleanup function
            from .processing.weather_logic import _check_run_completion

            # Mark this miner as failed but don't crash the entire batch
            await task_instance.db_manager.execute(
                """UPDATE weather_miner_responses 
                   SET status = 'verification_failed', 
                       error_message = 'Manifest verification failed during day1 scoring',
                       verification_passed = false
                   WHERE run_id = :run_id AND miner_hotkey = :miner_hotkey""",
                {
                    "run_id": run_id, 
                    "miner_hotkey": miner_hotkey,
                },
            )
            # Check if this was the last miner in the run
            await _check_run_completion(task_instance, run_id)
            day1_results["error_message"] = "Manifest verification failed"
            day1_results["overall_day1_score"] = 0.0
            day1_results["qc_passed_all_vars_leads"] = False
            return day1_results  # Return graceful failure rather than exception
        else:
            # Verification succeeded - mark as verified (do not freeze during scoring)
            await task_instance.db_manager.execute(
                """UPDATE weather_miner_responses 
                   SET verification_passed = true
                   WHERE run_id = :run_id AND miner_hotkey = :miner_hotkey""",
                {
                    "run_id": run_id, 
                    "miner_hotkey": miner_hotkey,
                },
            )

        hardcoded_valid_times: Optional[List[datetime]] = day1_scoring_config.get(
            "hardcoded_valid_times_for_eval"
        )
        if hardcoded_valid_times:
            logger.warning(
                f"[Day1Score] USING HARDCODED VALID TIMES FOR EVALUATION: {hardcoded_valid_times}"
            )
            times_to_evaluate = hardcoded_valid_times
        else:
            lead_times_to_score_hours: List[int] = day1_scoring_config.get(
                "lead_times_hours",
                task_instance.config.get("initial_scoring_lead_hours", [6, 12]),
            )
            times_to_evaluate = [
                run_gfs_init_time + timedelta(hours=h)
                for h in lead_times_to_score_hours
            ]
            logger.info(
                f"[Day1Score] Using lead_hours {lead_times_to_score_hours} relative to GFS init {run_gfs_init_time} for evaluation."
            )

        variables_to_score: List[Dict] = day1_scoring_config.get(
            "variables_levels_to_score", []
        )
        
        # CRITICAL: Debug scoring configuration to understand why scores are 0
        logger.info(
            f"[Day1Score] Miner {miner_hotkey[:8]}...{miner_hotkey[-8:]}: Scoring configuration:"
            f"\n  Variables to score: {len(variables_to_score)} variables"
            f"\n  Times to evaluate: {len(times_to_evaluate)} time steps"
            f"\n  Lead times: {day1_scoring_config.get('lead_times_hours', 'not set')}"
        )
        
        if not variables_to_score:
            logger.error(
                f"[Day1Score] Miner {miner_hotkey[:8]}...{miner_hotkey[-8:]}: "
                f"No variables configured for scoring! This will result in 0 score. "
                f"Check day1_variables_levels_to_score configuration."
            )
        
        if not times_to_evaluate:
            logger.error(
                f"[Day1Score] Miner {miner_hotkey[:8]}...{miner_hotkey[-8:]}: "
                f"No time steps to evaluate! This will result in 0 score. "
                f"Check lead_times_hours configuration."
            )

        # CRITICAL FIX: Apply variable mapping to ALL datasets for consistency
        logger.info(
            f"[Day1Score] Miner {miner_hotkey[:8]}...{miner_hotkey[-8:]}: "
            f"Applying variable mapping to all datasets for consistency..."
        )
        
        # Define the variable mapping from raw GFS names to scoring names
        var_mapping = {
            "tmp2m": "2t",
            "ugrd10m": "10u", 
            "vgrd10m": "10v",
            "prmslmsl": "msl",
            "tmpprs": "t",
            "ugrdprs": "u",
            "vgrdprs": "v", 
            "spfhprs": "q",
            "hgtprs": "z_height",
        }
        
        def apply_variable_mapping(dataset, dataset_name):
            """Apply variable mapping and coordinate renaming to a dataset"""
            logger.info(f"[Day1Score] Original {dataset_name} variables: {list(dataset.data_vars)}")
            logger.info(f"[Day1Score] Original {dataset_name} coordinates: {list(dataset.coords.keys())}")
            
            # Apply variable renaming
            vars_to_rename = {k: v for k, v in var_mapping.items() if k in dataset.data_vars}
            if vars_to_rename:
                logger.info(f"[Day1Score] Renaming {dataset_name} variables: {vars_to_rename}")
                dataset = dataset.rename(vars_to_rename)
                
                # Convert geopotential height to geopotential if present
                if "z_height" in dataset.data_vars:
                    logger.info(f"[Day1Score] Converting {dataset_name} geopotential height to geopotential...")
                    G = 9.80665  # Standard gravity
                    z_height_var = dataset["z_height"]
                    geopotential = G * z_height_var
                    dataset["z"] = geopotential
                    dataset["z"].attrs = {
                        "units": "m2 s-2",
                        "long_name": "Geopotential",
                        "standard_name": "geopotential"
                    }
                    dataset = dataset.drop_vars("z_height")
            
            # CRITICAL: Apply coordinate renaming for pressure levels
            coord_mapping = {
                "lev": "pressure_level",
                "latitude": "lat", 
                "longitude": "lon"
            }
            
            coords_to_rename = {k: v for k, v in coord_mapping.items() if k in dataset.coords}
            if coords_to_rename:
                logger.info(f"[Day1Score] Renaming {dataset_name} coordinates: {coords_to_rename}")
                dataset = dataset.rename(coords_to_rename)
                    
            logger.info(f"[Day1Score] Final {dataset_name} variables: {list(dataset.data_vars)}")
            logger.info(f"[Day1Score] Final {dataset_name} coordinates: {list(dataset.coords.keys())}")
            
            return dataset
        
        try:
            # Apply mapping to all three datasets
            miner_forecast_ds = apply_variable_mapping(miner_forecast_ds, "miner_forecast")
            gfs_analysis_data_for_run = apply_variable_mapping(gfs_analysis_data_for_run, "gfs_analysis")
            gfs_reference_forecast_for_run = apply_variable_mapping(gfs_reference_forecast_for_run, "gfs_reference")
            # Normalize and deduplicate time coordinates to avoid reindex errors
            def _dedup_time(ds: xr.Dataset, name: str) -> xr.Dataset:
                try:
                    if hasattr(ds, "time") and "time" in ds.coords:
                        time_vals = getattr(ds, "time", None)
                        if time_vals is not None:
                            try:
                                np_vals = np.array(time_vals.values)
                                if len(np_vals) > len(np.unique(np_vals)):
                                    logger.debug(f"[Day1Score] Removing duplicate time indices from {name}")
                                    _, unique_indices = np.unique(np_vals, return_index=True)
                                    ds = ds.isel(time=sorted(unique_indices))
                            except Exception:
                                pass
                except Exception:
                    pass
                return ds

            miner_forecast_ds = _dedup_time(miner_forecast_ds, "miner_forecast")
            gfs_analysis_data_for_run = _dedup_time(gfs_analysis_data_for_run, "gfs_analysis")
            gfs_reference_forecast_for_run = _dedup_time(gfs_reference_forecast_for_run, "gfs_reference")
                
        except Exception as e:
            logger.error(f"[Day1Score] Failed to apply variable mapping: {e}")
            day1_results["error_message"] = f"Variable mapping failed: {str(e)}"
            day1_results["overall_day1_score"] = -np.inf
            return day1_results

        aggregated_skill_scores = []
        aggregated_acc_scores = []

        # OPTIMIZED DAY1 QUALITY CONTROL: Fast sequential processing for essential variables
        # Day1 scoring focuses on atmospheric validity and GFS clone detection, not comprehensive analysis
        # Using minimal variable set (5 variables) for maximum speed while maintaining quality control

        logger.info(
            f"[Day1Score] Miner {miner_hotkey}: Starting OPTIMIZED DAY1 QC processing - {len(variables_to_score)} variables, {len(times_to_evaluate)} time steps"
        )
        parallel_timesteps_start = time.time()

        # SEQUENTIAL PRE-LOADING: Load variables one by one to avoid overwhelming the system
        # This prevents the massive memory spike of loading everything at once
        logger.info(f"[Day1Score] Miner {miner_hotkey}: Using sequential pre-loading strategy...")
        
        try:
            variables_needed = [var_config["name"] for var_config in variables_to_score]
            variables_in_dataset = [var for var in variables_needed if var in miner_forecast_ds.data_vars]
            
            if variables_in_dataset:
                # Convert times to proper format for selection
                time_coords = []
                for dt in times_to_evaluate:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)
                    time_coords.append(pd.Timestamp(dt))
                
                # Create focused subset
                miner_subset = miner_forecast_ds[variables_in_dataset].sel(time=time_coords, method="nearest")
                
                # SEQUENTIAL LOADING: Load each variable separately to control memory usage
                logger.info(f"[Day1Score] Pre-loading {len(variables_in_dataset)} variables sequentially...")
                preload_start = time.time()
                
                loaded_vars = {}
                for i, var_name in enumerate(variables_in_dataset):
                    try:
                        var_start = time.time()
                        # Load this variable's data
                        var_data = await asyncio.to_thread(lambda v=var_name: miner_subset[v].load())
                        loaded_vars[var_name] = var_data
                        var_time = time.time() - var_start
                        logger.debug(f"[Day1Score] Loaded variable {var_name} in {var_time:.2f}s ({i+1}/{len(variables_in_dataset)})")
                        
                        # Small delay between variables to avoid overwhelming the server
                        if i < len(variables_in_dataset) - 1:
                            await asyncio.sleep(0.1)
                            
                    except Exception as var_err:
                        logger.warning(f"[Day1Score] Failed to pre-load variable {var_name}: {var_err}")
                        # Continue with lazy loading for this variable
                        loaded_vars[var_name] = miner_subset[var_name]
                
                # Create new dataset with loaded variables
                if loaded_vars:
                    miner_forecast_ds = xr.Dataset(loaded_vars, attrs=miner_subset.attrs)
                    # Copy coordinates
                    for coord_name, coord_data in miner_subset.coords.items():
                        if coord_name not in miner_forecast_ds.coords:
                            miner_forecast_ds = miner_forecast_ds.assign_coords({coord_name: coord_data})
                
                preload_time = time.time() - preload_start
                logger.info(f"[Day1Score] Miner {miner_hotkey}: Sequential pre-loading completed in {preload_time:.2f}s")
                logger.info(f"[Day1Score] Loaded {len(loaded_vars)}/{len(variables_in_dataset)} variables successfully")
            else:
                logger.warning(f"[Day1Score] Miner {miner_hotkey}: No scoring variables found in dataset")
                
        except Exception as setup_err:
            logger.warning(f"[Day1Score] Miner {miner_hotkey}: Sequential pre-loading failed: {setup_err}")
            logger.warning("Falling back to standard lazy loading")

        # Process time steps sequentially for better CPU efficiency
        timestep_results = []
        for i, valid_time_dt in enumerate(times_to_evaluate):
            effective_lead_h = int(
                (valid_time_dt - run_gfs_init_time).total_seconds() / 3600
            )

            # Normalize timezone for processing
            if (
                valid_time_dt.tzinfo is None
                or valid_time_dt.tzinfo.utcoffset(valid_time_dt) is None
            ):
                valid_time_dt = valid_time_dt.replace(tzinfo=timezone.utc)
            else:
                valid_time_dt = valid_time_dt.astimezone(timezone.utc)

            # Initialize result structure for this time step
            time_key_for_results = effective_lead_h
            day1_results["lead_time_scores"][time_key_for_results] = {}

            logger.info(f"[Day1Score] Miner {miner_hotkey}: Processing time step {i+1}/{len(times_to_evaluate)}: +{effective_lead_h}h")
            
            # Process this time step sequentially
            timestep_result = await _process_single_timestep_sequential(
                valid_time_dt=valid_time_dt,
                effective_lead_h=effective_lead_h,
                variables_to_score=variables_to_score,
                miner_forecast_ds=miner_forecast_ds,
                gfs_analysis_data_for_run=gfs_analysis_data_for_run,
                gfs_reference_forecast_for_run=gfs_reference_forecast_for_run,
                era5_climatology=era5_climatology,
                precomputed_climatology_cache=precomputed_climatology_cache,
                day1_scoring_config=day1_scoring_config,
                run_gfs_init_time=run_gfs_init_time,
            miner_hotkey=miner_hotkey,
            zarr_store_url=zarr_store_url,
            claimed_manifest_content_hash=claimed_manifest_content_hash,
            access_token=access_token,
            job_id=job_id,
            frozen_manifest_json=frozen_manifest_json,
            )
            timestep_results.append(timestep_result)

        sequential_timesteps_time = time.time() - parallel_timesteps_start
        logger.info(
            f"[Day1Score] Miner {miner_hotkey}: SEQUENTIAL time step processing completed in {sequential_timesteps_time:.2f}s"
        )

        # Process results from sequential time step execution
        for i, result in enumerate(timestep_results):
            # Get the effective lead hour from the result
            time_key_for_results = result.get("effective_lead_h", i)

            if isinstance(result, Exception):
                logger.error(
                    f"[Day1Score] Miner {miner_hotkey}: PARALLEL time step {effective_lead_h}h failed: {result}",
                    exc_info=result,
                )
                day1_results["qc_passed_all_vars_leads"] = False
                continue

            if not isinstance(result, dict):
                logger.error(
                    f"[Day1Score] Miner {miner_hotkey}: Invalid result type from parallel time step {effective_lead_h}h: {type(result)}"
                )
                continue

            # Handle time step skips or errors
            if result.get("skip_reason"):
                logger.warning(
                    f"[Day1Score] Miner {miner_hotkey}: Time step {effective_lead_h}h skipped - {result['skip_reason']}"
                )
                day1_results["qc_passed_all_vars_leads"] = False
                continue

            if result.get("error_message"):
                logger.error(
                    f"[Day1Score] Miner {miner_hotkey}: Time step {effective_lead_h}h error - {result['error_message']}"
                )
                day1_results["qc_passed_all_vars_leads"] = False
                continue

            # Process successful time step results
            if not result.get("qc_passed", True) or result.get("qc_failure_reason"):
                day1_results["qc_passed_all_vars_leads"] = False

            # Copy variable results to the main results structure
            for var_key, var_result in result.get("variables", {}).items():
                day1_results["lead_time_scores"][time_key_for_results][
                    var_key
                ] = var_result

            # Add to aggregated scores from this time step
            timestep_skill_scores = result.get("aggregated_skill_scores", [])
            timestep_acc_scores = result.get("aggregated_acc_scores", [])

            aggregated_skill_scores.extend(timestep_skill_scores)
            aggregated_acc_scores.extend(timestep_acc_scores)

            logger.debug(
                f"[Day1Score] Miner {miner_hotkey}: Time step {effective_lead_h}h contributed {len(timestep_skill_scores)} skill scores and {len(timestep_acc_scores)} ACC scores"
            )

        # Cleanup: ensure references are dropped and GC runs
        try:
            try:
                del tasks_only
            except Exception:
                pass
            try:
                del timestep_results
            except Exception:
                pass
            collected = gc.collect()
            logger.debug(
                f"[Day1Score] Miner {miner_hotkey}: Post-processing cleanup collected {collected} objects"
            )
        except Exception as cleanup_err:
            logger.debug(
                f"[Day1Score] Miner {miner_hotkey}: Post-processing cleanup error: {cleanup_err}"
            )

        clipped_skill_scores = [
            max(0.0, s) for s in aggregated_skill_scores if np.isfinite(s)
        ]
        scaled_acc_scores = [
            (a + 1.0) / 2.0 for a in aggregated_acc_scores if np.isfinite(a)
        ]

        avg_clipped_skill = (
            np.mean(clipped_skill_scores) if clipped_skill_scores else 0.0
        )
        avg_scaled_acc = np.mean(scaled_acc_scores) if scaled_acc_scores else 0.0

        if not np.isfinite(avg_clipped_skill):
            avg_clipped_skill = 0.0
        if not np.isfinite(avg_scaled_acc):
            avg_scaled_acc = 0.0

        if not aggregated_skill_scores and not aggregated_acc_scores:
            logger.warning(
                f"[Day1Score] Miner {miner_hotkey}: No valid skill or ACC scores to aggregate. Setting overall score to 0."
            )
            day1_results["overall_day1_score"] = 0.0
            day1_results["qc_passed_all_vars_leads"] = False
        else:
            alpha = day1_scoring_config.get("alpha_skill", 0.5)
            beta = day1_scoring_config.get("beta_acc", 0.5)

            if not np.isclose(alpha + beta, 1.0):
                logger.warning(
                    f"[Day1Score] Miner {miner_hotkey}: Alpha ({alpha}) + Beta ({beta}) does not equal 1. Score may not be 0-1 bounded as intended."
                )

            normalized_score = alpha * avg_clipped_skill + beta * avg_scaled_acc

            # Strict near-GFS clamp: if any critical variable at any evaluated lead
            # has clone_distance_mse < strict_fraction * delta, clamp normalized_score to configured cap
            try:
                if day1_scoring_config.get("strict_clone_clamp_enabled", True):
                    strict_fraction = float(day1_scoring_config.get("strict_clone_clamp_fraction", 0.5) or 0.5)
                    clamp_value = float(day1_scoring_config.get("strict_clamp_value", 0.05) or 0.05)
                    delta_thresholds_config = day1_scoring_config.get("clone_delta_thresholds", {})

                    def _violates(var_key: str, var_result: dict) -> bool:
                        try:
                            delta = delta_thresholds_config.get(var_key)
                            if delta is None:
                                return False
                            cd = var_result.get("clone_penalty_applied")
                            # If penalty applied > 0, clone_distance_mse < delta already. Treat as violation under strict_fraction.
                            # Otherwise, if clone_distance_mse present, compare directly.
                            if cd is not None and float(cd) > 0:
                                return True
                            d = var_result.get("clone_distance_mse")
                            return (d is not None) and (float(d) < strict_fraction * float(delta))
                        except Exception:
                            return False

                    any_violation = False
                    for time_key, vars_map in day1_results.get("lead_time_scores", {}).items():
                        if not isinstance(vars_map, dict):
                            continue
                        for var_key, var_res in vars_map.items():
                            if isinstance(var_res, dict) and _violates(var_key, var_res):
                                any_violation = True
                                break
                        if any_violation:
                            break
                    if any_violation:
                        logger.warning(
                            f"[Day1Score] Miner {miner_hotkey}: Strict clone clamp triggered -> clamping Day1 score to {clamp_value}"
                        )
                        normalized_score = min(normalized_score, clamp_value)
            except Exception as _strict_ex:
                logger.debug(f"[Day1Score] Strict clone clamp evaluation failed: {_strict_ex}")

            day1_results["overall_day1_score"] = normalized_score
            logger.info(
                f"[Day1Score] Miner {miner_hotkey}: AvgClippedSkill={avg_clipped_skill:.3f}, AvgScaledACC={avg_scaled_acc:.3f}, Overall Day1 Score={normalized_score:.3f}"
            )

    except ConnectionError as e_conn:
        logger.error(
            f"[Day1Score] Connection error for miner {miner_hotkey} (Job {job_id}): {e_conn}"
        )
        day1_results["error_message"] = f"ConnectionError: {str(e_conn)}"
        day1_results["overall_day1_score"] = -np.inf
        day1_results["qc_passed_all_vars_leads"] = False
    except asyncio.TimeoutError:
        logger.error(
            f"[Day1Score] Timeout opening dataset for miner {miner_hotkey} (Job {job_id})."
        )
        day1_results["error_message"] = "TimeoutError: Opening dataset timed out."
        day1_results["overall_day1_score"] = -np.inf
        day1_results["qc_passed_all_vars_leads"] = False
    except Exception as e_main:
        logger.error(
            f"[Day1Score] Main error for miner {miner_hotkey} (Resp: {response_id}): {e_main}",
            exc_info=True,
        )
        day1_results["error_message"] = str(e_main)
        day1_results["overall_day1_score"] = -np.inf  # Penalize on error
        day1_results["qc_passed_all_vars_leads"] = False
    finally:
        # CRITICAL: Clean up miner-specific objects, but preserve shared datasets
        if miner_forecast_ds:
            try:
                miner_forecast_ds.close()
                logger.debug(
                    f"[Day1Score] Closed miner forecast dataset for {miner_hotkey}"
                )
            except Exception:
                pass

        # Clean up any remaining intermediate objects created during this miner's evaluation
        # But do NOT clean the shared datasets (gfs_analysis_data_for_run, gfs_reference_forecast_for_run, era5_climatology)
        try:
            miner_specific_objects = [
                "miner_forecast_ds",
                "miner_forecast_lead",
                "gfs_analysis_lead",
                "gfs_reference_lead",
            ]

            for obj_name in miner_specific_objects:
                if obj_name in locals():
                    try:
                        obj = locals()[obj_name]
                        if (
                            hasattr(obj, "close")
                            and obj_name != "gfs_analysis_lead"
                            and obj_name != "gfs_reference_lead"
                        ):
                            # Don't close slices of shared datasets, just del the reference
                            obj.close()
                        del obj
                    except Exception:
                        pass

            # Single garbage collection pass for this miner
            collected = gc.collect()
            if collected > 10:  # Only log if significant cleanup occurred
                logger.debug(
                    f"[Day1Score] Cleanup for {miner_hotkey}: collected {collected} objects"
                )

        except Exception as cleanup_err:
            logger.debug(
                f"[Day1Score] Error in cleanup for {miner_hotkey}: {cleanup_err}"
            )

        # Log total scoring time for this miner
        total_scoring_time = time.time() - scoring_start_time
        logger.success(
            f"[Day1Score] ✅ TIMING: Miner {miner_hotkey} scoring completed in {total_scoring_time:.2f} seconds"
        )

    logger.info(
        f"[Day1Score] Completed for {miner_hotkey}. Final score: {day1_results['overall_day1_score']}, QC Passed: {day1_results['qc_passed_all_vars_leads']}"
    )
    return day1_results


async def _process_single_variable_parallel(
    var_config: Dict,
    miner_forecast_lead: xr.Dataset,
    gfs_analysis_lead: xr.Dataset,
    gfs_reference_lead: xr.Dataset,
    era5_climatology: xr.Dataset,
    precomputed_climatology_cache: Optional[Dict],
    day1_scoring_config: Dict,
    valid_time_dt: datetime,
    effective_lead_h: int,
    miner_hotkey: str,
    cached_pressure_dims: Dict,
    cached_lat_weights: Optional[xr.DataArray],
    cached_grid_dims: Optional[tuple],
    variables_to_score: List[Dict],
) -> Dict:
    """
    Process a single variable for parallel execution.

    Returns a dictionary with:
    - status: 'success', 'skipped', or 'error'
    - var_key: Variable identifier
    - skill_score: Calculated skill score (if successful)
    - acc_score: Calculated ACC score (if successful)
    - clone_distance_mse: MSE distance from reference
    - clone_penalty_applied: Applied clone penalty
    - sanity_checks: Sanity check results
    - error_message: Error details (if failed)
    - qc_failure_reason: QC failure reason (if applicable)
    """
    var_name = var_config["name"]
    var_level = var_config.get("level")
    standard_name_for_clim = var_config.get("standard_name", var_name)
    var_key = f"{var_name}{var_level}" if var_level and var_level != "all" else var_name

    result = {
        "status": "processing",
        "var_key": var_key,
        "skill_score": None,
        "acc_score": None,
        "clone_distance_mse": None,
        "clone_penalty_applied": None,
        "sanity_checks": {},
        "error_message": None,
        "qc_failure_reason": None,
    }

    try:
        # CRITICAL: Early coordinate validation to prevent downstream errors
        try:
            # Validate that required coordinates exist in datasets
            if var_level and var_level != "all":
                # Check pressure level coordinates
                miner_pressure_dim = None
                for dim_name in ["pressure_level", "lev", "plev", "level"]:
                    if dim_name in miner_forecast_lead.dims:
                        miner_pressure_dim = dim_name
                        break
                
                if not miner_pressure_dim:
                    raise KeyError(f"No pressure level dimension found in miner data for {var_key}. Available dims: {list(miner_forecast_lead.dims)}")
                
                # Validate pressure level exists
                if miner_pressure_dim in miner_forecast_lead.coords:
                    pressure_levels = miner_forecast_lead.coords[miner_pressure_dim].values
                    if var_level not in pressure_levels:
                        raise KeyError(f"Pressure level {var_level} not found in {miner_pressure_dim}. Available: {pressure_levels}")
                        
            # Validate spatial coordinates
            for coord_name in ["lat", "lon", "latitude", "longitude"]:
                if coord_name in ["lat", "latitude"] and coord_name not in miner_forecast_lead.coords:
                    continue  # Will be handled by standardization
                if coord_name in ["lon", "longitude"] and coord_name not in miner_forecast_lead.coords:
                    continue  # Will be handled by standardization
                    
        except KeyError as coord_err:
            import traceback
            tb_str = traceback.format_exc()
            logger.error(
                f"[Day1Score] Miner {miner_hotkey}: Coordinate validation failed for {var_key}: {coord_err}\n"
                f"Full traceback:\n{tb_str}"
            )
            result["status"] = "error"
            result["error_message"] = f"Coordinate validation failed: {coord_err}"
            return result
        
        logger.debug(
            f"[Day1Score] Miner {miner_hotkey}: PARALLEL scoring Var: {var_key} at Valid Time: {valid_time_dt}"
        )

        # OPTIMIZATION 6: Extract all variables at once to reduce dataset access overhead
        miner_var_da_unaligned = miner_forecast_lead[var_name]
        truth_var_da_unaligned = gfs_analysis_lead[var_name]
        ref_var_da_unaligned = gfs_reference_lead[var_name]

        # DRY: Delegate scoring to the unified preloaded path using extracted DataArrays. All unit checks/conversions
        # are applied inside the preloaded path to keep logic reachable and unified.
        return await _process_single_variable_parallel_preloaded(
            var_config=var_config,
            miner_var_da=miner_var_da_unaligned,
            truth_var_da=truth_var_da_unaligned,
            ref_var_da=ref_var_da_unaligned,
            era5_climatology=era5_climatology,
            precomputed_climatology_cache=precomputed_climatology_cache,
            day1_scoring_config=day1_scoring_config,
            valid_time_dt=valid_time_dt,
            effective_lead_h=effective_lead_h,
            miner_hotkey=miner_hotkey,
            cached_pressure_dims=cached_pressure_dims,
            cached_lat_weights=cached_lat_weights,
            cached_grid_dims=cached_grid_dims,
            variables_to_score=variables_to_score,
        )

        # [UNREACHABLE] Legacy diagnostics/unit-conversion logic was moved into the preloaded path
        # to ensure the checks remain reachable after DRY refactor.
        # OPTIMIZATION 5A: Reduce logging overhead - only log diagnostics for first variable or on issues
        # Calculate ranges once for use in both logging and unit checks
        miner_min, miner_max, miner_mean = (
            float(miner_var_da_unaligned.min()),
            float(miner_var_da_unaligned.max()),
            float(miner_var_da_unaligned.mean()),
        )
        truth_min, truth_max, truth_mean = (
            float(truth_var_da_unaligned.min()),
            float(truth_var_da_unaligned.max()),
            float(truth_var_da_unaligned.mean()),
        )
        ref_min, ref_max, ref_mean = (
            float(ref_var_da_unaligned.min()),
            float(ref_var_da_unaligned.max()),
            float(ref_var_da_unaligned.mean()),
        )

        # Only log detailed diagnostics for first variable or when potential issues detected
        is_first_var = (
            var_config == variables_to_score[0] if variables_to_score else True
        )
        has_potential_issue = (
            (var_name == "z" and miner_mean < 10000)
            or (var_name == "2t" and (miner_mean < 200 or miner_mean > 350))
            or (var_name == "msl" and (miner_mean < 50000 or miner_mean > 150000))
        )

        if is_first_var or has_potential_issue:
            logger.info(
                f"[Day1Score] Miner {miner_hotkey}: RAW DATA DIAGNOSTICS for {var_key} at {valid_time_dt}:"
            )
            logger.info(
                f"[Day1Score] Miner {miner_hotkey} {var_key}: range=[{miner_min:.1f}, {miner_max:.1f}], mean={miner_mean:.1f}, units={miner_var_da_unaligned.attrs.get('units', 'unknown')}"
            )
            logger.info(
                f"[Day1Score] Truth {var_key}: range=[{truth_min:.1f}, {truth_max:.1f}], mean={truth_mean:.1f}, units={truth_var_da_unaligned.attrs.get('units', 'unknown')}"
            )
            logger.info(
                f"[Day1Score] Ref   {var_key}: range=[{ref_min:.1f}, {ref_max:.1f}], mean={ref_mean:.1f}, units={ref_var_da_unaligned.attrs.get('units', 'unknown')}"
            )
        else:
            logger.debug(
                f"[Day1Score] Miner {miner_hotkey} {var_key}: mean={miner_mean:.1f}"
            )  # Minimal logging for subsequent vars

        # Check for potential unit mismatch indicators
        if var_name == "z" and var_level == 500:
            # For z500, geopotential should be ~49000-58000 m²/s²
            # If it's geopotential height, it would be ~5000-6000 m
            miner_ratio = (
                miner_mean / 9.80665
            )  # If miner is geopotential, this ratio should be ~5000-6000
            truth_ratio = truth_mean / 9.80665
            logger.info(
                f"[Day1Score] Miner {miner_hotkey}: z500 UNIT CHECK - If geopotential (m²/s²): miner_mean/g={miner_ratio:.1f}m, truth_mean/g={truth_ratio:.1f}m"
            )

            if miner_mean < 10000:  # Much smaller than expected geopotential
                logger.warning(
                    f"[Day1Score] Miner {miner_hotkey}: POTENTIAL UNIT MISMATCH: z500 mean ({miner_mean:.1f}) suggests geopotential height (m) rather than geopotential (m²/s²)"
                )
            elif truth_mean > 40000 and miner_mean > 40000:
                logger.info(
                    f"[Day1Score] Unit check OK: Both miner and truth z500 appear to be geopotential (m²/s²)"
                )

        elif var_name == "2t":
            # Temperature should be ~200-320 K
            if miner_mean < 200 or miner_mean > 350:
                logger.warning(
                    f"[Day1Score] Miner {miner_hotkey}: POTENTIAL UNIT ISSUE: 2t mean ({miner_mean:.1f}) outside expected range for Kelvin"
                )

        elif var_name == "msl":
            # Mean sea level pressure should be ~90000-110000 Pa
            if miner_mean < 50000 or miner_mean > 150000:
                logger.warning(
                    f"[Day1Score] Miner {miner_hotkey}: POTENTIAL UNIT ISSUE: msl mean ({miner_mean:.1f}) outside expected range for Pa"
                )

        # AUTOMATIC UNIT CONVERSION: Convert geopotential height to geopotential if needed
        if var_name == "z" and miner_mean < 10000 and truth_mean > 40000:
            logger.warning(
                f"[Day1Score] Miner {miner_hotkey}: AUTOMATIC UNIT CONVERSION: Converting miner z from geopotential height (m) to geopotential (m²/s²)"
            )
            miner_var_da_unaligned = miner_var_da_unaligned * 9.80665
            miner_var_da_unaligned.attrs["units"] = "m2 s-2"
            miner_var_da_unaligned.attrs["long_name"] = (
                "Geopotential (auto-converted from height)"
            )
            logger.info(
                f"[Day1Score] Miner {miner_hotkey}: After conversion: z range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}"
            )

        # Check for temperature unit conversions (Celsius to Kelvin)
        elif var_name in ["2t", "t"] and miner_mean < 100 and truth_mean > 200:
            logger.warning(
                f"[Day1Score] Miner {miner_hotkey}: AUTOMATIC UNIT CONVERSION: Converting miner {var_name} from Celsius to Kelvin"
            )
            miner_var_da_unaligned = miner_var_da_unaligned + 273.15
            miner_var_da_unaligned.attrs["units"] = "K"
            miner_var_da_unaligned.attrs["long_name"] = (
                f'{miner_var_da_unaligned.attrs.get("long_name", var_name)} (auto-converted from Celsius)'
            )
            logger.info(
                f"[Day1Score] Miner {miner_hotkey}: After conversion: {var_name} range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}"
            )

        # Check for pressure unit conversions (hPa to Pa)
        elif var_name == "msl" and miner_mean < 2000 and truth_mean > 50000:
            logger.warning(
                f"[Day1Score] Miner {miner_hotkey}: AUTOMATIC UNIT CONVERSION: Converting miner msl from hPa to Pa"
            )
            miner_var_da_unaligned = miner_var_da_unaligned * 100.0
            miner_var_da_unaligned.attrs["units"] = "Pa"
            miner_var_da_unaligned.attrs["long_name"] = (
                "Mean sea level pressure (auto-converted from hPa)"
            )
            logger.info(
                f"[Day1Score] Miner {miner_hotkey}: After conversion: msl range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}"
            )

        if var_level:
            # OPTIMIZATION 6B: Use cached pressure dimensions to avoid repeated searches
            def find_pressure_dim_cached(data_array, dataset_name="dataset"):
                cache_key = (dataset_name, tuple(data_array.dims))
                if cache_key in cached_pressure_dims:
                    return cached_pressure_dims[cache_key]

                # Check for renamed coordinate first, then fallbacks
                for dim_name in ["pressure_level", "lev", "plev", "level"]:
                    if dim_name in data_array.dims:
                        cached_pressure_dims[cache_key] = dim_name
                        return dim_name

                logger.warning(
                    f"No pressure level dimension found in {dataset_name} for {var_key} level {var_level}. Available dims: {data_array.dims}"
                )
                cached_pressure_dims[cache_key] = None
                return None

            miner_pressure_dim = find_pressure_dim_cached(
                miner_var_da_unaligned, "miner"
            )
            truth_pressure_dim = find_pressure_dim_cached(
                truth_var_da_unaligned, "truth"
            )
            ref_pressure_dim = find_pressure_dim_cached(
                ref_var_da_unaligned, "reference"
            )

            if not all([miner_pressure_dim, truth_pressure_dim, ref_pressure_dim]):
                logger.warning(
                    f"Missing pressure dimensions for {var_key} level {var_level}. Skipping."
                )
                result["status"] = "skipped"
                result["qc_failure_reason"] = "Missing pressure dimensions"
                return result

            if var_level == "all":
                # Keep all pressure levels for comprehensive scoring
                miner_var_da_selected = miner_var_da_unaligned
                truth_var_da_selected = truth_var_da_unaligned  
                ref_var_da_selected = ref_var_da_unaligned
                logger.info(f"Scoring {var_key} across all pressure levels")
            else:
                # Select specific pressure level
                # Enforce exact level (no nearest). If mismatch, zero/fail early.
                try:
                    miner_var_da_selected = miner_var_da_unaligned.sel({miner_pressure_dim: var_level})
                    truth_var_da_selected = truth_var_da_unaligned.sel({truth_pressure_dim: var_level})
                    ref_var_da_selected = ref_var_da_unaligned.sel({ref_pressure_dim: var_level})
                except Exception:
                    logger.warning(f"[Day1Score] Level {var_level} exact selection failed for {var_key}; forcing zero score.")
                    result["status"] = "success"
                    result["skill_score"] = 0.0
                    result["acc_score"] = 0.0
                    result["clone_distance_mse"] = 0.0
                    result["qc_failure_reason"] = "pressure level mismatch"
                    return result

            # Skip level validation for "all" case since we're using all levels
            if var_level != "all":
                if abs(truth_var_da_selected[truth_pressure_dim].item() - var_level) > 1:
                    logger.warning(
                        f"Truth data for {var_key} level {var_level} too far ({truth_var_da_selected[truth_pressure_dim].item()}). Skipping."
                    )
                    result["status"] = "skipped"
                    result["qc_failure_reason"] = "Truth data level too far from target"
                    return result
                if abs(miner_var_da_selected[miner_pressure_dim].item() - var_level) > 1:
                    logger.warning(
                        f"Miner data for {var_key} level {var_level} too far ({miner_var_da_selected[miner_pressure_dim].item()}). Skipping."
                    )
                    result["status"] = "skipped"
                    result["qc_failure_reason"] = "Miner data level too far from target"
                    return result
                if abs(ref_var_da_selected[ref_pressure_dim].item() - var_level) > 1:
                    logger.warning(
                        f"GFS Ref data for {var_key} level {var_level} too far ({ref_var_da_selected[ref_pressure_dim].item()}). Skipping."
                    )
                    result["status"] = "skipped"
                    result["qc_failure_reason"] = "Reference data level too far from target"
                    return result
        else:
            miner_var_da_selected = miner_var_da_unaligned
            truth_var_da_selected = truth_var_da_unaligned
            ref_var_da_selected = ref_var_da_unaligned

        target_grid_da = truth_var_da_selected
        temp_spatial_dims = [
            d
            for d in target_grid_da.dims
            if d.lower() in ("latitude", "longitude", "lat", "lon")
        ]
        actual_lat_dim_in_target_for_ordering = next(
            (d for d in temp_spatial_dims if d.lower() in ("latitude", "lat")), None
        )

        if actual_lat_dim_in_target_for_ordering:
            lat_coord_values = target_grid_da[
                actual_lat_dim_in_target_for_ordering
            ].values
            if len(lat_coord_values) > 1 and lat_coord_values[0] < lat_coord_values[-1]:
                logger.info(
                    f"[Day1Score] Latitude coordinate '{actual_lat_dim_in_target_for_ordering}' in target_grid_da is ascending. Flipping to descending order for consistency."
                )
                target_grid_da = target_grid_da.isel(
                    {actual_lat_dim_in_target_for_ordering: slice(None, None, -1)}
                )
            else:
                logger.debug(
                    f"[Day1Score] Latitude coordinate '{actual_lat_dim_in_target_for_ordering}' in target_grid_da is already descending or has too few points to determine order."
                )
        else:
            logger.warning(
                "[Day1Score] Could not determine latitude dimension in target_grid_da to check/ensure descending order."
            )

        def _standardize_spatial_dims(data_array: xr.DataArray) -> xr.DataArray:
            if not isinstance(data_array, xr.DataArray):
                return data_array
            rename_dict = {}
            for dim_name in data_array.dims:
                if dim_name.lower() in ("latitude", "lat_0"):
                    rename_dict[dim_name] = "lat"
                elif dim_name.lower() in ("longitude", "lon_0"):
                    rename_dict[dim_name] = "lon"
            if rename_dict:
                logger.debug(
                    f"[Day1Score] Standardizing spatial dims for variable {var_key}: Renaming {rename_dict}"
                )
                return data_array.rename(rename_dict)
            return data_array

        # OPTIMIZATION 4A: Chain standardization and use target as reference to reduce copying
        target_grid_da_std = _standardize_spatial_dims(target_grid_da)
        truth_var_da_final = target_grid_da_std  # Use directly, no copy needed

        # OPTIMIZATION 4B: Keep interpolation threaded as it's CPU-intensive, but optimize data flow
        # ASYNC PROCESSING OPTION: For large datasets, use async processing for interpolation
        from .utils.async_processing import AsyncProcessingConfig

        # Check if we should use async processing (based on data size)
        use_async_processing = AsyncProcessingConfig.should_use_async_processing(
            miner_var_da_selected.size
        )

        if use_async_processing:
            logger.debug(
                f"[Day1Score] Using async processing for interpolation of {var_key}"
            )
            # Use xarray's optimized interpolation with dask backend

        # Use existing thread-based interpolation
        miner_var_da_aligned = await asyncio.to_thread(
            lambda: _standardize_spatial_dims(miner_var_da_selected).interp_like(
                target_grid_da_std, method="linear"
            )
        )

        ref_var_da_aligned = await asyncio.to_thread(
            lambda: _standardize_spatial_dims(ref_var_da_selected).interp_like(
                target_grid_da_std, method="linear"
            )
        )

        # Coverage and shape safeguards (mitigate NaN/mask doping and domain under-coverage) for preloaded path
        try:
            if tuple(miner_var_da_aligned.shape) != tuple(truth_var_da_final.shape):
                logger.warning(
                    f"[Day1Score] (preloaded) Shape mismatch after interp for {var_key}: miner={miner_var_da_aligned.shape}, truth={truth_var_da_final.shape}. Forcing zero score."
                )
                return {
                    "status": "success",
                    "var_key": var_key,
                    "skill_score": 0.0,
                    "acc_score": 0.0,
                    "clone_distance_mse": 0.0,
                    "clone_penalty_applied": 0.0,
                    "sanity_checks": {},
                    "qc_failure_reason": "grid shape mismatch",
                }
            try:
                min_cov = float(day1_scoring_config.get("min_finite_coverage_ratio", 0.999))
            except Exception:
                min_cov = 0.999
            miner_vals = miner_var_da_aligned.values
            total_elems = miner_vals.size if hasattr(miner_vals, "size") else 0
            finite_elems = int(np.isfinite(miner_vals).sum()) if total_elems else 0
            coverage_ratio = (finite_elems / total_elems) if total_elems else 0.0
            if coverage_ratio < min_cov:
                logger.warning(
                    f"[Day1Score] (preloaded) Low finite coverage for {var_key}: {coverage_ratio:.6f} < {min_cov:.6f}. Forcing zero score."
                )
                return {
                    "status": "success",
                    "var_key": var_key,
                    "skill_score": 0.0,
                    "acc_score": 0.0,
                    "clone_distance_mse": 0.0,
                    "clone_penalty_applied": 0.0,
                    "sanity_checks": {},
                    "qc_failure_reason": "finite coverage below threshold",
                }
        except Exception as cov_err:
            logger.debug(f"[Day1Score] (preloaded) Coverage/shape check skipped due to error: {cov_err}")

        # Coverage and shape safeguards (mitigate NaN/mask doping and domain under-coverage)
        try:
            # Enforce exact shape match after interpolation
            if tuple(miner_var_da_aligned.shape) != tuple(truth_var_da_final.shape):
                logger.warning(
                    f"[Day1Score] Shape mismatch after interp for {var_key}: miner={miner_var_da_aligned.shape}, truth={truth_var_da_final.shape}. Forcing zero score."
                )
                result["status"] = "success"
                result["skill_score"] = 0.0
                result["acc_score"] = 0.0
                result["clone_distance_mse"] = 0.0
                result["qc_failure_reason"] = "grid shape mismatch"
                return result
            # Finite coverage ratio on miner field
            try:
                min_cov = float(day1_scoring_config.get("min_finite_coverage_ratio", 0.999))
            except Exception:
                min_cov = 0.999
            miner_vals = miner_var_da_aligned.values
            total_elems = miner_vals.size if hasattr(miner_vals, "size") else 0
            finite_elems = int(np.isfinite(miner_vals).sum()) if total_elems else 0
            coverage_ratio = (finite_elems / total_elems) if total_elems else 0.0
            if coverage_ratio < min_cov:
                logger.warning(
                    f"[Day1Score] Low finite coverage for {var_key}: {coverage_ratio:.6f} < {min_cov:.6f}. Forcing zero score."
                )
                result["status"] = "success"
                result["skill_score"] = 0.0
                result["acc_score"] = 0.0
                result["clone_distance_mse"] = 0.0
                result["qc_failure_reason"] = "finite coverage below threshold"
                return result
        except Exception as cov_err:
            logger.debug(f"[Day1Score] Coverage/shape check skipped due to error: {cov_err}")

        broadcasted_weights_final = None
        # CRITICAL: Only use dimensions that exist in ALL arrays for MSE calculation
        # This prevents the "tuple.index(x): x not in tuple" error
        common_dims = set(truth_var_da_final.dims) & set(miner_var_da_aligned.dims) & set(ref_var_da_aligned.dims)
        
        # CRITICAL: Remove duplicate pressure level dimensions if they exist
        # This prevents the (13, 13) matrix issue when both 'plev' and 'pressure_level' exist
        pressure_dims_in_common = [d for d in common_dims if d in ("pressure_level", "plev", "lev", "level")]
        if len(pressure_dims_in_common) > 1:
            logger.warning(
                f"[Day1Score] Multiple pressure dimensions detected: {pressure_dims_in_common}. "
                f"Using only 'pressure_level' to prevent duplication."
            )
            # Remove old pressure dimension names, keep only 'pressure_level'
            common_dims = common_dims - set(pressure_dims_in_common) | {"pressure_level"}
        
        spatial_dims_for_metric = [
            d for d in common_dims if d not in ("time",)
        ]
        
        # Debug: Log the dimensions being used for MSE calculation
        logger.debug(
            f"[Day1Score] Dimension analysis for {var_key}:\n"
            f"  Truth dims: {truth_var_da_final.dims}\n"
            f"  Miner dims: {miner_var_da_aligned.dims}\n"
            f"  Ref dims: {ref_var_da_aligned.dims}\n"
            f"  MSE reduction dims: {spatial_dims_for_metric}"
        )

        actual_lat_dim_in_target = "lat" if "lat" in truth_var_da_final.dims else None

        if actual_lat_dim_in_target:
            try:
                # OPTIMIZATION 2: Use cached latitude weights if grid dimensions match
                current_grid_dims = (truth_var_da_final.dims, truth_var_da_final.shape)

                # Check if we can reuse cached weights, but ensure dimension compatibility
                is_current_pressure_level = "pressure_level" in truth_var_da_final.dims or any(dim in truth_var_da_final.dims for dim in ["lev", "plev", "level"])
                
                if (
                    cached_lat_weights is not None
                    and cached_grid_dims == current_grid_dims
                ):
                    # Check if cached weights are compatible with current variable type
                    cached_is_pressure_level = len(cached_lat_weights.dims) > 2  # More than lat, lon
                    
                    if is_current_pressure_level == cached_is_pressure_level:
                        # Compatible - reuse cached weights
                        broadcasted_weights_final = cached_lat_weights
                        if is_first_var:
                            logger.debug(
                                f"[Day1Score] For {var_key}, using CACHED latitude weights. Shape: {broadcasted_weights_final.shape}"
                            )
                    else:
                        # Incompatible - need to recalculate
                        logger.debug(f"[Day1Score] Cached weights incompatible for {var_key} (pressure level mismatch), recalculating")
                        cached_lat_weights = None  # Force recalculation
                else:
                    # Calculate weights and cache them for subsequent variables
                    target_lat_coord = truth_var_da_final[actual_lat_dim_in_target]
                    # OPTIMIZATION 3A: _calculate_latitude_weights is lightweight - run in main thread
                    one_d_lat_weights_target = _calculate_latitude_weights(
                        target_lat_coord
                    )
                    # CRITICAL: For pressure-level variables, only broadcast weights to spatial dims (lat, lon)
                    # to avoid dimension mismatch errors in xskillscore
                    if "pressure_level" in truth_var_da_final.dims or any(dim in truth_var_da_final.dims for dim in ["lev", "plev", "level"]):
                        # For pressure-level variables, create weights only for spatial dimensions
                        spatial_truth = truth_var_da_final.isel({dim: 0 for dim in truth_var_da_final.dims if dim not in ("lat", "lon")})
                        _, broadcasted_weights_final = xr.broadcast(
                            spatial_truth, one_d_lat_weights_target
                        )
                        logger.debug(f"[Day1Score] Created spatial-only weights for pressure-level variable {var_key}: {broadcasted_weights_final.dims}")
                    else:
                        # For surface variables, broadcast normally
                        _, broadcasted_weights_final = xr.broadcast(
                            truth_var_da_final, one_d_lat_weights_target
                        )
                        logger.debug(f"[Day1Score] Created full weights for surface variable {var_key}: {broadcasted_weights_final.dims}")

                    # Cache for subsequent variables in this time step
                    cached_lat_weights = broadcasted_weights_final
                    cached_grid_dims = current_grid_dims
                    logger.debug(
                        f"[Day1Score] For {var_key}, calculated and cached new latitude weights. Shape: {broadcasted_weights_final.shape}"
                    )

            except Exception as e_broadcast_weights:
                import traceback
                tb_str = traceback.format_exc()
                logger.error(
                    f"[Day1Score] Failed to create/broadcast latitude weights for {var_key}:\n"
                    f"Error: {e_broadcast_weights}\n"
                    f"Truth data dims: {truth_var_da_final.dims}, shape: {truth_var_da_final.shape}\n"
                    f"Is pressure level: {is_current_pressure_level}\n"
                    f"Full traceback:\n{tb_str}"
                )
                broadcasted_weights_final = None
        else:
            logger.warning(
                f"[Day1Score] For {var_key}, 'lat' dimension not found in truth_var_da_final (dims: {truth_var_da_final.dims}). No weights applied."
            )

        def _get_metric_scalar_value(metric_fn, *args, **kwargs):
            try:
                res = metric_fn(*args, **kwargs)
                if hasattr(res, "compute"):
                    res = res.compute()
                
                # Handle non-scalar results
                if hasattr(res, "size") and res.size > 1:
                    logger.warning(
                        f"[Day1Score] Metric result has {res.size} elements (dims: {getattr(res, 'dims', 'unknown')}), "
                        f"shape: {getattr(res, 'shape', 'unknown')} - taking mean to get scalar"
                    )
                    if hasattr(res, "mean"):
                        res = res.mean()
                    else:
                        import numpy as np
                        res = np.mean(res)
                
                # Convert to scalar
                if hasattr(res, "item"):
                    return float(res.item())
                else:
                    return float(res)
                    
            except Exception as metric_err:
                import traceback
                tb_str = traceback.format_exc()
                # Escape traceback to prevent loguru formatting issues
                tb_escaped = tb_str.replace('<', '\\<').replace('>', '\\>')
                
                logger.error(
                    f"[Day1Score] Metric calculation failed for {var_key}:\n"
                    f"Function: {metric_fn.__name__ if hasattr(metric_fn, '__name__') else str(metric_fn)}\n"
                    f"Args shapes: {[getattr(arg, 'shape', 'no shape') for arg in args if hasattr(arg, 'shape')]}\n"
                    f"Args dims: {[getattr(arg, 'dims', 'no dims') for arg in args if hasattr(arg, 'dims')]}\n"
                    f"Kwargs: {list(kwargs.keys())}\n"
                    f"Error: {metric_err}\n"
                    f"Full traceback:\n{tb_escaped}"
                )
                # Mitigation: treat metric errors as zero to remove incentive to trigger exceptions
                return 0.0

        # OPTIMIZATION 3D: MSE calculation is vectorized and fast - run in main thread
        clone_distance_mse_val = _get_metric_scalar_value(
            xs.mse,
            miner_var_da_aligned,
            ref_var_da_aligned,
            dim=spatial_dims_for_metric,
            weights=broadcasted_weights_final,
            skipna=True,
        )
        result["clone_distance_mse"] = clone_distance_mse_val

        delta_thresholds_config = day1_scoring_config.get("clone_delta_thresholds", {})
        delta_for_var = delta_thresholds_config.get(var_key)
        clone_penalty = 0.0

        if delta_for_var is not None and clone_distance_mse_val < delta_for_var:
            gamma = day1_scoring_config.get("clone_penalty_gamma", 1.0)
            clone_penalty = gamma * (1.0 - (clone_distance_mse_val / delta_for_var))
            clone_penalty = max(0.0, clone_penalty)
            logger.warning(
                f"[Day1Score] Miner {miner_hotkey}: GFS Clone Suspect: {var_key} at {effective_lead_h}h. "
                f"Distance MSE {clone_distance_mse_val:.4f} < Delta {delta_for_var:.4f}. Penalty: {clone_penalty:.4f}"
            )
            result["qc_failure_reason"] = f"Clone penalty triggered for {var_key}"
        result["clone_penalty_applied"] = clone_penalty

        # MAJOR OPTIMIZATION: Use pre-computed climatology if available
        if precomputed_climatology_cache:
            cache_key = f"{var_name}_{var_level}_{valid_time_dt.isoformat()}"
            clim_var_da_aligned = precomputed_climatology_cache.get(cache_key)

            if clim_var_da_aligned is not None:
                logger.debug(
                    f"[Day1Score] USING PRE-COMPUTED climatology for {var_key} - major speedup!"
                )
            else:
                logger.warning(
                    f"[Day1Score] Pre-computed climatology cache miss for {cache_key} - falling back to individual computation"
                )
                # Fallback to original computation
                clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
                clim_hour = valid_time_dt.hour
                clim_hour_rounded = (clim_hour // 6) * 6
                clim_var_da_raw = era5_climatology[standard_name_for_clim].sel(
                    dayofyear=clim_dayofyear, hour=clim_hour_rounded, method="nearest"
                )
                clim_var_to_interpolate = _standardize_spatial_dims(clim_var_da_raw)
                if var_level:
                    clim_pressure_dim = None
                    for dim_name in ["pressure_level", "lev", "plev", "level"]:
                        if dim_name in clim_var_to_interpolate.dims:
                            clim_pressure_dim = dim_name
                            break
                    if clim_pressure_dim:
                        clim_var_to_interpolate = clim_var_to_interpolate.sel(
                            **{clim_pressure_dim: var_level}, method="nearest"
                        )
                clim_var_da_aligned = await asyncio.to_thread(
                    lambda: clim_var_to_interpolate.interp_like(
                        truth_var_da_final, method="linear"
                    )
                )
        else:
            # Original climatology computation (fallback when no cache provided)
            clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
            clim_hour = valid_time_dt.hour

            clim_hour_rounded = (clim_hour // 6) * 6

            try:
                clim_var_da_raw = era5_climatology[standard_name_for_clim].sel(
                    dayofyear=clim_dayofyear, hour=clim_hour_rounded, method="nearest"
                )
            except (TypeError, ValueError) as sel_error:
                logger.error(
                    f"[Day1Score] Climatology selection failed for {standard_name_for_clim}: {sel_error}\n"
                    f"dayofyear: {clim_dayofyear} (type: {type(clim_dayofyear)})\n"
                    f"hour: {clim_hour_rounded} (type: {type(clim_hour_rounded)})\n"
                    f"Available dayofyear coords: {list(era5_climatology[standard_name_for_clim].coords.get('dayofyear', []))[:5]}...\n"
                    f"Available hour coords: {list(era5_climatology[standard_name_for_clim].coords.get('hour', []))[:5]}..."
                )
                # Try alternative selection without method parameter
                try:
                    logger.info(f"[Day1Score] Attempting fallback selection for {standard_name_for_clim}")
                    clim_var_da_raw = era5_climatology[standard_name_for_clim].isel(
                        dayofyear=min(int(clim_dayofyear) - 1, len(era5_climatology[standard_name_for_clim].coords.get('dayofyear', [])) - 1),
                        hour=min(int(clim_hour_rounded) // 6, len(era5_climatology[standard_name_for_clim].coords.get('hour', [])) - 1)
                    )
                except Exception as fallback_error:
                    logger.error(f"[Day1Score] Fallback climatology selection also failed: {fallback_error}")
                    raise sel_error
            
            # OPTIMIZATION 4C: Optimize climatology processing chain
            clim_var_to_interpolate = _standardize_spatial_dims(clim_var_da_raw)

            if var_level:
                # Handle different pressure level dimension names in climatology data
                clim_pressure_dim = None
                for dim_name in ["pressure_level", "lev", "plev", "level"]:
                    if dim_name in clim_var_to_interpolate.dims:
                        clim_pressure_dim = dim_name
                        break

                if clim_pressure_dim:
                    # OPTIMIZATION 3C: Climatology selection is lightweight - run in main thread
                    clim_var_to_interpolate = clim_var_to_interpolate.sel(
                        **{clim_pressure_dim: var_level}, method="nearest"
                    )
                    if (
                        abs(
                            clim_var_to_interpolate[clim_pressure_dim].item()
                            - var_level
                        )
                        > 10
                    ):
                        logger.warning(
                            f"[Day1Score] Climatology for {var_key} at target level {var_level} was found at {clim_var_to_interpolate[clim_pressure_dim].item()}. Using this nearest level data."
                        )

            # OPTIMIZATION 4D: Climatology interpolation in thread for consistency
            clim_var_da_aligned = await asyncio.to_thread(
                lambda: clim_var_to_interpolate.interp_like(
                    truth_var_da_final, method="linear"
                )
            )

        sanity_results = await perform_sanity_checks(
            forecast_da=miner_var_da_aligned,
            reference_da_for_corr=ref_var_da_aligned,
            variable_name=var_key,
            climatology_bounds_config=day1_scoring_config.get("climatology_bounds", {}),
            pattern_corr_threshold=day1_scoring_config.get(
                "pattern_correlation_threshold", 0.3
            ),
            lat_weights=broadcasted_weights_final,
        )
        result["sanity_checks"] = sanity_results

        if not sanity_results.get("climatology_passed") or not sanity_results.get(
            "pattern_correlation_passed"
        ):
            logger.warning(
                f"[Day1Score] Miner {miner_hotkey}: Sanity check failed for {var_key} at {effective_lead_h}h. Skipping metrics."
            )
            result["status"] = "skipped"
            result["qc_failure_reason"] = (
                f"Sanity check failed for {var_key} - "
                f"Climatology passed: {sanity_results.get('climatology_passed')}, "
                f"Pattern correlation passed: {sanity_results.get('pattern_correlation_passed')}"
            )
            return result

        # Bias Correction
        forecast_bc_da = await calculate_bias_corrected_forecast(
            miner_var_da_aligned, truth_var_da_final
        )

        # Check if this is a pressure-level variable for detailed scoring
        if var_level == "all" and "pressure_level" in miner_var_da_aligned.dims:
            # Per-pressure-level scoring for atmospheric variables
            logger.info(f"Calculating detailed per-pressure-level metrics for {var_name}")
            
            from .weather_scoring.metrics import (
                calculate_mse_skill_score_by_pressure_level,
                calculate_acc_by_pressure_level,
                calculate_rmse_by_pressure_level
            )
            
            # Single vectorized calculations that preserve pressure level dimension
            skill_scores_by_level = await calculate_mse_skill_score_by_pressure_level(
                forecast_bc_da, truth_var_da_final, ref_var_da_aligned, broadcasted_weights_final
            )
            acc_scores_by_level = await calculate_acc_by_pressure_level(
                miner_var_da_aligned, truth_var_da_final, clim_var_da_aligned, broadcasted_weights_final
            )
            rmse_scores_by_level = await calculate_rmse_by_pressure_level(
                miner_var_da_aligned, truth_var_da_final, broadcasted_weights_final
            )
            # Compute MAE per pressure level
            try:
                spatial_dims = [d for d in truth_var_da_final.dims if d.lower() in ("latitude", "longitude", "lat", "lon")]
                abs_err = abs(miner_var_da_aligned - truth_var_da_final)
                if broadcasted_weights_final is not None:
                    weighted_sum = await asyncio.to_thread(
                        lambda: (abs_err * broadcasted_weights_final).sum(dim=spatial_dims)
                    )
                    weights_sum = await asyncio.to_thread(
                        lambda: broadcasted_weights_final.sum(dim=spatial_dims)
                    )
                    mae_da = weighted_sum / weights_sum
                else:
                    mae_da = await asyncio.to_thread(abs_err.mean, spatial_dims)
                per_level_mae = {}
                if "pressure_level" in mae_da.dims:
                    for level in mae_da.coords["pressure_level"]:
                        per_level_mae[int(level.item())] = float(mae_da.sel(pressure_level=level).item())
                else:
                    per_level_mae[None] = float(mae_da.item())
            except Exception:
                per_level_mae = {}
            # Calculate per-level bias (weighted mean error over spatial dims)
            try:
                spatial_dims = [d for d in truth_var_da_final.dims if d.lower() in ("latitude", "longitude", "lat", "lon")]
                diff_da = miner_var_da_aligned - truth_var_da_final
                if broadcasted_weights_final is not None:
                    # Weighted mean over spatial dims
                    weighted_sum = await asyncio.to_thread(
                        lambda: (diff_da * broadcasted_weights_final).sum(dim=spatial_dims)
                    )
                    weights_sum = await asyncio.to_thread(
                        lambda: broadcasted_weights_final.sum(dim=spatial_dims)
                    )
                    bias_da = weighted_sum / weights_sum
                else:
                    bias_da = await asyncio.to_thread(diff_da.mean, spatial_dims)
                per_level_bias = {}
                if "pressure_level" in bias_da.dims:
                    for level in bias_da.coords["pressure_level"]:
                        per_level_bias[int(level.item())] = float(bias_da.sel(pressure_level=level).item())
                else:
                    # Fallback single bias
                    per_level_bias[None] = float(bias_da.item())
            except Exception as e_bias:
                logger.warning(f"[Day1Score] Failed to compute per-level bias for {var_name}: {e_bias}")
                per_level_bias = {}
            
            # Calculate averages across pressure levels
            avg_skill_score = 0.0
            avg_acc_score = 0.0
            avg_rmse_score = 0.0
            valid_levels = 0
            
            for level in skill_scores_by_level.keys():
                if level in acc_scores_by_level and level in rmse_scores_by_level:
                    avg_skill_score += skill_scores_by_level[level]
                    avg_acc_score += acc_scores_by_level[level]
                    avg_rmse_score += rmse_scores_by_level[level]
                    valid_levels += 1
            
            # Store averages (apply clone penalty consistently)
            _avg_skill = avg_skill_score / valid_levels if valid_levels > 0 else -np.inf
            _clone_penalty = float(result.get("clone_penalty_applied", 0.0) or 0.0)
            result["skill_score"] = _avg_skill - _clone_penalty
            result["acc_score"] = avg_acc_score / valid_levels if valid_levels > 0 else -np.inf
            result["rmse"] = avg_rmse_score / valid_levels if valid_levels > 0 else np.inf
            
            # Store detailed pressure-level results for component scoring
            result["pressure_level_scores"] = {
                "skill": skill_scores_by_level,
                "acc": acc_scores_by_level,
                "rmse": rmse_scores_by_level,
                "mae": per_level_mae,
                "bias": per_level_bias
            }
            
            logger.info(f"Extracted detailed metrics for {valid_levels} pressure levels")
            
        else:
            # Traditional single-level scoring for surface variables or specific pressure levels
            skill_score = await calculate_mse_skill_score(
                forecast_bc_da,
                truth_var_da_final,
                ref_var_da_aligned,
                broadcasted_weights_final,
            )
            
            # Apply clone penalty consistently to skill metric
            try:
                _clone_penalty = float(result.get("clone_penalty_applied", 0.0) or 0.0)
            except Exception:
                _clone_penalty = 0.0
            result["skill_score"] = skill_score - _clone_penalty

            # Also compute climatology-referenced skill to match ERA5 method
            try:
                skill_score_clim = await calculate_mse_skill_score(
                    forecast_bc_da,
                    truth_var_da_final,
                    clim_var_da_aligned,
                    broadcasted_weights_final,
                )
                result["skill_score_climatology"] = skill_score_clim
            except Exception:
                pass

            # ACC
            acc_score = await calculate_acc(
                miner_var_da_aligned,
                truth_var_da_final,
                clim_var_da_aligned,
                broadcasted_weights_final,
            )
            result["acc_score"] = acc_score
            # RMSE, MAE and Bias for surface variables
            try:
                from .weather_scoring.metrics import calculate_rmse, calculate_bias
                rmse_val = await calculate_rmse(miner_var_da_aligned, truth_var_da_final, broadcasted_weights_final)
                result["rmse"] = rmse_val
                # MAE calculation
                abs_err = abs(miner_var_da_aligned - truth_var_da_final)
                if broadcasted_weights_final is not None:
                    mae_val = float(((abs_err * broadcasted_weights_final).sum() / broadcasted_weights_final.sum()).item())
                else:
                    mae_val = float(abs_err.mean().item())
                result["mae"] = mae_val
                bias_val = await calculate_bias(miner_var_da_aligned, truth_var_da_final, broadcasted_weights_final)
                result["bias"] = bias_val
            except Exception as e_rmse_bias:
                logger.warning(f"[Day1Score] Failed to compute RMSE/Bias for {var_name}: {e_rmse_bias}")

        # ACC Lower Bound Check
        current_acc_score = result.get("acc_score", -np.inf)
        if (
            effective_lead_h == 12
            and np.isfinite(current_acc_score)
            and current_acc_score < day1_scoring_config.get("acc_lower_bound_d1", 0.6)
        ):
            logger.warning(
                f"[Day1Score] Miner {miner_hotkey}: ACC for {var_key} at valid time {valid_time_dt} (Eff. Lead 12h) ({current_acc_score:.3f}) is below threshold."
            )

        # Mark as successful
        result["status"] = "success"
        logger.debug(
            f"[Day1Score] Miner {miner_hotkey}: PARALLEL Variable {var_key} scored successfully"
        )

        return result

    except Exception as e_var:
        import traceback
        tb_str = traceback.format_exc()
        error_msg = str(e_var)
        
        # Escape any format string characters in the error message to prevent logging errors
        safe_error_msg = error_msg.replace('{', '{{').replace('}', '}}').replace("'", "''")
        safe_tb_str = tb_str.replace('{', '{{').replace('}', '}}').replace("'", "''")
        
        logger.error(
            f"[Day1Score] Miner {miner_hotkey}: Error in parallel scoring {var_key} at {valid_time_dt}:\n"
            f"Error: {safe_error_msg}\n"
            f"Variable config: {var_config}\n"
            f"Data shapes - Miner: {getattr(miner_forecast_lead, 'shape', 'unknown')}, "
            f"Truth: {getattr(gfs_analysis_lead, 'shape', 'unknown')}, "
            f"Ref: {getattr(gfs_reference_lead, 'shape', 'unknown')}\n"
            f"Full traceback:\n{safe_tb_str}"
        )
        result["status"] = "error"
        result["error_message"] = error_msg
        result["debug_info"] = {
            "var_config": var_config,
            "miner_shape": getattr(miner_forecast_lead, 'shape', None),
            "truth_shape": getattr(gfs_analysis_lead, 'shape', None),
            "ref_shape": getattr(gfs_reference_lead, 'shape', None),
            "traceback": tb_str
        }
        return result
    finally:
        # CRITICAL ABC MEMORY LEAK FIX: Clean up all temporary xarray ABC objects
        try:
            # List of all temporary ABC objects created during variable processing
            temp_objects = [
                "miner_var_da_unaligned",
                "truth_var_da_unaligned",
                "ref_var_da_unaligned",
                "miner_var_da_selected",
                "truth_var_da_selected",
                "ref_var_da_selected",
                "miner_var_da_aligned",
                "ref_var_da_aligned",
                "truth_var_da_final",
                "target_grid_da",
                "target_grid_da_std",
                "clim_var_da_aligned",
                "forecast_bc_da",
                "broadcasted_weights_final",
                "clim_var_da_raw",
                "clim_var_to_interpolate",
            ]

            cleaned_count = 0
            for obj_name in temp_objects:
                if obj_name in locals():
                    try:
                        obj = locals()[obj_name]
                        if hasattr(obj, "close") and callable(obj.close):
                            obj.close()
                        del obj
                        cleaned_count += 1
                    except Exception:
                        pass

            # Force garbage collection if we cleaned significant objects
            if cleaned_count > 3:
                collected = gc.collect()
                logger.debug(
                    f"[Day1Score] Variable {var_key} cleanup: removed {cleaned_count} objects, GC collected {collected}"
                )

                # Emergency memory check during variable processing
                try:
                    import psutil

                    process = psutil.Process()
                    current_memory_mb = process.memory_info().rss / (1024 * 1024)
                    if current_memory_mb > 15000:  # 15GB emergency threshold
                        logger.warning(
                            f"[Day1Score] Variable {var_key}: EMERGENCY MEMORY WARNING - {current_memory_mb:.1f} MB"
                        )
                        # Additional emergency cleanup
                        import sys

                        for _ in range(2):
                            gc.collect()
                except Exception:
                    pass

        except Exception as cleanup_err:
            logger.debug(f"[Day1Score] Variable {var_key} cleanup error: {cleanup_err}")


async def _process_single_variable_parallel_preloaded(
    var_config: Dict,
    miner_var_da: xr.DataArray,
    truth_var_da: xr.DataArray,
    ref_var_da: xr.DataArray,
    era5_climatology: xr.Dataset,
    precomputed_climatology_cache: Optional[Dict],
    day1_scoring_config: Dict,
    valid_time_dt: datetime,
    effective_lead_h: int,
    miner_hotkey: str,
    cached_pressure_dims: Dict,
    cached_lat_weights: Optional[xr.DataArray],
    cached_grid_dims: Optional[tuple],
    variables_to_score: List[Dict],
) -> Dict:
    """
    Process a single variable for parallel execution using pre-loaded DataArrays.
    
    This is an optimized version of _process_single_variable_parallel that works
    with pre-loaded variable data to prevent duplicate HTTP requests.

    Returns a dictionary with:
    - status: 'success', 'skipped', or 'error'
    - var_key: Variable identifier
    - skill_score: Calculated skill score (if successful)
    - acc_score: Calculated ACC score (if successful)
    - clone_distance_mse: MSE distance from reference
    - clone_penalty_applied: Applied clone penalty
    - sanity_checks: Sanity check results
    - error_message: Error details (if failed)
    - qc_failure_reason: QC failure reason (if applicable)
    """
    var_name = var_config["name"]
    var_level = var_config.get("level")
    standard_name_for_clim = var_config.get("standard_name", var_name)
    var_key = f"{var_name}{var_level}" if var_level and var_level != "all" else var_name

    result = {
        "status": "processing",
        "var_key": var_key,
        "skill_score": None,
        "acc_score": None,
        "clone_distance_mse": None,
        "clone_penalty_applied": None,
        "sanity_checks": {},
        "error_message": None,
        "qc_failure_reason": None,
    }

    try:
        logger.debug(
            f"[Day1Score] Miner {miner_hotkey}: PARALLEL scoring (preloaded) Var: {var_key} at Valid Time: {valid_time_dt}"
        )

        # Use pre-loaded DataArrays directly - no need to extract from datasets
        miner_var_da_unaligned = miner_var_da
        truth_var_da_unaligned = truth_var_da
        ref_var_da_unaligned = ref_var_da

        # Calculate ranges for logging and unit checks
        miner_min, miner_max, miner_mean = (
            float(miner_var_da_unaligned.min()),
            float(miner_var_da_unaligned.max()),
            float(miner_var_da_unaligned.mean()),
        )
        truth_min, truth_max, truth_mean = (
            float(truth_var_da_unaligned.min()),
            float(truth_var_da_unaligned.max()),
            float(truth_var_da_unaligned.mean()),
        )
        ref_min, ref_max, ref_mean = (
            float(ref_var_da_unaligned.min()),
            float(ref_var_da_unaligned.max()),
            float(ref_var_da_unaligned.mean()),
        )

        # Only log detailed diagnostics for first variable or when potential issues detected
        is_first_var = (
            var_config == variables_to_score[0] if variables_to_score else True
        )
        has_potential_issue = (
            (var_name == "z" and miner_mean < 10000)
            or (var_name == "2t" and (miner_mean < 200 or miner_mean > 350))
            or (var_name == "msl" and (miner_mean < 50000 or miner_mean > 150000))
        )

        if is_first_var or has_potential_issue:
            logger.info(
                f"[Day1Score] Miner {miner_hotkey}: RAW DATA DIAGNOSTICS (preloaded) for {var_key} at {valid_time_dt}:"
            )
            logger.info(
                f"[Day1Score] Miner {miner_hotkey} {var_key}: range=[{miner_min:.1f}, {miner_max:.1f}], mean={miner_mean:.1f}, units={miner_var_da_unaligned.attrs.get('units', 'unknown')}"
            )
            logger.info(
                f"[Day1Score] Truth {var_key}: range=[{truth_min:.1f}, {truth_max:.1f}], mean={truth_mean:.1f}, units={truth_var_da_unaligned.attrs.get('units', 'unknown')}"
            )
            logger.info(
                f"[Day1Score] Ref   {var_key}: range=[{ref_min:.1f}, {ref_max:.1f}], mean={ref_mean:.1f}, units={ref_var_da_unaligned.attrs.get('units', 'unknown')}"
            )
        else:
            logger.debug(
                f"[Day1Score] Miner {miner_hotkey} {var_key}: mean={miner_mean:.1f}"
            )

        # Apply the same unit conversion logic as the original function
        if var_name == "z" and var_level == 500:
            miner_ratio = miner_mean / 9.80665
            truth_ratio = truth_mean / 9.80665
            logger.info(
                f"[Day1Score] Miner {miner_hotkey}: z500 UNIT CHECK - If geopotential (m²/s²): miner_mean/g={miner_ratio:.1f}m, truth_mean/g={truth_ratio:.1f}m"
            )

            if miner_mean < 10000:
                logger.warning(
                    f"[Day1Score] Miner {miner_hotkey}: POTENTIAL UNIT MISMATCH: z500 mean ({miner_mean:.1f}) suggests geopotential height (m) rather than geopotential (m²/s²)"
                )
            elif truth_mean > 40000 and miner_mean > 40000:
                logger.info(
                    f"[Day1Score] Unit check OK: Both miner and truth z500 appear to be geopotential (m²/s²)"
                )

        elif var_name == "2t":
            if miner_mean < 200 or miner_mean > 350:
                logger.warning(
                    f"[Day1Score] Miner {miner_hotkey}: POTENTIAL UNIT ISSUE: 2t mean ({miner_mean:.1f}) outside expected range for Kelvin"
                )

        elif var_name == "msl":
            if miner_mean < 50000 or miner_mean > 150000:
                logger.warning(
                    f"[Day1Score] Miner {miner_hotkey}: POTENTIAL UNIT ISSUE: msl mean ({miner_mean:.1f}) outside expected range for Pa"
                )

        # Apply automatic unit conversions
        if var_name == "z" and miner_mean < 10000 and truth_mean > 40000:
            logger.warning(
                f"[Day1Score] Miner {miner_hotkey}: AUTOMATIC UNIT CONVERSION: Converting miner z from geopotential height (m) to geopotential (m²/s²)"
            )
            miner_var_da_unaligned = miner_var_da_unaligned * 9.80665
            miner_var_da_unaligned.attrs["units"] = "m2 s-2"
            miner_var_da_unaligned.attrs["long_name"] = (
                "Geopotential (auto-converted from height)"
            )
            logger.info(
                f"[Day1Score] Miner {miner_hotkey}: After conversion: z range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}"
            )

        elif var_name in ["2t", "t"] and miner_mean < 100 and truth_mean > 200:
            logger.warning(
                f"[Day1Score] Miner {miner_hotkey}: AUTOMATIC UNIT CONVERSION: Converting miner {var_name} from Celsius to Kelvin"
            )
            miner_var_da_unaligned = miner_var_da_unaligned + 273.15
            miner_var_da_unaligned.attrs["units"] = "K"
            miner_var_da_unaligned.attrs["long_name"] = (
                f'{miner_var_da_unaligned.attrs.get("long_name", var_name)} (auto-converted from Celsius)'
            )
            logger.info(
                f"[Day1Score] Miner {miner_hotkey}: After conversion: {var_name} range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}"
            )

        elif var_name == "msl" and miner_mean < 2000 and truth_mean > 50000:
            logger.warning(
                f"[Day1Score] Miner {miner_hotkey}: AUTOMATIC UNIT CONVERSION: Converting miner msl from hPa to Pa"
            )
            miner_var_da_unaligned = miner_var_da_unaligned * 100.0
            miner_var_da_unaligned.attrs["units"] = "Pa"
            miner_var_da_unaligned.attrs["long_name"] = (
                "Mean sea level pressure (auto-converted from hPa)"
            )
            logger.info(
                f"[Day1Score] Miner {miner_hotkey}: After conversion: msl range=[{float(miner_var_da_unaligned.min()):.1f}, {float(miner_var_da_unaligned.max()):.1f}], mean={float(miner_var_da_unaligned.mean()):.1f}"
            )

        # Handle pressure level selection
        if var_level:
            def find_pressure_dim_cached(data_array, dataset_name="dataset"):
                cache_key = (dataset_name, tuple(data_array.dims))
                if cache_key in cached_pressure_dims:
                    return cached_pressure_dims[cache_key]

                for dim_name in ["pressure_level", "lev", "plev", "level"]:
                    if dim_name in data_array.dims:
                        cached_pressure_dims[cache_key] = dim_name
                        return dim_name

                logger.warning(
                    f"No pressure level dimension found in {dataset_name} for {var_key} level {var_level}. Available dims: {data_array.dims}"
                )
                cached_pressure_dims[cache_key] = None
                return None

            miner_pressure_dim = find_pressure_dim_cached(
                miner_var_da_unaligned, "miner"
            )
            truth_pressure_dim = find_pressure_dim_cached(
                truth_var_da_unaligned, "truth"
            )
            ref_pressure_dim = find_pressure_dim_cached(
                ref_var_da_unaligned, "reference"
            )

            if not all([miner_pressure_dim, truth_pressure_dim, ref_pressure_dim]):
                logger.warning(
                    f"Missing pressure dimensions for {var_key} level {var_level}. Skipping."
                )
                result["status"] = "skipped"
                result["qc_failure_reason"] = "Missing pressure dimensions"
                return result

            # Ensure pressure coordinates are monotonic along each array before nearest selection
            try:
                miner_var_da_unaligned = miner_var_da_unaligned.sortby(miner_pressure_dim)
            except Exception:
                pass
            try:
                truth_var_da_unaligned = truth_var_da_unaligned.sortby(truth_pressure_dim)
            except Exception:
                pass
            try:
                ref_var_da_unaligned = ref_var_da_unaligned.sortby(ref_pressure_dim)
            except Exception:
                pass

            if var_level == "all":
                miner_var_da_selected = miner_var_da_unaligned
                truth_var_da_selected = truth_var_da_unaligned  
                ref_var_da_selected = ref_var_da_unaligned
                logger.info(f"Scoring {var_key} across all pressure levels")
            else:
                miner_var_da_selected = miner_var_da_unaligned.sel(
                    {miner_pressure_dim: var_level}, method="nearest"
                )
                truth_var_da_selected = truth_var_da_unaligned.sel(
                    {truth_pressure_dim: var_level}, method="nearest"
                )
                ref_var_da_selected = ref_var_da_unaligned.sel(
                    {ref_pressure_dim: var_level}, method="nearest"
                )

                # Validate level selection
                if var_level != "all":
                    if abs(truth_var_da_selected[truth_pressure_dim].item() - var_level) > 10:
                        logger.warning(
                            f"Truth data for {var_key} level {var_level} too far ({truth_var_da_selected[truth_pressure_dim].item()}). Skipping."
                        )
                        result["status"] = "skipped"
                        result["qc_failure_reason"] = "Truth data level too far from target"
                        return result
                    if abs(miner_var_da_selected[miner_pressure_dim].item() - var_level) > 10:
                        logger.warning(
                            f"Miner data for {var_key} level {var_level} too far ({miner_var_da_selected[miner_pressure_dim].item()}). Skipping."
                        )
                        result["status"] = "skipped"
                        result["qc_failure_reason"] = "Miner data level too far from target"
                        return result
                    if abs(ref_var_da_selected[ref_pressure_dim].item() - var_level) > 10:
                        logger.warning(
                            f"GFS Ref data for {var_key} level {var_level} too far ({ref_var_da_selected[ref_pressure_dim].item()}). Skipping."
                        )
                        result["status"] = "skipped"
                        result["qc_failure_reason"] = "Reference data level too far from target"
                        return result
        else:
            miner_var_da_selected = miner_var_da_unaligned
            truth_var_da_selected = truth_var_da_unaligned
            ref_var_da_selected = ref_var_da_unaligned

        # Continue with the same processing logic as the original function
        # (spatial standardization, interpolation, metrics calculation, etc.)
        # This is a large block of code that should be identical to the original function
        # from line ~935 onwards in _process_single_variable_parallel
        
        target_grid_da = truth_var_da_selected
        temp_spatial_dims = [
            d
            for d in target_grid_da.dims
            if d.lower() in ("latitude", "longitude", "lat", "lon")
        ]
        actual_lat_dim_in_target_for_ordering = next(
            (d for d in temp_spatial_dims if d.lower() in ("latitude", "lat")), None
        )

        if actual_lat_dim_in_target_for_ordering:
            lat_coord_values = target_grid_da[
                actual_lat_dim_in_target_for_ordering
            ].values
            if len(lat_coord_values) > 1 and lat_coord_values[0] < lat_coord_values[-1]:
                logger.info(
                    f"[Day1Score] Latitude coordinate '{actual_lat_dim_in_target_for_ordering}' in target_grid_da is ascending. Flipping to descending order for consistency."
                )
                target_grid_da = target_grid_da.isel(
                    {actual_lat_dim_in_target_for_ordering: slice(None, None, -1)}
                )
            else:
                logger.debug(
                    f"[Day1Score] Latitude coordinate '{actual_lat_dim_in_target_for_ordering}' in target_grid_da is already descending or has too few points to determine order."
                )
        else:
            logger.warning(
                "[Day1Score] Could not determine latitude dimension in target_grid_da to check/ensure descending order."
            )

        def _standardize_spatial_dims(data_array: xr.DataArray) -> xr.DataArray:
            if not isinstance(data_array, xr.DataArray):
                return data_array
            rename_dict = {}
            for dim_name in data_array.dims:
                if dim_name.lower() in ("latitude", "lat_0"):
                    rename_dict[dim_name] = "lat"
                elif dim_name.lower() in ("longitude", "lon_0"):
                    rename_dict[dim_name] = "lon"
            if rename_dict:
                logger.debug(
                    f"[Day1Score] Standardizing spatial dims for variable {var_key}: Renaming {rename_dict}"
                )
                return data_array.rename(rename_dict)
            return data_array

        target_grid_da_std = _standardize_spatial_dims(target_grid_da)
        truth_var_da_final = target_grid_da_std

        # Interpolate to common grid
        miner_var_da_aligned = await asyncio.to_thread(
            lambda: _standardize_spatial_dims(miner_var_da_selected).interp_like(
                target_grid_da_std, method="linear"
            )
        )

        ref_var_da_aligned = await asyncio.to_thread(
            lambda: _standardize_spatial_dims(ref_var_da_selected).interp_like(
                target_grid_da_std, method="linear"
            )
        )

        # Continue with the remaining processing logic from the original function
        # Calculate latitude weights and perform metrics calculations
        
        broadcasted_weights_final = None
        common_dims = set(truth_var_da_final.dims) & set(miner_var_da_aligned.dims) & set(ref_var_da_aligned.dims)
        
        # Remove duplicate pressure level dimensions if they exist
        pressure_dims_in_common = [d for d in common_dims if d in ("pressure_level", "plev", "lev", "level")]
        if len(pressure_dims_in_common) > 1:
            common_dims = common_dims - set(pressure_dims_in_common) | {"pressure_level"}
        
        spatial_dims_for_metric = [d for d in common_dims if d not in ("time",)]

        actual_lat_dim_in_target = "lat" if "lat" in truth_var_da_final.dims else None

        if actual_lat_dim_in_target:
            try:
                target_lat_coord = truth_var_da_final[actual_lat_dim_in_target]
                one_d_lat_weights_target = _calculate_latitude_weights(target_lat_coord)
                
                # Create a template with only the spatial dimensions that will be used in MSE
                template_dims = {dim: truth_var_da_final.sizes[dim] for dim in spatial_dims_for_metric}
                template_coords = {dim: truth_var_da_final.coords[dim] for dim in spatial_dims_for_metric if dim in truth_var_da_final.coords}
                template_da = xr.DataArray(
                    data=np.ones([template_dims[dim] for dim in spatial_dims_for_metric]),
                    dims=spatial_dims_for_metric,
                    coords=template_coords
                )
                
                _, broadcasted_weights_final = xr.broadcast(template_da, one_d_lat_weights_target)
            except Exception as e_broadcast_weights:
                logger.error(f"[Day1Score] Failed to create latitude weights for {var_key}: {e_broadcast_weights}")
                broadcasted_weights_final = None

        # Calculate clone distance MSE
        mse_result = xs.mse(
            miner_var_da_aligned,
            ref_var_da_aligned,
            dim=spatial_dims_for_metric,
            weights=broadcasted_weights_final,
            skipna=True,
        ).compute()
        
        # Ensure we get a scalar value - if there are remaining dimensions, take the mean
        if mse_result.size == 1:
            clone_distance_mse_val = mse_result.item()
        else:
            # If there are still dimensions remaining, reduce them to get a scalar
            clone_distance_mse_val = float(mse_result.mean().values)
            
        result["clone_distance_mse"] = clone_distance_mse_val

        # Clone penalty calculation
        delta_thresholds_config = day1_scoring_config.get("clone_delta_thresholds", {})
        delta_for_var = delta_thresholds_config.get(var_key)
        clone_penalty = 0.0

        if delta_for_var is not None and clone_distance_mse_val < delta_for_var:
            gamma = day1_scoring_config.get("clone_penalty_gamma", 1.0)
            clone_penalty = gamma * (1.0 - (clone_distance_mse_val / delta_for_var))
            clone_penalty = max(0.0, clone_penalty)
            result["qc_failure_reason"] = f"Clone penalty triggered for {var_key}"
        result["clone_penalty_applied"] = clone_penalty

        # Simplified climatology processing for now - use fallback computation
        clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
        clim_hour = valid_time_dt.hour
        clim_hour_rounded = (clim_hour // 6) * 6

        clim_var_da_raw = era5_climatology[standard_name_for_clim].sel(
            dayofyear=clim_dayofyear, hour=clim_hour_rounded, method="nearest"
        )
        clim_var_to_interpolate = _standardize_spatial_dims(clim_var_da_raw)

        if var_level and var_level != "all":
            for dim_name in ["pressure_level", "lev", "plev", "level"]:
                if dim_name in clim_var_to_interpolate.dims:
                    # Ensure var_level is numeric for pressure level selection
                    try:
                        numeric_level = float(var_level)
                        clim_var_to_interpolate = clim_var_to_interpolate.sel(
                            **{dim_name: numeric_level}, method="nearest"
                        )
                        break
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert var_level {var_level} to numeric for {var_key}")
                        break

        clim_var_da_aligned = await asyncio.to_thread(
            lambda: clim_var_to_interpolate.interp_like(truth_var_da_final, method="linear")
        )

        # Sanity checks
        sanity_results = await perform_sanity_checks(
            forecast_da=miner_var_da_aligned,
            reference_da_for_corr=ref_var_da_aligned,
            variable_name=var_key,
            climatology_bounds_config=day1_scoring_config.get("climatology_bounds", {}),
            pattern_corr_threshold=day1_scoring_config.get("pattern_correlation_threshold", 0.3),
            lat_weights=broadcasted_weights_final,
        )
        result["sanity_checks"] = sanity_results

        if not sanity_results.get("climatology_passed") or not sanity_results.get("pattern_correlation_passed"):
            result["status"] = "skipped"
            result["qc_failure_reason"] = f"Sanity check failed for {var_key}"
            return result

        # Bias correction and metrics calculation
        forecast_bc_da = await calculate_bias_corrected_forecast(miner_var_da_aligned, truth_var_da_final)

        # Calculate skill score and ACC
        skill_score = await calculate_mse_skill_score(
            forecast_bc_da, truth_var_da_final, ref_var_da_aligned, broadcasted_weights_final
        )
        skill_score_after_penalty = skill_score - clone_penalty
        result["skill_score"] = skill_score_after_penalty

        acc_score = await calculate_acc(
            miner_var_da_aligned, truth_var_da_final, clim_var_da_aligned, broadcasted_weights_final
        )
        result["acc_score"] = acc_score

        result["status"] = "success"
        
        logger.debug(
            f"[Day1Score] Miner {miner_hotkey}: PARALLEL Variable (preloaded) {var_key} scored successfully"
        )

        return result

    except Exception as e_var:
        import traceback
        tb_str = traceback.format_exc()
        error_msg = str(e_var)
        
        logger.error(
            f"[Day1Score] Miner {miner_hotkey}: Error in parallel preloaded scoring {var_key} at {valid_time_dt}:\n"
            f"Error: {error_msg}\n"
            f"Variable config: {var_config}\n"
            f"Full traceback:\n{tb_str}".replace("{", "{{").replace("}", "}}"),
            exc_info=True,
        )
        result["status"] = "error"
        result["error_message"] = error_msg
        return result


async def precompute_climatology_cache(
    era5_climatology: xr.Dataset,
    day1_scoring_config: Dict,
    times_to_evaluate: List[datetime],
    sample_target_grid: xr.DataArray,
) -> Dict[str, xr.DataArray]:
    """
    OPTIMIZATION 1: Pre-compute ALL climatology interpolations once per scoring run.
    This eliminates the massive redundant interpolation overhead per miner.
    Expected 50-200x speedup (250-1000 seconds → 5-20 seconds).

    Enhanced with MULTIPROCESSING for CPU-intensive interpolation operations.
    Expected additional 2-4x speedup from parallel processing.
    """
    cache = {}
    variables_to_score = day1_scoring_config.get("variables_levels_to_score", [])

    # Validate inputs
    if not variables_to_score:
        logger.warning(
            "⚠️ No variables found in day1_scoring_config['variables_levels_to_score'] - cache will be empty"
        )
        return cache

    if not times_to_evaluate:
        logger.warning(
            "⚠️ No times provided for climatology cache - cache will be empty"
        )
        return cache

    total_computations = len(variables_to_score) * len(times_to_evaluate)

    # SMART ASYNC PROCESSING: Only use async processing when workload justifies the overhead
    # Break-even point is around 10+ computations for async processing benefits
    async_threshold = int(
        os.getenv("GAIA_ASYNC_THRESHOLD", "10")
    )  # Configurable threshold
    use_async_processing = (
        total_computations >= async_threshold
        and not os.getenv("GAIA_DISABLE_ASYNC", "false").lower() == "true"
    )

    if use_async_processing:
        logger.info(
            f"🚀 ASYNC PROCESSED climatology cache for {len(variables_to_score)} variables × {len(times_to_evaluate)} times = {total_computations} total computations"
        )
        logger.info(
            f"   Async processing enabled (computations: {total_computations} >= threshold: {async_threshold})"
        )
    else:
        logger.info(
            f"⚡ SEQUENTIAL climatology cache for {len(variables_to_score)} variables × {len(times_to_evaluate)} times = {total_computations} total computations"
        )
        logger.info(
            f"   Async processing disabled (computations: {total_computations} < threshold: {async_threshold} or explicitly disabled)"
        )

    cache_start = time.time()

    def _standardize_spatial_dims_cache(data_array: xr.DataArray) -> xr.DataArray:
        """Local copy of standardization function for cache computation."""
        if not isinstance(data_array, xr.DataArray):
            return data_array
        rename_dict = {}
        for dim_name in data_array.dims:
            if dim_name.lower() in ("latitude", "lat_0"):
                rename_dict[dim_name] = "lat"
            elif dim_name.lower() in ("longitude", "lon_0"):
                rename_dict[dim_name] = "lon"
        if rename_dict:
            return data_array.rename(rename_dict)
        return data_array

    if use_async_processing:
        # Import async processing functions only when needed
        from .utils.async_processing import compute_climatology_interpolation_async

        # ASYNC PROCESSING PATH: Prepare all interpolation tasks for parallel execution
        interpolation_tasks = []
        task_metadata = []
    else:
        # SEQUENTIAL PATH: No async processing needed
        pass

    for valid_time_dt in times_to_evaluate:
        for var_config in variables_to_score:
            var_name = var_config["name"]
            var_level = var_config.get("level")
            standard_name_for_clim = var_config.get("standard_name", var_name)
            var_key = f"{var_name}{var_level}" if var_level and var_level != "all" else var_name

            cache_key = f"{var_name}_{var_level}_{valid_time_dt.isoformat()}"

            try:
                # Extract climatology data for this time
                clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
                clim_hour = valid_time_dt.hour
                clim_hour_rounded = (clim_hour // 6) * 6

                clim_var_da_raw = era5_climatology[standard_name_for_clim].sel(
                    dayofyear=clim_dayofyear, hour=clim_hour_rounded, method="nearest"
                )

                # Standardize spatial dimensions
                clim_var_to_interpolate = _standardize_spatial_dims_cache(
                    clim_var_da_raw
                )

                # Handle pressure levels if needed
                if var_level:
                    clim_pressure_dim = None
                    for dim_name in ["pressure_level", "lev", "plev", "level"]:
                        if dim_name in clim_var_to_interpolate.dims:
                            clim_pressure_dim = dim_name
                            break

                    if clim_pressure_dim:
                        clim_var_to_interpolate = clim_var_to_interpolate.sel(
                            **{clim_pressure_dim: var_level}, method="nearest"
                        )
                        actual_level = clim_var_to_interpolate[clim_pressure_dim].item()
                        if abs(actual_level - var_level) > 10:
                            logger.warning(
                                f"[CachePrecompute] Climatology for {var_key} at target level {var_level} found at {actual_level}"
                            )

                if use_async_processing:
                    # ASYNC PROCESSING PATH: Prepare data for parallel execution
                    # Convert to numpy for async processing
                    source_data = clim_var_to_interpolate.values
                    source_lat = clim_var_to_interpolate.lat.values
                    source_lon = clim_var_to_interpolate.lon.values
                    target_lat = sample_target_grid.lat.values
                    target_lon = sample_target_grid.lon.values

                    # Create async task for parallel execution
                    task = compute_climatology_interpolation_async(
                        source_data,
                        (source_lat, source_lon),
                        (target_lat, target_lon),
                        var_key,
                        "linear",
                    )

                    interpolation_tasks.append(task)
                    task_metadata.append(
                        {
                            "cache_key": cache_key,
                            "var_key": var_key,
                            "target_lat": target_lat,
                            "target_lon": target_lon,
                            "clim_coords": clim_var_to_interpolate.coords,
                        }
                    )
                else:
                    # SEQUENTIAL PATH: Do interpolation immediately
                    try:
                        clim_var_da_aligned = clim_var_to_interpolate.interp_like(
                            sample_target_grid, method="linear"
                        )
                        cache[cache_key] = clim_var_da_aligned
                        logger.debug(
                            f"[CachePrecompute] Sequential: Successfully cached {cache_key}"
                        )
                    except Exception as e:
                        logger.error(
                            f"[CachePrecompute] Sequential error for {cache_key}: {e}"
                        )
                        continue

            except Exception as e:
                logger.error(
                    f"[CachePrecompute] Error preparing climatology task for {cache_key}: {e}"
                )
                continue

    # Execute tasks based on chosen method
    if use_async_processing and interpolation_tasks:
        # ASYNC PROCESSING EXECUTION
        logger.info(
            f"[ClimatologyCache] Executing {len(interpolation_tasks)} interpolation tasks with async processing..."
        )
        parallel_start = time.time()

        try:
            interpolation_results = await asyncio.gather(
                *interpolation_tasks, return_exceptions=True
            )
            parallel_time = time.time() - parallel_start
            logger.info(
                f"[ClimatologyCache] Async parallel interpolation completed in {parallel_time:.2f}s"
            )

            # Process results and build cache
            successful_computations = 0
            for i, result in enumerate(interpolation_results):
                if isinstance(result, Exception):
                    logger.error(
                        f"[CachePrecompute] Parallel task {i} failed: {result}"
                    )
                    continue

                if result is None:
                    logger.warning(f"[CachePrecompute] Parallel task {i} returned None")
                    continue

                # Reconstruct xarray DataArray from numpy result
                metadata = task_metadata[i]
                cache_key = metadata["cache_key"]
                target_lat = metadata["target_lat"]
                target_lon = metadata["target_lon"]
                original_coords = metadata["clim_coords"]

                try:
                    # Create new DataArray with interpolated data
                    interpolated_da = xr.DataArray(
                        result,
                        dims=["lat", "lon"],
                        coords={"lat": target_lat, "lon": target_lon},
                        attrs=(
                            dict(original_coords.get("lat", {}).attrs)
                            if "lat" in original_coords
                            else {}
                        ),
                    )

                    cache[cache_key] = interpolated_da
                    successful_computations += 1
                    logger.debug(f"[CachePrecompute] Successfully cached {cache_key}")

                except Exception as e:
                    logger.error(
                        f"[CachePrecompute] Error reconstructing DataArray for {cache_key}: {e}"
                    )
                    continue

            cache_time = time.time() - cache_start
            cache_size = len(cache)
            expected_individual_ops = len(variables_to_score) * len(times_to_evaluate)

            logger.info(
                f"✅ ASYNC PROCESSED climatology cache pre-computation completed!"
            )
            logger.info(
                f"   - Cache size: {cache_size} entries ({successful_computations} successful)"
            )
            logger.info(f"   - Total computation time: {cache_time:.2f} seconds")
            logger.info(
                f"   - Async parallel execution time: {parallel_time:.2f} seconds"
            )

            # Calculate speedup
            if cache_size > 0 and expected_individual_ops > 0:
                speedup = expected_individual_ops / cache_size
                efficiency = (cache_size / expected_individual_ops) * 100
                async_benefit = cache_time / parallel_time if parallel_time > 0 else 1.0
                logger.info(
                    f"   - Expected miner speedup: {speedup:.1f}x faster per miner"
                )
                logger.info(
                    f"   - Async processing benefit: {async_benefit:.1f}x faster than sequential"
                )
                logger.info(
                    f"   - Cache efficiency: {efficiency:.1f}% (higher is better)"
                )

            if cache_size == 0:
                logger.warning(
                    "⚠️ Climatology cache is empty - will fallback to individual computation"
                )
            elif cache_size < expected_individual_ops * 0.8:
                logger.warning(
                    f"⚠️ Climatology cache incomplete ({cache_size}/{expected_individual_ops}) - some operations will fallback"
                )

        except Exception as e:
            logger.error(
                f"[CachePrecompute] Error in async parallel interpolation execution: {e}"
            )
            logger.warning("Falling back to sequential climatology computation...")

            # Fallback to original sequential method
            for valid_time_dt in times_to_evaluate:
                for var_config in variables_to_score:
                    var_name = var_config["name"]
                    var_level = var_config.get("level")
                    standard_name_for_clim = var_config.get("standard_name", var_name)
                    cache_key = f"{var_name}_{var_level}_{valid_time_dt.isoformat()}"

                    try:
                        clim_dayofyear = pd.Timestamp(valid_time_dt).dayofyear
                        clim_hour = valid_time_dt.hour
                        clim_hour_rounded = (clim_hour // 6) * 6

                        try:
                            clim_var_da_raw = era5_climatology[standard_name_for_clim].sel(
                                dayofyear=clim_dayofyear,
                                hour=clim_hour_rounded,
                                method="nearest",
                            )
                        except (TypeError, ValueError) as sel_error:
                            logger.error(
                                f"[CachePrecompute] Fallback climatology selection failed for {standard_name_for_clim}: {sel_error}\n"
                                f"dayofyear: {clim_dayofyear} (type: {type(clim_dayofyear)})\n"
                                f"hour: {clim_hour_rounded} (type: {type(clim_hour_rounded)})"
                            )
                            # Try alternative selection without method parameter
                            clim_var_da_raw = era5_climatology[standard_name_for_clim].isel(
                                dayofyear=min(int(clim_dayofyear) - 1, len(era5_climatology[standard_name_for_clim].coords.get('dayofyear', [])) - 1),
                                hour=min(int(clim_hour_rounded) // 6, len(era5_climatology[standard_name_for_clim].coords.get('hour', [])) - 1)
                            )

                        clim_var_to_interpolate = _standardize_spatial_dims_cache(
                            clim_var_da_raw
                        )

                        if var_level:
                            clim_pressure_dim = None
                            for dim_name in ["pressure_level", "plev", "level"]:
                                if dim_name in clim_var_to_interpolate.dims:
                                    clim_pressure_dim = dim_name
                                    break

                            if clim_pressure_dim:
                                clim_var_to_interpolate = clim_var_to_interpolate.sel(
                                    **{clim_pressure_dim: var_level}, method="nearest"
                                )

                        # Sequential fallback interpolation
                        clim_var_da_aligned = await asyncio.to_thread(
                            lambda: clim_var_to_interpolate.interp_like(
                                sample_target_grid, method="linear"
                            )
                        )

                        cache[cache_key] = clim_var_da_aligned

                    except Exception as e:
                        logger.error(
                            f"[CachePrecompute] Fallback error for {cache_key}: {e}"
                        )
                        continue

            fallback_time = time.time() - cache_start
            logger.info(
                f"✅ Fallback climatology cache completed in {fallback_time:.2f} seconds"
            )
    else:
        # SEQUENTIAL EXECUTION (no async processing)
        # Cache was already built during the loop above when use_async_processing=False
        cache_time = time.time() - cache_start
        cache_size = len(cache)
        expected_individual_ops = len(variables_to_score) * len(times_to_evaluate)

        logger.info(f"✅ SEQUENTIAL climatology cache pre-computation completed!")
        logger.info(f"   - Cache size: {cache_size} entries")
        logger.info(f"   - Total computation time: {cache_time:.2f} seconds")

        if cache_size == 0:
            logger.warning(
                "⚠️ Climatology cache is empty - will fallback to individual computation"
            )
        elif cache_size < expected_individual_ops * 0.8:
            logger.warning(
                f"⚠️ Climatology cache incomplete ({cache_size}/{expected_individual_ops}) - some operations will fallback"
            )

    return cache


async def _preload_all_time_slices(
    gfs_analysis_data: xr.Dataset,
    gfs_reference_data: xr.Dataset,
    miner_forecast_ds: xr.Dataset,
    times_to_evaluate: List[datetime],
    miner_hotkey: str,
) -> Dict[str, xr.Dataset]:
    """
    Pre-load all time slices at once to reduce I/O overhead.

    OPTIMIZATION: Bulk time slice loading (15-25% speedup)
    Expected benefit: Reduces repeated dataset access overhead
    """
    logger.info(
    f"[Day1Score] Miner {miner_hotkey}: Pre-loading all {len(times_to_evaluate)} time slices to reduce HTTP requests"
)
    preload_start = time.time()

    try:
        # Convert times to numpy datetime64 for selection
        time_coords = [
            np.datetime64(dt.replace(tzinfo=None)) for dt in times_to_evaluate
        ]

        # Load all time slices at once - much more efficient than individual selections
        gfs_analysis_slices = await asyncio.to_thread(
            lambda: gfs_analysis_data.sel(time=time_coords, method="nearest")
        )
        gfs_reference_slices = await asyncio.to_thread(
            lambda: gfs_reference_data.sel(time=time_coords, method="nearest")
        )

        # Handle miner forecast with timezone-aware selection
        time_dtype_str = str(miner_forecast_ds.time.dtype)
        if "datetime64" in time_dtype_str and "UTC" in time_dtype_str:
            # Use pandas timestamps for timezone-aware data
            miner_time_coords = [
                (
                    pd.Timestamp(dt).tz_localize("UTC")
                    if pd.Timestamp(dt).tzinfo is None
                    else pd.Timestamp(dt).tz_convert("UTC")
                )
                for dt in times_to_evaluate
            ]
        else:
            miner_time_coords = time_coords

        miner_forecast_slices = await asyncio.to_thread(
            lambda: miner_forecast_ds.sel(time=miner_time_coords, method="nearest")
        )

        preload_time = time.time() - preload_start
        logger.info(
            f"[Day1Score] Miner {miner_hotkey}: Pre-loaded all time slices in {preload_time:.2f}s"
        )

        return {
            "gfs_analysis": gfs_analysis_slices,
            "gfs_reference": gfs_reference_slices,
            "miner_forecast": miner_forecast_slices,
        }

    except Exception as e:
        logger.warning(
            f"[Day1Score] Miner {miner_hotkey}: Failed to pre-load time slices: {e}. Falling back to individual selection."
        )
        return None


async def _async_dataset_select(dataset, time_coord, method="nearest"):
    """
    Perform dataset time selection asynchronously to prevent I/O blocking.

    OPTIMIZATION: Async I/O for dataset operations (10-20% speedup)
    """
    return await asyncio.to_thread(lambda: dataset.sel(time=time_coord, method=method))


async def _process_single_timestep_sequential(
    valid_time_dt: datetime,
    effective_lead_h: int,
    variables_to_score: List[Dict],
    miner_forecast_ds: xr.Dataset,
    gfs_analysis_data_for_run: xr.Dataset,
    gfs_reference_forecast_for_run: xr.Dataset,
    era5_climatology: xr.Dataset,
    precomputed_climatology_cache: Optional[Dict],
    day1_scoring_config: Dict,
    run_gfs_init_time: datetime,
    miner_hotkey: str,
    zarr_store_url: str,
    claimed_manifest_content_hash: str,
    access_token: str,
    job_id: str,
    frozen_manifest_json: Optional[Dict],
) -> Dict:
    """
    Process a single time step with all its variables in parallel.

    This enables both time step AND variable parallelization for maximum performance.
    Expected additional speedup: 2x for 2 time steps (5s → 2.5s)

    Returns:
        Dictionary with time step results and variable scores
    """
    time_key_for_results = effective_lead_h
    timestep_results = {
        "time_key": time_key_for_results,
        "valid_time_dt": valid_time_dt,
        "effective_lead_h": effective_lead_h,
        "variables": {},
        "aggregated_skill_scores": [],
        "aggregated_acc_scores": [],
        "qc_passed": True,
        "error_message": None,
        "skip_reason": None,
    }

    try:
        logger.info(
            f"[Day1Score] Miner {miner_hotkey}: PARALLEL processing time step {valid_time_dt} (Lead: {effective_lead_h}h)"
        )
        timestep_start = time.time()

        # Time data selection (this is the expensive I/O part)
        # Deduplicate time indices to avoid reindexing errors during selection
        try:
            def _dedup_time_inline(ds: xr.Dataset, label: str) -> xr.Dataset:
                try:
                    if hasattr(ds, "time") and "time" in ds.coords:
                        arr = np.array(ds.time.values)
                        if len(arr) > len(np.unique(arr)):
                            logger.debug(f"[Day1Score] Removing duplicate time indices from {label}")
                            _, unique_idx = np.unique(arr, return_index=True)
                            ds = ds.isel(time=sorted(unique_idx))
                except Exception:
                    pass
                return ds
            gfs_analysis_data_for_run = _dedup_time_inline(gfs_analysis_data_for_run, "gfs_analysis")
            gfs_reference_forecast_for_run = _dedup_time_inline(gfs_reference_forecast_for_run, "gfs_reference")
            miner_forecast_ds = _dedup_time_inline(miner_forecast_ds, "miner_forecast")
        except Exception:
            pass

        valid_time_np = np.datetime64(valid_time_dt.replace(tzinfo=None))

        try:
            # Log available timesteps for debugging
            if effective_lead_h == 6:  # Only log once to avoid spam
                analysis_times = gfs_analysis_data_for_run.time.values[:10] if hasattr(gfs_analysis_data_for_run, 'time') else []
                logger.info(
                    f"[Day1Score] Available GFS analysis timesteps (first 10): {analysis_times}"
                )
                logger.info(
                    f"[Day1Score] Trying to select time: {valid_time_np} for lead {effective_lead_h}h"
                )
            
            gfs_analysis_lead = gfs_analysis_data_for_run.sel(
                time=valid_time_np, method="nearest"
            )
            gfs_reference_lead = gfs_reference_forecast_for_run.sel(
                time=valid_time_np, method="nearest"
            )

            selected_time_gfs_analysis = np.datetime64(
                gfs_analysis_lead.time.data.item(), "ns"
            )
            # GFS data comes in 6-hour intervals, so we should accept up to 3 hours difference
            # This handles cases where the exact timestep might not be available
            if abs(selected_time_gfs_analysis - valid_time_np) > np.timedelta64(3, "h"):
                timestep_results["skip_reason"] = (
                    f"GFS Analysis time {selected_time_gfs_analysis} too far from target {valid_time_np} (>3h difference)"
                )
                return timestep_results

            selected_time_gfs_reference = np.datetime64(
                gfs_reference_lead.time.data.item(), "ns"
            )
            # GFS data comes in 6-hour intervals, so we should accept up to 3 hours difference
            # This handles cases where the exact timestep might not be available
            if abs(selected_time_gfs_reference - valid_time_np) > np.timedelta64(
                3, "h"
            ):
                timestep_results["skip_reason"] = (
                    f"GFS Reference time {selected_time_gfs_reference} too far from target {valid_time_np} (>3h difference)"
                )
                return timestep_results

        except Exception as e_sel:
            timestep_results["error_message"] = (
                f"Could not select GFS data for lead {effective_lead_h}h: {e_sel}"
            )
            return timestep_results

        # Handle miner forecast time selection with proper timezone handling
        valid_time_dt_aware = (
            pd.Timestamp(valid_time_dt).tz_localize("UTC")
            if pd.Timestamp(valid_time_dt).tzinfo is None
            else pd.Timestamp(valid_time_dt).tz_convert("UTC")
        )
        valid_time_np_ns = np.datetime64(valid_time_dt_aware.replace(tzinfo=None), "ns")
        selection_label_for_miner = valid_time_np_ns

        # Handle timezone-aware datetime dtypes properly
        time_dtype_str = str(miner_forecast_ds.time.dtype)
        is_integer_time = False
        is_timezone_aware = False
        try:
            if "datetime64" in time_dtype_str and "UTC" in time_dtype_str:
                is_integer_time = False
                is_timezone_aware = True
                selection_label_for_miner = valid_time_dt_aware
            else:
                is_integer_time = np.issubdtype(
                    miner_forecast_ds.time.dtype, np.integer
                )
        except TypeError:
            is_integer_time = False

        if is_integer_time:
            try:
                selection_label_for_miner = valid_time_np_ns.astype(np.int64)
            except Exception as e_cast:
                timestep_results["error_message"] = (
                    f"Failed to cast datetime to int64: {e_cast}"
                )
                return timestep_results

        try:
            miner_forecast_lead = miner_forecast_ds.sel(
                time=selection_label_for_miner, method="nearest"
            )
        except Exception as e_sel_miner:
            timestep_results["error_message"] = (
                f"Error selecting from miner_forecast_ds: {e_sel_miner}"
            )
            return timestep_results

        # Validate time selection using robust ns conversion with default tolerance
        miner_time_value_from_sel = miner_forecast_lead.time.item()
        time_diff_too_large = False

        def _as_ns_int(val) -> Optional[int]:
            try:
                import numpy as _np
                import pandas as _pd
                # pandas Timestamp
                if hasattr(val, "value"):
                    return int(_pd.Timestamp(val).value)
                # numpy datetime64
                if isinstance(val, _np.datetime64):
                    return int(val.astype("datetime64[ns]").astype(_np.int64))
                # int-like
                if isinstance(val, (int, _np.integer)):
                    return int(val)
                # string or datetime
                return int(_pd.Timestamp(val).value)
            except Exception:
                return None

        try:
            max_time_dev_hours_cfg = float(day1_scoring_config.get("max_time_deviation_hours", 0))
        except Exception:
            max_time_dev_hours_cfg = 0.0
        # Default tolerance: use configured value; otherwise allow up to the lead hours (at least 3h)
        default_hours = max_time_dev_hours_cfg if max_time_dev_hours_cfg > 0 else float(max(3, effective_lead_h))
        allowed_ns = int(default_hours * 3600 * 1e9)

        # Normalize target label for miner comparison
        if is_integer_time:
            target_ns = _as_ns_int(selection_label_for_miner)
        else:
            if is_timezone_aware:
                try:
                    _ts = selection_label_for_miner.tz_convert("UTC").tz_localize(None)
                except Exception:
                    _ts = selection_label_for_miner
                target_ns = _as_ns_int(_ts)
            else:
                target_ns = _as_ns_int(selection_label_for_miner)

        miner_ns = _as_ns_int(miner_time_value_from_sel)

        if miner_ns is None or target_ns is None:
            # If we cannot determine times reliably, mark as error for this timestep
            timestep_results["error_message"] = (
                f"Unable to normalize times for comparison (miner={miner_time_value_from_sel}, target={selection_label_for_miner})"
            )
            return timestep_results

        if abs(miner_ns - target_ns) > allowed_ns:
            time_diff_too_large = True

        if time_diff_too_large:
            timestep_results["skip_reason"] = (
                f"Miner forecast time {miner_ns} too far from target {pd.to_datetime(target_ns, unit='ns', utc=True)}"
            )
            return timestep_results

        # PARALLEL VARIABLE PROCESSING within this time step
        logger.info(
            f"[Day1Score] Miner {miner_hotkey}: Starting PARALLEL processing of {len(variables_to_score)} variables at {valid_time_dt}"
        )

        # Cache objects for this time step
        cached_lat_weights = None
        cached_grid_dims = None
        cached_pressure_dims = {}

        # OPTIMIZED CACHING: Since data is pre-loaded, create simple variable references
        # The miner forecast data has already been downloaded and cached in memory
        logger.debug(f"[Day1Score] Creating variable references for {len(variables_to_score)} variables (data pre-loaded)")
        preloaded_variables = {}
        
        try:
            # Create references to pre-loaded variables
            vars_to_cache = [
                var_config["name"] for var_config in variables_to_score
                if var_config["name"] in miner_forecast_lead.data_vars
            ]
            
            if vars_to_cache:
                for var_name in vars_to_cache:
                    try:
                        preloaded_variables[var_name] = {
                            "miner": miner_forecast_lead[var_name],  # Pre-loaded DataArray
                            "truth": gfs_analysis_lead[var_name],    # Local data
                            "ref": gfs_reference_lead[var_name],     # Local data
                        }
                        logger.debug(f"[Day1Score] Cached reference for pre-loaded variable {var_name}")
                    except KeyError:
                        logger.warning(f"[Day1Score] Variable {var_name} not found in datasets")
                        continue
                
                logger.debug(f"[Day1Score] Created references for {len(preloaded_variables)}/{len(vars_to_cache)} pre-loaded variables")
            
        except Exception as cache_err:
            logger.warning(f"[Day1Score] Failed to create variable references: {cache_err}")
            # Fall back to direct dataset access
            preloaded_variables = {}

        # Process variables sequentially for better CPU efficiency
        for j, var_config in enumerate(variables_to_score):
            var_name = var_config["name"]
            var_level = var_config.get("level")
            var_key = f"{var_name}{var_level}" if var_level and var_level != "all" else var_name

            # Initialize result structure for this variable
            timestep_results["variables"][var_key] = {
                "skill_score": None,
                "acc": None,
                "sanity_checks": {},
                "clone_distance_mse": None,
                "clone_penalty_applied": None,
            }

            logger.debug(f"[Day1Score] Miner {miner_hotkey}: Processing variable {j+1}/{len(variables_to_score)}: {var_key}")

            # Use pre-loaded variables if available, otherwise pass datasets
            if var_name in preloaded_variables:
                task_datasets = preloaded_variables[var_name]
            else:
                try:
                    dataset_slice, _ = await open_verified_remote_zarr_variable(
                        zarr_store_url=zarr_store_url,
                        claimed_manifest_content_hash=claimed_manifest_content_hash,
                        miner_hotkey_ss58=miner_hotkey,
                        variable_names=[var_name],
                        storage_options={
                            "headers": {"Authorization": f"Bearer {access_token}"},
                            "ssl": False,
                        },
                        job_id=f"{job_id}_day1_var_{var_name}",
                        frozen_manifest=frozen_manifest_json,
                        return_manifest=True,
                    )
                    if dataset_slice is None or var_name not in dataset_slice.data_vars:
                        logger.warning(f"[Day1Score] Miner {miner_hotkey}: Variable {var_name} not returned by verified slice")
                        continue
                    task_datasets = {
                        "miner": dataset_slice[var_name],
                        "truth": gfs_analysis_lead[var_name],
                        "ref": gfs_reference_lead[var_name],
                    }
                    preloaded_variables[var_name] = task_datasets
                except Exception as var_fetch_err:
                    logger.error(f"[Day1Score] Failed to fetch variable {var_name} on-demand: {var_fetch_err}")
                    continue

            if var_name in preloaded_variables:
                result = await _process_single_variable_parallel_preloaded(
                    var_config=var_config,
                    miner_var_da=task_datasets["miner"],
                    truth_var_da=task_datasets["truth"],
                    ref_var_da=task_datasets["ref"],
                    era5_climatology=era5_climatology,
                    precomputed_climatology_cache=precomputed_climatology_cache,
                    day1_scoring_config=day1_scoring_config,
                    valid_time_dt=valid_time_dt,
                    effective_lead_h=effective_lead_h,
                    miner_hotkey=miner_hotkey,
                    cached_pressure_dims=cached_pressure_dims,
                    cached_lat_weights=cached_lat_weights,
                    cached_grid_dims=cached_grid_dims,
                    variables_to_score=variables_to_score,
                )
            else:
                # Use dataset-based path to ensure coverage of both code pathways
                result = await _process_single_variable_parallel(
                    var_config=var_config,
                    miner_forecast_lead=miner_forecast_lead,
                    gfs_analysis_lead=gfs_analysis_lead,
                    gfs_reference_lead=gfs_reference_lead,
                    era5_climatology=era5_climatology,
                    precomputed_climatology_cache=precomputed_climatology_cache,
                    day1_scoring_config=day1_scoring_config,
                    valid_time_dt=valid_time_dt,
                    effective_lead_h=effective_lead_h,
                    miner_hotkey=miner_hotkey,
                    cached_pressure_dims=cached_pressure_dims,
                    cached_lat_weights=cached_lat_weights,
                    cached_grid_dims=cached_grid_dims,
                    variables_to_score=variables_to_score,
                )

            # Process result immediately
            if isinstance(result, Exception):
                error_msg = str(result)
                logger.error(
                    f"[Day1Score] Miner {miner_hotkey}: Variable processing failed for {var_key} at {valid_time_dt}: {error_msg}",
                    exc_info=result if hasattr(result, '__traceback__') else None
                )
                
                timestep_results["variables"][var_key]["error"] = error_msg
                timestep_results["qc_passed"] = False
                
                # CRITICAL: If this is a coordinate/data structure error, fail fast
                if any(coord_err in error_msg.lower() for coord_err in ['pressure_level', 'coordinate', 'dimension', 'keyerror']):
                    logger.error(
                        f"[Day1Score] Miner {miner_hotkey}: CRITICAL coordinate error for {var_key} - failing entire timestep to prevent cascade failures"
                    )
                    timestep_results["error_message"] = f"Critical coordinate error in {var_key}: {error_msg}"
                    return timestep_results  # Early termination
                
                continue

            if not isinstance(result, dict):
                logger.error(
                    f"[Day1Score] Miner {miner_hotkey}: Invalid result type from parallel task {var_key}: {type(result)}"
                )
                continue

            status = result.get("status")

            if status == "success":
                # Successful variable processing
                timestep_results["variables"][var_key]["skill_score"] = result.get(
                    "skill_score"
                )
                timestep_results["variables"][var_key]["acc"] = result.get("acc_score")
                timestep_results["variables"][var_key]["clone_distance_mse"] = (
                    result.get("clone_distance_mse")
                )
                timestep_results["variables"][var_key]["clone_penalty_applied"] = (
                    result.get("clone_penalty_applied")
                )
                timestep_results["variables"][var_key]["sanity_checks"] = result.get(
                    "sanity_checks", {}
                )

                # Add to aggregated scores for this time step
                skill_score = result.get("skill_score")
                acc_score = result.get("acc_score")
                if skill_score is not None and np.isfinite(skill_score):
                    timestep_results["aggregated_skill_scores"].append(skill_score)
                if acc_score is not None and np.isfinite(acc_score):
                    timestep_results["aggregated_acc_scores"].append(acc_score)

                # Check for QC failures
                qc_failure_reason = result.get("qc_failure_reason")
                if qc_failure_reason:
                    timestep_results["qc_passed"] = False

            elif status == "skipped":
                qc_failure_reason = result.get("qc_failure_reason", "Variable skipped")
                logger.info(
                    f"[Day1Score] Miner {miner_hotkey}: Variable {var_key} skipped at {valid_time_dt} - {qc_failure_reason}"
                )
                timestep_results["qc_passed"] = False

            elif status == "error":
                error_message = result.get("error_message", "Unknown error")
                logger.error(
                    f"[Day1Score] Miner {miner_hotkey}: Variable {var_key} processing error at {valid_time_dt}: {error_message}"
                )
                timestep_results["variables"][var_key]["error"] = error_message
                timestep_results["qc_passed"] = False

        return timestep_results

    except Exception as e_timestep:
        logger.error(
            f"[Day1Score] Miner {miner_hotkey}: Error in parallel time step {valid_time_dt}: {e_timestep}",
            exc_info=True,
        )
        timestep_results["error_message"] = str(e_timestep)
        return timestep_results
    finally:
        # CRITICAL ABC MEMORY LEAK FIX: Clean up all timestep ABC objects
        try:
            # Clean up time slice datasets (major ABC objects)
            timestep_objects = [
                "miner_forecast_lead",
                "gfs_analysis_lead",
                "gfs_reference_lead",
                "cached_lat_weights",
                "cached_pressure_dims",
                "cached_grid_dims",
            ]

            cleaned_count = 0
            for obj_name in timestep_objects:
                if obj_name in locals():
                    try:
                        obj = locals()[obj_name]
                        if hasattr(obj, "close") and callable(obj.close):
                            obj.close()
                        del obj
                        cleaned_count += 1
                    except Exception:
                        pass

            # Force garbage collection for time step cleanup
            if cleaned_count > 0:
                collected = gc.collect()
                logger.debug(
                    f"[Day1Score] Timestep {valid_time_dt} cleanup: removed {cleaned_count} objects, GC collected {collected}"
                )

        except Exception as cleanup_err:
            logger.debug(
                f"[Day1Score] Timestep {valid_time_dt} cleanup error: {cleanup_err}"
            )


def _fast_interpolation(source_data, target_grid, method="nearest"):
    """
    OPTIMIZATION: Use faster interpolation method (5-10% speedup)
    Switch from 'linear' to 'nearest' for 2-3x faster interpolation with minimal accuracy loss
    """
    try:
        # Use nearest neighbor for speed - usually sufficient for weather data
        return source_data.interp_like(target_grid, method=method)
    except Exception:
        # Fallback to linear if nearest fails
        return source_data.interp_like(target_grid, method="linear")


def _vectorized_stats_calculation(data_arrays):
    """
    OPTIMIZATION: Vectorized statistics calculation (3-5% speedup)
    Calculate min/max/mean for multiple arrays simultaneously
    """
    try:
        import dask.array as da

        # Convert to dask arrays for vectorized computation
        dask_arrays = [
            da.from_array(arr.values, chunks=arr.chunks) for arr in data_arrays
        ]

        # Compute all statistics at once
        mins = [float(arr.min().compute()) for arr in dask_arrays]
        maxs = [float(arr.max().compute()) for arr in dask_arrays]
        means = [float(arr.mean().compute()) for arr in dask_arrays]

        return list(zip(mins, maxs, means))
    except ImportError:
        # Fallback to individual computation if dask not available
        return [
            (float(arr.min()), float(arr.max()), float(arr.mean()))
            for arr in data_arrays
        ]


def _reduce_precision(data_array):
    """
    OPTIMIZATION: Use float32 instead of float64 (2-3% speedup, 50% memory reduction)
    Weather data typically doesn't need double precision
    """
    if data_array.dtype == "float64":
        return data_array.astype("float32")
    return data_array
