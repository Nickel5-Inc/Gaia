import hashlib
import json
import struct
from typing import Dict, List, Tuple, Any, Optional, Set, Union
import numpy as np
import xarray as xr
import fsspec
import pickle
from datetime import datetime, timedelta, timezone
from pathlib import Path
import aiohttp
import asyncio
from functools import partial
import traceback
from .gfs_api import fetch_gfs_analysis_data, GFS_SURFACE_VARS, GFS_ATMOS_VARS
from fiber.logging_utils import get_logger
import urllib.parse
import requests
import time
import logging
import os
import psutil
import ssl
import base64
import warnings
import xskillscore as xs
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
import dask.array as da

logger = get_logger(__name__)

NUM_SAMPLES = 1000
HASH_VERSION = "3.0-miner_placeholder"  # Reflects minimal miner hash for now

ENABLE_PLOTTING = True
PLOT_SAVE_DIR = "./verification_plots"
if ENABLE_PLOTTING and not os.path.exists(PLOT_SAVE_DIR):
    os.makedirs(PLOT_SAVE_DIR)

ANALYSIS_LOG_DIR = "./analysis_logs"
if not os.path.exists(ANALYSIS_LOG_DIR):
    os.makedirs(ANALYSIS_LOG_DIR)

CANONICAL_VARS_FOR_HASHING = sorted([
    '2t', '10u', '10v', 'msl',
    't', 'u', 'v', 'q', 'z' 
])

logging.getLogger('fsspec').setLevel(logging.WARNING)

def get_current_memory_usage_mb():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss / (1024 * 1024)

def generate_deterministic_seed(
    forecast_date: str,
    source_model: str,
    grid_resolution: float,
    num_variables: int
) -> int:
    """
    Generate a deterministic seed from forecast metadata for reproducible sampling.
    
    Args:
        forecast_date: Date of the forecast initialization in YYYY-MM-DD format
        source_model: Name of the source model (e.g., "aurora-0.25-finetuned")
        grid_resolution: Resolution of the grid in degrees (e.g., 0.25)
        num_variables: Number of variables in the forecast
        
    Returns:
        A deterministic integer seed
    """
    seed_str = f"{forecast_date}_{source_model}_{grid_resolution}_{num_variables}_{HASH_VERSION}"
    hash_obj = hashlib.sha256(seed_str.encode())
    seed = int.from_bytes(hash_obj.digest()[:4], byteorder='big')

    return seed


def generate_sample_indices(
    rng: np.random.Generator,
    data_shape: Dict[str, Dict[str, Tuple[int, ...]]],
    variables: List[str],
    timesteps: List[int],
    num_samples: int = NUM_SAMPLES
) -> List[Dict[str, Any]]:
    """
    Generate indices for sampling data points from the forecast.
    
    Args:
        rng: Numpy random generator initialized with deterministic seed
        data_shape: Dictionary of variable categories and their shapes
        variables: List of variables to sample from
        timesteps: List of timestep indices to sample from
        num_samples: Number of sample points to generate
        
    Returns:
        List of dictionaries with sampling coordinates
    """
    sample_indices = []
    
    var_categories = {
        "surf_vars": [v for v in variables if v in ["2t", "10u", "10v", "msl"]],
        "atmos_vars": [v for v in variables if v in ["z", "u", "v", "t", "q"]]
    }
    
    samples_per_var = num_samples // len(variables)
    extra_samples = num_samples % len(variables)
    
    var_indices = []
    for var in variables:
        var_samples = samples_per_var + (1 if extra_samples > 0 else 0)
        extra_samples -= 1 if extra_samples > 0 else 0
        var_indices.extend([(var, i) for i in range(var_samples)])
    
    rng.shuffle(var_indices)

    if not timesteps:
        logger.warning("Empty timesteps list provided to generate_sample_indices, using [0] as fallback")
        timesteps = [0]
    
    logger.debug(f"Generating sample indices from timesteps: {timesteps}")
    
    for var, _ in var_indices:
        category = None
        for cat, vars_list in var_categories.items():
            if var in vars_list:
                category = cat
                break
        
        if category is None:
            continue
            
        shape = data_shape[category].get(var)
        if shape is None:
            continue
            
        max_time_idx = shape[1] - 1
        valid_timesteps = [t for t in timesteps if 0 <= t <= max_time_idx]
        
        if not valid_timesteps:
            valid_timesteps = list(range(min(20, shape[1])))  # Default to first 20 or fewer
            logger.warning(f"No valid timesteps found for {var}. Using {valid_timesteps} instead.")
        
        if category == "atmos_vars":
            t_idx = rng.choice(valid_timesteps)
            
            max_level = shape[2] - 1
            max_lat = shape[3] - 1
            max_lon = shape[4] - 1
            
            level_idx = rng.integers(0, max_level + 1)
            lat_idx = rng.integers(0, max_lat + 1)
            lon_idx = rng.integers(0, max_lon + 1)
            
            sample_indices.append({
                "variable": var,
                "category": category,
                "timestep": int(t_idx),
                "level": int(level_idx),
                "lat": int(lat_idx),
                "lon": int(lon_idx)
            })
        else:
            t_idx = rng.choice(valid_timesteps)
            
            max_lat = shape[2] - 1
            max_lon = shape[3] - 1
            
            lat_idx = rng.integers(0, max_lat + 1)
            lon_idx = rng.integers(0, max_lon + 1)
            
            sample_indices.append({
                "variable": var,
                "category": category,
                "timestep": int(t_idx),
                "lat": int(lat_idx),
                "lon": int(lon_idx)
            })
    
    return sample_indices


def serialize_float(value: float) -> bytes:
    """
    Serialize a float to bytes using IEEE 754 double precision format.
    
    Args:
        value: Float value to serialize
        
    Returns:
        Bytes representing the float in canonical form
    """
    return struct.pack('>d', float(value))


def canonical_serialization(
    sample_indices: List[Dict[str, Any]],
    data: Dict[str, Any]
) -> bytes:
    """
    Serialize sampled data points in a canonical format.
    
    Args:
        sample_indices: List of dictionaries with sampling coordinates
        data: Dictionary containing the forecast data
        
    Returns:
        Bytes representing the serialized sample data
    """
    serialized = bytearray()
    
    sorted_indices = sorted(
        sample_indices, 
        key=lambda x: (
            x["variable"], 
            x["category"], 
            x["timestep"], 
            x.get("level", 0),
            x["lat"], 
            x["lon"]
        )
    )
    
    for idx in sorted_indices:
        var = idx["variable"]
        category = idx["category"]
        t_idx = idx["timestep"]
        lat_idx = idx["lat"]
        lon_idx = idx["lon"]
        
        try:
            if category == "atmos_vars":
                level_idx = idx["level"]
                value = data[category][var][0, t_idx, level_idx, lat_idx, lon_idx].item()
            else:
                value = data[category][var][0, t_idx, lat_idx, lon_idx].item()
                
            serialized.extend(serialize_float(value))
            
        except (KeyError, IndexError) as e:
            serialized.extend(serialize_float(float('nan')))
    
    return bytes(serialized)


def compute_verification_hash(
    data: Dict[str, Dict[str, np.ndarray]], # Still passed, as final hash will use it
    metadata: Dict[str, Any],
    variables: List[str], # Still passed, for consistency with final design
    timesteps: List[int]  # Still passed, for consistency
) -> str:
    """
    (TEMPORARY - MINIMAL HASH FOR DEVELOPMENT)
    Computes a very basic hash based on metadata. 
    Actual data-driven hashing logic is to be rebuilt.
    
    Args:
        data: Forecast data (currently unused in this placeholder).
        metadata: Forecast metadata.
        variables: List of variables (currently unused).
        timesteps: List of timestep indices (currently unused).
        
    Returns:
        Hex string of the SHA-256 hash.
    """
    start_time = time.time()
    
    # Minimal profile for placeholder hash - primarily metadata-based
    profile = {
        "metadata": {
            "date": metadata.get("time")[0].strftime("%Y-%m-%d"),
            "model": metadata.get("source_model", "aurora"),
            "resolution": metadata.get("resolution", 0.25),
            "hash_version": HASH_VERSION 
        },
        "notes": "Placeholder hash using minimal metadata only during hashing algorithm rebuild."
        # No complex statistics or physical checks for this placeholder hash
    }
    
    canonical_json = json.dumps(profile, sort_keys=True)
    hash_obj = hashlib.sha256(canonical_json.encode())
    result_hash = hash_obj.hexdigest()
    
    elapsed_time = time.time() - start_time
    logger.info(f"Miner computed placeholder verification hash in {elapsed_time:.3f}s: {result_hash}")
    return result_hash


def _synchronous_zarr_open(
    zarr_store_url: str,
    http_fs_kwargs: Dict
) -> xr.Dataset:
    """
    Open a Zarr store over HTTP synchronously.
    
    Args:
        zarr_store_url: URL to the Zarr store (.zarr directory)
        http_fs_kwargs: Dictionary of HTTP options (headers, SSL verification)
        
    Returns:
        xarray Dataset opened from the store
    """
    if zarr_store_url.endswith(".zarr"):
        zarr_store_url = zarr_store_url + "/"
    zarr_store_url = zarr_store_url.rstrip('/') + '/'
    
    logger.info(f"SYNC_ZARR_OPEN: Starting. Initial Memory: {get_current_memory_usage_mb():.2f} MB. Target URL: {zarr_store_url}")
    overall_start_time = time.time()

    fsspec_remote_opts = {
        "ssl": False
    }
    logger.warning("SYNC_ZARR_OPEN: SSL certificate verification is explicitly DISABLED. FOR TESTING ONLY.")

    headers = {}
    if "headers" in http_fs_kwargs:
        headers.update(http_fs_kwargs["headers"])
        logger.info(f"SYNC_ZARR_OPEN: Using provided headers: {list(headers.keys())}")
        
    token = None
    if "Authorization" in headers:
        auth_header = headers["Authorization"]
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        else:
            token = auth_header
        logger.info("SYNC_ZARR_OPEN: Found token in Authorization header")
        
    fsspec_remote_opts["headers"] = headers
    
    protocol = "https" if zarr_store_url.startswith("https://") else "http"
    time_before_fs_init = time.time()
    
    try:
        logger.info(f"SYNC_ZARR_OPEN: Opening Zarr store with headers at URL: {zarr_store_url}")
        
        fs = fsspec.filesystem(
            protocol,
            **fsspec_remote_opts
        )  
        mapper = fs.get_mapper(zarr_store_url)
        logger.info(f"SYNC_ZARR_OPEN: Initialized filesystem and mapper in {time.time() - time_before_fs_init:.2f}s. Mapper URL: {zarr_store_url}")
        
        try:
            zmetadata_exists = fs.exists(zarr_store_url + '.zmetadata')
            if zmetadata_exists:
                logger.info(f"SYNC_ZARR_OPEN: Found .zmetadata file, will use consolidated=True")
            else:
                logger.warning(f"SYNC_ZARR_OPEN: .zmetadata file not found at {zarr_store_url}.zmetadata")
                return None
        except Exception as e_meta:
            logger.warning(f"SYNC_ZARR_OPEN: Error checking for .zmetadata: {str(e_meta)}")
        
    except Exception as e_fs_init:
        logger.error(f"SYNC_ZARR_OPEN: Failed to initialize filesystem: {e_fs_init!r}", exc_info=True)
        raise

    logger.info(f"SYNC_ZARR_OPEN: Memory before opening dataset: {get_current_memory_usage_mb():.2f} MB.")
    time_before_zarr_open = time.time()
    ds = None
    
    try:
        logger.info(f"SYNC_ZARR_OPEN: Opening Zarr store with consolidated=True")
        
        ds = xr.open_zarr(
            mapper,
            consolidated=True,
            decode_times=True,
            mask_and_scale=True,
            chunks={},
        )
        
        logger.info(f"SYNC_ZARR_OPEN: Successfully opened Zarr dataset with keys: {list(ds.keys())}")
        logger.info(f"SYNC_ZARR_OPEN: Memory after opening Zarr dataset: {get_current_memory_usage_mb():.2f} MB.")
        return ds
        
    except Exception as e_xr_open:
        logger.error(f"SYNC_ZARR_OPEN: Failed to open Zarr dataset: {e_xr_open!r}. Memory at failure: {get_current_memory_usage_mb():.2f} MB.", exc_info=True)
        if "KeyError: '.zmetadata'" in traceback.format_exc():
             logger.error("SYNC_ZARR_OPEN: Encountered KeyError for '.zmetadata'. Check if .zmetadata is accessible via the URL and miner permissions.")
        elif "GroupNotFoundError" in traceback.format_exc():
             logger.error("SYNC_ZARR_OPEN: Encountered GroupNotFoundError. Check if the root Zarr directory is accessible via the URL.")
        raise
    finally:
        logger.info(f"SYNC_ZARR_OPEN: Exiting. Total time in function: {time.time() - overall_start_time:.2f}s. Final Memory: {get_current_memory_usage_mb():.2f} MB.")


async def open_remote_zarr_dataset(
    zarr_store_url: str,
    variables: List[str],
    storage_options: Optional[Dict] = None
) -> Optional[xr.Dataset]:
    """
    Open a remote Zarr dataset asynchronously through an executor.
    
    Args:
        zarr_store_url: URL to the Zarr store (.zarr directory)
        variables: List of variable names to load (currently unused but kept for API compatibility)
        storage_options: Optional dictionary with HTTP options (headers, SSL verification)
        
    Returns:
        xarray Dataset opened from the Zarr store or None if opening fails
    """
    logger.info(f"Attempting to open remote Zarr dataset (sync in executor): {zarr_store_url} with storage_options: {storage_options is not None}")

    http_client_kwargs = {
        "headers": {},
        "ssl": False
    }

    if storage_options and "headers" in storage_options:
        http_client_kwargs["headers"] = storage_options["headers"]
        
    current_loop = asyncio.get_running_loop()
    ds = None
    try:
        logger.debug(f"Calling _synchronous_zarr_open for Zarr store {zarr_store_url} in executor...")
        ds = await current_loop.run_in_executor(
            None,
            _synchronous_zarr_open,
            zarr_store_url,
            http_client_kwargs
        )
        if ds:
            logger.info(f"Successfully opened remote Zarr dataset via executor: {zarr_store_url}")
        else:
            logger.error(f"_synchronous_zarr_open returned None for Zarr store: {zarr_store_url}")
        return ds
    except Exception as e_open:
        logger.error(f"Error in executor running _synchronous_zarr_open for Zarr store {zarr_store_url}: {e_open!r}", exc_info=True)
        return None


def _compute_analysis_profile(current_ds, current_metadata, current_variables, _claimed_hash_unused, current_job_id):
    inner_start_time = time.time()
    
    profile = {
        "job_id": current_job_id,
        "zarr_store_url": zarr_store_url,
        "analysis_timestamp_utc": datetime.now(timezone.utc).isoformat(), 
        "claimed_hash_received": _claimed_hash_unused, 
        "metadata": {
            "date": current_metadata.get("time")[0].strftime("%Y-%m-%d"),
            "model": current_metadata.get("source_model", "aurora"),
            "resolution": current_metadata.get("resolution", 0.25),
            "hash_version": "3.0-perf_test"
        },
        "query_performance_tests": [],
        "status": "performance_test_pending",
        "notes": "Performing Zarr read performance tests."
    }

    try:
        max_t = current_ds.sizes.get("time", 0)
        if max_t == 0:
            profile["status"] = "performance_test_error"
            profile["notes"] = "Dataset has no time dimension."
            profile["computation_time_seconds"] = time.time() - inner_start_time
            return profile

        profile_timesteps_indices = sorted(list(set([0, max_t // 2, max_t - 1]))) if max_t > 0 else [0]
        mid_timestep_idx = profile_timesteps_indices[1] if len(profile_timesteps_indices) > 1 else profile_timesteps_indices[0]

        var_mapping = {}
        standard_mapping = {
            "2t": ["2t", "t2m"], "10u": ["10u", "u10"], "10v": ["10v", "v10"], "msl": ["msl"],
            "t": ["t"], "u": ["u"], "v": ["v"], "q": ["q"], "z": ["z"]
        }
        ds_vars = list(current_ds.data_vars)
        for var_name_map, possible_names in standard_mapping.items():
            for pn in possible_names:
                if pn in ds_vars:
                    var_mapping[var_name_map] = pn
                    break
        
        # === Test Case 1: One variable, one step, one level, full lat/lon ===
        case1_results = {"case_name": "Case 1: Single Full 2D Slice (t, mid-time, mid-level)"}
        try:
            var_to_test_case1 = "t"
            if var_mapping.get(var_to_test_case1) in current_ds:
                data_array_case1 = current_ds[var_mapping[var_to_test_case1]]
                if "pressure_level" in data_array_case1.sizes:
                    mid_level_idx = data_array_case1.sizes["pressure_level"] // 2
                    selection_case1 = data_array_case1.isel(time=mid_timestep_idx, pressure_level=mid_level_idx)
                    
                    time_start_case1 = time.time()
                    loaded_slice_case1 = selection_case1.load()
                    case1_results["time_taken_seconds"] = time.time() - time_start_case1
                    case1_results["data_loaded_bytes"] = loaded_slice_case1.nbytes
                    case1_results["slice_shape"] = list(loaded_slice_case1.shape)
                    case1_results["status"] = "success"
                else:
                    case1_results["status"] = "skipped_not_3d_var"
            else:
                case1_results["status"] = f"skipped_var_not_found ({var_to_test_case1})"
        except Exception as e_case1:
            case1_results["status"] = "error"
            case1_results["error_message"] = str(e_case1)
        profile["query_performance_tests"].append(case1_results)

        # === Test Case 2: One chunk only===
        case2_results = {"case_name": "Case 2: Small Localized 2D Slice (t, mid-time, mid-level, 10x10 window)"}
        try:
            var_to_test_case2 = "t"
            if var_mapping.get(var_to_test_case2) in current_ds:
                data_array_case2 = current_ds[var_mapping[var_to_test_case2]]
                if "pressure_level" in data_array_case2.sizes and "lat" in data_array_case2.sizes and "lon" in data_array_case2.sizes:
                    mid_level_idx = data_array_case2.sizes["pressure_level"] // 2
                    lat_slice = slice(0, min(10, data_array_case2.sizes["lat"]))
                    lon_slice = slice(0, min(10, data_array_case2.sizes["lon"]))
                    selection_case2 = data_array_case2.isel(time=mid_timestep_idx, pressure_level=mid_level_idx, lat=lat_slice, lon=lon_slice)
                    
                    time_start_case2 = time.time()
                    loaded_slice_case2 = selection_case2.load()
                    case2_results["time_taken_seconds"] = time.time() - time_start_case2
                    case2_results["data_loaded_bytes"] = loaded_slice_case2.nbytes
                    case2_results["slice_shape"] = list(loaded_slice_case2.shape)
                    case2_results["status"] = "success"
                else:
                    case2_results["status"] = "skipped_missing_dims_for_small_slice"
            else:
                case2_results["status"] = f"skipped_var_not_found ({var_to_test_case2})"
        except Exception as e_case2:
            case2_results["status"] = "error"
            case2_results["error_message"] = str(e_case2)
        profile["query_performance_tests"].append(case2_results)

        # === Test Case 3: Three variables, three levels (for 3D vars), three steps, full lat/lon ===
        case3_results = {"case_name": "Case 3: Multi-Var/Time/Level Full Slices (t, u, 2t)"}
        try:
            vars_to_test_case3 = ["t", "u", "2t"] # Mix of 3D and 2D
            data_loaded_bytes_case3 = 0
            actual_slices_loaded_count = 0
            time_start_case3 = time.time()

            for var_c3 in vars_to_test_case3:
                if var_mapping.get(var_c3) in current_ds:
                    data_array_c3 = current_ds[var_mapping[var_c3]]
                    is_3d_c3 = "pressure_level" in data_array_c3.sizes
                    levels_to_load_c3 = [data_array_c3.sizes["pressure_level"] // 2]
                    if is_3d_c3:
                        num_levels_c3 = data_array_c3.sizes["pressure_level"]
                        levels_to_load_c3 = sorted(list(set([0, num_levels_c3 // 2, num_levels_c3 - 1]))) if num_levels_c3 > 0 else [0]
                    
                    for t_idx_c3 in profile_timesteps_indices:
                        if t_idx_c3 < data_array_c3.sizes.get("time",0):
                            if is_3d_c3:
                                for l_idx_c3 in levels_to_load_c3:
                                    if l_idx_c3 < data_array_c3.sizes["pressure_level"]:
                                        selection_c3 = data_array_c3.isel(time=t_idx_c3, pressure_level=l_idx_c3)
                                        loaded_slice_c3 = selection_c3.load()
                                        data_loaded_bytes_case3 += loaded_slice_c3.nbytes
                                        actual_slices_loaded_count +=1
                            else: # 2D variable
                                selection_c3 = data_array_c3.isel(time=t_idx_c3)
                                loaded_slice_c3 = selection_c3.load()
                                data_loaded_bytes_case3 += loaded_slice_c3.nbytes
                                actual_slices_loaded_count +=1
                else:
                    logger.warning(f"Case 3: Variable {var_c3} not found, skipping its part.")
            
            case3_results["time_taken_seconds"] = time.time() - time_start_case3
            case3_results["total_data_loaded_bytes"] = data_loaded_bytes_case3
            case3_results["total_2d_slices_loaded"] = actual_slices_loaded_count
            case3_results["status"] = "success"
        except Exception as e_case3:
            case3_results["status"] = "error"
            case3_results["error_message"] = str(e_case3)
        profile["query_performance_tests"].append(case3_results)
        
        profile["status"] = "performance_test_complete"

    except Exception as e_inner_fatal:
        logger.error(f"Fatal error inside _compute_analysis_profile (performance test) for job {current_job_id}: {e_inner_fatal}", exc_info=True)
        profile["status"] = "performance_test_fatal_error"
        profile["error"] = str(e_inner_fatal)
    
    profile["computation_time_seconds"] = time.time() - inner_start_time
    return profile 


async def verify_forecast_hash(
    zarr_store_url: str,
    claimed_hash: str, 
    metadata: Dict[str, Any],
    variables: List[str],
    timesteps: List[int], 
    headers: Optional[Dict[str, str]] = None,
    job_id: Optional[str] = "unknown_job"
) -> Dict[str, Any]: 
    """
    (PERFORMANCE TEST MODE)
    Opens dataset. _compute_analysis_profile performs specific Zarr read tests.
    Saves the performance profile to a JSON file.
    Returns a dictionary containing the performance profile or an error.
    """
    start_time_total_verify = time.time()
    logger.info(f"Starting Zarr read performance test for job {job_id}: {zarr_store_url}")
    
    storage_options = {}
    if headers:
        storage_options['headers'] = headers
    
    ds = None
    try:
        ds = await open_remote_zarr_dataset(zarr_store_url, variables, storage_options=storage_options)
        if ds is None:
            err_msg = f"Dataset open failed for job {job_id} at {zarr_store_url}."
            logger.error(err_msg)
            return {"error": err_msg, "job_id": job_id, "status": "dataset_open_failed", "zarr_store_url": zarr_store_url}
    except Exception as e:
        err_msg = f"Exception opening dataset for job {job_id} at {zarr_store_url}: {e}"
        logger.error(err_msg, exc_info=True)
        return {"error": err_msg, "job_id": job_id, "status": "dataset_open_exception", "zarr_store_url": zarr_store_url}
    
    try:
        analysis_result_profile = await asyncio.get_running_loop().run_in_executor(
            None, 
            _compute_analysis_profile, 
            ds, metadata, variables, claimed_hash, job_id 
        )
        
        if analysis_result_profile: 
            try:
                if "zarr_store_url" not in analysis_result_profile:
                    analysis_result_profile["zarr_store_url"] = zarr_store_url
                
                ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                log_filename = os.path.join(ANALYSIS_LOG_DIR, f"job_{job_id}_perf_profile_{ts}.json")
                with open(log_filename, 'w') as f:
                    json.dump(analysis_result_profile, f, indent=4)
                logger.info(f"Saved performance test profile for job {job_id} to: {log_filename}")
            except Exception as e_save:
                logger.error(f"Failed to save performance test profile for job {job_id} to file: {e_save}")

        final_status = analysis_result_profile.get("status", "unknown_error_in_profile")
        if not final_status.startswith("performance_test") and "error" in analysis_result_profile:
            logger.error(f"Performance test profile computation failed for job {job_id} ({zarr_store_url}) with error: {analysis_result_profile.get('error')}")
        elif final_status == "performance_test_complete":
            logger.info(f"Successfully completed performance tests for job {job_id}.")
        else:
            logger.warning(f"Performance tests for job {job_id} finished with status: {final_status}")

        logger.info(f"Performance test (executor part) for job {job_id} took: {analysis_result_profile.get('computation_time_seconds', -1):.3f} seconds")
        logger.info(f"Total time for verify_forecast_hash (performance test mode) for job {job_id}: {time.time() - start_time_total_verify:.3f} seconds")
        
        return analysis_result_profile 
        
    except Exception as e_executor_call:
        err_msg = f"Exception calling executor for performance test profile (job {job_id}, {zarr_store_url}): {e_executor_call}"
        logger.error(err_msg, exc_info=True)
        return {"error": err_msg, "job_id": job_id, "status": "executor_call_exception", "zarr_store_url": zarr_store_url}
    finally:
        if ds is not None:
            ds.close()
            logger.debug(f"Closed xarray dataset for job {job_id} ({zarr_store_url})")


def get_forecast_summary(
    zarr_store_url: str,
    variables: List[str]
) -> Dict[str, Any]:
    """
    Get a summary of forecast properties without loading all data.
    
    Args:
        zarr_store_url: URL to the Zarr store
        variables: List of variables to include
    Returns:
        Dictionary with summary statistics
    """

    return {
        "variables": variables,
        "zarr_store_url": zarr_store_url,
        "status": "placeholder"
    }


def _get_canonical_bytes(ds_t0: xr.Dataset, ds_t_minus_6: xr.Dataset) -> Optional[bytes]:
    """
    Prepares the two input datasets into a canonical byte representation for hashing.
    Sorts variables, ensures consistent data types, and flattens data.

    Args:
        ds_t0: Processed xarray Dataset for T=0h analysis.
        ds_t_minus_6: Processed xarray Dataset for T=-6h analysis.

    Returns:
        Bytes representation or None if input is invalid.
    """
    if not isinstance(ds_t0, xr.Dataset) or not isinstance(ds_t_minus_6, xr.Dataset):
        logger.error("Invalid input: Both ds_t0 and ds_t_minus_6 must be xarray Datasets.")
        return None
    if 'time' not in ds_t0.coords or 'time' not in ds_t_minus_6.coords:
         logger.error("Invalid input: Datasets must have a 'time' coordinate.")
         return None
    if len(ds_t0.time) != 1 or len(ds_t_minus_6.time) != 1:
         logger.error("Invalid input: Datasets should contain only one time step for analysis.")
         return None

    logger.debug("Creating canonical byte representation for input data hashing...")
    all_bytes_list = []

    ds_t_minus_6_squeezed = ds_t_minus_6.squeeze('time', drop=True)
    for var in CANONICAL_VARS_FOR_HASHING:
        if var in ds_t_minus_6_squeezed:
            data_array = ds_t_minus_6_squeezed[var].load().astype(np.float32).values
            all_bytes_list.append(data_array.tobytes())
            logger.debug(f"Added T-6h var '{var}' (shape: {data_array.shape}, dtype: {data_array.dtype}) to canonical bytes.")
        else:
             logger.warning(f"Required variable '{var}' missing in T-6h data for hashing.")
             pass

    ds_t0_squeezed = ds_t0.squeeze('time', drop=True)
    for var in CANONICAL_VARS_FOR_HASHING:
        if var in ds_t0_squeezed:
            data_array = ds_t0_squeezed[var].load().astype(np.float32).values
            all_bytes_list.append(data_array.tobytes())
            logger.debug(f"Added T=0h var '{var}' (shape: {data_array.shape}, dtype: {data_array.dtype}) to canonical bytes.")
        else:
             logger.warning(f"Required variable '{var}' missing in T=0h data for hashing.")
             pass

    if not all_bytes_list:
         logger.error("No data variables found to create canonical bytes.")
         return None

    canonical_bytes = b"".join(all_bytes_list)
    logger.info(f"Generated canonical byte representation: {len(canonical_bytes)} bytes.")
    return canonical_bytes


async def compute_input_data_hash(
    t0_run_time: datetime,
    t_minus_6_run_time: datetime,
    cache_dir: Path
) -> Optional[str]:
    """
    Fetches the required GFS analysis data subsets (T=0h, T=-6h),
    creates a canonical byte representation, and computes its SHA-256 hash.

    Args:
        t0_run_time: The datetime for the T=0h GFS analysis.
        t_minus_6_run_time: The datetime for the T=-6h GFS analysis.
        cache_dir: Path object for the GFS analysis cache directory.

    Returns:
        SHA-256 hash hex digest, or None if fetching/processing fails.
    """
    logger.info(f"Computing input data hash for T0={t0_run_time}, T-6={t_minus_6_run_time}")

    ds_t0 = None
    ds_t_minus_6 = None
    try:
        ds_t0 = await fetch_gfs_analysis_data([t0_run_time], cache_dir=cache_dir)
        ds_t_minus_6 = await fetch_gfs_analysis_data([t_minus_6_run_time], cache_dir=cache_dir)

        if ds_t0 is None or ds_t_minus_6 is None:
            logger.error("Failed to fetch one or both GFS analysis datasets needed for hashing.")
            return None

        t0_run_time_np = np.datetime64(t0_run_time.replace(tzinfo=None))
        t_minus_6_run_time_np = np.datetime64(t_minus_6_run_time.replace(tzinfo=None))

        if (t0_run_time_np not in ds_t0.time.values or
            t_minus_6_run_time_np not in ds_t_minus_6.time.values):
             logger.error("Fetched datasets do not contain the exact requested time coordinates.")
             logger.debug(f"Expected T0: {t0_run_time_np}, Got: {ds_t0.time.values if ds_t0 else 'None'}")
             logger.debug(f"Expected T-6: {t_minus_6_run_time_np}, Got: {ds_t_minus_6.time.values if ds_t_minus_6 else 'None'}")
             return None

        canonical_bytes = _get_canonical_bytes(ds_t0, ds_t_minus_6)

        if canonical_bytes is None:
            logger.error("Failed to generate canonical byte representation.")
            return None
        else:
            hasher = hashlib.sha256()
            hasher.update(canonical_bytes)
            hex_digest = hasher.hexdigest()
            logger.info(f"Computed input data SHA-256 hash: {hex_digest}")
            return hex_digest

    except Exception as e:
        logger.error(f"Error during input data hash computation: {e}", exc_info=True)
        return None
    finally:
        if ds_t0 is not None and hasattr(ds_t0, 'close'):
            try:
                ds_t0.close()
                logger.debug("Closed ds_t0 dataset.")
            except Exception as close_err:
                 logger.warning(f"Exception closing ds_t0: {close_err}")
        if ds_t_minus_6 is not None and hasattr(ds_t_minus_6, 'close'):
            try:
                ds_t_minus_6.close()
                logger.debug("Closed ds_t_minus_6 dataset.")
            except Exception as close_err:
                 logger.warning(f"Exception closing ds_t_minus_6: {close_err}")


def _get_data_shape_from_xarray_dataset(
    ds: xr.Dataset, 
    variables_to_include: List[str], 
    var_mapping: Dict[str, Tuple[str, str]]
) -> Dict[str, Dict[str, Tuple[int, ...]]]:
    data_shape = {"surf_vars": {}, "atmos_vars": {}}
    
    rev_mapping = {}
    for ds_name, (cat, aur_name) in var_mapping.items():
        rev_mapping[aur_name] = (cat, ds_name)
        
    direct_vars = {}
    for var in variables_to_include:
        if var in rev_mapping:
            cat, ds_var = rev_mapping[var]
            if ds_var in ds:
                data_shape[cat][var] = (1,) + ds[ds_var].shape
                logger.debug(f"Found {var} via mapping as {ds_var} with shape {ds[ds_var].shape}")
                continue
        
        if var in ds:
            if var in ["2t", "10u", "10v", "msl"]:
                cat = "surf_vars"
            else:
                cat = "atmos_vars"
            data_shape[cat][var] = (1,) + ds[var].shape
            logger.debug(f"Found {var} directly in dataset with shape {ds[var].shape}")
            continue
            
        variations = [var]
        if var == "10u": variations.append("u10")
        if var == "10v": variations.append("v10")
        if var == "2t": variations.append("t2m")
        
        found = False
        for v in variations:
            if v in ds:
                if var in ["2t", "10u", "10v", "msl"]:
                    cat = "surf_vars"
                else:
                    cat = "atmos_vars"
                data_shape[cat][var] = (1,) + ds[v].shape
                logger.debug(f"Found {var} as variant {v} with shape {ds[v].shape}")
                found = True
                break
        
        if not found:
            logger.warning(f"Variable '{var}' not found in dataset by any name. Cannot determine shape for hashing.")
    
    logger.info(f"_get_data_shape_from_xarray_dataset: Populated data_shape: {data_shape}")
    return data_shape


def _efficient_canonical_serialization(
    sample_indices: List[Dict[str, Any]],
    dataset: xr.Dataset,
    var_mapping: Dict[str, Tuple[str, str]]
) -> bytes:
    logger.info(f"SERIALIZATION: Starting serialization for {len(sample_indices)} sample indices.")
    overall_serialization_start_time = time.time()
    serialized = bytearray()
    sorted_indices = sorted(sample_indices, key=lambda x: (x["variable"], x["category"], x["timestep"], x.get("level", 0), x["lat"], x["lon"]))

    actual_vars = list(dataset.keys())
    logger.info(f"SERIALIZATION: Dataset contains variables: {actual_vars}")
    
    var_map = {}
    canonical_to_possible_names = {
        "10u": ["10u", "u10"],
        "10v": ["10v", "v10"],
        "2t": ["2t", "t2m"],
        "msl": ["msl"],
        "t": ["t"],
        "u": ["u"],
        "v": ["v"],
        "q": ["q"],
        "z": ["z"]
    }
    
    for aurora_name, possible_names in canonical_to_possible_names.items():
        for ds_name in possible_names:
            if ds_name in actual_vars:
                var_map[aurora_name] = ds_name
                logger.info(f"SERIALIZATION: Mapped {aurora_name} -> {ds_name}")
                break
    
    for aurora_name in CANONICAL_VARS_FOR_HASHING:
        if aurora_name not in var_map:
            logger.warning(f"SERIALIZATION: Could not find any matching variable for {aurora_name}")
    
    for i, idx_info in enumerate(sorted_indices):
        aurora_var_name = idx_info["variable"]
        category = idx_info["category"]
        
        ds_var_name = var_map.get(aurora_var_name)
        
        log_prefix = f"SERIALIZATION: Index {i+1}/{len(sorted_indices)} (Var: {aurora_var_name}, DS_Var: {ds_var_name}, Cat: {category}, T: {idx_info['timestep']}, Lt: {idx_info['lat']}, Ln: {idx_info['lon']}"
        if 'level' in idx_info:
            log_prefix += f", Lvl: {idx_info['level']}"
        log_prefix += ")"

        if not ds_var_name or ds_var_name not in dataset:
            logger.warning(f"{log_prefix} - Variable not found in dataset. Using NaN.")
            serialized.extend(serialize_float(float('nan')))
            continue

        try:
            time_dim = 'time' 
            lat_dim = 'lat'
            lon_dim = 'lon'
            level_dim = 'pressure_level'

            if idx_info["timestep"] >= dataset.sizes[time_dim]:
                logger.warning(f"{log_prefix} - Time index {idx_info['timestep']} out of bounds (max: {dataset.sizes[time_dim]-1}). Using NaN.")
                serialized.extend(serialize_float(float('nan')))
                continue
                
            if idx_info["lat"] >= dataset.sizes[lat_dim]:
                logger.warning(f"{log_prefix} - Lat index {idx_info['lat']} out of bounds (max: {dataset.sizes[lat_dim]-1}). Using NaN.")
                serialized.extend(serialize_float(float('nan')))
                continue
                
            if idx_info["lon"] >= dataset.sizes[lon_dim]:
                logger.warning(f"{log_prefix} - Lon index {idx_info['lon']} out of bounds (max: {dataset.sizes[lon_dim]-1}). Using NaN.")
                serialized.extend(serialize_float(float('nan')))
                continue
                
            if category == "atmos_vars" and level_dim in dataset.sizes and idx_info["level"] >= dataset.sizes[level_dim]:
                logger.warning(f"{log_prefix} - Level index {idx_info['level']} out of bounds (max: {dataset.sizes[level_dim]-1}). Using NaN.")
                serialized.extend(serialize_float(float('nan')))
                continue
            
            isel_kwargs = {
                time_dim: idx_info["timestep"],
                lat_dim: idx_info["lat"],
                lon_dim: idx_info["lon"]
            }
            if category == "atmos_vars" and level_dim in dataset.sizes:
                isel_kwargs[level_dim] = idx_info["level"]
            
            logger.debug(f"{log_prefix} - Attempting .isel() with kwargs: {isel_kwargs}")
            point_fetch_start_time = time.time()
            
            data_array = dataset[ds_var_name].isel(**isel_kwargs)
            
            if hasattr(data_array, 'values'):
                point_value = data_array.values
                if hasattr(point_value, 'item') and point_value.size == 1:
                    value = point_value.item()
                else:
                    logger.warning(f"{log_prefix} - Got non-scalar array of shape {point_value.shape}. Using first element.")
                    value = float(point_value.flat[0])
            elif hasattr(data_array, 'data'):
                computed_value = data_array.compute().data
                if hasattr(computed_value, 'item') and computed_value.size == 1:
                    value = computed_value.item()
                else:
                    value = float(computed_value.flat[0])
            else:
                value = float(data_array)
                
            point_fetch_duration = time.time() - point_fetch_start_time
            
            try:
                if isinstance(value, (float, int)):
                    value_str = f"{value:.4f}"
                elif isinstance(value, (np.float32, np.float64)):
                    value_str = f"{float(value):.4f}"
                else:
                    value_str = str(value)
                logger.debug(f"{log_prefix} - Successfully fetched point. Value: {value_str}. Time: {point_fetch_duration:.3f}s")
            except Exception as format_err:
                logger.debug(f"{log_prefix} - Successfully fetched point. Value type: {type(value)}. Time: {point_fetch_duration:.3f}s")
            
            serialized.extend(serialize_float(float(value)))
            
        except Exception as e_serial:
            logger.warning(f"{log_prefix} - Error serializing point: {e_serial}. Using NaN.", exc_info=True)
            serialized.extend(serialize_float(float('nan')))
            
    total_serialization_duration = time.time() - overall_serialization_start_time
    logger.info(f"SERIALIZATION: Finished. Total bytes: {len(serialized)}. Total time: {total_serialization_duration:.3f}s")
    return bytes(serialized) 