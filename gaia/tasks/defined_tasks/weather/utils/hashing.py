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

logger = get_logger(__name__)

NUM_SAMPLES = 1000
HASH_VERSION = "2.0"

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
    data: Dict[str, Dict[str, np.ndarray]],
    metadata: Dict[str, Any],
    variables: List[str],
    timesteps: List[int]
) -> str:
    """
    Compute a verification hash for a forecast dataset using efficient statistical profiling
    with physical relationship checks.
    
    Args:
        data: Dictionary with forecast data in Aurora-compatible format
        metadata: Dictionary with forecast metadata 
        variables: List of variables to include in hash
        timesteps: List of timestep indices to include in hash
        
    Returns:
        Hex string of the SHA-256 hash
    """
    start_time = time.time()
    
    profile = {
        "metadata": {
            "date": metadata.get("time")[0].strftime("%Y-%m-%d"),
            "model": metadata.get("source_model", "aurora"),
            "resolution": metadata.get("resolution", 0.25),
            "hash_version": HASH_VERSION
        },
        "variables": {},
        "physical_checks": {}
    }

    max_t = max([data[cat][var].shape[1] for cat in data for var in data[cat] if var in variables])
    profile_timesteps = sorted(list(set([0, max_t // 2, max_t - 1])))
    profile["timesteps"] = profile_timesteps
    
    t_data = data.get("atmos_vars", {}).get("t", None)
    u_data = data.get("atmos_vars", {}).get("u", None)
    v_data = data.get("atmos_vars", {}).get("v", None)
    z_data = data.get("atmos_vars", {}).get("z", None)
    q_data = data.get("atmos_vars", {}).get("q", None)
    t2m_data = data.get("surf_vars", {}).get("2t", None)
    msl_data = data.get("surf_vars", {}).get("msl", None)
    u10_data = data.get("surf_vars", {}).get("10u", None)
    v10_data = data.get("surf_vars", {}).get("10v", None)
    
    for var_name in sorted(variables):

        var_data = None
        var_category = None

        if var_name in ["2t", "10u", "10v", "msl"] and "surf_vars" in data:
            if var_name in data["surf_vars"]:
                var_data = data["surf_vars"][var_name]
                var_category = "surf_vars"
        

        if var_name in ["t", "u", "v", "q", "z"] and "atmos_vars" in data:
            if var_name in data["atmos_vars"]:
                var_data = data["atmos_vars"][var_name]
                var_category = "atmos_vars"
        
        if var_data is None:
            continue
        
        var_profile = {
            "shape": list(var_data.shape),
            "global_stats": {
                "mean": float(np.mean(var_data)),
                "std": float(np.std(var_data)),
                "min": float(np.min(var_data)),
                "max": float(np.max(var_data)),
                "median": float(np.median(var_data)),
                "percentile_5": float(np.percentile(var_data, 5)),
                "percentile_95": float(np.percentile(var_data, 95))
            },
            "timestep_stats": {}
        }
        
        for t_idx in profile_timesteps:
            if t_idx < var_data.shape[1]:

                if var_category == "surf_vars":
                    t_slice = var_data[:, t_idx, :, :]
                else:
                    t_slice = var_data[:, t_idx, :, :, :]

                var_profile["timestep_stats"][f"t{t_idx}"] = {
                    "mean": float(np.mean(t_slice)),
                    "std": float(np.std(t_slice)),
                    "min": float(np.min(t_slice)),
                    "max": float(np.max(t_slice))
                }
                

                if var_category == "atmos_vars":
                    levels = [0, t_slice.shape[1] // 2, t_slice.shape[1] - 1]
                    level_stats = {}
                    
                    for l_idx in levels:
                        l_slice = t_slice[:, l_idx, :, :]
                        level_stats[f"l{l_idx}"] = {
                            "mean": float(np.mean(l_slice)),
                            "std": float(np.std(l_slice))
                        }
                    
                    var_profile["timestep_stats"][f"t{t_idx}"]["levels"] = level_stats

        profile["variables"][var_name] = var_profile

    if all(x is not None for x in [t_data, u_data, v_data, z_data, q_data]):
        t_idx = profile_timesteps[1] if len(profile_timesteps) > 1 else profile_timesteps[0]
        if t_idx < t_data.shape[1]:
            l_idx = min(6, t_data.shape[2] - 1)
            
            try:

                t_mid = t_data[0, t_idx, l_idx, :, :]
                t_upper = t_data[0, t_idx, max(0, l_idx-2), :, :]
                u_diff = u_data[0, t_idx, max(0, l_idx-2), :, :] - u_data[0, t_idx, l_idx, :, :]
                v_diff = v_data[0, t_idx, max(0, l_idx-2), :, :] - v_data[0, t_idx, l_idx, :, :]
                
                t_grad_y, t_grad_x = np.gradient(t_mid)

                thermal_wind_corr_u = float(np.corrcoef(t_grad_y.flatten(), u_diff.flatten())[0, 1])
                thermal_wind_corr_v = float(np.corrcoef(t_grad_x.flatten(), v_diff.flatten())[0, 1])

                z_grad_y, z_grad_x = np.gradient(z_data[0, t_idx, l_idx, :, :])
                geo_balance_corr_u = float(np.corrcoef(z_grad_y.flatten(), u_data[0, t_idx, l_idx, :, :].flatten())[0, 1])
                geo_balance_corr_v = float(np.corrcoef(z_grad_x.flatten(), v_data[0, t_idx, l_idx, :, :].flatten())[0, 1])
                

                if l_idx + 2 < t_data.shape[2]:
                    lapse_rate = float(np.mean(t_data[0, t_idx, l_idx, :, :] - t_data[0, t_idx, l_idx+2, :, :]))
                else:
                    lapse_rate = 0.0
                
                q_mid = q_data[0, t_idx, l_idx, :, :]
                wind_speed = np.sqrt(u_data[0, t_idx, l_idx, :, :]**2 + v_data[0, t_idx, l_idx, :, :]**2)
                moisture_advection = float(np.corrcoef(q_mid.flatten(), wind_speed.flatten())[0, 1])
                
                profile["physical_checks"] = {
                    "thermal_wind_u": thermal_wind_corr_u,
                    "thermal_wind_v": thermal_wind_corr_v,
                    "geostrophic_u": geo_balance_corr_u,
                    "geostrophic_v": geo_balance_corr_v, 
                    "lapse_rate": lapse_rate,
                    "moisture_advection": moisture_advection
                }
            except Exception as e:
                logger.warning(f"Error computing physical checks: {e}")
                profile["physical_checks"] = {"error": str(e)}
    
    if all(x is not None for x in [t2m_data, u10_data, v10_data, msl_data]):
        try:
            t_idx = profile_timesteps[1] if len(profile_timesteps) > 1 else profile_timesteps[0]
            if t_idx < t2m_data.shape[1]:
                msl_grad_y, msl_grad_x = np.gradient(msl_data[0, t_idx, :, :])
                surf_geo_u = float(np.corrcoef(msl_grad_y.flatten(), u10_data[0, t_idx, :, :].flatten())[0, 1])
                surf_geo_v = float(np.corrcoef(msl_grad_x.flatten(), v10_data[0, t_idx, :, :].flatten())[0, 1])
                
                t2m_grad_y, t2m_grad_x = np.gradient(t2m_data[0, t_idx, :, :])
                t2m_wind_u = float(np.corrcoef(t2m_grad_y.flatten(), u10_data[0, t_idx, :, :].flatten())[0, 1])
                t2m_wind_v = float(np.corrcoef(t2m_grad_x.flatten(), v10_data[0, t_idx, :, :].flatten())[0, 1])
                
                profile["physical_checks"].update({
                    "surface_geostrophic_u": surf_geo_u,
                    "surface_geostrophic_v": surf_geo_v,
                    "surface_thermal_u": t2m_wind_u,
                    "surface_thermal_v": t2m_wind_v
                })
        except Exception as e:
            logger.warning(f"Error computing surface physical checks: {e}")
    
    canonical_json = json.dumps(profile, sort_keys=True)
    hash_obj = hashlib.sha256(canonical_json.encode())
    result_hash = hash_obj.hexdigest()
    
    elapsed_time = time.time() - start_time
    logger.info(f"Computed verification hash in {elapsed_time:.3f} seconds using statistical profiling with physical checks")
    logger.info(f"Hash result: {result_hash}")
    
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


async def verify_forecast_hash(
    zarr_store_url: str,
    metadata: Dict[str, Any],
    variables: List[str],
    timesteps: List[int],
    headers: Optional[Dict[str, str]] = None,
    job_id: Optional[str] = "unknown_job"
) -> Dict[str, Any]:
    """
    (TEMPORARY ANALYSIS MODE) 
    Fetches data slices, computes statistical profile, physical checks, generates plots,
    and saves the resulting profile to a JSON file.
    Returns a dictionary containing the computed profile or an error.
    All intensive operations run in an executor.
    """
    start_time_total_verify = time.time()
    logger.info(f"Starting forecast analysis for job {job_id}: {zarr_store_url}")
    
    storage_options = {}
    if headers:
        storage_options['headers'] = headers
    
    ds = None
    try:
        ds = await open_remote_zarr_dataset(zarr_store_url, variables, storage_options=storage_options)
        if ds is None:
            err_msg = f"Dataset open failed for job {job_id} at {zarr_store_url}."
            logger.error(err_msg)
            return {"error": err_msg, "job_id": job_id, "status": "dataset_open_failed"}
    except Exception as e:
        err_msg = f"Exception opening dataset for job {job_id} at {zarr_store_url}: {e}"
        logger.error(err_msg, exc_info=True)
        return {"error": err_msg, "job_id": job_id, "status": "dataset_open_exception"}
    
    def _compute_analysis_profile(current_ds, current_metadata, current_variables, current_job_id):
        inner_start_time = time.time()
        plot_idx = 0 
        profile = {
            "job_id": current_job_id,
            "zarr_store_url": zarr_store_url, 
            "analysis_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "metadata": {
                "date": current_metadata.get("time")[0].strftime("%Y-%m-%d"),
                "model": current_metadata.get("source_model", "aurora"),
                "resolution": current_metadata.get("resolution", 0.25),
                "hash_version": HASH_VERSION 
            },
            "variables": {},
            "physical_checks": {}
        }
        
        try:
            max_t = current_ds.sizes.get("time", 0)
            profile_timesteps_inner = sorted(list(set([0, max_t // 2, max_t - 1]))) if max_t > 0 else [0]
            profile["profiled_timesteps"] = profile_timesteps_inner 
            mid_profile_timestep_idx = profile_timesteps_inner[1] if len(profile_timesteps_inner) > 1 else profile_timesteps_inner[0]
            
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
            
            for var_name_canonical in sorted(current_variables):
                if var_name_canonical not in var_mapping or var_mapping[var_name_canonical] not in current_ds:
                    logger.warning(f"Job {current_job_id}: Variable {var_name_canonical} not found or not mapped in dataset. Skipping.")
                    continue
                
                ds_actual_name = var_mapping[var_name_canonical]
                var_data_xr = current_ds[ds_actual_name] 
                is_3d_var = "pressure_level" in var_data_xr.sizes
                var_profile_data = {"shape": list(var_data_xr.shape), "global_stats": {}}
                
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=RuntimeWarning)
                    current_global_stats = {
                        "mean": float(var_data_xr.mean().compute().data),
                        "std": float(var_data_xr.std().compute().data),
                        "min": float(var_data_xr.min().compute().data),
                        "max": float(var_data_xr.max().compute().data)
                    }
                    try:
                        current_global_stats["median"] = float(var_data_xr.median().compute().data)
                        current_global_stats["p05"] = float(var_data_xr.quantile(0.05).compute().data)
                        current_global_stats["p95"] = float(var_data_xr.quantile(0.95).compute().data)
                    except NotImplementedError:
                        logger.warning(f"Job {current_job_id}: Dask median/quantile not implemented for full array {ds_actual_name}. Setting to NaN.")
                        current_global_stats["median"] = np.nan; current_global_stats["p05"] = np.nan; current_global_stats["p95"] = np.nan
                    except Exception as e_stat_detail:
                        logger.warning(f"Job {current_job_id}: Error computing median/quantiles for {ds_actual_name}: {e_stat_detail}. Setting to NaN.")
                        current_global_stats["median"] = np.nan; current_global_stats["p05"] = np.nan; current_global_stats["p95"] = np.nan
                    var_profile_data["global_stats"] = current_global_stats
                
                var_profile_data["timestep_stats"] = {}
                current_t_slice_for_plot = None 

                for t_idx_loop_val in profile_timesteps_inner:
                    if t_idx_loop_val < current_ds.sizes.get("time", 0):
                        t_slice_xr = var_data_xr.isel(time=t_idx_loop_val) 
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", category=RuntimeWarning)
                            var_profile_data["timestep_stats"][f"t{t_idx_loop_val}"] = {
                                "mean": float(t_slice_xr.mean().compute().data),
                                "std": float(t_slice_xr.std().compute().data),
                                "min": float(t_slice_xr.min().compute().data),
                                "max": float(t_slice_xr.max().compute().data)
                            }
                        if is_3d_var:
                            num_levels_slice = t_slice_xr.sizes["pressure_level"]
                            levels_to_sample_inner = sorted(list(set([0, num_levels_slice // 2, num_levels_slice - 1])))
                            level_stats_inner = {}
                            for l_idx_loop in levels_to_sample_inner:
                                l_slice_level_xr = t_slice_xr.isel(pressure_level=l_idx_loop) 
                                with warnings.catch_warnings():
                                    warnings.simplefilter("ignore", category=RuntimeWarning)
                                    level_stats_inner[f"l{l_idx_loop}"] = {
                                        "mean": float(l_slice_level_xr.mean().compute().data),
                                        "std": float(l_slice_level_xr.std().compute().data)
                                    }
                            var_profile_data["timestep_stats"][f"t{t_idx_loop_val}"]["levels"] = level_stats_inner
                        if t_idx_loop_val == mid_profile_timestep_idx: 
                            current_t_slice_for_plot = t_slice_xr 
                profile["variables"][var_name_canonical] = var_profile_data

                if ENABLE_PLOTTING and var_name_canonical in ["2t", "t", "z", "u"] and current_t_slice_for_plot is not None:
                    try:
                        plt.figure(figsize=(12, 6))
                        data_to_plot_base = current_t_slice_for_plot 
                        plot_data_final_xr = data_to_plot_base.isel(pressure_level=data_to_plot_base.sizes['pressure_level'] // 2).load() if is_3d_var else data_to_plot_base.load()
                        plot_data_final_np = plot_data_final_xr.data
                        if len(plot_data_final_np.shape) > 2 and plot_data_final_np.shape[0] == 1: 
                            plot_data_final_np = plot_data_final_np[0]
                        lons = plot_data_final_xr.coords.get('lon', np.arange(plot_data_final_np.shape[1]))
                        lats = plot_data_final_xr.coords.get('lat', np.arange(plot_data_final_np.shape[0]))
                        min_val, max_val = float(np.nanmin(plot_data_final_np)), float(np.nanmax(plot_data_final_np))
                        if min_val == max_val: min_val -= 1; max_val +=1
                        norm = Normalize(vmin=min_val, vmax=max_val)
                        plt.contourf(lons, lats, plot_data_final_np, cmap='RdBu_r', norm=norm, levels=20, origin='lower')
                        plt.title(f"Job {current_job_id}: {ds_actual_name} at T={mid_profile_timestep_idx} (Mid Level if 3D)")
                        plt.colorbar(label=f"{ds_actual_name} units")
                        plt.xlabel("Longitude"); plt.ylabel("Latitude")
                        plot_filename = os.path.join(PLOT_SAVE_DIR, f"{current_job_id}_plot{plot_idx}_{ds_actual_name}_T{mid_profile_timestep_idx}.png")
                        plt.savefig(plot_filename); plt.close(); plot_idx += 1
                        logger.info(f"Saved plot for job {current_job_id}: {plot_filename}")
                    except Exception as plot_err:
                        logger.warning(f"Plotting error for {ds_actual_name} (job {current_job_id}): {plot_err}")
            
            phys_vars_present = {v: (v in var_mapping and var_mapping[v] in current_ds) for v in ["t", "u", "v", "z", "q", "2t", "10u", "10v", "msl"]}

            if all(phys_vars_present[v] for v in ["t", "u", "v", "z"]):
                try:
                    t_idx_phys_check = mid_profile_timestep_idx
                    level_dim_name_check = "pressure_level"
                    t_phys_xr = current_ds[var_mapping["t"]].isel(time=t_idx_phys_check) 
                    u_phys_xr = current_ds[var_mapping["u"]].isel(time=t_idx_phys_check)
                    v_phys_xr = current_ds[var_mapping["v"]].isel(time=t_idx_phys_check)
                    z_phys_xr = current_ds[var_mapping["z"]].isel(time=t_idx_phys_check)
                    mid_level_idx_phys = t_phys_xr.sizes[level_dim_name_check] // 2

                    if ENABLE_PLOTTING and phys_vars_present["u"] and phys_vars_present["v"] and phys_vars_present["z"]:
                        try:
                            u_contour_slice = u_phys_xr.isel({level_dim_name_check: mid_level_idx_phys}).load()
                            v_contour_slice = v_phys_xr.isel({level_dim_name_check: mid_level_idx_phys}).load()
                            z_contour_slice = z_phys_xr.isel({level_dim_name_check: mid_level_idx_phys}).load()
                            u_data_np = u_contour_slice.data; v_data_np = v_contour_slice.data; z_data_np = z_contour_slice.data
                            if u_data_np.ndim > 2 and u_data_np.shape[0] == 1: u_data_np = u_data_np[0]
                            if v_data_np.ndim > 2 and v_data_np.shape[0] == 1: v_data_np = v_data_np[0]
                            if z_data_np.ndim > 2 and z_data_np.shape[0] == 1: z_data_np = z_data_np[0]
                            lons_contour = z_contour_slice.coords.get('lon', np.arange(z_data_np.shape[1]))
                            lats_contour = z_contour_slice.coords.get('lat', np.arange(z_data_np.shape[0]))
                            plt.figure(figsize=(14, 7))
                            contour_levels_z = np.arange(int(z_data_np.min()/10)*10 - 60, int(z_data_np.max()/10)*10 + 120, 60) 
                            CS = plt.contour(lons_contour, lats_contour, z_data_np, levels=contour_levels_z, colors='k', linewidths=0.8, origin='lower')
                            plt.clabel(CS, CS.levels, inline=True, fontsize=8, fmt='%1.0f')
                            skip_factor = max(1, z_data_np.shape[1] // 72) # Aim for ~72 vectors across longitude
                            skip = (slice(None, None, skip_factor), slice(None, None, skip_factor)) 
                            plt.quiver(lons_contour[skip[1]], lats_contour[skip[0]], 
                                       u_data_np[skip], v_data_np[skip], 
                                       color='blue', scale_units='inches', scale= (u_data_np.max() / 2.0) if u_data_np.max() > 0 else None, headwidth=4, headlength=5, width=0.003, minshaft=1, minlength=1)
                            plt.title(f"Job {current_job_id}: Wind Vectors & Geopotential Height at ~500hPa, T={t_idx_phys_check}")
                            plt.xlabel("Longitude"); plt.ylabel("Latitude")
                            plot_filename = os.path.join(PLOT_SAVE_DIR, f"{current_job_id}_plot{plot_idx}_wind_geopotential_T{t_idx_phys_check}.png")
                            plt.savefig(plot_filename); plt.close(); plot_idx +=1;
                            logger.info(f"Saved Wind/Geopotential plot for job {current_job_id}")
                        except Exception as plot_err:
                            logger.warning(f"Wind/Geopotential plot error (job {current_job_id}): {plot_err}")

                    if mid_level_idx_phys > 1:
                        t_mid_phys_xr = t_phys_xr.isel({level_dim_name_check: mid_level_idx_phys})
                        u_shear_phys = u_phys_xr.isel({level_dim_name_check: mid_level_idx_phys-2}) - u_phys_xr.isel({level_dim_name_check: mid_level_idx_phys})
                        v_shear_phys = v_phys_xr.isel({level_dim_name_check: mid_level_idx_phys-2}) - v_phys_xr.isel({level_dim_name_check: mid_level_idx_phys})
                        t_grad_y_phys, t_grad_x_phys = np.gradient(t_mid_phys_xr.load().data) 
                        t_grad_y_xr = xr.DataArray(t_grad_y_phys, dims=t_mid_phys_xr.dims, coords=t_mid_phys_xr.coords)
                        t_grad_x_xr = xr.DataArray(t_grad_x_phys, dims=t_mid_phys_xr.dims, coords=t_mid_phys_xr.coords)
                        profile["physical_checks"]["thermal_wind_u"] = float(xs.pearson_r(t_grad_y_xr, u_shear_phys.compute(), dim=["lat", "lon"]).compute().data)
                        profile["physical_checks"]["thermal_wind_v"] = float(xs.pearson_r(t_grad_x_xr, v_shear_phys.compute(), dim=["lat", "lon"]).compute().data)
                        if ENABLE_PLOTTING: 
                            try:
                                fig, axs_tw = plt.subplots(1, 2, figsize=(12, 5))
                                axs_tw[0].hist2d(t_grad_y_xr.data.flatten(), u_shear_phys.load().data.flatten(), bins=50, norm=Normalize()); axs_tw[0].set_title(f'Job {current_job_id}: dT/dy vs U-Shear')
                                axs_tw[1].hist2d(t_grad_x_xr.data.flatten(), v_shear_phys.load().data.flatten(), bins=50, norm=Normalize()); axs_tw[1].set_title(f'Job {current_job_id}: dT/dx vs V-Shear')
                                plt.tight_layout(); plt.savefig(os.path.join(PLOT_SAVE_DIR, f"{current_job_id}_plot{plot_idx}_thermal_wind.png")); plt.close(); plot_idx +=1;
                                logger.info(f"Saved thermal wind plot for job {current_job_id}")
                            except Exception as plot_err: logger.warning(f"Plotting thermal wind error (job {current_job_id}): {plot_err}")

                    z_mid_phys_xr = z_phys_xr.isel({level_dim_name_check: mid_level_idx_phys})
                    u_mid_phys_xr = u_phys_xr.isel({level_dim_name_check: mid_level_idx_phys})
                    v_mid_phys_xr = v_phys_xr.isel({level_dim_name_check: mid_level_idx_phys})
                    z_grad_y_phys, z_grad_x_phys = np.gradient(z_mid_phys_xr.load().data)
                    z_grad_y_xr = xr.DataArray(z_grad_y_phys, dims=z_mid_phys_xr.dims, coords=z_mid_phys_xr.coords)
                    z_grad_x_xr = xr.DataArray(z_grad_x_phys, dims=z_mid_phys_xr.dims, coords=z_mid_phys_xr.coords)
                    profile["physical_checks"]["geostrophic_u"] = float(xs.pearson_r(z_grad_y_xr, u_mid_phys_xr.compute(), dim=["lat", "lon"]).compute().data)
                    profile["physical_checks"]["geostrophic_v"] = float(xs.pearson_r(z_grad_x_xr, v_mid_phys_xr.compute(), dim=["lat", "lon"]).compute().data)
                    if ENABLE_PLOTTING: 
                         try:
                            fig, axs_gb = plt.subplots(1, 2, figsize=(12, 5))
                            axs_gb[0].hist2d(z_grad_y_xr.data.flatten(), u_mid_phys_xr.load().data.flatten(), bins=50, norm=Normalize()); axs_gb[0].set_title(f'Job {current_job_id}: dZ/dy vs U')
                            axs_gb[1].hist2d(z_grad_x_xr.data.flatten(), v_mid_phys_xr.load().data.flatten(), bins=50, norm=Normalize()); axs_gb[1].set_title(f'Job {current_job_id}: dZ/dx vs V')
                            plt.tight_layout(); plt.savefig(os.path.join(PLOT_SAVE_DIR, f"{current_job_id}_plot{plot_idx}_geostrophic.png")); plt.close(); plot_idx +=1;
                            logger.info(f"Saved geostrophic plot for job {current_job_id}")
                         except Exception as plot_err: logger.warning(f"Plotting geostrophic error (job {current_job_id}): {plot_err}")

                    if mid_level_idx_phys + 2 < t_phys_xr.sizes[level_dim_name_check]:
                        t_mid_lev_xr = t_phys_xr.isel({level_dim_name_check: mid_level_idx_phys})
                        t_lower_phys_xr = t_phys_xr.isel({level_dim_name_check: mid_level_idx_phys+2})
                        profile["physical_checks"]["lapse_rate"] = float((t_mid_lev_xr - t_lower_phys_xr).mean().compute().data)
                    
                    if phys_vars_present["q"]:
                        q_phys_xr_slice = current_ds[var_mapping["q"]].isel(time=t_idx_phys_check, pressure_level=mid_level_idx_phys)
                        wind_speed_phys_xr = (u_mid_phys_xr**2 + v_mid_phys_xr**2)**0.5 
                        profile["physical_checks"]["moisture_advection"] = float(xs.pearson_r(q_phys_xr_slice.compute(), wind_speed_phys_xr.compute(), dim=["lat", "lon"]).compute().data)
                    
                    if len(profile_timesteps_inner) > 1:
                        try:
                            u_t0_phys = current_ds[var_mapping["u"]].isel(time=profile_timesteps_inner[0], pressure_level=mid_level_idx_phys)
                            u_t1_phys = current_ds[var_mapping["u"]].isel(time=profile_timesteps_inner[1], pressure_level=mid_level_idx_phys)
                            v_t0_phys = current_ds[var_mapping["v"]].isel(time=profile_timesteps_inner[0], pressure_level=mid_level_idx_phys)
                            v_t1_phys = current_ds[var_mapping["v"]].isel(time=profile_timesteps_inner[1], pressure_level=mid_level_idx_phys)
                            profile["physical_checks"]["temporal_consistency_u"] = float(xs.rmse(u_t0_phys, u_t1_phys, dim=["lat", "lon"]).compute().data)
                            profile["physical_checks"]["temporal_consistency_v"] = float(xs.rmse(v_t0_phys, v_t1_phys, dim=["lat", "lon"]).compute().data)
                            if ENABLE_PLOTTING: 
                                try:
                                    fig, axs_tc = plt.subplots(1, 2, figsize=(12,5))
                                    u_t0_np = u_t0_phys.load().data; u_t1_np = u_t1_phys.load().data
                                    if u_t0_np.ndim > 2 and u_t0_np.shape[0] == 1: u_t0_np = u_t0_np[0]
                                    if u_t1_np.ndim > 2 and u_t1_np.shape[0] == 1: u_t1_np = u_t1_np[0]
                                    u_minmax = (min(u_t0_np.min(), u_t1_np.min()), max(u_t0_np.max(), u_t1_np.max()))
                                    norm_tc = Normalize(vmin=u_minmax[0], vmax=u_minmax[1])
                                    axs_tc[0].imshow(u_t0_np, cmap='RdBu_r', norm=norm_tc, origin='lower'); axs_tc[0].set_title(f'Job {current_job_id}: U-wind T0 MidLvl')
                                    axs_tc[1].imshow(u_t1_np, cmap='RdBu_r', norm=norm_tc, origin='lower'); axs_tc[1].set_title(f'Job {current_job_id}: U-wind T1 MidLvl')
                                    plt.tight_layout(); plt.savefig(os.path.join(PLOT_SAVE_DIR, f"{current_job_id}_plot{plot_idx}_temporal_u.png")); plt.close(); plot_idx +=1;
                                    logger.info(f"Saved temporal U plot for job {current_job_id}")
                                except Exception as plot_err: logger.warning(f"Plotting temporal U error (job {current_job_id}): {plot_err}")
                        except Exception as e_skill_check:
                            logger.warning(f"Validator error computing temporal consistency (job {current_job_id}): {e_skill_check}")
                except Exception as e_phys_outer:
                    logger.warning(f"Validator error computing atmospheric physical checks (job {current_job_id}): {e_phys_outer}")
                    profile["physical_checks"]["atmos_error"] = str(e_phys_outer)
            
            if all(phys_vars_present[v] for v in ["2t", "10u", "10v", "msl"]):
                try:
                    t_idx_surf_check = mid_profile_timestep_idx
                    t2m_surf_xr = current_ds[var_mapping["2t"]].isel(time=t_idx_surf_check)
                    u10_surf_xr = current_ds[var_mapping["10u"]].isel(time=t_idx_surf_check)
                    v10_surf_xr = current_ds[var_mapping["10v"]].isel(time=t_idx_surf_check)
                    msl_surf_xr = current_ds[var_mapping["msl"]].isel(time=t_idx_surf_check)
                    
                    msl_grad_y_np, msl_grad_x_np = np.gradient(msl_surf_xr.load().data)
                    t2m_grad_y_np, t2m_grad_x_np = np.gradient(t2m_surf_xr.load().data)
                    msl_grad_y_xr = xr.DataArray(msl_grad_y_np, dims=msl_surf_xr.dims, coords=msl_surf_xr.coords)
                    msl_grad_x_xr = xr.DataArray(msl_grad_x_np, dims=msl_surf_xr.dims, coords=msl_surf_xr.coords)
                    t2m_grad_y_xr = xr.DataArray(t2m_grad_y_np, dims=t2m_surf_xr.dims, coords=t2m_surf_xr.coords)
                    t2m_grad_x_xr = xr.DataArray(t2m_grad_x_np, dims=t2m_surf_xr.dims, coords=t2m_surf_xr.coords)
                    
                    profile["physical_checks"]["surface_geostrophic_u"] = float(xs.pearson_r(msl_grad_y_xr, u10_surf_xr.compute(), dim=["lat", "lon"]).compute().data)
                    profile["physical_checks"]["surface_geostrophic_v"] = float(xs.pearson_r(msl_grad_x_xr, v10_surf_xr.compute(), dim=["lat", "lon"]).compute().data)
                    profile["physical_checks"]["surface_thermal_u"] = float(xs.pearson_r(t2m_grad_y_xr, u10_surf_xr.compute(), dim=["lat", "lon"]).compute().data)
                    profile["physical_checks"]["surface_thermal_v"] = float(xs.pearson_r(t2m_grad_x_xr, v10_surf_xr.compute(), dim=["lat", "lon"]).compute().data)
                    
                    if len(profile_timesteps_inner) > 1:
                        t2m_t0_surf = current_ds[var_mapping["2t"]].isel(time=profile_timesteps_inner[0])
                        t2m_t1_surf = current_ds[var_mapping["2t"]].isel(time=profile_timesteps_inner[1])
                        profile["physical_checks"]["temp_pattern_correlation"] = float(xs.pearson_r(t2m_t0_surf, t2m_t1_surf, dim=["lat", "lon"]).compute().data)
                        if ENABLE_PLOTTING: 
                            try:
                                t2m_plot_data_np = t2m_surf_xr.load().data
                                if t2m_plot_data_np.ndim > 2 and t2m_plot_data_np.shape[0] == 1: t2m_plot_data_np = t2m_plot_data_np[0]
                                plt.figure(figsize=(12, 6)); plt.contourf( 
                                    t2m_surf_xr.coords.get('lon', np.arange(t2m_plot_data_np.shape[1])),
                                    t2m_surf_xr.coords.get('lat', np.arange(t2m_plot_data_np.shape[0])),
                                    t2m_plot_data_np, cmap='RdBu_r', levels=20, origin='lower'); 
                                plt.title(f'Job {current_job_id}: 2m Temperature at T={t_idx_surf_check}'); plt.colorbar(label='2T (K)')
                                plt.xlabel("Longitude"); plt.ylabel("Latitude")
                                plt.savefig(os.path.join(PLOT_SAVE_DIR, f"{current_job_id}_plot{plot_idx}_2t_map.png")); plt.close(); plot_idx += 1;
                                logger.info(f"Saved 2T map for job {current_job_id}")

                                if phys_vars_present["t"]:
                                    temp_prof_xr = current_ds[var_mapping['t']].isel(time=t_idx_surf_check, lat=current_ds.sizes['lat']//2, lon=current_ds.sizes['lon']//2)
                                    if level_dim_name_check in temp_prof_xr.sizes:
                                        temp_prof_np = temp_prof_xr.load().data
                                        plevels_np = temp_prof_xr[level_dim_name_check].load().data 
                                        plt.figure(figsize=(6,8)); plt.plot(temp_prof_np, plevels_np); plt.gca().invert_yaxis();
                                        plt.ylabel('Pressure (hPa)'); plt.xlabel('Temperature (K)'); plt.title(f'Job {current_job_id}: Temp Profile MidLat/Lon T={t_idx_surf_check}')
                                        plt.savefig(os.path.join(PLOT_SAVE_DIR, f"{current_job_id}_plot{plot_idx}_temp_profile.png")); plt.close(); plot_idx +=1;
                                        logger.info(f"Saved temp profile plot for job {current_job_id}")
                            except Exception as plot_err: logger.warning(f"Plotting surface/profile error (job {current_job_id}): {plot_err}")
                except Exception as e_surf_phys_outer:
                    logger.warning(f"Validator error computing surface physical checks (job {current_job_id}): {e_surf_phys_outer}")
                    profile["physical_checks"]["surface_error"] = str(e_surf_phys_outer)
            
            profile["status"] = "analysis_complete"
            profile["computation_time_seconds"] = time.time() - inner_start_time
            return profile 
            
        except Exception as e_inner_fatal:
            logger.error(f"Fatal error inside _compute_analysis_profile for job {current_job_id}: {e_inner_fatal}", exc_info=True)
            return {"error": str(e_inner_fatal), "job_id": current_job_id, "status": "fatal_error_in_computation", "computation_time_seconds": time.time() - inner_start_time}
        finally:
            plt.close('all')

    try:
        computation_start_time = time.time()
        loop = asyncio.get_running_loop()
        analysis_result_profile = await loop.run_in_executor(
            None, 
            _compute_analysis_profile, 
            ds, metadata, variables, job_id 
        )
        
        if analysis_result_profile:
            try:
                ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                log_filename = os.path.join(ANALYSIS_LOG_DIR, f"job_{job_id}_profile_{ts}.json")
                with open(log_filename, 'w') as f:
                    json.dump(analysis_result_profile, f, indent=4)
                logger.info(f"Saved analysis profile for job {job_id} to: {log_filename}")
            except Exception as e_save:
                logger.error(f"Failed to save analysis profile for job {job_id} to file: {e_save}")

        if analysis_result_profile.get("status") != "analysis_complete":
            logger.error(f"Analysis profile computation failed for job {job_id} ({zarr_store_url}) with error: {analysis_result_profile.get('error')}")
            return analysis_result_profile 
            
        logger.info(f"Successfully computed analysis profile for job {job_id}.")
        logger.info(f"Analysis computation (executor part) for job {job_id} took: {analysis_result_profile.get('computation_time_seconds', -1):.3f} seconds")
        logger.info(f"Total time for verify_forecast_hash (analysis mode) for job {job_id}: {time.time() - start_time_total_verify:.3f} seconds")
        
        return analysis_result_profile 
        
    except Exception as e_executor_call:
        err_msg = f"Exception calling executor for analysis profile (job {job_id}, {zarr_store_url}): {e_executor_call}"
        logger.error(err_msg, exc_info=True)
        return {"error": err_msg, "job_id": job_id, "status": "executor_call_exception"}
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