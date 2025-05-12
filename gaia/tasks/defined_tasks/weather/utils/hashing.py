import hashlib
import json
import struct
from typing import Dict, List, Tuple, Any, Optional, Set, Union
import numpy as np
import xarray as xr
import fsspec
import pickle
from datetime import datetime, timedelta
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

logger = get_logger(__name__)

NUM_SAMPLES = 1000
HASH_VERSION = "1"

CANONICAL_VARS_FOR_HASHING = sorted([
    '2t', '10u', '10v', 'msl',
    't', 'u', 'v', 'q', 'z' 
])

logging.getLogger('fsspec').setLevel(logging.DEBUG)

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
            
        if category == "atmos_vars":
            t_idx = rng.choice(timesteps)
            level_idx = rng.choice(shape[2])
            lat_idx = rng.choice(shape[3])
            lon_idx = rng.choice(shape[4])
            
            sample_indices.append({
                "variable": var,
                "category": category,
                "timestep": int(t_idx),
                "level": int(level_idx),
                "lat": int(lat_idx),
                "lon": int(lon_idx)
            })
        else:
            t_idx = rng.choice(timesteps)
            lat_idx = rng.choice(shape[2])
            lon_idx = rng.choice(shape[3])
            
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
    Compute a verification hash for a forecast dataset (miner-side).
    
    Args:
        data: Dictionary with forecast data in Aurora-compatible format
        metadata: Dictionary with forecast metadata 
        variables: List of variables to include in hash
        timesteps: List of timestep indices to include in hash
        
    Returns:
        Hex string of the SHA-256 hash
    """
    forecast_date = metadata.get("time")[0].strftime("%Y-%m-%d")
    source_model = metadata.get("source_model", "aurora")
    grid_resolution = metadata.get("resolution", 0.25)
    num_variables = len(variables)
    
    data_shape = {
        category: {
            var_name: data_array.shape 
            for var_name, data_array in var_dict.items()
            if var_name in variables
        }
        for category, var_dict in data.items() 
        if category in ["surf_vars", "atmos_vars"]
    }
    
    seed = generate_deterministic_seed(
        forecast_date, source_model, grid_resolution, num_variables
    )
    
    rng = np.random.Generator(np.random.PCG64(seed))
    
    sample_indices = generate_sample_indices(
        rng, data_shape, variables, timesteps, NUM_SAMPLES
    )
    
    serialized_data = canonical_serialization(sample_indices, data)
    hash_obj = hashlib.sha256(serialized_data)

    return hash_obj.hexdigest()


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
    zarr_store_url = zarr_store_url.rstrip('/')
    logger.info(f"SYNC_ZARR_OPEN: Starting. Initial Memory: {get_current_memory_usage_mb():.2f} MB. Target URL: {zarr_store_url}")
    overall_start_time = time.time()

    should_verify_ssl = http_fs_kwargs.get("verify_ssl", True)
    if not should_verify_ssl:
        logger.warning("SYNC_ZARR_OPEN: SSL certificate verification is explicitly DISABLED. FOR TESTING ONLY.")
        fsspec_remote_opts = {"verify_ssl": False}
    else:
        logger.info("SYNC_ZARR_OPEN: SSL certificate verification is ENABLED.")
        fsspec_remote_opts = {"verify_ssl": True}

    if "headers" in http_fs_kwargs:
        fsspec_remote_opts["headers"] = http_fs_kwargs["headers"]
    
    protocol = "https"
    
    time_before_fs_init = time.time()
    try:
        logger.info(f"SYNC_ZARR_OPEN: Opening Zarr store at URL: {zarr_store_url}")
        fs = fsspec.filesystem(
            protocol,
            **fsspec_remote_opts
        )
        mapper = fs.get_mapper(zarr_store_url)
            
        logger.info(f"SYNC_ZARR_OPEN: Initialized filesystem and mapper in {time.time() - time_before_fs_init:.2f}s.")
    except Exception as e_fs_init:
        logger.error(f"SYNC_ZARR_OPEN: Failed to initialize filesystem: {e_fs_init!r}", exc_info=True)
        raise

    logger.info(f"SYNC_ZARR_OPEN: Memory before xr.open_dataset: {get_current_memory_usage_mb():.2f} MB.")
    logger.info("SYNC_ZARR_OPEN: Calling xr.open_dataset (engine='zarr', consolidated=True) with mapper.")
    time_before_xr_open = time.time()
    ds = None
    try:
        ds = xr.open_dataset(
            mapper,
            engine="zarr",
            consolidated=True,
            chunks={},
        )
        logger.info(f"SYNC_ZARR_OPEN: Successfully opened Zarr dataset in {time.time() - time_before_xr_open:.2f}s. Memory after: {get_current_memory_usage_mb():.2f} MB.")
        return ds
    except Exception as e_xr_open:
        logger.error(f"SYNC_ZARR_OPEN: Failed to open Zarr dataset with xarray: {e_xr_open!r}. Memory at failure: {get_current_memory_usage_mb():.2f} MB.", exc_info=True)
        if "KeyError: '.zmetadata'" in traceback.format_exc():
             logger.error("SYNC_ZARR_OPEN: Encountered KeyError for '.zmetadata'. Check if .zmetadata is accessible via the URL and miner permissions.")
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

    http_client_kwargs_for_fsspec = {
        "headers": (storage_options.get("headers") if storage_options else {}) or {},
        "verify_ssl": False
    }

    current_loop = asyncio.get_running_loop()
    ds = None
    try:
        logger.debug(f"Calling _synchronous_zarr_open for Zarr store {zarr_store_url} in executor...")
        ds = await current_loop.run_in_executor(
            None,
            _synchronous_zarr_open,
            zarr_store_url,
            http_client_kwargs_for_fsspec
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
    claimed_hash: str,
    metadata: Dict[str, Any],
    variables: List[str],
    timesteps: List[int],
    headers: Optional[Dict[str, str]] = None
) -> bool:
    """
    Verify a forecast hash against a claimed value (validator-side) by sampling from Zarr store.
    
    Args:
        zarr_store_url: URL to the Zarr store (.zarr directory)
        claimed_hash: Hash value claimed by the miner
        metadata: Dictionary with forecast metadata
        variables: List of variables to include in hash
        timesteps: List of timestep indices to include in hash
        headers: Optional HTTP headers for authentication
        
    Returns:
        True if the computed hash matches the claimed hash, False otherwise
    """
    storage_options = {}
    if headers:
        storage_options['headers'] = headers

    ds = None
    try:
        logger.debug(f"Calling open_remote_zarr_dataset with URL: {zarr_store_url} for hash verification.")
        ds = await open_remote_zarr_dataset(zarr_store_url, variables, storage_options=storage_options)
    except Exception as e:
        logger.error(f"Failed to open remote Zarr dataset {zarr_store_url} for hash verification: {e}", exc_info=True)
        return False
    
    if ds is None:
        logger.error(f"Opening remote Zarr dataset {zarr_store_url} returned None. Cannot verify hash.")
        return False

    try:
        logger.info(f"Starting efficient hash verification for {zarr_store_url}")
        var_mapping = {
            "t2m": ("surf_vars", "2t"), "u10": ("surf_vars", "10u"), "v10": ("surf_vars", "10v"), "msl": ("surf_vars", "msl"),
            "z": ("atmos_vars", "z"), "u": ("atmos_vars", "u"), "v": ("atmos_vars", "v"), 
            "t": ("atmos_vars", "t"), "q": ("atmos_vars", "q")
        }
        
        variables_for_hash = CANONICAL_VARS_FOR_HASHING
        timesteps_for_hash = timesteps
        hash_metadata_for_validator = {
            "time": metadata["time"],
            "source_model": metadata.get("source_model", "aurora"),
            "resolution": metadata.get("resolution", 0.25)
        }

        data_shape_validator = _get_data_shape_from_xarray_dataset(ds, variables_for_hash, var_mapping)
        if not data_shape_validator or (not data_shape_validator.get("surf_vars") and not data_shape_validator.get("atmos_vars")):
            logger.error("Could not determine shapes for any variables from the dataset. Cannot generate sample indices.")
            if ds is not None:
                ds.close()
                logger.debug(f"Closed xarray dataset for {zarr_store_url} after shape error.")
            return False

        seed = generate_deterministic_seed(
            hash_metadata_for_validator["time"][0].strftime("%Y-%m-%d"), 
            hash_metadata_for_validator["source_model"], 
            hash_metadata_for_validator["resolution"], 
            len(variables_for_hash)
        )
        rng = np.random.Generator(np.random.PCG64(seed))
        sample_indices = generate_sample_indices(rng, data_shape_validator, variables_for_hash, timesteps_for_hash, NUM_SAMPLES)

        if not sample_indices:
            logger.error("Failed to generate sample indices. Cannot compute hash.")
            if ds is not None:
                ds.close()
                logger.debug(f"Closed xarray dataset for {zarr_store_url} after sample index error.")
            return False

        serialized_data_validator = _efficient_canonical_serialization(sample_indices, ds, var_mapping)
        
        hash_obj = hashlib.sha256(serialized_data_validator)
        computed_hash_validator = hash_obj.hexdigest()

        logger.info(f"Validator computed hash: {computed_hash_validator}, Miner claimed hash: {claimed_hash}")
        return computed_hash_validator == claimed_hash

    except Exception as e_hash_verify:
        logger.error(f"Error during efficient hash verification for {zarr_store_url}: {e_hash_verify}", exc_info=True)
        return False
    finally:
        if ds is not None:
            ds.close()
            logger.debug(f"Closed xarray dataset for {zarr_store_url}")


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
    for aurora_var_name in variables_to_include:
        ds_var_name = None
        category = None
        for map_ds_name, (cat, aur_name) in var_mapping.items():
            if aur_name == aurora_var_name:
                ds_var_name = map_ds_name
                category = cat
                break
        
        if ds_var_name and ds_var_name in ds and category:
            actual_shape = ds[ds_var_name].shape
            data_shape[category][aurora_var_name] = (1,) + actual_shape 
            logger.debug(f"Shape for {aurora_var_name} (in ds as {ds_var_name}): {actual_shape} -> {(1,) + actual_shape} for sampling struct")
        else:
            logger.warning(f"Variable '{aurora_var_name}' (ds name: '{ds_var_name}') not found in dataset or category not identified. Cannot determine shape for hashing.")
    logger.info(f"_get_data_shape_from_xarray_dataset: Populated data_shape: {data_shape}")
    return data_shape


def _efficient_canonical_serialization(
    sample_indices: List[Dict[str, Any]],
    dataset: xr.Dataset,
    var_mapping: Dict[str, Tuple[str, str]]
) -> bytes:
    logger.info(f"EFFICIENT_SERIALIZATION: Starting serialization for {len(sample_indices)} sample indices.")
    overall_serialization_start_time = time.time()
    serialized = bytearray()
    sorted_indices = sorted(sample_indices, key=lambda x: (x["variable"], x["category"], x["timestep"], x.get("level", 0), x["lat"], x["lon"]))

    for i, idx_info in enumerate(sorted_indices):
        aurora_var_name = idx_info["variable"]
        category = idx_info["category"]
        
        ds_var_name = None
        for map_ds_name, (cat, aur_name) in var_mapping.items():
            if aur_name == aurora_var_name:
                ds_var_name = map_ds_name
                break
        
        log_prefix = f"EFFICIENT_SERIALIZATION: Index {i+1}/{len(sorted_indices)} (Var: {aurora_var_name}, DS_Var: {ds_var_name}, Cat: {category}, T: {idx_info['timestep']}, Lt: {idx_info['lat']}, Ln: {idx_info['lon']}"
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

            isel_kwargs = {
                time_dim: idx_info["timestep"],
                lat_dim: idx_info["lat"],
                lon_dim: idx_info["lon"]
            }
            if category == "atmos_vars":
                isel_kwargs[level_dim] = idx_info["level"]
            
            logger.debug(f"{log_prefix} - Attempting .isel() with kwargs: {isel_kwargs}")
            point_fetch_start_time = time.time()
            value = dataset[ds_var_name].isel(**isel_kwargs).data.item()
            point_fetch_duration = time.time() - point_fetch_start_time
            logger.debug(f"{log_prefix} - Successfully fetched point. Value: {value:.4f if isinstance(value, float) else value}. Time: {point_fetch_duration:.3f}s")
            serialized.extend(serialize_float(value))
        except Exception as e_serial:
            logger.warning(f"{log_prefix} - Error serializing point: {e_serial}. Using NaN.", exc_info=True)
            serialized.extend(serialize_float(float('nan')))
            
    total_serialization_duration = time.time() - overall_serialization_start_time
    logger.info(f"EFFICIENT_SERIALIZATION: Finished. Total bytes: {len(serialized)}. Total time: {total_serialization_duration:.3f}s")
    return bytes(serialized) 