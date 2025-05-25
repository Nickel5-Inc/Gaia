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
from cryptography.hazmat.primitives import serialization, hashes as crypto_hashes
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
import xxhash
import shutil
from substrateinterface.base import Keypair
from cryptography.exceptions import InvalidSignature

logger = get_logger(__name__)

NUM_SAMPLES = 1000
HASH_VERSION = "3.0-miner_placeholder"

ENABLE_PLOTTING = True
PLOT_SAVE_DIR = "./verification_plots"
if ENABLE_PLOTTING and not os.path.exists(PLOT_SAVE_DIR):
    os.makedirs(PLOT_SAVE_DIR)

ANALYSIS_LOG_DIR = "./analysis_logs"
if not os.path.exists(ANALYSIS_LOG_DIR):
    os.makedirs(ANALYSIS_LOG_DIR)

VERIFICATION_LOG_DIR = "./verification_logs" 
if not os.path.exists(VERIFICATION_LOG_DIR):
    os.makedirs(VERIFICATION_LOG_DIR)

CANONICAL_VARS_FOR_HASHING = sorted([
    '2t', '10u', '10v', 'msl',
    't', 'u', 'v', 'q', 'z' 
])

logging.getLogger('fsspec').setLevel(logging.WARNING)

# DEFAULT_MINER_SIGNING_KEY_PATH = "./miner_signing_key.pem" 
# DEFAULT_VALIDATOR_MINER_PUBKEY_PATH = "./miner_public_key.pem"

PROFILE_STRUCTURE_VERSION = "3.4.0-verify_on_read_mapper"

def get_current_memory_usage_mb():
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        return mem_info.rss / (1024 * 1024)
    except Exception:
        return -1

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


def generate_manifest_and_signature(
    zarr_store_path: Path,
    miner_hotkey_keypair: Keypair, 
    include_zarr_metadata_in_manifest: bool = True,
    chunk_hash_algo_name: str = "xxh64"
) -> Optional[Tuple[Dict[str, Any], bytes, str]]: 
    logger.info(f"Generating manifest (chunk_algo: {chunk_hash_algo_name}) and signature for Zarr: {zarr_store_path}")
    if not zarr_store_path.is_dir():
        logger.error(f"Zarr store path not found: {zarr_store_path}")
        return None
    chunk_hashes = {}
    try:
        for item_path in sorted(zarr_store_path.rglob('*')):
            if item_path.is_file():
                relative_path_str = str(item_path.relative_to(zarr_store_path))
                if relative_path_str in ["manifest.json", "manifest.sig"]:
                    continue
                if not include_zarr_metadata_in_manifest and \
                   (relative_path_str.startswith('.') or relative_path_str.endswith(('.zarray', '.zattrs', '.zgroup', '.zmetadata'))):
                    continue
                with open(item_path, 'rb') as f_chunk:
                    chunk_content = f_chunk.read()
                path_plus_content = relative_path_str.encode('utf-8') + b'\0' + chunk_content
                if chunk_hash_algo_name == "xxh64":
                    chunk_hashes[relative_path_str] = xxhash.xxh64(path_plus_content).hexdigest()
                elif chunk_hash_algo_name == "sha256":
                    chunk_hashes[relative_path_str] = hashlib.sha256(path_plus_content).hexdigest()
                else:
                    logger.error(f"Unsupported chunk_hash_algo_name: {chunk_hash_algo_name}")
                    return None
        
        manifest_data_to_sign = {
            "manifest_schema_version": "1.0", 
            "profile_structure_version": PROFILE_STRUCTURE_VERSION,
            "chunk_hash_algorithm": chunk_hash_algo_name,
            "signer_hotkey_ss58": miner_hotkey_keypair.ss58_address, 
            "files": chunk_hashes
        }
        manifest_json_bytes = json.dumps(manifest_data_to_sign, sort_keys=True, indent=4).encode('utf-8')
        manifest_content_sha256_hash = hashlib.sha256(manifest_json_bytes).hexdigest()

        try:
            signature_bytes = miner_hotkey_keypair.sign(manifest_json_bytes)
        except Exception as e_sign:
            logger.error(f"Failed to sign manifest: {e_sign}", exc_info=True)
            return None

        with open(zarr_store_path / "manifest.json", "wb") as f_manifest:
            f_manifest.write(manifest_json_bytes)
        with open(zarr_store_path / "manifest.sig", "wb") as f_sig:
            f_sig.write(signature_bytes)
        
        logger.info(f"Generated manifest for {miner_hotkey_keypair.ss58_address} at {zarr_store_path}")
        return manifest_data_to_sign, signature_bytes, manifest_content_sha256_hash
    except Exception as e:
        logger.error(f"Error generating manifest for {zarr_store_path}: {e}", exc_info=True)
        return None

def compute_verification_hash(
    data_xr: xr.Dataset, 
    metadata: Dict[str, Any],
    temp_zarr_dir: Path, 
    asset_base_name: str, 
    miner_hotkey_keypair: Keypair, 
    compression_level: int = 1,
    custom_chunks: Optional[Dict[str, Any]] = None,
    chunk_hash_algo: str = "xxh64"
) -> Optional[str]:
    start_time = time.time()
    logger.info(f"Miner ({miner_hotkey_keypair.ss58_address if miner_hotkey_keypair else 'UnknownKey'}): Starting {PROFILE_STRUCTURE_VERSION} (chunk_algo: {chunk_hash_algo}) hash generation for asset: {asset_base_name}")
    zarr_store_full_path = temp_zarr_dir / f"{asset_base_name}.zarr"
    try:
        logger.info(f"Saving dataset to Zarr store: {zarr_store_full_path}")
        if zarr_store_full_path.exists():
            logger.warning(f"Removing existing Zarr store at {zarr_store_full_path}")
            shutil.rmtree(zarr_store_full_path)
        zarr_store_full_path.parent.mkdir(parents=True, exist_ok=True)
        chunks = {}
        if 'time' in data_xr.dims: chunks['time'] = 1
        level_dim_name = next((d for d in data_xr.dims if d.lower() in ('level', 'pressure_level', 'isobaricinhpa', 'plev')), None)
        if level_dim_name: chunks[level_dim_name] = 1
        lat_dim_name = next((d for d in data_xr.dims if d.lower() in ('lat', 'latitude')), None)
        if lat_dim_name: chunks[lat_dim_name] = data_xr.sizes[lat_dim_name]
        lon_dim_name = next((d for d in data_xr.dims if d.lower() in ('lon', 'longitude')), None)
        if lon_dim_name: chunks[lon_dim_name] = data_xr.sizes[lon_dim_name]
        if custom_chunks:
            for dim, size in custom_chunks.items():
                actual_dim = dim 
                if actual_dim in data_xr.dims:
                    chunks[actual_dim] = data_xr.sizes[actual_dim] if size == -1 else size
        logger.info(f"Using chunking for Zarr: {chunks}")
        compressor = numcodecs.Blosc(cname='zstd', clevel=compression_level, shuffle=numcodecs.Blosc.BITSHUFFLE)
        encoding = {var_name: {'compressor': compressor} for var_name in data_xr.data_vars}
        data_xr.chunk(chunks).to_zarr(zarr_store_full_path, encoding=encoding, consolidated=True, compute=True)
        logger.info(f"Dataset successfully saved to Zarr: {zarr_store_full_path}")
        manifest_result = generate_manifest_and_signature(
            zarr_store_path=zarr_store_full_path,
            miner_hotkey_keypair=miner_hotkey_keypair, 
            include_zarr_metadata_in_manifest=True,
            chunk_hash_algo_name=chunk_hash_algo
        )
        if manifest_result is None:
            logger.error(f"Failed to generate manifest and signature for {zarr_store_full_path}")
            return None
        _manifest_dict, _signature_bytes, manifest_content_sha256_hash_hex = manifest_result
        elapsed_time = time.time() - start_time
        logger.info(f"Miner {PROFILE_STRUCTURE_VERSION} manifest hash for {asset_base_name} generated in {elapsed_time:.3f}s. Manifest SHA256 Hash: {manifest_content_sha256_hash_hex}")
        return manifest_content_sha256_hash_hex
    except Exception as e:
        logger.error(f"Error in miner compute_verification_hash for {asset_base_name}: {e}", exc_info=True)
        return None


def _compute_analysis_profile(current_ds: xr.Dataset, current_metadata: Dict, current_variables: List[str], _claimed_hash_unused: str, current_job_id: str, zarr_url_for_profile: str) -> Dict:
    inner_start_time = time.time()
    
    profile = {
        "job_id": current_job_id,
        "zarr_store_url": zarr_url_for_profile,
        "analysis_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "profile_schema_version": "1.1.0",
        "claimed_hash_received": _claimed_hash_unused,
        "dataset_info": {
            "variables": list(current_ds.data_vars),
            "dims": dict(current_ds.sizes),
            "coords": {str(k): list(map(str, v.values))[:5] + (["..."] if len(v.values) > 5 else []) for k, v in current_ds.coords.items()}, # Show first 5 coord values
        },
        "metadata_param": {
            "date": current_metadata.get("time")[0].strftime("%Y-%m-%d") if current_metadata.get("time") else "N/A",
            "model": current_metadata.get("source_model", "aurora"),
            "resolution": current_metadata.get("resolution", 0.25),
        },
        "performance_tests": [],
        "status": "test_pending",
        "notes": "Performing Zarr read performance tests."
    }

    time_dim = next((d for d in current_ds.dims if d.lower() in ['time', 'forecast_time', 'step']), 'time')
    lat_dim = next((d for d in current_ds.dims if d.lower() in ['lat', 'latitude', 'y']), 'lat')
    lon_dim = next((d for d in current_ds.dims if d.lower() in ['lon', 'longitude', 'x']), 'lon')
    level_dim_候補 = [d for d in current_ds.dims if d.lower() in ('level', 'pressure_level', 'isobaricinhpa', 'plev')]
    level_dim = level_dim_候補[0] if level_dim_候補 else None

    if time_dim not in current_ds.dims:
        profile["status"] = "test_error"; profile["notes"] = f"Dataset has no time dimension '{time_dim}'. Found: {list(current_ds.dims)}"
        profile["total_computation_time_seconds"] = time.time() - inner_start_time
        return profile
    
    max_t = current_ds.sizes[time_dim]
    if max_t == 0:
        profile["status"] = "test_error"; profile["notes"] = f"Dataset time dimension '{time_dim}' has size 0."
        profile["total_computation_time_seconds"] = time.time() - inner_start_time
        return profile

    profile_timesteps_indices = sorted(list(set([0, max_t // 2, max_t - 1 if max_t > 0 else 0])))
    profile_timesteps_indices = [min(idx, max_t -1) for idx in profile_timesteps_indices] # ensure valid
    
    var_mapping = {}
    standard_mapping = { 
        "2t": ["2t", "t2m"], "10u": ["10u", "u10"], "10v": ["10v", "v10"], "msl": ["msl", "mslp"],
        "t": ["t", "temperature"], "u": ["u", "u_component_of_wind"], "v": ["v", "v_component_of_wind"], 
        "q": ["q", "specific_humidity"], "z": ["z", "geopotential", "gh"]
    }
    ds_vars_lower = {v.lower(): v for v in current_ds.data_vars}
    for canonical_name, possible_names in standard_mapping.items():
        for pn in possible_names:
            if pn in current_ds.data_vars: var_mapping[canonical_name] = pn; break
            if pn.lower() in ds_vars_lower: var_mapping[canonical_name] = ds_vars_lower[pn.lower()]; break


    case1_results = {"case_name": "Single Full 2D Slice", "status": "skipped", "details": {}}
    vars_to_try_c1 = ["2t", "msl", "t"] 
    executed_case1 = False
    for var_c1_canonical in vars_to_try_c1:
        actual_var_name_c1 = var_mapping.get(var_c1_canonical)
        if actual_var_name_c1 and actual_var_name_c1 in current_ds:
            data_array_c1 = current_ds[actual_var_name_c1]
            selection_c1 = None
            selection_details_c1 = {time_dim: profile_timesteps_indices[len(profile_timesteps_indices)//2]} # Mid timestep

            is_3d_var = level_dim and level_dim in data_array_c1.dims
            
            if is_3d_var:
                if data_array_c1.sizes[level_dim] > 0:
                    mid_level_idx_c1 = data_array_c1.sizes[level_dim] // 2
                    selection_details_c1[level_dim] = mid_level_idx_c1
                    case1_results["details"]["slice_type"] = f"3D var ({actual_var_name_c1}), mid-level"
                else:
                    case1_results["details"]["slice_type"] = f"3D var ({actual_var_name_c1}), level dim size 0"
                    continue
            else:
                case1_results["details"]["slice_type"] = f"2D var ({actual_var_name_c1}), surface"

            try:
                selection_c1 = data_array_c1.isel(**selection_details_c1)
                time_start_c1 = time.time()
                loaded_slice_c1 = selection_c1.load() # Perform the read
                case1_results["time_taken_seconds"] = time.time() - time_start_c1
                case1_results["data_loaded_bytes"] = loaded_slice_c1.nbytes
                case1_results["details"]["variable_tested"] = actual_var_name_c1
                case1_results["details"]["selection_indices"] = {k: int(v) for k,v in selection_details_c1.items()}
                case1_results["details"]["slice_shape"] = list(loaded_slice_c1.shape)
                case1_results["status"] = "success"
                executed_case1 = True; break
            except Exception as e_c1_load:
                case1_results["status"] = "error"
                case1_results["error_message"] = f"Error loading slice for {actual_var_name_c1}: {str(e_c1_load)}"
                logger.warning(f"Job {current_job_id}: Case 1 Error - {case1_results['error_message']}")
                executed_case1 = True; break
    if not executed_case1:
        case1_results["notes"] = f"No suitable variable found or tested from options: {vars_to_try_c1}"
    profile["performance_tests"].append(case1_results)

    case2_results = {
        "case_name": "Multi-Variable, Multi-Level, Multi-Timestep Read",
        "status": "skipped",
        "details": {
            "variables_targeted": [], "timesteps_used_indices": [int(t) for t in profile_timesteps_indices], 
            "levels_used_indices": [], "total_slices_loaded": 0
        }
    }

    vars_for_case2_canonical = ["2t", "t", "u"]
    
    actual_vars_tested_c2 = []
    total_data_loaded_bytes_c2 = 0
    total_slices_loaded_c2 = 0
    
    levels_to_iterate_c2_indices = []
    if level_dim and current_ds.sizes.get(level_dim, 0) > 0:
        max_l = current_ds.sizes[level_dim]
        levels_to_iterate_c2_indices = sorted(list(set([0, max_l // 2, max_l - 1 if max_l > 0 else 0])))
        levels_to_iterate_c2_indices = [min(idx, max_l-1) for idx in levels_to_iterate_c2_indices] # ensure valid
        case2_results["details"]["levels_used_indices"] = [int(l) for l in levels_to_iterate_c2_indices]
    
    c2_ops_successful = True
    time_start_c2_total = time.time()

    try:
        for var_c2_canonical in vars_for_case2_canonical:
            actual_var_name_c2 = var_mapping.get(var_c2_canonical)
            if not actual_var_name_c2 or actual_var_name_c2 not in current_ds:
                logger.warning(f"Job {current_job_id}: Case 2 - Variable {var_c2_canonical} (mapped to {actual_var_name_c2}) not found. Skipping for this test.")
                continue
            
            actual_vars_tested_c2.append(actual_var_name_c2)
            data_array_c2 = current_ds[actual_var_name_c2]
            is_3d_var_c2 = level_dim and level_dim in data_array_c2.dims and data_array_c2.sizes[level_dim] > 0

            for t_idx_c2 in profile_timesteps_indices:
                if is_3d_var_c2:
                    for l_idx_c2 in levels_to_iterate_c2_indices:
                        selection_details_c2 = {time_dim: t_idx_c2, level_dim: l_idx_c2}
                        slice_to_load_c2 = data_array_c2.isel(**selection_details_c2)
                        loaded_slice_c2 = slice_to_load_c2.load()
                        total_data_loaded_bytes_c2 += loaded_slice_c2.nbytes
                        total_slices_loaded_c2 += 1
                else:
                    selection_details_c2 = {time_dim: t_idx_c2}
                    slice_to_load_c2 = data_array_c2.isel(**selection_details_c2)
                    loaded_slice_c2 = slice_to_load_c2.load()
                    total_data_loaded_bytes_c2 += loaded_slice_c2.nbytes
                    total_slices_loaded_c2 += 1
        
        if not actual_vars_tested_c2:
            case2_results["status"] = "skipped"
            case2_results["notes"] = f"No suitable variables found from target list: {vars_for_case2_canonical}"
            c2_ops_successful = False
        else:
            case2_results["status"] = "success"

    except Exception as e_c2_load:
        case2_results["status"] = "error"
        case2_results["error_message"] = f"Error during Case 2 data loading: {str(e_c2_load)}"
        logger.error(f"Job {current_job_id}: Case 2 Error - {case2_results['error_message']}", exc_info=True)
        c2_ops_successful = False

    if c2_ops_successful and actual_vars_tested_c2 :
        case2_results["time_taken_seconds"] = time.time() - time_start_c2_total
        case2_results["total_data_loaded_bytes"] = total_data_loaded_bytes_c2
        case2_results["details"]["total_slices_loaded"] = total_slices_loaded_c2
        case2_results["details"]["variables_actually_tested"] = actual_vars_tested_c2
    elif not actual_vars_tested_c2 and case2_results["status"] == "skipped":
        pass
    else:
        case2_results["time_taken_seconds"] = time.time() - time_start_c2_total
        if "notes" not in case2_results and "error_message" not in case2_results:
             case2_results["notes"] = "Test did not complete successfully or no variables were processed."

    profile["performance_tests"].append(case2_results)

    profile["status"] = "test_complete" if all(p.get("status") == "success" for p in profile["performance_tests"]) else "test_completed_with_issues"
    if any(p.get("status") == "error" for p in profile["performance_tests"]):
        profile["status"] = "test_errors_occurred"
    if not profile["performance_tests"]:
        profile["status"] = "test_error" 
        profile["notes"] = "No performance tests were executed or recorded."
        
    profile["total_computation_time_seconds"] = time.time() - inner_start_time
    
    try:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        log_filename = os.path.join(ANALYSIS_LOG_DIR, f"job_{current_job_id}_perf_profile_{ts}.json")
        with open(log_filename, 'w') as f_log:
            json.dump(profile, f_log, indent=4, default=str)
        logger.info(f"Job {current_job_id}: Performance profile saved to {log_filename}")
    except Exception as e_log_save:
        logger.error(f"Job {current_job_id}: Failed to save performance profile JSON: {e_log_save}")

    return profile


async def verify_manifest_and_get_trusted_store(
    zarr_store_url: str,
    claimed_manifest_content_hash: str,
    miner_hotkey_ss58: str,
    headers: Optional[Dict[str, str]] = None,
    job_id: Optional[str] = "unknown_job"
) -> Optional['VerifyingChunkMapper']:
    """
    Verifies manifest authenticity and integrity using miner's ss58 hotkey for signature.
    If successful, returns a VerifyingChunkMapper instance that performs on-read chunk hash checks.
    Returns None if initial manifest verification fails.
    This function runs its blocking fsspec calls in an executor.
    """
    start_time_total_verify = time.time()
    zarr_store_url_cleaned = zarr_store_url.rstrip('/') + ('/' if not zarr_store_url.endswith('/') and zarr_store_url.endswith('.zarr') else '')
    if not zarr_store_url_cleaned.endswith('/'): zarr_store_url_cleaned += '/'

    logger.info(f"Starting {PROFILE_STRUCTURE_VERSION} Manifest Verification for job {job_id} (Miner: {miner_hotkey_ss58}) for store: {zarr_store_url_cleaned}")
    
    verification_details_log = { 
        "job_id": job_id, "zarr_store_url_checked": zarr_store_url_cleaned,
        "claimed_manifest_content_hash": claimed_manifest_content_hash,
        "expected_signer_hotkey": miner_hotkey_ss58,
        "verification_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "steps": [], "final_status": "pending_preparation"
    }

    fs = None
    try:
        protocol = zarr_store_url_cleaned.split("://")[0]
        http_fs_kwargs = {}
        if headers: http_fs_kwargs["headers"] = headers
        http_fs_kwargs["ssl"] = False 
        fs = fsspec.filesystem(protocol, **http_fs_kwargs)
        verification_details_log["steps"].append({"name": "InitializeFilesystem", "status": "success"})
    except Exception as e_fs_init:
        logger.error(f"Job {job_id}: Failed to initialize fsspec filesystem for {protocol}: {e_fs_init}")
        verification_details_log["steps"].append({"name": "InitializeFilesystem", "status": "failed", "reason": str(e_fs_init)})
        verification_details_log["final_status"] = "error_fs_init"
        try:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            log_filename = os.path.join(VERIFICATION_LOG_DIR, f"job_{job_id}_manifest_verify_log_{ts}.json")
            verification_details_log["overall_manifest_verification_passed"] = False
            verification_details_log["total_verification_time_seconds"] = time.time() - start_time_total_verify
            with open(log_filename, 'w') as f: json.dump(verification_details_log, f, indent=4)
        except Exception as e_save_err: logger.error(f"Failed to save error log for job {job_id}: {e_save_err}")
        return None

    def _fetch_and_verify_manifest_sync_inner():
        trusted_manifest_dict_inner = None
        try:
            manifest_path = zarr_store_url_cleaned.rstrip('/') + "/manifest.json"
            sig_path = zarr_store_url_cleaned.rstrip('/') + "/manifest.sig"
            
            manifest_json_bytes = fs.cat(manifest_path)
            signature_bytes = fs.cat(sig_path)
            verification_details_log["steps"].append({"name": "FetchManifestAndSig", "status": "success", "manifest_size": len(manifest_json_bytes), "sig_size": len(signature_bytes)})

            computed_m_hash = hashlib.sha256(manifest_json_bytes).hexdigest()
            if computed_m_hash != claimed_manifest_content_hash:
                raise ValueError(f"Manifest content hash mismatch! Claimed: {claimed_manifest_content_hash}, Computed: {computed_m_hash}")
            verification_details_log["steps"].append({"name": "VerifyManifestContentHash", "status": "success"})
            
            trusted_manifest_dict_inner = json.loads(manifest_json_bytes.decode('utf-8'))
            manifest_signer = trusted_manifest_dict_inner.get("signer_hotkey_ss58")
            if not manifest_signer or manifest_signer != miner_hotkey_ss58:
                raise ValueError(f"Manifest signer hotkey mismatch. Expected: {miner_hotkey_ss58}, Got: {manifest_signer}")
            verification_details_log["steps"].append({"name": "VerifyManifestSigner", "status": "success"})

            verifier_kp = Keypair(ss58_address=miner_hotkey_ss58)
            if not verifier_kp.verify(manifest_json_bytes, signature_bytes):
                raise InvalidSignature("Manifest signature verification failed (Keypair.verify)")
            verification_details_log["steps"].append({"name": "VerifyManifestSignature", "status": "success"})
            
            verification_details_log["final_status"] = "manifest_verified_ok"
            trusted_manifest_dict_inner["job_id_for_logging"] = job_id 
            return trusted_manifest_dict_inner

        except FileNotFoundError as e_fnf:
            logger.error(f"Job {job_id}: File not found (manifest/sig): {e_fnf}")
            verification_details_log["steps"].append({"name": "FetchManifestAndSig", "status": "failed", "reason": str(e_fnf)})
            verification_details_log["final_status"] = "error_file_not_found"
            return None

        except (ValueError, InvalidSignature) as e_verify:
            logger.error(f"Job {job_id}: Manifest verification step failed: {e_verify}")
            
            if "VerifyManifestContentHash" in str(e_verify): verification_details_log["final_status"] = "failed_manifest_hash_mismatch"
            elif "VerifyManifestSigner" in str(e_verify): verification_details_log["final_status"] = "failed_manifest_signer_mismatch"
            elif "VerifyManifestSignature" in str(e_verify): verification_details_log["final_status"] = "failed_manifest_signature"
            else: verification_details_log["final_status"] = "failed_manifest_verification_other"
            
            if not any(s["name"] == "VerifyManifestContentHash" and s["status"] == "failed" for s in verification_details_log["steps"]) and \
               not any(s["name"] == "VerifyManifestSigner" and s["status"] == "failed" for s in verification_details_log["steps"]) and \
               not any(s["name"] == "VerifyManifestSignature" and s["status"] == "failed" for s in verification_details_log["steps"]):
                 verification_details_log["steps"].append({"name": "ManifestVerificationFailure", "status": "failed", "reason": str(e_verify)})
            
            return None
            
        except Exception as e_inner_sync:
            logger.error(f"Job {job_id}: Unexpected error in _fetch_and_verify_manifest_sync_inner: {e_inner_sync}", exc_info=True)
            verification_details_log["steps"].append({"name": "InternalSyncVerification", "status": "error", "reason": str(e_inner_sync)})
            verification_details_log["final_status"] = "error_unexpected_sync"
            return None

    trusted_manifest_final = None
    try:
        loop = asyncio.get_running_loop()
        trusted_manifest_final = await loop.run_in_executor(None, _fetch_and_verify_manifest_sync_inner)
    except Exception as e_executor_call:
        logger.error(f"Job {job_id}: Exception calling executor for manifest fetch/verify: {e_executor_call}", exc_info=True)
        verification_details_log["steps"].append({"name": "ExecutorCallForManifest", "status": "failed", "reason": str(e_executor_call)})
        verification_details_log["final_status"] = "error_executor_manifest_call"
    
    log_overall_success = trusted_manifest_final is not None
    try:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        log_filename = os.path.join(VERIFICATION_LOG_DIR, f"job_{job_id}_manifest_verify_log_{ts}.json")
        verification_details_log["overall_manifest_verification_passed"] = log_overall_success
        verification_details_log["total_verification_time_seconds"] = time.time() - start_time_total_verify
        with open(log_filename, 'w') as f: json.dump(verification_details_log, f, indent=4)
        logger.info(f"Saved manifest verification log for job {job_id} to: {log_filename}")
    except Exception as e_save: logger.error(f"Failed to save manifest log for job {job_id}: {e_save}")

    if log_overall_success:
        logger.info(f"Job {job_id}: Manifest successfully verified. Returning VerifyingChunkMapper.")
        base_fs_map = fs.get_mapper(zarr_store_url_cleaned)
        verifying_mapper = VerifyingChunkMapper(base_fs_map.root, fs, trusted_manifest_final, check_existence_upon_init=False)
        return verifying_mapper
    else:
        logger.error(f"Job {job_id}: Manifest verification failed. Cannot provide trusted store. Final status: {verification_details_log.get('final_status')}")
        return None

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
    Computes a canonical hash for GFS input data (T0 and T-6).
    Ensures that the potentially CPU-bound part (_get_canonical_bytes) is run in a thread.
    """
    logger.info(f"Computing input data hash for T0={t0_run_time}, T-6={t_minus_6_run_time}")
    ds_t0 = None
    ds_t_minus_6 = None
    try:
        ds_t0 = await fetch_gfs_analysis_data([t0_run_time], cache_dir=cache_dir)
        ds_t_minus_6 = await fetch_gfs_analysis_data([t_minus_6_run_time], cache_dir=cache_dir)

        if ds_t0 is None or ds_t_minus_6 is None:
            logger.error("Failed to fetch GFS data for hash computation.")
            return None

        canonical_bytes = await asyncio.to_thread(
            _get_canonical_bytes, 
            ds_t0.copy(deep=True),
            ds_t_minus_6.copy(deep=True)
        )

        if canonical_bytes is None:
            logger.error("Failed to get canonical byte representation of GFS data.")
            return None

        input_hash = hashlib.sha256(canonical_bytes).hexdigest()
        logger.info(f"Computed input data hash: {input_hash}")
        return input_hash
    except Exception as e:
        logger.error(f"Error in compute_input_data_hash: {e}", exc_info=True)
        return None
    finally:
        if ds_t0 is not None and hasattr(ds_t0, 'close'):
            try:
                ds_t0.close()
            except Exception:
                pass
        if ds_t_minus_6 is not None and hasattr(ds_t_minus_6, 'close'):
            try:
                ds_t_minus_6.close()
            except Exception:
                pass


def _get_data_shape_from_xarray_dataset(
    ds: xr.Dataset, 
    variables_to_include: List[str], 
    var_mapping: Dict[str, Tuple[str, str]]
) -> Dict[str, Dict[str, Tuple[int, ...]]]:
    data_shape = {"surf_vars": {}, "atmos_vars": {}}
    
    rev_mapping = {}
    for ds_name, (cat, aur_name) in var_mapping.items():
        rev_mapping[aur_name] = (cat, ds_name)
        
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

class VerifyingChunkMapper(fsspec.mapping.FSMap):
    def __init__(self, root: str, fs: fsspec.AbstractFileSystem, 
                 trusted_manifest: Dict[str, Any], 
                 job_id_for_logging: str = "unknown_job"):
        super().__init__(root, fs) 
        self.trusted_manifest_files = trusted_manifest.get("files", {})
        self.chunk_hash_algorithm = trusted_manifest.get("chunk_hash_algorithm", "xxh64")
        self.job_id_for_logging = job_id_for_logging
        if not self.trusted_manifest_files:
            logger.warning(f"Job {self.job_id_for_logging}: VerifyingChunkMapper initialized with empty manifest file list.")

    def _hash_chunk(self, key: str, content_bytes: bytes) -> str:
        path_plus_content = key.encode('utf-8') + b'\0' + content_bytes
        if self.chunk_hash_algorithm == "xxh64":
            return xxhash.xxh64(path_plus_content).hexdigest()
        elif self.chunk_hash_algorithm == "sha256":
            return hashlib.sha256(path_plus_content).hexdigest()
        else:
            msg = f"Unsupported chunk hash algorithm '{self.chunk_hash_algorithm}' in manifest for job {self.job_id_for_logging}."
            logger.error(msg)
            raise ValueError(msg)

    def __getitem__(self, key: str) -> bytes:
        chunk_content_bytes = super().__getitem__(key) 
        expected_hash = self.trusted_manifest_files.get(key)
        if expected_hash is None:
            if key.startswith('.') or key.endswith(('.zarray', '.zattrs', '.zgroup', '.zmetadata')):
                logger.debug(f"Job {self.job_id_for_logging}: Passthrough for Zarr metadata file: {key} (not in manifest 'files')")
                return chunk_content_bytes
            else:
                msg = f"Data chunk path '{key}' not found in trusted manifest for job {self.job_id_for_logging}. Cannot verify integrity."
                logger.error(msg)
                raise KeyError(msg) 
        computed_hash = self._hash_chunk(key, chunk_content_bytes)
        if computed_hash != expected_hash:
            msg = f"CHUNK INTEGRITY FAIL Job {self.job_id_for_logging}: For chunk '{key}'. Expected: {expected_hash}, Computed: {computed_hash}"
            logger.error(msg)
            raise IOError(msg) 
        logger.debug(f"Job {self.job_id_for_logging}: Chunk integrity PASSED for '{key}'")
        return chunk_content_bytes

def _verify_manifest_integrity_sync(
    fs: fsspec.AbstractFileSystem, 
    zarr_root_on_server: str, 
    claimed_manifest_content_hash: str, 
    expected_signer_hotkey: str,
    job_id: Optional[str]
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    verification_details_log = { 
        "job_id": job_id, "zarr_store_url_checked": zarr_root_on_server,
        "claimed_manifest_content_hash": claimed_manifest_content_hash,
        "expected_signer_hotkey": expected_signer_hotkey,
        "verification_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "steps": [], "final_status": "pending_sync_verification"
    }
    trusted_manifest_dict = None

    try:
        manifest_path = zarr_root_on_server.rstrip('/') + "/manifest.json"
        sig_path = zarr_root_on_server.rstrip('/') + "/manifest.sig"
        
        logger.info(f"Job {job_id}: Fetching manifest from {manifest_path}")
        manifest_json_bytes = fs.cat(manifest_path)
        logger.info(f"Job {job_id}: Fetched manifest.json ({len(manifest_json_bytes)} bytes)")
        
        logger.info(f"Job {job_id}: Fetching signature from {sig_path}")
        signature_bytes = fs.cat(sig_path)
        logger.info(f"Job {job_id}: Fetched manifest.sig ({len(signature_bytes)} bytes)")
        
        verification_details_log["steps"].append({
            "name": "FetchManifestAndSig", 
            "status": "success", 
            "manifest_size": len(manifest_json_bytes), 
            "sig_size": len(signature_bytes)
        })

        computed_m_hash = hashlib.sha256(manifest_json_bytes).hexdigest()
        if computed_m_hash != claimed_manifest_content_hash:
            raise ValueError(f"Manifest content hash mismatch! Claimed: {claimed_manifest_content_hash[:10]}..., Computed: {computed_m_hash[:10]}...")
        
        logger.info(f"Job {job_id}: Manifest content hash verified successfully.")
        verification_details_log["steps"].append({"name": "VerifyManifestContentHash", "status": "success"})
        
        trusted_manifest_dict = json.loads(manifest_json_bytes.decode('utf-8'))
        manifest_signer = trusted_manifest_dict.get("signer_hotkey_ss58")
        
        if not manifest_signer or manifest_signer != expected_signer_hotkey:
            raise ValueError(f"Manifest signer hotkey mismatch. Expected: {expected_signer_hotkey}, Got: {manifest_signer}")
        
        logger.info(f"Job {job_id}: Manifest signer hotkey in manifest matches expected: {manifest_signer}")
        verification_details_log["steps"].append({"name": "VerifyManifestSigner", "status": "success"})

        verifier_kp = Keypair(ss58_address=expected_signer_hotkey)
        if not verifier_kp.verify(manifest_json_bytes, signature_bytes):
            raise InvalidSignature("Manifest signature verification failed (Keypair.verify)")
        
        logger.info(f"Job {job_id}: Manifest signature verified successfully for hotkey {expected_signer_hotkey}.")
        verification_details_log["steps"].append({"name": "VerifyManifestSignature", "status": "success"})
        
        verification_details_log["final_status"] = "manifest_verified_ok"
        trusted_manifest_dict["job_id_for_logging"] = job_id 
        return trusted_manifest_dict, verification_details_log

    except FileNotFoundError as e_fnf:
        logger.error(f"Job {job_id}: File not found (manifest/sig) in _verify_manifest_integrity_sync: {e_fnf}")
        verification_details_log["steps"].append({"name": "FetchManifestAndSig", "status": "failed", "reason": str(e_fnf)})
        verification_details_log["final_status"] = "error_file_not_found"

    except (ValueError, InvalidSignature) as e_verify:
        logger.error(f"Job {job_id}: Manifest verification step failed: {e_verify}")
        step_name = "UnknownVerificationStep"
        if "Manifest content hash mismatch" in str(e_verify): step_name = "VerifyManifestContentHash"
        elif "Manifest signer hotkey mismatch" in str(e_verify): step_name = "VerifyManifestSigner"
        elif "Manifest signature verification failed" in str(e_verify): step_name = "VerifyManifestSignature"
        verification_details_log["steps"].append({"name": step_name, "status": "failed", "reason": str(e_verify)})
        verification_details_log["final_status"] = f"failed_{step_name.lower().replace('verify', '')}"

    except Exception as e_inner_sync:
        logger.error(f"Job {job_id}: Unexpected error in _verify_manifest_integrity_sync: {e_inner_sync}", exc_info=True)
        verification_details_log["steps"].append({"name": "InternalSyncVerification", "status": "error", "reason": str(e_inner_sync)})
        verification_details_log["final_status"] = "error_unexpected_sync"
        
    return None, verification_details_log

async def get_trusted_manifest(
    zarr_store_url: str,
    claimed_manifest_content_hash: str,
    miner_hotkey_ss58: str,
    headers: Optional[Dict[str, str]] = None,
    job_id: Optional[str] = "unknown_job"
) -> Optional[Dict[str, Any]]:
    start_time = time.time()
    zarr_store_url_cleaned = zarr_store_url.rstrip('/') + ('/' if not zarr_store_url.endswith('/') and zarr_store_url.endswith('.zarr') else '')
    if not zarr_store_url_cleaned.endswith('/'): zarr_store_url_cleaned += '/'
    logger.info(f"Attempting to get trusted manifest for job {job_id} (Miner: {miner_hotkey_ss58}), store: {zarr_store_url_cleaned}")
    fs = None
    trusted_manifest_dict = None
    verification_log = {}
    try:
        protocol = zarr_store_url_cleaned.split("://")[0]
        http_fs_kwargs = {}
        if headers: http_fs_kwargs["headers"] = headers
        http_fs_kwargs["ssl"] = False 
        fs = fsspec.filesystem(protocol, **http_fs_kwargs)
    except Exception as e_fs_init:
        logger.error(f"Job {job_id}: Failed to initialize fsspec for manifest: {e_fs_init}")
        verification_log = {"job_id": job_id, "final_status": "error_fs_init", "reason": str(e_fs_init)}
    if fs:
        try:
            loop = asyncio.get_running_loop()
            trusted_manifest_dict, verification_log_from_executor = await loop.run_in_executor(
                None, _verify_manifest_integrity_sync, fs, zarr_store_url_cleaned, 
                claimed_manifest_content_hash, miner_hotkey_ss58, job_id
            )
            verification_log.update(verification_log_from_executor) # Merge logs
        except Exception as e_exec:
            logger.error(f"Job {job_id}: Executor call for manifest verification failed: {e_exec}", exc_info=True)
            verification_log = verification_log or {"job_id": job_id, "steps": []}
            verification_log["steps"].append({"name": "ExecutorCall", "status": "error", "reason": str(e_exec)})
            verification_log["final_status"] = "error_executor_call"
            trusted_manifest_dict = None
    
    log_overall_success = trusted_manifest_dict is not None
    if not isinstance(verification_log, dict): verification_log = {}
    for key_to_ensure in ["job_id", "zarr_store_url_checked", "claimed_manifest_content_hash", "expected_signer_hotkey", "verification_timestamp_utc", "steps", "final_status"]:
        if key_to_ensure not in verification_log:
            if key_to_ensure == "job_id": verification_log[key_to_ensure] = job_id
            elif key_to_ensure == "zarr_store_url_checked": verification_log[key_to_ensure] = zarr_store_url_cleaned
            elif key_to_ensure == "claimed_manifest_content_hash": verification_log[key_to_ensure] = claimed_manifest_content_hash
            elif key_to_ensure == "expected_signer_hotkey": verification_log[key_to_ensure] = miner_hotkey_ss58
            elif key_to_ensure == "verification_timestamp_utc": verification_log[key_to_ensure] = datetime.now(timezone.utc).isoformat()
            elif key_to_ensure == "steps": verification_log[key_to_ensure] = []
            elif key_to_ensure == "final_status" and log_overall_success : verification_log[key_to_ensure] = "manifest_verified_ok"
            elif key_to_ensure == "final_status" and not log_overall_success and not verification_log.get("final_status"): verification_log[key_to_ensure] = "failed_unknown"

    verification_log["overall_manifest_verification_passed"] = log_overall_success
    verification_log["total_verification_time_seconds"] = time.time() - start_time
    
    try:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        log_filename = os.path.join(VERIFICATION_LOG_DIR, f"job_{job_id}_manifest_verify_log_{ts}.json")
        with open(log_filename, 'w') as f: json.dump(verification_log, f, indent=4)
        logger.info(f"Saved manifest verification log to {log_filename}")
    except Exception as e_save: logger.error(f"Failed to save manifest log: {e_save}")

    if log_overall_success:
        logger.info(f"Job {job_id}: Manifest integrity VERIFIED.")
        return trusted_manifest_dict
    else:
        logger.error(f"Job {job_id}: Manifest integrity FAILED. Final status: {verification_log.get('final_status')}")
        return None
        