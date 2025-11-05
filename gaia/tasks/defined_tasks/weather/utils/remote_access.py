import asyncio
import os
import time
import traceback
from typing import Dict, List, Optional, Union, Tuple, Any
import fsspec
import xarray as xr
import pandas as pd
from gaia.utils.custom_logger import get_logger

logger = get_logger(__name__)

# Process-local: capture last verified open error details by job_id
_last_verified_open_error = {}

def set_last_verified_open_error(job_id: str, message: str):
    try:
        _last_verified_open_error[job_id] = message
    except Exception:
        pass

def get_last_verified_open_error(job_id: str) -> Optional[str]:
    try:
        return _last_verified_open_error.get(job_id)
    except Exception:
        return None

# Ensure blosc codec is available for zarr operations
try:
    import blosc
    import numcodecs

    # Force registration of blosc codec - correct way is to just import it
    import numcodecs.blosc

    # Verify the codec is available
    codec = numcodecs.registry.get_codec({"id": "blosc"})
    print(
        f"Blosc codec successfully imported and available. Version: {blosc.__version__}"
    )
except ImportError as e:
    print(
        f"Failed to import blosc codec: {e}. Zarr datasets using blosc compression may fail to open."
    )
except Exception as e:
    print(
        f"Failed to verify blosc codec availability: {e}. Zarr datasets using blosc compression may fail to open."
    )

try:
    from .hashing import get_trusted_manifest, VerifyingChunkMapper
except ImportError:
    try:
        from hashing import get_trusted_manifest, VerifyingChunkMapper
    except ImportError as e:
        print(
            f"CRITICAL: Could not import hashing utilities from hashing.py. Error: {e}"
        )
        get_trusted_manifest = None
        VerifyingChunkMapper = None
        print(
            "Warning: `get_trusted_manifest` and `VerifyingChunkMapper` not imported. Verified access will fail if called."
        )

logger = get_logger(__name__)

 


def _synchronous_open_with_verifying_mapper(
    verifying_mapper, consolidated: bool
) -> Optional[xr.Dataset]:
    """Synchronous helper to open dataset with the VerifyingChunkMapper."""
    # CRITICAL: Ensure blosc codec is available in this executor thread
    try:
        import blosc
        import numcodecs
        import numcodecs.blosc
        import zarr

        # Force re-import and registration of blosc codec in this thread
        try:
            import importlib

            importlib.reload(numcodecs.blosc)

            # Explicitly register all standard codecs
            from numcodecs import Blosc, LZ4, Zstd, GZip, BZ2

            # Register blosc with all common configurations
            for cname in ["lz4", "lz4hc", "snappy", "zlib", "zstd"]:
                try:
                    codec = Blosc(cname=cname, clevel=5, shuffle=Blosc.BITSHUFFLE)
                    numcodecs.register_codec(codec)
                except:
                    pass

            # Ensure basic blosc codec is registered
            blosc_codec = Blosc()
            numcodecs.register_codec(blosc_codec)

            # Test the registry
            test_codec = numcodecs.registry.get_codec({"id": "blosc"})
            logger.debug(
                f"Job {verifying_mapper.job_id_for_logging}: Blosc codec successfully registered and retrieved in executor thread"
            )

        except Exception as codec_err:
            logger.warning(
                f"Job {verifying_mapper.job_id_for_logging}: Codec registration failed: {codec_err}",
                exc_info=True,
            )

    except Exception as e:
        logger.warning(
            f"Job {verifying_mapper.job_id_for_logging}: Failed to ensure blosc codec in executor thread: {e}",
            exc_info=True,
        )

    try:
        logger.info(
            f"Job {verifying_mapper.job_id_for_logging}: xr.open_zarr called with VerifyingChunkMapper."
        )
        # Open with verifying mapper; avoid unsupported args for our xarray version
        ds = xr.open_zarr(verifying_mapper, consolidated=consolidated, chunks="auto")

        if ds is not None and "time" in ds.coords:
            time_coord = ds.coords["time"]
            # Check if dtype is datetime64[ns] and it's timezone-naive
            if (
                pd.api.types.is_datetime64_ns_dtype(time_coord.dtype)
                and getattr(time_coord.dt, "tz", None) is None
            ):
                logger.info(
                    f"Job {verifying_mapper.job_id_for_logging}: Time coordinate is datetime64[ns] and timezone-naive. Localizing to UTC."
                )
                try:
                    # For xarray, we need to use assign_coords with pd.to_datetime
                    time_values = pd.to_datetime(time_coord.values).tz_localize("UTC")
                    ds = ds.assign_coords(time=time_values)
                    logger.info(
                        f"Job {verifying_mapper.job_id_for_logging}: Successfully localized time coordinate to UTC. New dtype: {ds.time.dtype}"
                    )
                except Exception as e_tz_localize:
                    logger.warning(
                        f"Job {verifying_mapper.job_id_for_logging}: Failed to localize time coordinate to UTC: {e_tz_localize}. Proceeding with naive time."
                    )
            elif (
                pd.api.types.is_datetime64_any_dtype(time_coord.dtype)
                and getattr(time_coord.dt, "tz", None) is not None
            ):
                if str(getattr(time_coord.dt, "tz")) != "UTC":
                    logger.info(
                        f"Job {verifying_mapper.job_id_for_logging}: Time coordinate is already timezone-aware ({time_coord.dtype}) but not UTC. Converting to UTC."
                    )
                    try:
                        # For xarray, we need to use assign_coords with pd.to_datetime
                        time_values = pd.to_datetime(time_coord.values).tz_convert(
                            "UTC"
                        )
                        ds = ds.assign_coords(time=time_values)
                        logger.info(
                            f"Job {verifying_mapper.job_id_for_logging}: Successfully converted time coordinate to UTC. New dtype: {ds.time.dtype}"
                        )
                    except Exception as e_tz_convert:
                        logger.warning(
                            f"Job {verifying_mapper.job_id_for_logging}: Failed to convert time coordinate to UTC: {e_tz_convert}. Proceeding with original timezone."
                        )
                else:
                    logger.info(
                        f"Job {verifying_mapper.job_id_for_logging}: Time coordinate is already timezone-aware and UTC: {time_coord.dtype}. No localization needed."
                    )
            else:
                logger.info(
                    f"Job {verifying_mapper.job_id_for_logging}: Time coordinate is not a timezone-naive datetime64[ns] (dtype: {time_coord.dtype}). Skipping UTC localization/conversion."
                )

        logger.info(
            f"Job {verifying_mapper.job_id_for_logging}: Successfully opened Zarr dataset with VerifyingChunkMapper."
        )
        return ds
    except Exception as e:
        logger.error(
            f"Job {verifying_mapper.job_id_for_logging}: xr.open_zarr with VerifyingChunkMapper FAILED: {e}",
            exc_info=True,
        )
        return None


async def open_verified_remote_zarr_variable(
    zarr_store_url: str,
    claimed_manifest_content_hash: str,
    miner_hotkey_ss58: str,
    variable_names: List[str],
    storage_options: Optional[Dict] = None,
    job_id: Optional[str] = "unknown_job",
    frozen_manifest: Optional[Dict[str, Any]] = None,
    return_manifest: bool = False,
) -> Optional[Union[xr.Dataset, Tuple[Optional[xr.Dataset], Optional[Dict[str, Any]]]]]:
    """
    Opens specific variables from a remote Zarr dataset to minimize HTTP requests.
    Only loads the requested variables instead of the entire dataset.
    If frozen_manifest is provided, it will be used instead of fetching the manifest from the miner.
    """
    import multiprocessing as mp
    import threading
    process_name = mp.current_process().name if mp.current_process() else "unknown"
    thread_name = threading.current_thread().name
    
    logger.info(
        f"🔍 ZARR OPEN [{process_name}:{thread_name}] Job {job_id}: "
        f"Opening SPECIFIC variables {variable_names} from Zarr: {zarr_store_url}"
    )

    if get_trusted_manifest is None or VerifyingChunkMapper is None:
        logger.critical(
            f"Job {job_id}: Hashing utilities not available. Cannot perform verified open."
        )
        return (None, None) if return_manifest else None

    trusted_manifest = frozen_manifest
    if trusted_manifest is None:
        logger.error(
            f"Job {job_id}: No frozen manifest provided for verified variable open. Refusing to fetch from miner."
        )
        return (None, None) if return_manifest else None

    try:
        zarr_store_url_cleaned = zarr_store_url + (
            "/" if not zarr_store_url.endswith("/") and zarr_store_url.endswith(".zarr") else ""
        )
        if not zarr_store_url_cleaned.endswith("/"):
            zarr_store_url_cleaned += "/"

        protocol = zarr_store_url_cleaned.split("://")[0]
        http_fs_kwargs = {}
        if storage_options and "headers" in storage_options:
            http_fs_kwargs["headers"] = storage_options["headers"]
        http_fs_kwargs["ssl"] = storage_options.get("ssl", False) if storage_options else False
        
        # CRITICAL: Disable all caching to ensure every read goes through VerifyingChunkMapper
        http_fs_kwargs["cache_type"] = "none"  # Disable fsspec caching
        http_fs_kwargs["cache"] = None  # Explicitly disable cache
        
        fs = fsspec.filesystem(protocol, **http_fs_kwargs)
        verifying_mapper = VerifyingChunkMapper(
            root=zarr_store_url_cleaned,
            fs=fs,
            trusted_manifest=trusted_manifest,
            job_id_for_logging=job_id,
            strict_metadata=True,
        )

        is_consolidated = ".zmetadata" in trusted_manifest.get("files", {})
        
        # Open metadata first to determine available variables; then select with alias support
        loop = asyncio.get_running_loop()
        base_ds = await loop.run_in_executor(
            None,
            lambda: xr.open_zarr(
                verifying_mapper,
                consolidated=is_consolidated,
                decode_times=True,
                mask_and_scale=True,
                chunks="auto",
            )
        )
        if base_ds is None:
            logger.error(f"Job {job_id}: Failed to open Zarr dataset for variable selection")
            return (None, trusted_manifest) if return_manifest else None

        present_vars = set(list(base_ds.data_vars))
        # Minimal alias map for common surface vars
        alias_map = {
            "2t": ["2t", "t2m", "t_2m"],
            "10u": ["10u", "u10"],
            "10v": ["10v", "v10"],
            # Leave others as-is
        }
        selected_vars = []
        rename_map = {}
        for req in variable_names:
            candidates = alias_map.get(req, [req])
            found = None
            for cand in candidates:
                if cand in present_vars:
                    found = cand
                    break
            if found is not None:
                selected_vars.append(found)
                if found != req:
                    rename_map[found] = req
            else:
                logger.warning(f"Job {job_id}: Requested variable '{req}' not present in dataset vars {sorted(list(present_vars))[:12]}...")

        if not selected_vars:
            logger.error(f"Job {job_id}: None of requested variables {variable_names} (with aliases) found in dataset")
            return (None, trusted_manifest) if return_manifest else None

        ds = base_ds[selected_vars]
        if rename_map:
            try:
                ds = ds.rename(rename_map)
            except Exception:
                pass

        # CRITICAL: Ensure dataset uses dask arrays for chunk-by-chunk verification
        # Check if variables are dask arrays
        for var_name in ds.data_vars:
            var_da = ds[var_name]
            if hasattr(var_da, 'chunks'):
                logger.info(f"Job {job_id}: Variable '{var_name}' is a dask array with chunks: {var_da.chunks}")
            else:
                logger.warning(f"Job {job_id}: WARNING - Variable '{var_name}' is NOT a dask array! Verification may not work!")

        # Mark dataset as verified-open with manifest context for downstream assertions/logging
        try:
            ds.attrs["verified_open"] = True
            ds.attrs["verifying_job_id"] = job_id
            ds.attrs["claimed_manifest_content_hash"] = claimed_manifest_content_hash
            ds.attrs["trusted_manifest_files_count"] = len(trusted_manifest.get("files", {}))
        except Exception:
            pass

        logger.success(f"Job {job_id}: ✅ Opened variables {list(ds.data_vars)} from Zarr (aliases resolved)")

        if return_manifest:
            return ds, trusted_manifest
        return ds

    except Exception as e:
        logger.error(f"Job {job_id}: Error opening variable-specific Zarr: {e}")
        return (None, frozen_manifest) if return_manifest else None


async def open_verified_remote_zarr_dataset(
    zarr_store_url: str,
    claimed_manifest_content_hash: str,
    miner_hotkey_ss58: str,
    storage_options: Optional[Dict] = None,
    job_id: Optional[str] = "unknown_job",
    frozen_manifest: Optional[Dict[str, Any]] = None,
    return_manifest: bool = False,
) -> Optional[Union[xr.Dataset, Tuple[Optional[xr.Dataset], Optional[Dict[str, Any]]]]]:
    """
    Opens a remote Zarr dataset with on-read chunk verification after validating manifest.
    1. Verifies manifest (content hash, signature) by calling hashing.get_trusted_manifest (unless frozen_manifest provided).
    2. If manifest is OK, creates a VerifyingChunkMapper.
    3. Opens the Zarr store using xarray with this verifying mapper.
    Returns an xarray.Dataset if successful, None otherwise. If return_manifest is True, returns a tuple (dataset, manifest_dict).
    """
    import multiprocessing as mp
    import threading
    process_name = mp.current_process().name if mp.current_process() else "unknown"
    thread_name = threading.current_thread().name
    
    logger.info(
        f"🔍 ZARR OPEN FULL [{process_name}:{thread_name}] Job {job_id}: "
        f"Attempting VERIFIED open for Zarr: {zarr_store_url}"
    )
    # Log strict verification flags
    try:
        strict_meta_env = os.getenv("WEATHER_STRICT_ZARR_METADATA", "true")
        logger.info(
            f"Job {job_id}: Strict metadata verification flag WEATHER_STRICT_ZARR_METADATA={strict_meta_env}"
        )
    except Exception:
        pass
    # removed code marker

    if get_trusted_manifest is None or VerifyingChunkMapper is None:
        logger.critical(
            f"Job {job_id}: Hashing utilities (get_trusted_manifest or VerifyingChunkMapper) not imported. Cannot perform verified open."
        )
        return (None, None) if return_manifest else None

    trusted_manifest = frozen_manifest
    if trusted_manifest is None:
        logger.error(
            f"Job {job_id}: No frozen manifest provided for verified dataset open. Refusing to fetch from miner."
        )
        return (None, None) if return_manifest else None

    try:
        zarr_store_url_cleaned = zarr_store_url.rstrip("/") + (
            "/"
            if not zarr_store_url.endswith("/") and zarr_store_url.endswith(".zarr")
            else ""
        )
        if not zarr_store_url_cleaned.endswith("/"):
            zarr_store_url_cleaned += "/"

        protocol = zarr_store_url_cleaned.split("://")[0]

        http_fs_kwargs = {}
        if storage_options and "headers" in storage_options:
            http_fs_kwargs["headers"] = storage_options["headers"]
        http_fs_kwargs["ssl"] = (
            storage_options.get("ssl", False) if storage_options else False
        )
        
        # CRITICAL: Disable all caching to ensure every read goes through VerifyingChunkMapper
        http_fs_kwargs["cache_type"] = "none"  # Disable fsspec caching
        http_fs_kwargs["cache"] = None  # Explicitly disable cache

        fs = fsspec.filesystem(protocol, **http_fs_kwargs)

        verifying_mapper = VerifyingChunkMapper(
            root=zarr_store_url_cleaned,
            fs=fs,
            trusted_manifest=trusted_manifest,
            job_id_for_logging=job_id,
            strict_metadata=True,
        )
        logger.info(
            f"Job {job_id}: VerifyingChunkMapper created for {zarr_store_url_cleaned}."
        )

        # Detect consolidated heuristically: prefer presence of .zmetadata on server if available in listing,
        # but don't rely solely on manifest contents since many manifests exclude metadata files.
        is_consolidated = ".zmetadata" in trusted_manifest.get("files", {})
        logger.info(
            f"Job {job_id}: Initial consolidated guess from manifest: {is_consolidated}"
        )

        loop = asyncio.get_running_loop()
        dataset = await loop.run_in_executor(
            None,
            _synchronous_open_with_verifying_mapper,
            verifying_mapper,
            is_consolidated,
        )
        if dataset is None:
            # Retry with opposite consolidated flag to handle incorrect guess without extra network I/O
            logger.info(
                f"Job {job_id}: Retrying xr.open_zarr with consolidated={not is_consolidated}"
            )
            try:
                import traceback as _tb
                logger.error(
                    f"Job {job_id}: First xr.open_zarr returned None (consolidated={is_consolidated}).\nStack:\n" + ''.join(_tb.format_stack(limit=16))
                )
            except Exception:
                pass
            try:
                dataset = await loop.run_in_executor(
                    None,
                    _synchronous_open_with_verifying_mapper,
                    verifying_mapper,
                    (not is_consolidated),
                )
            except Exception as retry_exc:
                logger.error(
                    f"Job {job_id}: Retry open_zarr failed: {retry_exc}",
                    exc_info=True,
                )
        if dataset is None:
            try:
                import traceback as _tb
                logger.error(
                    f"Job {job_id}: Second xr.open_zarr returned None (consolidated={not is_consolidated}).\nStack:\n" + ''.join(_tb.format_stack(limit=16))
                )
            except Exception:
                pass

        if dataset is not None:
            logger.info(
                f"Job {job_id}: Successfully opened VERIFIED remote Zarr dataset: {zarr_store_url}"
            )
            # Mark dataset as verified-open with manifest context for downstream assertions/logging
            try:
                dataset.attrs["verified_open"] = True
                dataset.attrs["verifying_job_id"] = job_id
                dataset.attrs["claimed_manifest_content_hash"] = claimed_manifest_content_hash
                dataset.attrs["trusted_manifest_files_count"] = len(trusted_manifest.get("files", {}))
            except Exception:
                pass
            if return_manifest:
                return dataset, trusted_manifest
            return dataset
        else:
            logger.error(
                f"Job {job_id}: Opening Zarr with VerifyingChunkMapper returned None for {zarr_store_url}"
            )
            set_last_verified_open_error(job_id, "open_with_verifying_mapper returned None")
            raise RuntimeError("open_with_verifying_mapper returned None")

    except Exception as e:
        logger.error(
            f"Job {job_id}: Error in open_verified_remote_zarr_dataset (post-manifest check) for {zarr_store_url}: {e}",
            exc_info=True,
        )
        try:
            set_last_verified_open_error(job_id, f"exception: {e}")
        except Exception:
            pass
        if return_manifest:
            return None, frozen_manifest
        raise
