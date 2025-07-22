import asyncio
import os
import time
import traceback
from typing import Dict, List, Optional
import fsspec
import xarray as xr
import psutil
import pandas as pd
from fiber.logging_utils import get_logger

# Ensure blosc codec is available for zarr operations
try:
    import blosc
    import numcodecs
    # Force registration of blosc codec - correct way is to just import it
    import numcodecs.blosc
    # Verify the codec is available
    codec = numcodecs.registry.get_codec({'id': 'blosc'})
    print(f"Blosc codec successfully imported and available. Version: {blosc.__version__}")
except ImportError as e:
    print(f"Failed to import blosc codec: {e}. Zarr datasets using blosc compression may fail to open.")
except Exception as e:
    print(f"Failed to verify blosc codec availability: {e}. Zarr datasets using blosc compression may fail to open.")

try:
    from .hashing import get_trusted_manifest, VerifyingChunkMapper
except ImportError:
    try:
        from hashing import get_trusted_manifest, VerifyingChunkMapper
    except ImportError as e:
        print(f"CRITICAL: Could not import hashing utilities from hashing.py. Error: {e}")
        get_trusted_manifest = None
        VerifyingChunkMapper = None
        print("Warning: `get_trusted_manifest` and `VerifyingChunkMapper` not imported. Verified access will fail if called.")

logger = get_logger(__name__)

def get_current_memory_usage_mb(): 
    try:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        return mem_info.rss / (1024 * 1024)
    except Exception:
        return -1

def _synchronous_zarr_open_unverified(
    zarr_store_url: str,
    http_fs_kwargs: Dict
) -> Optional[xr.Dataset]:
    """
    Opens a Zarr store over HTTP synchronously WITHOUT manifest/chunk verification.
    Returns xr.Dataset or None on failure.
    """
    # CRITICAL: Ensure blosc codec is available in this executor thread
    try:
        import blosc
        import numcodecs
        import numcodecs.blosc
        import zarr
        
        # More robust codec registration
        try:
            from numcodecs import Blosc, LZ4, Zstd
            import importlib
            importlib.reload(numcodecs.blosc)
            
            # Test codec functionality
            blosc_codec = Blosc()
            import numpy as np
            test_data = np.array([1, 2, 3], dtype='f4')
            compressed = blosc_codec.encode(test_data)
            # Correct decode method - only needs the compressed buffer
            decompressed = blosc_codec.decode(compressed)
            # Reshape the decoded data back to original format
            decompressed = np.frombuffer(decompressed, dtype=test_data.dtype).reshape(test_data.shape)
            
            logger.debug(f"SYNC_ZARR_OPEN_UNVERIFIED: Blosc codec test successful in executor thread")
        except Exception as codec_err:
            logger.warning(f"SYNC_ZARR_OPEN_UNVERIFIED: Codec test failed: {codec_err}")
        
        # Additional fallback: set zarr codec for this thread explicitly
        try:
            blosc_codec = numcodecs.Blosc()
            codec_id = blosc_codec.codec_id
            numcodecs.registry.codec_registry[codec_id] = blosc_codec
            
            if hasattr(numcodecs, 'register_codec'):
                numcodecs.register_codec(blosc_codec)
                
            if hasattr(zarr, 'codec_registry') and hasattr(zarr.codec_registry, 'register_codec'):
                zarr.codec_registry.register_codec(blosc_codec)
        except Exception as e:
            logger.debug(f"Failed to register blosc codec: {e}")
    except Exception as e:
        logger.warning(f"SYNC_ZARR_OPEN_UNVERIFIED: Failed to ensure blosc codec in executor thread: {e}")
    
    if zarr_store_url.endswith(".zarr") and not zarr_store_url.endswith("/"):
        zarr_store_url += '/'
    elif not zarr_store_url.endswith('/'):
        zarr_store_url += '/'
    
    logger.info(f"SYNC_ZARR_OPEN_UNVERIFIED: Starting. Target URL: {zarr_store_url}")
    overall_start_time = time.time()
    initial_mem_mb = get_current_memory_usage_mb()
    if initial_mem_mb != -1: logger.info(f"SYNC_ZARR_OPEN_UNVERIFIED: Initial Memory: {initial_mem_mb:.2f} MB.")

    protocol = zarr_store_url.split("://")[0]
    fs = None
    mapper = None

    try:
        fs = fsspec.filesystem(protocol, **http_fs_kwargs) 
        
        zmetadata_path = zarr_store_url + '.zmetadata'
        consolidated_flag = fs.exists(zmetadata_path)

        if consolidated_flag:
            logger.info(f"SYNC_ZARR_OPEN_UNVERIFIED: Found .zmetadata at {zmetadata_path}.")
        else:
            logger.warning(f"SYNC_ZARR_OPEN_UNVERIFIED: .zmetadata not found. Attempting non-consolidated.")
            zgroup_path = zarr_store_url + '.zgroup'
            if not fs.exists(zgroup_path):
                logger.error(f"SYNC_ZARR_OPEN_UNVERIFIED: Neither .zmetadata nor .zgroup found. Not a Zarr store or inaccessible: {zarr_store_url}")
                return None
            logger.info(f"SYNC_ZARR_OPEN_UNVERIFIED: Found .zgroup. Will attempt to open as non-consolidated store.")
        
        mapper = fs.get_mapper(zarr_store_url)
    except Exception as e_fs_init:
        logger.error(f"SYNC_ZARR_OPEN_UNVERIFIED: Failed to init filesystem/mapper for {zarr_store_url}: {e_fs_init!r}", exc_info=True)
        return None

    ds = None
    try:
        ds = xr.open_zarr(
            mapper,
            consolidated=consolidated_flag,
            decode_times=True,
            mask_and_scale=True,
            chunks="auto", 
        )
        logger.info(f"SYNC_ZARR_OPEN_UNVERIFIED: Successfully opened Zarr. Keys: {list(ds.keys()) if ds else 'N/A'}")
        return ds
    except Exception as e_xr_open:
        logger.error(f"SYNC_ZARR_OPEN_UNVERIFIED: Failed to open Zarr dataset {zarr_store_url}: {e_xr_open!r}. ", exc_info=True)
        return None
    finally:
        final_mem_mb = get_current_memory_usage_mb()
        if final_mem_mb != -1: logger.info(f"SYNC_ZARR_OPEN_UNVERIFIED: Exiting. Total time: {time.time() - overall_start_time:.2f}s. Final Memory: {final_mem_mb:.2f} MB.")
        else: logger.info(f"SYNC_ZARR_OPEN_UNVERIFIED: Exiting. Total time: {time.time() - overall_start_time:.2f}s.")

async def open_remote_zarr_dataset_unverified(
    zarr_store_url: str,
    storage_options: Optional[Dict] = None 
) -> Optional[xr.Dataset]:
    """
    Opens a remote Zarr dataset asynchronously WITHOUT manifest/chunk verification.
    """
    logger.info(f"Attempting UNVERIFIED open of remote Zarr: {zarr_store_url}")
    
    fsspec_fs_kwargs = {}
    if storage_options:
        if "headers" in storage_options: fsspec_fs_kwargs["headers"] = storage_options["headers"]
        fsspec_fs_kwargs["ssl"] = storage_options.get("ssl", False) 
    else:
        fsspec_fs_kwargs["ssl"] = False

    loop = asyncio.get_running_loop()
    try:
        ds = await loop.run_in_executor(
            None,  
            _synchronous_zarr_open_unverified,
            zarr_store_url,
            fsspec_fs_kwargs
        )
        if ds is not None:
            logger.info(f"Successfully opened remote Zarr (unverified): {zarr_store_url}")
        else:
            logger.error(f"_synchronous_zarr_open_unverified returned None for: {zarr_store_url}")
        return ds
    except Exception as e_open:
        logger.error(f"Error in executor for _synchronous_zarr_open_unverified for {zarr_store_url}: {e_open!r}", exc_info=True)
        return None

def _synchronous_open_with_verifying_mapper(
        verifying_mapper: VerifyingChunkMapper, 
        consolidated: bool
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
            for cname in ['lz4', 'lz4hc', 'snappy', 'zlib', 'zstd']:
                try:
                    codec = Blosc(cname=cname, clevel=5, shuffle=Blosc.BITSHUFFLE)
                    numcodecs.register_codec(codec)
                except:
                    pass
            
            # Ensure basic blosc codec is registered
            blosc_codec = Blosc()
            numcodecs.register_codec(blosc_codec)
            
            # Test the registry
            test_codec = numcodecs.registry.get_codec({'id': 'blosc'})
            logger.debug(f"Job {verifying_mapper.job_id_for_logging}: Blosc codec successfully registered and retrieved in executor thread")
            
        except Exception as codec_err:
            logger.warning(f"Job {verifying_mapper.job_id_for_logging}: Codec registration failed: {codec_err}")
        
    except Exception as e:
        logger.warning(f"Job {verifying_mapper.job_id_for_logging}: Failed to ensure blosc codec in executor thread: {e}")
    
    try:
        logger.info(f"Job {verifying_mapper.job_id_for_logging}: xr.open_zarr called with VerifyingChunkMapper.")
        ds = xr.open_zarr(verifying_mapper, consolidated=consolidated, chunks="auto")
        
        if ds is not None and 'time' in ds.coords:
            time_coord = ds.coords['time']
            # Check if dtype is datetime64[ns] and it's timezone-naive
            if pd.api.types.is_datetime64_ns_dtype(time_coord.dtype) and getattr(time_coord.dt, 'tz', None) is None:
                logger.info(f"Job {verifying_mapper.job_id_for_logging}: Time coordinate is datetime64[ns] and timezone-naive. Localizing to UTC.")
                try:
                    # For xarray, we need to use assign_coords with pd.to_datetime
                    time_values = pd.to_datetime(time_coord.values).tz_localize('UTC')
                    ds = ds.assign_coords(time=time_values)
                    logger.info(f"Job {verifying_mapper.job_id_for_logging}: Successfully localized time coordinate to UTC. New dtype: {ds.time.dtype}")
                except Exception as e_tz_localize:
                    logger.warning(f"Job {verifying_mapper.job_id_for_logging}: Failed to localize time coordinate to UTC: {e_tz_localize}. Proceeding with naive time.")
            elif pd.api.types.is_datetime64_any_dtype(time_coord.dtype) and getattr(time_coord.dt, 'tz', None) is not None:
                if str(getattr(time_coord.dt, 'tz')) != 'UTC':
                    logger.info(f"Job {verifying_mapper.job_id_for_logging}: Time coordinate is already timezone-aware ({time_coord.dtype}) but not UTC. Converting to UTC.")
                    try:
                        # For xarray, we need to use assign_coords with pd.to_datetime
                        time_values = pd.to_datetime(time_coord.values).tz_convert('UTC')
                        ds = ds.assign_coords(time=time_values)
                        logger.info(f"Job {verifying_mapper.job_id_for_logging}: Successfully converted time coordinate to UTC. New dtype: {ds.time.dtype}")
                    except Exception as e_tz_convert:
                        logger.warning(f"Job {verifying_mapper.job_id_for_logging}: Failed to convert time coordinate to UTC: {e_tz_convert}. Proceeding with original timezone.")
                else:
                    logger.info(f"Job {verifying_mapper.job_id_for_logging}: Time coordinate is already timezone-aware and UTC: {time_coord.dtype}. No localization needed.")
            else:
                logger.info(f"Job {verifying_mapper.job_id_for_logging}: Time coordinate is not a timezone-naive datetime64[ns] (dtype: {time_coord.dtype}). Skipping UTC localization/conversion.")

        logger.info(f"Job {verifying_mapper.job_id_for_logging}: Successfully opened Zarr dataset with VerifyingChunkMapper.")
        return ds
    except Exception as e:
        logger.error(f"Job {verifying_mapper.job_id_for_logging}: xr.open_zarr with VerifyingChunkMapper FAILED: {e}", exc_info=True)
        return None

async def open_verified_remote_zarr_dataset(
    zarr_store_url: str,
    claimed_manifest_content_hash: str,
    miner_hotkey_ss58: str,
    storage_options: Optional[Dict] = None, 
    job_id: Optional[str] = "unknown_job"
) -> Optional[xr.Dataset]:
    """
    Opens a remote Zarr dataset with on-read chunk verification after validating manifest.
    1. Verifies manifest (content hash, signature) by calling hashing.get_trusted_manifest.
    2. If manifest is OK, creates a VerifyingChunkMapper.
    3. Opens the Zarr store using xarray with this verifying mapper.
    Returns an xarray.Dataset if successful, None otherwise.
    """
    logger.info(f"Job {job_id}: Attempting VERIFIED open for Zarr: {zarr_store_url}")

    if get_trusted_manifest is None or VerifyingChunkMapper is None:
        logger.critical(f"Job {job_id}: Hashing utilities (get_trusted_manifest or VerifyingChunkMapper) not imported. Cannot perform verified open.")
        return None

    headers_for_manifest = storage_options.get("headers") if storage_options else None

    trusted_manifest = await get_trusted_manifest(
        zarr_store_url=zarr_store_url,
        claimed_manifest_content_hash=claimed_manifest_content_hash,
        miner_hotkey_ss58=miner_hotkey_ss58,
        headers=headers_for_manifest, 
        job_id=job_id
    )

    if trusted_manifest is None:
        logger.error(f"Job {job_id}: Manifest verification failed for {zarr_store_url}. Cannot open verified dataset.")
        return None

    try:
        zarr_store_url_cleaned = zarr_store_url.rstrip('/') + ('/' if not zarr_store_url.endswith('/') and zarr_store_url.endswith('.zarr') else '')
        if not zarr_store_url_cleaned.endswith('/'): zarr_store_url_cleaned += '/'
        
        protocol = zarr_store_url_cleaned.split("://")[0]
        
        http_fs_kwargs = {}
        if storage_options and "headers" in storage_options: 
            http_fs_kwargs["headers"] = storage_options["headers"]
        http_fs_kwargs["ssl"] = storage_options.get("ssl", False) if storage_options else False

        fs = fsspec.filesystem(protocol, **http_fs_kwargs)

        
        verifying_mapper = VerifyingChunkMapper(
            root=zarr_store_url_cleaned,
            fs=fs, 
            trusted_manifest=trusted_manifest, 
            job_id_for_logging=job_id
        )
        logger.info(f"Job {job_id}: VerifyingChunkMapper created for {zarr_store_url_cleaned}.")

        is_consolidated = ".zmetadata" in trusted_manifest.get("files", {})
        logger.info(f"Job {job_id}: Store determined to be consolidated from manifest: {is_consolidated}")

        loop = asyncio.get_running_loop()
        dataset = await loop.run_in_executor(
            None,
            _synchronous_open_with_verifying_mapper,
            verifying_mapper,
            is_consolidated 
        )

        if dataset is not None:
            logger.info(f"Job {job_id}: Successfully opened VERIFIED remote Zarr dataset: {zarr_store_url}")
            return dataset
        else:
            logger.error(f"Job {job_id}: Opening Zarr with VerifyingChunkMapper returned None for {zarr_store_url}")
            return None

    except Exception as e:
        logger.error(f"Job {job_id}: Error in open_verified_remote_zarr_dataset (post-manifest check) for {zarr_store_url}: {e}", exc_info=True)
        return None