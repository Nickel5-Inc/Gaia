"""
Weather hashing compute handlers for the Compute Worker.

This module contains synchronous functions that perform CPU-intensive
hash computations in the compute worker processes.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def handle_gfs_hash_computation(config, gfs_t0_run_time_iso: str, cache_dir: str, **kwargs) -> str:
    """
    Synchronous handler for GFS hash computation.
    Heavy imports are done locally to keep worker memory footprint small.
    
    Args:
        config: Configuration object with settings
        gfs_t0_run_time_iso: ISO format string of GFS T0 run time
        cache_dir: Directory path for GFS analysis cache
        **kwargs: Additional parameters
    
    Returns:
        str: Computed validator hash
        
    Raises:
        ValueError: If hash computation fails or returns None
    """
    from datetime import datetime
    
    # Import the actual hashing function - delayed import to keep worker lightweight
    try:
        from gaia.tasks.defined_tasks.weather.utils.hashing import compute_input_data_hash
    except ImportError as e:
        raise ImportError(f"Failed to import hashing utility: {e}")
    
    logger.info(f"Computing GFS hash for T0 time: {gfs_t0_run_time_iso}")
    
    try:
        # Parse the ISO time string
        gfs_t0_run_time = datetime.fromisoformat(gfs_t0_run_time_iso.replace('Z', '+00:00'))
        gfs_t_minus_6 = gfs_t0_run_time - timedelta(hours=6)
        
        # Convert cache_dir to Path
        cache_path = Path(cache_dir)
        
        # Since the original function is async, we need to run it in the event loop
        # But this handler is synchronous, so we'll create a new event loop
        try:
            # Try to get the existing event loop
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If loop is running, we need to run in a thread
                # This shouldn't happen in compute workers, but just in case
                raise RuntimeError("Event loop already running")
        except RuntimeError:
            # No event loop, create one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        try:
            # Run the async hash computation
            validator_hash = loop.run_until_complete(
                compute_input_data_hash(
                    t0_run_time=gfs_t0_run_time,
                    t_minus_6_run_time=gfs_t_minus_6,
                    cache_dir=cache_path
                )
            )
        finally:
            # Clean up the event loop
            loop.close()
        
        if not validator_hash:
            raise ValueError("Hash computation returned None or empty string")
        
        logger.info(f"Successfully computed hash: {validator_hash[:12]}...")
        return validator_hash
        
    except Exception as e:
        logger.error(f"Error computing GFS hash: {e}")
        raise ValueError(f"Hash computation failed: {e}")


def handle_verification_hash_computation(
    config, 
    zarr_data: bytes,
    metadata: Dict[str, Any],
    asset_base_name: str,
    miner_hotkey_ss58: str,
    compression_level: int = 1,
    **kwargs
) -> str:
    """
    Synchronous handler for verification hash computation.
    
    Args:
        config: Configuration object
        zarr_data: Serialized zarr dataset bytes
        metadata: Dataset metadata
        asset_base_name: Base name for the asset
        miner_hotkey_ss58: Miner's hotkey in SS58 format
        compression_level: Compression level for zarr
        **kwargs: Additional parameters
    
    Returns:
        str: Computed verification hash
        
    Raises:
        ValueError: If verification hash computation fails
    """
    import tempfile
    import pickle
    from pathlib import Path
    from substrateinterface.base import Keypair
    
    try:
        from gaia.tasks.defined_tasks.weather.utils.hashing import compute_verification_hash
    except ImportError as e:
        raise ImportError(f"Failed to import hashing utility: {e}")
    
    logger.info(f"Computing verification hash for asset: {asset_base_name}")
    
    try:
        # Deserialize the zarr dataset
        dataset = pickle.loads(zarr_data)
        
        # Create a temporary directory for zarr store
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_zarr_dir = Path(temp_dir)
            
            # Create a keypair from the SS58 address
            miner_keypair = Keypair(ss58_address=miner_hotkey_ss58)
            
            # Compute the verification hash
            verification_hash = compute_verification_hash(
                data_xr=dataset,
                metadata=metadata,
                temp_zarr_dir=temp_zarr_dir,
                asset_base_name=asset_base_name,
                miner_hotkey_keypair=miner_keypair,
                compression_level=compression_level,
                chunk_hash_algo="xxh64"
            )
            
            if not verification_hash:
                raise ValueError("Verification hash computation returned None")
            
            logger.info(f"Successfully computed verification hash: {verification_hash[:12]}...")
            return verification_hash
            
    except Exception as e:
        logger.error(f"Error computing verification hash: {e}")
        raise ValueError(f"Verification hash computation failed: {e}")


def handle_forecast_hash_verification(
    config,
    zarr_store_url: str,
    claimed_hash: str,
    miner_hotkey_ss58: str,
    job_id: str = "unknown",
    **kwargs
) -> Dict[str, Any]:
    """
    Synchronous handler for forecast hash verification.
    
    Args:
        config: Configuration object
        zarr_store_url: URL of the zarr store to verify
        claimed_hash: Hash claimed by the miner
        miner_hotkey_ss58: Miner's hotkey in SS58 format
        job_id: Job identifier for logging
        **kwargs: Additional parameters
    
    Returns:
        Dict containing verification results
        
    Raises:
        ValueError: If verification fails
    """
    try:
        from gaia.tasks.defined_tasks.weather.utils.hashing import verify_manifest_and_get_trusted_store
    except ImportError as e:
        raise ImportError(f"Failed to import hashing utility: {e}")
    
    logger.info(f"Verifying forecast hash for job {job_id}")
    
    try:
        # Since the original function is async, we need to handle the event loop
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                raise RuntimeError("Event loop already running")
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        try:
            # Run the async verification
            trusted_mapper = loop.run_until_complete(
                verify_manifest_and_get_trusted_store(
                    zarr_store_url=zarr_store_url,
                    claimed_manifest_content_hash=claimed_hash,
                    miner_hotkey_ss58=miner_hotkey_ss58,
                    job_id=job_id
                )
            )
        finally:
            loop.close()
        
        if trusted_mapper is None:
            return {
                "verified": False,
                "error": "Manifest verification failed",
                "job_id": job_id
            }
        
        logger.info(f"Successfully verified forecast hash for job {job_id}")
        return {
            "verified": True,
            "job_id": job_id,
            "trusted_mapper_info": {
                "root": getattr(trusted_mapper, 'root', None),
                "manifest_version": getattr(trusted_mapper, 'trusted_manifest', {}).get('manifest_schema_version')
            }
        }
        
    except Exception as e:
        logger.error(f"Error verifying forecast hash for job {job_id}: {e}")
        return {
            "verified": False,
            "error": str(e),
            "job_id": job_id
        }