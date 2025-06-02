import asyncio
import traceback
import base64
import pickle
from typing import Any, Dict, Optional
import xarray as xr
from fiber.logging_utils import get_logger
from ..utils.data_prep import create_aurora_batch_from_gfs
from aurora import Batch
import gc

logger = get_logger(__name__)

async def prepare_miner_batch_from_payload(
    data: Optional[Dict[str, Any]] = None,
) -> Optional[Batch]:
    """
    Loads and preprocesses the input GFS data payload received from the validator.
    Decodes the data, combines timesteps, and creates an Aurora Batch object.

    Args:
        data: Dictionary containing the raw payload from the validator,
                expected to have 'gfs_timestep_1' and 'gfs_timestep_2'
                containing base64 encoded pickled xarray Datasets.

    Returns:
        An aurora.Batch object ready for model inference, or None if preprocessing fails.
    """
    if not data:
        logger.error("No data provided to prepare_miner_batch_from_payload.")
        return None

    try:
        logger.info("Starting miner preprocessing...")

        if 'gfs_timestep_1' not in data or 'gfs_timestep_2' not in data:
                logger.error("Missing 'gfs_timestep_1' or 'gfs_timestep_2' in input data.")
                return None

        # Track memory usage for large data processing
        ds_hist = None
        ds_curr = None
        ds_hist_bytes = None
        ds_curr_bytes = None
        
        try:
            logger.debug("Decoding gfs_timestep_1 (historical, e.g., 00z)")
            ds_hist_bytes = base64.b64decode(data['gfs_timestep_1'])
            
            # Log payload size for memory tracking
            hist_size_mb = len(ds_hist_bytes) / (1024 * 1024)
            if hist_size_mb > 20:  # Log large payloads
                logger.warning(f"Large GFS historical payload: {hist_size_mb:.1f}MB")
            
            ds_hist = pickle.loads(ds_hist_bytes)
            if not isinstance(ds_hist, xr.Dataset):
                raise TypeError("Decoded gfs_timestep_1 is not an xarray Dataset")
            
            # Immediately clean up intermediate bytes
            del ds_hist_bytes
            ds_hist_bytes = None

            logger.debug("Decoding gfs_timestep_2 (current, e.g., 06z)")
            ds_curr_bytes = base64.b64decode(data['gfs_timestep_2'])
            
            # Log payload size for memory tracking
            curr_size_mb = len(ds_curr_bytes) / (1024 * 1024)
            if curr_size_mb > 20:  # Log large payloads
                logger.warning(f"Large GFS current payload: {curr_size_mb:.1f}MB")
            
            ds_curr = pickle.loads(ds_curr_bytes)
            if not isinstance(ds_curr, xr.Dataset):
                raise TypeError("Decoded gfs_timestep_2 is not an xarray Dataset")
            
            # Immediately clean up intermediate bytes
            del ds_curr_bytes
            ds_curr_bytes = None

        except (TypeError, pickle.UnpicklingError, base64.binascii.Error) as decode_err:
            logger.error(f"Failed to decode/unpickle GFS data: {decode_err}")
            logger.error(traceback.format_exc())
            # Clean up on error
            if ds_hist_bytes:
                del ds_hist_bytes
            if ds_curr_bytes:
                del ds_curr_bytes
            if ds_hist:
                ds_hist.close()
            if ds_curr:
                ds_curr.close()
            return None

        if 'time' not in ds_hist.dims or 'time' not in ds_curr.dims:
                logger.error("Decoded datasets missing 'time' dimension.")
                # Clean up on error
                if ds_hist:
                    ds_hist.close()
                if ds_curr:
                    ds_curr.close()
                return None
                
        if ds_hist.time.values[0] >= ds_curr.time.values[0]:
            logger.warning("gfs_timestep_1 (historical) time is not strictly before gfs_timestep_2 (current). Ensure correct order.")

        combined_gfs_data = None
        try:
            logger.info("Combining historical and current GFS timesteps.")
            combined_gfs_data = xr.concat([ds_hist, ds_curr], dim='time')
            combined_gfs_data = combined_gfs_data.sortby('time')
            logger.info(f"Combined dataset time range: {combined_gfs_data.time.min().values} to {combined_gfs_data.time.max().values}")
            if len(combined_gfs_data.time) != 2:
                logger.warning(f"Expected 2 time steps after combining, found {len(combined_gfs_data.time)}")

            # Clean up source datasets immediately after combination
            if ds_hist:
                ds_hist.close()
                del ds_hist
                ds_hist = None
            if ds_curr:
                ds_curr.close()
                del ds_curr
                ds_curr = None

        except Exception as combine_err:
                logger.error(f"Failed to combine GFS datasets: {combine_err}")
                logger.error(traceback.format_exc())
                # Clean up on error
                if ds_hist:
                    ds_hist.close()
                if ds_curr:
                    ds_curr.close()
                if combined_gfs_data:
                    combined_gfs_data.close()
                return None

        resolution = '0.25' 
        static_download_dir = './static_data' 

        logger.info(f"Creating Aurora Batch using {resolution} resolution settings.")
        aurora_batch = None
        try:
            aurora_batch = create_aurora_batch_from_gfs(
                gfs_data=combined_gfs_data,
                resolution=resolution,
                download_dir=static_download_dir,
                history_steps=2
            )
        finally:
            # Clean up combined dataset after batch creation
            if combined_gfs_data:
                combined_gfs_data.close()
                del combined_gfs_data

        # Validate the aurora_batch
        try:
            from aurora import Batch as AuroraBatch
            _AURORA_AVAILABLE = True
        except ImportError:
            _AURORA_AVAILABLE = False
            
        if _AURORA_AVAILABLE and aurora_batch is not None:
            if not isinstance(aurora_batch, AuroraBatch):
                logger.error(f"Failed to create a valid Aurora Batch object. Type was {type(aurora_batch)}.")
                return None
        elif aurora_batch is None:
             logger.error(f"create_aurora_batch_from_gfs returned None.")
             return None

        # Force garbage collection for large data cleanup
        total_size_mb = (hist_size_mb if 'hist_size_mb' in locals() else 0) + (curr_size_mb if 'curr_size_mb' in locals() else 0)
        if total_size_mb > 50:
            collected = gc.collect()
            logger.info(f"Cleaned up large weather data ({total_size_mb:.1f}MB), GC collected {collected} objects")

        logger.info("Successfully finished miner preprocessing.")
        return aurora_batch

    except Exception as e:
        logger.error(f"Unhandled error in prepare_miner_batch_from_payload: {e}")
        logger.error(traceback.format_exc())
        
        # Final cleanup on any unhandled error
        try:
            if 'ds_hist' in locals() and ds_hist:
                ds_hist.close()
            if 'ds_curr' in locals() and ds_curr:
                ds_curr.close()
            if 'combined_gfs_data' in locals() and combined_gfs_data:
                combined_gfs_data.close()
            gc.collect()
        except Exception:
            pass
            
        return None