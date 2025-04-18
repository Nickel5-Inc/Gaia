import asyncio
import traceback
import base64
import pickle
from typing import Any, Dict, Optional
import xarray as xr
from fiber.logging_utils import get_logger
from ..utils.data_prep import create_aurora_batch_from_gfs
from aurora import Batch

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

        try:
            logger.debug("Decoding gfs_timestep_1 (historical, e.g., 00z)")
            ds_hist_bytes = base64.b64decode(data['gfs_timestep_1'])
            ds_hist = pickle.loads(ds_hist_bytes)
            if not isinstance(ds_hist, xr.Dataset):
                raise TypeError("Decoded gfs_timestep_1 is not an xarray Dataset")

            logger.debug("Decoding gfs_timestep_2 (current, e.g., 06z)")
            ds_curr_bytes = base64.b64decode(data['gfs_timestep_2'])
            ds_curr = pickle.loads(ds_curr_bytes)
            if not isinstance(ds_curr, xr.Dataset):
                raise TypeError("Decoded gfs_timestep_2 is not an xarray Dataset")

        except (TypeError, pickle.UnpicklingError, base64.binascii.Error) as decode_err:
            logger.error(f"Failed to decode/unpickle GFS data: {decode_err}")
            logger.error(traceback.format_exc())
            return None

        if 'time' not in ds_hist.dims or 'time' not in ds_curr.dims:
                logger.error("Decoded datasets missing 'time' dimension.")
                return None
                
        if ds_hist.time.values[0] >= ds_curr.time.values[0]:
            logger.warning("gfs_timestep_1 (historical) time is not strictly before gfs_timestep_2 (current). Ensure correct order.")

        try:
            logger.info("Combining historical and current GFS timesteps.")
            combined_gfs_data = xr.concat([ds_hist, ds_curr], dim='time')
            combined_gfs_data = combined_gfs_data.sortby('time')
            logger.info(f"Combined dataset time range: {combined_gfs_data.time.min().values} to {combined_gfs_data.time.max().values}")
            if len(combined_gfs_data.time) != 2:
                logger.warning(f"Expected 2 time steps after combining, found {len(combined_gfs_data.time)}")

        except Exception as combine_err:
                logger.error(f"Failed to combine GFS datasets: {combine_err}")
                logger.error(traceback.format_exc())
                return None

        resolution = '0.25' 
        static_download_dir = './static_data' 

        logger.info(f"Creating Aurora Batch using {resolution} resolution settings.")
        aurora_batch = create_aurora_batch_from_gfs(
            gfs_data=combined_gfs_data,
            resolution=resolution,
            download_dir=static_download_dir,
            history_steps=2
        )

        if _AURORA_AVAILABLE and Batch != Any:
            if not isinstance(aurora_batch, Batch):
                logger.error(f"Failed to create a valid Aurora Batch object. Type was {type(aurora_batch)}.")
                return None
        elif aurora_batch is None:
             logger.error(f"create_aurora_batch_from_gfs returned None.")
             return None

        logger.info("Successfully finished miner preprocessing.")
        return aurora_batch

    except Exception as e:
        logger.error(f"Unhandled error in prepare_miner_batch_from_payload: {e}")
        logger.error(traceback.format_exc())
        return None