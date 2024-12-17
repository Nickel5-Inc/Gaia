from datetime import datetime, timezone
from prefect import flow, task, get_run_logger
from typing import Dict, List, Optional, Tuple, Any, Union
import tempfile
import os
from ..utils.smap_api import (
    construct_smap_url,
    download_smap_data,
    get_smap_data_for_sentinel_bounds,
    get_valid_smap_time
)

logger = get_run_logger()

@task(
    name="download_smap_data_task",
    retries=3,
    retry_delay_seconds=60,
    tags=["data_collection", "smap"],
    description="Download SMAP data with caching"
)
async def download_smap_data_task(url: str, output_path: str) -> Optional[str]:
    """
    Download SMAP data with Prefect task management.
    """
    try:
        logger.info(f"Downloading SMAP data from {url}")
        success = download_smap_data(url, output_path)
        if success:
            logger.info(f"Successfully downloaded SMAP data to {output_path}")
            return output_path
        logger.error("Failed to download SMAP data")
        return None
    except Exception as e:
        logger.error(f"Error downloading SMAP data: {str(e)}")
        raise

@task(
    name="process_smap_data_task",
    retries=2,
    retry_delay_seconds=30,
    tags=["data_processing", "smap"],
    description="Process SMAP data for given bounds"
)
async def process_smap_data_task(
    filepath: str,
    bounds: Tuple[float, float, float, float],
    crs: str
) -> Optional[Dict[str, Any]]:
    """
    Process SMAP data with Prefect task management.
    """
    try:
        logger.info(f"Processing SMAP data for bounds {bounds}")
        result = get_smap_data_for_sentinel_bounds(filepath, bounds, crs)
        if result:
            logger.info("Successfully processed SMAP data")
            return result
        logger.error("Failed to process SMAP data")
        return None
    except Exception as e:
        logger.error(f"Error processing SMAP data: {str(e)}")
        raise

@task(
    name="validate_smap_data_task",
    retries=2,
    tags=["validation", "smap"],
    description="Validate SMAP data quality"
)
async def validate_smap_data_task(data: Dict[str, Any]) -> bool:
    """
    Validate SMAP data quality with Prefect task management.
    """
    try:
        import numpy as np
        
        # Check surface soil moisture
        surface_sm = data.get("surface_sm")
        if surface_sm is None or not isinstance(surface_sm, np.ndarray):
            logger.error("Invalid surface soil moisture data")
            return False
            
        # Check rootzone soil moisture
        rootzone_sm = data.get("rootzone_sm")
        if rootzone_sm is None or not isinstance(rootzone_sm, np.ndarray):
            logger.error("Invalid rootzone soil moisture data")
            return False
            
        # Check for valid ranges (typical SMAP ranges)
        if (np.nanmin(surface_sm) < 0 or np.nanmax(surface_sm) > 1 or
            np.nanmin(rootzone_sm) < 0 or np.nanmax(rootzone_sm) > 1):
            logger.error("Soil moisture values outside valid range [0,1]")
            return False
            
        # Check for sufficient valid data
        min_valid_ratio = 0.5  # At least 50% valid data
        surface_valid_ratio = np.sum(~np.isnan(surface_sm)) / surface_sm.size
        rootzone_valid_ratio = np.sum(~np.isnan(rootzone_sm)) / rootzone_sm.size
        
        if surface_valid_ratio < min_valid_ratio or rootzone_valid_ratio < min_valid_ratio:
            logger.error(f"Insufficient valid data: surface={surface_valid_ratio:.2f}, rootzone={rootzone_valid_ratio:.2f}")
            return False
            
        logger.info("SMAP data validation successful")
        return True
        
    except Exception as e:
        logger.error(f"Error validating SMAP data: {str(e)}")
        raise

@flow(
    name="smap_data_flow",
    retries=3,
    description="Orchestrates SMAP data collection and processing"
)
async def smap_data_flow(
    datetime_obj: datetime,
    bounds: Tuple[float, float, float, float],
    crs: str
) -> Optional[Dict[str, Any]]:
    """
    Main flow for SMAP data handling.
    """
    try:
        # Get valid SMAP time
        valid_time = get_valid_smap_time(datetime_obj)
        logger.info(f"Using SMAP time: {valid_time}")
        
        # Construct URL and prepare temp file
        smap_url = construct_smap_url(valid_time)
        temp_dir = tempfile.mkdtemp(prefix="smap_")
        temp_file = os.path.join(temp_dir, f"smap_{valid_time.strftime('%Y%m%d_%H%M')}.h5")
        
        # Download data
        downloaded_file = await download_smap_data_task(smap_url, temp_file)
        if not downloaded_file:
            raise ValueError("Failed to download SMAP data")
            
        # Process data
        processed_data = await process_smap_data_task(downloaded_file, bounds, crs)
        if not processed_data:
            raise ValueError("Failed to process SMAP data")
            
        # Validate data
        is_valid = await validate_smap_data_task(processed_data)
        if not is_valid:
            raise ValueError("SMAP data validation failed")
            
        logger.info("Successfully completed SMAP data flow")
        return processed_data
        
    except Exception as e:
        logger.error(f"Error in SMAP data flow: {str(e)}")
        raise
    finally:
        # Cleanup
        if 'temp_dir' in locals():
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up temporary files: {str(cleanup_error)}") 