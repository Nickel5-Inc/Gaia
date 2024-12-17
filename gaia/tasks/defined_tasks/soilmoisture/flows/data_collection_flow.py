from datetime import datetime, timezone
from prefect import flow, task, get_run_logger
from typing import Dict, List, Optional, Tuple, Any, Union
import tempfile
import os
from ..utils.soil_apis import (
    fetch_hls_b4_b8,
    fetch_srtm,
    fetch_ifs_forecast,
    combine_tiffs,
    get_data_dir
)

logger = get_run_logger()

@task(
    name="fetch_sentinel_data",
    retries=3,
    retry_delay_seconds=60,
    tags=["data_collection", "sentinel"],
    description="Fetch Sentinel-2 B4 and B8 bands"
)
async def fetch_sentinel_data(
    bbox: Tuple[float, float, float, float],
    datetime_obj: datetime
) -> Optional[List[str]]:
    """
    Fetch Sentinel-2 data with Prefect task management.
    """
    try:
        logger.info(f"Fetching Sentinel data for bbox {bbox} at {datetime_obj}")
        result = await fetch_hls_b4_b8(bbox, datetime_obj)
        if result:
            logger.info(f"Successfully fetched Sentinel data: {result}")
            return result
        logger.error("Failed to fetch Sentinel data")
        return None
    except Exception as e:
        logger.error(f"Error fetching Sentinel data: {str(e)}")
        raise

@task(
    name="fetch_elevation_data",
    retries=3,
    retry_delay_seconds=60,
    tags=["data_collection", "srtm"],
    description="Fetch SRTM elevation data"
)
async def fetch_elevation_data(
    bbox: Tuple[float, float, float, float],
    sentinel_bounds: Optional[Tuple[float, float, float, float]] = None,
    sentinel_crs: Optional[str] = None,
    sentinel_shape: Optional[Tuple[int, int]] = None
) -> Optional[Tuple[Any, Any, Any, Any]]:
    """
    Fetch SRTM elevation data with Prefect task management.
    """
    try:
        logger.info(f"Fetching SRTM data for bbox {bbox}")
        result = await fetch_srtm(
            bbox,
            sentinel_bounds=sentinel_bounds,
            sentinel_crs=sentinel_crs,
            sentinel_shape=sentinel_shape
        )
        if result[0] is not None:
            logger.info("Successfully fetched SRTM data")
            return result
        logger.error("Failed to fetch SRTM data")
        return None
    except Exception as e:
        logger.error(f"Error fetching SRTM data: {str(e)}")
        raise

@task(
    name="fetch_weather_data",
    retries=3,
    retry_delay_seconds=60,
    tags=["data_collection", "weather"],
    description="Fetch IFS weather forecast data"
)
async def fetch_weather_data(
    bbox: Tuple[float, float, float, float],
    datetime_obj: datetime,
    sentinel_bounds: Optional[Tuple[float, float, float, float]] = None,
    sentinel_crs: Optional[str] = None,
    sentinel_shape: Optional[Tuple[int, int]] = None
) -> Optional[List[Any]]:
    """
    Fetch IFS weather data with Prefect task management.
    """
    try:
        logger.info(f"Fetching weather data for bbox {bbox} at {datetime_obj}")
        result = await fetch_ifs_forecast(
            bbox,
            datetime_obj,
            sentinel_bounds=sentinel_bounds,
            sentinel_crs=sentinel_crs,
            sentinel_shape=sentinel_shape
        )
        if result:
            logger.info("Successfully fetched weather data")
            return result
        logger.error("Failed to fetch weather data")
        return None
    except Exception as e:
        logger.error(f"Error fetching weather data: {str(e)}")
        raise

@task(
    name="combine_data_sources",
    retries=2,
    retry_delay_seconds=30,
    tags=["data_processing"],
    description="Combine all data sources into a single package"
)
async def combine_data_sources(
    sentinel_data: List[str],
    elevation_data: Tuple[Any, Any, Any, Any],
    weather_data: List[Any],
    bbox: Tuple[float, float, float, float],
    datetime_obj: datetime
) -> Optional[str]:
    """
    Combine all data sources with Prefect task management.
    """
    try:
        logger.info("Combining data sources")
        result = await combine_tiffs(
            sentinel_data,
            weather_data,
            elevation_data,
            bbox,
            datetime_obj.strftime("%Y-%m-%d_%H%M"),
            {}  # Profile will be generated inside combine_tiffs
        )
        if result:
            logger.info(f"Successfully combined data: {result}")
            return result
        logger.error("Failed to combine data sources")
        return None
    except Exception as e:
        logger.error(f"Error combining data sources: {str(e)}")
        raise

@task(
    name="monitor_data_quality",
    retries=2,
    tags=["monitoring"],
    description="Monitor quality of collected data"
)
async def monitor_data_quality(
    data_path: str,
    context: str
) -> Dict[str, Any]:
    """
    Monitor quality of collected data.
    """
    try:
        import rasterio
        import numpy as np
        
        metrics = {
            "context": context,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "quality_checks": {}
        }
        
        with rasterio.open(data_path) as src:
            data = src.read()
            metrics["quality_checks"].update({
                "bands": src.count,
                "shape": data.shape,
                "has_valid_data": bool(np.any(data != src.nodata)),
                "percent_valid": float(np.sum(data != src.nodata)) / data.size * 100,
                "value_range": {
                    "min": float(np.nanmin(data)),
                    "max": float(np.nanmax(data)),
                    "mean": float(np.nanmean(data))
                }
            })
            
        logger.info(f"Data quality metrics for {context}: {metrics}")
        return metrics
    except Exception as e:
        logger.error(f"Error monitoring data quality: {str(e)}")
        raise

@flow(
    name="soil_data_collection_flow",
    retries=3,
    description="Orchestrates the collection of all required soil data"
)
async def collect_soil_data_flow(
    bbox: Tuple[float, float, float, float],
    datetime_obj: datetime,
    sentinel_bounds: Optional[Tuple[float, float, float, float]] = None,
    sentinel_crs: Optional[str] = None,
    sentinel_shape: Optional[Tuple[int, int]] = (222, 222)  # Default shape
) -> Optional[str]:
    """
    Main flow for collecting all required soil data.
    """
    try:
        # Execute tasks with proper dependencies
        sentinel_result = await fetch_sentinel_data(bbox, datetime_obj)
        if not sentinel_result:
            raise ValueError("Failed to fetch Sentinel data")
            
        elevation_result = await fetch_elevation_data(
            bbox,
            sentinel_bounds,
            sentinel_crs,
            sentinel_shape
        )
        if not elevation_result:
            raise ValueError("Failed to fetch elevation data")
            
        weather_result = await fetch_weather_data(
            bbox,
            datetime_obj,
            sentinel_bounds,
            sentinel_crs,
            sentinel_shape
        )
        if not weather_result:
            raise ValueError("Failed to fetch weather data")
            
        combined_result = await combine_data_sources(
            sentinel_result,
            elevation_result,
            weather_result,
            bbox,
            datetime_obj
        )
        if not combined_result:
            raise ValueError("Failed to combine data sources")
            
        # Monitor data quality
        quality_metrics = await monitor_data_quality(
            combined_result,
            f"soil_data_collection_{datetime_obj.strftime('%Y%m%d_%H%M')}"
        )
        
        logger.info(f"Successfully completed data collection flow: {quality_metrics}")
        return combined_result
        
    except Exception as e:
        logger.error(f"Error in soil data collection flow: {str(e)}")
        raise 