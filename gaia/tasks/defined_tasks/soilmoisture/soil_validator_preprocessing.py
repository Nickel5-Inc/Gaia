from typing import Dict, Optional, List, Any, Tuple, Union
from datetime import datetime
import json
import os
import shutil
import traceback
from sqlalchemy import text
from fiber.logging_utils import get_logger

from gaia.tasks.base.components.preprocessing import Preprocessing
from gaia.tasks.base.components.inputs import Inputs
from gaia.tasks.defined_tasks.soilmoisture.soil_inputs import SoilMoistureInputs
from gaia.tasks.defined_tasks.soilmoisture.utils.soil_apis import get_soil_data, get_data_dir

logger = get_logger(__name__)

class SoilValidatorPreprocessing(Preprocessing):
    """Preprocessing for soil moisture task on validator side."""

    def __init__(self, db_manager, regions_per_timestep=10):
        """Initialize preprocessing."""
        super().__init__()
        self.db = db_manager
        self.regions_per_timestep = regions_per_timestep
        self._daily_regions = {}
        self._urban_cells = set()
        self._lakes_cells = set()

    async def _check_existing_regions(self, target_time: datetime) -> bool:
        """Check if regions already exist for the given target time."""
        try:
            query = """
            SELECT COUNT(*) as count 
            FROM soil_moisture_regions 
            WHERE target_time = :target_time
            """
            params = {"target_time": target_time}
            existing_regions = await self.db.fetch_many(query, params)
            count = existing_regions[0]["count"] if existing_regions else 0
            return count >= self.regions_per_timestep
        except Exception as e:
            logger.error(f"Error checking existing regions: {str(e)}")
            return False

    async def get_soil_data(self, bbox: Dict[str, float], current_time: datetime) -> Optional[Tuple[str, Tuple[float, float, float, float], int]]:
        """
        Collect and prepare data for a given region.
        
        Args:
            bbox: Dictionary containing bounding box coordinates
            current_time: Current datetime
            
        Returns:
            Optional tuple containing (file_path: str, bounds: Tuple[float, float, float, float], crs: int)
            or None if data collection fails
        """
        try:
            soil_data = await get_soil_data(bbox=bbox, datetime_obj=current_time)
            if soil_data is None:
                logger.warning(f"Failed to get soil data for region {bbox}")
                return None

            # Validate the return type
            if not isinstance(soil_data, tuple) or len(soil_data) != 3:
                logger.error(f"Invalid soil data format: {soil_data}")
                return None

            file_path, bounds, crs = soil_data
            if not isinstance(file_path, str):
                logger.error(f"Invalid file path type: {type(file_path)}")
                return None

            if not isinstance(bounds, tuple) or len(bounds) != 4 or not all(isinstance(x, (int, float)) for x in bounds):
                logger.error(f"Invalid bounds format: {bounds}")
                return None

            if not isinstance(crs, int):
                logger.error(f"Invalid CRS type: {type(crs)}")
                return None

            return file_path, bounds, crs

        except Exception as e:
            logger.error(f"Error collecting soil data: {str(e)}")
            return None

    async def store_region(self, region: Dict[str, Any], target_time: datetime) -> Optional[int]:
        """Store region data in database."""
        try:
            logger.info(f"Storing region with bbox: {region['bbox']}")

            # Read and validate the tiff file
            with open(region["combined_data"], "rb") as f:
                combined_data_bytes = f.read()
                logger.info(
                    f"Read TIFF file, size: {len(combined_data_bytes) / (1024 * 1024):.2f} MB"
                )

                # Check TIFF header (supports both little-endian 'II' and big-endian 'MM' formats)
                if not (
                    combined_data_bytes.startswith(b"II\x2A\x00")
                    or combined_data_bytes.startswith(b"MM\x00\x2A")
                ):
                    logger.error("Invalid TIFF format: Missing TIFF header")
                    logger.error(f"First 16 bytes: {combined_data_bytes[:16].hex()}")
                    raise ValueError(
                        "Invalid TIFF format: File does not start with valid TIFF header"
                    )

            sentinel_bounds = [float(x) for x in region["sentinel_bounds"]]
            sentinel_crs = int(str(region["sentinel_crs"]).split(":")[-1])

            data = {
                "region_date": target_time.date(),
                "target_time": target_time,
                "bbox": json.dumps(region["bbox"]),
                "combined_data": combined_data_bytes,
                "sentinel_bounds": sentinel_bounds,
                "sentinel_crs": sentinel_crs,
                "array_shape": region["array_shape"],
                "status": "pending",
            }

            conn = await self.db.get_connection()
            try:
                result = await conn.execute(
                    text(
                        """
                        INSERT INTO soil_moisture_regions 
                        (region_date, target_time, bbox, combined_data, 
                         sentinel_bounds, sentinel_crs, array_shape, status)
                        VALUES (:region_date, :target_time, :bbox, :combined_data, 
                                :sentinel_bounds, :sentinel_crs, :array_shape, :status)
                        RETURNING id
                    """
                    ),
                    data,
                )
                region_id = await result.scalar_one()
                await conn.commit()
                return region_id

            finally:
                await conn.close()

        except Exception as e:
            logger.error(f"Error storing region: {str(e)}")
            raise RuntimeError(f"Failed to store region: {str(e)}")

    def process_miner_data(self, data: Union[Dict[str, Any], Inputs]) -> Optional[Inputs]:
        """
        Process miner data for validation.
        
        Args:
            data: Input data to process, either as a dictionary or Inputs object
            
        Returns:
            Optional[Inputs]: Processed data for validation or None if validation fails
        """
        try:
            # Convert dictionary input to Inputs type if needed
            if isinstance(data, dict):
                inputs = SoilMoistureInputs()
                inputs.inputs = data
                data = inputs
                
            if not isinstance(data, Inputs):
                logger.error(f"Input data must be of type Inputs or Dict, got {type(data)}")
                return None
                
            data_dict = data.inputs
            if not isinstance(data_dict, dict):
                logger.error("Input data.inputs must be a dictionary")
                return None
                
            # Extract required fields
            region_id = data_dict.get("region_id")
            combined_data = data_dict.get("combined_data")
            sentinel_bounds = data_dict.get("sentinel_bounds")
            sentinel_crs = data_dict.get("sentinel_crs")
            target_time = data_dict.get("target_time")
            
            if any(x is None for x in [region_id, combined_data, sentinel_bounds, sentinel_crs, target_time]):
                logger.error("Missing required fields in input data")
                return None

            # Validate data types
            if not isinstance(region_id, int):
                logger.error("region_id must be an integer")
                return None
                
            if not isinstance(sentinel_bounds, list) or len(sentinel_bounds) != 4:
                logger.error("sentinel_bounds must be a list of 4 floats")
                return None
                
            if not isinstance(sentinel_crs, int):
                logger.error("sentinel_crs must be an integer")
                return None
                
            if not isinstance(target_time, (str, datetime)):
                logger.error("target_time must be a string or datetime")
                return None

            # Convert target_time to datetime if it's a string
            if isinstance(target_time, str):
                try:
                    target_time = datetime.fromisoformat(target_time)
                except ValueError:
                    logger.error("Invalid target_time format")
                    return None
                    
            # Return processed data as Inputs
            processed_data = SoilMoistureInputs()
            processed_data.inputs = {
                "region_id": region_id,
                "combined_data": combined_data,
                "sentinel_bounds": sentinel_bounds,
                "sentinel_crs": sentinel_crs,
                "target_time": target_time
            }
            
            return processed_data
            
        except Exception as e:
            logger.error(f"Error processing miner data: {e}")
            logger.error(traceback.format_exc())
            return None
