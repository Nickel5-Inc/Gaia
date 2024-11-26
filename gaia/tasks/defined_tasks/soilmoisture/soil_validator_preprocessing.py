from gaia.tasks.base.components.preprocessing import Preprocessing
from datetime import datetime, timezone, date
from huggingface_hub import hf_hub_download
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.soilmoisture.utils.region_selection import (
    select_random_region,
)
from gaia.tasks.defined_tasks.soilmoisture.utils.soil_apis import get_soil_data
import json
from typing import Dict, Optional, List
import os
from sqlalchemy import text
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class SoilValidatorPreprocessing(Preprocessing):
    """Handles region selection and data collection for soil moisture task."""

    def __init__(self):
        super().__init__()
        self.db = ValidatorDatabaseManager()
        self._h3_data = self._load_h3_map()
        self._base_cells = [
            {"index": cell["index"], "resolution": cell["resolution"]}
            for cell in self._h3_data["base_cells"]
        ]
        self._urban_cells = set(
            cell["index"] for cell in self._h3_data["urban_overlay_cells"]
        )
        self._lakes_cells = set(
            cell["index"] for cell in self._h3_data["lakes_overlay_cells"]
        )
        self._daily_regions = {}
        self.max_daily_regions = 2

    def _load_h3_map(self):
        """Load H3 map data, first checking locally then from HuggingFace."""
        local_path = "./data/h3_map/full_h3_map.json"

        try:
            if os.path.exists(local_path):
                with open(local_path, "r") as f:
                    return json.load(f)

            logger.info("Local H3 map not found, downloading from HuggingFace...")
            map_path = hf_hub_download(
                repo_id="Nickel5HF/gaia_h3_mapping",
                filename="full_h3_map.json",
                repo_type="dataset",
                local_dir="./data/h3_map",
            )
            with open(map_path, "r") as f:
                return json.load(f)

        except Exception as e:
            logger.error(f"Error accessing H3 map: {str(e)}")
            logger.info("Using fallback local map...")
            raise RuntimeError("No H3 map available")

    def _can_select_region(self) -> bool:
        """Check if we can select more regions today."""
        today = date.today()
        count = self._daily_regions.get(today, 0)
        return count < self.max_daily_regions

    def get_soil_data(self, bbox: Dict, current_time: datetime) -> Optional[Dict]:
        """Collect and prepare data for a given region."""
        try:
            soil_data = get_soil_data(bbox=bbox, datetime_obj=current_time)
            if soil_data is None:
                logger.warning(f"Failed to get soil data for region {bbox}")
                return None

            return soil_data

        except Exception as e:
            logger.error(f"Error collecting soil data: {str(e)}")
            return None

    async def store_region(self, region: Dict, target_time: datetime) -> int:
        """Store region data in database."""
        logger.info(f"Storing region with bbox: {region['bbox']}")
        
        # Read the tiff file into bytes
        with open(region["combined_data"], 'rb') as f:
            combined_data_bytes = f.read()
            logger.info(f"Read TIFF file, size: {len(combined_data_bytes) / (1024 * 1024):.2f} MB")
        
        sentinel_bounds = [float(x) for x in region["sentinel_bounds"]]
        sentinel_crs = int(str(region["sentinel_crs"]).split(':')[-1])
        
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
                text("""
                    INSERT INTO soil_moisture_regions 
                    (region_date, target_time, bbox, combined_data, 
                     sentinel_bounds, sentinel_crs, array_shape, status)
                    VALUES (:region_date, :target_time, :bbox, :combined_data, 
                            :sentinel_bounds, :sentinel_crs, :array_shape, :status)
                    RETURNING id
                """), 
                data
            )
            region_id = result.scalar_one()
            
            await conn.execute(
                text("""
                    UPDATE soil_moisture_regions 
                    SET status = 'sent_to_miners'
                    WHERE id = :id
                """), 
                {"id": region_id}
            )
            
            await conn.commit()
            return region_id
        finally:
            await conn.close()

    async def get_daily_regions(
        self, target_time: datetime, ifs_forecast_time: datetime
    ) -> List[Dict]:
        """Get regions for today, selecting new ones if needed."""
        regions = []
        today = date.today()
        count = self._daily_regions.get(today, 0)

        while self._can_select_region():
            try:
                bbox = select_random_region(
                    base_cells=self._base_cells,
                    urban_cells_set=self._urban_cells,
                    lakes_cells_set=self._lakes_cells
                )
                soil_data = self.get_soil_data(bbox, ifs_forecast_time)

                if soil_data is not None:
                    tiff_path, bounds, crs = soil_data
                    region_data = {
                        "datetime": target_time,
                        "bbox": bbox,
                        "combined_data": tiff_path,
                        "sentinel_bounds": bounds,
                        "sentinel_crs": crs,
                        "array_shape": (222, 222)
                    }
                    region_id = await self.store_region(region_data, target_time)
                    region_data["id"] = region_id
                    regions.append(region_data)
                    count += 1
                    self._daily_regions[today] = count

            except Exception as e:
                logger.error(f"Error processing region: {str(e)}")
                continue

        return regions

    def _update_daily_count(self, date: date) -> None:
        """Update daily region count."""
        if date not in self._daily_regions:
            self._daily_regions[date] = 0
        self._daily_regions[date] += 1
