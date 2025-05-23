from gaia.tasks.base.components.preprocessing import Preprocessing
from datetime import datetime, timezone, date
from huggingface_hub import hf_hub_download
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.soilmoisture.utils.region_selection import (
    select_random_region,
    get_deterministic_seed,
)
from gaia.tasks.defined_tasks.soilmoisture.utils.soil_apis import get_soil_data, get_data_dir
import json
from typing import Dict, Optional, List
import os
import shutil
from sqlalchemy import text
from fiber.logging_utils import get_logger
import random
import traceback

logger = get_logger(__name__)


class SentinelServerError(Exception):
    """Raised when Sentinel server returns 500 error"""
    pass


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
        self.regions_per_timestep = 5
        self._logged_timestamps = set()

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

    def _can_select_region(self, target_time: datetime) -> bool:
        """Check if we can select more regions for this timestep."""
        today = target_time.date()
        hour = target_time.hour

        if today not in self._daily_regions:
            self._daily_regions[today] = {}
        if hour not in self._daily_regions[today]:
            self._daily_regions[today][hour] = 0

        return self._daily_regions[today][hour] < self.regions_per_timestep

    async def get_soil_data(self, bbox: Dict, current_time: datetime) -> Optional[Dict]:
        """Collect and prepare data for a given region."""
        try:
            soil_data = await get_soil_data(bbox=bbox, datetime_obj=current_time)
            if soil_data is None:
                logger.warning(f"Failed to get soil data for region {bbox}")
                return None

            return soil_data

        except Exception as e:
            if "Failed to fetch data: 500 Server Error" in str(e):
                logger.warning(f"Sentinel server returned 500 error for region {bbox}")
                raise SentinelServerError("Sentinel server maintenance")
            logger.warning(f"Failed to get soil data for region {bbox}")
            raise

    async def store_region(self, region: Dict, target_time: datetime) -> int:
        """Store region data in database."""
        try:
            logger.info(f"Storing region with bbox: {region['bbox']}")

            if region.get("combined_data") is None:
                logger.error(f"Combined data (TIFF path) is None for region {region.get('bbox')}, cannot store region.")
                # Option 1: Raise an error to stop the current region processing
                raise ValueError(f"Invalid TIFF path: None for region {region.get('bbox')}")
                # Option 2: Return an indicator of failure (e.g., None or -1 for region_id)
                # return -1 

            # Read and validate the tiff file
            with open(region["combined_data"], "rb") as f:
                combined_data_bytes = f.read()
                logger.info(
                    f"Read TIFF file, size: {len(combined_data_bytes) / (1024 * 1024):.2f} MB"
                )

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

            # WRITE operation - use execute for storing region
            result = await self.db.execute(
                """
                INSERT INTO soil_moisture_regions 
                (region_date, target_time, bbox, combined_data, 
                 sentinel_bounds, sentinel_crs, array_shape, status)
                VALUES (:region_date, :target_time, :bbox, :combined_data, 
                        :sentinel_bounds, :sentinel_crs, :array_shape, :status)
                RETURNING id
                """,
                data
            )
            region_id = result.scalar_one()
            return region_id

        except Exception as e:
            logger.error(f"Error storing region: {str(e)}")
            raise RuntimeError(f"Failed to store region: {str(e)}")

    async def _check_existing_regions(self, target_time: datetime) -> bool:
        """Check if regions already exist for the given target time."""
        try:
            # READ operation - use fetch_one for checking existing regions
            result = await self.db.fetch_one(
                """
                SELECT COUNT(*) as count 
                FROM soil_moisture_regions 
                WHERE target_time = :target_time
                """,
                {"target_time": target_time}
            )
            count = result['count'] if result else 0
            return count >= self.regions_per_timestep
        except Exception as e:
            logger.error(f"Error checking existing regions: {str(e)}")
            return False

    async def get_daily_regions(
        self, target_time: datetime, ifs_forecast_time: datetime
    ) -> List[Dict]:
        """Get regions for today, selecting new ones if needed."""
        processed_regions = [] # Renamed from regions to avoid conflict with module name
        try:
            has_existing_regions = await self._check_existing_regions(target_time)
            if has_existing_regions:
                if target_time not in self._logged_timestamps:
                    logger.info(
                        f"Already have {self.regions_per_timestep} regions for {target_time}, skipping download"
                    )
                    self._logged_timestamps.add(target_time)
                    self._cleanup_logged_timestamps()
                return []

            today = target_time.date()
            hour = target_time.hour
            seed = get_deterministic_seed(today, hour)
            random.seed(seed)
            logger.info(f"Set random seed to {seed} for target_time {target_time}")
            
            regions = []
            used_bounds = set()
            consecutive_500_errors = 0
            MAX_500_ERRORS = 3
            
            while len(processed_regions) < self.regions_per_timestep:
                bbox = None # Initialize bbox to None
                try:
                    bbox = select_random_region(
                        base_cells=self._base_cells,
                        urban_cells_set=self._urban_cells,
                        lakes_cells_set=self._lakes_cells, # Corrected typo: _lakes_cells_set
                        timestamp=target_time,
                        used_bounds=used_bounds
                    )
                    if bbox:
                        # get_soil_data now returns a tuple: (tiff_path, sentinel_bounds, sentinel_crs)
                        # or (None, None, None) on failure.
                        tiff_path, sentinel_bounds_val, sentinel_crs_val = await self.get_soil_data(bbox, ifs_forecast_time)

                        if tiff_path is not None and sentinel_bounds_val is not None and sentinel_crs_val is not None:
                            region_data = {
                                "datetime": target_time,
                                "bbox": bbox,
                                "combined_data": tiff_path, # This is the path string
                                "sentinel_bounds": sentinel_bounds_val,
                                "sentinel_crs": sentinel_crs_val,
                                "array_shape": (222, 222), # Assuming this is fixed or derived elsewhere if not
                            }
                            try:
                                region_id = await self.store_region(region_data, target_time)
                                if region_id != -1 and region_id is not None: # Check if store_region was successful
                                    region_data["id"] = region_id
                                    processed_regions.append(region_data)
                                    used_bounds.add(bbox) # Corrected: bbox is already a tuple
                                    self._update_daily_count(target_time)
                                else:
                                    logger.warning(f"Failed to store region for bbox {bbox}, store_region returned {region_id}.")
                            except ValueError as ve_store: # Catch specific error from store_region if tiff_path was None
                                logger.error(f"Error storing region for bbox {bbox}: {ve_store}")
                                # Decide if to continue or break, for now, continue to try other regions
                                continue 

                            # Clean up downloaded files immediately after successful processing of one region
                            data_dir = get_data_dir() # Make sure get_data_dir() is accessible or defined
                            for filename in os.listdir(data_dir):
                                filepath = os.path.join(data_dir, filename)
                                if os.path.isdir(filepath) and filename.startswith('tmp'):
                                    try:
                                        shutil.rmtree(filepath)
                                        logger.info(f"Removed temp directory: {filepath}")
                                    except Exception as e_rm_dir:
                                        logger.error(f"Failed to remove temp directory {filepath}: {e_rm_dir}")
                                elif filename.endswith('.tif') or filename.endswith('.grib2') or filename.endswith('.nc') or filename.endswith('.h5'):
                                    try:
                                        os.remove(filepath)
                                        logger.info(f"Removed data file: {filepath}")
                                    except Exception as e_rm_file:
                                        logger.error(f"Failed to remove data file {filepath}: {e_rm_file}")
                        else:
                            logger.warning(f"Failed to get complete soil data (tiff_path, bounds, or crs is None) for bbox {bbox}. Skipping this region.")

                except SentinelServerError:
                    consecutive_500_errors += 1
                    logger.warning(f"Sentinel server error ({consecutive_500_errors}/{MAX_500_ERRORS}) for bbox {bbox}. Retrying with new region if possible.")
                    if consecutive_500_errors >= MAX_500_ERRORS:
                        logger.error(f"Hit {MAX_500_ERRORS} consecutive Sentinel 500 errors, stopping region collection for this cycle.")
                        break # Stop trying to collect regions for this cycle
                    # Continue to the next attempt to select a region
                except Exception as e_region_loop:
                    logger.error(f"Unexpected error processing region for bbox {bbox}: {e_region_loop}")
                    logger.error(traceback.format_exc())
                    # Potentially add a counter for general errors too if needed
                    # Continue to try and get more regions unless a critical number of general errors occur

            random.seed() # Reset random seed
            return processed_regions
        except Exception as e:
            logger.error(f"Error in get_daily_regions: {str(e)}")
            logger.error(traceback.format_exc())
            random.seed() # Ensure random seed is reset in case of outer exception
            return [] # Return empty list on failure

    def _update_daily_count(self, target_time: datetime) -> None:
        """Update the count of regions selected for this hour."""
        today = target_time.date()
        hour = target_time.hour

        if today not in self._daily_regions:
            self._daily_regions[today] = {}
        if hour not in self._daily_regions[today]:
            self._daily_regions[today][hour] = 0

        self._daily_regions[today][hour] += 1

    def _cleanup_logged_timestamps(self):
        """Clean up old timestamps from the logging cache."""
        current_time = datetime.now(timezone.utc)
        self._logged_timestamps = {
            ts
            for ts in self._logged_timestamps
            if (current_time - ts).total_seconds()
            < 3600  # Keep last hour's worth of timestamps
        }
