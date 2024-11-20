from gaia.tasks.base.components.preprocessing import Preprocessing
from datetime import datetime, timezone, date
from huggingface_hub import hf_hub_download
from gaia.tasks.defined_tasks.soilmoisture.utils.region_selection import select_random_region
from gaia.tasks.defined_tasks.soilmoisture.utils.soil_apis import get_soil_data
import json
from typing import Dict, Optional, List
import os

class SoilValidatorPreprocessing(Preprocessing):
    """Handles region selection and data collection for soil moisture task."""
    
    def __init__(self):
        super().__init__()
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
        self.max_daily_regions = 10
        
    def _load_h3_map(self):
        """Load H3 map data, first checking locally then from HuggingFace."""
        local_path = 'Nickel5HF/gaia_h3_mapping/full_h3_map.json'
        
        try:
            if os.path.exists(local_path):
                with open(local_path, 'r') as f:
                    return json.load(f)
                    
            print("Local H3 map not found, downloading from HuggingFace...")
            map_path = hf_hub_download(
                repo_id="Nickel5HF/gaia_h3_mapping/",
                filename="full_h3_map.json"
            )
            with open(map_path, 'r') as f:
                return json.load(f)
                
        except Exception as e:
            raise RuntimeError(f"Failed to load H3 map: {str(e)}")

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
                print(f"Failed to get soil data for region {bbox}")
                return None
            
            return soil_data
            
        except Exception as e:
            print(f"Error collecting soil data: {str(e)}")
            return None

    async def store_region(self, region: Dict, target_time: datetime) -> int:
        """Store region data in database."""
        data = {
            'region_date': target_time.date(),
            'target_time': target_time,
            'bbox': json.dumps(region['bbox']),
            'combined_data': region['combined_data'],
            'sentinel_bounds': region['sentinel_bounds'],
            'sentinel_crs': region['sentinel_crs'],
            'array_shape': region['array_shape'],
            'status': 'pending'
        }
        region_id = await self.db.store_task_data('soil_moisture_regions', data)

        async with self.db.get_connection() as conn:
            await conn.execute("""
                UPDATE soil_moisture_regions 
                SET status = 'sent_to_miners'
                WHERE id = $1
            """, region_id)
        
        return region_id

    async def get_daily_regions(self, target_time: datetime) -> List[Dict]:
        """Get and store daily regions."""
        regions = []
        today = date.today()
        count = self._daily_regions.get(today, 0)
        
        while self._can_select_region():
            try:
                bbox = select_random_region(self._h3_data)
                soil_data = self.get_soil_data(bbox, target_time)
                
                if soil_data is not None:
                    region_data = {
                        'datetime': target_time,
                        'bbox': bbox,
                        **soil_data
                    }
                    region_id = await self.store_region(region_data, target_time)
                    region_data['id'] = region_id
                    regions.append(region_data)
                    
            except Exception as e:
                print(f"Error processing region: {str(e)}")
                continue

        self._daily_regions[today] = count + len(regions)
        return regions

    def _update_daily_count(self, date: date) -> None:
        """Update daily region count."""
        if date not in self._daily_regions:
            self._daily_regions[date] = 0
        self._daily_regions[date] += 1
