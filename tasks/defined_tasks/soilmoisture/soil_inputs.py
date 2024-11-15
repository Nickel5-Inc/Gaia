from tasks.base.components.inputs import Inputs
from datetime import datetime, timezone, date
from huggingface_hub import hf_hub_download
from tasks.defined_tasks.soilmoisture.region_selection import select_random_region
import json
from typing import Dict, Optional, List
import os

class SoilMoistureInputs(Inputs):
    """Handles region selection for soil moisture task."""
    
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
        local_path = 'tasks/defined_tasks/soilmoisture/full_h3_map.json'
        
        try:
            if os.path.exists(local_path):
                with open(local_path, 'r') as f:
                    return json.load(f)
                    
            print("Local H3 map not found, downloading from HuggingFace...")
            map_path = hf_hub_download(
                repo_id="your-hf-repo/soil-moisture-task",
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

    def get_daily_regions(self) -> List[Dict]:
        """Get all available regions for today."""
        regions = []
        today = date.today()
        count = self._daily_regions.get(today, 0)
        remaining = self.max_daily_regions - count
        
        if remaining <= 0:
            print(f"Daily region limit of {self.max_daily_regions} reached")
            return regions
            
        for _ in range(remaining):
            try:
                bbox = select_random_region(
                    self._base_cells,
                    self._urban_cells,
                    self._lakes_cells,
                    min_lat=-56,
                    max_lat=60
                )
                regions.append({
                    'bbox': bbox,
                    'datetime': datetime.now(timezone.utc)
                })
            except Exception as e:
                print(f"Error selecting region: {str(e)}")
                continue

        self._daily_regions[today] = count + len(regions)
        return regions
