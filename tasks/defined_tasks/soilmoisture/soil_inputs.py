from tasks.base.components.inputs import Inputs
from datetime import datetime, timezone
from huggingface_hub import hf_hub_download
from tasks.defined_tasks.soilmoisture.region_selection import select_random_region
import json
from typing import Dict, Optional
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
        
    def _load_h3_map(self):
        """Load H3 map data, first checking locally then from HuggingFace."""
        local_path = 'tasks/defined_tasks/soilmoisture/full_h3_map.json'
        
        try:
            # Try loading locally first
            if os.path.exists(local_path):
                with open(local_path, 'r') as f:
                    return json.load(f)
                
            # If not found locally, download from HuggingFace
            print("Local H3 map not found, downloading from HuggingFace...")
            map_path = hf_hub_download(
                repo_id="your-hf-repo/soil-moisture-task",
                filename="full_h3_map.json"
            )
            with open(map_path, 'r') as f:
                return json.load(f)
            
        except Exception as e:
            raise RuntimeError(f"Failed to load H3 map: {str(e)}")

    def get_random_region(self) -> Optional[Dict]:
        """Get random valid region for soil moisture prediction."""
        try:
            bbox = select_random_region(
                self._base_cells,
                self._urban_cells,
                self._lakes_cells,
                min_lat=-56,
                max_lat=60
            )
            
            return {
                'bbox': bbox,
                'datetime': datetime.now(timezone.utc)
            }
            
        except Exception as e:
            print(f"Error selecting region: {str(e)}")
            return None

