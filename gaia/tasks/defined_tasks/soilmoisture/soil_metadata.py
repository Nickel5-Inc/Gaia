"""Soil Moisture Task Metadata.

Defines metadata and configuration for the soil moisture prediction task.
This includes hardware requirements, dependencies, and task-specific settings.
"""

from gaia.tasks.base.components.metadata import Metadata, CoreMetadata
from typing import Dict, Any, Optional, ClassVar
from gaia.tasks.base.decorators import handle_validation_error
from datetime import timedelta
from prefect.tasks import task_input_hash
from prefect import task

class SoilMoistureMetadata(Metadata):
    """Metadata for soil moisture task."""
    
    get_task_config: ClassVar[Any]

    def __init__(self):
        """Initialize soil moisture task metadata."""
        super().__init__(
            core_metadata=CoreMetadata(
                name="SoilMoistureTask",
                description="Soil moisture prediction using satellite and weather data",
                dependencies_file="requirements.txt",
                hardware_requirements_file="hardware.yml",
                author="Gaia Team",
                version="0.1.0",
            ),
            extended_metadata={
                "task_type": "prediction",
                "prediction_horizon": timedelta(hours=6),
                "scoring_delay": timedelta(days=3),
                
                "input_data": {
                    "sentinel2": {
                        "bands": ["B4", "B8"],
                        "resolution": "10m",
                        "revisit_time": "5 days"
                    },
                    "ifs_weather": {
                        "variables": [
                            "t2m", "tp", "ssrd", "sot", "sp", 
                            "d2m", "u10", "v10", "ro", "msl"
                        ],
                        "resolution": "0.25 degrees",
                        "forecast_hours": [0, 6, 12, 18, 24]
                    },
                    "srtm_elevation": {
                        "resolution": "30m",
                        "coverage": "global"
                    }
                },
                
                "model": {
                    "type": "soil_moisture",
                    "architecture": "custom_unet",
                    "input_size": [220, 220],
                    "output_variables": ["surface_soil_moisture", "rootzone_soil_moisture"],
                    "uncertainty_estimation": False
                },
                
                "compute": {
                    "min_memory": "4GB",
                    "min_cpu_cores": 2,
                    "gpu_required": False,
                    "gpu_memory": "4GB",
                    "batch_size": 1
                },
                
                "scheduling": {
                    "preparation_windows": ["01:30", "07:30", "13:30", "19:30"],
                    "execution_windows": ["02:00", "08:00", "14:00", "20:00"],
                    "max_retries": 3,
                    "retry_delay": timedelta(minutes=5)
                }
            },
        )

    @task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=24))
    def get_task_config(self) -> Dict[str, Any]:
        """Get task configuration from metadata."""
        return {
            "prediction_horizon": self.extended_metadata["prediction_horizon"],
            "scoring_delay": self.extended_metadata["scoring_delay"],
            "compute": self.extended_metadata["compute"],
            "scheduling": self.extended_metadata["scheduling"]
        }

    @handle_validation_error
    def validate_metadata(
        self, core_metadata: Dict[str, Any], extended_metadata: Dict[str, Any]
    ) -> bool:
        """Validate metadata based on their type."""
        if not core_metadata:
            raise ValueError("Core metadata dictionary cannot be empty")

        if not extended_metadata:
            raise ValueError("Extended metadata dictionary cannot be empty")

        required_extended_fields = [
            "task_type",
            "prediction_horizon",
            "scoring_delay",
            "input_data",
            "model",
            "compute",
            "scheduling"
        ]

        for field in required_extended_fields:
            if field not in extended_metadata:
                raise ValueError(f"Missing required extended metadata field: {field}")

        if not isinstance(extended_metadata["prediction_horizon"], timedelta):
            raise ValueError("prediction_horizon must be a timedelta")
        
        if not isinstance(extended_metadata["scoring_delay"], timedelta):
            raise ValueError("scoring_delay must be a timedelta")

        if not isinstance(extended_metadata["input_data"], dict):
            raise ValueError("input_data must be a dictionary")

        if not isinstance(extended_metadata["compute"]["min_memory"], str):
            raise ValueError("compute.min_memory must be a string")

        return True

    def get_resource_requirements(self) -> Dict[str, Any]:
        """Get compute resource requirements."""
        return self.extended_metadata["compute"]

    def get_scheduling_windows(self) -> Dict[str, list]:
        """Get task scheduling windows."""
        return self.extended_metadata["scheduling"]

    def get_model_config(self) -> Dict[str, Any]:
        """Get model configuration."""
        return self.extended_metadata["model"]
