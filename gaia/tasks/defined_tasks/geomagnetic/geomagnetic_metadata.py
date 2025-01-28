"""Geomagnetic Task Metadata.

Defines metadata and configuration for the geomagnetic prediction task.
This includes hardware requirements, dependencies, and task-specific settings.
"""

from gaia.tasks.base.components.metadata import Metadata, CoreMetadata
from typing import Dict, Any, Optional
from gaia.tasks.base.decorators import handle_validation_error
from datetime import timedelta
from prefect.tasks import task_input_hash
from prefect import task


class GeomagneticMetadata(Metadata):
    """Metadata for geomagnetic task."""

    def __init__(self):
        """Initialize geomagnetic task metadata."""
        super().__init__(
            core_metadata=CoreMetadata(
                name="GeomagneticTask",
                description="Geomagnetic disturbance prediction using DST index data",
                dependencies_file="requirements.txt",
                hardware_requirements_file="hardware.yml",
                author="Gaia Team",
                version="0.1.0",
            ),
            extended_metadata={
                "task_type": "prediction",
                "prediction_horizon": timedelta(hours=1),
                "scoring_delay": timedelta(minutes=30),
                
                "input_data": {
                    "dst_index": {
                        "source": "kyoto_dst",
                        "resolution": "1 hour",
                        "history_window": "72 hours"
                    },
                    "solar_wind": {
                        "variables": [
                            "bz_gsm", "speed", "density", "temperature"
                        ],
                        "resolution": "1 hour",
                        "forecast_hours": [0, 1]
                    }
                },
                
                "model": {
                    "type": "geomagnetic",
                    "architecture": "prophet",
                    "input_window": 72,
                    "output_variables": ["dst_index"],
                    "uncertainty_estimation": True
                },
                
                "compute": {
                    "min_memory": "2GB",
                    "min_cpu_cores": 1,
                    "gpu_required": False,
                    "batch_size": 1
                },
                
                "scheduling": {
                    "preparation_windows": ["00:45", "01:45", "02:45", "03:45", 
                                         "04:45", "05:45", "06:45", "07:45",
                                         "08:45", "09:45", "10:45", "11:45",
                                         "12:45", "13:45", "14:45", "15:45",
                                         "16:45", "17:45", "18:45", "19:45",
                                         "20:45", "21:45", "22:45", "23:45"],
                    "execution_windows": ["00:55", "01:55", "02:55", "03:55",
                                        "04:55", "05:55", "06:55", "07:55",
                                        "08:55", "09:55", "10:55", "11:55",
                                        "12:55", "13:55", "14:55", "15:55",
                                        "16:55", "17:55", "18:55", "19:55",
                                        "20:55", "21:55", "22:55", "23:55"],
                    "max_retries": 2,
                    "retry_delay": timedelta(minutes=2)
                }
            },
        )

    @task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
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
