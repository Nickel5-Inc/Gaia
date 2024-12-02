from gaia.tasks.base.components.metadata import Metadata, CoreMetadata
from typing import Dict, Any, Optional
from gaia.tasks.base.decorators import handle_validation_error


class SoilMoistureMetadata(Metadata):
    """Metadata for soil moisture task."""

    def __init__(self):
        super().__init__(
            core_metadata=CoreMetadata(
                name="SoilMoistureTask",
                description="Task for soil moisture prediction using satellite and weather data",
                dependencies_file="requirements.txt",
                hardware_requirements_file="hardware.yml",
                author="Gaia Team",
                version="0.1.0",
            ),
            extended_metadata={
                "model_type": "soil_moisture",
                "prediction_horizon": "6h",
                "scoring_delay": "3d",
                "input_data": ["sentinel2", "ifs_weather", "srtm_elevation"],
                "output_data": ["surface_soil_moisture", "rootzone_soil_moisture"],
            },
        )

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
            "model_type",
            "prediction_horizon",
            "scoring_delay",
            "input_data",
            "output_data",
        ]

        for field in required_extended_fields:
            if field not in extended_metadata:
                raise ValueError(f"Missing required extended metadata field: {field}")

        return True
