from typing import Dict, Any, Optional, List
from pydantic import validator, root_validator
from gaia.tasks.base.components.metadata import Metadata, CoreMetadata
from gaia.tasks.base.decorators import handle_validation_error


class WeatherMetadata(Metadata):
    """
    Metadata for the Weather Forecasting Task.
    """
    def __init__(self):
        super().__init__(
            core_metadata=CoreMetadata(
                name="WeatherTask",
                description="Weather forecasting task using the Microsoft Aurora model to predict atmospheric variables.",
                dependencies_file="requirements.txt",
                hardware_requirements_file="hardware.yml",
                author="Gaia Team",
                version="0.1.0",
            ),
            extended_metadata={
                "model_type": "Aurora_0.25",
                "prediction_horizon": "240h",
                "forecast_steps": 20,
                "scoring_delay": "72h",
            },
        )

    @handle_validation_error
    def validate_metadata(
        self, core_metadata: Dict[str, Any], extended_metadata: Dict[str, Any]
    ) -> bool:
        """
        Validate the metadata for the Weather Forecasting Task.
        
        Ensures both core and extended metadata contain required fields
        and validates the types and values of those fields.
        
        Args:
            core_metadata: Dictionary of core metadata
            extended_metadata: Dictionary of extended metadata
            
        Returns:
            bool: True if validation passes, raises an exception otherwise
        """
        if not core_metadata:
            raise ValueError("Core metadata dictionary cannot be empty")
            
        required_core_fields = [
            "name", "description", "dependencies_file", 
            "hardware_requirements_file", "author", "version"
        ]
        for field in required_core_fields:
            if field not in core_metadata:
                raise ValueError(f"Missing required core metadata field: {field}")
        
        if not extended_metadata:
            raise ValueError("Extended metadata dictionary cannot be empty")
            
        required_extended_fields = [
            "model_type", "prediction_horizon", "forecast_steps", "scoring_delay"
        ]
        for field in required_extended_fields:
            if field not in extended_metadata:
                raise ValueError(f"Missing required extended metadata field: {field}")
        
        if "forecast_steps" in extended_metadata:
            steps = extended_metadata["forecast_steps"]
            if not isinstance(steps, int) or steps <= 0:
                raise ValueError("forecast_steps must be a positive integer")
        
        return True
