"""
Soil Moisture Task Protocol

Defines the data structures and formats for communication between validator and miner nodes.
This serves as the single source of truth for data formats in the soil moisture task.
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from datetime import datetime

class SoilRequestData(BaseModel):
    """Data structure for soil moisture task requests"""
    latitude: float = Field(..., description="Latitude of the location")
    longitude: float = Field(..., description="Longitude of the location")
    timestamp: datetime = Field(..., description="Timestamp for the data request")
    depth: Optional[float] = Field(None, description="Soil depth in meters")
    
    class Config:
        json_schema_extra = {
            "example": {
                "latitude": 45.523064,
                "longitude": -122.676483,
                "timestamp": "2024-01-23T00:00:00Z",
                "depth": 0.1
            }
        }

class SoilMeasurement(BaseModel):
    """Individual soil moisture measurement"""
    value: float = Field(..., description="Soil moisture value")
    uncertainty: float = Field(..., description="Measurement uncertainty")
    source: str = Field(..., description="Data source identifier")
    quality_flag: int = Field(..., description="Quality indicator (0-5)")

class SoilResponseData(BaseModel):
    """Data structure for soil moisture task responses"""
    request: SoilRequestData = Field(..., description="Original request parameters")
    measurements: List[SoilMeasurement] = Field(..., description="List of soil moisture measurements")
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata about the response"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "request": {
                    "latitude": 45.523064,
                    "longitude": -122.676483,
                    "timestamp": "2024-01-23T00:00:00Z",
                    "depth": 0.1
                },
                "measurements": [
                    {
                        "value": 0.32,
                        "uncertainty": 0.05,
                        "source": "SMAP",
                        "quality_flag": 1
                    }
                ],
                "metadata": {
                    "processing_time": "2024-01-23T00:01:23Z",
                    "data_sources": ["SMAP", "SMOS"]
                }
            }
        }

class ValidationResult(BaseModel):
    """Validation results for a soil moisture response"""
    is_valid: bool = Field(..., description="Whether the response is valid")
    score: float = Field(..., description="Quality score (0-1)")
    errors: List[str] = Field(default_factory=list, description="Validation errors if any")
    metrics: Dict[str, float] = Field(
        default_factory=dict,
        description="Validation metrics"
    ) 


##########################################
# inputs py
##########################################

from pydantic import BaseModel
from typing import Dict, List, Optional, Any
from datetime import datetime
from gaia.tasks.base.components.inputs import Inputs
from gaia.tasks.base.decorators import handle_validation_error


class SoilMoisturePayload(BaseModel):
    """Schema for soil moisture prediction payload."""

    region_id: int
    combined_data: bytes
    sentinel_bounds: list[float]  # [left, bottom, right, top]
    sentinel_crs: int  # EPSG code
    target_time: datetime


class SoilMoistureInputs(Inputs):
    """Input schema definitions for soil moisture task."""

    inputs: Dict[str, Any] = {
        "validator_input": {"regions": List[Dict[str, Any]], "target_time": datetime},
        "miner_input": SoilMoisturePayload,
    }

    @handle_validation_error
    def validate_inputs(self, inputs: Dict[str, Any]) -> bool:
        """Validate inputs based on their type."""
        if "validator_input" in inputs:
            assert isinstance(inputs["validator_input"]["regions"], list)
            assert isinstance(inputs["validator_input"]["target_time"], datetime)

        if "miner_input" in inputs:
            SoilMoisturePayload(**inputs["miner_input"])

        return True

##########################################
# outputs py
##########################################

from pydantic import BaseModel, validator, ConfigDict
from typing import Dict, List, Optional, Any, Tuple
import numpy as np
from numpy.typing import NDArray
from gaia.tasks.base.components.outputs import Outputs
from gaia.tasks.base.decorators import handle_validation_error
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class SoilMoisturePrediction(BaseModel):
    """Schema for soil moisture prediction response."""

    surface_sm: NDArray[np.float32]
    rootzone_sm: NDArray[np.float32]
    uncertainty_surface: Optional[NDArray[np.float32]] = None
    uncertainty_rootzone: Optional[NDArray[np.float32]] = None
    sentinel_bounds: list[float]  # [left, bottom, right, top]
    sentinel_crs: int  # EPSG code
    target_time: datetime

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @validator(
        "surface_sm", "rootzone_sm", "uncertainty_surface", "uncertainty_rootzone"
    )
    def validate_array(cls, v):
        if v is None:
            return v
        if not isinstance(v, np.ndarray):
            raise ValueError("Must be a numpy array")
        if v.dtype != np.float32:
            v = v.astype(np.float32)
        return v

    @validator("sentinel_bounds")
    def validate_bounds(cls, v):
        if len(v) != 4:
            raise ValueError("Bounds must have 4 values [left, bottom, right, top]")
        return v

    @classmethod
    def validate_prediction(cls, data: dict) -> bool:
        """Validate prediction shape and values."""
        try:
            if isinstance(data.get("surface_sm"), list):
                data["surface_sm"] = np.array(data["surface_sm"], dtype=np.float32)
            if isinstance(data.get("rootzone_sm"), list):
                data["rootzone_sm"] = np.array(data["rootzone_sm"], dtype=np.float32)

            # Check array shapes - must be 11x11
            surface_shape = data["surface_sm"].shape
            rootzone_shape = data["rootzone_sm"].shape
            
            logger.info(f"Validating prediction shapes - surface: {surface_shape}, rootzone: {rootzone_shape}")
            
            if surface_shape != (11, 11) or rootzone_shape != (11, 11):
                logger.warning(f"Invalid prediction shape - surface: {surface_shape}, rootzone: {rootzone_shape}")
                logger.warning(f"Expected shape: (11, 11)")
                return False

            if np.isnan(data["surface_sm"]).any() or np.isnan(data["rootzone_sm"]).any():
                logger.warning("Prediction contains NaN values")
                return False
            if np.isinf(data["surface_sm"]).any() or np.isinf(data["rootzone_sm"]).any():
                logger.warning("Prediction contains infinite values")
                return False

            return True
        except Exception as e:
            logger.warning(f"Invalid prediction data: {str(e)}")
            return False


class SoilMoistureOutputs(Outputs):
    """Output schema definitions for soil moisture task."""

    outputs: Dict[str, Any] = {"prediction": SoilMoisturePrediction}

    @handle_validation_error
    def validate_outputs(self, outputs: Dict[str, Any]) -> bool:
        """Validate outputs based on their type."""
        if not outputs:
            raise ValueError("Outputs dictionary cannot be empty")

        if "prediction" not in outputs:
            raise ValueError("Missing required 'prediction' key in outputs")

        try:
            SoilMoisturePrediction(**outputs["prediction"])
        except Exception as e:
            raise ValueError(f"Invalid prediction format: {str(e)}")

        return True