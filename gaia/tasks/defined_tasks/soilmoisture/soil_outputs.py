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
