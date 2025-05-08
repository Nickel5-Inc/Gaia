from pydantic import BaseModel, validator, ConfigDict
from typing import Dict, List, Optional, Any, Tuple
import numpy as np
from numpy.typing import NDArray
from gaia.tasks.base.components.outputs import Outputs
from gaia.tasks.base.decorators import handle_validation_error
from datetime import datetime
from fiber.logging_utils import get_logger
from scipy.stats import sigmaclip

logger = get_logger(__name__)

ROBUST_STD_THRESHOLD = 0.005
SIGMA_CLIP_LOW = 3.0
SIGMA_CLIP_HIGH = 3.0

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
            if isinstance(v, list):
                try:
                    v = np.array(v, dtype=np.float32)
                except Exception as e:
                    raise ValueError(f"Input must be a numpy array or convertible list: {e}")
            else:
                raise ValueError("Input must be a numpy array or convertible list")

        if v.dtype != np.float32:
            logger.debug(f"Casting array from {v.dtype} to float32")
            v = v.astype(np.float32)
        return v

    @validator("sentinel_bounds")
    def validate_bounds(cls, v):
        if len(v) != 4:
            raise ValueError("Bounds must have 4 values [left, bottom, right, top]")
        return v

    @classmethod
    def validate_prediction(cls, data: dict) -> bool:
        """Validate prediction shape, values, and basic statistics."""
        try:
            if isinstance(data.get("surface_sm"), list):
                data["surface_sm"] = np.array(data["surface_sm"], dtype=np.float32)
            if isinstance(data.get("rootzone_sm"), list):
                data["rootzone_sm"] = np.array(data["rootzone_sm"], dtype=np.float32)

            surface_arr = data["surface_sm"]
            rootzone_arr = data["rootzone_sm"]

            surface_shape = surface_arr.shape
            rootzone_shape = rootzone_arr.shape

            if surface_shape != (11, 11) or rootzone_shape != (11, 11):
                logger.warning(f"Invalid prediction shape - surface: {surface_shape}, rootzone: {rootzone_shape}. Expected: (11, 11)")
                return False

            if np.isnan(surface_arr).any() or np.isnan(rootzone_arr).any():
                logger.warning("Prediction contains NaN values")
                return False
            if np.isinf(surface_arr).any() or np.isinf(rootzone_arr).any():
                logger.warning("Prediction contains infinite values")
                return False

            if np.all(surface_arr == surface_arr.flat[0]):
                logger.warning(f"Surface prediction array is constant (value: {surface_arr.flat[0]})")
                return False
            if np.all(rootzone_arr == rootzone_arr.flat[0]):
                logger.warning(f"Rootzone prediction array is constant (value: {rootzone_arr.flat[0]})")
                return False

            clipped_surface, _, _ = sigmaclip(surface_arr.flatten(), low=SIGMA_CLIP_LOW, high=SIGMA_CLIP_HIGH)
            robust_std_surface = np.std(clipped_surface) if clipped_surface.size > 0 else 0.0

            if robust_std_surface < ROBUST_STD_THRESHOLD:
                logger.warning(f"Surface prediction array failed robust standard deviation check (RobustStd: {robust_std_surface:.6f} < {ROBUST_STD_THRESHOLD})")
                return False

            clipped_rootzone, _, _ = sigmaclip(rootzone_arr.flatten(), low=SIGMA_CLIP_LOW, high=SIGMA_CLIP_HIGH)
            robust_std_rootzone = np.std(clipped_rootzone) if clipped_rootzone.size > 0 else 0.0

            if robust_std_rootzone < ROBUST_STD_THRESHOLD:
                logger.warning(f"Rootzone prediction array failed robust standard deviation check (RobustStd: {robust_std_rootzone:.6f} < {ROBUST_STD_THRESHOLD})")
                return False

            logger.debug(f"Prediction validation passed. Surface RobustStd: {robust_std_surface:.4f}, Rootzone RobustStd: {robust_std_rootzone:.4f}")
            return True

        except Exception as e:
            logger.warning(f"Error during prediction validation: {str(e)}")
            logger.debug(traceback.format_exc())
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
            prediction_data = outputs["prediction"]
            if not SoilMoisturePrediction.validate_prediction(prediction_data):
                raise ValueError("Prediction failed validation checks (constant, low robust stddev, NaN/Inf, or shape). See logs.")

        except Exception as e:
            raise ValueError(f"Invalid prediction format or failed validation: {str(e)}")

        return True
