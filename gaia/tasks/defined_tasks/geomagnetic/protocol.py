"""Geomagnetic Protocol Models.

This module defines the protocol models for geomagnetic task validation.
It includes models for:
1. Input/Output validation using base components
2. Request/Response data validation
3. Measurement and prediction validation
"""

from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, validator, ConfigDict
from datetime import datetime
import numpy as np
from numpy.typing import NDArray
from gaia.tasks.base.components.inputs import Inputs
from gaia.tasks.base.components.outputs import Outputs
from gaia.tasks.base.decorators import handle_validation_error
import logging

logger = logging.getLogger(__name__)

##########################################
# Input/Output Base Extensions
##########################################

class GeomagneticInputs(Inputs):
    """Input schema definitions for geomagnetic task."""
    
    inputs: Dict[str, Any] = Field(
        default_factory=lambda: {
            "validator_input": {
                "regions": List[Dict[str, Any]], 
                "target_time": datetime
            },
            "miner_input": Dict[str, Any]  # Validated by GeomagneticPayload
        },
        description="Input schema for geomagnetic task"
    )

    @handle_validation_error
    def validate_inputs(self, inputs: Dict[str, Any]) -> bool:
        """Validate inputs based on their type."""
        try:
            if "validator_input" in inputs:
                validator_input = inputs["validator_input"]
                if not isinstance(validator_input, dict):
                    raise ValueError("Validator input must be a dictionary")
                if "regions" not in validator_input or "target_time" not in validator_input:
                    raise ValueError("Validator input missing required fields: regions, target_time")
                if not isinstance(validator_input["regions"], list):
                    raise ValueError("Regions must be a list")
                if not isinstance(validator_input["target_time"], datetime):
                    raise ValueError("Target time must be a datetime object")

            if "miner_input" in inputs:
                if not isinstance(inputs["miner_input"], dict):
                    raise ValueError("Miner input must be a dictionary")
                GeomagneticPayload(**inputs["miner_input"])

            return True
        except Exception as e:
            logger.error(f"Input validation failed: {str(e)}")
            raise

class GeomagneticOutputs(Outputs):
    """Output schema definitions for geomagnetic task."""
    
    outputs: Dict[str, Any] = Field(
        default_factory=dict,
        description="Dictionary to store geomagnetic task outputs"
    )

    def format_results(self, results: Union[List[float], NDArray]) -> NDArray:
        """Format results as numpy array."""
        if isinstance(results, list):
            return np.array(results, dtype=np.float32)
        elif isinstance(results, np.ndarray):
            return results.astype(np.float32)
        else:
            raise ValueError("Results must be a list or numpy array")

    @handle_validation_error
    def validate_outputs(self, outputs: Dict[str, Any]) -> bool:
        """Validate the outputs generated by the geomagnetic task."""
        try:
            if not isinstance(outputs, dict):
                raise ValueError("Outputs must be a dictionary")

            required_keys = ["predictions", "metadata"]
            if not all(key in outputs for key in required_keys):
                raise ValueError(f"Outputs missing required keys: {required_keys}")

            predictions = outputs.get("predictions")
            if not GeomagneticPrediction.validate_prediction(predictions):
                raise ValueError("Invalid prediction format")

            metadata = outputs.get("metadata")
            if not isinstance(metadata, dict):
                raise ValueError("Metadata must be a dictionary")

            return True
        except Exception as e:
            logger.error(f"Output validation failed: {str(e)}")
            raise

##########################################
# Protocol Models
##########################################

class GeomagneticPayload(BaseModel):
    """Input payload for geomagnetic predictions."""
    
    nonce: str = Field(..., description="Unique identifier for the request")
    data: Dict[str, Any] = Field(..., description="Request data containing timestamp and values")

    @validator("data")
    def validate_data(cls, v):
        """Validate the data structure."""
        try:
            required_fields = ["timestamp", "value", "historical_values"]
            if not all(field in v for field in required_fields):
                raise ValueError(f"Missing required fields: {required_fields}")
            
            historical = v.get("historical_values", [])
            if not isinstance(historical, list):
                raise ValueError("Historical values must be a list")
            
            try:
                datetime.fromisoformat(v["timestamp"])
            except (ValueError, TypeError):
                raise ValueError("Invalid timestamp format")
                
            try:
                value = float(v["value"])
                if not -500 <= value <= 100:  # DST typically ranges from -500 to 100
                    raise ValueError("Current value must be between -500 and 100")
            except (ValueError, TypeError):
                raise ValueError("Invalid value format")
                
            return v
        except Exception as e:
            logger.error(f"Payload data validation failed: {str(e)}")
            raise

class GeomagneticPrediction(BaseModel):
    """Model for geomagnetic predictions."""
    
    predicted_values: List[float] = Field(..., description="Predicted DST values")
    uncertainty: Optional[List[float]] = Field(None, description="Prediction uncertainties")
    timestamp: str = Field(..., description="Target time for predictions")
    miner_hotkey: str = Field(..., description="Hotkey of the predicting miner")

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @validator("predicted_values")
    def validate_predictions(cls, v):
        """Validate prediction values."""
        if not v:
            raise ValueError("Predictions cannot be empty")
        if not all(-500 <= x <= 100 for x in v):
            raise ValueError("All predictions must be between -500 and 100")
        return v

    @validator("uncertainty")
    def validate_uncertainty(cls, v):
        """Validate uncertainty values if present."""
        if v is not None:
            if not hasattr(cls, 'predicted_values'):
                raise ValueError("Cannot validate uncertainty without predictions")
            if len(v) != len(cls.predicted_values):
                raise ValueError("Uncertainty must match prediction length")
            if not all(x >= 0 for x in v):
                raise ValueError("Uncertainty values must be non-negative")
        return v

    @validator("timestamp")
    def validate_timestamp(cls, v):
        """Validate timestamp format."""
        try:
            datetime.fromisoformat(v)
            return v
        except (ValueError, TypeError):
            raise ValueError("Invalid timestamp format")

    @classmethod
    def validate_prediction(cls, data: Dict[str, Any]) -> bool:
        """Validate prediction data."""
        try:
            GeomagneticPrediction(**data)
            return True
        except Exception as e:
            logger.error(f"Prediction validation failed: {str(e)}")
            return False

class GeomagneticMeasurement(BaseModel):
    """Model for ground truth measurements."""
    
    timestamp: str = Field(..., description="UTC timestamp of measurement")
    dst_value: float = Field(..., description="Measured DST value")
    source: Optional[str] = Field(None, description="Data source identifier")
    
    @validator("dst_value")
    def validate_dst(cls, v):
        """Validate DST value is within reasonable bounds."""
        if not -500 <= v <= 500:
            raise ValueError("DST value must be between -500 and 500 un-normalized")
        return v

    @validator("timestamp")
    def validate_timestamp(cls, v):
        """Validate timestamp format."""
        try:
            datetime.fromisoformat(v)
            return v
        except (ValueError, TypeError):
            raise ValueError("Invalid timestamp format")


