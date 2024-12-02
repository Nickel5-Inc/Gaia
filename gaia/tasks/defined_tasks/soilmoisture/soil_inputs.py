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
