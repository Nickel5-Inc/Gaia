from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
import numpy as np
import xarray as xr
from pydantic import BaseModel, Field, validator
from gaia.tasks.base.components.inputs import Inputs
from gaia.tasks.base.decorators import handle_validation_error

SURFACE_VARIABLES = ["2t", "10u", "10v", "msl"]
STATIC_VARIABLES = ["lsm", "slt", "z"]
ATMOSPHERIC_VARIABLES = ["t", "u", "v", "q", "z"]
PRESSURE_LEVELS = [50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000]

class WeatherInputPayload(BaseModel):
    """
    Schema for weather forecast input payload sent to miners.
    Contains GFS data in the format required for Aurora model prediction.
    """
    compressed_data: bytes = Field(..., description="Compressed binary data containing GFS inputs")
    reference_time: datetime = Field(..., description="Reference time of forecast")
    forecast_id: str = Field(..., description="Unique identifier for this forecast task")
    grid_dimensions: Dict[str, int] = Field(..., description="Grid dimensions (lat, lon)")
    
    class Config:
        arbitrary_types_allowed = True

class WeatherRequestPayload(BaseModel):
    """
    Schema for requesting forecasts from miners.
    Used to request specific subsets of the forecast.
    """
    forecast_id: str = Field(..., description="Forecast ID to request")
    miner_id: Optional[str] = Field(None, description="Specific miner to request from (if None, request from all)")
    variables: Optional[List[str]] = Field(None, description="Specific variables to request (if None, request all)")
    lead_times: Optional[List[int]] = Field(None, description="Specific lead times to request in hours (if None, request all)")
    pressure_levels: Optional[List[int]] = Field(None, description="Specific pressure levels to request (if None, request all)")
    region: Optional[Dict[str, float]] = Field(None, description="Region bounds to request (if None, request global)")

class WeatherInputs(Inputs):
    """
    Input schema definitions for weather forecasting task.
    Defines both validator and miner input formats.
    """
    
    inputs: Dict[str, Any] = {
        "validator_input": {
            "forecast_config": {
                "reference_time": datetime,
                "forecast_horizon": int,  # In hours
                "forecast_id": str,
                "region": Optional[Dict[str, Any]]
            }
        },
        "miner_input": WeatherInputPayload,
        "forecast_request": WeatherRequestPayload
    }
    
    @handle_validation_error
    def validate_inputs(self, inputs: Dict[str, Any]) -> bool:
        """
        Validate inputs based on their type.
        
        Args:
            inputs: Dictionary containing input data
            
        Returns:
            bool: True if validation passes, raises exception otherwise
        """
        if "validator_input" in inputs:
            config = inputs["validator_input"].get("forecast_config", {})
            assert isinstance(config.get("reference_time"), datetime), "reference_time must be a datetime"
            assert isinstance(config.get("forecast_horizon"), int), "forecast_horizon must be an integer"
            assert isinstance(config.get("forecast_id"), str), "forecast_id must be a string"

        if "miner_input" in inputs:
            WeatherInputPayload(**inputs["miner_input"])
        if "forecast_request" in inputs:
            WeatherRequestPayload(**inputs["forecast_request"])
            
        return True
        
    def prepare_miner_input(self, gfs_data: xr.Dataset, forecast_id: str) -> WeatherInputPayload:
        """
        Convert GFS data to the format required by miners.
        
        Args:
            gfs_data: xarray Dataset containing GFS data
            forecast_id: Unique identifier for this forecast task
            
        Returns:
            WeatherInputPayload containing compressed data and metadata
        """
        compressed_data = self._compress_dataset(gfs_data)
        
        return WeatherInputPayload(
            compressed_data=compressed_data,
            reference_time=gfs_data.time.values[0],
            forecast_id=forecast_id,
            grid_dimensions={
                "lat": len(gfs_data.latitude),
                "lon": len(gfs_data.longitude)
            }
        )
    
    def _compress_dataset(self, dataset: xr.Dataset) -> bytes:
        """
        Compress xarray dataset to binary format.
        
        Args:
            dataset: xarray Dataset to compress
            
        Returns:
            bytes: Compressed binary data
        """
        # This is a placeholder - actual implementation will use NetCDF4 with compression
        # or a custom binary format optimized for weather data
        bio = xr.backends.to_netcdf(dataset, compute=True)
        return bio.read()
    