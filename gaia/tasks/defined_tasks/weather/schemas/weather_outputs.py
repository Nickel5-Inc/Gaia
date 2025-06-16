from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any, Union, Set, Literal
import numpy as np
import xarray as xr
from pydantic import BaseModel, Field, validator, ConfigDict, root_validator, model_validator
from gaia.tasks.base.components.outputs import Outputs
from gaia.tasks.base.decorators import handle_validation_error
from fiber.logging_utils import get_logger
from enum import Enum

logger = get_logger(__name__)

"""
Weather forecast output handling for the Aurora-based weather task.

This module defines the output structures for weather forecasts.
"""

SURFACE_VARIABLES = ["2t", "10u", "10v", "msl"]
ATMOSPHERIC_VARIABLES = ["t", "u", "v", "q", "z"]
PRESSURE_LEVELS = [50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000]
GRID_DIMENSIONS = (721, 1440) # 721x1440 grid (lat x lon)
LAT_RANGE = (90.0, -90.0)  # Latitudes must be decreasing from 90 to -90
LON_RANGE = (0.0, 360.0)  # Longitudes must be 0 to 360 (not including 360)

class WeatherTaskStatus(str, Enum):
    """Standardized status values for Weather Task responses"""
    # Initiate Fetch Response Statuses
    FETCH_ACCEPTED = "fetch_accepted"
    FETCH_REJECTED = "fetch_rejected"
    
    # Input Status Response Statuses  
    FETCH_QUEUED = "fetch_queued"
    FETCHING_GFS = "fetching_gfs"
    HASHING_INPUT = "hashing_input"
    INPUT_HASHED_AWAITING_VALIDATION = "input_hashed_awaiting_validation"
    FETCH_ERROR = "fetch_error"
    
    # Inference Response Statuses
    INFERENCE_STARTED = "inference_started"
    INFERENCE_FAILED = "inference_failed"
    
    # Kerchunk/Data Response Statuses
    COMPLETED = "completed"
    PROCESSING = "processing"
    NOT_FOUND = "not_found"
    ERROR = "error"
    
    # General Error Statuses
    PARSE_ERROR = "parse_error"
    VALIDATOR_POLL_ERROR = "validator_poll_error" 
    VALIDATOR_POLL_FAILED = "validator_poll_failed"

class Variable(BaseModel):
    """Base class for variable validation."""
    name: str
    values: np.ndarray
    
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    @validator('values')
    def validate_array(cls, v, values):
        """Validate that the array is properly formed."""
        if not isinstance(v, np.ndarray):
            raise ValueError("Values must be a numpy array")
        
        if np.isnan(v).any():
            raise ValueError(f"Variable {values.get('name', 'unknown')} contains NaN values")
        if np.isinf(v).any():
            raise ValueError(f"Variable {values.get('name', 'unknown')} contains Inf values")
            
        return v

class SurfaceVariable(Variable):
    """Surface variable with validation."""
    
    @validator('name')
    def validate_surface_name(cls, v):
        """Validate surface variable name against requirements."""
        if v not in SURFACE_VARIABLES:
            raise ValueError(f"Invalid surface variable name: {v}. Must be one of {SURFACE_VARIABLES}")
        return v
        
    @validator('values')
    def validate_surface_dims(cls, v):
        """Validate surface variable dimensions."""
        if len(v.shape) != 4:
            raise ValueError(f"Surface variable must have 4 dimensions (batch, time, lat, lon)")
        if v.shape[1] != 1:
            raise ValueError(f"Time dimension must be 1 for output (got {v.shape[1]})")
        if v.shape[2:] != GRID_DIMENSIONS:
            raise ValueError(f"Spatial dimensions must be {GRID_DIMENSIONS} (got {v.shape[2:]})")
        return v

class AtmosphericVariable(Variable):
    """Atmospheric variable with validation."""
    
    @validator('name')
    def validate_atmos_name(cls, v):
        """Validate atmospheric variable name against Aurora requirements."""
        if v not in ATMOSPHERIC_VARIABLES:
            raise ValueError(f"Invalid atmospheric variable name: {v}. Must be one of {ATMOSPHERIC_VARIABLES}")
        return v
        
    @validator('values')
    def validate_atmos_dims(cls, v):
        """Validate atmospheric variable dimensions."""
        if len(v.shape) != 5:
            raise ValueError(f"Atmospheric variable must have 5 dimensions (batch, time, pressure, lat, lon)")
        if v.shape[1] != 1:
            raise ValueError(f"Time dimension must be 1 for output (got {v.shape[1]})")
        if v.shape[2] != len(PRESSURE_LEVELS):
            raise ValueError(f"Pressure dimension must match pressure levels (got {v.shape[2]}, expected {len(PRESSURE_LEVELS)})")
        if v.shape[3:] != GRID_DIMENSIONS:
            raise ValueError(f"Spatial dimensions must be {GRID_DIMENSIONS} (got {v.shape[3:]})")
        return v

class ForecastMetadata(BaseModel):
    """Forecast metadata with validation."""
    
    forecast_id: str = Field(..., description="Unique identifier for the forecast task")
    miner_id: str = Field(..., description="ID of the miner that produced this forecast")
    model_version: str = Field(..., description="Version identifier of the model used")
    creation_time: datetime = Field(..., description="When the forecast was created")
    reference_time: datetime = Field(..., description="Base time of the forecast")
    lead_time: int = Field(..., description="Lead time in hours")
    latitude: np.ndarray = Field(..., description="Latitude values (must be decreasing from 90 to -90)")
    longitude: np.ndarray = Field(..., description="Longitude values (must be 0 to 360, not including 360)")
    pressure_levels: Optional[List[int]] = Field(None, description="Pressure levels in hPa")
    
    model_config = ConfigDict(
        protected_namespaces=(),
        arbitrary_types_allowed=True
    )
    
    @validator('lead_time')
    def validate_lead_time(cls, v):
        """Validate lead time."""
        if v < 0:
            raise ValueError(f"Lead time must be non-negative (got {v})")
        return v
    
    @validator('pressure_levels')
    def validate_pressure_levels(cls, v):
        """Validate pressure levels match requirements."""
        if v is not None:
            for level in v:
                if level not in PRESSURE_LEVELS:
                    raise ValueError(f"Invalid pressure level: {level}. Must be one of {PRESSURE_LEVELS}")
        return v
        
    @validator('latitude')
    def validate_latitude(cls, v):
        """Validate latitude values match requirements."""
        if not isinstance(v, np.ndarray):
            raise ValueError("Latitude must be a numpy array")
            
        if len(v) != GRID_DIMENSIONS[0]:
            raise ValueError(f"Latitude dimension must be {GRID_DIMENSIONS[0]} (got {len(v)})")
            
        if v[0] != LAT_RANGE[0] or v[-1] != LAT_RANGE[1]:
            raise ValueError(f"Latitude range must be {LAT_RANGE} (got {v[0]} to {v[-1]})")
            
        if not np.all(np.diff(v) < 0):
            raise ValueError("Latitude values must be monotonically decreasing")
            
        return v
        
    @validator('longitude')
    def validate_longitude(cls, v):
        """Validate longitude values match requirements."""
        if not isinstance(v, np.ndarray):
            raise ValueError("Longitude must be a numpy array")
            
        if len(v) != GRID_DIMENSIONS[1]:
            raise ValueError(f"Longitude dimension must be {GRID_DIMENSIONS[1]} (got {len(v)})")
            
        if v[0] != LON_RANGE[0] or v[-1] >= LON_RANGE[1]:
            raise ValueError(f"Longitude range must be {LON_RANGE[0]} to <{LON_RANGE[1]} (got {v[0]} to {v[-1]})")
            
        if not np.all(np.diff(v) > 0):
            raise ValueError("Longitude values must be monotonically increasing")
            
        return v

class WeatherForecast(BaseModel):
    """Weather forecast model with variable selection validation."""
    
    metadata: ForecastMetadata = Field(..., description="Forecast metadata")
    surface_variables: Dict[str, SurfaceVariable] = Field({}, description="Surface variables")
    atmospheric_variables: Dict[str, AtmosphericVariable] = Field({}, description="Atmospheric variables")
    
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    @model_validator(mode='after')
    def validate_variables_present(self) -> 'WeatherForecast':
        """Ensure at least one surface or atmospheric variable is present."""
        if not self.surface_variables and not self.atmospheric_variables:
            raise ValueError("Forecast must contain at least one surface or atmospheric variable.")
        return self
    
    @classmethod
    def from_aurora_batch(cls, batch, forecast_id: str, miner_id: str, model_version: str) -> "WeatherForecast":
        """
        Create a WeatherForecast from an Aurora Batch object.
        
        Args:
            batch: Aurora Batch object
            forecast_id: Unique identifier for this forecast
            miner_id: ID of the miner that produced this forecast
            model_version: Version of the model used
            
        Returns:
            WeatherForecast instance
        """
        metadata = ForecastMetadata(
            forecast_id=forecast_id,
            miner_id=miner_id,
            model_version=model_version,
            creation_time=datetime.now(),
            reference_time=batch.metadata.time[0],
            lead_time=0,  # Default, will be set based on prediction step
            latitude=batch.metadata.lat.numpy(),
            longitude=batch.metadata.lon.numpy(),
            pressure_levels=list(batch.metadata.atmos_levels) if hasattr(batch.metadata, 'atmos_levels') else None
        )
        
        surface_variables = {}
        if hasattr(batch, 'surf_vars'):
            for name, values in batch.surf_vars.items():
                if name in SURFACE_VARIABLES:
                    surface_variables[name] = SurfaceVariable(
                        name=name,
                        values=values.numpy()
                    )
        
        atmospheric_variables = {}
        if hasattr(batch, 'atmos_vars'):
            for name, values in batch.atmos_vars.items():
                if name in ATMOSPHERIC_VARIABLES:
                    atmospheric_variables[name] = AtmosphericVariable(
                        name=name,
                        values=values.numpy()
                    )
        
        return cls(
            metadata=metadata,
            surface_variables=surface_variables,
            atmospheric_variables=atmospheric_variables
        )
    
    @classmethod
    def from_xarray(cls, ds: xr.Dataset, forecast_id: str, miner_id: str, model_version: str) -> "WeatherForecast":
        """
        Create a WeatherForecast from an xarray Dataset.
        
        Args:
            ds: xarray Dataset containing forecast data
            forecast_id: Unique identifier for this forecast
            miner_id: ID of the miner that produced this forecast
            model_version: Version of the model used
            
        Returns:
            WeatherForecast instance
        """
        lead_time = int(ds.attrs.get("lead_time", 0))
        
        if "time" in ds.coords:
            reference_time = ds.time.values[0]
        else:
            reference_time = datetime.now()
            
        if "latitude" in ds.coords:
            latitude = ds.latitude.values
        elif "lat" in ds.coords:
            latitude = ds.lat.values
        else:
            raise ValueError("Dataset must contain latitude coordinates")
            
        if "longitude" in ds.coords:
            longitude = ds.longitude.values
        elif "lon" in ds.coords:
            longitude = ds.lon.values
        else:
            raise ValueError("Dataset must contain longitude coordinates")
            
        pressure_levels = None
        if "level" in ds.coords:
            pressure_levels = list(ds.level.values)
        
        metadata = ForecastMetadata(
            forecast_id=forecast_id,
            miner_id=miner_id,
            model_version=model_version,
            creation_time=datetime.now(),
            reference_time=reference_time,
            lead_time=lead_time,
            latitude=latitude,
            longitude=longitude,
            pressure_levels=pressure_levels
        )
        
        surface_variables = {}
        for var_name in SURFACE_VARIABLES:
            if var_name in ds:
                var_data = ds[var_name].values
                
                if len(var_data.shape) == 2:  # (lat, lon)
                    var_data = var_data[np.newaxis, np.newaxis, :, :]
                elif len(var_data.shape) == 3:  # (time, lat, lon)
                    var_data = var_data[np.newaxis, :, :, :]
                
                if var_data.shape[1] > 1:
                    var_data = var_data[:, 0:1, :, :]
                
                surface_variables[var_name] = SurfaceVariable(
                    name=var_name,
                    values=var_data
                )
        
        atmospheric_variables = {}
        for var_name in ATMOSPHERIC_VARIABLES:
            if var_name in ds and pressure_levels:
                var_data = ds[var_name].values
                
                if len(var_data.shape) == 3:  # (level, lat, lon)
                    var_data = var_data[np.newaxis, np.newaxis, :, :, :]
                elif len(var_data.shape) == 4:  # (time, level, lat, lon)
                    var_data = var_data[np.newaxis, :, :, :, :]
                
                if var_data.shape[1] > 1:
                    var_data = var_data[:, 0:1, :, :, :]
                
                atmospheric_variables[var_name] = AtmosphericVariable(
                    name=var_name,
                    values=var_data
                )
        
        return cls(
            metadata=metadata,
            surface_variables=surface_variables,
            atmospheric_variables=atmospheric_variables
        )
    
    def to_xarray(self) -> xr.Dataset:
        """
        Convert the forecast to an xarray Dataset.
        
        Returns:
            xr.Dataset containing the forecast data
        """
        coords = {
            "time": [self.metadata.reference_time],
            "latitude": self.metadata.latitude,
            "longitude": self.metadata.longitude
        }
        
        if self.metadata.pressure_levels:
            coords["level"] = self.metadata.pressure_levels
        
        data_vars = {}
        
        for var_name, var in self.surface_variables.items():
            values = var.values[0, 0, :, :]
            data_vars[var_name] = (["latitude", "longitude"], values)
        
        for var_name, var in self.atmospheric_variables.items():
            values = var.values[0, 0, :, :, :]
            data_vars[var_name] = (["level", "latitude", "longitude"], values)
        
        ds = xr.Dataset(
            data_vars=data_vars,
            coords=coords,
            attrs={
                "forecast_id": self.metadata.forecast_id,
                "miner_id": self.metadata.miner_id,
                "model_version": self.metadata.model_version,
                "creation_time": str(self.metadata.creation_time),
                "reference_time": str(self.metadata.reference_time),
                "lead_time": self.metadata.lead_time
            }
        )
        
        return ds
    
    def select_variables(self, surface_vars: Optional[List[str]] = None,
                        atmos_vars: Optional[List[str]] = None) -> "WeatherForecast":
        """
        Create a new forecast with only the selected variables.
        
        Args:
            surface_vars: List of surface variables to include (if None, include all)
            atmos_vars: List of atmospheric variables to include (if None, include all)
            
        Returns:
            New WeatherForecast instance with only the selected variables
        """
        new_surface_vars = {}
        if surface_vars is not None:
            for var_name in surface_vars:
                if var_name in self.surface_variables:
                    new_surface_vars[var_name] = self.surface_variables[var_name]
        else:
            new_surface_vars = self.surface_variables
        
        new_atmos_vars = {}
        if atmos_vars is not None:
            for var_name in atmos_vars:
                if var_name in self.atmospheric_variables:
                    new_atmos_vars[var_name] = self.atmospheric_variables[var_name]
        else:
            new_atmos_vars = self.atmospheric_variables
        
        return WeatherForecast(
            metadata=self.metadata,
            surface_variables=new_surface_vars,
            atmospheric_variables=new_atmos_vars
        )
    
    def validate_variable_requirements(self, required_surface_vars: Optional[List[str]] = None,
                                      required_atmos_vars: Optional[List[str]] = None) -> bool:
        """
        Validate that the forecast contains all required variables.
        
        Args:
            required_surface_vars: List of required surface variables
            required_atmos_vars: List of required atmospheric variables
            
        Returns:
            True if all required variables are present, raises ValueError otherwise
        """
        if required_surface_vars:
            missing_surf = [var for var in required_surface_vars if var not in self.surface_variables]
            if missing_surf:
                raise ValueError(f"Missing required surface variables: {missing_surf}")
        
        if required_atmos_vars:
            missing_atmos = [var for var in required_atmos_vars if var not in self.atmospheric_variables]
            if missing_atmos:
                raise ValueError(f"Missing required atmospheric variables: {missing_atmos}")
        
        return True

class WeatherOutputs(Outputs):
    """Output schema definitions for weather forecasting task with flexible variable selection."""
    
    outputs: Dict[str, Any] = {
        "forecast": WeatherForecast,
        "error": Optional[str]
    }
    
    @handle_validation_error
    def validate_outputs(self, outputs: Dict[str, Any]) -> bool:
        """
        Validate outputs based on their type.
        
        Args:
            outputs: Dictionary containing output data
            
        Returns:
            bool: True if validation passes, raises exception otherwise
        """
        if not outputs:
            raise ValueError("Outputs dictionary cannot be empty")
        
        if "error" in outputs and outputs["error"]:
            return True
            
        if "forecast" not in outputs:
            raise ValueError("Missing required 'forecast' key in outputs")
            
        try:
            WeatherForecast(**outputs["forecast"])
        except Exception as e:
            logger.error(f"Invalid forecast format: {str(e)}")
            raise ValueError(f"Invalid forecast format: {str(e)}")
            
        return True
    
    def validate_forecast_data(self, forecast: WeatherForecast, 
                              required_surface_vars: Optional[List[str]] = None,
                              required_atmos_vars: Optional[List[str]] = None) -> bool:
        """
        Validate forecast data meets specific variable requirements.
        
        Args:
            forecast: WeatherForecast to validate
            required_surface_vars: List of required surface variables
            required_atmos_vars: List of required atmospheric variables
            
        Returns:
            bool: True if validation passes, raises exception otherwise
        """
        if not isinstance(forecast, WeatherForecast):
            if isinstance(forecast, dict):
                try:
                    forecast = WeatherForecast(**forecast)
                except Exception as e:
                    logger.error(f"Invalid forecast format: {str(e)}")
                    raise ValueError(f"Invalid forecast format: {str(e)}")
            else:
                raise ValueError("Forecast must be a WeatherForecast instance or dictionary")
        
        forecast.validate_variable_requirements(
            required_surface_vars=required_surface_vars,
            required_atmos_vars=required_atmos_vars
        )
        
        return True
    
    def compress_forecast(self, forecast: WeatherForecast) -> bytes:
        """
        Compress a forecast to binary format for network transfer.
        
        Args:
            forecast: WeatherForecast to compress
            
        Returns:
            bytes: Compressed binary data
        """
        ds = forecast.to_xarray()
        
        encoded = xr.backends.to_netcdf(ds, compute=True, encoding={
            var: {'zlib': True, 'complevel': 6} for var in ds.data_vars
        })
        
        return encoded.read()
    
    def decompress_forecast(self, data: bytes, validate: bool = True,
                           required_surface_vars: Optional[List[str]] = None,
                           required_atmos_vars: Optional[List[str]] = None) -> WeatherForecast:
        """
        Decompress binary data to a WeatherForecast.
        
        Args:
            data: Compressed binary data
            validate: Whether to validate the decompressed forecast
            required_surface_vars: List of required surface variables
            required_atmos_vars: List of required atmospheric variables
            
        Returns:
            WeatherForecast: Decompressed forecast
        """
        ds = xr.open_dataset(data)

        forecast_id = ds.attrs.get("forecast_id", "unknown")
        miner_id = ds.attrs.get("miner_id", "unknown")
        model_version = ds.attrs.get("model_version", "unknown")
        forecast = WeatherForecast.from_xarray(ds, forecast_id, miner_id, model_version)
        
        if validate:
            self.validate_forecast_data(
                forecast, 
                required_surface_vars=required_surface_vars,
                required_atmos_vars=required_atmos_vars
            )
            
        return forecast

class WeatherKerchunkResponseData(BaseModel):
    """ Defines the structure for the miner's response to a forecast data request."""
    status: Literal['completed', 'processing', 'error', 'not_found'] = Field(..., description="The status of the requested forecast job.")
    message: Optional[str] = Field(None, description="Optional message, e.g., error details or status update.")
    zarr_store_url: Optional[str] = Field(None, description="URL from which the validator can access the Zarr store. Required if status is 'completed'.")
    verification_hash: Optional[str] = Field(None, description="The SHA256 verification hash claimed by the miner. Required if status is 'completed'.")
    access_token: Optional[str] = Field(None, description="Short-lived JWT token required to access the forecast data via the /forecasts/ endpoint. Required if status is 'completed'.")

class WeatherInitiateFetchResponse(BaseModel):
    """ Response model for /weather-initiate-fetch """
    status: str = Field(..., description="Status indicator, e.g., 'fetch_accepted'")
    job_id: str = Field(..., description="The unique job ID assigned by the miner.")
    message: Optional[str] = Field(None, description="Optional message.")

class WeatherGetInputStatusResponse(BaseModel):
    """ Response model for /weather-get-input-status """
    job_id: str = Field(..., description="The job ID.")
    status: str = Field(..., description="Current status of the input fetching/hashing process (e.g., 'fetch_queued', 'fetching_gfs', 'hashing_input', 'input_hashed_awaiting_validation', 'fetch_error').")
    input_data_hash: Optional[str] = Field(None, description="The computed SHA-256 hash of the input data, if available.")
    message: Optional[str] = Field(None, description="Optional message, especially for errors.")

class WeatherStartInferenceResponse(BaseModel):
     """ Response model for /weather-start-inference """
     status: str = Field(..., description="Status indicator, e.g., 'inference_started', 'error'")
     message: Optional[str] = Field(None, description="Optional message.")

class WeatherProgressUpdate(BaseModel):
    """Progress update for weather task operations"""
    operation: str  # e.g., "gfs_download", "era5_download", "inference", "verification"
    stage: str      # e.g., "downloading", "processing", "caching", "completed"
    progress: float # 0.0 to 1.0
    message: str
    estimated_time_remaining: Optional[int] = None  # seconds
    bytes_downloaded: Optional[int] = None
    bytes_total: Optional[int] = None
    files_completed: Optional[int] = None
    files_total: Optional[int] = None

class WeatherFileLocation(BaseModel):
    """Information about where weather files are stored"""
    file_type: str  # e.g., "gfs_cache", "era5_cache", "forecast", "input_batch"
    local_path: str
    size_bytes: Optional[int] = None
    created_time: Optional[datetime] = None
    description: str