import numpy as np
import xarray as xr
from typing import Dict, List, Tuple, Union, Optional
import gc
import asyncio
from gaia.utils.custom_logger import get_logger

logger = get_logger(__name__)

"""
Weather scoring constants.

This module primarily defines constants used in weather forecast scoring,
like weights for different variables.
"""

# Variable weights based on impact, subject to tuning.
# Weights are normalized so that surface + atmospheric variables sum to 1.0
VARIABLE_WEIGHTS = {
    # Surface variables (higher weight due to direct impact)
    "2t": 0.25,   # 2m temperature - most important for human impact
    "msl": 0.20,  # Mean sea level pressure - critical for weather patterns
    "10u": 0.10,  # 10m U wind - surface conditions
    "10v": 0.10,  # 10m V wind - surface conditions
    
    # Atmospheric variables (distributed across pressure levels)
    "t": 0.15,    # Temperature at pressure levels - atmospheric structure
    "z": 0.10,    # Geopotential - atmospheric dynamics
    "q": 0.05,    # Specific humidity - moisture content
    "u": 0.025,   # U wind at pressure levels - atmospheric flow
    "v": 0.025,   # V wind at pressure levels - atmospheric flow
}
