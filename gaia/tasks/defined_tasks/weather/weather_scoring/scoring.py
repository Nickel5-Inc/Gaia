import numpy as np
import xarray as xr
from typing import Dict, List, Tuple, Union, Optional
import gc
import asyncio
from fiber.logging_utils import get_logger
logger = get_logger(__name__)

"""
Weather scoring constants.

This module primarily defines constants used in weather forecast scoring,
like weights for different variables.
"""

# Variable weights based on impact, subject to tuning.
VARIABLE_WEIGHTS = {
    "2t": 0.20,   # 2m temperature 
    "msl": 0.15,  # Mean sea level pressure
    "10u": 0.10,  # 10m U wind
    "10v": 0.10,  # 10m V wind
    "t": 0.15,    # Temperature at pressure levels
    "q": 0.10,    # Specific humidity
    "z": 0.10,    # Geopotential
    "u": 0.05,    # U wind at pressure levels
    "v": 0.05     # V wind at pressure levels
}
