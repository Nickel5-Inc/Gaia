"""
Soil Moisture Task Core Module

This module contains the core components of the soil moisture task including
the main task interface and configuration management.
"""

from .config import SoilMoistureConfig, load_soil_moisture_config
from .task import SoilMoistureTask

__all__ = ["SoilMoistureTask", "SoilMoistureConfig", "load_soil_moisture_config"]