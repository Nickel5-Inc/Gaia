"""
Geomagnetic Task Core Module

This module contains the core components of the geomagnetic task including
the main task interface and configuration management.
"""

from .config import GeomagneticConfig, load_geomagnetic_config
from .task import GeomagneticTask

__all__ = ["GeomagneticTask", "GeomagneticConfig", "load_geomagnetic_config"]