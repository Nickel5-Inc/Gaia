"""
Weather Task Core Module

This module contains the core components of the weather task including
the main task interface and configuration management.

Extracted from the monolithic weather_task.py (5,737 lines) for better modularity.
"""

from .config import WeatherConfig, load_weather_config
from .task import WeatherTask

__all__ = ["WeatherTask", "WeatherConfig", "load_weather_config"]