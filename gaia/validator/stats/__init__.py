"""
Weather Forecast Statistics Management Module

This module provides comprehensive tracking and analysis of weather forecast
performance metrics for miners in the Gaia network.
"""

from .stats_integration import WeatherStatsIntegration
from .weather_stats_hooks import WeatherStatsHooks
from .weather_stats_manager import WeatherStatsManager

__all__ = [
    "WeatherStatsManager",
    "WeatherStatsIntegration",
    "WeatherStatsHooks",
]
