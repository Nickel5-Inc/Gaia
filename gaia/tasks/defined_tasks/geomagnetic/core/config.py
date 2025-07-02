"""
Geomagnetic Task Configuration

Centralized configuration management for the geomagnetic prediction task.
Extracted from the monolithic geomagnetic_task.py (2,845 lines) for better modularity.
"""

import os
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseSettings, Field


class GeomagneticConfig(BaseSettings):
    """Configuration settings for the geomagnetic prediction task."""

    # Node configuration
    node_type: str = Field(default="validator", description="Node type (validator or miner)")
    test_mode: bool = Field(default=False, description="Enable test mode")

    # Timing and execution configuration
    execution_window_start_minutes: int = Field(
        default=2, description="Execution window start (minutes after hour)"
    )
    execution_window_end_minutes: int = Field(
        default=15, description="Execution window end (minutes after hour)"
    )
    prediction_horizon_hours: int = Field(
        default=1, description="Prediction horizon in hours"
    )

    # Data fetching configuration
    max_wait_minutes_production: int = Field(
        default=30, description="Maximum minutes to wait for target hour data in production"
    )
    max_wait_minutes_test: int = Field(
        default=5, description="Maximum minutes to wait for target hour data in test mode"
    )

    # Security and temporal separation
    ground_truth_delay_minutes_production: int = Field(
        default=90, description="Minimum delay for ground truth fetching in production (security)"
    )
    ground_truth_delay_minutes_test: int = Field(
        default=30, description="Minimum delay for ground truth fetching in test mode"
    )
    max_ground_truth_attempts: int = Field(
        default=3, description="Maximum attempts to fetch ground truth"
    )

    # Retry and cleanup configuration
    max_retry_attempts: int = Field(
        default=10, description="Maximum retry attempts for failed tasks"
    )
    pending_retry_interval_minutes: int = Field(
        default=15, description="Interval for checking pending tasks"
    )
    comprehensive_cleanup_interval_hours: int = Field(
        default=6, description="Interval for comprehensive cleanup of old pending tasks"
    )

    # Custom model configuration
    custom_model_path: str = Field(
        default="gaia/models/custom_models/custom_geomagnetic_model.py",
        description="Path to custom geomagnetic model implementation",
    )

    # Database and performance configuration
    batch_size: int = Field(default=100, description="Batch size for database operations")
    query_timeout_seconds: int = Field(default=300, description="Query timeout in seconds")
    
    # Historical data configuration
    include_historical_data: bool = Field(
        default=True, description="Include historical data in miner queries"
    )
    historical_data_months: int = Field(
        default=1, description="Number of months of historical data to include"
    )

    # Sleep and polling configuration
    error_sleep_seconds: int = Field(
        default=3600, description="Sleep duration after errors (1 hour)"
    )
    idle_sleep_seconds: int = Field(
        default=300, description="Sleep duration when idle (5 minutes)"
    )
    test_mode_sleep_seconds: int = Field(
        default=300, description="Sleep duration in test mode (5 minutes)"
    )

    # Scoring configuration
    enable_scoring: bool = Field(default=True, description="Enable prediction scoring")
    score_recalculation_enabled: bool = Field(
        default=True, description="Enable score recalculation for recent tasks"
    )

    class Config:
        env_prefix = "GEOMAG_"
        case_sensitive = False


def load_geomagnetic_config() -> GeomagneticConfig:
    """Load geomagnetic configuration from environment variables."""
    config = GeomagneticConfig()

    # Override test mode from environment
    if os.getenv("GEOMAG_TEST_MODE", "false").lower() == "true":
        config.test_mode = True

    # Override node type from environment
    node_type = os.getenv("NODE_TYPE")
    if node_type:
        config.node_type = node_type

    return config


def get_execution_window(config: Optional[GeomagneticConfig] = None) -> Tuple[int, int]:
    """Get execution window (start_minutes, end_minutes)."""
    if config is None:
        config = load_geomagnetic_config()
    return config.execution_window_start_minutes, config.execution_window_end_minutes


def get_ground_truth_delay_minutes(config: Optional[GeomagneticConfig] = None) -> int:
    """Get ground truth delay based on test mode."""
    if config is None:
        config = load_geomagnetic_config()
    
    if config.test_mode:
        return config.ground_truth_delay_minutes_test
    else:
        return config.ground_truth_delay_minutes_production


def get_max_wait_minutes(config: Optional[GeomagneticConfig] = None) -> int:
    """Get maximum wait time for data based on test mode."""
    if config is None:
        config = load_geomagnetic_config()
    
    if config.test_mode:
        return config.max_wait_minutes_test
    else:
        return config.max_wait_minutes_production


def should_include_historical_data(config: Optional[GeomagneticConfig] = None) -> bool:
    """Check if historical data should be included."""
    if config is None:
        config = load_geomagnetic_config()
    return config.include_historical_data


def get_custom_model_path(config: Optional[GeomagneticConfig] = None) -> str:
    """Get the path to the custom geomagnetic model."""
    if config is None:
        config = load_geomagnetic_config()
    return config.custom_model_path


def is_in_execution_window(current_time, config: Optional[GeomagneticConfig] = None) -> bool:
    """Check if current time is within the execution window."""
    if config is None:
        config = load_geomagnetic_config()
    
    start_min, end_min = get_execution_window(config)
    current_minute = current_time.minute
    
    return start_min <= current_minute <= end_min