"""
Soil Moisture Task Configuration

Centralized configuration management for the soil moisture prediction task.
Extracted from the monolithic soil_task.py (3,195 lines) for better modularity.
"""

import os
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseSettings, Field


class SoilMoistureConfig(BaseSettings):
    """Configuration settings for the soil moisture prediction task."""

    # Task timing configuration
    prediction_horizon: timedelta = Field(
        default_factory=lambda: timedelta(hours=6),
        description="Prediction horizon for the task",
    )
    scoring_delay: timedelta = Field(
        default_factory=lambda: timedelta(days=3),
        description="Delay before scoring due to SMAP data latency",
    )

    # Node configuration
    node_type: str = Field(default="miner", description="Node type (validator or miner)")
    test_mode: bool = Field(default=False, description="Enable test mode")
    use_raw_preprocessing: bool = Field(
        default=False, description="Use raw preprocessing for custom models"
    )

    # Performance optimization
    use_threaded_scoring: bool = Field(
        default=False, description="Enable threaded scoring for performance improvement"
    )

    # Scoring mechanism configuration
    baseline_rmse: float = Field(default=50.0, description="Baseline RMSE for scoring")
    alpha: float = Field(default=10.0, description="Alpha parameter for scoring")
    beta: float = Field(default=0.1, description="Beta parameter for scoring")

    # Data processing configuration
    max_tiff_size_mb: float = Field(
        default=50.0, description="Maximum TIFF size in MB before logging warning"
    )
    cleanup_age_hours: int = Field(
        default=24, description="Age in hours after which to cleanup old regions"
    )

    # Validator windows configuration
    validator_windows: List[Tuple[int, int, int, int]] = Field(
        default_factory=lambda: [
            (0, 0, 5, 59),    # 00:00-05:59
            (6, 0, 11, 59),   # 06:00-11:59
            (12, 0, 17, 59),  # 12:00-17:59
            (18, 0, 23, 59),  # 18:00-23:59
        ],
        description="Validator processing windows (start_hour, start_min, end_hour, end_min)",
    )

    # Custom model configuration
    custom_model_path: str = Field(
        default="gaia/models/custom_models/custom_soil_model.py",
        description="Path to custom soil model implementation",
    )

    # Memory management
    enable_memory_logging: bool = Field(
        default=False, description="Enable memory usage logging"
    )
    gc_collection_threshold: int = Field(
        default=50, description="GC collection threshold for large TIFF processing"
    )

    # Database configuration
    scoring_interval_minutes: int = Field(
        default=5, description="Interval in minutes for scoring checks"
    )
    max_concurrent_scoring: int = Field(
        default=10, description="Maximum concurrent scoring operations"
    )

    # SMAP data configuration
    smap_api_timeout: int = Field(default=300, description="SMAP API timeout in seconds")
    smap_retry_attempts: int = Field(default=3, description="Number of SMAP API retry attempts")

    # Preprocessing configuration
    preprocessing_timeout: int = Field(
        default=600, description="Preprocessing timeout in seconds"
    )
    max_region_size: int = Field(
        default=1000000, description="Maximum region size in pixels"
    )

    class Config:
        env_prefix = "SOIL_"
        case_sensitive = False


def load_soil_moisture_config() -> SoilMoistureConfig:
    """Load soil moisture configuration from environment variables."""
    config = SoilMoistureConfig()

    # Override threaded scoring from environment
    if os.getenv("SOIL_THREADED_SCORING", "false").lower() == "true":
        config.use_threaded_scoring = True

    # Override test mode from environment
    if os.getenv("SOIL_TEST_MODE", "false").lower() == "true":
        config.test_mode = True

    # Override node type from environment
    node_type = os.getenv("NODE_TYPE")
    if node_type:
        config.node_type = node_type

    return config


def get_validator_windows(config: Optional[SoilMoistureConfig] = None) -> List[Tuple[int, int, int, int]]:
    """Get validator processing windows."""
    if config is None:
        config = load_soil_moisture_config()
    return config.validator_windows


def is_in_window(
    current_time, window: Tuple[int, int, int, int], config: Optional[SoilMoistureConfig] = None
) -> bool:
    """Check if current time is within a validator processing window."""
    start_hr, start_min, end_hr, end_min = window
    current_mins = current_time.hour * 60 + current_time.minute
    start_mins = start_hr * 60 + start_min
    end_mins = end_hr * 60 + end_min

    return start_mins <= current_mins <= end_mins


def get_custom_model_path(config: Optional[SoilMoistureConfig] = None) -> str:
    """Get the path to the custom soil model."""
    if config is None:
        config = load_soil_moisture_config()
    return config.custom_model_path


def should_use_threaded_scoring(config: Optional[SoilMoistureConfig] = None) -> bool:
    """Check if threaded scoring should be used."""
    if config is None:
        config = load_soil_moisture_config()
    return config.use_threaded_scoring