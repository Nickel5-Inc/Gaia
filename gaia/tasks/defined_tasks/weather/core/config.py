"""
Weather Task Configuration Management

Centralizes all environment variable handling and configuration
validation for the weather task.
"""

import os
from typing import Any, Dict, List, Optional

from fiber.logging_utils import get_logger
from pydantic import BaseModel, Field

# High-performance JSON operations for weather task
try:
    from gaia.utils.performance import dumps, loads
except ImportError:
    import json

    def dumps(obj, **kwargs):
        return json.dumps(obj, **kwargs)

    def loads(s):
        return json.loads(s)


logger = get_logger(__name__)


class WeatherConfig(BaseModel):
    """Type-safe weather task configuration."""
    
    # Worker/Run Parameters
    max_concurrent_inferences: int = Field(default=1)
    inference_steps: int = Field(default=40)
    forecast_step_hours: int = Field(default=6)
    forecast_duration_hours: int = Field(default=240)  # computed: steps * step_hours
    
    # Scoring Parameters
    initial_scoring_lead_hours: List[int] = Field(default=[6, 12])
    final_scoring_lead_hours: List[int] = Field(default=[60, 78, 138])
    verification_wait_minutes: int = Field(default=60)
    verification_timeout_seconds: int = Field(default=3600)
    final_scoring_check_interval_seconds: int = Field(default=3600)
    era5_delay_days: int = Field(default=5)
    era5_buffer_hours: int = Field(default=6)
    cleanup_check_interval_seconds: int = Field(default=21600)  # 6 hours
    
    # Cache/Storage Paths
    gfs_analysis_cache_dir: str = Field(default="./gfs_analysis_cache")
    era5_cache_dir: str = Field(default="./era5_cache")
    gfs_cache_retention_days: int = Field(default=7)
    era5_cache_retention_days: int = Field(default=30)
    ensemble_retention_days: int = Field(default=14)
    db_run_retention_days: int = Field(default=90)
    input_batch_retention_days: int = Field(default=3)
    
    # Run Timing
    run_hour_utc: int = Field(default=18)
    run_minute_utc: int = Field(default=0)
    validator_hash_wait_minutes: int = Field(default=10)
    
    # R2 Cleanup Config
    r2_cleanup_enabled: bool = Field(default=True)
    r2_cleanup_interval_seconds: int = Field(default=1800)  # 30 minutes
    r2_max_inputs_to_keep: int = Field(default=0)  # Keep NO inputs
    r2_max_forecasts_to_keep: int = Field(default=1)  # Keep only 1 forecast
    
    # JWT Configuration
    miner_jwt_secret_key: str = Field(default="insecure_default_key_for_development_only")
    jwt_algorithm: str = Field(default="HS256")
    access_token_expire_minutes: int = Field(default=120)
    
    # ERA5 Climatology
    era5_climatology_path: str = Field(
        default="gs://weatherbench2/datasets/era5-hourly-climatology/1990-2019_6h_1440x721.zarr"
    )
    
    # Day-1 Scoring Configuration
    day1_variables_levels_to_score: List[Dict[str, Any]] = Field(default_factory=lambda: [
        {"name": "z", "level": 500, "standard_name": "geopotential"},
        {"name": "t", "level": 850, "standard_name": "temperature"},
        {"name": "2t", "level": None, "standard_name": "2m_temperature"},
        {"name": "msl", "level": None, "standard_name": "mean_sea_level_pressure"},
    ])
    day1_climatology_bounds: Dict[str, tuple] = Field(default_factory=lambda: {
        "2t": (180, 340),  # Kelvin
        "msl": (90000, 110000),  # Pascals
        "t850": (220, 320),  # Kelvin for 850hPa Temperature
        "z500": (45000, 60000),  # m^2/s^2 for Geopotential at 500hPa
    })
    day1_pattern_correlation_threshold: float = Field(default=0.3)
    day1_acc_lower_bound: float = Field(default=0.6)
    day1_alpha_skill: float = Field(default=0.6)
    day1_beta_acc: float = Field(default=0.4)
    
    # Quality Control Config
    day1_clone_penalty_gamma: float = Field(default=1.0)
    day1_clone_delta_thresholds: Dict[str, float] = Field(default_factory=lambda: {
        "2t": 0.0025,  # (RMSE 0.1K)^2
        "msl": 400,  # (RMSE 100Pa or 1hPa)^2
        "z500": 100,  # (RMSE 100 m^2/s^2)^2
        "t850": 0.04,  # (RMSE 0.5K)^2
    })
    
    # Scoring Weights
    weather_score_day1_weight: float = Field(default=0.2)
    weather_score_era5_weight: float = Field(default=0.8)
    weather_bonus_value_add: float = Field(default=0.05)
    
    # RunPod Specific Config
    runpod_poll_interval_seconds: int = Field(default=10)
    runpod_max_poll_attempts: int = Field(default=900)  # 2.5 hours
    runpod_download_endpoint_suffix: str = Field(default="run/download_step")
    runpod_upload_input_suffix: str = Field(default="run/upload_input")


def parse_int_list(env_var: str, default_list: List[int]) -> List[int]:
    """Parse a comma-separated list of integers from environment variable."""
    val_str = os.getenv(env_var)
    if val_str:
        try:
            return [int(x.strip()) for x in val_str.split(",")]
        except ValueError:
            logger.warning(f"Invalid format for {env_var}: '{val_str}'. Using default.")
    return default_list


def load_weather_config() -> WeatherConfig:
    """Load weather task configuration from environment variables."""
    logger.info("Loading WeatherTask configuration from environment variables...")
    
    # Worker/Run Parameters
    max_concurrent_inferences = int(os.getenv("WEATHER_MAX_CONCURRENT_INFERENCES", "1"))
    inference_steps = int(os.getenv("WEATHER_INFERENCE_STEPS", "40"))
    forecast_step_hours = int(os.getenv("WEATHER_FORECAST_STEP_HOURS", "6"))
    forecast_duration_hours = inference_steps * forecast_step_hours
    
    # Scoring Parameters
    initial_scoring_lead_hours = parse_int_list("WEATHER_INITIAL_SCORING_LEAD_HOURS", [6, 12])
    final_scoring_lead_hours = parse_int_list("WEATHER_FINAL_SCORING_LEAD_HOURS", [60, 78, 138])
    verification_wait_minutes = int(os.getenv("WEATHER_VERIFICATION_WAIT_MINUTES", "60"))
    verification_timeout_seconds = int(os.getenv("WEATHER_VERIFICATION_TIMEOUT_SECONDS", "3600"))
    final_scoring_check_interval_seconds = int(os.getenv("WEATHER_FINAL_SCORING_INTERVAL_S", "3600"))
    era5_delay_days = int(os.getenv("WEATHER_ERA5_DELAY_DAYS", "5"))
    era5_buffer_hours = int(os.getenv("WEATHER_ERA5_BUFFER_HOURS", "6"))
    cleanup_check_interval_seconds = int(os.getenv("WEATHER_CLEANUP_INTERVAL_S", "21600"))
    
    # Cache/Storage
    gfs_analysis_cache_dir = os.getenv("WEATHER_GFS_CACHE_DIR", "./gfs_analysis_cache")
    era5_cache_dir = os.getenv("WEATHER_ERA5_CACHE_DIR", "./era5_cache")
    gfs_cache_retention_days = int(os.getenv("WEATHER_GFS_CACHE_RETENTION_DAYS", "7"))
    era5_cache_retention_days = int(os.getenv("WEATHER_ERA5_CACHE_RETENTION_DAYS", "30"))
    ensemble_retention_days = int(os.getenv("WEATHER_ENSEMBLE_RETENTION_DAYS", "14"))
    db_run_retention_days = int(os.getenv("WEATHER_DB_RUN_RETENTION_DAYS", "90"))
    input_batch_retention_days = int(os.getenv("WEATHER_INPUT_BATCH_RETENTION_DAYS", "3"))
    
    # Run Timing
    run_hour_utc = int(os.getenv("WEATHER_RUN_HOUR_UTC", "18"))
    run_minute_utc = int(os.getenv("WEATHER_RUN_MINUTE_UTC", "0"))
    validator_hash_wait_minutes = int(os.getenv("WEATHER_VALIDATOR_HASH_WAIT_MINUTES", "10"))
    
    # R2 Cleanup Config
    r2_cleanup_enabled = os.getenv("WEATHER_R2_CLEANUP_ENABLED", "true").lower() in ["true", "1", "yes"]
    r2_cleanup_interval_seconds = int(os.getenv("WEATHER_R2_CLEANUP_INTERVAL_S", "1800"))
    r2_max_inputs_to_keep = int(os.getenv("WEATHER_R2_MAX_INPUTS_TO_KEEP", "0"))
    r2_max_forecasts_to_keep = int(os.getenv("WEATHER_R2_MAX_FORECASTS_TO_KEEP", "1"))
    
    # JWT Configuration
    miner_jwt_secret_key = os.getenv("MINER_JWT_SECRET_KEY", "insecure_default_key_for_development_only")
    jwt_algorithm = os.getenv("MINER_JWT_ALGORITHM", "HS256")
    access_token_expire_minutes = int(os.getenv("WEATHER_ACCESS_TOKEN_EXPIRE_MINUTES", "120"))
    
    # ERA5 Climatology
    era5_climatology_path = os.getenv(
        "WEATHER_ERA5_CLIMATOLOGY_PATH",
        "gs://weatherbench2/datasets/era5-hourly-climatology/1990-2019_6h_1440x721.zarr",
    )
    
    # Day-1 Scoring Variables/Levels Configuration
    default_day1_vars_levels = [
        {"name": "z", "level": 500, "standard_name": "geopotential"},
        {"name": "t", "level": 850, "standard_name": "temperature"},
        {"name": "2t", "level": None, "standard_name": "2m_temperature"},
        {"name": "msl", "level": None, "standard_name": "mean_sea_level_pressure"},
    ]
    try:
        day1_vars_levels_json = os.getenv("WEATHER_DAY1_VARIABLES_LEVELS_JSON")
        day1_variables_levels_to_score = (
            loads(day1_vars_levels_json) if day1_vars_levels_json else default_day1_vars_levels
        )
    except Exception:
        logger.warning("Invalid JSON for WEATHER_DAY1_VARIABLES_LEVELS_JSON. Using default.")
        day1_variables_levels_to_score = default_day1_vars_levels
    
    # Day-1 Climatology Bounds
    default_day1_clim_bounds = {
        "2t": (180, 340),  # Kelvin
        "msl": (90000, 110000),  # Pascals
        "t850": (220, 320),  # Kelvin for 850hPa Temperature
        "z500": (45000, 60000),  # m^2/s^2 for Geopotential at 500hPa
    }
    try:
        day1_clim_bounds_json = os.getenv("WEATHER_DAY1_CLIMATOLOGY_BOUNDS_JSON")
        day1_climatology_bounds = (
            loads(day1_clim_bounds_json) if day1_clim_bounds_json else default_day1_clim_bounds
        )
    except Exception:
        logger.warning("Invalid JSON for WEATHER_DAY1_CLIMATOLOGY_BOUNDS_JSON. Using default.")
        day1_climatology_bounds = default_day1_clim_bounds
    
    # Day-1 Scoring Parameters
    day1_pattern_correlation_threshold = float(os.getenv("WEATHER_DAY1_PATTERN_CORR_THRESHOLD", "0.3"))
    day1_acc_lower_bound = float(os.getenv("WEATHER_DAY1_ACC_LOWER_BOUND", "0.6"))
    day1_alpha_skill = float(os.getenv("WEATHER_DAY1_ALPHA_SKILL", "0.6"))
    day1_beta_acc = float(os.getenv("WEATHER_DAY1_BETA_ACC", "0.4"))
    
    # Quality Control Config
    day1_clone_penalty_gamma = float(os.getenv("WEATHER_DAY1_CLONE_PENALTY_GAMMA", "1.0"))
    default_clone_delta_thresholds = {
        "2t": 0.0025,  # (RMSE 0.1K)^2
        "msl": 400,  # (RMSE 100Pa or 1hPa)^2
        "z500": 100,  # (RMSE 100 m^2/s^2)^2
        "t850": 0.04,  # (RMSE 0.5K)^2
    }
    try:
        clone_delta_json = os.getenv("WEATHER_DAY1_CLONE_DELTA_THRESHOLDS_JSON")
        day1_clone_delta_thresholds = (
            loads(clone_delta_json) if clone_delta_json else default_clone_delta_thresholds
        )
    except Exception:
        logger.warning("Invalid JSON for WEATHER_DAY1_CLONE_DELTA_THRESHOLDS_JSON. Using default.")
        day1_clone_delta_thresholds = default_clone_delta_thresholds
    
    # Scoring Weights
    weather_score_day1_weight = float(os.getenv("WEATHER_SCORE_DAY1_WEIGHT", "0.2"))
    weather_score_era5_weight = float(os.getenv("WEATHER_SCORE_ERA5_WEIGHT", "0.8"))
    weather_bonus_value_add = float(os.getenv("WEATHER_BONUS_VALUE_ADD", "0.05"))
    
    # RunPod Specific Config
    runpod_poll_interval_seconds = int(os.getenv("RUNPOD_POLL_INTERVAL_SECONDS", "10"))
    runpod_max_poll_attempts = int(os.getenv("RUNPOD_MAX_POLL_ATTEMPTS", "900"))
    runpod_download_endpoint_suffix = os.getenv("RUNPOD_DOWNLOAD_ENDPOINT_SUFFIX", "run/download_step")
    runpod_upload_input_suffix = os.getenv("RUNPOD_UPLOAD_INPUT_SUFFIX", "run/upload_input")
    
    config = WeatherConfig(
        max_concurrent_inferences=max_concurrent_inferences,
        inference_steps=inference_steps,
        forecast_step_hours=forecast_step_hours,
        forecast_duration_hours=forecast_duration_hours,
        initial_scoring_lead_hours=initial_scoring_lead_hours,
        final_scoring_lead_hours=final_scoring_lead_hours,
        verification_wait_minutes=verification_wait_minutes,
        verification_timeout_seconds=verification_timeout_seconds,
        final_scoring_check_interval_seconds=final_scoring_check_interval_seconds,
        era5_delay_days=era5_delay_days,
        era5_buffer_hours=era5_buffer_hours,
        cleanup_check_interval_seconds=cleanup_check_interval_seconds,
        gfs_analysis_cache_dir=gfs_analysis_cache_dir,
        era5_cache_dir=era5_cache_dir,
        gfs_cache_retention_days=gfs_cache_retention_days,
        era5_cache_retention_days=era5_cache_retention_days,
        ensemble_retention_days=ensemble_retention_days,
        db_run_retention_days=db_run_retention_days,
        input_batch_retention_days=input_batch_retention_days,
        run_hour_utc=run_hour_utc,
        run_minute_utc=run_minute_utc,
        validator_hash_wait_minutes=validator_hash_wait_minutes,
        r2_cleanup_enabled=r2_cleanup_enabled,
        r2_cleanup_interval_seconds=r2_cleanup_interval_seconds,
        r2_max_inputs_to_keep=r2_max_inputs_to_keep,
        r2_max_forecasts_to_keep=r2_max_forecasts_to_keep,
        miner_jwt_secret_key=miner_jwt_secret_key,
        jwt_algorithm=jwt_algorithm,
        access_token_expire_minutes=access_token_expire_minutes,
        era5_climatology_path=era5_climatology_path,
        day1_variables_levels_to_score=day1_variables_levels_to_score,
        day1_climatology_bounds=day1_climatology_bounds,
        day1_pattern_correlation_threshold=day1_pattern_correlation_threshold,
        day1_acc_lower_bound=day1_acc_lower_bound,
        day1_alpha_skill=day1_alpha_skill,
        day1_beta_acc=day1_beta_acc,
        day1_clone_penalty_gamma=day1_clone_penalty_gamma,
        day1_clone_delta_thresholds=day1_clone_delta_thresholds,
        weather_score_day1_weight=weather_score_day1_weight,
        weather_score_era5_weight=weather_score_era5_weight,
        weather_bonus_value_add=weather_bonus_value_add,
        runpod_poll_interval_seconds=runpod_poll_interval_seconds,
        runpod_max_poll_attempts=runpod_max_poll_attempts,
        runpod_download_endpoint_suffix=runpod_download_endpoint_suffix,
        runpod_upload_input_suffix=runpod_upload_input_suffix,
    )
    
    logger.info(f"WeatherTask configuration loaded successfully")
    return config