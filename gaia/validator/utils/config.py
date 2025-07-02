from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Dict, List, Optional, Any
import os


class WeatherSettings(BaseSettings):
    """Settings specific to the Weather task, prefixed with WEATHER_"""
    
    # Worker/Run Parameters
    MAX_CONCURRENT_INFERENCES: int = 1
    INFERENCE_STEPS: int = 40
    FORECAST_STEP_HOURS: int = 6
    
    # Scoring Parameters
    INITIAL_SCORING_LEAD_HOURS: List[int] = [6, 12]  # Day 0.25, 0.5
    FINAL_SCORING_LEAD_HOURS: List[int] = [60, 78, 138]  # Day 2, 3, 5
    VERIFICATION_WAIT_MINUTES: int = 60
    VERIFICATION_TIMEOUT_SECONDS: int = 3600
    FINAL_SCORING_CHECK_INTERVAL_SECONDS: int = 3600
    ERA5_DELAY_DAYS: int = 5
    ERA5_BUFFER_HOURS: int = 6
    CLEANUP_CHECK_INTERVAL_SECONDS: int = 21600  # 6 hours
    
    # Cache and Storage
    GFS_CACHE_DIR: str = "./gfs_analysis_cache"
    ERA5_CACHE_DIR: str = "./era5_cache"
    GFS_CACHE_RETENTION_DAYS: int = 7
    ERA5_CACHE_RETENTION_DAYS: int = 30
    ENSEMBLE_RETENTION_DAYS: int = 14
    DB_RUN_RETENTION_DAYS: int = 90
    INPUT_BATCH_RETENTION_DAYS: int = 3
    
    # Scheduling
    RUN_HOUR_UTC: int = 18
    RUN_MINUTE_UTC: int = 0
    VALIDATOR_HASH_WAIT_MINUTES: int = 10
    
    # R2 Cleanup Config
    R2_CLEANUP_ENABLED: bool = True
    R2_CLEANUP_INTERVAL_SECONDS: int = 1800  # 30 minutes
    R2_MAX_INPUTS_TO_KEEP: int = 0  # Keep NO inputs (delete immediately)
    R2_MAX_FORECASTS_TO_KEEP: int = 1  # Keep only 1 forecast per timestep
    
    # Authentication
    MINER_JWT_SECRET_KEY: str = "insecure_default_key_for_development_only"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 120
    
    # ERA5 Climatology
    ERA5_CLIMATOLOGY_PATH: str = "gs://weatherbench2/datasets/era5-hourly-climatology/1990-2019_6h_1440x721.zarr"
    
    # Day-1 Scoring Configurations
    DAY1_PATTERN_CORRELATION_THRESHOLD: float = 0.3
    DAY1_ACC_LOWER_BOUND: float = 0.6
    DAY1_ALPHA_SKILL: float = 0.6
    DAY1_BETA_ACC: float = 0.4
    DAY1_CLONE_PENALTY_GAMMA: float = 1.0
    
    # Scoring Weights
    SCORE_DAY1_WEIGHT: float = 0.2
    SCORE_ERA5_WEIGHT: float = 0.8
    BONUS_VALUE_ADD: float = 0.05
    
    # RunPod Configuration
    RUNPOD_POLL_INTERVAL_SECONDS: int = 10
    RUNPOD_MAX_POLL_ATTEMPTS: int = 900  # 150 minutes max
    RUNPOD_DOWNLOAD_ENDPOINT_SUFFIX: str = "run/download_step"
    RUNPOD_UPLOAD_INPUT_SUFFIX: str = "run/upload_input"
    
    # Inference Service
    INFERENCE_SERVICE_URL: Optional[str] = None
    INFERENCE_TYPE: str = "local_model"  # or "http_service"
    
    # File Serving
    FILE_SERVING_MODE: str = "local"  # or "r2_proxy"
    
    model_config = SettingsConfigDict(env_prefix='WEATHER_')


class SubstrateSettings(BaseSettings):
    """Settings for Substrate/Bittensor interaction"""
    
    NETWORK: str = "test"
    NETUID: int = 237
    WALLET_NAME: str = "default"
    HOTKEY_NAME: str = "default"
    SUBTENSOR_ADDRESS: Optional[str] = None
    
    model_config = SettingsConfigDict(env_prefix='SUBTENSOR_')


class DatabaseSettings(BaseSettings):
    """Database configuration settings"""
    
    URL: str = "postgresql://localhost/gaia_validator"
    NAME: str = "gaia_validator"
    HOST: str = "localhost"
    PORT: int = 5432
    USER: str = "postgres"
    PASSWORD: str = "postgres"
    
    # Connection Pool Settings
    POOL_MIN_SIZE: int = 5
    POOL_MAX_SIZE: int = 20
    
    model_config = SettingsConfigDict(env_prefix='DB_')


class R2Settings(BaseSettings):
    """R2/Cloudflare storage settings"""
    
    BUCKET: Optional[str] = None
    ENDPOINT: Optional[str] = None
    ACCESS_KEY_ID: Optional[str] = None
    SECRET_ACCESS_KEY: Optional[str] = None
    REGION: str = "auto"
    
    model_config = SettingsConfigDict(env_prefix='PGBACKREST_R2_')


class Settings(BaseSettings):
    """The main application settings object."""
    
    # Process Management
    NUM_COMPUTE_WORKERS: int = max(1, os.cpu_count() - 2 if os.cpu_count() else 2)
    MAX_JOBS_PER_WORKER: int = 100
    SUPERVISOR_CHECK_INTERVAL_S: int = 15
    WORKER_QUEUE_TIMEOUT_S: int = 300
    
    # Memory Limits (MB)
    PROCESS_MAX_RSS_MB: Dict[str, int] = {
        "io": 2048,        # IO-Engine process
        "compute": 1024,   # Each compute worker
        "supervisor": 256  # Supervisor process
    }
    
    # IPC Configuration
    IPC_QUEUE_SIZE: int = 1000
    
    # HTTP Client Configuration
    HTTP_TIMEOUT_SECONDS: int = 30
    HTTP_MAX_CONNECTIONS: int = 100
    
    # Task Scheduling
    SCHEDULER_TIMEZONE: str = "UTC"
    
    # Logging Configuration
    LOG_LEVEL: str = "INFO"
    
    # Performance Monitoring
    ENABLE_PROFILING: bool = False
    PROFILE_INTERVAL_SECONDS: int = 60
    PROFILE_OUTPUT_DIR: str = "./profiling_output"
    
    # Memory Monitoring
    MEMORY_MONITOR_ENABLED: bool = True
    MEMORY_WARNING_THRESHOLD_MB: int = 8000
    MEMORY_EMERGENCY_THRESHOLD_MB: int = 10000
    MEMORY_CRITICAL_THRESHOLD_MB: int = 12000
    
    # Database Sync
    IS_SOURCE_VALIDATOR_FOR_DB_SYNC: bool = False
    DB_SYNC_INTERVAL_HOURS: int = 1
    
    # Nested Task Settings
    WEATHER: WeatherSettings = WeatherSettings()
    SUBSTRATE: SubstrateSettings = SubstrateSettings()
    DATABASE: DatabaseSettings = DatabaseSettings()
    R2: R2Settings = R2Settings()
    
    # PGBackrest/Backup Settings
    PGBACKREST_STANZA: Optional[str] = None
    PGBACKREST_PGDATA: Optional[str] = None
    
    # Authentication
    EARTHDATA_USERNAME: Optional[str] = None
    EARTHDATA_PASSWORD: Optional[str] = None
    
    model_config = SettingsConfigDict(
        env_file='.env',
        env_nested_delimiter='__',  # Allows WEATHER__DB_RUN_RETENTION_DAYS in .env
        case_sensitive=False
    )


# Global settings instance
settings = Settings()