from pydantic_settings import BaseSettings
from typing import Dict
import os


class Settings(BaseSettings):
    """Configuration settings for the Gaia Validator v4.0 multi-process architecture."""
    
    # Process Management
    NUM_COMPUTE_WORKERS: int = 8
    MAX_JOBS_PER_WORKER: int = 100
    SUPERVISOR_CHECK_INTERVAL_S: int = 30
    WORKER_QUEUE_TIMEOUT_S: int = 60
    
    # Memory Limits (MB)
    PROCESS_MAX_RSS_MB: Dict[str, int] = {
        "io": 2048,        # IO-Engine process
        "compute": 1024,   # Each compute worker
        "supervisor": 256  # Supervisor process
    }
    
    # IPC Configuration
    IPC_QUEUE_SIZE: int = 1000
    
    # Database Configuration
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://localhost/gaia_validator")
    DATABASE_POOL_MIN_SIZE: int = 5
    DATABASE_POOL_MAX_SIZE: int = 20
    
    # HTTP Client Configuration
    HTTP_TIMEOUT_SECONDS: int = 30
    HTTP_MAX_CONNECTIONS: int = 100
    
    # Task Scheduling
    SCHEDULER_TIMEZONE: str = "UTC"
    
    # Logging Configuration
    LOG_LEVEL: str = "INFO"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()