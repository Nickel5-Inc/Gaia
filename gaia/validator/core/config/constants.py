"""
Validator System Constants
=========================

All constants and default values used throughout the validator system.
Organized by functional domain for easy reference.
"""

from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple


# =============================================================================
# TIMEOUT CONSTANTS
# =============================================================================

DEFAULT_TIMEOUTS = {
    'scoring': {
        'default': 1800,          # 30 minutes
        'weight_setting': 300,    # 5 minutes
        'database_query': 120,    # 2 minutes
    },
    'deregistration': {
        'default': 1800,          # 30 minutes
        'db_check': 300,          # 5 minutes
    },
    'geomagnetic': {
        'default': 1800,          # 30 minutes
        'data_fetch': 300,        # 5 minutes
        'miner_query': 600,       # 10 minutes
    },
    'soil': {
        'default': 3600,          # 1 hour
        'data_download': 1800,    # 30 minutes
        'miner_query': 1800,      # 30 minutes
        'region_processing': 900, # 15 minutes
    },
    'weather': {
        'default': 1800,          # 30 minutes
        'data_fetch': 600,        # 10 minutes
        'miner_query': 900,       # 15 minutes
    },
    'http': {
        'connect': 10.0,
        'read': 120.0,
        'write': 30.0,
        'pool': 5.0,
    }
}


# =============================================================================
# MEMORY MANAGEMENT CONSTANTS  
# =============================================================================

MEMORY_THRESHOLDS = {
    'warning_mb': 8000,           # 8GB
    'emergency_mb': 10000,        # 10GB  
    'critical_mb': 12000,         # 12GB
    'log_interval_seconds': 300,  # 5 minutes
    'gc_cooldown_seconds': 60,    # 1 minute between emergency GC
}

MEMORY_CLEANUP_PATTERNS = [
    'cache', 'registry', '_cached', '__pycache__', '_instance_cache',
    '_memo', '_lru', '_registry', '_store', '_buffer'
]

TARGET_CLEANUP_MODULES = [
    'substrate', 'scalecodec', 'scale_info', 'metadata', 'fiber', 'substrateinterface',
    'xarray', 'dask', 'numpy', 'pandas', 'fsspec', 'zarr', 
    'netcdf4', 'h5py', 'scipy', 'era5', 'gcsfs', 'cloudpickle',
    'numcodecs', 'blosc', 'lz4', 'snappy', 'zstd'
]


# =============================================================================
# TASK WEIGHT SCHEDULE
# =============================================================================

TASK_WEIGHTS_SCHEDULE: List[Tuple[datetime, Dict[str, float]]] = [
    (datetime(2025, 5, 28, 0, 0, 0, tzinfo=timezone.utc), 
     {"weather": 0.50, "geomagnetic": 0.25, "soil": 0.25}),
    
    (datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc), 
     {"weather": 0.65, "geomagnetic": 0.175, "soil": 0.175}), 
    
    (datetime(2025, 6, 5, 0, 0, 0, tzinfo=timezone.utc), 
     {"weather": 0.80, "geomagnetic": 0.10, "soil": 0.10})
]


# =============================================================================
# NETWORK CONSTANTS
# =============================================================================

NETWORK_CONSTANTS = {
    'default_netuid': 237,
    'max_uids': 256,
    'connection_limits': {
        'max_connections': 100,
        'max_keepalive_connections': 50,
        'keepalive_expiry': 300,  # 5 minutes
    },
    'retry_config': {
        'max_retries': 2,
        'base_timeout': 15.0,
        'progressive_timeout_multiplier': 1.5,
    },
    'miner_query': {
        'chunk_size': 50,           # Process miners in chunks
        'chunk_concurrency': 15,    # Concurrent requests per chunk
        'chunk_delay': 0.5,         # Delay between chunks
    }
}


# =============================================================================
# DATABASE CONSTANTS
# =============================================================================

DATABASE_CONSTANTS = {
    'connection_timeout': 30,
    'query_timeout': 60,
    'max_pool_size': 20,
    'pool_recycle': 3600,  # 1 hour
    'monitor_history_max_size': 120,  # 2 hours at 1-minute intervals
}


# =============================================================================
# SYSTEM MONITORING CONSTANTS
# =============================================================================

MONITORING_INTERVALS = {
    'status_logger': 60,           # 1 minute
    'memory_monitor': 60,          # 1 minute  
    'client_health': 300,          # 5 minutes
    'substrate_cleanup': 300,      # 5 minutes
    'aggressive_cleanup': 120,     # 2 minutes
    'earthdata_token': 86400,      # 24 hours
    'miner_deregistration': 600,   # 10 minutes
}


# =============================================================================
# SCORING CONSTANTS
# =============================================================================

SCORING_CONSTANTS = {
    'weight_normalization': {
        'percentile_midpoint': 80,
        'sigmoid_params': {
            'b': 70, 'Q': 8, 'v': 0.3, 'k': 0.98, 'a': 0.0, 'slope': 0.01
        },
        'rank_fallback_power': 1.2,
        'uniformity_threshold': 0.01,
    },
    'data_retention': {
        'max_weather_records': 50,
        'max_geomagnetic_records': 50, 
        'max_soil_records': 200,
        'score_age_days': 1,
    }
}


# =============================================================================
# CLEANUP CONSTANTS
# =============================================================================

CLEANUP_CONSTANTS = {
    'intervals': {
        'gfs_cache_retention_days': 7,
        'era5_cache_retention_days': 30,
        'ensemble_retention_days': 14,
        'db_run_retention_days': 90,
        'check_interval_seconds': 6 * 3600,  # 6 hours
    },
    'file_patterns': {
        'cache_files': "*.nc",
        'ensemble_files': ["*.nc", "*.json"],
        'zarr_stores': "*.zarr",
    }
} 