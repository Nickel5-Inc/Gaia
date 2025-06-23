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
    'http': {
        'connect': 10.0,
        'read': 120.0,
        'write': 30.0,
        'pool': 5.0,
    },
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
        'data_fetch': 300,        # 5 minutes
        'inference': 900,         # 15 minutes
    }
}


# =============================================================================
# MEMORY CONSTANTS
# =============================================================================

MEMORY_THRESHOLDS = {
    'warning_mb': 8000,     # 8GB
    'emergency_mb': 10000,  # 10GB  
    'critical_mb': 12000,   # 12GB
    'log_interval': 300,    # 5 minutes
    'gc_cooldown': 60,      # 1 minute between emergency GC
}


# =============================================================================
# NETWORK CONSTANTS
# =============================================================================

NETWORK_CONSTANTS = {
    'connection_limits': {
        'max_connections': 100,
        'max_keepalive_connections': 50,
        'keepalive_expiry': 300,  # 5 minutes
    },
    'retry_config': {
        'max_retries': 2,
        'base_timeout': 15.0,
    },
    'miner_query': {
        'chunk_size': 50,
        'chunk_concurrency': 15,
        'chunk_delay': 0.5,
    },
}


# =============================================================================
# TASK WEIGHT SCHEDULE
# =============================================================================

TASK_WEIGHTS_SCHEDULE = [
    (datetime(2025, 5, 28, 0, 0, 0, tzinfo=timezone.utc), 
     {"weather": 0.50, "geomagnetic": 0.25, "soil": 0.25}),
    
    # Transition Point 1: June 1st, 2025, 00:00:00 UTC
    (datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc), 
     {"weather": 0.65, "geomagnetic": 0.175, "soil": 0.175}), 
    
    # Target Weights: June 5th, 2025, 00:00:00 UTC
    (datetime(2025, 6, 5, 0, 0, 0, tzinfo=timezone.utc), 
     {"weather": 0.80, "geomagnetic": 0.10, "soil": 0.10})
]


# =============================================================================
# DATABASE CONSTANTS
# =============================================================================

DATABASE_CONSTANTS = {
    'connection_pool': {
        'min_size': 1,
        'max_size': 20,
        'timeout': 30.0,
    },
    'query_timeouts': {
        'default': 30.0,
        'long_running': 120.0,
        'bulk_operation': 300.0,
    },
    'cleanup_intervals': {
        'stale_operations': 3600,  # 1 hour
        'connection_health': 300,  # 5 minutes
    }
}


# =============================================================================
# SYSTEM MONITORING CONSTANTS
# =============================================================================

MONITORING_CONSTANTS = {
    'intervals': {
        'watchdog_check': 60,       # 1 minute
        'memory_log': 300,          # 5 minutes
        'client_health': 300,       # 5 minutes
        'substrate_cleanup': 300,   # 5 minutes
        'metagraph_sync': 300,      # 5 minutes
    },
    'health_tracking': {
        'max_consecutive_errors': 3,
        'task_timeout_multiplier': 1.5,
    }
} 