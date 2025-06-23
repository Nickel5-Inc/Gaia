"""
Configuration Management for Gaia Validator
==========================================

This module provides clean, centralized configuration management for the validator.
All configuration loading, validation, and constants are organized here.
"""

from .settings import (
    load_validator_config,
    get_network_config,
    get_database_config,
    get_memory_config,
    get_task_config
)

from .constants import (
    DEFAULT_TIMEOUTS,
    MEMORY_THRESHOLDS,
    TASK_WEIGHTS_SCHEDULE,
    NETWORK_CONSTANTS
)

__all__ = [
    'load_validator_config',
    'get_network_config', 
    'get_database_config',
    'get_memory_config',
    'get_task_config',
    'DEFAULT_TIMEOUTS',
    'MEMORY_THRESHOLDS', 
    'TASK_WEIGHTS_SCHEDULE',
    'NETWORK_CONSTANTS'
] 