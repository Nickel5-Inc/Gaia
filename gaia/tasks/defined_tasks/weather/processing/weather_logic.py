"""
Weather logic compatibility module.

This module provides backward compatibility by importing functions from the new 
async/dask-based processing modules where available, and falling back to legacy 
implementations where needed.

This is part of the migration from multiprocessing to async/dask-based processing.
"""

# Import from new async processing modules where available
try:
    from ..validator.scoring import _trigger_initial_scoring, _update_run_status
except ImportError:
    # Fallback to legacy implementations
    from .weather_logic_legacy import _trigger_initial_scoring, _update_run_status

try:
    from ..scoring.day1 import _request_fresh_token
except ImportError:
    # Fallback to legacy implementation
    from .weather_logic_legacy import _request_fresh_token

# These functions are still only available in legacy module
from .weather_logic_legacy import (
    _trigger_final_scoring,
    update_job_status,
    verify_miner_response,
)

# Re-export all functions for backward compatibility
__all__ = [
    "_trigger_initial_scoring",
    "_update_run_status", 
    "_request_fresh_token",
    "_trigger_final_scoring",
    "update_job_status",
    "verify_miner_response",
] 