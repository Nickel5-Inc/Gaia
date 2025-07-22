"""
JSON sanitization utilities for weather scoring data.

PostgreSQL's JSON type doesn't accept infinity or NaN values, so we need to 
sanitize these values before database insertion.
"""

import json
import math
import numpy as np
from typing import Any, Dict, List, Union
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


def sanitize_for_json(obj: Any) -> Any:
    """
    Recursively sanitize an object to replace infinity and NaN values with null.
    This ensures the resulting object can be safely serialized to JSON for PostgreSQL.
    
    Args:
        obj: The object to sanitize (can be dict, list, or scalar)
        
    Returns:
        Sanitized object with infinity/NaN values replaced with None
    """
    if isinstance(obj, dict):
        return {key: sanitize_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, (float, np.floating)):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return float(obj)  # Convert numpy floats to Python floats
    elif isinstance(obj, (np.integer,)):
        return int(obj)  # Convert numpy ints to Python ints
    elif hasattr(obj, 'item'):  # numpy scalar
        val = obj.item()
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return None
        return val
    else:
        return obj


def safe_json_dumps(obj: Any, **kwargs) -> str:
    """
    Safely serialize an object to JSON string, replacing infinity/NaN values with null.
    
    Args:
        obj: The object to serialize
        **kwargs: Additional arguments passed to json.dumps
        
    Returns:
        JSON string with infinity/NaN values replaced with null
    """
    # Sanitize the object first
    sanitized_obj = sanitize_for_json(obj)
    
    # Use default=str as fallback for any remaining non-serializable objects
    kwargs.setdefault('default', str)
    
    try:
        return json.dumps(sanitized_obj, **kwargs)
    except (ValueError, TypeError) as e:
        logger.warning(f"JSON serialization failed even after sanitization: {e}")
        # Fallback: convert everything to strings
        return json.dumps(sanitized_obj, default=str)


def safe_json_dumps_for_db(obj: Any) -> str:
    """
    Convenience function for database JSON serialization with consistent parameters.
    
    Args:
        obj: The object to serialize for database storage
        
    Returns:
        JSON string safe for PostgreSQL JSON columns
    """
    return safe_json_dumps(obj, ensure_ascii=False, separators=(',', ':')) 