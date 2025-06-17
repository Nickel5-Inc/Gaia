"""
Miner Communication module for the Gaia validator.

This module contains utilities for communicating with miners,
including custom serialization and communication helpers.
"""

import base64
import logging
import pandas as pd
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class MinerCommunicator:
    """
    Handles communication utilities for miner interactions.
    
    Provides serialization, communication helpers, and other utilities
    needed for validator-miner communication.
    """
    
    def __init__(self, validator_instance):
        """Initialize the miner communicator."""
        self.validator = validator_instance

    def custom_serializer(self, obj: Any) -> Any:
        """Custom JSON serializer for handling datetime objects and bytes."""
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return {
                "_type": "bytes",
                "encoding": "base64",
                "data": base64.b64encode(obj).decode("ascii"),
            }
        raise TypeError(f"Type {type(obj)} not serializable")