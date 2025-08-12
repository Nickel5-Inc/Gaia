import os
import torch
import numpy as np
import traceback
import asyncio
import tempfile
from fiber.logging_utils import get_logger
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any, List, Tuple, Union
import pandas as pd

# Geomagnetic and soil moisture base models and processors disabled
from huggingface_hub import hf_hub_download

logger = get_logger(__name__)


class BaseModelEvaluator:
    """
    Handles initialization and execution of baseline models for scoring comparison.

    This class is responsible for:
    1. Weather task baseline scoring (geomagnetic and soil moisture disabled)
    2. Providing baseline scores to compare against miner performance for weather task only

    Geomagnetic and soil moisture baseline functionality has been removed.
    """

    def __init__(self, db_manager=None, test_mode: bool = False):
        """
        Initialize the BaseModelEvaluator for weather task only.

        Args:
            db_manager: Database manager for storing/retrieving predictions
            test_mode: If True, runs in test mode with limited resources
        """
        self.test_mode = test_mode
        self.db_manager = db_manager
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Geomagnetic and soil moisture models disabled
        logger.debug(
            f"BaseModelEvaluator ready (device={self.device}, weather-only)"
        )

    # Geomagnetic and soil moisture helper methods removed

    async def initialize_models(self):
        """Initialize baseline models (only weather task enabled)."""
        try:
            # Only weather task is active - no baseline models needed for weather currently
            logger.debug("Baseline model initialization skipped (weather-only)")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize baseline models: {e}")
            logger.error(traceback.format_exc())
            return False

    # All geomagnetic and soil moisture baseline model methods removed
    # Future weather baseline functionality can be added here if needed
