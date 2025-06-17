import asyncio
import traceback
import gc
import os
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
import uuid
import numpy as np
import xarray as xr
import pandas as pd
import xskillscore as xs

from fiber.logging_utils import get_logger
from typing import TYPE_CHECKING, Any, Optional, Dict, List, Tuple
if TYPE_CHECKING:
    from .weather_task import WeatherTask 

from .processing.weather_logic import _request_fresh_token
from .utils.remote_access import open_verified_remote_zarr_dataset

from .weather_scoring.metrics import (
    calculate_bias_corrected_forecast,
    calculate_mse_skill_score,
    calculate_acc,
    perform_sanity_checks,
    _calculate_latitude_weights,
)

logger = get_logger(__name__)

# Constants for Day-1 Scoring
# DEFAULT_DAY1_ALPHA_SKILL = 0.5
# DEFAULT_DAY1_BETA_ACC = 0.5
# DEFAULT_DAY1_PATTERN_CORR_THRESHOLD = 0.3
# DEFAULT_DAY1_ACC_LOWER_BOUND = 0.6 # For a specific lead like +12h

async def evaluate_miner_forecast_day1(
    task_instance: 'WeatherTask',
    miner_response_db_record: Dict,
    gfs_analysis_data_for_run: xr.Dataset,
    gfs_reference_forecast_for_run: xr.Dataset,
    era5_climatology: xr.Dataset,
    day1_scoring_config: Dict,
    run_gfs_init_time: datetime
) -> Dict:
    """
    Performs Day-1 scoring for a single miner's forecast.
    Uses GFS analysis as truth and GFS forecast as reference.
    Calculates bias-corrected skill scores and ACC for specified variables and lead times.
    """
    response_id = miner_response_db_record['id']
    miner_hotkey = miner_response_db_record['miner_hotkey']
    job_id = miner_response_db_record.get('job_id')
    
    # This is a placeholder for the file-based implementation
    # For now, we pass the datasets in memory.
    # This will be changed to pass file paths.
    gfs_analysis_ds_path = None
    gfs_reference_ds_path = None
    era5_climatology_ds_path = None

    # This will be replaced by a call to the executor.
    # For now, we call the async function directly.
    return await _execute_scoring_for_miner_process(
        miner_response_db_record=miner_response_db_record,
        gfs_analysis_ds=gfs_analysis_data_for_run,
        gfs_reference_ds=gfs_reference_forecast_for_run,
        era5_climatology_ds=era5_climatology,
        day1_scoring_config=day1_scoring_config,
        run_gfs_init_time=run_gfs_init_time,
        task_config=task_instance.config,
        miner_hotkey=miner_hotkey,
        job_id=job_id,
        db_manager_config=task_instance.db_manager.config
    )

async def _execute_scoring_for_miner_process(
    miner_response_db_record: Dict,
    gfs_analysis_ds_path: str,
    gfs_reference_ds_path: str,
    era5_climatology_ds_path: str,
    day1_scoring_config: Dict,
    run_gfs_init_time: datetime,
    task_config: Dict, # General task config, from task_instance.config
    miner_hotkey: str,
    job_id: str,
    db_manager_config: Dict, # Config to create a new db manager
) -> Dict:
    """
    Process-safe version of the scoring logic.
    Loads data from disk, performs scoring, and returns results.
    Can be run in a separate process.
    """
    # Load datasets from paths
    gfs_analysis_data_for_run = xr.open_dataset(gfs_analysis_ds_path, engine="netcdf4")
    gfs_reference_forecast_for_run = xr.open_dataset(gfs_reference_ds_path, engine="netcdf4")
    era5_climatology = xr.open_dataset(era5_climatology_ds_path, engine="netcdf4")
