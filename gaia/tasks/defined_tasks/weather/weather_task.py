import asyncio
import traceback
from typing import Any, Dict, List, Optional, Union, Tuple, Set
from uuid import uuid4
from datetime import datetime, timezone, timedelta
import os
import sys
import importlib.util
from pydantic import Field, ConfigDict

# High-performance JSON operations for weather task
try:
    from gaia.utils.performance import dumps, loads
except ImportError:
    import json

    def dumps(obj, **kwargs):
        return json.dumps(obj, **kwargs)

    def loads(s):
        return json.loads(s)


from fiber.logging_utils import get_logger
from gaia.tasks.base.task import Task
from gaia.tasks.base.deterministic_job_id import DeterministicJobID
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.miner.database.miner_database_manager import MinerDatabaseManager
import uuid
import time
from pathlib import Path
import xarray as xr
import pickle
import base64
import jwt
import numpy as np
import torch
import fsspec
from kerchunk.hdf import SingleHdf5ToZarr
import pandas as pd
from cryptography.fernet import Fernet
from fiber.encrypted.validator import handshake
from fiber.encrypted.validator import client as vali_client
from sqlalchemy import text, bindparam, TEXT
import httpx
import gc
import shutil
import zarr
import numcodecs
from typing import Dict
import gzip
import tarfile
import io
import tempfile
import subprocess

import boto3  # For R2
from botocore.exceptions import (
    ClientError as BotoClientError,
)  # For R2 error handling, aliased to avoid conflict if other ClientError exists
from . import weather_http_utils

# Ensure blosc codec is available for zarr operations
try:
    import blosc
    import numcodecs

    # Force registration of blosc codec - correct way is to just import it
    import numcodecs.blosc

    # Verify the codec is available
    codec = numcodecs.registry.get_codec({"id": "blosc"})
    print(
        f"[WeatherTask] Blosc codec successfully imported and available. Version: {blosc.__version__}"
    )
except ImportError as e:
    print(
        f"[WeatherTask] Failed to import blosc codec: {e}. Zarr datasets using blosc compression may fail to open."
    )
except Exception as e:
    print(
        f"[WeatherTask] Failed to verify blosc codec availability: {e}. Zarr datasets using blosc compression may fail to open."
    )

from .utils.era5_api import fetch_era5_data
from .utils.gfs_api import fetch_gfs_analysis_data, fetch_gfs_data
from .utils.hashing import compute_verification_hash, compute_input_data_hash
from .utils.kerchunk_utils import generate_kerchunk_json_from_local_file
from .utils.data_prep import create_aurora_batch_from_gfs
from .schemas.weather_metadata import WeatherMetadata
from .schemas.weather_inputs import (
    WeatherInputs,
    WeatherForecastRequest,
    WeatherInputData,
    WeatherInitiateFetchData,
    WeatherGetInputStatusData,
    WeatherStartInferenceData,
)
from .schemas.weather_outputs import (
    WeatherOutputs,
    WeatherKerchunkResponseData,
    WeatherTaskStatus,
)
from .schemas.weather_outputs import (
    WeatherInitiateFetchResponse,
    WeatherGetInputStatusResponse,
    WeatherStartInferenceResponse,
)
from .schemas.weather_outputs import WeatherProgressUpdate, WeatherFileLocation
from .weather_scoring.metrics import calculate_rmse
from .processing.weather_miner_preprocessing import prepare_miner_batch_from_payload
from .processing.weather_logic import (
    _update_run_status,
    build_score_row,
    get_ground_truth_data,
    _trigger_initial_scoring,
    _request_fresh_token,
    verify_miner_response,
    get_job_by_gfs_init_time,
    update_job_status,
    update_job_paths,
    _trigger_final_scoring,
)
from .hardening_integration import WeatherTaskHardeningMixin
from .processing.weather_workers import (
    initial_scoring_worker,
    finalize_scores_worker,
    cleanup_worker,
    run_inference_background,
    fetch_and_hash_gfs_task,
    r2_cleanup_worker,
    poll_runpod_job_worker,
    weather_job_status_logger,
)
from gaia.tasks.defined_tasks.weather.utils.inference_class import (
    WeatherInferenceRunner,
)

logger = get_logger(__name__)
from aurora import Batch

DEFAULT_FORECAST_DIR_BG = Path("./miner_forecasts/")
MINER_FORECAST_DIR_BG = Path(os.getenv("MINER_FORECAST_DIR", DEFAULT_FORECAST_DIR_BG))
VALIDATOR_ENSEMBLE_DIR = Path("./validator_ensembles/")
MINER_FORECAST_DIR_BG.mkdir(parents=True, exist_ok=True)
VALIDATOR_ENSEMBLE_DIR.mkdir(parents=True, exist_ok=True)


def _load_config(self):
    """Loads configuration for the WeatherTask from environment variables."""
    logger.info("Loading WeatherTask configuration from environment variables...")
    config = {}

    def parse_int_list(env_var, default_list):
        val_str = os.getenv(env_var)
        if val_str:
            try:
                return [int(x.strip()) for x in val_str.split(",")]
            except ValueError:
                logger.warning(
                    f"Invalid format for {env_var}: '{val_str}'. Using default."
                )
        return default_list

    # Worker/Run Parameters
    config["max_concurrent_inferences"] = int(
        os.getenv("WEATHER_MAX_CONCURRENT_INFERENCES", "1")
    )
    config["inference_steps"] = int(os.getenv("WEATHER_INFERENCE_STEPS", "40"))
    config["forecast_step_hours"] = int(os.getenv("WEATHER_FORECAST_STEP_HOURS", "6"))
    config["forecast_duration_hours"] = (
        config["inference_steps"] * config["forecast_step_hours"]
    )

    # Scoring Parameters
    config["initial_scoring_lead_hours"] = parse_int_list(
        "WEATHER_INITIAL_SCORING_LEAD_HOURS", [6, 12]
    )  # Day 0.25, 0.5
    # Progressive daily scoring: score each day from 1-10 as ERA5 data becomes available
    config["final_scoring_lead_hours"] = parse_int_list(
        "WEATHER_FINAL_SCORING_LEAD_HOURS",
        [24, 48, 72, 96, 120, 144, 168, 192, 216, 240],
    )  # Days 1-10
    config["verification_wait_minutes"] = int(
        os.getenv("WEATHER_VERIFICATION_WAIT_MINUTES", "60")
    )

    # Optional scoring timeout configuration - disabled by default for long-running operations
    # Set WEATHER_SCORING_TIMEOUT_ENABLED=true to enable safety timeouts
    config["scoring_timeout_enabled"] = (
        os.getenv("WEATHER_SCORING_TIMEOUT_ENABLED", "false").lower() == "true"
    )
    config["scoring_timeout_safety_hours"] = float(
        os.getenv("WEATHER_SCORING_TIMEOUT_SAFETY_HOURS", "2.0")
    )  # Safety timeout in hours
    config["scoring_progress_log_interval"] = int(
        os.getenv("WEATHER_SCORING_PROGRESS_LOG_INTERVAL", "300")
    )  # Progress logging every 5 minutes
    config["verification_timeout_seconds"] = int(
        os.getenv("WEATHER_VERIFICATION_TIMEOUT_SECONDS", "3600")
    )
    # More frequent checking for progressive scoring (check every 30 minutes instead of 1 hour)
    config["final_scoring_check_interval_seconds"] = int(
        os.getenv("WEATHER_FINAL_SCORING_INTERVAL_S", "1800")
    )
    # Optimized for progressive scoring: reduce delay since we're scoring days individually
    config["era5_delay_days"] = int(
        os.getenv("WEATHER_ERA5_DELAY_DAYS", "4")
    )  # Reduced from 5 to 4 days
    config["era5_buffer_hours"] = int(os.getenv("WEATHER_ERA5_BUFFER_HOURS", "6"))
    # Smart retry backoff: avoid redundant API calls for known-unavailable ERA5 dates
    config["era5_retry_backoff_hours"] = int(
        os.getenv("WEATHER_ERA5_RETRY_BACKOFF_HOURS", "6")
    )  # Don't retry failed dates for 6 hours
    config["cleanup_check_interval_seconds"] = int(
        os.getenv("WEATHER_CLEANUP_INTERVAL_S", "21600")
    )  # 6 hours
    config["gfs_analysis_cache_dir"] = os.getenv(
        "WEATHER_GFS_CACHE_DIR", "./gfs_analysis_cache"
    )
    config["era5_cache_dir"] = os.getenv("WEATHER_ERA5_CACHE_DIR", "./era5_cache")
    config["gfs_cache_retention_days"] = int(
        os.getenv("WEATHER_GFS_CACHE_RETENTION_DAYS", "7")
    )
    config["era5_cache_retention_days"] = int(
        os.getenv("WEATHER_ERA5_CACHE_RETENTION_DAYS", "30")
    )
    # Progressive scoring optimization: enable more selective ERA5 data fetching
    config["progressive_era5_fetch"] = os.getenv(
        "WEATHER_PROGRESSIVE_ERA5_FETCH", "true"
    ).lower() in ["true", "1", "yes"]
    config["era5_fetch_chunk_hours"] = int(
        os.getenv("WEATHER_ERA5_FETCH_CHUNK_HOURS", "24")
    )  # Fetch daily chunks

    # Data transfer optimization: enable compressed miner-validator communication
    config["enable_compression"] = os.getenv(
        "WEATHER_ENABLE_COMPRESSION", "true"
    ).lower() in ["true", "1", "yes"]
    config["compression_level"] = int(
        os.getenv("WEATHER_COMPRESSION_LEVEL", "6")
    )  # gzip compression level (1-9)

    # Memory management thresholds and batch sizes for OOM prevention
    config["memory_cleanup_threshold_mb"] = int(
        os.getenv("WEATHER_MEMORY_CLEANUP_THRESHOLD_MB", "8000")
    )  # Trigger proactive cleanup
    config["memory_emergency_threshold_mb"] = int(
        os.getenv("WEATHER_MEMORY_EMERGENCY_THRESHOLD_MB", "15000")
    )  # Emergency warning threshold
    config["scoring_batch_size"] = int(
        os.getenv("WEATHER_SCORING_BATCH_SIZE", "20")
    )  # Day-1 scoring batch size
    config["era5_scoring_batch_size"] = int(
        os.getenv("WEATHER_ERA5_SCORING_BATCH_SIZE", "10")
    )  # ERA5 scoring batch size

    # Memory management configuration for handling many miners
    config["per_miner_cleanup_threshold"] = int(
        os.getenv("WEATHER_PER_MINER_CLEANUP_THRESHOLD", "15")
    )  # Switch to per-miner cleanup above this many miners
    config["era5_per_miner_cleanup_threshold"] = int(
        os.getenv("WEATHER_ERA5_PER_MINER_CLEANUP_THRESHOLD", "10")
    )  # ERA5 per-miner cleanup threshold

    # Partial recovery configuration
    config["partial_recovery_log_threshold"] = int(
        os.getenv("WEATHER_PARTIAL_RECOVERY_LOG_THRESHOLD", "25")
    )  # Log efficiency gains above this % completion
    config["efficient_recovery_threshold"] = int(
        os.getenv("WEATHER_EFFICIENT_RECOVERY_THRESHOLD", "50")
    )  # Consider "efficient" above this % completion
    config["ensemble_retention_days"] = int(
        os.getenv("WEATHER_ENSEMBLE_RETENTION_DAYS", "14")
    )
    config["db_run_retention_days"] = int(
        os.getenv("WEATHER_DB_RUN_RETENTION_DAYS", "90")
    )
    config["input_batch_retention_days"] = int(
        os.getenv("WEATHER_INPUT_BATCH_RETENTION_DAYS", "3")
    )  # New: retention for input batch pickle files
    config["run_hour_utc"] = int(os.getenv("WEATHER_RUN_HOUR_UTC", "18"))
    config["run_minute_utc"] = int(os.getenv("WEATHER_RUN_MINUTE_UTC", "0"))
    config["validator_hash_wait_minutes"] = int(
        os.getenv("WEATHER_VALIDATOR_HASH_WAIT_MINUTES", "10")
    )

    # --- R2 Cleanup Config (Credentials are handled by the Miner, not here) ---
    config["r2_cleanup_enabled"] = os.getenv(
        "WEATHER_R2_CLEANUP_ENABLED", "true"
    ).lower() in ["true", "1", "yes"]
    config["r2_cleanup_interval_seconds"] = int(
        os.getenv("WEATHER_R2_CLEANUP_INTERVAL_S", "1800")
    )  # 30 minutes - more frequent cleanup
    config["r2_max_inputs_to_keep"] = int(
        os.getenv("WEATHER_R2_MAX_INPUTS_TO_KEEP", "0")
    )  # Keep NO inputs (delete immediately)
    config["r2_max_forecasts_to_keep"] = int(
        os.getenv("WEATHER_R2_MAX_FORECASTS_TO_KEEP", "1")
    )  # Keep only 1 forecast per timestep

    config["miner_jwt_secret_key"] = os.getenv(
        "MINER_JWT_SECRET_KEY", "insecure_default_key_for_development_only"
    )
    config["jwt_algorithm"] = os.getenv("MINER_JWT_ALGORITHM", "HS256")
    config["access_token_expire_minutes"] = int(
        os.getenv("WEATHER_ACCESS_TOKEN_EXPIRE_MINUTES", "120")
    )

    config["era5_climatology_path"] = os.getenv(
        "WEATHER_ERA5_CLIMATOLOGY_PATH",
        "gs://weatherbench2/datasets/era5-hourly-climatology/1990-2019_6h_1440x721.zarr",
    )

    # Day-1 Scoring Specific Configurations
    default_day1_vars_levels = [
        {"name": "z", "level": 500, "standard_name": "geopotential"},
        {"name": "t", "level": 850, "standard_name": "temperature"},
        {"name": "2t", "level": None, "standard_name": "2m_temperature"},
        {"name": "msl", "level": None, "standard_name": "mean_sea_level_pressure"},
    ]
    try:
        day1_vars_levels_json = os.getenv("WEATHER_DAY1_VARIABLES_LEVELS_JSON")
        config["day1_variables_levels_to_score"] = (
            loads(day1_vars_levels_json)
            if day1_vars_levels_json
            else default_day1_vars_levels
        )
    except Exception:
        logger.warning(
            "Invalid JSON for WEATHER_DAY1_VARIABLES_LEVELS_JSON. Using default."
        )
        config["day1_variables_levels_to_score"] = default_day1_vars_levels

    default_day1_clim_bounds = {
        "2t": (180, 340),  # Kelvin
        "msl": (90000, 110000),  # Pascals
        "t850": (220, 320),  # Kelvin for 850hPa Temperature
        "z500": (
            45000,
            60000,
        ),  # m^2/s^2 for Geopotential at 500hPa (approx 4500-6000 gpm)
    }
    try:
        day1_clim_bounds_json = os.getenv("WEATHER_DAY1_CLIMATOLOGY_BOUNDS_JSON")
        config["day1_climatology_bounds"] = (
            loads(day1_clim_bounds_json)
            if day1_clim_bounds_json
            else default_day1_clim_bounds
        )
    except Exception:
        logger.warning(
            "Invalid JSON for WEATHER_DAY1_CLIMATOLOGY_BOUNDS_JSON. Using default."
        )
        config["day1_climatology_bounds"] = default_day1_clim_bounds

    config["day1_pattern_correlation_threshold"] = float(
        os.getenv("WEATHER_DAY1_PATTERN_CORR_THRESHOLD", "0.3")
    )
    config["day1_acc_lower_bound"] = float(
        os.getenv("WEATHER_DAY1_ACC_LOWER_BOUND", "0.6")
    )
    config["day1_alpha_skill"] = float(os.getenv("WEATHER_DAY1_ALPHA_SKILL", "0.6"))
    config["day1_beta_acc"] = float(os.getenv("WEATHER_DAY1_BETA_ACC", "0.4"))

    # Quality Control Config - I need to tune these values
    config["day1_clone_penalty_gamma"] = float(
        os.getenv("WEATHER_DAY1_CLONE_PENALTY_GAMMA", "1.0")
    )
    default_clone_delta_thresholds = {
        "2t": 0.0025,  # (RMSE 0.1K)^2
        "msl": 400,  # (RMSE 100Pa or 1hPa)^2
        "z500": 100,  # (RMSE 100 m^2/s^2)^2 for geopotential
        "t850": 0.04,  # (RMSE 0.5K)^2
    }
    try:
        clone_delta_json = os.getenv("WEATHER_DAY1_CLONE_DELTA_THRESHOLDS_JSON")
        config["day1_clone_delta_thresholds"] = (
            loads(clone_delta_json)
            if clone_delta_json
            else default_clone_delta_thresholds
        )
    except Exception:
        logger.warning(
            "Invalid JSON for WEATHER_DAY1_CLONE_DELTA_THRESHOLDS_JSON. Using default."
        )
        config["day1_clone_delta_thresholds"] = default_clone_delta_thresholds

    config["weather_score_day1_weight"] = float(
        os.getenv("WEATHER_SCORE_DAY1_WEIGHT", "0.2")
    )
    config["weather_score_era5_weight"] = float(
        os.getenv("WEATHER_SCORE_ERA5_WEIGHT", "0.8")
    )
    config["weather_bonus_value_add"] = float(
        os.getenv("WEATHER_BONUS_VALUE_ADD", "0.05")
    )  # Value to add for winning a bonus category

    # --- RunPod Specific Config ---
    config["runpod_poll_interval_seconds"] = int(
        os.getenv("RUNPOD_POLL_INTERVAL_SECONDS", "10")
    )
    config["runpod_max_poll_attempts"] = int(
        os.getenv("RUNPOD_MAX_POLL_ATTEMPTS", "900")
    )  # 900 attempts * 10s/attempt = 9000s = 150 minutes = 2.5 hours
    config["runpod_download_endpoint_suffix"] = os.getenv(
        "RUNPOD_DOWNLOAD_ENDPOINT_SUFFIX", "run/download_step"
    )  # e.g., "run/download_step"
    config["runpod_upload_input_suffix"] = os.getenv(
        "RUNPOD_UPLOAD_INPUT_SUFFIX", "run/upload_input"
    )  # e.g., "run/upload_input"

    # Default to 6 hours

    logger.info(f"WeatherTask configuration loaded: {config}")
    return config


class WeatherTask(Task, WeatherTaskHardeningMixin):
    db_manager: Union[ValidatorDatabaseManager, MinerDatabaseManager]
    node_type: str = Field(default="validator")
    test_mode: bool = Field(default=False)
    era5_climatology_ds: Optional[xr.Dataset] = Field(default=None, exclude=True)

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    def __init__(
        self,
        db_manager=None,
        node_type=None,
        test_mode=False,
        keypair=None,
        inference_service_url: Optional[str] = None,
        runpod_api_key: Optional[
            str
        ] = None,  # For RunPod API Key (if CREDENTIAL was set)
        r2_config: Optional[Dict[str, str]] = None,
        **data,
    ):
        loaded_config = self._load_config()

        # Merge keyword arguments into the loaded config, allowing overrides
        loaded_config.update(data)

        super_data = {
            "name": "WeatherTask",
            "description": "Weather forecast generation and verification task",
            "task_type": "atomic",
            "metadata": WeatherMetadata(),
            "db_manager": db_manager,
            "node_type": node_type,
            "test_mode": test_mode,
            **data,
        }
        super().__init__(**super_data)

        self.keypair = keypair
        self.db_manager = db_manager
        self.node_type = node_type
        self.test_mode = test_mode
        self.config = loaded_config
        self.r2_config = r2_config  # Store the dedicated R2 config
        self.validator = data.get("validator")
        self.era5_climatology_ds = None
        self.inference_service_url = inference_service_url  # Store the URL
        self.runpod_api_key = (
            runpod_api_key  # Store RunPod API key (will be None if not provided)
        )
        logger.info(
            f"WeatherTask initialized. RunPod API Key is {'SET' if self.runpod_api_key else 'NOT SET'}. First 5 chars: {self.runpod_api_key[:5] if self.runpod_api_key else 'N/A'}"
        )

        self.initial_scoring_queue = asyncio.Queue()
        self.initial_scoring_worker_running = False
        self.initial_scoring_workers = []
        self.final_scoring_worker_running = False
        self.final_scoring_workers = []
        self.cleanup_worker_running = False
        self.cleanup_workers = []
        self.r2_cleanup_worker_running = False  # For R2 cleanup
        self.r2_cleanup_workers = []  # For R2 cleanup
        self.job_status_logger_running = False  # For job status logging
        self.job_status_logger_workers = []  # For job status logging

        self.test_mode_run_scored_event = asyncio.Event()
        self.last_test_mode_run_id = None

        self.gpu_semaphore = asyncio.Semaphore(
            self.config.get("max_concurrent_inferences", 1)
        )

        era5_scoring_concurrency = int(
            os.getenv("WEATHER_VALIDATOR_ERA5_SCORING_CONCURRENCY", "4")
        )
        self.era5_scoring_semaphore = asyncio.Semaphore(era5_scoring_concurrency)
        logger.info(
            f"ERA5 scoring concurrency for validator set to: {era5_scoring_concurrency}"
        )

        # Configure file serving mode for miners
        if self.node_type == "miner":
            file_serving_mode = os.getenv("WEATHER_FILE_SERVING_MODE", "local").lower()
            if file_serving_mode not in ["local", "r2_proxy"]:
                logger.warning(
                    f"Invalid WEATHER_FILE_SERVING_MODE: {file_serving_mode}. Defaulting to 'local'."
                )
                file_serving_mode = "local"

            self.config["file_serving_mode"] = file_serving_mode
            logger.info(f"Weather file serving mode: {file_serving_mode}")

            if file_serving_mode == "local":
                logger.info(
                    "Files will be downloaded from R2 to local storage and served via HTTP/zarr"
                )
            else:
                logger.info(
                    "Files will be served by proxying requests to R2 (no local storage)"
                )

        self.inference_runner = None
        if self.node_type == "miner":
            try:
                # Inference type from .env or defaults
                self.config["weather_inference_type"] = os.getenv(
                    "WEATHER_INFERENCE_TYPE", "local_model"
                ).lower()
                load_local_model_flag = True

                if self.config["weather_inference_type"] == "http_service":
                    logger.info(
                        f"WeatherTask.__init__: Configured to use HTTP Inference Service. Provided URL: '{self.inference_service_url}'"
                    )  # Log the provided URL
                    if not self.inference_service_url:
                        logger.error(
                            "WeatherTask.__init__: HTTP Inference Service selected, but no valid URL provided to WeatherTask constructor. Cannot use HTTP inference."
                        )
                        # Fallback or error needed? For now, it will try to load local if URL is missing.
                        # To prevent local model loading if HTTP was intended but URL is missing:
                        load_local_model_flag = (
                            True  # Fallback to trying local model if URL is bad.
                        )
                        self.config["weather_inference_type"] = (
                            "local_model"  # Explicitly set to local_model
                        )
                        logger.warning(
                            "WeatherTask.__init__: Falling back to 'local_model' due to missing/invalid HTTP service URL."
                        )
                    else:
                        logger.info(
                            "WeatherTask.__init__: HTTP Inference Service URL is present. Will proceed with HTTP client logic. Local model will NOT be loaded."
                        )
                        load_local_model_flag = (
                            False  # Don't load local model if using HTTP service
                        )

                elif self.config["weather_inference_type"] == "azure_foundry":
                    logger.info(
                        "Configured to use Azure Foundry for inference. Local model will not be loaded."
                    )
                    load_local_model_flag = False
                elif self.config["weather_inference_type"] == "local_model":
                    logger.info("Configured to use local model for inference.")
                else:
                    logger.warning(
                        f"Invalid WEATHER_INFERENCE_TYPE: '{self.config['weather_inference_type']}'. Defaulting to 'local_model'."
                    )
                    self.config["weather_inference_type"] = "local_model"

                device = "cuda" if torch.cuda.is_available() else "cpu"
                if load_local_model_flag:
                    self.inference_runner = WeatherInferenceRunner(
                        device=device, load_local_model=True
                    )
                    if self.inference_runner.model is not None:
                        logger.info(
                            f"Initialized Miner components (Local Inference Runner on {device}, model loaded)."
                        )
                    else:
                        logger.error(
                            f"Initialized Miner components (Local Inference Runner on {device}, BUT MODEL FAILED TO LOAD)."
                        )
                elif (
                    self.config["weather_inference_type"] == "http_service"
                    and self.inference_service_url
                ):
                    logger.info(
                        f"Miner configured for HTTP inference. Local model not loaded. Inference runner will be None."
                    )
                    self.inference_runner = (
                        None  # Explicitly None if using HTTP service
                    )
                else:
                    logger.info(
                        f"Miner components initialized. Local model not loaded (inference type: {self.config['weather_inference_type']}). Inference runner will be None."
                    )
                    self.inference_runner = (
                        None  # Explicitly None if local model is not to be loaded
                    )

            except Exception as e:
                logger.error(
                    f"Failed to initialize WeatherInferenceRunner or set inference type: {e}",
                    exc_info=True,
                )
                self.inference_runner = None
                self.config["weather_inference_type"] = "local"  # Fallback
        else:  # Validator
            logger.info("Initialized validator components for WeatherTask")

        # Add progress tracking
        self.progress_callbacks: List[callable] = []
        self.current_operations: Dict[str, WeatherProgressUpdate] = {}

        # Initialize async processing configuration and logging
        try:
            from .utils.async_processing import log_async_processing_summary

            log_async_processing_summary()
        except Exception as async_config_error:
            logger.warning(
                f"Could not load async processing summary: {async_config_error}"
            )
            logger.info(
                "Weather task async processing optimizations may not be available"
            )

        # Initialize hardening system
        try:
            self.__init_hardening__()
            logger.debug("Weather task hardening system initialized")
        except Exception as hardening_error:
            logger.warning(f"Could not initialize hardening system: {hardening_error}")
            logger.info(
                "Weather task will run without enhanced state consistency checks"
            )

    _load_config = _load_config

    ############################################################
    # RunPodCTL Helpers (Synchronous, for WeatherTask execution path)
    ############################################################
    def _run_weather_task_subprocess_command(
        self,
        command_parts: List[str],
        timeout_seconds: int = 300,
        extract_code_and_terminate: bool = False,
        extract_code_prefix: str = "Code is: ",
    ) -> Tuple[bool, str, str]:
        """
        Runs a subprocess command.
        If extract_code_and_terminate is True, it reads stdout line by line for a specific prefix,
        extracts the value, terminates the process, and returns the code.
        Otherwise, it waits for the command to complete or timeout using communicate().
        Returns a tuple: (success_boolean, stdout_str, stderr_str).
        """
        try:
            full_command = " ".join(command_parts)
            logger.info(
                f"Executing command: {full_command} with timeout {timeout_seconds}s. Extract and terminate: {extract_code_and_terminate}"
            )

            current_env = os.environ.copy()
            logger.debug(f"Subprocess PATH: {current_env.get('PATH', 'N/A')}")

            process = subprocess.Popen(
                command_parts,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
                env=current_env,
            )

            if extract_code_and_terminate:
                stdout_lines = []
                stderr_lines = []
                extracted_code = ""
                code_found = False

                start_time = time.time()
                while time.time() - start_time < timeout_seconds:
                    if process.poll() is not None:
                        logger.warning(
                            f"Process '{full_command}' terminated prematurely with code {process.returncode} while waiting for code line."
                        )
                        break

                    if process.stdout:
                        line = process.stdout.readline()
                        if line:
                            stdout_lines.append(line)
                            # MODIFIED CONDITION: If the stripped line from stdout is non-empty, consider it the code.
                            line_stripped = line.strip()
                            if line_stripped:
                                extracted_code = line_stripped
                                code_found = True
                                logger.info(
                                    f"Extracted code from stdout: '{extracted_code}' (from raw line: '{line.strip()}')"
                                )
                                break
                        else:
                            logger.debug(
                                f"End of stdout stream reached for '{full_command}' before code found or timeout."
                            )
                            break
                    time.sleep(0.01)

                if code_found:
                    logger.info(
                        f"Code '{extracted_code}' extracted. Terminating process '{full_command}'."
                    )
                    process.terminate()
                    try:
                        remaining_stdout, final_stderr = process.communicate(timeout=5)
                        if remaining_stdout:
                            stdout_lines.append(remaining_stdout)
                        if final_stderr:
                            stderr_lines.append(final_stderr)
                    except subprocess.TimeoutExpired:
                        logger.warning(
                            f"Timeout waiting for process '{full_command}' to terminate/communicate after code extraction."
                        )
                        process.kill()
                        try:
                            remaining_stdout_kill, final_stderr_kill = (
                                process.communicate()
                            )
                            if remaining_stdout_kill:
                                stdout_lines.append(remaining_stdout_kill)
                            if final_stderr_kill:
                                stderr_lines.append(final_stderr_kill)
                        except Exception as e_kill_comm:
                            logger.warning(
                                f"Exception during final communicate after kill for '{full_command}': {e_kill_comm}"
                            )
                    except Exception as e_comm:
                        logger.warning(
                            f"Exception during final communicate for '{full_command}' after code extraction: {e_comm}"
                        )

                    full_stdout_str = "".join(stdout_lines).strip()
                    full_stderr_str = "".join(stderr_lines).strip()
                    logger.debug(
                        f"Command '{full_command}' (extract mode) successful. Code: {extracted_code}. Full Stdout (first 500 chars): {full_stdout_str[:500]}. Full Stderr (first 500 chars): {full_stderr_str[:500]}."
                    )
                    # Verify if "Code is: <extracted_code>" is in stderr for confirmation
                    # This uses the extract_code_prefix passed to the function.
                    if f"{extract_code_prefix}{extracted_code}" in full_stderr_str:
                        logger.info(
                            f"Confirmation: Extracted code '{extracted_code}' found in stderr output with prefix '{extract_code_prefix}'."
                        )
                    else:
                        logger.warning(
                            f"Verification: Extracted code '{extracted_code}' from stdout NOT found in stderr output (stderr: '{full_stderr_str[:200]}...') with prefix '{extract_code_prefix}'. This might be okay if stdout is the sole reliable source for the code itself."
                        )

                    full_stdout_log = "".join(stdout_lines).strip()
                    return True, extracted_code, full_stderr_str
                else:
                    if process.poll() is None:
                        process.kill()
                    try:
                        remaining_stdout, remaining_stderr = process.communicate(
                            timeout=5
                        )
                        if remaining_stdout:
                            stdout_lines.append(remaining_stdout)
                        if remaining_stderr:
                            stderr_lines.append(remaining_stderr)
                    except Exception:
                        pass

                    full_stdout_str = "".join(stdout_lines).strip()
                    full_stderr_str = "".join(stderr_lines).strip()
                    logger.error(
                        f"Command '{full_command}' (extract mode) failed: Code '{extract_code_prefix}...' not found or timeout. Full Stdout (first 500 chars): {full_stdout_str[:500]}. Full Stderr (first 500 chars): {full_stderr_str[:500]}."
                    )
                    return (
                        False,
                        full_stdout_str,
                        f"Code not found or timeout. Partial stderr: {full_stderr_str}",
                    )

            else:  # Original behavior: wait for process to complete
                try:
                    stdout, stderr = process.communicate(timeout=timeout_seconds)
                except subprocess.TimeoutExpired:
                    process.kill()
                    stdout_on_timeout, stderr_on_timeout = process.communicate()
                    logger.error(
                        f"Command '{full_command}' timed out after {timeout_seconds} seconds."
                    )
                    s_out = stdout_on_timeout.strip() if stdout_on_timeout else ""
                    s_err = stderr_on_timeout.strip() if stderr_on_timeout else ""
                    logger.debug(f"Stdout on timeout (first 500 chars): {s_out[:500]}")
                    logger.debug(f"Stderr on timeout (first 500 chars): {s_err[:500]}")
                    return False, s_out, f"Command timed out. Partial stderr: {s_err}"

                max_log_len = 1000
                stripped_stdout = stdout.strip() if stdout else ""
                stripped_stderr = stderr.strip() if stderr else ""

                logger.debug(
                    f"Command '{full_command}' completed. Return code: {process.returncode}"
                )
                logger.debug(
                    f"Stdout (len: {len(stripped_stdout)}): {stripped_stdout[:max_log_len]}{'...' if len(stripped_stdout) > max_log_len else ''}"
                )
                logger.debug(
                    f"Stderr (len: {len(stripped_stderr)}): {stripped_stderr[:max_log_len]}{'...' if len(stripped_stderr) > max_log_len else ''}"
                )

                if process.returncode == 0:
                    logger.info(f"Command '{full_command}' successful.")
                    return True, stripped_stdout, stripped_stderr
                else:
                    logger.error(
                        f"Command '{full_command}' failed with return code {process.returncode}."
                    )
                    return False, stripped_stdout, stripped_stderr

        except FileNotFoundError:
            logger.error(
                f"Command not found: {command_parts[0]}. Ensure it is installed and in PATH."
            )
            return False, "", f"Command not found: {command_parts[0]}"
        except Exception as e:
            # Corrected f-string for the exception logging
            logger.error(
                f"An error occurred while running command '{' '.join(command_parts)}': {e}",
                exc_info=True,
            )
            return False, "", str(e)

    async def _get_r2_s3_client(self) -> Optional[boto3.client]:
        """
        Initializes and returns a boto3 S3 client configured for R2.
        Credentials are now sourced from self.r2_config, which is provided
        by the Miner during instantiation.
        Returns None if configuration is missing.
        """
        if not self.r2_config:
            logger.error(
                "R2 client configuration (self.r2_config) not provided to WeatherTask. Cannot create S3 client."
            )
            return None

        endpoint_url = self.r2_config.get("r2_endpoint_url")
        access_key_id = self.r2_config.get("r2_access_key_id")
        secret_access_key = self.r2_config.get("r2_secret_access_key")
        bucket_name = self.r2_config.get("r2_bucket_name")

        if not all([endpoint_url, access_key_id, secret_access_key, bucket_name]):
            logger.error(
                "R2 client configuration is incomplete in self.r2_config. Please ensure R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, and R2_BUCKET_NAME are set in the environment for the miner."
            )
            return None

        try:
            # Enhanced R2 client configuration for better reliability
            from botocore.config import Config

            # Configure connection pool and retry settings (without s3 parameter)
            config = Config(
                signature_version="s3v4",
                region_name="auto",  # R2 is region-less
                retries={"max_attempts": 5, "mode": "adaptive"},
                max_pool_connections=200,  # Increased from default 10
                connect_timeout=10,
                read_timeout=60,
            )

            s3_client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                config=config,
            )
            return s3_client
        except Exception as e:
            logger.error(f"Failed to create R2 S3 client: {e}", exc_info=True)
            return None

    async def _upload_input_to_r2(
        self, s3_client: boto3.client, job_id: str, initial_batch: "Batch"
    ) -> Optional[str]:
        """Uploads the initial batch pickle to R2 with robust retry logic."""
        if not self.r2_config or not self.r2_config.get("r2_bucket_name"):
            logger.error(
                f"[{job_id}] R2 bucket name not found in self.r2_config. Cannot upload input."
            )
            return None
        bucket_name = self.r2_config.get("r2_bucket_name")

        object_key = f"inputs/{job_id}/initial_batch.pkl"

        # Serialize the batch to bytes first
        try:
            with io.BytesIO() as f:
                pickle.dump(initial_batch, f)
                f.seek(0)
                data_bytes = f.getvalue()

            data_size_mb = len(data_bytes) / (1024 * 1024)
            logger.info(f"[{job_id}] Prepared batch for upload: {data_size_mb:.2f} MB")

            # Use robust upload with retry logic
            success = await self._robust_upload_bytes_to_r2(
                s3_client, bucket_name, object_key, data_bytes, job_id
            )

            if success:
                logger.info(
                    f"[{job_id}] Successfully uploaded initial batch to R2: s3://{bucket_name}/{object_key}"
                )
                return object_key
            else:
                logger.error(
                    f"[{job_id}] Failed to upload initial batch to R2 after retries"
                )
                return None

        except Exception as e:
            logger.error(
                f"[{job_id}] Error preparing batch for upload: {e}", exc_info=True
            )
            return None

    async def _robust_upload_bytes_to_r2(
        self,
        s3_client: boto3.client,
        bucket_name: str,
        object_key: str,
        data_bytes: bytes,
        job_id: str,
        max_retries: int = 5,
    ) -> bool:
        """
        Robust upload to R2 with exponential backoff retry logic.
        Handles multipart upload failures gracefully.
        """
        base_delay = 2.0  # seconds

        for attempt in range(max_retries):
            try:
                # Use TransferConfig for better multipart handling
                from boto3.s3.transfer import TransferConfig

                transfer_config = TransferConfig(
                    multipart_threshold=1024 * 1024 * 64,  # 64MB
                    multipart_chunksize=1024 * 1024 * 8,  # 8MB chunks
                    max_concurrency=5,  # Limit concurrency to prevent connection pool exhaustion
                    use_threads=True,
                    max_bandwidth=None,
                )

                # Upload with transfer config
                with io.BytesIO(data_bytes) as f:
                    await asyncio.to_thread(
                        s3_client.upload_fileobj,
                        f,
                        bucket_name,
                        object_key,
                        Config=transfer_config,
                    )

                # Verify upload by checking object existence
                try:
                    await asyncio.to_thread(
                        s3_client.head_object, Bucket=bucket_name, Key=object_key
                    )
                    logger.info(
                        f"[{job_id}] Upload verified: s3://{bucket_name}/{object_key}"
                    )
                    return True
                except Exception as verify_error:
                    logger.warning(
                        f"[{job_id}] Upload verification failed: {verify_error}"
                    )
                    # Continue to retry logic

            except Exception as e:
                error_msg = f"Attempt {attempt + 1}/{max_retries} failed: {type(e).__name__}: {str(e)}"

                if attempt < max_retries - 1:
                    delay = base_delay * (2**attempt)  # Exponential backoff
                    logger.warning(
                        f"[{job_id}] {error_msg}. Retrying in {delay:.1f}s..."
                    )
                    await asyncio.sleep(delay)

                    # Try to clean up any partial multipart uploads
                    try:
                        await self._cleanup_failed_multipart_uploads(
                            s3_client, bucket_name, object_key, job_id
                        )
                    except Exception as cleanup_error:
                        logger.debug(
                            f"[{job_id}] Cleanup error (non-critical): {cleanup_error}"
                        )

                    continue
                else:
                    logger.error(
                        f"[{job_id}] All {max_retries} upload attempts failed. Final error: {e}"
                    )
                    return False

        return False

    async def _cleanup_failed_multipart_uploads(
        self, s3_client: boto3.client, bucket_name: str, object_key: str, job_id: str
    ):
        """Clean up any failed multipart uploads for the given object key."""
        try:
            # List multipart uploads
            response = await asyncio.to_thread(
                s3_client.list_multipart_uploads, Bucket=bucket_name, Prefix=object_key
            )

            if "Uploads" in response:
                for upload in response["Uploads"]:
                    upload_id = upload["UploadId"]
                    logger.info(
                        f"[{job_id}] Aborting failed multipart upload: {upload_id}"
                    )

                    await asyncio.to_thread(
                        s3_client.abort_multipart_upload,
                        Bucket=bucket_name,
                        Key=object_key,
                        UploadId=upload_id,
                    )

        except Exception as e:
            logger.debug(f"[{job_id}] Error during multipart cleanup: {e}")
            # This is non-critical, so we don't re-raise
            return None

    async def _robust_upload_bytes_to_r2(
        self,
        s3_client: boto3.client,
        bucket_name: str,
        object_key: str,
        data_bytes: bytes,
        job_id: str,
        max_retries: int = 5,
    ) -> bool:
        """
        Robust upload to R2 with exponential backoff retry logic.
        Handles multipart upload failures gracefully.
        """
        base_delay = 2.0  # seconds

        for attempt in range(max_retries):
            try:
                # Use TransferConfig for better multipart handling
                from boto3.s3.transfer import TransferConfig

                transfer_config = TransferConfig(
                    multipart_threshold=1024 * 1024 * 64,  # 64MB
                    multipart_chunksize=1024 * 1024 * 8,  # 8MB chunks
                    max_concurrency=5,  # Limit concurrency to prevent connection pool exhaustion
                    use_threads=True,
                    max_bandwidth=None,
                )

                # Upload with transfer config
                with io.BytesIO(data_bytes) as f:
                    await asyncio.to_thread(
                        s3_client.upload_fileobj,
                        f,
                        bucket_name,
                        object_key,
                        Config=transfer_config,
                    )

                # Verify upload by checking object existence
                try:
                    await asyncio.to_thread(
                        s3_client.head_object, Bucket=bucket_name, Key=object_key
                    )
                    logger.info(
                        f"[{job_id}] Upload verified: s3://{bucket_name}/{object_key}"
                    )
                    return True
                except Exception as verify_error:
                    logger.warning(
                        f"[{job_id}] Upload verification failed: {verify_error}"
                    )
                    # Continue to retry logic

            except Exception as e:
                error_msg = f"Attempt {attempt + 1}/{max_retries} failed: {type(e).__name__}: {str(e)}"

                if attempt < max_retries - 1:
                    delay = base_delay * (2**attempt)  # Exponential backoff
                    logger.warning(
                        f"[{job_id}] {error_msg}. Retrying in {delay:.1f}s..."
                    )
                    await asyncio.sleep(delay)

                    # Try to clean up any partial multipart uploads
                    try:
                        await self._cleanup_failed_multipart_uploads(
                            s3_client, bucket_name, object_key, job_id
                        )
                    except Exception as cleanup_error:
                        logger.debug(
                            f"[{job_id}] Cleanup error (non-critical): {cleanup_error}"
                        )

                    continue
                else:
                    logger.error(
                        f"[{job_id}] All {max_retries} upload attempts failed. Final error: {e}"
                    )
                    return False

        return False

    async def _invoke_inference_service(
        self, job_id: str, input_r2_key: str
    ) -> Optional[str]:
        """
        Invokes the asynchronous inference service (e.g., RunPod) to START a job.
        Returns the runpod_job_id if successful, otherwise None. Does NOT poll.
        """
        if not self.inference_service_url or not self.runpod_api_key:
            logger.error(
                f"[{job_id}] Inference service URL or API key is not configured."
            )
            return None

        headers = {"Authorization": f"Bearer {self.runpod_api_key}"}

        # Ensure the URL is for the 'run' endpoint
        if not self.inference_service_url.endswith(("/run", "/run/")):
            logger.error(
                f"[{job_id}] Inference URL is not a 'run' endpoint: {self.inference_service_url}"
            )
            return None

        payload = {
            "input": {
                "action": "run_inference_from_r2",
                "input_r2_object_key": input_r2_key,
                "job_run_uuid": job_id,
            }
        }

        async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
            try:
                logger.info(
                    f"[{job_id}] Sending inference start request to {self.inference_service_url}"
                )
                response = await client.post(self.inference_service_url, json=payload)
                response.raise_for_status()

                start_data = response.json()
                runpod_job_id = start_data.get("id")

                if not runpod_job_id:
                    logger.error(
                        f"[{job_id}] Service did not return a RunPod job ID. Response: {start_data}"
                    )
                    return None

                logger.info(
                    f"[{job_id}] Job started on RunPod. RunPod Job ID: {runpod_job_id}"
                )
                return runpod_job_id

            except (httpx.HTTPStatusError, httpx.RequestError, Exception) as e:
                logger.error(
                    f"[{job_id}] Error starting inference job: {e}", exc_info=True
                )
                return None

    async def _run_inference_via_http_service(self, job_id: str) -> bool:
        """
        Runs weather inference using an external HTTP service (e.g., RunPod).
        This method should only be called on miner nodes.
        """
        if self.node_type != "miner":
            logger.error(
                f"[{job_id}] _run_inference_via_http_service called on {self.node_type} node. This method should only run on miners."
            )
            return False

        logger.info(f"[{job_id}] Starting HTTP service inference...")

        # Duplicate check and job validation for miners only
        try:
            # Get job details including GFS timestep
            job_check_query = """
                SELECT status, gfs_init_time_utc, runpod_job_id FROM weather_miner_jobs WHERE id = :job_id
            """
            job_check_details = await self.db_manager.fetch_one(
                job_check_query, {"job_id": job_id}
            )

            if not job_check_details:
                logger.error(
                    f"[{job_id}] Job not found during HTTP inference duplicate check. Aborting."
                )
                return False

            current_status = job_check_details["status"]
            gfs_init_time = job_check_details["gfs_init_time_utc"]
            existing_runpod_id = job_check_details["runpod_job_id"]

            logger.info(
                f"[{job_id}] DIAGNOSTIC - Job status: {current_status}, GFS time: {gfs_init_time}"
            )

            # Check if this job already has a RunPod job running
            if existing_runpod_id and current_status == "in_progress":
                logger.warning(
                    f"[{job_id}] Job already has RunPod ID {existing_runpod_id} and is in progress. Skipping duplicate HTTP inference."
                )
                return True  # Return True since inference is already running

            # Check if this job is already completed
            if current_status == "completed":
                logger.warning(
                    f"[{job_id}] Job already completed. Skipping duplicate HTTP inference."
                )
                return True

            # Check for other jobs with same timestep that are already in progress or completed
            if gfs_init_time:
                duplicate_check_query = """
                    SELECT id, status, runpod_job_id FROM weather_miner_jobs 
                    WHERE gfs_init_time_utc = :gfs_time 
                    AND id != :current_job_id 
                    AND status IN ('in_progress', 'completed')
                    ORDER BY id DESC LIMIT 1
                """
                duplicate_job = await self.db_manager.fetch_one(
                    duplicate_check_query,
                    {"gfs_time": gfs_init_time, "current_job_id": job_id},
                )

                if duplicate_job:
                    logger.warning(
                        f"[{job_id}] Found existing job {duplicate_job['id']} for same timestep {gfs_init_time} with status '{duplicate_job['status']}'. Aborting duplicate HTTP inference."
                    )
                    await update_job_status(
                        self,
                        job_id,
                        "skipped_duplicate",
                        f"Duplicate of job {duplicate_job['id']}",
                    )
                    return False

        except Exception as e:
            logger.error(
                f"[{job_id}] Error during HTTP inference duplicate check: {e}",
                exc_info=True,
            )
            # Continue with inference if duplicate check fails to avoid blocking valid jobs

        logger.info(
            f"[{job_id}] DIAGNOSTIC - HTTP SERVICE - Loading data from batch pickle path..."
        )
        s3_client = await self._get_r2_s3_client()
        if not s3_client:
            await update_job_status(
                self, job_id, "failed", "R2 client configuration invalid."
            )
            return False

        initial_batch = await self._load_batch_from_db(job_id)
        if not initial_batch:
            await update_job_status(
                self, job_id, "failed", "Failed to load initial data batch."
            )
            return False

        # DIAGNOSTIC: Log batch details to compare with local processing
        try:
            logger.info(f"[{job_id}] DIAGNOSTIC - HTTP SERVICE BATCH LOADED:")
            logger.info(f"[{job_id}]   - Type: {type(initial_batch)}")
            if hasattr(initial_batch, "metadata"):
                if hasattr(initial_batch.metadata, "time"):
                    logger.info(
                        f"[{job_id}]   - Metadata time: {initial_batch.metadata.time}"
                    )
                if hasattr(initial_batch.metadata, "lat"):
                    logger.info(
                        f"[{job_id}]   - Lat shape: {initial_batch.metadata.lat.shape}, range: [{float(initial_batch.metadata.lat.min()):.3f}, {float(initial_batch.metadata.lat.max()):.3f}]"
                    )
                if hasattr(initial_batch.metadata, "lon"):
                    logger.info(
                        f"[{job_id}]   - Lon shape: {initial_batch.metadata.lon.shape}, range: [{float(initial_batch.metadata.lon.min()):.3f}, {float(initial_batch.metadata.lon.max()):.3f}]"
                    )
                if hasattr(initial_batch.metadata, "atmos_levels"):
                    logger.info(
                        f"[{job_id}]   - Pressure levels: {initial_batch.metadata.atmos_levels}"
                    )

            if hasattr(initial_batch, "surf_vars"):
                logger.info(
                    f"[{job_id}]   - Surface variables: {list(initial_batch.surf_vars.keys())}"
                )
                for var_name, tensor in initial_batch.surf_vars.items():
                    var_min, var_max, var_mean = (
                        float(tensor.min()),
                        float(tensor.max()),
                        float(tensor.mean()),
                    )
                    logger.info(
                        f"[{job_id}]     - {var_name}: shape={tensor.shape}, range=[{var_min:.6f}, {var_max:.6f}], mean={var_mean:.6f}"
                    )

            if hasattr(initial_batch, "atmos_vars"):
                logger.info(
                    f"[{job_id}]   - Atmospheric variables: {list(initial_batch.atmos_vars.keys())}"
                )
                for var_name, tensor in initial_batch.atmos_vars.items():
                    var_min, var_max, var_mean = (
                        float(tensor.min()),
                        float(tensor.max()),
                        float(tensor.mean()),
                    )
                    logger.info(
                        f"[{job_id}]     - {var_name}: shape={tensor.shape}, range=[{var_min:.6f}, {var_max:.6f}], mean={var_mean:.6f}"
                    )

            if hasattr(initial_batch, "static_vars"):
                logger.info(
                    f"[{job_id}]   - Static variables: {list(initial_batch.static_vars.keys())}"
                )
                for var_name, tensor in initial_batch.static_vars.items():
                    var_min, var_max, var_mean = (
                        float(tensor.min()),
                        float(tensor.max()),
                        float(tensor.mean()),
                    )
                    logger.info(
                        f"[{job_id}]     - {var_name}: shape={tensor.shape}, range=[{var_min:.6f}, {var_max:.6f}], mean={var_mean:.6f}"
                    )

        except Exception as e:
            logger.warning(f"[{job_id}] Error during batch diagnostics: {e}")

        input_r2_key = await self._upload_input_to_r2(s3_client, job_id, initial_batch)
        if not input_r2_key:
            await update_job_status(
                self, job_id, "failed", "Failed to upload input to R2."
            )
            return False

        runpod_job_id = await self._invoke_inference_service(job_id, input_r2_key)
        if not runpod_job_id:
            await update_job_status(
                self, job_id, "failed", "Failed to start job on RunPod."
            )
            return False

        # Save the RunPod job ID and launch the poller
        try:
            query = "UPDATE weather_miner_jobs SET runpod_job_id = :runpod_id WHERE id = :job_id"
            await self.db_manager.execute(
                query, {"runpod_id": runpod_job_id, "job_id": job_id}
            )

            logger.info(
                f"[{job_id}] Saved RunPod ID {runpod_job_id}. Launching background poller."
            )
            # Track the polling task to prevent memory leaks
            if hasattr(self, "validator") and hasattr(
                self.validator, "create_tracked_task"
            ):
                self.validator.create_tracked_task(
                    poll_runpod_job_worker(self, job_id, runpod_job_id),
                    f"runpod_poll_{job_id}",
                )
            else:
                asyncio.create_task(poll_runpod_job_worker(self, job_id, runpod_job_id))
            return True
        except Exception as e:
            error_msg = f"DB error after starting RunPod job: {e}"
            logger.error(f"[{job_id}] {error_msg}", exc_info=True)
            await update_job_status(self, job_id, "failed", error_msg)
            # Should also attempt to cancel the runpod job here if possible
            return False

    ############################################################
    # Validator methods
    ############################################################

    def _load_era5_climatology_sync(
        self, climatology_path: str
    ) -> Optional[xr.Dataset]:
        """Synchronous helper to load ERA5 climatology with thread-safe blosc support."""
        import xarray as xr
        import os

        # THREADING FIX: Simple blosc codec registration in this executor thread
        def ensure_blosc_in_thread():
            """Ensure blosc codec is available in this thread context."""
            try:
                import numcodecs
                from numcodecs import Blosc

                # Create and register blosc codec in this thread
                blosc_codec = Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)

                # Register with numcodecs in this thread
                if hasattr(numcodecs, "register_codec"):
                    numcodecs.register_codec(blosc_codec)

                # Also register in the registry dictionary directly
                if hasattr(numcodecs.registry, "codec_registry"):
                    numcodecs.registry.codec_registry["blosc"] = blosc_codec

                logger.debug("ERA5 climatology: Blosc codec registered in thread")
                return True

            except Exception as e:
                logger.debug(
                    f"ERA5 climatology: Failed to register blosc in thread: {e}"
                )
                return False

        # Try multiple opening strategies with thread-safe codec registration
        strategies = [
            ("consolidated", {"consolidated": True}),
            ("non-consolidated", {"consolidated": False}),
            ("no-decode", {"consolidated": False, "decode_times": False}),
        ]

        for strategy_name, kwargs in strategies:
            try:
                logger.info(f"ERA5 climatology: Trying {strategy_name} strategy...")

                # Register blosc codec in this thread before each attempt
                ensure_blosc_in_thread()

                # Attempt to open with current strategy
                dataset = xr.open_zarr(climatology_path, **kwargs)

                if dataset is not None:
                    logger.info(
                        f"ERA5 climatology: Successfully loaded using {strategy_name} strategy"
                    )
                    return dataset

            except Exception as strategy_err:
                if strategy_name == "consolidated" and "codec not available" in str(
                    strategy_err
                ):
                    logger.info(
                        f"ERA5 climatology: {strategy_name} strategy unavailable (blosc codec not found), trying next strategy..."
                    )
                else:
                    logger.warning(
                        f"ERA5 climatology: {strategy_name} strategy failed: {strategy_err}"
                    )
                continue

        # Simple fallback: try fsspec mapper directly
        try:
            logger.info("ERA5 climatology: Attempting fsspec fallback...")
            import fsspec

            # Register blosc one more time
            ensure_blosc_in_thread()

            # Simple fsspec approach
            fs = fsspec.filesystem("gcs", token="anon")
            mapper = fs.get_mapper(climatology_path)
            dataset = xr.open_zarr(mapper, consolidated=False)

            if dataset is not None:
                logger.info(
                    "ERA5 climatology: Successfully loaded using fsspec fallback"
                )
                return dataset

        except Exception as fallback_err:
            logger.warning(f"ERA5 climatology: fsspec fallback failed: {fallback_err}")

        # If all strategies failed, return None
        logger.warning(
            "ERA5 climatology: All opening strategies failed - returning None"
        )
        return None

    async def _get_or_load_era5_climatology(self) -> Optional[xr.Dataset]:
        if self.era5_climatology_ds is None:
            climatology_path = self.config.get("era5_climatology_path")
            if climatology_path:
                try:
                    logger.info(f"Loading ERA5 climatology from: {climatology_path}")
                    self.era5_climatology_ds = await asyncio.to_thread(
                        self._load_era5_climatology_sync, climatology_path
                    )
                    if self.era5_climatology_ds is not None:
                        logger.info("ERA5 climatology loaded successfully.")
                    else:
                        logger.error(
                            "ERA5 climatology loading failed - all strategies returned None"
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to load ERA5 climatology from {climatology_path}: {e}",
                        exc_info=True,
                    )
                    self.era5_climatology_ds = None
            else:
                logger.error(
                    "WEATHER_ERA5_CLIMATOLOGY_PATH not configured. Cannot load climatology for ACC calculation."
                )
        return self.era5_climatology_ds

    async def build_score_row(
        self,
        run_id: int,
        gfs_init_time: datetime,
        evaluation_results: List[Dict],
        task_name_prefix: str,
    ):
        """
        Builds the score row for a given run and stores it in the score_table.
        evaluation_results is a list of dicts, each from an evaluation function (e.g., evaluate_miner_forecast_day1).
        task_name_prefix is used to form the task_name in score_table (e.g., 'weather_day1_qc', 'weather_era5_final').
        """
        logger.info(
            f"[BuildScoreRow] Building {task_name_prefix} score row for run_id: {run_id}"
        )
        all_miner_scores_for_run: Dict[int, float] = {}

        for eval_result in evaluation_results:
            if isinstance(eval_result, Exception) or not isinstance(eval_result, dict):
                logger.warning(
                    f"[BuildScoreRow] Skipping invalid evaluation result for {task_name_prefix}: {type(eval_result)}"
                )
                continue

            miner_uid = eval_result.get("miner_uid")
            score_value = eval_result.get("final_score_for_uid")

            if (
                miner_uid is not None
                and score_value is not None
                and np.isfinite(score_value)
            ):
                all_miner_scores_for_run[miner_uid] = float(score_value)
            elif miner_uid is not None:
                all_miner_scores_for_run[miner_uid] = 0.0

        final_scores_list = [0.0] * 256

        for uid, score in all_miner_scores_for_run.items():
            if 0 <= uid < 256:
                final_scores_list[uid] = score

        # Build individual column names and values for the new schema
        column_names = ["task_name", "task_id", "status", "created_at"]
        column_values = [":task_name", ":task_id", ":status", ":timestamp"]

        # Add individual UID score columns
        for uid in range(256):
            column_names.append(f"uid_{uid}_score")
            column_values.append(f":uid_{uid}_score")

        # Build INSERT query with individual columns
        query = f"""
        INSERT INTO score_table 
        ({', '.join(column_names)})
        VALUES ({', '.join(column_values)})
        ON CONFLICT (task_name, task_id)
        DO UPDATE SET
            status = EXCLUDED.status,
            created_at = EXCLUDED.created_at,
            {', '.join([f'uid_{uid}_score = EXCLUDED.uid_{uid}_score' for uid in range(256)])}
        """

        # Build parameters dictionary with individual scores
        db_params = {
            "task_name": task_name_prefix,
            "task_id": str(run_id),
            "status": f"{task_name_prefix}_scores_compiled",
            "timestamp": gfs_init_time,
        }

        # Add individual UID scores
        for uid in range(256):
            db_params[f"uid_{uid}_score"] = (
                final_scores_list[uid] if uid < len(final_scores_list) else 0.0
            )

        await self.db_manager.execute(query, db_params)
        logger.info(
            f"[BuildScoreRow] Stored/updated score row for task '{task_name_prefix}' with task_id '{run_id}'"
        )

    async def check_and_build_missing_weather_scores(
        self, target_timestep: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Check for missing weather score rows and build them if needed.

        Args:
            target_timestep: Specific timestep to check (if None, checks recent timesteps)

        Returns:
            Dict with status and details of operations performed
        """
        logger.info(
            f"[MissingScoresChecker] Checking for missing weather score rows{f' for timestep {target_timestep}' if target_timestep else ' for recent timesteps'}"
        )

        result = {
            "checked_timesteps": 0,
            "missing_score_rows_found": 0,
            "score_rows_created": 0,
            "errors": [],
            "details": [],
        }

        try:
            # Get timesteps to check
            if target_timestep:
                timesteps_to_check = [target_timestep]
            else:
                # Check ALL runs that have weather_miner_scores but might be missing score_table entries
                # This is more comprehensive than just checking last 24 hours
                timestep_query = """
                WITH scored_runs AS (
                    SELECT DISTINCT wms.run_id, wfr.gfs_init_time_utc
                    FROM weather_miner_scores wms
                    JOIN weather_forecast_runs wfr ON wfr.id = wms.run_id
                    WHERE wms.score_type IN ('day1_qc_score', 'era5_final_composite_score')
                ),
                score_table_runs AS (
                    SELECT DISTINCT 
                        CASE 
                            WHEN task_id LIKE 'initial_weather_scores_%' THEN CAST(SUBSTRING(task_id FROM 'initial_weather_scores_(.*)') AS INTEGER)
                            WHEN task_id LIKE 'final_weather_scores_%' THEN CAST(SUBSTRING(task_id FROM 'final_weather_scores_(.*)') AS INTEGER)
                            ELSE NULL
                        END as run_id
                    FROM score_table 
                    WHERE task_name = 'weather' 
                    AND task_id NOT IN ('initial_weather_scores', 'final_weather_scores')
                )
                SELECT sr.gfs_init_time_utc, sr.run_id
                FROM scored_runs sr
                LEFT JOIN score_table_runs str ON sr.run_id = str.run_id
                WHERE str.run_id IS NULL
                ORDER BY sr.run_id DESC
                LIMIT 50
                """
                timestep_records = await self.db_manager.fetch_all(timestep_query)
                timesteps_to_check = [
                    (rec["gfs_init_time_utc"], rec["run_id"])
                    for rec in timestep_records
                ]

            result["checked_timesteps"] = len(timesteps_to_check)

            for timestep_info in timesteps_to_check:
                if target_timestep:
                    gfs_init_time = target_timestep
                    # Find run_id for this timestep
                    run_query = """
                    SELECT id FROM weather_forecast_runs 
                    WHERE gfs_init_time_utc = :gfs_time
                    ORDER BY id DESC LIMIT 1
                    """
                    run_result = await self.db_manager.fetch_one(
                        run_query, {"gfs_time": gfs_init_time}
                    )
                    if not run_result:
                        result["errors"].append(
                            f"No run found for timestep {gfs_init_time}"
                        )
                        continue
                    run_id = run_result["id"]
                else:
                    gfs_init_time, run_id = timestep_info

                # Check if score row exists for this timestep
                existing_scores = await self._check_existing_score_rows(
                    gfs_init_time, run_id
                )

                # Determine what scores should exist based on run status
                required_scores = await self._determine_required_score_rows(run_id)

                # Find missing scores
                missing_scores = required_scores - existing_scores

                if missing_scores:
                    result["missing_score_rows_found"] += len(missing_scores)
                    result["details"].append(
                        f"Timestep {gfs_init_time} (Run {run_id}): Missing {missing_scores}"
                    )

                    # Build missing score rows
                    for score_type in missing_scores:
                        try:
                            if await self._build_missing_score_row(
                                gfs_init_time, run_id, score_type
                            ):
                                result["score_rows_created"] += 1
                                logger.info(
                                    f"[MissingScoresChecker] Built missing {score_type} score row for run {run_id}"
                                )
                            else:
                                result["errors"].append(
                                    f"Failed to build {score_type} score for run {run_id}"
                                )
                        except Exception as e:
                            error_msg = f"Error building {score_type} score for run {run_id}: {e}"
                            result["errors"].append(error_msg)
                            logger.error(
                                f"[MissingScoresChecker] {error_msg}", exc_info=True
                            )

            logger.info(
                f"[MissingScoresChecker] Completed check: {result['score_rows_created']}/{result['missing_score_rows_found']} missing scores built from {result['checked_timesteps']} timesteps"
            )

        except Exception as e:
            error_msg = f"Error in missing scores checker: {e}"
            result["errors"].append(error_msg)
            logger.error(f"[MissingScoresChecker] {error_msg}", exc_info=True)

        return result

    async def _check_existing_score_rows(
        self, gfs_init_time: datetime, run_id: int
    ) -> Set[str]:
        """Check what score rows already exist for a given timestep/run."""
        existing_query = """
        SELECT task_id FROM score_table 
        WHERE task_name = 'weather' 
        AND (task_id LIKE :run_pattern OR task_id IN ('initial_weather_scores', 'final_weather_scores'))
        """
        existing_records = await self.db_manager.fetch_all(
            existing_query, {"run_pattern": f"%_{run_id}"}
        )

        existing_scores = set()
        for record in existing_records:
            task_id = record["task_id"]
            if task_id == "initial_weather_scores":
                existing_scores.add("initial_combined")
            elif task_id == "final_weather_scores":
                existing_scores.add("final_combined")
            elif task_id.startswith("initial_weather_scores_"):
                existing_scores.add("initial_run_specific")
            elif task_id.startswith("final_weather_scores_"):
                existing_scores.add("final_run_specific")

        return existing_scores

    async def _determine_required_score_rows(self, run_id: int) -> Set[str]:
        """Determine what score rows should exist based on run status and data availability."""
        run_query = """
        SELECT status, gfs_init_time_utc FROM weather_forecast_runs WHERE id = :run_id
        """
        run_info = await self.db_manager.fetch_one(run_query, {"run_id": run_id})

        if not run_info:
            return set()

        status = run_info["status"]
        required_scores = set()

        # Check if day1 scores exist
        day1_scores_query = """
        SELECT COUNT(*) as count FROM weather_miner_scores 
        WHERE run_id = :run_id AND score_type = 'day1_qc_score'
        """
        day1_result = await self.db_manager.fetch_one(
            day1_scores_query, {"run_id": run_id}
        )
        has_day1_scores = day1_result and day1_result["count"] > 0

        # Check if ERA5 scores exist
        era5_scores_query = """
        SELECT COUNT(*) as count FROM weather_miner_scores 
        WHERE run_id = :run_id AND score_type = 'era5_final_composite_score'
        """
        era5_result = await self.db_manager.fetch_one(
            era5_scores_query, {"run_id": run_id}
        )
        has_era5_scores = era5_result and era5_result["count"] > 0

        # Determine required scores based on status and available data
        if (
            status in ["day1_scoring_complete", "scored", "final_scoring_complete"]
            and has_day1_scores
        ):
            if has_era5_scores:
                required_scores.add("final_run_specific")  # Should have final scores
            else:
                required_scores.add(
                    "initial_run_specific"
                )  # Should have initial scores

        return required_scores

    async def _build_missing_score_row(
        self, gfs_init_time: datetime, run_id: int, score_type: str
    ) -> bool:
        """Build a specific missing score row."""
        try:
            logger.info(
                f"[MissingScoresChecker] Building {score_type} score row for run {run_id}"
            )

            if score_type in ["initial_run_specific", "final_run_specific"]:
                # Use the existing update_combined_weather_scores method with run_id trigger
                force_phase = (
                    "initial" if score_type == "initial_run_specific" else "final"
                )

                # Call update_combined_weather_scores with the specific run_id to rebuild the score
                await self.update_combined_weather_scores(
                    run_id_trigger=run_id, force_phase=force_phase
                )

                return True
            else:
                logger.warning(
                    f"[MissingScoresChecker] Unknown score type: {score_type}"
                )
                return False

        except Exception as e:
            logger.error(
                f"[MissingScoresChecker] Error building {score_type} score for run {run_id}: {e}",
                exc_info=True,
            )
            return False

    async def validator_prepare_subtasks(self):
        """
        Prepares data needed for a forecast run (e.g., identifying GFS data).
        Since this is 'atomic', it doesn't prepare sub-tasks in the composite sense,
        but rather the overall input for querying miners.
        """
        pass

    async def initiate_fetch_retry_once(self, validator) -> None:
        """Retry initiate-fetch for newest run stuck without any responses."""
        try:
            row = await self.db_manager.fetch_one(
                """
                SELECT id, status FROM weather_forecast_runs
                WHERE status IN ('sending_fetch_requests')
                ORDER BY run_initiation_time DESC
                LIMIT 1
                """
            )
            if not row:
                return
            run_id = int(row["id"]) if row.get("id") is not None else None
            if run_id is None:
                return
            cnt_row = await self.db_manager.fetch_one(
                "SELECT COUNT(1) AS c FROM weather_miner_responses WHERE run_id = :rid",
                {"rid": run_id},
            )
            have = int(cnt_row.get("c") if cnt_row else 0)
            if have > 0:
                return
            gfs_run = await self.db_manager.fetch_one(
                "SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :rid",
                {"rid": run_id},
            )
            if not gfs_run:
                return
            gfs_t0_run_time = gfs_run["gfs_init_time_utc"]
            gfs_t_minus_6_run_time = gfs_t0_run_time - timedelta(hours=6)
            from gaia.validator.miner.miner_query import initiate_fetch as _init_fetch
            hotkeys: list[str] = []
            try:
                if hasattr(validator, "metagraph") and validator.metagraph and getattr(validator.metagraph, "nodes", None):
                    hotkeys = list(validator.metagraph.nodes.keys())
            except Exception:
                hotkeys = []
            if not hotkeys:
                try:
                    rows = await self.db_manager.fetch_all(
                        "SELECT hotkey FROM node_table WHERE hotkey IS NOT NULL ORDER BY uid"
                    )
                    hotkeys = [r["hotkey"] for r in rows]
                except Exception:
                    hotkeys = []
            targeted = len(hotkeys)
            logger.info(f"[Run {run_id}] Initiate-fetch retry targeting {targeted} miners")
            responses = {}
            for miner_hotkey in hotkeys:
                try:
                    res = await _init_fetch(
                        validator,
                        miner_hotkey,
                        forecast_start_time=gfs_t0_run_time,
                        previous_step_time=gfs_t_minus_6_run_time,
                        validator_hotkey=(
                            validator.keypair.ss58_address if validator.keypair else None
                        ),
                    )
                    if res is not None:
                        responses[miner_hotkey] = res
                except Exception as e:
                    logger.warning(f"[Run {run_id}] retry initiate failed for {miner_hotkey[:8]}: {e}")
                    continue
            accepted_count = 0
            for miner_hotkey, response_data in responses.items():
                try:
                    miner_response = response_data
                    if isinstance(response_data, dict) and "text" in response_data:
                        try:
                            miner_response = loads(response_data["text"])
                        except Exception:
                            miner_response = {"status": "parse_error"}
                    if (
                        isinstance(miner_response, dict)
                        and miner_response.get("status") == WeatherTaskStatus.FETCH_ACCEPTED
                        and miner_response.get("job_id")
                    ):
                        miner_uid_result = await self.db_manager.fetch_one(
                            "SELECT uid FROM node_table WHERE hotkey = :hk",
                            {"hk": miner_hotkey},
                        )
                        miner_uid = (
                            miner_uid_result["uid"] if miner_uid_result else -1
                        )
                        if miner_uid != -1:
                            await self.db_manager.execute(
                                """
                                INSERT INTO weather_miner_responses
                                 (run_id, miner_uid, miner_hotkey, response_time, status, job_id)
                                 VALUES (:run_id, :uid, :hk, :resp_time, :status, :job_id)
                                ON CONFLICT (run_id, miner_uid) DO UPDATE SET
                                 response_time = EXCLUDED.response_time,
                                 status = EXCLUDED.status,
                                 job_id = EXCLUDED.job_id
                                """,
                                {
                                    "run_id": run_id,
                                    "uid": miner_uid,
                                    "hk": miner_hotkey,
                                    "resp_time": datetime.now(timezone.utc),
                                    "status": "fetch_initiated",
                                    "job_id": miner_response.get("job_id"),
                                },
                            )
                            accepted_count += 1
                except Exception as resp_proc_err:
                    logger.error(
                        f"[Run {run_id}] Error processing response from {miner_hotkey}: {resp_proc_err}",
                        exc_info=True,
                    )
                    continue
            logger.info(
                f"[Run {run_id}] Initiate-fetch retry summary: targeted={targeted}, responses={len(responses)}, accepted={accepted_count}, rejected={len(responses)-accepted_count}"
            )
            if accepted_count > 0:
                await _update_run_status(self, run_id, "awaiting_inference_results")
                try:
                    _ = await self.db_manager.enqueue_miner_poll_jobs(limit=1000)
                except Exception:
                    pass
                try:
                    _ = await self.db_manager.enqueue_weather_step_jobs(limit=1000)
                except Exception:
                    pass
        except Exception:
            return

    async def initiate_fetch_retry_loop(self, validator) -> None:
        """Periodic retry loop to ensure initiate-fetch is sent for active runs."""
        while True:
            try:
                await self.initiate_fetch_retry_once(validator)
            except Exception:
                pass
            await asyncio.sleep(300)

    async def validator_execute(self, validator):
        """
        Orchestrates the weather forecast task for the validator:
        1. Waits for the scheduled run time (e.g., daily post-00Z GFS availability).
        2. Fetches necessary GFS analysis data (T=0h from 00Z run, T=-6h from previous 18Z run).
        3. Serializes data and creates a run record in DB.
        4. Queries miners with the payload (/weather-forecast-request).
        5. Records miner acceptances in DB.
        """
        self.validator = validator
        logger.info("Starting WeatherTask validator execution loop...")

        if not self.initial_scoring_worker_running:
            # Apply global LRU cache limits to prevent unlimited memory growth during scoring
            try:
                from .utils.memory_management import apply_global_lru_limits

                apply_global_lru_limits()
            except Exception as e:
                logger.warning(f"Failed to apply global LRU cache limits: {e}")

            await self.start_background_workers(
                num_initial_scoring_workers=1,
                num_final_scoring_workers=1,
                num_cleanup_workers=1,
            )
            logger.info("Started background workers for scoring and cleanup.")

        run_hour_utc = self.config.get("run_hour_utc", 18)
        run_minute_utc = self.config.get("run_minute_utc", 0)
        logger.info(
            f"Validator execute loop configured to run around {run_hour_utc:02d}:{run_minute_utc:02d} UTC."
        )

        if self.test_mode:
            logger.warning("Running in TEST MODE: Execution will run once immediately.")

        # Track recovery state to avoid too frequent recovery attempts
        last_recovery_time = 0
        recovery_interval = 300  # 5 minutes between recovery attempts
        processed_runs_this_session = (
            {}
        )  # Track run attempts this session: {run_id: attempt_count}
        max_runs_per_session = (
            20  # Limit runs processed per session to avoid indefinite loops
        )
        max_attempts_per_run = (
            3  # Allow multiple attempts per run in case of transient issues
        )

        while True:
            try:
                await validator.update_task_status("weather", "active", "waiting")

                # Perform gradual, non-blocking recovery every 5 minutes
                current_time = time.time()
                if current_time - last_recovery_time > recovery_interval:
                    # New pipeline handles recovery via weather_forecast_steps + scheduler.
                    # Periodic heavy recovery tasks from legacy system are disabled.
                        last_recovery_time = current_time

                now_utc = datetime.now(timezone.utc)

                if not self.test_mode:
                    target_run_time_today = now_utc.replace(
                        hour=run_hour_utc,
                        minute=run_minute_utc,
                        second=0,
                        microsecond=0,
                    )
                    if now_utc >= target_run_time_today:
                        next_run_trigger_time = target_run_time_today + timedelta(
                            days=1
                        )
                    else:
                        next_run_trigger_time = target_run_time_today

                    wait_seconds = (next_run_trigger_time - now_utc).total_seconds()
                    logger.info(
                        f"Current time: {now_utc}. Next weather run scheduled at {next_run_trigger_time}. Waiting for {wait_seconds:.2f} seconds."
                    )
                    if wait_seconds > 0:
                        await asyncio.sleep(wait_seconds)
                    now_utc = datetime.now(timezone.utc)

                logger.info(
                    f"Initiating weather forecast run triggered around {now_utc}..."
                )
                await validator.update_task_status(
                    "weather", "processing", "initializing_run"
                )

                if self.test_mode:
                    # Centralize hindcast shift configuration via util_time.get_effective_gfs_init
                    # Default shift is -7 days; configurable via config fields test_hindcast_days/hours.
                    from gaia.tasks.defined_tasks.weather.pipeline.steps.util_time import (
                        get_effective_gfs_init,
                    )
                    shifted = get_effective_gfs_init(self, now_utc)
                    if shifted != now_utc:
                        logger.info(
                            f"Test mode enabled. Hindcast shift applied: {now_utc.strftime('%Y-%m-%d %H:%M')} -> {shifted.strftime('%Y-%m-%d %H:%M')} UTC"
                        )
                        now_utc = shifted

                gfs_t0_run_time = now_utc.replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                gfs_t_minus_6_run_time = gfs_t0_run_time - timedelta(
                    hours=6
                )  # This will be 18Z from the previous day

                logger.info(f"Target GFS T=0h Analysis Run Time: {gfs_t0_run_time}")
                logger.info(
                    f"Target GFS T=-6h Analysis Run Time: {gfs_t_minus_6_run_time}"
                )

                # Check if there's already an active run for this GFS timestep
                existing_run_query = """
                SELECT id, status, run_initiation_time
                FROM weather_forecast_runs 
                WHERE gfs_init_time_utc = :gfs_init 
                AND status NOT IN ('completed', 'error', 'stale_abandoned', 'restarted_as_new_run', 'recovery_failed')
                ORDER BY run_initiation_time DESC
                LIMIT 1
                """
                # Route via validator DB accessor to avoid main-loop blocking
                try:
                    req_id = str(uuid.uuid4())
                    fut = asyncio.get_event_loop().create_future()
                    if hasattr(self, "validator") and hasattr(self.validator, "_db_results"):
                        self.validator._db_results[req_id] = fut
                        await self.validator._db_work_queue.put(
                            {
                                "id": req_id,
                                "kind": "fetch_one",
                                "query": existing_run_query,
                                "params": {"gfs_init": gfs_t0_run_time},
                            }
                        )
                        existing_run = await fut
                    else:
                        existing_run = await self.db_manager.fetch_one(
                            existing_run_query, {"gfs_init": gfs_t0_run_time}
                        )
                except Exception:
                    existing_run = await self.db_manager.fetch_one(
                        existing_run_query, {"gfs_init": gfs_t0_run_time}
                    )

                if existing_run:
                    run_id = existing_run["id"]
                    existing_status = existing_run["status"]
                    run_age_hours = (
                        now_utc - existing_run["run_initiation_time"]
                    ).total_seconds() / 3600

                    logger.info(
                        f"[Run {run_id}] Found existing run for GFS time {gfs_t0_run_time} with status '{existing_status}' (age: {run_age_hours:.1f}h)"
                    )

                    if run_age_hours > 24:
                        logger.warning(
                            f"[Run {run_id}] Existing run is too old ({run_age_hours:.1f}h), marking as stale and creating new run"
                        )
                        await _update_run_status(self, run_id, "stale_abandoned")
                        existing_run = None  # Will create new run below
                    else:
                        logger.info(
                            f"[Run {run_id}] Reusing existing active run. Ensuring seed rows exist, then continuing with validator_score."
                        )
                        try:
                            # Backfill seed rows if fewer than expected present
                            cnt_row = await self.db_manager.fetch_one(
                                "SELECT COUNT(1) AS cnt FROM weather_forecast_stats WHERE run_id = :rid",
                                {"rid": run_id},
                            )
                            total = (cnt_row and cnt_row.get("cnt")) or 0
                            if total < 255:
                                from gaia.tasks.defined_tasks.weather.pipeline.steps.seed_step import (
                                    seed_forecast_run,
                                )
                                # Determine validator hotkey if available
                                try:
                                    vhk = (
                                        getattr(
                                            getattr(self.validator, "validator_wallet", None),
                                            "hotkey",
                                            None,
                                        ).ss58_address
                                        if hasattr(self, "validator") and self.validator is not None
                                        else "unknown_validator"
                                    )
                                except Exception:
                                    vhk = "unknown_validator"
                                seeded = await seed_forecast_run(
                                    self.db_manager, run_id, validator_hotkey=vhk
                                )
                                logger.info(
                                    f"[Run {run_id}] Seed backfill completed (rows now: {total} + upserts)."
                                )
                                # Warm GFS caches now handled by worker via 'weather.seed' job
                        except Exception as seed_err:
                            logger.warning(
                                f"[Run {run_id}] Seed backfill check failed: {seed_err}"
                            )

                        # If we restarted before sending requests, re-send initiate-fetch when no responses exist
                        try:
                            resp_cnt_row = await self.db_manager.fetch_one(
                                """
                                SELECT COUNT(1) AS cnt
                                FROM weather_miner_responses
                                WHERE run_id = :rid
                                """,
                                {"rid": run_id},
                            )
                            resp_cnt = int(resp_cnt_row.get("cnt") if resp_cnt_row else 0)
                        except Exception:
                            resp_cnt = 0
                        if resp_cnt == 0:
                            logger.info(
                                f"[Run {run_id}] No prior initiate-fetch responses recorded. Re-sending initiate fetch to miners."
                            )
                            from gaia.validator.miner.miner_query import initiate_fetch as _init_fetch
                            # Prefer metagraph; fallback to node_table
                            hotkeys: list[str] = []
                            if hasattr(validator, "metagraph") and validator.metagraph and getattr(validator.metagraph, "nodes", None):
                                try:
                                    hotkeys = list(validator.metagraph.nodes.keys())
                                except Exception:
                                    hotkeys = []
                            if not hotkeys:
                                try:
                                    rows = await self.db_manager.fetch_all(
                                        "SELECT hotkey FROM node_table WHERE hotkey IS NOT NULL ORDER BY uid"
                                    )
                                    hotkeys = [r["hotkey"] for r in rows]
                                except Exception:
                                    hotkeys = []
                            responses = {}
                            for miner_hotkey in hotkeys:
                                try:
                                    res = await _init_fetch(
                                        validator,
                                        miner_hotkey,
                                        forecast_start_time=gfs_t0_run_time,
                                        previous_step_time=gfs_t_minus_6_run_time,
                                        validator_hotkey=(
                                            validator.keypair.ss58_address if validator.keypair else None
                                        ),
                                    )
                                    if res is not None:
                                        responses[miner_hotkey] = res
                                except Exception:
                                    continue
                            # Record acceptances
                            accepted_count = 0
                            for miner_hotkey, response_data in responses.items():
                                try:
                                    miner_response = response_data
                                    if isinstance(response_data, dict) and "text" in response_data:
                                        try:
                                            miner_response = loads(response_data["text"])
                                        except Exception:
                                            miner_response = {"status": "parse_error"}
                                    if (
                                        isinstance(miner_response, dict)
                                        and miner_response.get("status") == WeatherTaskStatus.FETCH_ACCEPTED
                                        and miner_response.get("job_id")
                                    ):
                                        miner_uid_result = await self.db_manager.fetch_one(
                                            "SELECT uid FROM node_table WHERE hotkey = :hk",
                                            {"hk": miner_hotkey},
                                        )
                                        miner_uid = (
                                            miner_uid_result["uid"] if miner_uid_result else -1
                                        )
                                        if miner_uid != -1:
                                            await self.db_manager.execute(
                                                """
                                                INSERT INTO weather_miner_responses
                                                 (run_id, miner_uid, miner_hotkey, response_time, status, job_id)
                                                 VALUES (:run_id, :uid, :hk, :resp_time, :status, :job_id)
                                                ON CONFLICT (run_id, miner_uid) DO UPDATE SET
                                                 response_time = EXCLUDED.response_time,
                                                 status = EXCLUDED.status,
                                                 job_id = EXCLUDED.job_id
                                                """,
                                                {
                                                    "run_id": run_id,
                                                    "uid": miner_uid,
                                                    "hk": miner_hotkey,
                                                    "resp_time": datetime.now(timezone.utc),
                                                    "status": "fetch_initiated",
                                                    "job_id": miner_response.get("job_id"),
                                                },
                                            )
                                            accepted_count += 1
                                except Exception as resp_proc_err:
                                    logger.error(
                                        f"[Run {run_id}] Error processing response from {miner_hotkey}: {resp_proc_err}",
                                        exc_info=True,
                                    )
                                    continue
                            if accepted_count > 0:
                                await _update_run_status(self, run_id, "awaiting_inference_results")
                                try:
                                    _ = await self.db_manager.enqueue_miner_poll_jobs(limit=1000)
                                except Exception:
                                    pass
                                try:
                                    _ = await self.db_manager.enqueue_weather_step_jobs(limit=1000)
                                except Exception:
                                    pass

                        await self.validator_score()

                        if self.test_mode:
                            logger.info(
                                "TEST MODE: Found existing run. Exiting validator_execute loop."
                            )
                            break
                        else:
                            # Continue loop to wait for next scheduled time
                            continue

                # Only create new run if no existing run found
                if not existing_run:
                    run_id = None
                    try:
                        run_insert_query = """
                            INSERT INTO weather_forecast_runs (run_initiation_time, target_forecast_time_utc, gfs_init_time_utc, status)
                            VALUES (:init_time, :target_time, :gfs_init, :status)
                            RETURNING id
                        """
                        effective_forecast_start_time = gfs_t0_run_time
                        run_record = await self.db_manager.fetch_one(
                            run_insert_query,
                            {
                                "init_time": now_utc,
                                "target_time": effective_forecast_start_time,
                                "gfs_init": effective_forecast_start_time,
                                "status": "fetching_gfs",
                            },
                        )

                        if run_record and "id" in run_record:
                            run_id = run_record["id"]
                            logger.info(
                                f"[Run {run_id}] Created NEW weather_forecast_runs record with ID: {run_id}"
                            )
                            # Gate seeding on node_table availability; enqueue dereg job and wait briefly if empty
                            try:
                                row = await self.db_manager.fetch_one(
                                    "SELECT COUNT(*) AS c FROM node_table WHERE hotkey IS NOT NULL"
                                )
                                count_nodes = int(row.get("c") if row else 0)
                            except Exception:
                                count_nodes = 0
                            if count_nodes == 0:
                                try:
                                    await self.db_manager.enqueue_singleton_job(
                                        job_type="miners.handle_deregistrations", payload={}, priority=160
                                    )
                                except Exception:
                                    pass
                                # brief wait for workers to populate node_table
                                await asyncio.sleep(5)
                            # Seed per-miner stats and step timelines (idempotent)
                            try:
                                from gaia.tasks.defined_tasks.weather.pipeline.steps.seed_step import (
                                    seed_forecast_run,
                                )
                                try:
                                    vhk = (
                                        getattr(
                                            getattr(self.validator, "validator_wallet", None),
                                            "hotkey",
                                            None,
                                        ).ss58_address
                                        if hasattr(self, "validator") and self.validator is not None
                                        else "unknown_validator"
                                    )
                                except Exception:
                                    vhk = "unknown_validator"
                                seeded = await seed_forecast_run(
                                    self.db_manager, run_id, validator_hotkey=vhk
                                )
                                logger.info(
                                    f"[Run {run_id}] Seeded per-miner stats/steps for {seeded} miners."
                                )
                            except Exception as seed_err:
                                logger.warning(
                                    f"[Run {run_id}] Seeding per-miner rows failed: {seed_err}"
                            )
                        else:
                            logger.error(
                                "Failed to retrieve run_id using fetch_one after insert."
                            )
                            raise RuntimeError(
                                "Failed to create run_id for forecast run."
                            )

                    except Exception as db_err:
                        logger.error(
                            f"Failed to create forecast run record in DB: {db_err}",
                            exc_info=True,
                        )
                        if self.test_mode:
                            logger.warning(
                                "TEST MODE: DB error during run_id creation. Exiting validator_execute loop."
                            )
                            break
                        else:
                            await asyncio.sleep(60)
                            continue

                    await validator.update_task_status(
                        "weather", "processing", "sending_fetch_requests"
                    )
                    await _update_run_status(self, run_id, "sending_fetch_requests")

                    payload_data = WeatherInitiateFetchData(
                        forecast_start_time=gfs_t0_run_time,  # T=0h time
                        previous_step_time=gfs_t_minus_6_run_time,  # T=-6h time
                        validator_hotkey=(
                            validator.keypair.ss58_address
                            if validator.keypair
                            else None
                        ),
                    )
                    payload_dict = payload_data.model_dump(mode="json")

                    payload = {"nonce": str(uuid.uuid4()), "data": payload_dict}

                    logger.info(
                        f"[Run {run_id}] Querying miners with weather initiate fetch request (Endpoint: /weather-initiate-fetch)... Payload size approx: {len(dumps(payload))} bytes"
                    )
                    # Per-miner secure queries using Fiber handshake
                    from gaia.validator.miner.miner_query import initiate_fetch as _init_fetch
                    # Prefer metagraph; fall back to node_table if not yet synced
                    hotkeys: list[str] = []
                    try:
                        if hasattr(validator, "metagraph") and validator.metagraph and getattr(validator.metagraph, "nodes", None):
                            hotkeys = list(validator.metagraph.nodes.keys())
                    except Exception:
                        hotkeys = []
                    if not hotkeys:
                        try:
                            rows = await self.db_manager.fetch_all(
                                "SELECT hotkey FROM node_table WHERE hotkey IS NOT NULL ORDER BY uid"
                            )
                            hotkeys = [r["hotkey"] for r in rows]
                            logger.info(
                                f"[Run {run_id}] Metagraph not ready; using {len(hotkeys)} hotkeys from node_table for initiate-fetch"
                            )
                        except Exception as _e:
                            logger.warning(
                                f"[Run {run_id}] Could not obtain miner hotkeys from node_table: {_e}"
                            )
                            hotkeys = []
                    targeted = len(hotkeys)
                    logger.info(f"[Run {run_id}] Initiate-fetch targeting {targeted} miners")
                    responses = {}
                    for miner_hotkey in hotkeys:
                        try:
                            res = await _init_fetch(
                                validator,
                                miner_hotkey,
                                forecast_start_time=gfs_t0_run_time,
                                previous_step_time=gfs_t_minus_6_run_time,
                                validator_hotkey=(
                                    validator.keypair.ss58_address if validator.keypair else None
                                ),
                            )
                            if res is not None:
                                responses[miner_hotkey] = res
                        except Exception as e:
                            logger.warning(
                                f"[Run {run_id}] initiate_fetch failed for {miner_hotkey[:8]}: {e}"
                            )
                            continue
                    logger.info(
                        f"[Run {run_id}] Received {len(responses)} initial responses from miners for fetch initiation (targeted={targeted})"
                    )

                    await validator.update_task_status(
                        "weather", "processing", "recording_acceptances"
                    )
                    accepted_count = 0
                    for miner_hotkey, response_data in responses.items():
                        try:
                            miner_response = response_data
                            if (
                                isinstance(response_data, dict)
                                and "text" in response_data
                            ):
                                try:
                                    miner_response = loads(response_data["text"])
                                    logger.debug(
                                        f"[Run {run_id}] Parsed JSON from response text for {miner_hotkey}: {miner_response}"
                                    )
                                except (Exception, TypeError) as json_err:
                                    logger.warning(
                                        f"[Run {run_id}] Failed to parse response text for {miner_hotkey}: {json_err}"
                                    )
                                    miner_response = {
                                        "status": "parse_error",
                                        "message": str(json_err),
                                    }

                            if (
                                isinstance(miner_response, dict)
                                and miner_response.get("status")
                                == WeatherTaskStatus.FETCH_ACCEPTED
                                and miner_response.get("job_id")
                            ):
                                miner_uid_result = await self.db_manager.fetch_one(
                                    "SELECT uid FROM node_table WHERE hotkey = :hk",
                                    {"hk": miner_hotkey},
                                )
                                miner_uid = (
                                    miner_uid_result["uid"] if miner_uid_result else -1
                                )

                                if miner_uid == -1:
                                    logger.warning(
                                        f"[Run {run_id}] Miner {miner_hotkey} accepted but UID not found in node_table."
                                    )
                                    continue

                                miner_job_id = miner_response.get("job_id")

                                insert_resp_query = """
                                       INSERT INTO weather_miner_responses
                                        (run_id, miner_uid, miner_hotkey, response_time, status, job_id)
                                        VALUES (:run_id, :uid, :hk, :resp_time, :status, :job_id)
                                       ON CONFLICT (run_id, miner_uid) DO UPDATE SET
                                        response_time = EXCLUDED.response_time, 
                                        status = EXCLUDED.status,
                                        job_id = EXCLUDED.job_id
                                  """
                                await self.db_manager.execute(
                                    insert_resp_query,
                                    {
                                        "run_id": run_id,
                                        "uid": miner_uid,
                                        "hk": miner_hotkey,
                                        "resp_time": datetime.now(timezone.utc),
                                        "status": "fetch_initiated",
                                        "job_id": miner_job_id,
                                    },
                                )
                                accepted_count += 1
                                logger.debug(
                                    f"[Run {run_id}] Recorded acceptance from Miner UID {miner_uid} ({miner_hotkey}). Miner Job ID: {miner_job_id}"
                                )
                            else:
                                logger.warning(
                                    f"[Run {run_id}] Miner {miner_hotkey} did not return successful 'fetch_accepted' status or job_id. Response: {miner_response}"
                                )
                        except Exception as resp_proc_err:
                            logger.error(
                                f"[Run {run_id}] Error processing response from {miner_hotkey}: {resp_proc_err}",
                                exc_info=True,
                            )

                    logger.info(
                        f"[Run {run_id}] Completed initiate-fetch processing: accepted={accepted_count}, rejected={len(responses) - accepted_count}, targeted={targeted}"
                    )
                    # Simplified flow: skip input-hash verification; move directly to awaiting inference
                    await _update_run_status(self, run_id, "awaiting_inference_results")
                    try:
                        # Create polling jobs for miners to report readiness
                        _ = await self.db_manager.enqueue_miner_poll_jobs(limit=1000)
                    except Exception:
                        pass
                    try:
                        # Ensure step jobs are enqueued (day1/era5)
                        _ = await self.db_manager.enqueue_weather_step_jobs(limit=1000)
                    except Exception:
                        pass
                    # Continue to scoring orchestrator which now depends on polling readiness
                    await self.validator_score()
                    # Proceed to next loop iteration
                    continue

                    miner_hash_results = {}
                    polling_tasks = []

                    async def _poll_single_miner(response_rec):
                        resp_id = response_rec["id"]
                        miner_hk = response_rec["miner_hotkey"]
                        miner_job_id = response_rec["job_id"]
                        logger.debug(
                            f"[Run {run_id}] Polling miner {miner_hk[:8]} (Job: {miner_job_id}) for input status using query_miners."
                        )

                        try:
                            from gaia.validator.miner.miner_query import get_input_status

                            status_response = await get_input_status(
                                self, miner_hk, job_id=miner_job_id
                            )

                            if status_response:
                                parsed_response = status_response
                                if isinstance(status_response, dict) and "text" in status_response:
                                    try:
                                        parsed_response = loads(status_response["text"])
                                    except (Exception, TypeError) as json_err:
                                        logger.warning(
                                            f"[Run {run_id}] Failed to parse JSON response from miner {miner_hk[:8]}: {json_err}. Raw response: {status_response.get('text', '')[:200]}"
                                        )
                                        parsed_response = {
                                            "status": "parse_error",
                                            "message": f"JSON parse error from miner {miner_hk[:8]}: {type(json_err).__name__}: {str(json_err)}",
                                        }

                                logger.debug(
                                    f"[Run {run_id}] Received status from {miner_hk[:8]}: {parsed_response}"
                                )
                                return resp_id, parsed_response
                            else:
                                logger.warning(
                                    f"[Run {run_id}] No response received from miner {miner_hk[:8]} via get_input_status."
                                )
                                return resp_id, {
                                    "status": "validator_poll_failed",
                                "message": f"No response from miner {miner_hk[:8]} on /weather-get-input-status",
                                }

                        except asyncio.TimeoutError as timeout_err:
                            logger.error(f"[Run {run_id}] Timeout polling miner {miner_hk[:8]}: {timeout_err}")
                            return resp_id, {
                                "status": "validator_poll_error",
                                "message": f"Timeout polling miner {miner_hk[:8]}: {str(timeout_err)}",
                            }
                        except ConnectionError as conn_err:
                            logger.error(f"[Run {run_id}] Connection error polling miner {miner_hk[:8]}: {conn_err}")
                            return resp_id, {
                                "status": "validator_poll_error",
                                "message": f"Connection error to miner {miner_hk[:8]}: {str(conn_err)}",
                            }
                        except Exception as poll_err:
                            logger.error(f"[Run {run_id}] Error polling miner {miner_hk[:8]}: {poll_err}", exc_info=True)
                            return resp_id, {
                                "status": "validator_poll_error",
                                "message": f"Error polling miner {miner_hk[:8]}: {type(poll_err).__name__}: {str(poll_err)}",
                            }

                    for resp_rec in miners_to_poll:
                        polling_tasks.append(_poll_single_miner(resp_rec))

                    poll_results = await asyncio.gather(
                        *polling_tasks, return_exceptions=True
                    )

                    for i, result in enumerate(poll_results):
                        if isinstance(result, Exception):
                            # Extract resp_id from the task if possible
                            logger.error(
                                f"[Run {run_id}] Polling task failed with exception: {result}"
                            )
                            continue
                        resp_id, status_data = result
                        miner_hash_results[resp_id] = status_data
                    logger.info(
                        f"[Run {run_id}] Collected input status from {len(miner_hash_results)}/{len(miners_to_poll)} miners."
                    )

                    validator_input_hash = None
                    try:
                        logger.info(
                            f"[Run {run_id}] Validator computing its own reference input hash..."
                        )
                        gfs_cache_dir = Path(
                            self.config.get(
                                "gfs_analysis_cache_dir", "./gfs_analysis_cache"
                            )
                        )
                        validator_input_hash = await compute_input_data_hash(
                            t0_run_time=gfs_t0_run_time,
                            t_minus_6_run_time=gfs_t_minus_6_run_time,
                            cache_dir=gfs_cache_dir,
                        )
                        if validator_input_hash:
                            logger.info(
                                f"[Run {run_id}] Validator computed reference hash: {validator_input_hash[:10]}..."
                            )
                        else:
                            logger.error(
                                f"[Run {run_id}] Validator failed to compute its own reference input hash. Cannot verify miners."
                            )
                            await _update_run_status(
                                self,
                                run_id,
                                "error",
                                "Validator failed hash computation",
                            )
                            miners_to_trigger = []

                    except Exception as val_hash_err:
                        logger.error(
                            f"[Run {run_id}] Error during validator hash computation: {val_hash_err}",
                            exc_info=True,
                        )
                        await _update_run_status(
                            self,
                            run_id,
                            "error",
                            f"Validator hash error: {val_hash_err}",
                        )
                        miners_to_trigger = []

                    miners_to_trigger = []
                    if validator_input_hash:
                        update_tasks = []
                        for i, (resp_id, status_data) in enumerate(
                            miner_hash_results.items()
                        ):
                            miner_status = status_data.get("status")
                            miner_hash = status_data.get("input_data_hash")
                            error_msg = status_data.get("message")
                            new_db_status = None
                            hash_match = None

                            if (
                                miner_hash
                                and miner_status
                                == WeatherTaskStatus.INPUT_HASHED_AWAITING_VALIDATION.value
                            ):
                                if miner_hash == validator_input_hash:
                                    logger.info(
                                        f"[Run {run_id}] Hash MATCH for response ID {resp_id} (Miner status: {miner_status})!"
                                    )
                                    new_db_status = "input_validation_complete"
                                    hash_match = True
                                    orig_rec = next(
                                        (
                                            m
                                            for m in miners_to_poll
                                            if m["id"] == resp_id
                                        ),
                                        None,
                                    )
                                    if orig_rec:
                                        miners_to_trigger.append(
                                            (
                                                resp_id,
                                                orig_rec["miner_hotkey"],
                                                orig_rec["job_id"],
                                            )
                                        )
                                    else:
                                        logger.error(
                                            f"[Run {run_id}] Could not find original record for resp_id {resp_id} to trigger inference."
                                        )
                                else:
                                    logger.warning(
                                        f"[Run {run_id}] Hash MISMATCH for response ID {resp_id}. Miner: {miner_hash[:10]}... Validator: {validator_input_hash[:10]}... (Miner status: {miner_status})"
                                    )
                                    new_db_status = "input_hash_mismatch"
                                    hash_match = False
                            elif miner_status == WeatherTaskStatus.FETCH_ERROR.value:
                                new_db_status = "input_fetch_error"
                            elif miner_status in [
                                WeatherTaskStatus.FETCHING_GFS.value,
                                WeatherTaskStatus.HASHING_INPUT.value,
                                WeatherTaskStatus.FETCH_QUEUED.value,
                            ]:
                                # Get current retry count for this response
                                retry_count_query = "SELECT retry_count FROM weather_miner_responses WHERE id = :resp_id"
                                retry_result = await self.db_manager.fetch_one(
                                    retry_count_query, {"resp_id": resp_id}
                                )
                                current_retry_count = (
                                    retry_result["retry_count"] if retry_result else 0
                                )

                                # Define retry intervals in minutes: 5, 10, 15
                                retry_intervals = [5, 10, 15]

                                if current_retry_count < len(retry_intervals):
                                    # Schedule next retry
                                    next_interval = retry_intervals[current_retry_count]
                                    next_retry_time = datetime.now(
                                        timezone.utc
                                    ) + timedelta(minutes=next_interval)

                                    logger.info(
                                        f"[Run {run_id}] Miner for response ID {resp_id} is still working (status: {miner_status}). "
                                        f"Scheduling retry {current_retry_count + 1}/3 in {next_interval} minutes at {next_retry_time.strftime('%H:%M:%S')} UTC."
                                    )

                                    # Update to retry_scheduled status with next retry time and incremented retry count
                                    update_query = """
                                        UPDATE weather_miner_responses
                                        SET status = 'retry_scheduled',
                                            retry_count = :retry_count,
                                            next_retry_time = :next_retry_time,
                                            last_polled_time = :now
                                        WHERE id = :resp_id
                                    """
                                    update_tasks.append(
                                        self.db_manager.execute(
                                            update_query,
                                            {
                                                "resp_id": resp_id,
                                                "retry_count": current_retry_count + 1,
                                                "next_retry_time": next_retry_time,
                                                "now": datetime.now(timezone.utc),
                                            },
                                        )
                                    )
                                    new_db_status = (
                                        None  # Don't process in the main update logic
                                    )
                                else:
                                    # Exhausted all retries, mark as failed
                                    logger.warning(
                                        f"[Run {run_id}] Miner for response ID {resp_id} exhausted all 3 retries (status: {miner_status}). "
                                        f"Marking as input timeout after final retry at 15 minutes."
                                    )
                                    new_db_status = "input_hash_timeout"
                            elif miner_status in [
                                WeatherTaskStatus.VALIDATOR_POLL_FAILED.value,
                                WeatherTaskStatus.VALIDATOR_POLL_ERROR.value,
                            ]:
                                new_db_status = "input_poll_error"
                                # The detailed error message is already captured in error_msg from status_data.get('message')
                            else:
                                new_db_status = "input_fetch_error"

                            if new_db_status:
                                update_query = """
                                    UPDATE weather_miner_responses
                                    SET status = :status,
                                        input_hash_miner = :m_hash,
                                        input_hash_validator = :v_hash,
                                        input_hash_match = :match,
                                        error_message = :err,
                                        last_polled_time = :now
                                    WHERE id = :resp_id
                                """
                                update_tasks.append(
                                    self.db_manager.execute(
                                        update_query,
                                        {
                                            "resp_id": resp_id,
                                            "status": new_db_status,
                                            "m_hash": miner_hash,
                                            "v_hash": validator_input_hash,
                                            "match": hash_match,
                                            "err": (
                                                error_msg
                                                if error_msg is not None
                                                else ""
                                            ),
                                            "now": datetime.now(timezone.utc),
                                        },
                                    )
                                )

                                # Yield control if processing many results
                                if (
                                    len(miner_hash_results) > 20 and i % 20 == 19
                                ):  # Yield every 20 items after the 20th
                                    await asyncio.sleep(0)

                        # Execute all update tasks after the loop completes
                        if update_tasks:
                            results = await asyncio.gather(
                                *update_tasks, return_exceptions=True
                            )
                            failed_updates = sum(
                                1 for r in results if isinstance(r, Exception)
                            )
                            if failed_updates > 0:
                                logger.error(
                                    f"[Run {run_id}] {failed_updates}/{len(update_tasks)} DB updates failed"
                                )
                                for i, r in enumerate(results):
                                    if isinstance(r, Exception):
                                        logger.error(
                                            f"[Run {run_id}] Update {i} failed: {r}"
                                        )
                            else:
                                logger.info(
                                    f"[Run {run_id}] Updated DB for {len(update_tasks)} miner responses after hash check."
                                )

                    if miners_to_trigger:
                        logger.info(
                            f"[Run {run_id}] Triggering inference for {len(miners_to_trigger)} miners with matching input hashes."
                        )
                        await validator.update_task_status(
                            "weather", "processing", "triggering_inference"
                        )
                        trigger_tasks = []

                        async def _trigger_single_miner(
                            resp_id, miner_hk, miner_job_id
                        ):
                            logger.debug(
                                f"[Run {run_id}] Attempting to trigger inference for {miner_hk[:8]} (Job: {miner_job_id}) using query_miners."
                            )
                            try:
                                trigger_payload_data = WeatherStartInferenceData(
                                    job_id=miner_job_id
                                )
                                trigger_payload = {
                                    "nonce": str(uuid.uuid4()),
                                    "data": trigger_payload_data.model_dump(),
                                }
                                endpoint = "/weather-start-inference"

                                all_responses = await validator.query_miners(
                                    payload=trigger_payload,
                                    endpoint=endpoint,
                                    hotkeys=[miner_hk],
                                )

                                trigger_response = all_responses.get(miner_hk)

                                parsed_response = trigger_response
                                if (
                                    isinstance(trigger_response, dict)
                                    and "text" in trigger_response
                                ):
                                    try:
                                        parsed_response = loads(
                                            trigger_response["text"]
                                        )
                                    except (Exception, TypeError) as json_err:
                                        logger.warning(
                                            f"[Run {run_id}] Failed to parse trigger response text for {miner_hk[:8]}: {json_err}"
                                        )
                                        parsed_response = {
                                            "status": "parse_error",
                                            "message": str(json_err),
                                        }

                                if (
                                    parsed_response
                                    and parsed_response.get("status")
                                    == WeatherTaskStatus.INFERENCE_STARTED.value
                                ):
                                    logger.info(
                                        f"[Run {run_id}] Successfully triggered inference for {miner_hk[:8]} (Job: {miner_job_id})."
                                    )
                                    return resp_id, True
                                else:
                                    logger.warning(
                                        f"[Run {run_id}] Failed to trigger inference for {miner_hk[:8]} (Job: {miner_job_id}). Response: {parsed_response}"
                                    )
                                    return resp_id, False
                            except Exception as trigger_err:
                                logger.error(
                                    f"[Run {run_id}] Error triggering inference for {miner_hk[:8]} (Job: {miner_job_id}): {trigger_err}",
                                    exc_info=True,
                                )
                                return resp_id, False

                        for resp_id, miner_hk, miner_job_id in miners_to_trigger:
                            trigger_tasks.append(
                                _trigger_single_miner(resp_id, miner_hk, miner_job_id)
                            )

                        trigger_results = await asyncio.gather(
                            *trigger_tasks, return_exceptions=True
                        )

                        final_update_tasks = []
                        triggered_count = 0
                        for i, result in enumerate(trigger_results):
                            if isinstance(result, Exception):
                                logger.error(
                                    f"[Run {run_id}] Trigger task {i} failed with exception: {result}"
                                )
                                continue
                            resp_id, success = result
                            if success:
                                triggered_count += 1
                                final_update_tasks.append(
                                    self.db_manager.execute(
                                        "UPDATE weather_miner_responses SET status = 'inference_triggered' WHERE id = :id",
                                        {"id": resp_id},
                                    )
                                )
                            # Yield control if processing many results
                            if (
                                len(trigger_results) > 20 and i % 20 == 19
                            ):  # Yield every 20 items after the 20th
                                await asyncio.sleep(0)

                        if final_update_tasks:
                            results = await asyncio.gather(
                                *final_update_tasks, return_exceptions=True
                            )
                        failed_updates = sum(
                            1 for r in results if isinstance(r, Exception)
                        )
                        if failed_updates > 0:
                            logger.error(
                                f"[Run {run_id}] {failed_updates} final DB updates failed"
                            )
                        logger.info(
                            f"[Run {run_id}] Completed inference trigger process. Successfully triggered {triggered_count}/{len(miners_to_trigger)} miners."
                        )
                        if triggered_count > 0:
                            await _update_run_status(
                                self, run_id, "awaiting_inference_results"
                            )
                        else:
                            await _update_run_status(
                                self, run_id, "inference_trigger_failed"
                            )  # No miners successfully triggered
                    else:
                        logger.warning(
                            f"[Run {run_id}] No miners eligible for inference trigger after hash verification."
                        )
                        await _update_run_status(self, run_id, "no_matching_hashes")

                    logger.info(
                        f"[ValidatorExecute] Concluded processing for run {run_id}. Triggering validator_score."
                    )
                    await self.validator_score()

                    if self.test_mode:
                        logger.info(
                            f"TEST MODE: validator_execute iteration for run {run_id} complete."
                        )
                        if (
                            hasattr(self, "last_test_mode_run_id")
                            and self.last_test_mode_run_id == run_id
                        ):
                            logger.info(
                                f"TEST MODE: Waiting for Day-1 scoring of run {run_id} to complete..."
                            )

                            # Use optional timeout or wait indefinitely for long-running scoring
                            timeout = await self._get_scoring_timeout_if_enabled()
                            if timeout:
                                logger.info(
                                    f"TEST MODE: Using safety timeout of {timeout:.0f} seconds for Day-1 scoring"
                                )
                            else:
                                logger.info(
                                    "TEST MODE: No timeout - waiting indefinitely for Day-1 scoring to complete (long-running operation)"
                                )

                            try:
                                if timeout:
                                    # Use timeout with periodic progress logging
                                    await self._wait_for_scoring_with_progress(
                                        self.test_mode_run_scored_event, timeout, run_id
                                    )
                                else:
                                    # Wait indefinitely with periodic progress logging
                                    await self._wait_for_scoring_with_progress(
                                        self.test_mode_run_scored_event, None, run_id
                                    )
                                logger.info(
                                    f"TEST MODE: Day-1 scoring for run {run_id} completed successfully."
                                )
                            except asyncio.TimeoutError:
                                logger.error(
                                    f"TEST MODE: Safety timeout reached waiting for Day-1 scoring of run {run_id} after {timeout:.0f} seconds."
                                )
                            self.test_mode_run_scored_event.clear()
                        else:
                            logger.info(
                                f"TEST MODE: Not waiting for scoring event as run_id ({run_id}) doesn't match last_test_mode_run_id ({getattr(self, 'last_test_mode_run_id', None)})."
                            )
                        logger.info(
                            "TEST MODE: Exiting validator loop after one successful attempt or error within the attempt."
                        )
                        break

            except Exception as loop_err:
                logger.error(
                    f"Error in validator_execute main loop: {loop_err}", exc_info=True
                )
                await validator.update_task_status("weather", "error")
                if "run_id" in locals() and run_id is not None:
                    try:
                        await _update_run_status(
                            self,
                            run_id,
                            "error",
                            error_message=f"Unhandled loop error: {loop_err}",
                        )
                    except:
                        pass

                if self.test_mode:
                    logger.info(
                        "TEST MODE: Encountered an error. Exiting validator_execute loop."
                    )
                    break

                await asyncio.sleep(600)  # Sleep only if not in test mode

    async def validator_score(self, result=None, force_run_id=None):
        """
        Initiates the verification process for completed miner responses.
        Actual scoring happens in background workers.

        Args:
            force_run_id: If provided, process this specific run regardless of normal criteria
        """
        logger.info("Validator scoring check initiated...")

        verification_wait_minutes_actual = self.config.get(
            "verification_wait_minutes", 30
        )
        if self.test_mode:
            logger.info(
                "[validator_score] TEST MODE: Setting verification_wait_minutes to 0 for immediate processing."
            )
            verification_wait_minutes_actual = 0
            logger.info(
                "[validator_score] TEST MODE: Adding a 30-second delay before processing runs for miner data preparation."
            )
            await asyncio.sleep(30)

        if force_run_id:
            # Process a specific run for recovery
            logger.info(
                f"[validator_score] Processing specific run {force_run_id} for recovery"
            )
            query = """
            SELECT id, gfs_init_time_utc 
            FROM weather_forecast_runs
            WHERE id = :run_id
            AND (status = 'awaiting_inference_results' OR status = 'verifying_miner_forecasts')
            """
            forecast_runs = await self.db_manager.fetch_all(
                query, {"run_id": force_run_id}
            )
        else:
            # Normal processing - look for runs awaiting inference results OR already verifying with verified responses
            cutoff_time = datetime.now(timezone.utc) - timedelta(
                minutes=verification_wait_minutes_actual
            )

            query = """
            SELECT DISTINCT wfr.id, wfr.gfs_init_time_utc, wfr.status, wfr.run_initiation_time
            FROM weather_forecast_runs wfr
            WHERE (
                (wfr.status = 'awaiting_inference_results' AND wfr.run_initiation_time < :cutoff_time)
                OR 
                (wfr.status = 'verifying_miner_forecasts' AND EXISTS (
                    SELECT 1 FROM weather_miner_responses wmr 
                    WHERE wmr.run_id = wfr.id 
                    AND wmr.verification_passed = TRUE 
                    AND wmr.status = 'verified_manifest_store_opened'
                ))
            )
            ORDER BY wfr.run_initiation_time ASC
            LIMIT 10
            """
            forecast_runs = await self.db_manager.fetch_all(
                query, {"cutoff_time": cutoff_time}
            )

        if not forecast_runs:
            logger.debug(
                f"No runs found awaiting inference results or ready for scoring within cutoff (test_mode active: {self.test_mode}, cutoff: {cutoff_time})."
            )
            return

        for run_record in forecast_runs:
            run_id = run_record["id"]
            logger.info(f"[Run {run_id}] Checking responses for verification...")
            current_run_status_rec = await self.db_manager.fetch_one(
                "SELECT status FROM weather_forecast_runs WHERE id = :run_id",
                {"run_id": run_id},
            )
            current_run_status = (
                current_run_status_rec["status"]
                if current_run_status_rec
                else "unknown"
            )

            if current_run_status == "awaiting_inference_results":
                await _update_run_status(self, run_id, "verifying_miner_forecasts")
            elif current_run_status == "verifying_miner_forecasts":
                logger.info(
                    f"[Run {run_id}] Already in verifying_miner_forecasts status - checking for scoring readiness"
                )
            else:
                logger.info(
                    f"[Run {run_id}] Status is '{current_run_status}' (expected 'awaiting_inference_results' or 'verifying_miner_forecasts'), skipping verification trigger step."
                )
                continue

            # Handle verification logic for both awaiting_inference_results and verifying_miner_forecasts
            if current_run_status == "awaiting_inference_results":
                # Defer per-miner verification entirely to worker pool
                logger.info(
                    f"[Run {run_id}] Deferring verification to worker pool; scheduler will claim per-miner verify items."
                )
                num_attempted_verification = 0
            else:
                # For runs already in verifying_miner_forecasts, skip verification and go straight to scoring check
                num_attempted_verification = 0
                logger.info(
                    f"[Run {run_id}] Run already in verifying_miner_forecasts - skipping verification, checking scoring readiness."
                )

            verified_responses_query = "SELECT COUNT(*) as count FROM weather_miner_responses WHERE run_id = :run_id AND verification_passed = TRUE"
            verified_count_result = await self.db_manager.fetch_one(
                verified_responses_query, {"run_id": run_id}
            )
            verified_count = (
                verified_count_result["count"] if verified_count_result else 0
            )

            current_run_status_rec_after_verify = await self.db_manager.fetch_one(
                "SELECT status FROM weather_forecast_runs WHERE id = :run_id",
                {"run_id": run_id},
            )
            current_run_status_after_verify = (
                current_run_status_rec_after_verify["status"]
                if current_run_status_rec_after_verify
                else "unknown"
            )

            if current_run_status_after_verify == "verifying_miner_forecasts":
                if verified_count >= 1:
                    logger.info(
                        f"[Run {run_id}] {verified_count} verified response(s). Triggering Day-1 QC scoring."
                    )
                    await _trigger_initial_scoring(self, run_id)
                elif num_attempted_verification > 0:
                    if verified_count == 0:
                        # Check if any miner responses are scheduled for retry
                        pending_retry_q = "SELECT COUNT(*) AS cnt FROM weather_miner_responses WHERE run_id = :run_id AND status = 'retry_scheduled'"
                        retry_cnt_rec = await self.db_manager.fetch_one(
                            pending_retry_q, {"run_id": run_id}
                        )
                        retry_cnt = retry_cnt_rec["cnt"] if retry_cnt_rec else 0
                        if retry_cnt > 0:
                            logger.info(
                                f"[Run {run_id}] All {num_attempted_verification} verifications failed but {retry_cnt} miner responses are scheduled for retry. Keeping run in 'verifying_miner_forecasts'."
                            )
                        else:
                            logger.warning(
                                f"[Run {run_id}] No responses passed verification and no retries pending. Marking as all_forecasts_failed_verification."
                            )
                            await _update_run_status(
                                self, run_id, "all_forecasts_failed_verification"
                            )
                else:
                    logger.warning(
                        f"[Run {run_id}] Run was '{current_run_status_after_verify}' but no 'inference_triggered' miner responses found to verify. Setting status to 'stalled_no_valid_forecasts'."
                    )
                    await _update_run_status(self, run_id, "stalled_no_valid_forecasts")
            else:
                logger.info(
                    f"[Run {run_id}] Status changed from 'verifying_miner_forecasts' to '{current_run_status_after_verify}' during verification logic. No further status update needed here."
                )

    async def update_combined_weather_scores(
        self, run_id_trigger: Optional[int] = None, force_phase: Optional[str] = None
    ):
        logger.info(
            f"[CombinedWeatherScore] Updating combined weather scores (triggered by run {run_id_trigger if run_id_trigger else 'periodic/manual call'})."
        )

        # If triggered by a specific run, check if that run actually has completed scoring
        if run_id_trigger:
            run_check = await self.db_manager.fetch_one(
                """SELECT status FROM weather_forecast_runs WHERE id = :run_id""",
                {"run_id": run_id_trigger},
            )
            if run_check and run_check["status"] in [
                "day1_scoring_started",
                "initial_scoring_queued",
                "verifying_miner_forecasts",
            ]:
                logger.warning(
                    f"[CombinedWeatherScore] Run {run_id_trigger} is still in scoring phase (status: {run_check['status']}), skipping score update to avoid using stale data"
                )
                return

        latest_day1_scores_array = np.full(256, 0.0)
        latest_era5_composite_scores_array = np.full(256, 0.0)

        active_uids = set()
        async with self.db_manager.session(
            operation_name="fetch_active_miner_uids_for_combined_score"
        ) as session:
            active_miners_query = "SELECT DISTINCT uid FROM node_table WHERE hotkey IS NOT NULL AND uid >= 0 AND uid < 256"
            result = await session.execute(text(active_miners_query))
            active_miner_uids_records = result.fetchall()
            active_uids = {rec.uid for rec in active_miner_uids_records}

        day1_qc_score_type = "day1_qc_score"
        query_day1_miner_scores = """
            SELECT score, calculation_time FROM weather_miner_scores 
            WHERE miner_uid = :miner_uid AND score_type = :score_type
            ORDER BY calculation_time DESC LIMIT 1
        """
        latest_day1_timestamp = None

        for uid in active_uids:
            async with self.db_manager.session(
                operation_name="fetch_latest_day1_score_for_uid"
            ) as session:
                result = await session.execute(
                    text(query_day1_miner_scores),
                    {"miner_uid": uid, "score_type": day1_qc_score_type},
                )
                res_day1 = result.fetchone()
                if (
                    res_day1
                    and res_day1.score is not None
                    and np.isfinite(res_day1.score)
                ):
                    latest_day1_scores_array[uid] = float(res_day1.score)
                    if (
                        latest_day1_timestamp is None
                        or res_day1.calculation_time > latest_day1_timestamp
                    ):
                        latest_day1_timestamp = res_day1.calculation_time

        if latest_day1_timestamp:
            logger.info(
                f"[CombinedWeatherScore] Fetched latest Day-1 QC scores for {len(active_uids)} active UIDs, latest timestamp: {latest_day1_timestamp}."
            )
        else:
            logger.warning(
                "[CombinedWeatherScore] No Day-1 QC scores found in weather_miner_scores for active UIDs."
            )
            latest_day1_timestamp = datetime.now(timezone.utc)

        # Progressive scoring: ERA5 scores may come from multiple partial scoring runs
        # This query will get the latest composite score regardless of progressive timing
        era5_composite_score_type = "era5_final_composite_score"
        query_era5_composite = """
            SELECT score FROM weather_miner_scores WHERE miner_uid = :miner_uid AND score_type = :score_type
            ORDER BY calculation_time DESC LIMIT 1
        """
        for uid in active_uids:
            async with self.db_manager.session(
                operation_name="fetch_latest_era5_composite_score_for_uid"
            ) as session:
                result = await session.execute(
                    text(query_era5_composite),
                    {"miner_uid": uid, "score_type": era5_composite_score_type},
                )
                res_era5 = result.fetchone()
                if (
                    res_era5
                    and res_era5.score is not None
                    and np.isfinite(res_era5.score)
                ):
                    latest_era5_composite_scores_array[uid] = float(res_era5.score)
        logger.info(
            f"[CombinedWeatherScore] Fetched ERA5 composite scores for {len(active_uids)} active UIDs."
        )

        W_day1 = self.config.get("weather_score_day1_weight", 0.2)
        W_era5 = self.config.get("weather_score_era5_weight", 0.8)
        proportional_weather_scores = (latest_day1_scores_array * W_day1) + (
            latest_era5_composite_scores_array * W_era5
        )
        logger.info(f"[CombinedWeatherScore] Calculated proportional scores.")

        # Calculate availability flags before clearing arrays
        has_era5_scores = np.any(latest_era5_composite_scores_array > 0)
        has_day1_scores = np.any(
            proportional_weather_scores > 0
        )  # This includes day1 component

        # MEMORY LEAK FIX: Clear large score arrays immediately after use
        try:
            del latest_day1_scores_array, latest_era5_composite_scores_array
            import gc

            gc.collect()
            logger.debug("Weather score calculation: cleared intermediate score arrays")
        except Exception:
            pass

        # Apply excellence bonuses (unchanged)
        miner_bonuses_applied = np.zeros(256)
        bonus_categories = [
            {
                "id": "top_weather",
                "score_field": "weather",
                "min_threshold": 0.7,
                "higher_is_better": True,
            },
            {
                "id": "top_geo",
                "score_field": "geomagnetic",
                "min_threshold": 0.8,
                "higher_is_better": True,
            },
            {
                "id": "top_soil",
                "score_field": "soil",
                "min_threshold": 0.6,
                "higher_is_better": True,
            },
        ]
        bonus_value_add = 0.05  # Small bonus for excellence

        for category in bonus_categories:
            score_field = category["score_field"]
            min_threshold = category["min_threshold"]
            higher_is_better = category["higher_is_better"]

            candidate_scores_for_category = []
            if score_field == "weather":
                for uid in active_uids:
                    score = proportional_weather_scores[uid]
                    if np.isfinite(score) and score >= min_threshold:
                        candidate_scores_for_category.append((score, uid))

            if not candidate_scores_for_category:
                logger.info(
                    f"[CombinedWeatherScore] No eligible miners for bonus: {category['id']}"
                )
                continue

            candidate_scores_for_category.sort(
                key=lambda x: x[0], reverse=higher_is_better
            )
            best_score = candidate_scores_for_category[0][0]
            winners = [
                uid
                for score, uid in candidate_scores_for_category
                if score == best_score
            ]

            if 1 <= len(winners) <= 2:
                bonus_per_winner = bonus_value_add / len(winners)
                for winner_uid in winners:
                    miner_bonuses_applied[winner_uid] += bonus_per_winner
                    logger.info(
                        f"[CombinedWeatherScore] Bonus for {category['id']} awarded to UID {winner_uid} (value: {bonus_per_winner:.3f}). Winning score: {best_score:.4f}"
                    )
            elif len(winners) > 2:
                logger.info(
                    f"[CombinedWeatherScore] Bonus for {category['id']} skipped: too many winners ({len(winners)}) with score {best_score:.4f}."
                )

        final_weather_scores_for_table = np.clip(
            proportional_weather_scores + miner_bonuses_applied, 0.0, 1.1
        )
        logger.info(
            f"[CombinedWeatherScore] Applied bonuses. Max combined score: {np.max(final_weather_scores_for_table):.4f}"
        )

        mock_evaluation_results = []
        for uid_idx in range(256):
            mock_evaluation_results.append(
                {
                    "miner_uid": uid_idx,
                    "final_score_for_uid": final_weather_scores_for_table[uid_idx],
                }
            )

        # Determine the appropriate score row ID based on available score types
        # (has_era5_scores and has_day1_scores calculated earlier before array cleanup)

        if run_id_trigger:
            # If triggered by a specific run, use run-specific naming
            if has_era5_scores:
                # If we have ERA5 scores, this run has completed final scoring
                id_for_combined_row = f"final_weather_scores_{run_id_trigger}"
                logger.info(
                    f"[CombinedWeatherScore] Creating final weather scores for run {run_id_trigger}"
                )
            else:
                # Only day1 scores available, this is initial scoring
                id_for_combined_row = f"initial_weather_scores_{run_id_trigger}"
                logger.info(
                    f"[CombinedWeatherScore] Creating initial weather scores for run {run_id_trigger}"
                )
        else:
            # Periodic/manual call - use generic naming for overall system state
            if has_era5_scores:
                id_for_combined_row = "final_weather_scores"
            else:
                id_for_combined_row = "initial_weather_scores"
            logger.info(
                f"[CombinedWeatherScore] Creating combined weather scores (periodic): {id_for_combined_row}"
            )

        timestamp_for_combined_score_row = datetime.now(timezone.utc)

        await self.build_score_row(
            run_id=id_for_combined_row,
            gfs_init_time=timestamp_for_combined_score_row,
            evaluation_results=mock_evaluation_results,
            task_name_prefix="weather",
        )

        # Cleanup: If we just created a final score row for a specific run, remove the corresponding initial row
        if (
            run_id_trigger
            and has_era5_scores
            and id_for_combined_row.startswith("final_weather_scores_")
        ):
            initial_row_id = f"initial_weather_scores_{run_id_trigger}"
            try:
                cleanup_query = """
                DELETE FROM score_table 
                WHERE task_name = 'weather' AND task_id = :initial_task_id
                """
                await self.db_manager.execute(
                    cleanup_query, {"initial_task_id": initial_row_id}
                )
                logger.info(
                    f"[CombinedWeatherScore] Cleaned up initial score row: {initial_row_id}"
                )
            except Exception as e:
                logger.warning(
                    f"[CombinedWeatherScore] Failed to cleanup initial score row {initial_row_id}: {e}"
                )

        logger.info(
            f"[CombinedWeatherScore] Update completed for 'weather' (task_id: {id_for_combined_row})."
        )

    ############################################################
    # Miner methods
    ############################################################

    async def miner_preprocess(
        self,
        data: Optional[Dict[str, Any]] = None,
    ) -> Optional[Batch]:
        """
        Loads and preprocesses the input GFS data payload received from the validator
        by calling the dedicated preprocessing function.

        Args:
            data: Dictionary containing the raw payload from the validator.

        Returns:
            An aurora.Batch object ready for model inference, or None if preprocessing fails.
        """
        logger.debug("Calling prepare_miner_batch_from_payload...")
        try:
            result_batch = await prepare_miner_batch_from_payload(data)
            logger.debug(
                f"prepare_miner_batch_from_payload returned: {type(result_batch)}"
            )
            return result_batch
        except Exception as e:
            logger.error(
                f"Error calling prepare_miner_batch_from_payload: {e}", exc_info=True
            )
            return None

    async def miner_execute(
        self, data: Dict[str, Any], miner
    ) -> Optional[Dict[str, Any]]:
        """
        Handles the initial request from the validator, preprocesses data,
        creates a job record, launches the background inference task,
        and returns an immediate 'Accepted' response.
        Checks for existing jobs for the same forecast time to avoid redundant runs.
        """
        logger.info("Miner execute called for WeatherTask")

        # Extract validator and miner hotkeys for deterministic job ID generation
        validator_hotkey = data.get("sender_hotkey", "unknown")
        miner_hotkey = self.keypair.ss58_address if self.keypair else "unknown_miner"
        payload_data = data.get("data", {})

        try:
            gfs_init_time = payload_data.get("forecast_start_time")
            if not isinstance(gfs_init_time, datetime):
                try:
                    gfs_init_time_str = str(gfs_init_time)
                    if gfs_init_time_str.endswith("Z"):
                        gfs_init_time_str = gfs_init_time_str[:-1] + "+00:00"
                    gfs_init_time = datetime.fromisoformat(gfs_init_time_str)
                    if gfs_init_time.tzinfo is None:
                        gfs_init_time = gfs_init_time.replace(tzinfo=timezone.utc)
                    else:
                        gfs_init_time = gfs_init_time.astimezone(timezone.utc)

                except (ValueError, TypeError) as parse_err:
                    logger.error(
                        f"[Miner Execute] Invalid forecast_start_time format: {gfs_init_time}. Error: {parse_err}"
                    )
                    return {
                        "status": "error",
                        "message": f"Invalid forecast_start_time format: {parse_err}",
                    }

            # Generate deterministic job ID using scheduled GFS time
            job_id = DeterministicJobID.generate_weather_job_id(
                gfs_init_time=gfs_init_time,  # SCHEDULED time, not processing time
                miner_hotkey=miner_hotkey,
                validator_hotkey=validator_hotkey,
                job_type="forecast",
            )

            logger.info(f"Processing request for GFS init time: {gfs_init_time}")
            logger.info(f"Generated deterministic job ID: {job_id}")

            # Basic validation checks after job ID generation
            if self.inference_runner is None:
                logger.error(
                    f"[Job {job_id}] Cannot execute: Inference Runner not available."
                )
                return {
                    "status": "error",
                    "message": "Miner inference component not ready",
                }
            if not data or "data" not in data:
                logger.error(f"[Job {job_id}] Invalid or missing payload data.")
                return {"status": "error", "message": "Invalid payload structure"}

            existing_job = await self.get_job_by_gfs_init_time(
                gfs_init_time, validator_hotkey
            )

            if existing_job:
                existing_job_id = existing_job["id"]
                existing_status = existing_job["status"]
                logger.info(
                    f"[Job {existing_job_id}] Found existing {existing_status} job for GFS init time {gfs_init_time} and validator {validator_hotkey[:8]}. Expected deterministic ID: {job_id}"
                )

                # Verify the existing job ID matches our deterministic generation
                if existing_job_id == job_id:
                    logger.info(
                        f"[Job {job_id}] ✅ Existing job ID matches deterministic generation. Reusing."
                    )
                else:
                    logger.warning(
                        f"[Job {job_id}] ⚠️  Existing job ID {existing_job_id} doesn't match deterministic ID {job_id}. This might be from before deterministic upgrade."
                    )

                return {
                    "status": "accepted",
                    "job_id": existing_job_id,
                    "message": f"Accepted. Reusing existing {existing_status} job.",
                }

            # Check if there's completed inference for this GFS timestep from any other validator
            # If so, create a new job record but reuse the existing inference files
            completed_inference_query = """
                SELECT id, target_netcdf_path, verification_hash
                FROM weather_miner_jobs 
                WHERE gfs_init_time_utc = :gfs_time 
                AND status = 'completed'
                AND target_netcdf_path IS NOT NULL
                AND verification_hash IS NOT NULL
                ORDER BY processing_end_time DESC 
                LIMIT 1
            """
            completed_inference = await self.db_manager.fetch_one(
                completed_inference_query, {"gfs_time": gfs_init_time}
            )

            if completed_inference:
                logger.info(
                    f"[Job {job_id}] Found completed inference for GFS time {gfs_init_time} from job {completed_inference['id']}. Creating new job record but reusing files."
                )

                # Create new job record with reused files
                insert_query = """
                    INSERT INTO weather_miner_jobs (id, validator_request_time, validator_hotkey, gfs_init_time_utc, gfs_input_metadata, status, processing_start_time, processing_end_time, target_netcdf_path, verification_hash)
                    VALUES (:id, :req_time, :val_hk, :gfs_init, :gfs_meta, :status, :proc_start, :proc_end, :target_path, :hash)
                """
                if gfs_init_time.tzinfo is None:
                    gfs_init_time = gfs_init_time.replace(tzinfo=timezone.utc)

                await self.db_manager.execute(
                    insert_query,
                    {
                        "id": job_id,
                        "req_time": datetime.now(timezone.utc),
                        "val_hk": validator_hotkey,
                        "gfs_init": gfs_init_time,
                        "gfs_meta": dumps(payload_data, default=str),
                        "status": "completed",
                        "proc_start": datetime.now(timezone.utc),
                        "proc_end": datetime.now(timezone.utc),
                        "target_path": completed_inference["target_netcdf_path"],
                        "hash": completed_inference["verification_hash"],
                    },
                )

                logger.info(
                    f"[Job {job_id}] Created new job record reusing inference files from job {completed_inference['id']}."
                )
                return {
                    "status": "accepted",
                    "job_id": job_id,
                    "message": f"Accepted. Created new job reusing existing inference for timestep {gfs_init_time}.",
                }

            logger.info(
                f"[Job {job_id}] No existing inference found for GFS time {gfs_init_time}. Creating new job for computation."
            )

            logger.info(f"[Job {job_id}] Starting preprocessing...")
            preprocessing_start_time = time.time()
            initial_batch = await self.miner_preprocess(data=payload_data)
            if initial_batch is None:
                logger.error(f"[Job {job_id}] Preprocessing failed.")
                return {"status": "error", "message": "Failed to preprocess input data"}
            logger.info(
                f"[Job {job_id}] Preprocessing completed in {time.time() - preprocessing_start_time:.2f} seconds."
            )

            # Check if job already exists (multiple validators can generate same job ID)
            existing_job_check = """
                SELECT id, status FROM weather_miner_jobs 
                WHERE id = :job_id
            """
            existing_job = await self.db_manager.fetch_one(
                existing_job_check, {"job_id": job_id}
            )

            if existing_job:
                logger.info(
                    f"[Job {job_id}] Job already exists with status '{existing_job['status']}'. Skipping duplicate creation (likely from another validator)."
                )
            else:
                logger.info(f"[Job {job_id}] Creating initial job record in database.")
                insert_query = """
                    INSERT INTO weather_miner_jobs (id, validator_request_time, validator_hotkey, gfs_init_time_utc, gfs_input_metadata, status, processing_start_time)
                    VALUES (:id, :req_time, :val_hk, :gfs_init, :gfs_meta, :status, :proc_start)
                """
                if gfs_init_time.tzinfo is None:
                    gfs_init_time = gfs_init_time.replace(tzinfo=timezone.utc)

                await self.db_manager.execute(
                    insert_query,
                    {
                        "id": job_id,
                        "req_time": datetime.now(timezone.utc),
                        "val_hk": validator_hotkey,
                        "gfs_init": gfs_init_time,
                        "gfs_meta": dumps(payload_data, default=str),
                        "status": "received",
                        "proc_start": datetime.now(timezone.utc),
                    },
                )
                logger.info(f"[Job {job_id}] Initial job record created.")

            logger.info(f"[Job {job_id}] Launching background inference task...")

            return {
                "status": "accepted",
                "job_id": job_id,
                "message": "Weather forecast job accepted for processing.",
            }

        except Exception as e:
            # Use temporary job ID for error logging if we couldn't generate the deterministic one
            temp_job_id = f"error_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
            logger.error(
                f"[Job {temp_job_id}] Error during initial miner_execute: {e}",
                exc_info=True,
            )
            return {
                "status": "error",
                "job_id": None,
                "message": f"Failed to initiate job: {e}",
            }

    async def handle_kerchunk_request(self, job_id: str) -> Dict[str, Any]:
        """
        Handle a request for forecast data for a specific job.
        Now returns information about the Zarr store directly instead of Kerchunk JSON.

        RESILIENCE: Implements fallback strategies for database synchronization scenarios.

        Args:
            job_id: The unique identifier for the job

        Returns:
            Dict containing status, message, and if completed:
            - zarr_store_url: URL to access the Zarr store
            - verification_hash: Hash to verify forecast integrity
            - access_token: JWT token for accessing forecast files
        """
        logger.info(f"Handling forecast data request for job_id: {job_id}")

        try:
            # Primary lookup: Try to find the exact job ID
            query = """
            SELECT id as job_id, status, target_netcdf_path, verification_hash, error_message, gfs_init_time_utc, validator_hotkey
            FROM weather_miner_jobs
            WHERE id = :job_id 
            """
            job = await self.db_manager.fetch_one(query, {"job_id": job_id})

            if not job:
                # DATABASE SYNC RESILIENCE: Try alternative lookup methods
                logger.warning(
                    f"Job not found for job_id: {job_id}. Attempting database sync resilience fallback..."
                )

                # Import the fallback function
                from .processing.weather_logic import find_job_by_alternative_methods

                fallback_job = await find_job_by_alternative_methods(
                    self, job_id, "current_miner"
                )

                if fallback_job:
                    job = fallback_job
                    original_job_id = job["job_id"]
                    logger.warning(
                        f"Database sync resilience: Using equivalent job {original_job_id} for missing job {job_id}"
                    )
                else:
                    logger.warning(
                        f"Job not found for job_id: {job_id} even after fallback attempts"
                    )
                    return self._validate_and_format_response(
                        {
                            "status": WeatherTaskStatus.NOT_FOUND.value,
                            "message": f"Job with ID {job_id} not found (tried fallback methods)",
                        },
                        ["status", "message"],
                    )

            if job["status"] == "completed":
                zarr_path_str = job["target_netcdf_path"]
                verification_hash = job["verification_hash"]

                # Auto-fix missing verification hash for completed jobs with zarr stores
                if zarr_path_str and not verification_hash:
                    logger.warning(
                        f"Job {job_id} completed but missing verification hash. Attempting to auto-generate..."
                    )
                    try:
                        zarr_path = Path(zarr_path_str)
                        if zarr_path.exists():
                            # Generate manifest and signature for the existing zarr store
                            def _generate_manifest_sync():
                                from .utils.hashing import (
                                    generate_manifest_and_signature,
                                )

                                return generate_manifest_and_signature(
                                    zarr_store_path=zarr_path,
                                    miner_hotkey_keypair=(
                                        self.keypair if self.keypair else None
                                    ),
                                )

                            manifest_dict, signature_bytes, verification_hash = (
                                await asyncio.to_thread(_generate_manifest_sync)
                            )

                            if verification_hash:
                                # Update the database with the generated hash
                                update_query = "UPDATE weather_miner_jobs SET verification_hash = :hash WHERE id = :job_id"
                                await self.db_manager.execute(
                                    update_query,
                                    {"hash": verification_hash, "job_id": job_id},
                                )
                                logger.info(
                                    f"Job {job_id} auto-generated and saved verification hash: {verification_hash[:16]}..."
                                )
                            else:
                                logger.error(
                                    f"Job {job_id} failed to auto-generate verification hash"
                                )
                                return self._validate_and_format_response(
                                    {
                                        "status": WeatherTaskStatus.ERROR.value,
                                        "message": "Job completed but failed to generate verification hash",
                                    },
                                    ["status", "message"],
                                )
                        else:
                            logger.error(
                                f"Job {job_id} zarr path does not exist: {zarr_path_str}"
                            )
                            return self._validate_and_format_response(
                                {
                                    "status": WeatherTaskStatus.ERROR.value,
                                    "message": "Job completed but zarr store not found",
                                },
                                ["status", "message"],
                            )
                    except Exception as e:
                        logger.error(
                            f"Job {job_id} failed to auto-generate verification hash: {e}",
                            exc_info=True,
                        )
                        return self._validate_and_format_response(
                            {
                                "status": WeatherTaskStatus.ERROR.value,
                                "message": f"Job completed but failed to generate verification hash: {e}",
                            },
                            ["status", "message"],
                        )

                elif not zarr_path_str:
                    logger.error(f"Job {job_id} completed but missing zarr path")
                    return self._validate_and_format_response(
                        {
                            "status": WeatherTaskStatus.ERROR.value,
                            "message": "Job completed but zarr path missing",
                        },
                        ["status", "message"],
                    )

                zarr_path = Path(zarr_path_str)
                zarr_dir_name = zarr_path.name

                if not zarr_dir_name.endswith(".zarr"):
                    logger.error(f"Expected Zarr directory but found: {zarr_dir_name}")
                    return self._validate_and_format_response(
                        {
                            "status": WeatherTaskStatus.ERROR.value,
                            "message": "Invalid Zarr store format",
                        },
                        ["status", "message"],
                    )

                logger.info(
                    f"[Job {job_id}] Using Zarr directory for JWT claim: {zarr_dir_name}"
                )

                miner_jwt_secret_key = self.config.get(
                    "miner_jwt_secret_key", os.getenv("MINER_JWT_SECRET_KEY")
                )
                if not miner_jwt_secret_key:
                    logger.warning(
                        "MINER_JWT_SECRET_KEY not set in config or environment. Using default insecure key."
                    )
                    miner_jwt_secret_key = "insecure_default_key_for_development_only"
                jwt_algorithm = self.config.get("jwt_algorithm", "HS256")
                token_expire_minutes = int(
                    self.config.get("access_token_expire_minutes", 60)
                )

                token_data = {
                    "job_id": job_id,
                    "file_path": zarr_dir_name,
                    "exp": datetime.now(timezone.utc)
                    + timedelta(minutes=token_expire_minutes),
                }

                access_token = jwt.encode(
                    token_data, miner_jwt_secret_key, algorithm=jwt_algorithm
                )

                zarr_url_for_response = f"/forecasts/{zarr_dir_name}"

                return self._validate_and_format_response(
                    {
                        "status": WeatherTaskStatus.COMPLETED.value,
                        "message": "Forecast completed and ready for access",
                        "zarr_store_url": zarr_url_for_response,
                        "verification_hash": verification_hash,
                        "access_token": access_token,
                    },
                    ["status", "message"],
                )

            elif job["status"] == "error":
                logger.warning(
                    f"[Job {job_id}] Forecast data requested but job failed. Error: {job['error_message'] or 'Unknown error'}"
                )
                return self._validate_and_format_response(
                    {
                        "status": WeatherTaskStatus.ERROR.value,
                        "message": f"Job failed: {job['error_message'] or 'Unknown error'}",
                    },
                    ["status", "message"],
                )
            else:
                logger.info(
                    f"[Job {job_id}] Forecast data requested but job still processing (status: {job['status']}). Validator will need to retry later."
                )
                return self._validate_and_format_response(
                    {
                        "status": WeatherTaskStatus.PROCESSING.value,
                        "message": f"Job is currently in status: {job['status']}",
                    },
                    ["status", "message"],
                )

        except Exception as e:
            logger.error(
                f"Error handling forecast data request for job_id {job_id}: {e}",
                exc_info=True,
            )
            return self._validate_and_format_response(
                {
                    "status": WeatherTaskStatus.ERROR.value,
                    "message": f"Failed to process request: {str(e)}",
                },
                ["status", "message"],
            )

    async def handle_initiate_fetch(
        self, request_data: "WeatherInitiateFetchData"
    ) -> Dict[str, Any]:
        """
        Handles the /weather-initiate-fetch request.
        Creates a job record and launches the background task for fetching GFS and hashing.
        If a failed job for the same timestep exists, it will be reset and retried.
        """
        if self.node_type != "miner":
            logger.error("handle_initiate_fetch called on non-miner node.")
            return self._validate_and_format_response(
                {
                    "status": WeatherTaskStatus.ERROR.value,
                    "job_id": None,
                    "message": "Invalid node type",
                },
                ["status", "message"],
            )

        try:
            t0_run_time = request_data.forecast_start_time
            t_minus_6_run_time = request_data.previous_step_time

            if t0_run_time.tzinfo is None:
                t0_run_time = t0_run_time.replace(tzinfo=timezone.utc)
            if t_minus_6_run_time.tzinfo is None:
                t_minus_6_run_time = t_minus_6_run_time.replace(tzinfo=timezone.utc)

            logger.info(f"[Miner] Received initiate_fetch request for T0={t0_run_time}")

            # Extract validator hotkey from request data
            validator_hotkey = request_data.validator_hotkey

            # Find existing job for this exact time and validator
            if validator_hotkey:
                existing_job_query = """
                    SELECT id, status, input_data_hash
                    FROM weather_miner_jobs 
                    WHERE gfs_init_time_utc = :gfs_init
                    AND gfs_t_minus_6_time_utc = :gfs_t_minus_6
                    AND validator_hotkey = :validator_hotkey
                    ORDER BY validator_request_time DESC
                    LIMIT 1
                """
                existing_job = await self.db_manager.fetch_one(
                    existing_job_query,
                    {
                        "gfs_init": t0_run_time,
                        "gfs_t_minus_6": t_minus_6_run_time,
                        "validator_hotkey": validator_hotkey,
                    },
                )
            else:
                # Fallback to original behavior if validator hotkey is not available
                existing_job_query = """
                    SELECT id, status, input_data_hash
                    FROM weather_miner_jobs 
                    WHERE gfs_init_time_utc = :gfs_init
                    AND gfs_t_minus_6_time_utc = :gfs_t_minus_6
                    ORDER BY validator_request_time DESC
                    LIMIT 1
                """
                existing_job = await self.db_manager.fetch_one(
                    existing_job_query,
                    {"gfs_init": t0_run_time, "gfs_t_minus_6": t_minus_6_run_time},
                )

            if existing_job:
                job_id = existing_job["id"]
                status = existing_job["status"]

                # If the job is in a recoverable (failed) state, reset and retry it.
                if status in [
                    "error",
                    "fetch_error",
                    "failed",
                    "input_hash_mismatch",
                    "input_hash_timeout",
                    "input_poll_error",
                ]:
                    logger.warning(
                        f"[Miner Job {job_id}] Found existing FAILED job (status: {status}). Resetting and retrying."
                    )

                    # Reset status to 'fetch_queued' and clear any previous error message.
                    await update_job_status(
                        self, job_id, "fetch_queued", error_message=""
                    )

                    # Relaunch the background task to fetch and hash the data.
                    asyncio.create_task(
                        fetch_and_hash_gfs_task(
                            task_instance=self,
                            job_id=job_id,
                            t0_run_time=t0_run_time,
                            t_minus_6_run_time=t_minus_6_run_time,
                        )
                    )

                    return self._validate_and_format_response(
                        {
                            "status": WeatherTaskStatus.FETCH_ACCEPTED.value,
                            "job_id": job_id,
                            "message": f"Retrying existing failed job (previous status: {status}).",
                        },
                        ["status", "job_id"],
                    )

                # If the job is in a valid, non-failed state, reuse it.
                else:
                    logger.info(
                        f"[Miner Job {job_id}] Found existing active job (status: {status}). Reusing."
                    )
                    response = {
                        "status": WeatherTaskStatus.FETCH_ACCEPTED.value,
                        "job_id": job_id,
                        "message": f"Reusing existing job (status: {status})",
                    }
                    if existing_job.get("input_data_hash"):
                        response["input_data_hash"] = existing_job["input_data_hash"]
                    return self._validate_and_format_response(
                        response, ["status", "job_id"]
                    )

            # Check if input data has already been fetched and hashed for this timestep
            # This allows reuse of GFS data and hash across different validator requests
            existing_input_query = """
                SELECT id, input_data_hash, input_batch_pickle_path, status
                FROM weather_miner_jobs 
                WHERE gfs_init_time_utc = :gfs_init
                AND gfs_t_minus_6_time_utc = :gfs_t_minus_6
                AND input_data_hash IS NOT NULL
                AND input_batch_pickle_path IS NOT NULL
                AND status IN ('input_hashed_awaiting_validation', 'in_progress', 'completed')
                ORDER BY validator_request_time DESC
                LIMIT 1
            """
            existing_input_job = await self.db_manager.fetch_one(
                existing_input_query,
                {"gfs_init": t0_run_time, "gfs_t_minus_6": t_minus_6_run_time},
            )

            # Extract miner hotkey for deterministic generation
            miner_hotkey = (
                self.keypair.ss58_address if self.keypair else "unknown_miner"
            )
            # Use the validator hotkey extracted above, or fall back to placeholder
            if not validator_hotkey:
                validator_hotkey = "unknown_validator"

            job_id = DeterministicJobID.generate_weather_job_id(
                gfs_init_time=t0_run_time,  # SCHEDULED GFS time
                miner_hotkey=miner_hotkey,
                validator_hotkey=validator_hotkey,
                job_type="forecast",  # Changed from "fetch" to "forecast" to unify job IDs
            )

            if existing_input_job:
                # Validate that the batch file still exists before reusing
                batch_file_path = Path(existing_input_job["input_batch_pickle_path"])
                if not batch_file_path.exists():
                    logger.warning(
                        f"[Miner Job {job_id}] Batch file {batch_file_path} from job {existing_input_job['id']} no longer exists. Cannot reuse input data."
                    )
                else:
                    # Reuse existing input data and hash
                    logger.info(
                        f"[Miner Job {job_id}] Found existing input data from job {existing_input_job['id']}. Reusing GFS data and hash."
                    )

                    # Check if job already exists before inserting
                    job_exists_check = """
                        SELECT id FROM weather_miner_jobs WHERE id = :job_id
                    """
                    job_exists = await self.db_manager.fetch_one(
                        job_exists_check, {"job_id": job_id}
                    )

                    if not job_exists:
                        insert_query = """
                            INSERT INTO weather_miner_jobs
                            (id, validator_request_time, validator_hotkey, gfs_init_time_utc, gfs_t_minus_6_time_utc, 
                             input_data_hash, input_batch_pickle_path, status)
                            VALUES (:id, :req_time, :val_hk, :gfs_init, :gfs_t_minus_6, :hash, :pickle_path, :status)
                        """
                        await self.db_manager.execute(
                            insert_query,
                            {
                                "id": job_id,
                                "req_time": datetime.now(timezone.utc),
                                "val_hk": validator_hotkey,
                                "gfs_init": t0_run_time,
                                "gfs_t_minus_6": t_minus_6_run_time,
                                "hash": existing_input_job["input_data_hash"],
                                "pickle_path": existing_input_job[
                                    "input_batch_pickle_path"
                                ],
                                "status": "input_hashed_awaiting_validation",
                            },
                        )
                        logger.info(
                            f"[Miner Job {job_id}] Created new job record reusing input data and hash from job {existing_input_job['id']}."
                        )
                    else:
                        logger.info(
                            f"[Miner Job {job_id}] Job already exists, skipping creation (likely from another validator)."
                        )
                    # Immediately start inference in background (idempotent, duplicate-safe)
                    try:
                        from .processing.weather_workers import run_inference_background
                        asyncio.create_task(run_inference_background(self, job_id))
                    except Exception as inf_err:
                        logger.warning(f"[Miner Job {job_id}] Failed to schedule inference: {inf_err}")

                    return self._validate_and_format_response(
                        {
                            "status": WeatherTaskStatus.FETCH_ACCEPTED.value,
                            "job_id": job_id,
                            "message": f"Accepted. Reusing existing input data and hash from job {existing_input_job['id']}.",
                            "input_data_hash": existing_input_job["input_data_hash"],
                        },
                        ["status", "job_id"],
                    )
            else:
                # No existing input data found, need to fetch and hash
                logger.info(
                    f"[Miner Job {job_id}] No existing input data found. Creating new job for GFS fetch and hash computation."
                )

                # Check if job already exists before inserting
                job_exists_check = """
                    SELECT id FROM weather_miner_jobs WHERE id = :job_id
                """
                job_exists = await self.db_manager.fetch_one(
                    job_exists_check, {"job_id": job_id}
                )

                if not job_exists:
                    insert_query = """
                        INSERT INTO weather_miner_jobs
                        (id, validator_request_time, validator_hotkey, gfs_init_time_utc, gfs_t_minus_6_time_utc, status)
                        VALUES (:id, :req_time, :val_hk, :gfs_init, :gfs_t_minus_6, :status)
                    """
                    await self.db_manager.execute(
                        insert_query,
                        {
                            "id": job_id,
                            "req_time": datetime.now(timezone.utc),
                            "val_hk": validator_hotkey,
                            "gfs_init": t0_run_time,
                            "gfs_t_minus_6": t_minus_6_run_time,
                            "status": "fetch_queued",
                        },
                    )
                    logger.info(
                        f"[Miner Job {job_id}] DB record created. Launching background fetch/hash task."
                    )
                else:
                    logger.info(
                        f"[Miner Job {job_id}] Job already exists, skipping creation (likely from another validator)."
                    )

                # Start input fetch/hash and inference concurrently (each is duplicate-safe)
                asyncio.create_task(
                    fetch_and_hash_gfs_task(
                        task_instance=self,
                        job_id=job_id,
                        t0_run_time=t0_run_time,
                        t_minus_6_run_time=t_minus_6_run_time,
                    )
                )
                try:
                    from .processing.weather_workers import run_inference_background
                    asyncio.create_task(run_inference_background(self, job_id))
                except Exception as inf_err:
                    logger.warning(f"[Miner Job {job_id}] Failed to schedule inference: {inf_err}")

            return self._validate_and_format_response(
                {
                    "status": WeatherTaskStatus.FETCH_ACCEPTED.value,
                    "job_id": job_id,
                    "message": "Fetch and hash process initiated.",
                },
                ["status", "job_id"],
            )

        except Exception as e:
            logger.error(
                f"[Miner] Error during handle_initiate_fetch: {e}", exc_info=True
            )
            return self._validate_and_format_response(
                {
                    "status": WeatherTaskStatus.ERROR.value,
                    "job_id": None,
                    "message": f"Failed to initiate fetch: {e}",
                },
                ["status", "message"],
            )

    async def handle_get_input_status(self, job_id: str) -> Dict[str, Any]:
        """
        Handles the /weather-get-input-status request.
        Returns the current status and input hash (if computed) for the job.
        If the job is ready for inference but hasn't been triggered, it reports
        the status as 'input_hashed_awaiting_validation' to conform to the validator.
        """
        if self.node_type != "miner":
            logger.error("handle_get_input_status called on non-miner node.")
            return self._validate_and_format_response(
                {
                    "status": WeatherTaskStatus.ERROR.value,
                    "job_id": job_id,
                    "message": "Invalid node type",
                },
                ["status", "job_id", "message"],
            )

        logger.debug(f"[Miner Job {job_id}] Received get_input_status request.")
        try:
            query = "SELECT status, input_data_hash, error_message FROM weather_miner_jobs WHERE id = :job_id"
            result = await self.db_manager.fetch_one(query, {"job_id": job_id})

            if not result:
                logger.warning(
                    f"[Miner Job {job_id}] Status requested for non-existent job."
                )
                return self._validate_and_format_response(
                    {
                        "status": WeatherTaskStatus.NOT_FOUND.value,
                        "job_id": job_id,
                        "message": "Job ID not found.",
                    },
                    ["status", "job_id", "message"],
                )

            status_to_report = result["status"]
            # If hashing is done and we are waiting for the validator to trigger inference,
            # report the specific status the validator is looking for.
            # For completed jobs, also report as awaiting validation so validator includes us in scoring
            if result.get("input_data_hash"):
                if status_to_report in ["in_progress", "completed"]:
                    status_to_report = (
                        WeatherTaskStatus.INPUT_HASHED_AWAITING_VALIDATION.value
                    )
                    logger.debug(
                        f"[Miner Job {job_id}] Job status '{result['status']}' with hash converted to '{status_to_report}' for validator compatibility"
                    )

            # Simplified reporting: expose raw job status for polling
            response = {
                "job_id": job_id,
                "status": status_to_report,
                "message": result.get("error_message"),
            }
            logger.debug(
                f"[Miner Job {job_id}] Reporting status: {response['status']}, Hash available: {response['input_data_hash'] is not None}"
            )
            return self._validate_and_format_response(response, ["job_id", "status"])

        except Exception as e:
            logger.error(
                f"[Miner Job {job_id}] Error during handle_get_input_status: {e}",
                exc_info=True,
            )
            return self._validate_and_format_response(
                {
                    "status": WeatherTaskStatus.ERROR.value,
                    "job_id": job_id,
                    "message": f"Failed to get status: {e}",
                },
                ["status", "job_id", "message"],
            )

    async def handle_start_inference(self, job_id: str) -> Dict[str, Any]:
        """
        Handles a request from a validator to start the inference process for a given job_id.
        This method should return quickly after launching the background inference task.
        Ensures we don't launch duplicate inference for the same timestep.
        """
        if not job_id:
            return self._validate_and_format_response(
                {
                    "status": WeatherTaskStatus.ERROR.value,
                    "message": "job_id is required.",
                },
                ["status", "message"],
            )

        logger.info(f"Miner received request to start inference for job_id: {job_id}")

        try:
            # Get detailed job information including GFS timesteps
            job_query = """
                SELECT status, runpod_job_id, gfs_init_time_utc, gfs_t_minus_6_time_utc, target_netcdf_path
                FROM weather_miner_jobs WHERE id = :job_id
            """
            job_details = await self.db_manager.fetch_one(job_query, {"job_id": job_id})

            if not job_details:
                return self._validate_and_format_response(
                    {
                        "status": WeatherTaskStatus.ERROR.value,
                        "message": f"Job ID {job_id} not found.",
                    },
                    ["status", "message"],
                )

            current_status = job_details["status"]
            gfs_init_time = job_details["gfs_init_time_utc"]

            # Check if inference is already running or completed for this job
            if current_status in ["in_progress", "completed"]:
                logger.info(
                    f"[{job_id}] Job already in status '{current_status}'. Not starting duplicate inference."
                )
                if current_status == "completed":
                    # For completed jobs, acknowledge that inference was successful
                    return self._validate_and_format_response(
                        {
                            "status": WeatherTaskStatus.INFERENCE_STARTED.value,
                            "message": f"Inference already completed successfully.",
                        },
                        ["status", "message"],
                    )
                else:
                    # For in-progress jobs, acknowledge that inference is ongoing
                    return self._validate_and_format_response(
                        {
                            "status": WeatherTaskStatus.INFERENCE_STARTED.value,
                            "message": f"Inference already in progress.",
                        },
                        ["status", "message"],
                    )

            # Check if THIS specific job is already in progress or completed
            if gfs_init_time:
                # Check the current job status to avoid duplicate processing
                current_job_check_query = """
                    SELECT id, status, validator_request_time, processing_start_time FROM weather_miner_jobs 
                    WHERE id = :current_job_id 
                    AND status IN ('in_progress', 'completed', 'processing', 'running_inference', 'processing_input', 'processing_output')
                    LIMIT 1
                """
                current_job = await self.db_manager.fetch_one(
                    current_job_check_query, {"current_job_id": job_id}
                )

                if current_job:
                    logger.info(
                        f"[{job_id}] This job is already in status '{current_job['status']}'. Acknowledging existing inference."
                    )
                    return self._validate_and_format_response(
                        {
                            "status": WeatherTaskStatus.INFERENCE_STARTED.value,
                            "message": f"Inference already in status: {current_job['status']}",
                        },
                        ["status", "message"],
                    )

                # Check if inference has already been completed for this GFS timestep by any validator
                # If so, reuse the existing files instead of running inference again
                completed_jobs_query = """
                    SELECT id, status, target_netcdf_path, verification_hash
                    FROM weather_miner_jobs 
                    WHERE gfs_init_time_utc = :gfs_time 
                    AND id != :current_job_id 
                    AND status = 'completed'
                    AND target_netcdf_path IS NOT NULL
                    AND verification_hash IS NOT NULL
                    ORDER BY processing_end_time DESC 
                    LIMIT 1
                """
                completed_job = await self.db_manager.fetch_one(
                    completed_jobs_query,
                    {"gfs_time": gfs_init_time, "current_job_id": job_id},
                )

                if completed_job:
                    logger.info(
                        f"[{job_id}] Found completed inference for GFS time {gfs_init_time} from job {completed_job['id']}. Reusing files instead of running new inference."
                    )

                    # Update current job to reuse the existing files
                    await self.db_manager.execute(
                        """
                        UPDATE weather_miner_jobs 
                        SET target_netcdf_path = :target_path,
                            verification_hash = :hash,
                            status = 'completed',
                            processing_end_time = NOW()
                        WHERE id = :job_id
                    """,
                        {
                            "job_id": job_id,
                            "target_path": completed_job["target_netcdf_path"],
                            "hash": completed_job["verification_hash"],
                        },
                    )

                    return self._validate_and_format_response(
                        {
                            "status": WeatherTaskStatus.INFERENCE_STARTED.value,
                            "message": f"Reusing completed inference from existing job for timestep {gfs_init_time}",
                        },
                        ["status", "message"],
                    )

                # Log other jobs for informational purposes
                other_jobs_query = """
                    SELECT id, status, validator_request_time FROM weather_miner_jobs 
                    WHERE gfs_init_time_utc = :gfs_time 
                    AND id != :current_job_id 
                    AND validator_request_time >= NOW() - INTERVAL '6 hours'
                    ORDER BY validator_request_time DESC LIMIT 5
                """
                other_jobs = await self.db_manager.fetch_all(
                    other_jobs_query,
                    {"gfs_time": gfs_init_time, "current_job_id": job_id},
                )

                if other_jobs:
                    logger.info(
                        f"[{job_id}] Found {len(other_jobs)} other job(s) for same timestep {gfs_init_time}:"
                    )
                    for job in other_jobs:
                        hours_ago = (
                            datetime.now(timezone.utc) - job["validator_request_time"]
                        ).total_seconds() / 3600
                        logger.info(
                            f"[{job_id}]   - Job {job['id']}: status='{job['status']}', requested {hours_ago:.1f}h ago"
                        )
                    logger.info(
                        f"[{job_id}] No completed inference found to reuse. Proceeding with new inference."
                    )
                else:
                    logger.info(
                        f"[{job_id}] No other jobs found for timestep {gfs_init_time}. Proceeding with new inference."
                    )

            # Check RunPod-specific duplicate prevention
            if job_details["runpod_job_id"] and current_status == "in_progress":
                logger.warning(
                    f"[{job_id}] Job already has RunPod ID and is in progress. Acknowledging existing inference."
                )
                return self._validate_and_format_response(
                    {
                        "status": WeatherTaskStatus.INFERENCE_STARTED.value,
                        "message": "Inference already in progress on RunPod.",
                    },
                    ["status", "message"],
                )

            inference_type = self.config.get("weather_inference_type", "local_model")
            logger.info(
                f"Launching inference for job {job_id} using type: {inference_type}"
            )

            if inference_type == "http_service":
                # Launch background task for HTTP service inference
                asyncio.create_task(self._run_inference_via_http_service(job_id))
                return self._validate_and_format_response(
                    {
                        "status": WeatherTaskStatus.INFERENCE_STARTED.value,
                        "message": "HTTP inference process initiated.",
                    },
                    ["status", "message"],
                )

            elif inference_type == "local_model":
                if not self.inference_runner or not self.inference_runner.model:
                    msg = "Local model selected but not loaded."
                    logger.error(f"[{job_id}] {msg}")
                    await update_job_status(self, job_id, "failed", msg)
                    return self._validate_and_format_response(
                        {"status": WeatherTaskStatus.ERROR.value, "message": msg},
                        ["status", "message"],
                    )

                # Launch background task for local model inference
                asyncio.create_task(
                    run_inference_background(task_instance=self, job_id=job_id)
                )
                return self._validate_and_format_response(
                    {
                        "status": WeatherTaskStatus.INFERENCE_STARTED.value,
                        "message": "Local inference process initiated.",
                    },
                    ["status", "message"],
                )

            else:
                msg = f"Unsupported inference type: {inference_type}"
                logger.error(f"[{job_id}] {msg}")
                await update_job_status(self, job_id, "failed", msg)
                return self._validate_and_format_response(
                    {"status": WeatherTaskStatus.ERROR.value, "message": msg},
                    ["status", "message"],
                )

        except Exception as e:
            logger.error(
                f"Unexpected error in handle_start_inference for job {job_id}: {e}",
                exc_info=True,
            )
            await update_job_status(self, job_id, "failed", f"Unhandled exception: {e}")
            return self._validate_and_format_response(
                {
                    "status": WeatherTaskStatus.ERROR.value,
                    "message": f"An unexpected error occurred: {e}",
                },
                ["status", "message"],
            )

    ############################################################
    # Helper Methods
    ############################################################

    async def cleanup_resources(self):
        """
        Cleans up old files from the local filesystem.
        """
        logger.info("Cleaning up weather task resources...")

        # Stop all background workers with proper awaiting
        logger.info("Stopping all background workers...")
        try:
            await self.stop_background_workers()
        except Exception as e:
            logger.error(f"Error during background worker cleanup: {e}")

        # Clean up ERA5 climatology dataset
        try:
            if (
                hasattr(self, "era5_climatology_ds")
                and self.era5_climatology_ds is not None
            ):
                logger.info("Closing ERA5 climatology dataset...")
                self.era5_climatology_ds.close()
                self.era5_climatology_ds = None
                logger.info("Closed ERA5 climatology dataset")
        except Exception as e:
            logger.warning(f"Error closing ERA5 climatology dataset: {e}")

        # Clean up any HTTP clients
        try:
            if (
                hasattr(self, "validator")
                and self.validator
                and hasattr(self.validator, "miner_client")
            ):
                logger.info("Closing validator HTTP clients...")
                if not self.validator.miner_client.is_closed:
                    await self.validator.miner_client.aclose()
                logger.info("Closed validator HTTP clients")
        except Exception as e:
            logger.warning(f"Error closing HTTP clients: {e}")

        # Clean up fsspec/gcsfs caches and sessions
        try:
            logger.info("Clearing fsspec filesystem cache...")
            # Suppress fsspec warnings during shutdown
            import logging

            logging.getLogger("fsspec").setLevel(logging.ERROR)
            logging.getLogger("gcsfs").setLevel(logging.ERROR)
            logging.getLogger("aiohttp").setLevel(logging.ERROR)

            # Clear fsspec registry and cache to prevent session cleanup issues
            fsspec.config.conf.clear()
            if hasattr(fsspec.filesystem, "_cache"):
                fsspec.filesystem._cache.clear()
            logger.info("Cleared fsspec caches")
        except Exception as e:
            logger.warning(f"Error clearing fsspec caches: {e}")

        # Clean up async processing resources (minimal cleanup needed for async)
        try:
            # Async processing uses built-in asyncio/dask which handles cleanup automatically
            logger.info(
                "Async processing cleanup completed (no explicit cleanup needed)"
            )
        except Exception as e:
            logger.warning(f"Error during async processing cleanup: {e}")

        # Force garbage collection to help with cleanup
        try:
            import gc

            collected = gc.collect()
            logger.info(f"Garbage collection freed {collected} objects")
        except Exception as e:
            logger.warning(f"Error during garbage collection: {e}")

        logger.info("Weather task cleanup completed")

    async def start_initial_scoring_workers(self, num_workers=1):
        if self.node_type != "validator":
            return
        logger.info(
            f"[WeatherTask] Attempting to start {num_workers} initial scoring worker(s). Currently {len(self.initial_scoring_workers)} active."
        )
        if (
            self.initial_scoring_worker_running
            and len(self.initial_scoring_workers) >= num_workers
        ):
            logger.info(
                f"[WeatherTask] Initial scoring worker(s) already running ({len(self.initial_scoring_workers)}/{num_workers}). Not starting more."
            )
            return

        self.initial_scoring_worker_running = True
        needed_workers = num_workers - len(self.initial_scoring_workers)
        for i in range(needed_workers):
            worker_task = asyncio.create_task(initial_scoring_worker(self))
            self.initial_scoring_workers.append(worker_task)
            logger.info(
                f"[WeatherTask] Started initial_scoring_worker task {i+1}/{needed_workers} (Total active: {len(self.initial_scoring_workers)}). Task ID: {id(worker_task)}"
            )

    async def stop_initial_scoring_workers(self):
        if self.node_type != "validator":
            return
        logger.info("Stopping initial scoring workers...")
        for worker in self.initial_scoring_workers:
            worker.cancel()

        self.initial_scoring_workers = []
        logger.info("Stopped all initial scoring workers")

    async def start_final_scoring_workers(self, num_workers=1):
        """Start background workers for final ERA5-based scoring."""
        if self.final_scoring_worker_running:
            logger.info("Final scoring workers already running")
            return

        self.final_scoring_worker_running = True
        for _ in range(num_workers):
            worker = asyncio.create_task(finalize_scores_worker(self))
            self.final_scoring_workers.append(worker)
        logger.info(f"Started {num_workers} final scoring workers")

    async def stop_final_scoring_workers(self):
        """Stop all background final scoring workers."""
        if not self.final_scoring_worker_running:
            return

        self.final_scoring_worker_running = False
        logger.info("Stopping final scoring workers...")
        for worker in self.final_scoring_workers:
            worker.cancel()

        self.final_scoring_workers = []
        logger.info("Stopped all final scoring workers")

    async def start_cleanup_workers(self, num_workers=1):
        """Start background workers for cleaning up old data."""
        if self.cleanup_worker_running:
            logger.info("Cleanup workers already running")
            return

        self.cleanup_worker_running = True
        for i in range(num_workers):
            # Track cleanup workers to prevent memory leaks
            if hasattr(self, "validator") and hasattr(
                self.validator, "create_tracked_task"
            ):
                worker = self.validator.create_tracked_task(
                    cleanup_worker(self), f"cleanup_worker_{i}"
                )
            else:
                worker = asyncio.create_task(cleanup_worker(self))
            self.cleanup_workers.append(worker)

        logger.info(f"Started {num_workers} cleanup workers")

    async def stop_cleanup_workers(self):
        """Stop all background cleanup workers."""
        if not self.cleanup_worker_running:
            return

        self.cleanup_worker_running = False
        logger.info("Stopping cleanup workers...")
        for worker in self.cleanup_workers:
            worker.cancel()

        self.cleanup_workers = []
        logger.info("Stopped all cleanup workers")

    async def start_background_workers(
        self,
        num_ensemble_workers=1,
        num_initial_scoring_workers=1,
        num_final_scoring_workers=1,
        num_cleanup_workers=1,
    ):
        if self.node_type == "validator":
            # Always skip legacy batch workers; per-miner pipeline is active by default
            logger.info("[WeatherTask] Per-miner pipeline active; skipping legacy batch workers")
            await self.start_cleanup_workers(num_workers=num_cleanup_workers)
        elif self.node_type == "miner":
            if self.config.get("r2_cleanup_enabled", False):
                await self.start_r2_cleanup_workers(num_workers=1)
            else:
                logger.info("R2 cleanup worker is disabled by configuration.")

            # Start job status logger for miners (enabled by default)
            if self.config.get("job_status_logger_enabled", True):
                await self.start_job_status_logger_workers(num_workers=1)
            else:
                logger.info("Job status logger worker is disabled by configuration.")

    async def stop_background_workers(self):
        if self.node_type == "validator":
            await self.stop_initial_scoring_workers()
            await self.stop_final_scoring_workers()
            await self.stop_cleanup_workers()
        elif self.node_type == "miner":
            if self.r2_cleanup_worker_running:
                await self.stop_r2_cleanup_workers()
            if self.job_status_logger_running:
                await self.stop_job_status_logger_workers()

    async def _stop_worker_list(self, worker_list, worker_name):
        """Helper to properly stop a list of worker tasks."""
        if not worker_list:
            return

        logger.info(f"Stopping {len(worker_list)} {worker_name} worker(s)...")

        # Cancel all tasks
        for worker in worker_list:
            if not worker.done():
                worker.cancel()

        # Wait for cancellation to complete with timeout
        if worker_list:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*worker_list, return_exceptions=True), timeout=5.0
                )
                logger.info(f"Successfully stopped {worker_name} workers")
            except asyncio.TimeoutError:
                logger.warning(
                    f"Timeout stopping {worker_name} workers, forcing cleanup"
                )
            except Exception as e:
                logger.warning(f"Error stopping {worker_name} workers: {e}")

    async def miner_fetch_hash_worker(self):
        """Worker that periodically checks for jobs awaiting input hash verification."""
        CHECK_INTERVAL_SECONDS = 10 if self.test_mode else 60

        while getattr(self, "miner_fetch_hash_worker_running", False):
            try:
                logger.info(
                    "Miner fetch hash worker checking for runs awaiting hash verification..."
                )

                query = """
                SELECT id, gfs_init_time_utc
                FROM weather_forecast_runs 
                WHERE status = 'awaiting_input_hashes'
                ORDER BY run_initiation_time ASC
                LIMIT 5
                """

                runs = await self.db_manager.fetch_all(query)
                if not runs:
                    logger.debug(
                        f"No runs awaiting hash verification. Sleeping for {CHECK_INTERVAL_SECONDS}s..."
                    )
                    await asyncio.sleep(CHECK_INTERVAL_SECONDS)
                    continue

                for run in runs:
                    run_id = run["id"]
                    gfs_init_time = run["gfs_init_time_utc"]
                    current_time = datetime.now(timezone.utc)
                    elapsed_minutes = (
                        current_time - gfs_init_time
                    ).total_seconds() / 60

                    min_wait_minutes = getattr(
                        self.config, "verification_wait_minutes", 30
                    )

                    if elapsed_minutes < min_wait_minutes and not self.test_mode:
                        logger.info(
                            f"[HashWorker] [Run {run_id}] Only {elapsed_minutes:.1f} minutes elapsed, "
                            f"waiting for minimum {min_wait_minutes} minutes"
                        )
                        continue
                    elif self.test_mode and elapsed_minutes < 0.1:
                        logger.info(
                            f"[HashWorker] [Run {run_id}] TEST MODE: Minimal wait of 0.1 minutes"
                        )
                        await asyncio.sleep(6)

                    responses_query = """
                    SELECT id, miner_hotkey, job_id
                    FROM weather_miner_responses
                    WHERE run_id = :run_id
                    AND status = 'fetch_initiated'
                    """
                    miners_to_poll = await self.db_manager.fetch_all(
                        responses_query, {"run_id": run_id}
                    )
                    logger.info(
                        f"[HashWorker] [Run {run_id}] Polling {len(miners_to_poll)} miners for input hash status."
                    )

                    for resp_rec in miners_to_poll:
                        resp_id = resp_rec["id"]
                        miner_hk = resp_rec["miner_hotkey"]
                        miner_job_id = resp_rec["job_id"]
                        logger.debug(
                            f"[HashWorker] [Run {run_id}] Querying miner {miner_hk[:8]} (Job: {miner_job_id}) for input status."
                        )
                        try:
                            status_payload_data = WeatherGetInputStatusData(
                                job_id=miner_job_id
                            )
                            status_payload = {
                                "nonce": str(uuid.uuid4()),
                                "data": status_payload_data.model_dump(),
                            }

                            from gaia.validator.miner.miner_query import get_input_status as _gis
                            status_response = await _gis(self, miner_hk, job_id=miner_job_id)

                            if status_response:
                                parsed_response = status_response
                                if (
                                    isinstance(status_response, dict)
                                    and "text" in status_response
                                ):
                                    try:
                                        parsed_response = loads(status_response["text"])
                                    except (Exception, TypeError) as json_err:
                                        logger.warning(
                                            f"[HashWorker] [Run {run_id}] Failed to parse JSON response from miner {miner_hk[:8]}: {json_err}. Raw response: {status_response.get('text', '')[:200]}"
                                        )
                                        parsed_response = {
                                            "status": "parse_error",
                                            "message": f"JSON parse error from miner {miner_hk[:8]}: {type(json_err).__name__}: {str(json_err)}",
                                        }

                                logger.debug(
                                    f"[Run {run_id}] Received status from {miner_hk[:8]}: {parsed_response}"
                                )
                                return resp_id, parsed_response
                            else:
                                logger.warning(
                                    f"[HashWorker] [Run {run_id}] No response received from miner {miner_hk[:8]} on /weather-get-input-status endpoint."
                                )
                                return resp_id, {
                                    "status": "validator_poll_failed",
                                    "message": f"No response from miner {miner_hk[:8]} on /weather-get-input-status endpoint",
                                }
                        except asyncio.TimeoutError as timeout_err:
                            logger.error(
                                f"[HashWorker] [Run {run_id}] Timeout polling miner {miner_hk[:8]}: {timeout_err}"
                            )
                            return resp_id, {
                                "status": "validator_poll_error",
                                "message": f"Timeout polling miner {miner_hk[:8]}: {str(timeout_err)}",
                            }
                        except ConnectionError as conn_err:
                            logger.error(
                                f"[HashWorker] [Run {run_id}] Connection error polling miner {miner_hk[:8]}: {conn_err}"
                            )
                            return resp_id, {
                                "status": "validator_poll_error",
                                "message": f"Connection error to miner {miner_hk[:8]}: {str(conn_err)}",
                            }
                        except Exception as poll_err:
                            logger.error(
                                f"[HashWorker] [Run {run_id}] Error polling miner {miner_hk[:8]}: {poll_err}",
                                exc_info=True,
                            )
                            return resp_id, {
                                "status": "validator_poll_error",
                                "message": f"Error polling miner {miner_hk[:8]}: {type(poll_err).__name__}: {str(poll_err)}",
                            }

            except Exception as e:
                logger.error(f"Error in miner_fetch_hash_worker: {e}", exc_info=True)
                await asyncio.sleep(60)

    async def stop_r2_cleanup_workers(self):
        await self._stop_worker_list(self.r2_cleanup_workers, "R2 Cleanup")
        self.r2_cleanup_worker_running = False

    async def start_r2_cleanup_workers(self, num_workers=1):
        if self.r2_cleanup_worker_running:
            logger.info("R2 cleanup workers are already running.")
            return
        self.r2_cleanup_worker_running = True
        for i in range(num_workers):
            # Track R2 cleanup workers to prevent memory leaks
            if hasattr(self, "validator") and hasattr(
                self.validator, "create_tracked_task"
            ):
                task = self.validator.create_tracked_task(
                    r2_cleanup_worker(self), f"r2_cleanup_worker_{i}"
                )
            else:
                task = asyncio.create_task(r2_cleanup_worker(self))
            self.r2_cleanup_workers.append(task)
        logger.info(f"Started {num_workers} R2 cleanup workers.")

    async def start_job_status_logger_workers(self, num_workers=1):
        if self.job_status_logger_running:
            logger.info("Job status logger workers are already running.")
            return
        self.job_status_logger_running = True
        for i in range(num_workers):
            # Track job status logger workers to prevent memory leaks
            if hasattr(self, "validator") and hasattr(
                self.validator, "create_tracked_task"
            ):
                task = self.validator.create_tracked_task(
                    weather_job_status_logger(self), f"job_status_logger_{i}"
                )
            else:
                task = asyncio.create_task(weather_job_status_logger(self))
            self.job_status_logger_workers.append(task)
        logger.info(f"Started {num_workers} job status logger workers.")

    async def stop_job_status_logger_workers(self):
        await self._stop_worker_list(
            self.job_status_logger_workers, "Job Status Logger"
        )
        self.job_status_logger_running = False

    async def _load_batch_from_db(self, job_id: str) -> Optional[Batch]:
        """Loads the initial batch object from the pickle file path stored in the database.
        This method should only be called on miner nodes."""
        if self.node_type != "miner":
            logger.error(
                f"[{job_id}] _load_batch_from_db called on {self.node_type} node. This method should only run on miners."
            )
            return None

        logger.info(f"[{job_id}] Loading initial batch from database path.")
        try:
            query = "SELECT input_batch_pickle_path FROM weather_miner_jobs WHERE id = :job_id"
            result = await self.db_manager.fetch_one(query, {"job_id": job_id})

            if not result or not result["input_batch_pickle_path"]:
                logger.error(
                    f"[{job_id}] No input_batch_pickle_path found in DB for this job."
                )
                return None

            pickle_path = Path(result["input_batch_pickle_path"])
            if not pickle_path.exists():
                logger.error(
                    f"[{job_id}] Pickle file not found at path from DB: {pickle_path}"
                )
                return None

            with open(pickle_path, "rb") as f:
                initial_batch = pickle.load(f)

            logger.info(f"[{job_id}] Successfully loaded batch from {pickle_path}.")
            return initial_batch

        except Exception as e:
            logger.error(
                f"[{job_id}] Error loading batch from DB path: {e}", exc_info=True
            )
            return None

    async def recover_incomplete_http_jobs(self):
        """
        Finds jobs that were in-progress on RunPod and restarts the polling worker for them.
        This should be called on miner startup.
        """
        if self.node_type != "miner":
            logger.debug(
                "recover_incomplete_http_jobs: Not a miner node, skipping recovery."
            )
            return

        if self.config.get("weather_inference_type") != "http_service":
            logger.debug(
                "recover_incomplete_http_jobs: Not using HTTP service inference, skipping recovery."
            )
            return

        logger.info("Checking for incomplete HTTP jobs to recover...")
        try:
            query = "SELECT id, runpod_job_id FROM weather_miner_jobs WHERE status = 'in_progress' AND runpod_job_id IS NOT NULL"
            incomplete_jobs = await self.db_manager.fetch_all(query)

            if not incomplete_jobs:
                logger.info("No incomplete HTTP jobs found.")
                return

            logger.info(
                f"Found {len(incomplete_jobs)} incomplete HTTP job(s). Restarting pollers..."
            )
            for job in incomplete_jobs:
                job_id = job["id"]
                runpod_job_id = job["runpod_job_id"]
                logger.info(
                    f"  - Restarting poller for Job ID: {job_id}, RunPod ID: {runpod_job_id}"
                )
                # Track the recovery polling task to prevent memory leaks
                if hasattr(self, "validator") and hasattr(
                    self.validator, "create_tracked_task"
                ):
                    self.validator.create_tracked_task(
                        poll_runpod_job_worker(self, job_id, runpod_job_id),
                        f"recovery_runpod_poll_{job_id}",
                    )
                else:
                    asyncio.create_task(
                        poll_runpod_job_worker(self, job_id, runpod_job_id)
                    )
        except Exception as e:
            logger.error(
                f"Error during incomplete HTTP job recovery: {e}", exc_info=True
            )

    def _validate_and_format_response(
        self, response_dict: Dict[str, Any], expected_fields: List[str]
    ) -> Dict[str, Any]:
        """
        Helper method to validate and format responses consistently.

        Args:
            response_dict: Raw response dictionary from method
            expected_fields: List of required fields for this response type

        Returns:
            Validated response dictionary
        """
        if not isinstance(response_dict, dict):
            logger.error(
                f"Response validation failed: expected dict, got {type(response_dict)}"
            )
            return {
                "status": WeatherTaskStatus.ERROR.value,
                "message": "Invalid response format",
            }

        # Convert any WeatherTaskStatus enum values to strings throughout the entire dictionary
        def convert_enums_to_strings(obj):
            if isinstance(obj, WeatherTaskStatus):
                return obj.value
            elif isinstance(obj, dict):
                return {k: convert_enums_to_strings(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_enums_to_strings(item) for item in obj]
            else:
                return obj

        response_dict = convert_enums_to_strings(response_dict)

        # Check for required fields
        missing_fields = [
            field for field in expected_fields if field not in response_dict
        ]
        if missing_fields:
            logger.warning(f"Response missing required fields: {missing_fields}")
            # Don't fail, just log the warning as some fields might be optional

        # Ensure status is valid if present
        if "status" in response_dict:
            try:
                # Validate that status is a valid enum value (by string)
                WeatherTaskStatus(response_dict["status"])
            except ValueError:
                logger.warning(f"Unknown status value: {response_dict['status']}")

        return response_dict

    async def register_progress_callback(self, callback: callable):
        """Register a callback function to receive progress updates"""
        self.progress_callbacks.append(callback)

    async def _emit_progress(self, update: WeatherProgressUpdate):
        """Emit progress update to all registered callbacks"""
        operation_key = (
            f"{update.operation}_{getattr(self, 'current_job_id', 'system')}"
        )
        self.current_operations[operation_key] = update

        # Log progress
        if update.bytes_downloaded and update.bytes_total:
            percent = (update.bytes_downloaded / update.bytes_total) * 100
            logger.info(
                f"[Progress] {update.operation}: {update.stage} - {percent:.1f}% ({update.bytes_downloaded}/{update.bytes_total} bytes) - {update.message}"
            )
        elif update.files_completed and update.files_total:
            percent = (update.files_completed / update.files_total) * 100
            logger.info(
                f"[Progress] {update.operation}: {update.stage} - {percent:.1f}% ({update.files_completed}/{update.files_total} files) - {update.message}"
            )
        else:
            percent = update.progress * 100
            logger.info(
                f"[Progress] {update.operation}: {update.stage} - {percent:.1f}% - {update.message}"
            )

        # Call registered callbacks
        for callback in self.progress_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(update)
                else:
                    callback(update)
            except Exception as e:
                logger.warning(f"Progress callback error: {e}")

    async def get_file_locations(self) -> List[WeatherFileLocation]:
        """Get information about all weather-related file storage locations"""
        locations = []

        # Cache directories
        cache_dirs = [
            (
                "gfs_cache",
                self.config.get("gfs_analysis_cache_dir", "./gfs_analysis_cache"),
                "GFS analysis data cache",
            ),
            (
                "era5_cache",
                self.config.get("era5_cache_dir", "./era5_cache"),
                "ERA5 reanalysis data cache",
            ),
            ("forecast", str(MINER_FORECAST_DIR_BG), "Miner forecast outputs"),
            ("ensemble", str(VALIDATOR_ENSEMBLE_DIR), "Validator ensemble files"),
        ]

        for file_type, path_str, description in cache_dirs:
            path = Path(path_str)
            if path.exists():
                try:
                    # Calculate directory size
                    total_size = sum(
                        f.stat().st_size for f in path.rglob("*") if f.is_file()
                    )

                    locations.append(
                        WeatherFileLocation(
                            file_type=file_type,
                            local_path=str(path.absolute()),
                            size_bytes=total_size,
                            created_time=datetime.fromtimestamp(
                                path.stat().st_ctime, tz=timezone.utc
                            ),
                            description=description,
                        )
                    )
                except Exception as e:
                    logger.warning(f"Error getting info for {path}: {e}")
                    locations.append(
                        WeatherFileLocation(
                            file_type=file_type,
                            local_path=str(path.absolute()),
                            description=f"{description} (error reading size)",
                        )
                    )
            else:
                locations.append(
                    WeatherFileLocation(
                        file_type=file_type,
                        local_path=str(path.absolute()),
                        size_bytes=0,
                        description=f"{description} (directory not yet created)",
                    )
                )

        return locations

    async def get_storage_summary(self) -> Dict[str, Any]:
        """Get a summary of storage usage for weather task files"""
        locations = await self.get_file_locations()
        summary = {
            "total_size_bytes": 0,
            "total_size_mb": 0,
            "locations": {},
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

        for loc in locations:
            if loc.size_bytes:
                summary["total_size_bytes"] += loc.size_bytes
                summary["locations"][loc.file_type] = {
                    "path": loc.local_path,
                    "size_bytes": loc.size_bytes,
                    "size_mb": round(loc.size_bytes / (1024 * 1024), 2),
                    "description": loc.description,
                    "created": (
                        loc.created_time.isoformat() if loc.created_time else None
                    ),
                }

        summary["total_size_mb"] = round(summary["total_size_bytes"] / (1024 * 1024), 2)
        return summary

    def _format_bytes(self, bytes_count: int) -> str:
        """Format bytes count into human readable string"""
        if bytes_count < 1024:
            return f"{bytes_count} B"
        elif bytes_count < 1024 * 1024:
            return f"{bytes_count / 1024:.1f} KB"
        elif bytes_count < 1024 * 1024 * 1024:
            return f"{bytes_count / (1024 * 1024):.1f} MB"
        else:
            return f"{bytes_count / (1024 * 1024 * 1024):.1f} GB"

    def _format_time_remaining(self, seconds: Optional[int]) -> str:
        """Format time remaining into human readable string"""
        if seconds is None:
            return "unknown"

        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            return f"{seconds // 60}m {seconds % 60}s"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}h {minutes}m"

    async def get_progress_status(self, job_id: Optional[str] = None) -> Dict[str, Any]:
        """Get current progress status for all operations or a specific job"""
        if job_id:
            # Filter for specific job
            job_operations = {
                k: v for k, v in self.current_operations.items() if job_id in k
            }
            return {
                "job_id": job_id,
                "operations": {k: v.dict() for k, v in job_operations.items()},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        else:
            # Return all current operations
            return {
                "all_operations": {
                    k: v.dict() for k, v in self.current_operations.items()
                },
                "active_operations_count": len(self.current_operations),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

    async def get_detailed_storage_info(self) -> Dict[str, Any]:
        """Get detailed information about storage locations and their contents"""
        storage_info = await self.get_storage_summary()

        # Add configuration information
        storage_info.update(
            {
                "configuration": {
                    "gfs_cache_dir": self.config.get(
                        "gfs_analysis_cache_dir", "./gfs_analysis_cache"
                    ),
                    "era5_cache_dir": self.config.get("era5_cache_dir", "./era5_cache"),
                    "miner_forecast_dir": str(MINER_FORECAST_DIR_BG),
                    "validator_ensemble_dir": str(VALIDATOR_ENSEMBLE_DIR),
                    "gfs_retention_days": self.config.get(
                        "gfs_cache_retention_days", 7
                    ),
                    "era5_retention_days": self.config.get(
                        "era5_cache_retention_days", 30
                    ),
                    "ensemble_retention_days": self.config.get(
                        "ensemble_retention_days", 14
                    ),
                },
                "environment_variables": {
                    "WEATHER_GFS_CACHE_DIR": os.getenv(
                        "WEATHER_GFS_CACHE_DIR", "not set"
                    ),
                    "WEATHER_ERA5_CACHE_DIR": os.getenv(
                        "WEATHER_ERA5_CACHE_DIR", "not set"
                    ),
                    "MINER_FORECAST_DIR": os.getenv("MINER_FORECAST_DIR", "not set"),
                },
            }
        )

        # Add file counts and recent files for each location
        for loc_type, loc_info in storage_info["locations"].items():
            path = Path(loc_info["path"])
            if path.exists():
                try:
                    files = list(path.rglob("*"))
                    files = [f for f in files if f.is_file()]

                    loc_info.update({"file_count": len(files), "recent_files": []})

                    # Get 5 most recently modified files
                    if files:
                        recent_files = sorted(
                            files, key=lambda x: x.stat().st_mtime, reverse=True
                        )[:5]
                        for f in recent_files:
                            stat = f.stat()
                            loc_info["recent_files"].append(
                                {
                                    "name": f.name,
                                    "relative_path": str(f.relative_to(path)),
                                    "size_bytes": stat.st_size,
                                    "size_formatted": self._format_bytes(stat.st_size),
                                    "modified": datetime.fromtimestamp(
                                        stat.st_mtime, tz=timezone.utc
                                    ).isoformat(),
                                }
                            )
                except Exception as e:
                    loc_info["error"] = f"Unable to analyze directory: {e}"

        return storage_info

    async def _is_run_stale(
        self, run_id: int, status: str, run_age_hours: float
    ) -> Tuple[bool, str]:
        """
        Intelligent stale detection that considers run status, test mode, and ERA5 backoff states.
        Returns (is_stale, reason_string)
        """
        now_utc = datetime.now(timezone.utc)

        # Different timeout thresholds based on status and mode
        if self.test_mode:
            # Much more generous in test mode
            general_timeout_hours = 240  # 10 days
            era5_timeout_hours = 480  # 20 days
        else:
            # Production timeouts
            general_timeout_hours = 48  # 2 days
            era5_timeout_hours = (
                168  # 7 days for ERA5 states (data availability can take time)
            )

        # ERA5 final scoring states need special handling
        if status in ["era5_final_scoring_started", "era5_final_scoring_partial"]:
            # Check if this run might be in ERA5 backoff
            if hasattr(self, "era5_failed_attempts") and self.era5_failed_attempts:
                # Get the GFS init time to check relevant dates
                gfs_query = "SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :run_id"
                gfs_result = await self.db_manager.fetch_one(
                    gfs_query, {"run_id": run_id}
                )

                if gfs_result:
                    gfs_init_time = gfs_result["gfs_init_time_utc"]
                    sparse_lead_hours = self.config.get(
                        "final_scoring_lead_hours",
                        [24, 48, 72, 96, 120, 144, 168, 192, 216, 240],
                    )

                    # Check if any dates for this run are in backoff
                    dates_in_backoff = []
                    latest_backoff_expiry = None

                    for lead_hour in sparse_lead_hours:
                        forecast_target_time = gfs_init_time + timedelta(
                            hours=lead_hour
                        )
                        date_str = forecast_target_time.strftime("%Y-%m-%d")

                        if date_str in self.era5_failed_attempts:
                            retry_after = self.era5_failed_attempts[date_str].get(
                                "retry_after"
                            )
                            if retry_after and now_utc < retry_after:
                                dates_in_backoff.append(date_str)
                                if (
                                    latest_backoff_expiry is None
                                    or retry_after > latest_backoff_expiry
                                ):
                                    latest_backoff_expiry = retry_after

                    # If dates are still in backoff, don't mark as stale
                    if dates_in_backoff:
                        time_until_backoff_expires = (
                            latest_backoff_expiry - now_utc
                        ).total_seconds() / 3600
                        logger.debug(
                            f"Run {run_id}: {len(dates_in_backoff)} dates in ERA5 backoff, expires in {time_until_backoff_expires:.1f}h"
                        )
                        return False, f"waiting for ERA5 backoff to expire"

                    # If backoff expired recently, give extra time before marking stale
                    if (
                        latest_backoff_expiry
                        and (now_utc - latest_backoff_expiry).total_seconds()
                        < 12 * 3600
                    ):  # 12 hours grace period
                        return False, f"ERA5 backoff recently expired, allowing retry"

            # Use ERA5-specific timeout
            if run_age_hours > era5_timeout_hours:
                logger.debug(
                    f"Run {run_id}: marking as stale - ERA5 age {run_age_hours:.1f}h > {era5_timeout_hours}h threshold"
                )
                return (
                    True,
                    f"age {run_age_hours:.1f}h > {era5_timeout_hours}h threshold for ERA5 scoring",
                )
            else:
                logger.debug(
                    f"Run {run_id}: not stale - ERA5 age {run_age_hours:.1f}h within {era5_timeout_hours}h threshold"
                )
                return (
                    False,
                    f"within ERA5 timeout ({run_age_hours:.1f}h < {era5_timeout_hours}h)",
                )

        # General timeout for other statuses
        if run_age_hours > general_timeout_hours:
            logger.debug(
                f"Run {run_id}: marking as stale - age {run_age_hours:.1f}h > {general_timeout_hours}h threshold"
            )
            return (
                True,
                f"age {run_age_hours:.1f}h > {general_timeout_hours}h threshold",
            )
        else:
            logger.debug(
                f"Run {run_id}: not stale - age {run_age_hours:.1f}h within {general_timeout_hours}h threshold"
            )
            return (
                False,
                f"within timeout ({run_age_hours:.1f}h < {general_timeout_hours}h)",
            )

    async def _check_and_recover_incomplete_runs_sequential(
        self, processed_runs_this_session=None, max_attempts_per_run=3
    ):
        """
        Sequential version of run recovery that processes one run at a time to avoid system overload.
        Designed for periodic execution without blocking normal operations.
        """
        logger.debug("_check_and_recover_incomplete_runs_sequential disabled in new pipeline")
        return "disabled"

        if processed_runs_this_session is None:
            processed_runs_this_session = {}

        # Look for one incomplete run at a time
        recoverable_states = [
            "sending_fetch_requests",
            "no_miners_to_poll",  # Added to allow recovery of runs with no miners
            "awaiting_input_hashes",
            "verifying_input_hashes",
            "triggering_inference",
            "awaiting_inference_results",
            "verifying_miner_forecasts",
            # INCLUDE SCORING STATES - can get stuck due to restarts during scoring
            "day1_scoring_started",
            "era5_final_scoring_started",
            "era5_final_scoring_partial",
            # AGGRESSIVELY RETRY FAILED DAY1 SCORING
            "day1_scoring_failed",
            "initial_scoring_queued",
            "initial_scoring_failed",
        ]

        # Also include failure states that might be recoverable with full workflow
        potentially_recoverable_failure_states = [
            "stalled_no_valid_forecasts",
            "all_forecasts_failed_verification",
            "no_matching_hashes",
            "inference_trigger_failed",
            "hash_verification_failed",
        ]

        # Also check for runs that might be ready for scoring
        cutoff_time = datetime.now(timezone.utc) - timedelta(
            minutes=self.config.get("verification_wait_minutes", 30)
        )

        # Get up to 20 runs to check, prioritizing runs that haven't completed day1 scoring
        # Include both normal recoverable states and failure states that might be recoverable
        all_recoverable_states = (
            recoverable_states + potentially_recoverable_failure_states
        )

        # First, prioritize runs that haven't completed day1 scoring
        day1_priority_query = """
        SELECT id, gfs_init_time_utc, status, run_initiation_time, 
               CASE WHEN status IN ('verifying_miner_forecasts', 'day1_scoring_started', 'initial_scoring_failed') THEN 1 ELSE 2 END as priority
        FROM weather_forecast_runs 
        WHERE status IN ({}) OR (status = 'awaiting_inference_results' AND run_initiation_time < :cutoff_time)
        ORDER BY priority ASC, run_initiation_time DESC
        LIMIT 20
        """.format(
            ",".join(f"'{state}'" for state in all_recoverable_states)
        )

        recovery_query = day1_priority_query

        incomplete_runs = await self.db_manager.fetch_all(
            recovery_query, {"cutoff_time": cutoff_time}
        )

        # Debug: Log what we found with priority information
        logger.debug(
            f"Recovery query found {len(incomplete_runs)} incomplete runs (prioritized by day1 scoring)"
        )
        for run in incomplete_runs:
            priority_str = "HIGH" if run.get("priority", 2) == 1 else "NORMAL"
            logger.debug(
                f"  - Run {run['id']}: status='{run['status']}', priority={priority_str}, age={(datetime.now(timezone.utc) - run['run_initiation_time']).total_seconds() / 3600:.1f}h"
            )

        if not incomplete_runs:
            # Check if there are any runs in unexpected states
            debug_query = """
            SELECT id, status, run_initiation_time, gfs_init_time_utc
            FROM weather_forecast_runs 
            WHERE run_initiation_time > (CURRENT_TIMESTAMP - INTERVAL '72 hours')
            ORDER BY run_initiation_time DESC
            LIMIT 5
            """
            all_recent_runs = await self.db_manager.fetch_all(debug_query)
            logger.debug(f"Recent runs for debugging ({len(all_recent_runs)} found):")
            for run in all_recent_runs:
                run_age = (
                    datetime.now(timezone.utc) - run["run_initiation_time"]
                ).total_seconds() / 3600
                logger.debug(
                    f"  - Run {run['id']}: status='{run['status']}', age={run_age:.1f}h"
                )

            return "no_more_runs"

        # Find the first run we can process (not exceeded max attempts and not in cooldown)
        run_cooldown_minutes = 15  # Don't retry failed runs for at least 15 minutes
        run_cooldown_time = datetime.now(timezone.utc) - timedelta(
            minutes=run_cooldown_minutes
        )

        run_to_process = None
        for run in incomplete_runs:
            run_id = run["id"]
            status = run["status"]
            attempt_count = processed_runs_this_session.get(run_id, 0)

            # Skip if exceeded max attempts
            if attempt_count >= max_attempts_per_run:
                continue

            # Smart stale detection - different logic for different statuses
            run_age_hours = (
                datetime.now(timezone.utc) - run["run_initiation_time"]
            ).total_seconds() / 3600
            is_stale, stale_reason = await self._is_run_stale(
                run_id, status, run_age_hours
            )

            if is_stale:
                logger.warning(
                    f"Run {run_id} is stale ({stale_reason}), marking as abandoned"
                )
                await _update_run_status(self, run_id, "stale_abandoned")
                logger.info(
                    f"Stale run {run_id} marked as abandoned - validator will immediately proceed to check for new run creation"
                )
                return "stale_processed"  # Special return value for stale runs

            # Check cooldown for failure states
            if status in potentially_recoverable_failure_states:
                # Use run_initiation_time as a proxy for when the run entered failure state
                # This is less precise but better than crashing on missing columns
                last_status_time = run["run_initiation_time"]

                if last_status_time and last_status_time > run_cooldown_time:
                    time_since_failure = (
                        datetime.now(timezone.utc) - last_status_time
                    ).total_seconds() / 60
                    logger.debug(
                        f"Run {run_id} in failure state '{status}' is still in cooldown period ({time_since_failure:.1f}min < {run_cooldown_minutes}min). Skipping to check next run."
                    )
                    continue  # Skip this run and check the next one

            # This run is eligible for processing
            run_to_process = run
            break

        if not run_to_process:
            logger.debug(
                f"All {len(incomplete_runs)} incomplete runs are either at max attempts ({max_attempts_per_run}) or in cooldown periods"
            )
            return "no_more_runs"

        # Process the selected run
        run = run_to_process
        run_id = run["id"]
        status = run["status"]
        gfs_time = run["gfs_init_time_utc"]
        run_age_hours = (
            datetime.now(timezone.utc) - run["run_initiation_time"]
        ).total_seconds() / 3600

        # Track this attempt
        attempt_count = processed_runs_this_session.get(run_id, 0) + 1
        processed_runs_this_session[run_id] = attempt_count

        logger.info(
            f"Sequential recovery processing: Run {run_id} (status='{status}', age={run_age_hours:.1f}h, attempt={attempt_count}/{max_attempts_per_run})"
        )

        # Handle failure states that might be recoverable (we already passed cooldown check above)
        if status in potentially_recoverable_failure_states:
            logger.info(
                f"Recovering run {run_id}: resetting failure state '{status}' and re-running full hash verification workflow (passed {run_cooldown_minutes}min cooldown)"
            )

            # Reset run status
            await _update_run_status(self, run_id, "verifying_input_hashes")

            # Also reset miner responses back to fresh state for full retry
            reset_miner_responses_query = """
            UPDATE weather_miner_responses 
            SET status = 'fetch_initiated', 
                input_hash_miner = NULL,
                input_hash_validator = NULL, 
                input_hash_match = NULL,
                error_message = '',
                last_polled_time = NULL
            WHERE run_id = :run_id 
            AND status IN ('input_hash_mismatch', 'input_fetch_error', 'input_hash_timeout', 
                          'input_poll_error', 'input_validation_complete', 'inference_triggered')
            """
            reset_result = await self.db_manager.execute(
                reset_miner_responses_query, {"run_id": run_id}
            )
            logger.info(
                f"[Run {run_id}] Reset miner responses to fresh state for recovery retry"
            )

            await self._execute_hash_verification_workflow(run_id)
            return "run_processed"

        # Recover based on current status - process synchronously
        try:
            if status == "sending_fetch_requests" or status == "no_miners_to_poll":
                # Check if we have any miner responses for this run
                existing_responses = await self.db_manager.fetch_one(
                    "SELECT COUNT(*) as count FROM weather_miner_responses WHERE run_id = :run_id",
                    {"run_id": run_id},
                )
                response_count = (
                    existing_responses["count"] if existing_responses else 0
                )

                if response_count == 0 or status == "no_miners_to_poll":
                    # No responses recorded - need to actually send fetch requests
                    logger.info(
                        f"Recovering run {run_id}: Actually re-sending fetch requests to miners (found {response_count} existing responses)"
                    )

                    # Get the GFS times from the run
                    run_data = await self.db_manager.fetch_one(
                        "SELECT gfs_init_time_utc FROM weather_forecast_runs WHERE id = :run_id",
                        {"run_id": run_id},
                    )

                    if not run_data:
                        logger.error(f"Run {run_id} not found in database")
                        return False

                    gfs_t0_run_time = run_data["gfs_init_time_utc"]
                    gfs_t_minus_6_run_time = gfs_t0_run_time - timedelta(hours=6)

                    # Prepare the payload
                    payload_data = WeatherInitiateFetchData(
                        forecast_start_time=gfs_t0_run_time,
                        previous_step_time=gfs_t_minus_6_run_time,
                        validator_hotkey=(
                            self.validator.keypair.ss58_address
                            if self.validator.keypair
                            else None
                        ),
                    )
                    payload_dict = payload_data.model_dump(mode="json")

                    payload = {"nonce": str(uuid.uuid4()), "data": payload_dict}

                    # Actually send the requests to miners
                    logger.info(
                        f"[Run {run_id}] Sending weather initiate fetch requests to miners..."
                    )
                    responses = await self.validator.query_miners(
                        payload=payload, endpoint="/weather-initiate-fetch"
                    )
                    logger.info(
                        f"[Run {run_id}] Received {len(responses)} responses from miners."
                    )

                    # Process responses and record them
                    accepted_count = 0
                    for miner_hotkey, response_data in responses.items():
                        try:
                            miner_response = response_data
                            if (
                                isinstance(response_data, dict)
                                and "text" in response_data
                            ):
                                try:
                                    miner_response = loads(response_data["text"])
                                except (Exception, TypeError):
                                    miner_response = {"status": "parse_error"}

                            if (
                                isinstance(miner_response, dict)
                                and miner_response.get("status")
                                == WeatherTaskStatus.FETCH_ACCEPTED
                                and miner_response.get("job_id")
                            ):

                                miner_uid_result = await self.db_manager.fetch_one(
                                    "SELECT uid FROM node_table WHERE hotkey = :hk",
                                    {"hk": miner_hotkey},
                                )
                                miner_uid = (
                                    miner_uid_result["uid"] if miner_uid_result else -1
                                )

                                if miner_uid != -1:
                                    miner_job_id = miner_response.get("job_id")

                                    insert_resp_query = """
                                        INSERT INTO weather_miner_responses
                                        (run_id, miner_uid, miner_hotkey, response_time, status, job_id)
                                        VALUES (:run_id, :uid, :hk, :resp_time, :status, :job_id)
                                        ON CONFLICT (run_id, miner_uid) DO UPDATE SET
                                        response_time = EXCLUDED.response_time,
                                        status = EXCLUDED.status,
                                        job_id = EXCLUDED.job_id
                                    """
                                    await self.db_manager.execute(
                                        insert_resp_query,
                                        {
                                            "run_id": run_id,
                                            "uid": miner_uid,
                                            "hk": miner_hotkey,
                                            "resp_time": datetime.now(timezone.utc),
                                            "status": "fetch_initiated",
                                            "job_id": miner_job_id,
                                        },
                                    )
                                    accepted_count += 1
                                    logger.debug(
                                        f"[Run {run_id}] Recorded acceptance from Miner UID {miner_uid}"
                                    )
                        except Exception as e:
                            logger.error(
                                f"[Run {run_id}] Error processing response from {miner_hotkey}: {e}"
                            )

                    logger.info(
                        f"[Run {run_id}] Recovery: {accepted_count} miners accepted fetch requests"
                    )

                    if accepted_count > 0:
                        await _update_run_status(self, run_id, "awaiting_input_hashes")
                        # Continue with hash verification after waiting
                        await self._continue_run_workflow(run_id)
                    else:
                        logger.warning(
                            f"[Run {run_id}] No miners accepted fetch requests during recovery"
                        )
                        await _update_run_status(self, run_id, "no_miners_to_poll")
                else:
                    # We have existing responses, just continue with hash verification
                    logger.info(
                        f"Recovering run {run_id}: Found {response_count} existing responses, continuing with hash verification"
                    )
                    await _update_run_status(self, run_id, "awaiting_input_hashes")
                    await self._continue_run_workflow(run_id)

            elif status == "awaiting_input_hashes":
                logger.info(f"Recovering run {run_id}: continuing hash verification")
                await _update_run_status(self, run_id, "verifying_input_hashes")
                await self._continue_run_workflow(run_id)

            elif status == "verifying_input_hashes":
                logger.info(f"Recovering run {run_id}: re-running hash verification")
                await self._continue_run_workflow(run_id)

            elif status == "triggering_inference":
                logger.info(
                    f"Recovering run {run_id}: checking inference trigger status"
                )
                await _update_run_status(self, run_id, "awaiting_inference_results")
                await self._continue_run_workflow(run_id)

            elif status in ["awaiting_inference_results", "verifying_miner_forecasts"]:
                logger.info(f"Recovering run {run_id}: processing ready for scoring")
                await self._continue_run_workflow(run_id)

            elif status == "day1_scoring_started":
                # Check how long the run has been in this status before considering it stuck
                # Scoring can take several minutes, especially with many miners
                scoring_job_check = await self.db_manager.fetch_one(
                    """SELECT started_at, status FROM weather_scoring_jobs 
                       WHERE run_id = :run_id AND score_type = 'day1_qc'""",
                    {"run_id": run_id},
                )

                should_recover = True
                if scoring_job_check and scoring_job_check["started_at"]:
                    time_in_scoring = (
                        datetime.now(timezone.utc) - scoring_job_check["started_at"]
                    ).total_seconds()
                    # Allow at least 10 minutes for scoring to complete
                    if time_in_scoring < 600:  # 10 minutes
                        logger.debug(
                            f"Run {run_id}: Still in active Day-1 scoring (elapsed: {time_in_scoring:.0f}s), skipping recovery"
                        )
                        should_recover = False
                    else:
                        logger.warning(
                            f"Run {run_id}: Day-1 scoring stuck for {time_in_scoring:.0f}s, triggering recovery"
                        )

                if should_recover:
                    logger.info(
                        f"Recovering run {run_id}: resetting stuck Day-1 scoring state '{status}' after timeout"
                    )

                    # Reset run status to allow re-processing
                    await _update_run_status(self, run_id, "verifying_miner_forecasts")

                    # Reset Day-1 scoring job to queued status for retry with smart partial logic
                    reset_job_query = """
                    UPDATE weather_scoring_jobs 
                    SET status = 'queued', started_at = NULL, error_message = 'Reset due to scoring timeout'
                    WHERE run_id = :run_id AND score_type = 'day1_qc' AND status IN ('in_progress', 'queued')
                    """
                    await self.db_manager.execute(
                        reset_job_query, {"run_id": run_id, "score_type": "day1_qc"}
                    )

                    logger.info(
                        f"Run {run_id}: Reset from '{status}' to 'verifying_miner_forecasts' with 'day1_qc' job queued for smart partial retry"
                    )

                    # Trigger immediate scoring check
                    await self._continue_run_workflow(run_id)

            elif status in [
                "day1_scoring_failed",
                "initial_scoring_failed",
                "initial_scoring_queued",
            ]:
                logger.info(
                    f"Recovering run {run_id}: Retrying failed/stuck Day-1 scoring (status: '{status}')"
                )

                # Check if we have any verified responses to score
                verified_check = await self.db_manager.fetch_one(
                    """SELECT COUNT(*) as count FROM weather_miner_responses 
                       WHERE run_id = :run_id AND verification_passed = TRUE""",
                    {"run_id": run_id},
                )
                verified_count = verified_check["count"] if verified_check else 0

                if verified_count > 0:
                    # Check if we already have some day1 scores (partial completion)
                    existing_scores = await self.db_manager.fetch_one(
                        "SELECT COUNT(*) as count FROM weather_miner_scores WHERE run_id = :run_id AND score_type = 'day1_qc_score'",
                        {"run_id": run_id},
                    )
                    existing_count = existing_scores["count"] if existing_scores else 0

                    logger.info(
                        f"Run {run_id}: Found {verified_count} verified responses, {existing_count} already scored"
                    )

                    # Reset to trigger re-scoring
                    await _update_run_status(self, run_id, "verifying_miner_forecasts")

                    # Ensure scoring job exists and is queued
                    await self.db_manager.execute(
                        """INSERT INTO weather_scoring_jobs (run_id, score_type, status, created_at)
                           VALUES (:run_id, 'day1_qc', 'queued', NOW())
                           ON CONFLICT (run_id, score_type) 
                           DO UPDATE SET status = 'queued', started_at = NULL, error_message = 'Retry after failure'""",
                        {"run_id": run_id},
                    )

                    # Trigger scoring workflow
                    await self._continue_run_workflow(run_id)

                    # Don't call update_combined_weather_scores here - scoring hasn't completed yet
                    # The scoring workers will call it when they complete

                else:
                    logger.warning(
                        f"Run {run_id}: No verified responses available, cannot retry scoring"
                    )
                    await _update_run_status(
                        self, run_id, "no_verified_responses_for_scoring"
                    )

            elif status == "era5_final_scoring_started":
                logger.info(
                    f"Recovering run {run_id}: resetting stuck ERA5 scoring state '{status}' - likely interrupted by restart"
                )

                # Check progress of both Day-1 and ERA5 scoring for smart recovery
                day1_complete_check = await self.db_manager.fetch_one(
                    "SELECT COUNT(*) as count FROM weather_miner_scores WHERE run_id = :run_id AND score_type = 'day1_qc_score'",
                    {"run_id": run_id},
                )

                era5_partial_check = await self.db_manager.fetch_one(
                    "SELECT COUNT(*) as era5_count FROM weather_miner_scores WHERE run_id = :run_id AND score_type = 'era5_final_composite_score'",
                    {"run_id": run_id},
                )

                day1_count = day1_complete_check["count"] if day1_complete_check else 0
                era5_count = (
                    era5_partial_check["era5_count"] if era5_partial_check else 0
                )

                # Log partial recovery information
                if era5_count > 0:
                    logger.info(
                        f"Run {run_id}: ERA5 partial progress detected - {era5_count} miners already have ERA5 scores"
                    )

                if day1_count > 0:
                    # Day-1 scoring is complete, reset to day1_scoring_complete to trigger ERA5 scoring
                    await _update_run_status(self, run_id, "day1_scoring_complete")
                    logger.info(
                        f"Run {run_id}: Reset from '{status}' to 'day1_scoring_complete' (Day-1 complete: {day1_count} miners) for ERA5 partial recovery"
                    )
                else:
                    # Day-1 scoring not complete, reset to earlier state
                    await _update_run_status(self, run_id, "verifying_miner_forecasts")
                    logger.info(
                        f"Run {run_id}: Reset from '{status}' to 'verifying_miner_forecasts' (Day-1 not complete)"
                    )

                # Reset ERA5 scoring job to queued status for smart partial recovery
                reset_job_query = """
                UPDATE weather_scoring_jobs 
                SET status = 'queued', started_at = NULL, error_message = 'Reset for partial recovery after validator restart'
                WHERE run_id = :run_id AND score_type = 'era5_final' AND status IN ('in_progress', 'queued')
                """
                await self.db_manager.execute(
                    reset_job_query, {"run_id": run_id, "score_type": "era5_final"}
                )

                if era5_count > 0:
                    logger.info(
                        f"Run {run_id}: Reset ERA5 scoring job for smart partial recovery (will resume from {era5_count} completed miners)"
                    )
                else:
                    logger.info(
                        f"Run {run_id}: Reset ERA5 scoring job for full scoring"
                    )

                # Trigger immediate scoring check
                await self._continue_run_workflow(run_id)

        except Exception as recovery_error:
            logger.error(
                f"Error recovering run {run_id}: {recovery_error}", exc_info=True
            )
            await _update_run_status(
                self, run_id, "recovery_failed", error_message=str(recovery_error)
            )

        return "run_processed"  # Normal recovery, not stale

    async def _check_and_recover_incomplete_runs(self):
        """
        Check for incomplete runs from previous validator sessions and attempt to recover them.
        Returns True if any runs were recovered and should be processed.
        """
        logger.info("Checking for incomplete runs from previous validator sessions...")

        # Look for runs that are in intermediate states and could be continued
        recoverable_states = [
            "sending_fetch_requests",
            "awaiting_input_hashes",
            "verifying_input_hashes",
            "triggering_inference",
            "awaiting_inference_results",
            "verifying_miner_forecasts",
            # INCLUDE SCORING STATES - can get stuck due to restarts during scoring
            "day1_scoring_started",
            "era5_final_scoring_started",
            "era5_final_scoring_partial",
        ]

        # Also check for runs that might be ready for scoring
        cutoff_time = datetime.now(timezone.utc) - timedelta(
            minutes=self.config.get("verification_wait_minutes", 30)
        )

        recovery_query = """
        SELECT id, gfs_init_time_utc, status, run_initiation_time
        FROM weather_forecast_runs 
        WHERE status IN ({}) OR (status = 'awaiting_inference_results' AND run_initiation_time < :cutoff_time)
        ORDER BY run_initiation_time DESC
        LIMIT 5
        """.format(
            ",".join(f"'{state}'" for state in recoverable_states)
        )

        incomplete_runs = await self.db_manager.fetch_all(
            recovery_query, {"cutoff_time": cutoff_time}
        )

        if not incomplete_runs:
            logger.info("No incomplete runs found to recover.")
            return False

        logger.info(f"Found {len(incomplete_runs)} incomplete run(s) to recover:")
        for run in incomplete_runs:
            run_id = run["id"]
            status = run["status"]
            gfs_time = run["gfs_init_time_utc"]
            run_age_hours = (
                datetime.now(timezone.utc) - run["run_initiation_time"]
            ).total_seconds() / 3600

            logger.info(
                f"  - Run {run_id}: status='{status}', GFS time={gfs_time}, age={run_age_hours:.1f}h"
            )

            # Skip runs that are too old (older than 48 hours)
            if run_age_hours > 48:
                logger.warning(
                    f"  - Run {run_id} is too old ({run_age_hours:.1f}h), marking as stale"
                )
                await _update_run_status(self, run_id, "stale_abandoned")
                continue

            # Recover based on current status
            try:
                if status == "sending_fetch_requests":
                    logger.info(
                        f"  - Recovering run {run_id}: re-triggering fetch requests"
                    )
                    await _update_run_status(self, run_id, "awaiting_input_hashes")
                    # Continue with hash verification immediately
                    asyncio.create_task(self._continue_run_workflow(run_id))

                elif status == "awaiting_input_hashes":
                    logger.info(
                        f"  - Recovering run {run_id}: continuing hash verification"
                    )
                    await _update_run_status(self, run_id, "verifying_input_hashes")
                    asyncio.create_task(self._continue_run_workflow(run_id))

                elif status == "verifying_input_hashes":
                    logger.info(
                        f"  - Recovering run {run_id}: re-running hash verification"
                    )
                    asyncio.create_task(self._continue_run_workflow(run_id))

                elif status == "triggering_inference":
                    logger.info(
                        f"  - Recovering run {run_id}: checking inference trigger status"
                    )
                    await _update_run_status(self, run_id, "awaiting_inference_results")
                    asyncio.create_task(self._continue_run_workflow(run_id))

                elif status in [
                    "awaiting_inference_results",
                    "verifying_miner_forecasts",
                ]:
                    logger.info(
                        f"  - Recovering run {run_id}: processing ready for scoring"
                    )
                    asyncio.create_task(self._continue_run_workflow(run_id))

                elif status in [
                    "day1_scoring_started",
                    "era5_final_scoring_started",
                    "era5_final_scoring_partial",
                ]:
                    logger.info(
                        f"  - Recovering run {run_id}: resetting stuck scoring state '{status}' - likely interrupted by restart"
                    )

                    # For partial scoring, don't reset status - let it continue from where it left off
                    if status == "era5_final_scoring_partial":
                        logger.info(
                            f"  - Run {run_id}: Partial scoring state - will continue progressive scoring from existing progress"
                        )
                        # Don't reset status for partial scoring - it should continue as-is
                        # Just trigger scoring check to resume where it left off
                        asyncio.create_task(self._continue_run_workflow(run_id))
                    else:
                        # For started states, reset to allow re-processing
                        await _update_run_status(
                            self, run_id, "verifying_miner_forecasts"
                        )

                        # Reset scoring jobs to queued status for retry with smart partial logic
                        scoring_type = (
                            "day1_qc"
                            if status == "day1_scoring_started"
                            else "era5_final"
                        )
                        reset_job_query = """
                        UPDATE weather_scoring_jobs 
                        SET status = 'queued', started_at = NULL, error_message = 'Reset due to validator restart during scoring'
                        WHERE run_id = :run_id AND score_type = :score_type AND status IN ('in_progress', 'queued')
                        """
                        await self.db_manager.execute(
                            reset_job_query,
                            {"run_id": run_id, "score_type": scoring_type},
                        )

                        logger.info(
                            f"  - Run {run_id}: Reset from '{status}' to 'verifying_miner_forecasts' with '{scoring_type}' job queued for smart partial retry"
                        )

                        # Trigger immediate scoring check
                        asyncio.create_task(self._continue_run_workflow(run_id))

            except Exception as recovery_error:
                logger.error(
                    f"  - Error recovering run {run_id}: {recovery_error}",
                    exc_info=True,
                )
                await _update_run_status(
                    self, run_id, "recovery_failed", error_message=str(recovery_error)
                )

        return len(incomplete_runs) > 0

    async def _continue_run_workflow(self, run_id: int):
        """Continue the workflow for a recovered run."""
        try:
            logger.info(f"[Run {run_id}] Continuing workflow from recovery...")

            # Get current run status
            run_query = "SELECT status, gfs_init_time_utc FROM weather_forecast_runs WHERE id = :run_id"
            run_info = await self.db_manager.fetch_one(run_query, {"run_id": run_id})

            if not run_info:
                logger.error(
                    f"[Run {run_id}] Run not found during workflow continuation"
                )
                return

            status = run_info["status"]

            if status in ["verifying_input_hashes", "awaiting_input_hashes"]:
                # Actually do the hash verification work that was interrupted
                logger.info(
                    f"[Run {run_id}] Re-running input hash verification by calling main validator execute logic..."
                )
                await self._execute_hash_verification_workflow(run_id)

            elif status in ["awaiting_inference_results", "verifying_miner_forecasts"]:
                # Check if it's ready for scoring
                logger.info(f"[Run {run_id}] Checking if ready for scoring...")
                await self.validator_score()

        except Exception as e:
            logger.error(
                f"[Run {run_id}] Error continuing workflow: {e}", exc_info=True
            )
            await _update_run_status(
                self, run_id, "workflow_continuation_failed", error_message=str(e)
            )

    async def _execute_hash_verification_workflow(self, run_id: int):
        """
        Execute the complete hash verification workflow for a run.
        This is extracted from the main validator_execute flow and reusable for recovery.
        """
        try:
            logger.info(
                f"[Run {run_id}] Starting complete hash verification workflow..."
            )

            # Get run details
            run_query = "SELECT gfs_init_time_utc, status FROM weather_forecast_runs WHERE id = :run_id"
            run_info = await self.db_manager.fetch_one(run_query, {"run_id": run_id})

            if not run_info:
                logger.error(f"[Run {run_id}] Run not found for hash verification")
                await _update_run_status(
                    self, run_id, "recovery_failed", "Run not found"
                )
                return

            gfs_t0_run_time = run_info["gfs_init_time_utc"]
            gfs_t_minus_6_run_time = gfs_t0_run_time - timedelta(hours=6)

            # 1. Wait for miners to fetch and hash data (same as normal flow)
            wait_minutes = self.config.get("validator_hash_wait_minutes", 10)
            if self.test_mode:
                original_wait = wait_minutes
                wait_minutes = 1
                logger.info(
                    f"[Run {run_id}] TEST MODE: Using shortened wait time of {wait_minutes} minute(s) instead of {original_wait} minutes for recovery"
                )

            logger.info(
                f"[Run {run_id}] Recovery workflow: Waiting {wait_minutes} minutes for miners to fetch GFS and compute input hash..."
            )
            await asyncio.sleep(wait_minutes * 60)
            logger.info(
                f"[Run {run_id}] Recovery wait finished. Proceeding with input hash verification."
            )

            # 2. Get miners to poll
            responses_to_check_query = """
                SELECT id, miner_hotkey, job_id
                FROM weather_miner_responses
                WHERE run_id = :run_id AND status = 'fetch_initiated'
            """
            miners_to_poll = await self.db_manager.fetch_all(
                responses_to_check_query, {"run_id": run_id}
            )
            logger.info(
                f"[Run {run_id}] Polling {len(miners_to_poll)} miners for input hash status."
            )

            if not miners_to_poll:
                logger.warning(
                    f"[Run {run_id}] No miners to poll for hash verification."
                )
                await _update_run_status(self, run_id, "no_miners_to_poll")
                return

            # 3. Poll miners for hash status (exact logic from validator_execute)
            miner_hash_results = {}
            polling_tasks = []

            async def _poll_single_miner(response_rec):
                resp_id = response_rec["id"]
                miner_hk = response_rec["miner_hotkey"]
                miner_job_id = response_rec["job_id"]
                logger.debug(
                    f"[Run {run_id}] Polling miner {miner_hk[:8]} (Job: {miner_job_id}) for input status using query_miners."
                )

                node = self.validator.metagraph.nodes.get(miner_hk)
                if not node or not node.ip or not node.port:
                    logger.warning(
                        f"[Run {run_id}] Miner {miner_hk[:8]} not found in metagraph or missing IP/Port. Cannot poll."
                    )
                    return resp_id, {
                        "status": "validator_poll_error",
                        "message": "Miner not found in metagraph",
                    }

                try:
                    from .schemas.weather_inputs import WeatherGetInputStatusData

                    status_payload_data = WeatherGetInputStatusData(job_id=miner_job_id)
                    status_payload = {
                        "nonce": str(uuid.uuid4()),
                        "data": status_payload_data.model_dump(),
                    }
                    endpoint = "/weather-get-input-status"

                    all_responses = await self.validator.query_miners(
                        payload=status_payload, endpoint=endpoint, hotkeys=[miner_hk]
                    )

                    status_response = all_responses.get(miner_hk)

                    if status_response:
                        parsed_response = status_response
                        if (
                            isinstance(status_response, dict)
                            and "text" in status_response
                        ):
                            try:
                                parsed_response = loads(status_response["text"])
                            except (Exception, TypeError) as json_err:
                                logger.warning(
                                    f"[Run {run_id}] Failed to parse status response text for {miner_hk[:8]}: {json_err}"
                                )
                                parsed_response = {
                                    "status": "parse_error",
                                    "message": str(json_err),
                                }

                        logger.debug(
                            f"[Run {run_id}] Received status from {miner_hk[:8]}: {parsed_response}"
                        )
                        return resp_id, parsed_response
                    else:
                        logger.warning(
                            f"[Run {run_id}] No response received from target miner {miner_hk[:8]} via query_miners."
                        )
                        return resp_id, {
                            "status": "validator_poll_failed",
                            "message": "No response from miner via query_miners",
                        }

                except Exception as poll_err:
                    logger.error(
                        f"[Run {run_id}] Error polling miner {miner_hk[:8]}: {poll_err}",
                        exc_info=True,
                    )
                    return resp_id, {
                        "status": "validator_poll_error",
                        "message": str(poll_err),
                    }

            for resp_rec in miners_to_poll:
                polling_tasks.append(_poll_single_miner(resp_rec))

            poll_results = await asyncio.gather(*polling_tasks)

            for resp_id, status_data in poll_results:
                miner_hash_results[resp_id] = status_data
            logger.info(
                f"[Run {run_id}] Collected input status from {len(miner_hash_results)}/{len(miners_to_poll)} miners."
            )

            # 4. Compute validator reference hash (exact logic from validator_execute)
            validator_input_hash = None
            try:
                logger.info(
                    f"[Run {run_id}] Validator computing its own reference input hash..."
                )
                from .utils.hashing import compute_input_data_hash
                from pathlib import Path

                gfs_cache_dir = Path(
                    self.config.get("gfs_analysis_cache_dir", "./gfs_analysis_cache")
                )
                validator_input_hash = await compute_input_data_hash(
                    t0_run_time=gfs_t0_run_time,
                    t_minus_6_run_time=gfs_t_minus_6_run_time,
                    cache_dir=gfs_cache_dir,
                )
                if validator_input_hash:
                    logger.info(
                        f"[Run {run_id}] Validator computed reference hash: {validator_input_hash[:10]}..."
                    )
                else:
                    logger.error(
                        f"[Run {run_id}] Validator failed to compute its own reference input hash. Cannot verify miners."
                    )
                    await _update_run_status(
                        self, run_id, "error", "Validator failed hash computation"
                    )
                    return

            except Exception as val_hash_err:
                logger.error(
                    f"[Run {run_id}] Error during validator hash computation: {val_hash_err}",
                    exc_info=True,
                )
                await _update_run_status(
                    self, run_id, "error", f"Validator hash error: {val_hash_err}"
                )
                return

            # 5. Compare hashes and update database (exact logic from validator_execute)
            miners_to_trigger = []
            if validator_input_hash:
                from .schemas.weather_outputs import WeatherTaskStatus

                update_tasks = []
                for i, (resp_id, status_data) in enumerate(miner_hash_results.items()):
                    miner_status = status_data.get("status")
                    miner_hash = status_data.get("input_data_hash")
                    error_msg = status_data.get("message")
                    new_db_status = None
                    hash_match = None

                    if (
                        miner_hash
                        and miner_status
                        == WeatherTaskStatus.INPUT_HASHED_AWAITING_VALIDATION.value
                    ):
                        if miner_hash == validator_input_hash:
                            logger.info(
                                f"[Run {run_id}] Hash MATCH for response ID {resp_id} (Miner status: {miner_status})!"
                            )
                            new_db_status = "input_validation_complete"
                            hash_match = True
                            orig_rec = next(
                                (m for m in miners_to_poll if m["id"] == resp_id), None
                            )
                            if orig_rec:
                                miners_to_trigger.append(
                                    (
                                        resp_id,
                                        orig_rec["miner_hotkey"],
                                        orig_rec["job_id"],
                                    )
                                )
                            else:
                                logger.error(
                                    f"[Run {run_id}] Could not find original record for resp_id {resp_id} to trigger inference."
                                )
                        else:
                            logger.warning(
                                f"[Run {run_id}] Hash MISMATCH for response ID {resp_id}. Miner: {miner_hash[:10]}... Validator: {validator_input_hash[:10]}... (Miner status: {miner_status})"
                            )
                            new_db_status = "input_hash_mismatch"
                            hash_match = False
                    elif miner_status == WeatherTaskStatus.FETCH_ERROR.value:
                        new_db_status = "input_fetch_error"
                    elif miner_status in [
                        WeatherTaskStatus.FETCHING_GFS.value,
                        WeatherTaskStatus.HASHING_INPUT.value,
                        WeatherTaskStatus.FETCH_QUEUED.value,
                    ]:
                        # Get current retry count for this response
                        retry_count_query = "SELECT retry_count FROM weather_miner_responses WHERE id = :resp_id"
                        retry_result = await self.db_manager.fetch_one(
                            retry_count_query, {"resp_id": resp_id}
                        )
                        current_retry_count = (
                            retry_result["retry_count"] if retry_result else 0
                        )

                        # Define retry intervals in minutes: 5, 10, 15
                        retry_intervals = [5, 10, 15]

                        if current_retry_count < len(retry_intervals):
                            # Schedule next retry
                            next_interval = retry_intervals[current_retry_count]
                            next_retry_time = datetime.now(timezone.utc) + timedelta(
                                minutes=next_interval
                            )

                            logger.info(
                                f"[Run {run_id}] Miner for response ID {resp_id} is still working (status: {miner_status}). "
                                f"Scheduling retry {current_retry_count + 1}/3 in {next_interval} minutes at {next_retry_time.strftime('%H:%M:%S')} UTC."
                            )

                            # Update to retry_scheduled status with next retry time and incremented retry count
                            update_query = """
                                UPDATE weather_miner_responses
                                SET status = 'retry_scheduled',
                                    retry_count = :retry_count,
                                    next_retry_time = :next_retry_time,
                                    last_polled_time = :now
                                WHERE id = :resp_id
                            """
                            update_tasks.append(
                                self.db_manager.execute(
                                    update_query,
                                    {
                                        "resp_id": resp_id,
                                        "retry_count": current_retry_count + 1,
                                        "next_retry_time": next_retry_time,
                                        "now": datetime.now(timezone.utc),
                                    },
                                )
                            )
                            new_db_status = (
                                None  # Don't process in the main update logic
                            )
                        else:
                            # Exhausted all retries, mark as failed
                            logger.warning(
                                f"[Run {run_id}] Miner for response ID {resp_id} exhausted all 3 retries (status: {miner_status}). "
                                f"Marking as input timeout after final retry at 15 minutes."
                            )
                            new_db_status = "input_hash_timeout"
                    elif miner_status in [
                        "validator_poll_failed",
                        "validator_poll_error",
                    ]:
                        new_db_status = "input_poll_error"
                        # The detailed error message is already captured in error_msg from status_data.get('message')
                    else:
                        new_db_status = "input_fetch_error"

                    if new_db_status:
                        update_query = """
                            UPDATE weather_miner_responses
                            SET status = :status,
                                input_hash_miner = :m_hash,
                                input_hash_validator = :v_hash,
                                input_hash_match = :match,
                                error_message = :err,
                                last_polled_time = :now
                            WHERE id = :resp_id
                        """
                        update_tasks.append(
                            self.db_manager.execute(
                                update_query,
                                {
                                    "resp_id": resp_id,
                                    "status": new_db_status,
                                    "m_hash": miner_hash,
                                    "v_hash": validator_input_hash,
                                    "match": hash_match,
                                    "err": error_msg if error_msg is not None else "",
                                    "now": datetime.now(timezone.utc),
                                },
                            )
                        )

                        # Yield control if processing many results
                        if (
                            len(miner_hash_results) > 20 and i % 20 == 19
                        ):  # Yield every 20 items after the 20th
                            await asyncio.sleep(0)

                # Execute all update tasks after the loop completes
                if update_tasks:
                    await asyncio.gather(*update_tasks)
                    logger.info(
                        f"[Run {run_id}] Updated DB for {len(update_tasks)} miner responses after hash check."
                    )

            # 6. Trigger inference for matching miners (exact logic from validator_execute)
            if miners_to_trigger:
                logger.info(
                    f"[Run {run_id}] Triggering inference for {len(miners_to_trigger)} miners with matching input hashes."
                )
                await _update_run_status(self, run_id, "triggering_inference")

                from .schemas.weather_inputs import WeatherStartInferenceData

                trigger_tasks = []

                async def _trigger_single_miner(resp_id, miner_hk, miner_job_id):
                    logger.debug(
                        f"[Run {run_id}] Attempting to trigger inference for {miner_hk[:8]} (Job: {miner_job_id}) using query_miners."
                    )
                    try:
                        trigger_payload_data = WeatherStartInferenceData(
                            job_id=miner_job_id
                        )
                        trigger_payload = {
                            "nonce": str(uuid.uuid4()),
                            "data": trigger_payload_data.model_dump(),
                        }
                        endpoint = "/weather-start-inference"

                        all_responses = await self.validator.query_miners(
                            payload=trigger_payload,
                            endpoint=endpoint,
                            hotkeys=[miner_hk],
                        )

                        trigger_response = all_responses.get(miner_hk)

                        parsed_response = trigger_response
                        if (
                            isinstance(trigger_response, dict)
                            and "text" in trigger_response
                        ):
                            try:
                                parsed_response = loads(trigger_response["text"])
                            except (Exception, TypeError) as json_err:
                                logger.warning(
                                    f"[Run {run_id}] Failed to parse trigger response text for {miner_hk[:8]}: {json_err}"
                                )
                                parsed_response = {
                                    "status": "parse_error",
                                    "message": str(json_err),
                                }

                        if (
                            parsed_response
                            and parsed_response.get("status")
                            == WeatherTaskStatus.INFERENCE_STARTED.value
                        ):
                            logger.info(
                                f"[Run {run_id}] Successfully triggered inference for {miner_hk[:8]} (Job: {miner_job_id})."
                            )
                            return resp_id, True
                        else:
                            logger.warning(
                                f"[Run {run_id}] Failed to trigger inference for {miner_hk[:8]} (Job: {miner_job_id}). Response: {parsed_response}"
                            )
                            return resp_id, False
                    except Exception as trigger_err:
                        logger.error(
                            f"[Run {run_id}] Error triggering inference for {miner_hk[:8]} (Job: {miner_job_id}): {trigger_err}",
                            exc_info=True,
                        )
                        return resp_id, False

                for resp_id, miner_hk, miner_job_id in miners_to_trigger:
                    trigger_tasks.append(
                        _trigger_single_miner(resp_id, miner_hk, miner_job_id)
                    )

                trigger_results = await asyncio.gather(*trigger_tasks)

                final_update_tasks = []
                triggered_count = 0
                for i, (resp_id, success) in enumerate(trigger_results):
                    if success:
                        triggered_count += 1
                        final_update_tasks.append(
                            self.db_manager.execute(
                                "UPDATE weather_miner_responses SET status = 'inference_triggered' WHERE id = :id",
                                {"id": resp_id},
                            )
                        )
                    # Yield control if processing many results
                    if (
                        len(trigger_results) > 20 and i % 20 == 19
                    ):  # Yield every 20 items after the 20th
                        await asyncio.sleep(0)

                if final_update_tasks:
                    await asyncio.gather(*final_update_tasks)
                logger.info(
                    f"[Run {run_id}] Completed inference trigger process. Successfully triggered {triggered_count}/{len(miners_to_trigger)} miners."
                )

                # 7. Update run status appropriately
                if triggered_count > 0:
                    await _update_run_status(self, run_id, "awaiting_inference_results")
                    logger.info(
                        f"[Run {run_id}] Hash verification workflow completed successfully."
                    )

                    # Trigger validator_score to continue the flow
                    logger.info(
                        f"[Run {run_id}] Triggering validator_score to continue processing..."
                    )
                    await self.validator_score()
                else:
                    await _update_run_status(
                        self, run_id, "inference_trigger_failed"
                    )  # No miners successfully triggered
            else:
                logger.warning(
                    f"[Run {run_id}] No miners eligible for inference trigger after hash verification."
                )

                # Check if there are miner responses scheduled for retry
                retry_cnt_row = await self.db_manager.fetch_one(
                    "SELECT COUNT(*) AS cnt FROM weather_miner_responses WHERE run_id = :rid AND status = 'retry_scheduled'",
                    {"rid": run_id},
                )
                retry_cnt = retry_cnt_row["cnt"] if retry_cnt_row else 0

                if retry_cnt > 0:
                    logger.info(
                        f"[Run {run_id}] {retry_cnt} miner responses are scheduled for retry. Keeping run in 'verifying_input_hashes' for retry cycle."
                    )
                    await _update_run_status(self, run_id, "verifying_input_hashes")
                else:
                    logger.warning(
                        f"[Run {run_id}] No miners with matching hashes and no retries scheduled. Marking as 'no_matching_hashes'."
                    )
                    await _update_run_status(self, run_id, "no_matching_hashes")

        except Exception as e:
            logger.error(
                f"[Run {run_id}] Error during hash verification workflow: {e}",
                exc_info=True,
            )
            await _update_run_status(
                self, run_id, "hash_verification_failed", error_message=str(e)
            )

    async def _create_scoring_job(self, run_id: int, score_type: str) -> bool:
        """Create a persistent scoring job record."""
        try:
            # Also enqueue into generic validator_jobs for unified dispatch
            job_type = (
                "weather.scoring.day1_qc" if score_type == "day1_qc" else "weather.scoring.era5_final"
            )
            payload = {"run_id": run_id, "score_type": score_type}
            _ = await self.db_manager.enqueue_validator_job(
                job_type=job_type, payload=payload, priority=50, run_id=run_id
            )
            # Keep existing table updated for compatibility
            query = """
            INSERT INTO weather_scoring_jobs 
            (run_id, score_type, status, created_at)
            VALUES (:run_id, :score_type, 'queued', :created_at)
            ON CONFLICT (run_id, score_type) DO UPDATE SET
            status = 'queued', created_at = EXCLUDED.created_at
            """
            await self.db_manager.execute(
                query,
                {
                    "run_id": run_id,
                    "score_type": score_type,
                    "created_at": datetime.now(timezone.utc),
                },
            )
            logger.info(f"[Run {run_id}] Created {score_type} scoring job (+ generic queue)")
            return True
        except Exception as e:
            logger.error(
                f"[Run {run_id}] Error creating {score_type} scoring job: {e}",
                exc_info=True,
            )
            return False

    async def _start_scoring_job(self, run_id: int, score_type: str) -> bool:
        """Mark a scoring job as started."""
        try:
            # Update generic queue status if exists (best-effort)
            try:
                jtype = (
                    "weather.scoring.day1_qc" if score_type == "day1_qc" else "weather.scoring.era5_final"
                )
                await self.db_manager.execute(
                    """
                    UPDATE validator_jobs
                    SET status = 'in_progress', started_at = COALESCE(started_at, NOW())
                    WHERE run_id = :rid AND job_type = :jtype AND status IN ('pending','retry_scheduled')
                    """,
                    {"rid": run_id, "jtype": jtype},
                )
            except Exception:
                pass
            query = """
            UPDATE weather_scoring_jobs 
            SET status = 'in_progress', started_at = :started_at
            WHERE run_id = :run_id AND score_type = :score_type
            """
            await self.db_manager.execute(
                query,
                {
                    "run_id": run_id,
                    "score_type": score_type,
                    "started_at": datetime.now(timezone.utc),
                },
            )
            logger.info(f"[Run {run_id}] Started {score_type} scoring job")
            return True
        except Exception as e:
            logger.error(
                f"[Run {run_id}] Error starting {score_type} scoring job: {e}",
                exc_info=True,
            )
            return False

    async def _complete_scoring_job(
        self,
        run_id: int,
        score_type: str,
        success: bool = True,
        error_message: str = None,
    ) -> bool:
        """Mark a scoring job as completed."""
        try:
            status = "completed" if success else "failed"
            # Best-effort finish generic queue item if exists
            try:
                jtype = (
                    "weather.scoring.day1_qc" if score_type == "day1_qc" else "weather.scoring.era5_final"
                )
                if success:
                    await self.db_manager.execute(
                        """
                        UPDATE validator_jobs
                        SET status = 'completed', completed_at = NOW(), lease_expires_at = NULL
                        WHERE run_id = :rid AND job_type = :jtype
                        """,
                        {"rid": run_id, "jtype": jtype},
                    )
                else:
                    await self.db_manager.execute(
                        """
                        UPDATE validator_jobs
                        SET status = 'failed', last_error = :err, completed_at = NOW(), lease_expires_at = NULL
                        WHERE run_id = :rid AND job_type = :jtype
                        """,
                        {"rid": run_id, "jtype": jtype, "err": (error_message or "")[:10000]},
                    )
            except Exception:
                pass
            query = """
            UPDATE weather_scoring_jobs 
            SET status = :status, completed_at = :completed_at, error_message = :error_msg
            WHERE run_id = :run_id AND score_type = :score_type
            """
            await self.db_manager.execute(
                query,
                {
                    "run_id": run_id,
                    "score_type": score_type,
                    "status": status,
                    "completed_at": datetime.now(timezone.utc),
                    "error_msg": error_message,
                },
            )
            logger.info(
                f"[Run {run_id}] Completed {score_type} scoring job (success: {success})"
            )
            return True
        except Exception as e:
            logger.error(
                f"[Run {run_id}] Error completing {score_type} scoring job: {e}",
                exc_info=True,
            )
            return False

    async def _track_progressive_scoring_completion(
        self, run_id: int, completed_lead_hours: List[int]
    ) -> bool:
        """Track completion of specific lead hours for progressive scoring."""
        try:
            # Update the final_scoring_attempted_time to track overall progress
            # but don't mark as fully complete until all desired lead hours are done
            all_configured_hours = self.config.get(
                "final_scoring_lead_hours",
                [24, 48, 72, 96, 120, 144, 168, 192, 216, 240],
            )

            # Check how many total lead hours have been scored for this run
            scored_hours_query = """
            SELECT DISTINCT lead_hours FROM weather_miner_scores 
            WHERE run_id = :run_id AND score_type LIKE '%era5%' AND score_type != 'era5_final_composite_score'
            AND lead_hours IS NOT NULL AND lead_hours >= 0
            ORDER BY lead_hours
            """
            scored_results = await self.db_manager.fetch_all(
                scored_hours_query, {"run_id": run_id}
            )
            already_scored_hours = [
                r["lead_hours"] for r in scored_results if r["lead_hours"] is not None
            ]

            total_scored_hours = set(already_scored_hours + completed_lead_hours)
            completion_ratio = len(total_scored_hours) / len(all_configured_hours)

            logger.info(
                f"[Run {run_id}] Progressive scoring status: {len(total_scored_hours)}/{len(all_configured_hours)} lead hours completed ({completion_ratio:.2%})"
            )

            # Update run status based on completion ratio
            if completion_ratio >= 0.8:  # 80% completion threshold
                new_status = "scored"
                logger.info(
                    f"[Run {run_id}] Progressive scoring substantially complete, marking as scored"
                )
            elif completion_ratio >= 0.5:  # 50% completion threshold
                new_status = "era5_final_scoring_partial"
            else:
                new_status = "era5_final_scoring_started"

            await self.db_manager.execute(
                "UPDATE weather_forecast_runs SET status = :status, final_scoring_attempted_time = :now WHERE id = :run_id",
                {
                    "status": new_status,
                    "now": datetime.now(timezone.utc),
                    "run_id": run_id,
                },
            )

            return True
        except Exception as e:
            logger.error(
                f"[Run {run_id}] Error tracking progressive scoring completion: {e}",
                exc_info=True,
            )
            return False

    async def _get_scoring_timeout_if_enabled(self) -> Optional[float]:
        """Get timeout for scoring operations if enabled, otherwise return None for no timeout."""
        if not self.config.get("scoring_timeout_enabled", False):
            return None

        # Return safety timeout in seconds
        timeout_hours = self.config.get("scoring_timeout_safety_hours", 2.0)
        return timeout_hours * 3600.0

    async def _wait_for_scoring_with_progress(
        self, event: asyncio.Event, timeout: Optional[float], run_id: int
    ):
        """Wait for scoring event with periodic progress logging."""
        start_time = time.time()
        progress_interval = self.config.get(
            "scoring_progress_log_interval", 300
        )  # 5 minutes
        last_progress_log = start_time

        while True:
            try:
                # Wait for either the event or the progress interval
                wait_time = (
                    min(progress_interval, timeout - (time.time() - start_time))
                    if timeout
                    else progress_interval
                )

                if wait_time <= 0 and timeout:
                    # Timeout reached
                    raise asyncio.TimeoutError()

                await asyncio.wait_for(event.wait(), timeout=wait_time)
                # Event was set - scoring completed
                return

            except asyncio.TimeoutError:
                current_time = time.time()
                elapsed = current_time - start_time

                # Check if this is a progress log timeout or actual timeout
                if timeout and elapsed >= timeout:
                    # Actual timeout reached
                    raise asyncio.TimeoutError()

                # Progress log timeout - log progress and continue waiting
                if current_time - last_progress_log >= progress_interval:
                    logger.info(
                        f"TEST MODE: Still waiting for Day-1 scoring of run {run_id} - elapsed: {elapsed/60:.1f} minutes"
                    )
                    last_progress_log = current_time

    async def _recover_incomplete_scoring_jobs(self):
        """Recover scoring jobs that were interrupted by restarts."""
        try:
            # Find jobs that were in progress when restart happened
            # Exclude runs that have already completed their scoring to avoid race conditions
            query = """
            SELECT wj.run_id, wj.score_type, wj.started_at
            FROM weather_scoring_jobs wj
            JOIN weather_forecast_runs wfr ON wj.run_id = wfr.id
            WHERE wj.status = 'in_progress'
            AND wj.started_at > (CURRENT_TIMESTAMP - INTERVAL '24 hours')
            AND NOT (
                (wj.score_type = 'day1_qc' AND wfr.status IN ('day1_scoring_complete', 'completed', 'scored')) OR
                (wj.score_type = 'era5_final' AND wfr.status IN ('completed', 'scored')) OR
                (wj.score_type = 'day1_qc' AND wfr.status LIKE 'era5_%')  -- Don't re-trigger Day-1 if already in ERA5 phases
            )
            ORDER BY wj.started_at ASC
            """
            incomplete_jobs = await self.db_manager.fetch_all(query)

            # Additional filtering to prevent Day-1 jobs from being re-triggered for runs with existing Day-1 scores
            filtered_jobs = []
            for job in incomplete_jobs:
                if job["score_type"] == "day1_qc":
                    # Check if this run already has Day-1 scores
                    existing_scores = await self.db_manager.fetch_one(
                        "SELECT COUNT(*) as count FROM weather_miner_scores WHERE run_id = :run_id AND score_type = 'day1_qc_score'",
                        {"run_id": job["run_id"]},
                    )

                    if existing_scores and existing_scores["count"] > 0:
                        logger.info(
                            f"Run {job['run_id']}: Skipping Day-1 job recovery - already has {existing_scores['count']} Day-1 scores"
                        )
                        # Mark the job as completed to clear the in_progress status
                        await self.db_manager.execute(
                            "UPDATE weather_scoring_jobs SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE run_id = :run_id AND score_type = 'day1_qc'",
                            {"run_id": job["run_id"]},
                        )
                        continue

                filtered_jobs.append(job)

            incomplete_jobs = filtered_jobs

            # Also find runs that are in partial scoring states but might have stalled
            stalled_partial_query = """
            SELECT id as run_id, status, final_scoring_attempted_time
            FROM weather_forecast_runs 
            WHERE status IN ('era5_final_scoring_partial', 'era5_final_scoring_started')
            AND (
                final_scoring_attempted_time IS NULL 
                OR final_scoring_attempted_time < (CURRENT_TIMESTAMP - INTERVAL '2 hours')
            )
            ORDER BY final_scoring_attempted_time ASC NULLS FIRST
            LIMIT 10
            """
            stalled_runs = await self.db_manager.fetch_all(stalled_partial_query)

            total_recoveries = len(incomplete_jobs) + len(stalled_runs)
            if total_recoveries == 0:
                logger.info(
                    "No incomplete scoring jobs or stalled runs found to recover"
                )
                return

            logger.info(
                f"Found {len(incomplete_jobs)} incomplete scoring job(s) and {len(stalled_runs)} stalled partial run(s) to recover:"
            )

            # Recover incomplete scoring jobs
            for job in incomplete_jobs:
                run_id = job["run_id"]
                score_type = job["score_type"]
                started_at = job["started_at"]

                logger.info(
                    f"  - Run {run_id}: {score_type} scoring job (started: {started_at}) - resetting and retrying"
                )

                # Reset to queued status and re-trigger
                await self._reset_scoring_job(run_id, score_type)
                await self._trigger_scoring_job(run_id, score_type)

            # Recover stalled partial runs
            for run in stalled_runs:
                run_id = run["run_id"]
                status = run["status"]
                last_attempt = run["final_scoring_attempted_time"]

                logger.info(
                    f"  - Run {run_id}: stalled in '{status}' (last attempt: {last_attempt}) - resuming progressive scoring"
                )

                # Create a new era5_final scoring job to resume progressive scoring
                if await self._create_scoring_job(run_id, "era5_final"):
                    # Update the attempted time to mark it as actively being retried
                    await self.db_manager.execute(
                        "UPDATE weather_forecast_runs SET final_scoring_attempted_time = :now WHERE id = :run_id",
                        {"now": datetime.now(timezone.utc), "run_id": run_id},
                    )
                    logger.info(
                        f"  - Run {run_id}: created new era5_final scoring job for progressive resume"
                    )
                else:
                    logger.warning(
                        f"  - Run {run_id}: failed to create scoring job for recovery"
                    )

        except Exception as e:
            logger.error(
                f"Error recovering incomplete scoring jobs: {e}", exc_info=True
            )

    async def _reset_scoring_job(self, run_id: int, score_type: str):
        """Reset a scoring job back to queued status."""
        try:
            query = """
            UPDATE weather_scoring_jobs 
            SET status = 'queued', started_at = NULL, error_message = NULL
            WHERE run_id = :run_id AND score_type = :score_type
            """
            await self.db_manager.execute(
                query, {"run_id": run_id, "score_type": score_type}
            )
            logger.info(f"[Run {run_id}] Reset {score_type} scoring job to queued")
        except Exception as e:
            logger.error(
                f"[Run {run_id}] Error resetting {score_type} scoring job: {e}",
                exc_info=True,
            )

    async def _trigger_scoring_job(self, run_id: int, score_type: str):
        """Trigger a specific scoring job."""
        try:
            if score_type == "day1_qc":
                # Add to initial scoring queue
                await self.initial_scoring_queue.put(run_id)
                logger.info(f"[Run {run_id}] Re-queued day1_qc scoring job")

            elif score_type == "era5_final":
                # Trigger final scoring directly - we need to import the function
                from .processing.weather_logic import _trigger_final_scoring

                await _trigger_final_scoring(self, run_id)
                logger.info(f"[Run {run_id}] Re-triggered era5_final scoring job")

        except Exception as e:
            logger.error(
                f"[Run {run_id}] Error triggering {score_type} scoring job: {e}",
                exc_info=True,
            )

    async def _backfill_scoring_jobs_from_existing_data(self):
        """
        One-time backfill of scoring jobs table from existing weather_forecast_runs and weather_miner_responses data.
        This recovers scoring work that existed before the scoring jobs table was created.
        Compatible with all possible miner response stages and run states.
        """
        try:
            logger.info(
                "Checking if scoring jobs table needs backfilling from existing data..."
            )

            # Check if we've already done this backfill (look for any scoring jobs)
            existing_jobs_count = await self.db_manager.fetch_one(
                "SELECT COUNT(*) as count FROM weather_scoring_jobs"
            )

            if existing_jobs_count["count"] > 0:
                logger.debug(
                    "Scoring jobs table already contains data, skipping backfill"
                )
                return

            logger.info(
                "Backfilling scoring jobs table from existing incomplete work..."
            )

            # 1. Find runs that need Day-1 scoring - comprehensive approach covering all miner response stages
            day1_candidates_query = """
            SELECT DISTINCT wfr.id as run_id, wfr.gfs_init_time_utc, wfr.status as run_status,
                   COUNT(CASE WHEN wr.verification_passed = TRUE THEN 1 END) as verified_count,
                   COUNT(CASE WHEN wr.status IN ('inference_triggered', 'awaiting_forecast_submission', 'forecast_submitted') THEN 1 END) as inference_ready_count,
                   COUNT(CASE WHEN wr.status IN ('input_validation_complete', 'inference_triggered', 'awaiting_forecast_submission', 'forecast_submitted') THEN 1 END) as hash_passed_count
            FROM weather_forecast_runs wfr
            LEFT JOIN weather_miner_responses wr ON wfr.id = wr.run_id
            WHERE wfr.id NOT IN (
                SELECT DISTINCT run_id 
                FROM weather_miner_scores 
                WHERE score_type = 'day1_qc_score'
            )
            AND wfr.status IN (
                'awaiting_inference_results', 'verifying_miner_forecasts', 
                'day1_scoring_complete', 'completed', 'scored'
            )
            GROUP BY wfr.id, wfr.gfs_init_time_utc, wfr.status
            HAVING (
                COUNT(CASE WHEN wr.verification_passed = TRUE THEN 1 END) > 0 OR
                COUNT(CASE WHEN wr.status IN ('inference_triggered', 'awaiting_forecast_submission', 'forecast_submitted') THEN 1 END) > 0
            )
            ORDER BY wfr.gfs_init_time_utc DESC
            LIMIT 20
            """

            day1_candidates = await self.db_manager.fetch_all(day1_candidates_query)
            logger.info(f"Found {len(day1_candidates)} runs needing Day-1 scoring jobs")

            for candidate in day1_candidates:
                run_id = candidate["run_id"]
                verified_count = candidate["verified_count"]
                inference_ready_count = candidate["inference_ready_count"]
                hash_passed_count = candidate["hash_passed_count"]
                run_status = candidate["run_status"]

                try:
                    await self._create_scoring_job(run_id, "day1_qc")
                    logger.debug(
                        f"Created day1_qc scoring job for run {run_id} (status: {run_status}, verified: {verified_count}, inference_ready: {inference_ready_count}, hash_passed: {hash_passed_count})"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to create day1_qc job for run {run_id}: {e}"
                    )

            # 2. Additional check for runs that might be in intermediate states needing workflow recovery
            intermediate_runs_query = """
            SELECT DISTINCT wfr.id as run_id, wfr.gfs_init_time_utc, wfr.status as run_status,
                   COUNT(wr.id) as response_count,
                   COUNT(CASE WHEN wr.status = 'input_validation_complete' THEN 1 END) as hash_validated_count,
                   COUNT(CASE WHEN wr.status = 'inference_triggered' THEN 1 END) as inference_triggered_count
            FROM weather_forecast_runs wfr
            LEFT JOIN weather_miner_responses wr ON wfr.id = wr.run_id
            WHERE wfr.status IN (
                'sending_fetch_requests', 'awaiting_input_hashes', 'verifying_input_hashes', 
                'triggering_inference', 'awaiting_inference_results'
            )
            AND wfr.run_initiation_time > (CURRENT_TIMESTAMP - INTERVAL '48 hours')
            GROUP BY wfr.id, wfr.gfs_init_time_utc, wfr.status
            HAVING COUNT(wr.id) > 0
            ORDER BY wfr.gfs_init_time_utc DESC
            LIMIT 15
            """

            intermediate_runs = await self.db_manager.fetch_all(intermediate_runs_query)
            logger.info(
                f"Found {len(intermediate_runs)} runs in intermediate states that may need workflow recovery"
            )

            for run in intermediate_runs:
                run_id = run["run_id"]
                run_status = run["run_status"]
                response_count = run["response_count"]
                hash_validated_count = run["hash_validated_count"]
                inference_triggered_count = run["inference_triggered_count"]

                # Create scoring jobs for runs that are far enough along in the process
                if run_status in ["awaiting_inference_results"] or (
                    hash_validated_count > 0 or inference_triggered_count > 0
                ):
                    try:
                        # Check if already has day1_qc scoring job from previous query
                        existing_job = await self.db_manager.fetch_one(
                            "SELECT COUNT(*) as count FROM weather_scoring_jobs WHERE run_id = :run_id AND score_type = 'day1_qc'",
                            {"run_id": run_id},
                        )

                        if existing_job["count"] == 0:
                            await self._create_scoring_job(run_id, "day1_qc")
                            logger.debug(
                                f"Created day1_qc scoring job for intermediate run {run_id} (status: {run_status}, responses: {response_count}, hash_validated: {hash_validated_count}, inference_triggered: {inference_triggered_count})"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Failed to create day1_qc job for intermediate run {run_id}: {e}"
                        )

            # 3. Find runs that need ERA5 final scoring (have day1_qc scores but no era5 scores, and are old enough)
            era5_delay_days = self.config.get("era5_delay_days", 5)
            cutoff_time = datetime.now(timezone.utc) - timedelta(days=era5_delay_days)

            era5_candidates_query = """
            SELECT DISTINCT wms.run_id, wfr.gfs_init_time_utc, wfr.status as run_status
            FROM weather_miner_scores wms
            JOIN weather_forecast_runs wfr ON wms.run_id = wfr.id
            WHERE wms.score_type = 'day1_qc_score'
            AND wfr.gfs_init_time_utc < :cutoff_time
            AND wms.run_id NOT IN (
                SELECT DISTINCT run_id 
                FROM weather_miner_scores 
                WHERE score_type LIKE '%era5%'
            )
            AND wfr.status IN ('day1_scoring_complete', 'completed', 'scored')
            ORDER BY wfr.gfs_init_time_utc DESC
            LIMIT 10
            """

            era5_candidates = await self.db_manager.fetch_all(
                era5_candidates_query, {"cutoff_time": cutoff_time}
            )
            logger.info(
                f"Found {len(era5_candidates)} runs needing ERA5 final scoring jobs"
            )

            for candidate in era5_candidates:
                run_id = candidate["run_id"]
                run_status = candidate["run_status"]
                try:
                    await self._create_scoring_job(run_id, "era5_final")
                    logger.debug(
                        f"Created era5_final scoring job for run {run_id} (status: {run_status})"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to create era5_final job for run {run_id}: {e}"
                    )

            total_backfilled = (
                len(day1_candidates) + len(intermediate_runs) + len(era5_candidates)
            )
            if total_backfilled > 0:
                logger.info(
                    f"✅ Successfully backfilled {total_backfilled} scoring jobs from existing data"
                )
                logger.info(f"   - Day-1 scoring: {len(day1_candidates)} runs")
                logger.info(f"   - Intermediate states: {len(intermediate_runs)} runs")
                logger.info(f"   - ERA5 final scoring: {len(era5_candidates)} runs")
            else:
                logger.info("No incomplete scoring work found in existing data")

        except Exception as e:
            logger.error(f"Error during scoring jobs backfill: {e}", exc_info=True)
