import asyncio
import traceback
from typing import Any, Dict, List, Optional, Union, Tuple
from uuid import uuid4
from datetime import datetime, timezone, timedelta
import os
import importlib.util
import json
from pydantic import Field, ConfigDict
from fiber.logging_utils import get_logger
from gaia.tasks.base.task import Task
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

from .utils.era5_api import fetch_era5_data
from .utils.gfs_api import fetch_gfs_analysis_data, fetch_gfs_data
from .utils.hashing import compute_verification_hash, compute_input_data_hash
from .utils.kerchunk_utils import generate_kerchunk_json_from_local_file
from .utils.data_prep import create_aurora_batch_from_gfs
from .schemas.weather_metadata import WeatherMetadata
from .schemas.weather_inputs import WeatherInputs, WeatherForecastRequest, WeatherInputData, WeatherInitiateFetchData, WeatherGetInputStatusData, WeatherStartInferenceData
from .schemas.weather_outputs import WeatherOutputs, WeatherKerchunkResponseData
from .weather_scoring.metrics import calculate_rmse
from .processing.weather_miner_preprocessing import prepare_miner_batch_from_payload
from .processing.weather_logic import (
    _update_run_status, build_score_row, get_ground_truth_data,
    _trigger_initial_scoring, _request_fresh_token, verify_miner_response,
    get_job_by_gfs_init_time, update_job_status, update_job_paths
)
from .processing.weather_workers import (
    initial_scoring_worker, 
    finalize_scores_worker, 
    cleanup_worker,
    run_inference_background,
    fetch_and_hash_gfs_task
)
from gaia.tasks.defined_tasks.weather.utils.inference_class import WeatherInferenceRunner
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
            try: return [int(x.strip()) for x in val_str.split(',')]
            except ValueError: logger.warning(f"Invalid format for {env_var}: '{val_str}'. Using default.")
        return default_list
        
    # Worker/Run Parameters
    config['max_concurrent_inferences'] = int(os.getenv('WEATHER_MAX_CONCURRENT_INFERENCES', '1'))
    config['inference_steps'] = int(os.getenv('WEATHER_INFERENCE_STEPS', '40'))
    config['forecast_step_hours'] = int(os.getenv('WEATHER_FORECAST_STEP_HOURS', '6'))
    config['forecast_duration_hours'] = config['inference_steps'] * config['forecast_step_hours']
    
    # Scoring Parameters
    config['initial_scoring_lead_hours'] = parse_int_list('WEATHER_INITIAL_SCORING_LEAD_HOURS', [6, 12]) # Day 0.25, 0.5
    config['final_scoring_lead_hours'] = parse_int_list('WEATHER_FINAL_SCORING_LEAD_HOURS', [120, 168]) # Day 5, 7
    config['verification_wait_minutes'] = int(os.getenv('WEATHER_VERIFICATION_WAIT_MINUTES', '30'))
    config['verification_timeout_seconds'] = int(os.getenv('WEATHER_VERIFICATION_TIMEOUT_SECONDS', '3600')) # Default to 600s (10 minutes)
    config['final_scoring_check_interval_seconds'] = int(os.getenv('WEATHER_FINAL_SCORING_INTERVAL_S', '3600')) # 1 hour
    config['era5_delay_days'] = int(os.getenv('WEATHER_ERA5_DELAY_DAYS', '5'))
    config['cleanup_check_interval_seconds'] = int(os.getenv('WEATHER_CLEANUP_INTERVAL_S', '21600')) # 6 hours
    config['gfs_analysis_cache_dir'] = os.getenv('WEATHER_GFS_CACHE_DIR', './gfs_analysis_cache')
    config['era5_cache_dir'] = os.getenv('WEATHER_ERA5_CACHE_DIR', './era5_cache')
    config['gfs_cache_retention_days'] = int(os.getenv('WEATHER_GFS_CACHE_RETENTION_DAYS', '7'))
    config['era5_cache_retention_days'] = int(os.getenv('WEATHER_ERA5_CACHE_RETENTION_DAYS', '30'))
    config['ensemble_retention_days'] = int(os.getenv('WEATHER_ENSEMBLE_RETENTION_DAYS', '14'))
    config['db_run_retention_days'] = int(os.getenv('WEATHER_DB_RUN_RETENTION_DAYS', '90'))
    config['run_hour_utc'] = int(os.getenv('WEATHER_RUN_HOUR_UTC', '1'))
    config['run_minute_utc'] = int(os.getenv('WEATHER_RUN_MINUTE_UTC', '0'))
    config['validator_hash_wait_minutes'] = int(os.getenv('WEATHER_VALIDATOR_HASH_WAIT_MINUTES', '5'))

    config['miner_jwt_secret_key'] = os.getenv("MINER_JWT_SECRET_KEY", "insecure_default_key_for_development_only")
    config['jwt_algorithm'] = os.getenv("MINER_JWT_ALGORITHM", "HS256")
    config['access_token_expire_minutes'] = int(os.getenv('WEATHER_ACCESS_TOKEN_EXPIRE_MINUTES', '120'))

    config['era5_climatology_path'] = os.getenv(
        'WEATHER_ERA5_CLIMATOLOGY_PATH', 
        'gs://weatherbench2/datasets/era5-hourly-climatology/1990-2019_6h_1440x721.zarr'
    )

    # Day-1 Scoring Specific Configurations
    default_day1_vars_levels = [
        {"name": "z", "level": 500, "standard_name": "geopotential"},
        {"name": "t", "level": 850, "standard_name": "temperature"},
        {"name": "2t", "level": None, "standard_name": "2m_temperature"},
        {"name": "msl", "level": None, "standard_name": "mean_sea_level_pressure"}
    ]
    try:
        day1_vars_levels_json = os.getenv('WEATHER_DAY1_VARIABLES_LEVELS_JSON')
        config['day1_variables_levels_to_score'] = json.loads(day1_vars_levels_json) if day1_vars_levels_json else default_day1_vars_levels
    except json.JSONDecodeError:
        logger.warning("Invalid JSON for WEATHER_DAY1_VARIABLES_LEVELS_JSON. Using default.")
        config['day1_variables_levels_to_score'] = default_day1_vars_levels

    default_day1_clim_bounds = {
        "2t": (180, 340),                             # Kelvin
        "msl": (90000, 110000),                       # Pascals
        "t850": (220, 320),                           # Kelvin for 850hPa Temperature
        "z500": (45000, 60000)                        # m^2/s^2 for Geopotential at 500hPa (approx 4500-6000 gpm)
    }
    try:
        day1_clim_bounds_json = os.getenv('WEATHER_DAY1_CLIMATOLOGY_BOUNDS_JSON')
        config['day1_climatology_bounds'] = json.loads(day1_clim_bounds_json) if day1_clim_bounds_json else default_day1_clim_bounds
    except json.JSONDecodeError:
        logger.warning("Invalid JSON for WEATHER_DAY1_CLIMATOLOGY_BOUNDS_JSON. Using default.")
        config['day1_climatology_bounds'] = default_day1_clim_bounds

    config['day1_pattern_correlation_threshold'] = float(os.getenv('WEATHER_DAY1_PATTERN_CORR_THRESHOLD', '0.3'))
    config['day1_acc_lower_bound'] = float(os.getenv('WEATHER_DAY1_ACC_LOWER_BOUND', '0.6'))
    config['day1_alpha_skill'] = float(os.getenv('WEATHER_DAY1_ALPHA_SKILL', '0.6'))
    config['day1_beta_acc'] = float(os.getenv('WEATHER_DAY1_BETA_ACC', '0.4'))

    # Quality Control Config - I need to tune these values
    config['day1_clone_penalty_gamma'] = float(os.getenv('WEATHER_DAY1_CLONE_PENALTY_GAMMA', '1.0'))
    default_clone_delta_thresholds = {
        "2t": 0.01,  # (RMSE 0.1K)^2
        "msl": 20000, # (RMSE 100Pa or 1hPa)^2
        "z500": 10000,   # (RMSE 100 m^2/s^2)^2 for geopotential
        "t850": 0.25    # (RMSE 0.5K)^2 
    }
    try:
        clone_delta_json = os.getenv('WEATHER_DAY1_CLONE_DELTA_THRESHOLDS_JSON')
        config['day1_clone_delta_thresholds'] = json.loads(clone_delta_json) if clone_delta_json else default_clone_delta_thresholds
    except json.JSONDecodeError:
        logger.warning("Invalid JSON for WEATHER_DAY1_CLONE_DELTA_THRESHOLDS_JSON. Using default.")
        config['day1_clone_delta_thresholds'] = default_clone_delta_thresholds

    logger.info(f"WeatherTask configuration loaded: {config}")
    return config

class WeatherTask(Task):
    db_manager: Union[ValidatorDatabaseManager, MinerDatabaseManager]
    node_type: str = Field(default="validator")
    test_mode: bool = Field(default=False)
    era5_climatology_ds: Optional[xr.Dataset] = Field(default=None, exclude=True) # Exclude from Pydantic model
 
    model_config = ConfigDict(
        extra = 'allow',
        arbitrary_types_allowed=True
    )

    def __init__(self, db_manager=None, node_type=None, test_mode=False, keypair=None, **data):
        loaded_config = self._load_config() 
        
        super_data = {
            "name": "WeatherTask",
            "description": "Weather forecast generation and verification task",
            "task_type": "atomic",
            "metadata": WeatherMetadata(),
            "db_manager": db_manager,
            "node_type": node_type,
            "test_mode": test_mode,
            **data 
        }
        super().__init__(**super_data)
        
        self.keypair = keypair
        self.db_manager = db_manager
        self.node_type = node_type
        self.test_mode = test_mode
        self.config = loaded_config 
        self.validator = data.get('validator')
        self.era5_climatology_ds = None

        self.initial_scoring_queue = asyncio.Queue()
        self.initial_scoring_worker_running = False
        self.initial_scoring_workers = []
        self.final_scoring_worker_running = False
        self.final_scoring_workers = []
        self.cleanup_worker_running = False
        self.cleanup_workers = []
        
        self.test_mode_run_scored_event = asyncio.Event()
        self.last_test_mode_run_id = None
        
        self.gpu_semaphore = asyncio.Semaphore(self.config.get('max_concurrent_inferences', 1))
        
        self.inference_runner = None 
        if self.node_type == "miner":
            try:
                # Inference type from .env
                inference_type = os.getenv("WEATHER_INFERENCE_TYPE", "local").lower()
                load_local_model_flag = True

                if inference_type == "azure_foundry":
                    logger.info("Configured to use Azure Foundry for inference. Local model will not be loaded.")
                    load_local_model_flag = False
                elif inference_type == "local":
                    logger.info("Configured to use local model for inference.")
                else:
                    logger.warning(f"Invalid WEATHER_INFERENCE_TYPE: '{inference_type}'. Defaulting to 'local'.")
                    inference_type = "local"
                
                self.config['weather_inference_type'] = inference_type 

                device = "cuda" if torch.cuda.is_available() else "cpu"
                self.inference_runner = WeatherInferenceRunner(device=device, load_local_model=load_local_model_flag)
                 
                if load_local_model_flag and self.inference_runner.model is not None:
                    logger.info(f"Initialized Miner components (Local Inference Runner on {device}, model loaded).")
                elif load_local_model_flag and self.inference_runner.model is None:
                    logger.error(f"Initialized Miner components (Local Inference Runner on {device}, BUT MODEL FAILED TO LOAD).")
                else:
                    logger.info(f"Initialized Miner components (Prepared for {inference_type} inference).")

            except Exception as e:
                logger.error(f"Failed to initialize WeatherInferenceRunner or set inference type: {e}", exc_info=True)
                self.inference_runner = None 
                self.config['weather_inference_type'] = "local" # Fallback
        else: # Validator
             logger.info("Initialized validator components for WeatherTask")

    _load_config = _load_config

    ############################################################
    # Validator methods
    ############################################################

    async def _get_or_load_era5_climatology(self) -> Optional[xr.Dataset]:
        if self.era5_climatology_ds is None:
            climatology_path = self.config.get("era5_climatology_path")
            if climatology_path:
                try:
                    logger.info(f"Loading ERA5 climatology from: {climatology_path}")
                    self.era5_climatology_ds = await asyncio.to_thread(
                        xr.open_zarr, climatology_path, consolidated=True 
                    )
                    logger.info("ERA5 climatology loaded successfully.")
                except Exception as e:
                    logger.error(f"Failed to load ERA5 climatology from {climatology_path}: {e}", exc_info=True)
                    self.era5_climatology_ds = None
            else:
                logger.error("WEATHER_ERA5_CLIMATOLOGY_PATH not configured. Cannot load climatology for ACC calculation.")
        return self.era5_climatology_ds

    async def build_score_row(self, run_id: int, gfs_init_time: datetime, evaluation_results: List[Dict], task_name_prefix: str):
        """
        Builds the score row for a given run and stores it in the score_table.
        evaluation_results is a list of dicts, each from an evaluation function (e.g., evaluate_miner_forecast_day1).
        task_name_prefix is used to form the task_name in score_table (e.g., 'weather_day1_qc', 'weather_era5_final').
        """
        logger.info(f"[BuildScoreRow] Building {task_name_prefix} score row for run_id: {run_id}")
        all_miner_scores_for_run: Dict[int, float] = {}

        for eval_result in evaluation_results:
            if isinstance(eval_result, Exception) or not isinstance(eval_result, dict):
                logger.warning(f"[BuildScoreRow] Skipping invalid evaluation result for {task_name_prefix}: {type(eval_result)}")
                continue
            
            miner_uid = eval_result.get("miner_uid")
            score_value = eval_result.get("final_score_for_uid") 

            if miner_uid is not None and score_value is not None and np.isfinite(score_value):
                all_miner_scores_for_run[miner_uid] = float(score_value)
            elif miner_uid is not None:
                all_miner_scores_for_run[miner_uid] = 0.0 

        final_scores_list = [0.0] * 256
        
        for uid, score in all_miner_scores_for_run.items():
            if 0 <= uid < 256:
                final_scores_list[uid] = score
        

        score_row_data = {
            "task_name": task_name_prefix,
            "task_id": str(run_id),
            "score": final_scores_list,
            "status": f"{task_name_prefix}_scores_compiled",
            "gfs_init_time_for_table": gfs_init_time
        }

        try:
            upsert_score_table_query = """
                INSERT INTO score_table (task_name, task_id, score, status, created_at)
                VALUES (:task_name, :task_id, :score, :status, :created_at_val)
                ON CONFLICT (task_name, task_id) DO UPDATE SET
                    score = EXCLUDED.score,
                    status = EXCLUDED.status,
                    created_at = EXCLUDED.created_at
            """
            
            db_params_score_table = {
                "task_name": score_row_data["task_name"],
                "task_id": score_row_data["task_id"],
                "score": score_row_data["score"],
                "status": score_row_data["status"],
                "created_at_val": score_row_data["gfs_init_time_for_table"]
            }

            await self.db_manager.execute(upsert_score_table_query, db_params_score_table)
            logger.info(f"[BuildScoreRow] Upserted score_table entry for {task_name_prefix}, task_id (run_id): {run_id}")

        except Exception as e_db_score_table:
            logger.error(f"[BuildScoreRow] DB error storing {task_name_prefix} score row for run {run_id}: {e_db_score_table}", exc_info=True)

    async def validator_prepare_subtasks(self):
        """
        Prepares data needed for a forecast run (e.g., identifying GFS data).
        Since this is 'atomic', it doesn't prepare sub-tasks in the composite sense,
        but rather the overall input for querying miners.
        """
        pass

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
            await self.start_background_workers(num_initial_scoring_workers=1, num_final_scoring_workers=1, num_cleanup_workers=1)
            logger.info("Started background workers for scoring and cleanup.")

        run_hour_utc = self.config.get('run_hour_utc', 12) # Default to 12 if not in config for some reason
        run_minute_utc = self.config.get('run_minute_utc', 0)
        logger.info(f"Validator execute loop configured to run around {run_hour_utc:02d}:{run_minute_utc:02d} UTC.")

        if self.test_mode:
             logger.warning("Running in TEST MODE: Execution will run once immediately.")

        while True:
            try:
                await validator.update_task_status('weather', 'active', 'waiting')
                now_utc = datetime.now(timezone.utc)

                if not self.test_mode:
                    target_run_time_today = now_utc.replace(hour=run_hour_utc, minute=run_minute_utc, second=0, microsecond=0)
                    if now_utc >= target_run_time_today:
                        next_run_trigger_time = target_run_time_today + timedelta(days=1)
                    else:
                         next_run_trigger_time = target_run_time_today

                    wait_seconds = (next_run_trigger_time - now_utc).total_seconds()
                    logger.info(f"Current time: {now_utc}. Next weather run scheduled at {next_run_trigger_time}. Waiting for {wait_seconds:.2f} seconds.")
                    if wait_seconds > 0:
                         await asyncio.sleep(wait_seconds)
                    now_utc = datetime.now(timezone.utc)

                logger.info(f"Initiating weather forecast run triggered around {now_utc}...")
                await validator.update_task_status('weather', 'processing', 'initializing_run')

                gfs_t0_run_time = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
                gfs_t_minus_6_run_time = gfs_t0_run_time - timedelta(hours=6) # This will be 18Z from the previous day

                logger.info(f"Target GFS T=0h Analysis Run Time: {gfs_t0_run_time}")
                logger.info(f"Target GFS T=-6h Analysis Run Time: {gfs_t_minus_6_run_time}")

                run_id = None

                try:
                    run_insert_query = """
                        INSERT INTO weather_forecast_runs (run_initiation_time, target_forecast_time_utc, gfs_init_time_utc, status)
                        VALUES (:init_time, :target_time, :gfs_init, :status)
                        RETURNING id
                    """
                    effective_forecast_start_time = gfs_t0_run_time
                    run_record = await self.db_manager.fetch_one(run_insert_query, { 
                         "init_time": now_utc,
                         "target_time": effective_forecast_start_time,
                         "gfs_init": effective_forecast_start_time,
                         "status": "fetching_gfs"
                    })

                    if run_record and 'id' in run_record:
                        run_id = run_record['id']
                        logger.info(f"[Run {run_id}] Created weather_forecast_runs record with ID: {run_id}")
                    else:
                        logger.error("Failed to retrieve run_id using fetch_one after insert.")
                        raise RuntimeError("Failed to create run_id for forecast run.")

                except Exception as db_err:
                     logger.error(f"Failed to create forecast run record in DB: {db_err}", exc_info=True)
                     await asyncio.sleep(60)
                     continue 

                await validator.update_task_status('weather', 'processing', 'sending_fetch_requests')
                await _update_run_status(self, run_id, "sending_fetch_requests")

                payload_data = WeatherInitiateFetchData(
                     forecast_start_time=gfs_t0_run_time,   # T=0h time
                     previous_step_time=gfs_t_minus_6_run_time # T=-6h time
                )
                payload_dict = payload_data.model_dump(mode='json')
                
                payload = {
                     "nonce": str(uuid.uuid4()),
                     "data": payload_dict 
                }

                logger.info(f"[Run {run_id}] Querying miners with weather initiate fetch request (Endpoint: /weather-initiate-fetch)... Payload size approx: {len(json.dumps(payload))} bytes")
                responses = await validator.query_miners(
                     payload=payload,
                     endpoint="/weather-initiate-fetch"
                )
                logger.info(f"[Run {run_id}] Received {len(responses)} initial responses from miners for fetch initiation.")

                await validator.update_task_status('weather', 'processing', 'recording_acceptances')
                accepted_count = 0
                for miner_hotkey, response_data in responses.items():
                     try:
                         miner_response = response_data
                         if isinstance(response_data, dict) and 'text' in response_data:
                             try:
                                 miner_response = json.loads(response_data['text'])
                                 logger.debug(f"[Run {run_id}] Parsed JSON from response text for {miner_hotkey}: {miner_response}")
                             except (json.JSONDecodeError, TypeError) as json_err:
                                 logger.warning(f"[Run {run_id}] Failed to parse response text for {miner_hotkey}: {json_err}")
                                 miner_response = {"status": "parse_error", "message": str(json_err)}
                                 
                         if (isinstance(miner_response, dict) and 
                             miner_response.get("status") == "fetch_accepted" and 
                             miner_response.get("job_id")):
                              miner_uid_result = await self.db_manager.fetch_one("SELECT uid FROM node_table WHERE hotkey = :hk", {"hk": miner_hotkey})
                              miner_uid = miner_uid_result['uid'] if miner_uid_result else -1
 
                              if miner_uid == -1:
                                  logger.warning(f"[Run {run_id}] Miner {miner_hotkey} accepted but UID not found in node_table.")
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
                              await self.db_manager.execute(insert_resp_query, {
                                   "run_id": run_id,
                                   "uid": miner_uid,
                                   "hk": miner_hotkey,
                                   "resp_time": datetime.now(timezone.utc),
                                   "status": "fetch_initiated",
                                   "job_id": miner_job_id
                              })
                              accepted_count += 1
                              logger.debug(f"[Run {run_id}] Recorded acceptance from Miner UID {miner_uid} ({miner_hotkey}). Miner Job ID: {miner_job_id}")
                         else:
                              logger.warning(f"[Run {run_id}] Miner {miner_hotkey} did not return successful 'fetch_accepted' status or job_id. Response: {miner_response}")
                     except Exception as resp_proc_err:
                          logger.error(f"[Run {run_id}] Error processing response from {miner_hotkey}: {resp_proc_err}", exc_info=True)

                logger.info(f"[Run {run_id}] Completed processing initiate fetch responses. {accepted_count} miners accepted.")
                await _update_run_status(self, run_id, "awaiting_input_hashes") 

                wait_minutes = self.config.get('validator_hash_wait_minutes', 10)
                if self.test_mode:
                    original_wait = wait_minutes
                    wait_minutes = 1
                    logger.info(f"TEST MODE: Using shortened wait time of {wait_minutes} minute(s) instead of {original_wait} minutes")
                logger.info(f"[Run {run_id}] Waiting for {wait_minutes} minutes for miners to fetch GFS and compute input hash...")
                await validator.update_task_status('weather', 'waiting', 'miner_fetch_wait')
                await asyncio.sleep(wait_minutes * 60)
                logger.info(f"[Run {run_id}] Wait finished. Proceeding with input hash verification.")
                await validator.update_task_status('weather', 'processing', 'verifying_hashes')
                await _update_run_status(self, run_id, "verifying_input_hashes")

                responses_to_check_query = """
                    SELECT id, miner_hotkey, job_id
                    FROM weather_miner_responses
                    WHERE run_id = :run_id AND status = 'fetch_initiated'
                """
                miners_to_poll = await self.db_manager.fetch_all(responses_to_check_query, {"run_id": run_id})
                logger.info(f"[Run {run_id}] Polling {len(miners_to_poll)} miners for input hash status.")

                miner_hash_results = {}
                polling_tasks = []

                async def _poll_single_miner(response_rec):
                    resp_id = response_rec['id']
                    miner_hk = response_rec['miner_hotkey']
                    miner_job_id = response_rec['job_id']
                    logger.debug(f"[Run {run_id}] Polling miner {miner_hk[:8]} (Job: {miner_job_id}) for input status using query_miners.")
                    
                    node = validator.metagraph.nodes.get(miner_hk)
                    if not node or not node.ip or not node.port:
                         logger.warning(f"[Run {run_id}] Miner {miner_hk[:8]} not found in metagraph or missing IP/Port. Cannot poll.")
                         return resp_id, {"status": "validator_poll_error", "message": "Miner not found in metagraph"}

                    try:
                        status_payload_data = WeatherGetInputStatusData(job_id=miner_job_id)
                        status_payload = {"nonce": str(uuid.uuid4()), "data": status_payload_data.model_dump()}
                        endpoint = "/weather-get-input-status"
                        
                        all_responses = await validator.query_miners(
                            payload=status_payload,
                            endpoint=endpoint
                        )
                        
                        status_response = all_responses.get(miner_hk)
                        
                        if status_response:
                            parsed_response = status_response
                            if isinstance(status_response, dict) and 'text' in status_response:
                                try:
                                    parsed_response = json.loads(status_response['text'])
                                except (json.JSONDecodeError, TypeError) as json_err:
                                    logger.warning(f"[Run {run_id}] Failed to parse status response text for {miner_hk[:8]}: {json_err}")
                                    parsed_response = {"status": "parse_error", "message": str(json_err)}
                            
                            logger.debug(f"[Run {run_id}] Received status from {miner_hk[:8]}: {parsed_response}")
                            return resp_id, parsed_response
                        else:
                             logger.warning(f"[Run {run_id}] No response received from target miner {miner_hk[:8]} via query_miners.")
                             return resp_id, {"status": "validator_poll_failed", "message": "No response from miner via query_miners"}
                             
                    except Exception as poll_err:
                        logger.error(f"[Run {run_id}] Error polling miner {miner_hk[:8]}: {poll_err}", exc_info=True)
                        return resp_id, {"status": "validator_poll_error", "message": str(poll_err)}

                for resp_rec in miners_to_poll:
                    polling_tasks.append(_poll_single_miner(resp_rec))
                
                poll_results = await asyncio.gather(*polling_tasks)

                for resp_id, status_data in poll_results:
                    miner_hash_results[resp_id] = status_data
                logger.info(f"[Run {run_id}] Collected input status from {len(miner_hash_results)}/{len(miners_to_poll)} miners.")

                validator_input_hash = None
                try:
                    logger.info(f"[Run {run_id}] Validator computing its own reference input hash...")
                    gfs_cache_dir = Path(self.config.get('gfs_analysis_cache_dir', './gfs_analysis_cache'))
                    validator_input_hash = await compute_input_data_hash(
                        t0_run_time=gfs_t0_run_time,
                        t_minus_6_run_time=gfs_t_minus_6_run_time,
                        cache_dir=gfs_cache_dir
                    )
                    if validator_input_hash:
                        logger.info(f"[Run {run_id}] Validator computed reference hash: {validator_input_hash[:10]}...")
                    else:
                        logger.error(f"[Run {run_id}] Validator failed to compute its own reference input hash. Cannot verify miners.")
                        await _update_run_status(self, run_id, "error", "Validator failed hash computation")
                        miners_to_trigger = []

                except Exception as val_hash_err:
                    logger.error(f"[Run {run_id}] Error during validator hash computation: {val_hash_err}", exc_info=True)
                    await _update_run_status(self, run_id, "error", f"Validator hash error: {val_hash_err}")
                    miners_to_trigger = []

                miners_to_trigger = []
                if validator_input_hash:
                    update_tasks = []
                    for resp_id, status_data in miner_hash_results.items():
                        miner_status = status_data.get('status')
                        miner_hash = status_data.get('input_data_hash')
                        error_msg = status_data.get('message')
                        new_db_status = None
                        hash_match = None

                        if miner_status == 'input_hashed_awaiting_validation' and miner_hash:
                            if miner_hash == validator_input_hash:
                                logger.info(f"[Run {run_id}] Hash MATCH for response ID {resp_id}!")
                                new_db_status = 'input_validation_complete'
                                hash_match = True
                                orig_rec = next((m for m in miners_to_poll if m['id'] == resp_id), None)
                                if orig_rec:
                                    miners_to_trigger.append((resp_id, orig_rec['miner_hotkey'], orig_rec['job_id']))
                                else:
                                     logger.error(f"[Run {run_id}] Could not find original record for resp_id {resp_id} to trigger inference.")
                            else:
                                logger.warning(f"[Run {run_id}] Hash MISMATCH for response ID {resp_id}. Miner: {miner_hash[:10]}... Validator: {validator_input_hash[:10]}...")
                                new_db_status = 'input_hash_mismatch'
                                hash_match = False
                        elif miner_status == 'fetch_error':
                            new_db_status = 'input_fetch_error'
                        elif miner_status in ['fetching_gfs', 'hashing_input', 'fetch_queued']:
                            logger.warning(f"[Run {run_id}] Miner for response ID {resp_id} timed out (status: {miner_status}).")
                            new_db_status = 'input_hash_timeout'
                        elif miner_status in ['validator_poll_failed', 'validator_poll_error']:
                             new_db_status = 'input_poll_error'
                        else:
                            new_db_status = 'input_fetch_error'

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
                            update_tasks.append(self.db_manager.execute(update_query, {
                                "resp_id": resp_id,
                                "status": new_db_status,
                                "m_hash": miner_hash,
                                "v_hash": validator_input_hash,
                                "match": hash_match,
                                "err": error_msg if error_msg is not None else "",
                                "now": datetime.now(timezone.utc)
                            }))
                    
                    if update_tasks:
                         await asyncio.gather(*update_tasks)
                         logger.info(f"[Run {run_id}] Updated DB for {len(update_tasks)} miner responses after hash check.")
                 
                if miners_to_trigger:
                    logger.info(f"[Run {run_id}] Triggering inference for {len(miners_to_trigger)} miners with matching input hashes.")
                    await validator.update_task_status('weather', 'processing', 'triggering_inference')
                    trigger_tasks = []

                    async def _trigger_single_miner(resp_id, miner_hk, miner_job_id):
                        logger.debug(f"[Run {run_id}] Attempting to trigger inference for {miner_hk[:8]} (Job: {miner_job_id}) using query_miners.")
                        try:
                            trigger_payload_data = WeatherStartInferenceData(job_id=miner_job_id)
                            trigger_payload = {"nonce": str(uuid.uuid4()), "data": trigger_payload_data.model_dump()}
                            endpoint="/weather-start-inference"
                            
                            all_responses = await validator.query_miners(
                                payload=trigger_payload,
                                endpoint=endpoint
                            )
                            
                            trigger_response = all_responses.get(miner_hk)
                            
                            parsed_response = trigger_response
                            if isinstance(trigger_response, dict) and 'text' in trigger_response:
                                try:
                                    parsed_response = json.loads(trigger_response['text'])
                                except (json.JSONDecodeError, TypeError) as json_err:
                                    logger.warning(f"[Run {run_id}] Failed to parse trigger response text for {miner_hk[:8]}: {json_err}")
                                    parsed_response = {"status": "parse_error", "message": str(json_err)}
                                    
                            if parsed_response and parsed_response.get('status') == 'inference_started':
                                logger.info(f"[Run {run_id}] Successfully triggered inference for {miner_hk[:8]} (Job: {miner_job_id}).")
                                return resp_id, True
                            else:
                                logger.warning(f"[Run {run_id}] Failed to trigger inference for {miner_hk[:8]} (Job: {miner_job_id}). Response: {parsed_response}")
                                return resp_id, False
                        except Exception as trigger_err:
                            logger.error(f"[Run {run_id}] Error triggering inference for {miner_hk[:8]} (Job: {miner_job_id}): {trigger_err}", exc_info=True)
                            return resp_id, False

                    for resp_id, miner_hk, miner_job_id in miners_to_trigger:
                        trigger_tasks.append(_trigger_single_miner(resp_id, miner_hk, miner_job_id))
                    
                    trigger_results = await asyncio.gather(*trigger_tasks)

                    final_update_tasks = []
                    triggered_count = 0
                    for resp_id, success in trigger_results:
                        if success:
                            triggered_count += 1
                            final_update_tasks.append(self.db_manager.execute(
                                "UPDATE weather_miner_responses SET status = 'inference_triggered' WHERE id = :id",
                                {"id": resp_id}
                            ))
                        else:
                             final_update_tasks.append(self.db_manager.execute(
                                "UPDATE weather_miner_responses SET status = 'inference_trigger_failed', error_message = COALESCE(error_message || '; ', '') || 'Failed to trigger inference' WHERE id = :id",
                                {"id": resp_id}
                            ))
                    
                    if final_update_tasks:
                         await asyncio.gather(*final_update_tasks)
                    logger.info(f"[Run {run_id}] Completed inference trigger process. Successfully triggered {triggered_count}/{len(miners_to_trigger)} miners.")
                    if triggered_count > 0:
                        await _update_run_status(self, run_id, "awaiting_inference_results")
                    else:
                         await _update_run_status(self, run_id, "inference_trigger_failed") # No miners successfully triggered
                else:
                     logger.warning(f"[Run {run_id}] No miners eligible for inference trigger after hash verification.")
                     await _update_run_status(self, run_id, "no_matching_hashes")

                logger.info(f"[ValidatorExecute] Concluded processing for run {run_id}. Triggering validator_score.")
                await self.validator_score()

                if self.test_mode:
                    logger.info(f"TEST MODE: validator_execute iteration for run {run_id} complete.")
                    if hasattr(self, 'last_test_mode_run_id') and self.last_test_mode_run_id == run_id: 
                        logger.info(f"TEST MODE: Waiting for Day-1 scoring of run {run_id} to complete...")
                        try:
                            await asyncio.wait_for(self.test_mode_run_scored_event.wait(), timeout=600.0) # 10 min timeout
                            logger.info(f"TEST MODE: Day-1 scoring for run {run_id} event received.")
                        except asyncio.TimeoutError:
                            logger.error(f"TEST MODE: Timeout waiting for Day-1 scoring of run {run_id}.")
                        self.test_mode_run_scored_event.clear() 
                    else:
                        logger.info(f"TEST MODE: Not waiting for scoring event as run_id ({run_id}) doesn't match last_test_mode_run_id ({getattr(self, 'last_test_mode_run_id', None)}).")
                    logger.info("TEST MODE: Exiting validator loop after one successful attempt or error within the attempt.")
                    break

            except Exception as loop_err:
                 logger.error(f"Error in validator_execute main loop: {loop_err}", exc_info=True)
                 await validator.update_task_status('weather', 'error')
                 if 'run_id' in locals() and run_id is not None:
                      try: await _update_run_status(self, run_id, "error", error_message=f"Unhandled loop error: {loop_err}")
                      except: pass
                 
                 # In test mode, log the error but continue to try the full pipeline
                 if self.test_mode:
                     logger.info("TEST MODE: Encountered an error but continuing to try and complete the full pipeline.")
                 
                 await asyncio.sleep(600) # Sleep only if not in test mode

    async def validator_score(self, result=None):
        """
        Initiates the verification process for completed miner responses.
        Actual scoring happens in background workers.
        """
        logger.info("Validator scoring check initiated...")
        
        verification_wait_minutes_actual = self.config.get('verification_wait_minutes', 30)
        if self.test_mode:
            logger.info("[validator_score] TEST MODE: Setting verification_wait_minutes to 0 for immediate processing.")
            verification_wait_minutes_actual = 0
            logger.info("[validator_score] TEST MODE: Adding a 30-second delay before processing runs for miner data preparation.")
            await asyncio.sleep(30)

        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=verification_wait_minutes_actual)
        
        query = """
        SELECT id, gfs_init_time_utc 
        FROM weather_forecast_runs
        WHERE status = 'awaiting_inference_results' 
        AND run_initiation_time < :cutoff_time 
        ORDER BY run_initiation_time ASC
        LIMIT 10
        """
        forecast_runs = await self.db_manager.fetch_all(query, {"cutoff_time": cutoff_time})
        
        if not forecast_runs:
            logger.debug(f"No runs found awaiting inference results within cutoff (test_mode active: {self.test_mode}, cutoff: {cutoff_time}).")
            return
        
        for run_record in forecast_runs:
            run_id = run_record['id']
            logger.info(f"[Run {run_id}] Checking responses for verification...")
            current_run_status_rec = await self.db_manager.fetch_one("SELECT status FROM weather_forecast_runs WHERE id = :run_id", {"run_id": run_id})
            current_run_status = current_run_status_rec['status'] if current_run_status_rec else 'unknown'
            
            if current_run_status == 'awaiting_inference_results':
                await _update_run_status(self, run_id, "verifying_miner_forecasts") 
            else:
                 logger.info(f"[Run {run_id}] Status is already '{current_run_status}' (expected 'awaiting_inference_results'), skipping verification trigger step.")
                 continue
            
            responses_query = """
            SELECT mr.id, mr.miner_hotkey, mr.status, mr.job_id
            FROM weather_miner_responses mr
            WHERE mr.run_id = :run_id
            AND mr.status = 'inference_triggered'
            """
            miner_responses = await self.db_manager.fetch_all(responses_query, {"run_id": run_id})
            
            num_attempted_verification = len(miner_responses)
            if not miner_responses:
                logger.info(f"[Run {run_id}] No miner responses found with status 'inference_triggered'.")
            else:
                logger.info(f"[Run {run_id}] Found {num_attempted_verification} 'inference_triggered' responses to verify.")

            verification_tasks = []
            for response in miner_responses:
                 verification_tasks.append(verify_miner_response(self, run_record, response))
                 
            if verification_tasks:
                 await asyncio.gather(*verification_tasks)
                 logger.info(f"[Run {run_id}] Completed verification attempts for {len(verification_tasks)} responses.")
                 
            verified_responses_query = "SELECT COUNT(*) as count FROM weather_miner_responses WHERE run_id = :run_id AND verification_passed = TRUE"
            verified_count_result = await self.db_manager.fetch_one(verified_responses_query, {"run_id": run_id})
            verified_count = verified_count_result["count"] if verified_count_result else 0
            
            current_run_status_rec_after_verify = await self.db_manager.fetch_one("SELECT status FROM weather_forecast_runs WHERE id = :run_id", {"run_id": run_id})
            current_run_status_after_verify = current_run_status_rec_after_verify['status'] if current_run_status_rec_after_verify else 'unknown'
            
            if current_run_status_after_verify == 'verifying_miner_forecasts':
                if verified_count >= 1:
                    logger.info(f"[Run {run_id}] {verified_count} verified response(s). Triggering Day-1 QC scoring.")
                    await _trigger_initial_scoring(self, run_id)
                elif num_attempted_verification > 0: 
                     if verified_count == 0:
                         logger.warning(f"[Run {run_id}] No responses passed verification out of {num_attempted_verification} attempted. Status: all_forecasts_failed_verification.")
                         await _update_run_status(self, run_id, "all_forecasts_failed_verification")
                else:
                    logger.warning(f"[Run {run_id}] Run was '{current_run_status_after_verify}' but no 'inference_triggered' miner responses found to verify. Setting status to 'stalled_no_valid_forecasts'.")
                    await _update_run_status(self, run_id, "stalled_no_valid_forecasts")
            else:
                 logger.info(f"[Run {run_id}] Status changed from 'verifying_miner_forecasts' to '{current_run_status_after_verify}' during verification logic. No further status update needed here.")

    async def update_combined_weather_scores(self, run_id_trigger: Optional[int] = None):
        """
        Fetches the latest Day-1 QC scores and final ERA5 scores for all miners,
        combines them with an 80/20 ERA5/Day-1 weighting, and updates the score_table
        with a 'weather_combined_score'.
        run_id_trigger is optional, for logging purposes, indicating which run triggered this update.
        """
        logger.info(f"[CombinedWeatherScore] Updating combined weather scores (triggered by run {run_id_trigger if run_id_trigger else 'periodic/manual call'}).")

        latest_day1_scores_array = np.full(256, 0.0) 
        latest_era5_scores_array = np.full(256, 0.0) 
        
        day1_qc_task_name = "weather_day1_qc"
        query_day1 = """
            SELECT score, created_at, task_id 
            FROM score_table
            WHERE task_name = :task_name
            ORDER BY created_at DESC
            LIMIT 1
        """
        timestamp_for_combined_score_row = datetime.now(timezone.utc) 
        day1_run_id_for_log = "N/A"

        day1_result = await self.db_manager.fetch_one(query_day1, {"task_name": day1_qc_task_name})

        if day1_result and day1_result['score']:
            scores_from_db = day1_result['score']
            day1_run_id_for_log = day1_result.get('task_id', "N/A") 
            if len(scores_from_db) == 256:
                latest_day1_scores_array = np.array([
                    float(s) if s is not None and not (isinstance(s, float) and np.isnan(s)) else 0.0 
                    for s in scores_from_db
                ], dtype=float)
                logger.info(f"[CombinedWeatherScore] Fetched latest Day-1 QC scores (from run_id/task_id {day1_run_id_for_log} created at {day1_result['created_at']}). Example UID 0: {latest_day1_scores_array[0] if len(latest_day1_scores_array)>0 else 'N/A'}")
                timestamp_for_combined_score_row = day1_result['created_at'] 
            else:
                logger.warning(f"[CombinedWeatherScore] Day-1 QC score array from DB (run_id/task_id {day1_run_id_for_log}) has incorrect length: {len(scores_from_db)}. Using zeros.")
        else:
            logger.warning("[CombinedWeatherScore] No Day-1 QC scores found in score_table. Using zeros for Day-1 component.")

        active_miners_query = "SELECT DISTINCT uid FROM node_table WHERE hotkey IS NOT NULL AND uid IS NOT NULL AND uid >= 0 AND uid < 256"
        active_miner_uids_records = await self.db_manager.fetch_all(active_miners_query)
        
        era5_score_type = "era5_final_composite_score"
        query_era5_latest = """
            SELECT score FROM weather_miner_scores
            WHERE miner_uid = :miner_uid AND score_type = :score_type
            ORDER BY calculation_time DESC
            LIMIT 1
        """
        found_era5_scores_count = 0
        for miner_rec in active_miner_uids_records:
            uid = miner_rec['uid']
            if 0 <= uid < 256: 
                era5_score_result = await self.db_manager.fetch_one(query_era5_latest, {"miner_uid": uid, "score_type": era5_score_type})
                if era5_score_result and era5_score_result['score'] is not None and np.isfinite(era5_score_result['score']):
                    latest_era5_scores_array[uid] = float(era5_score_result['score'])
                    found_era5_scores_count += 1
        
        logger.info(f"[CombinedWeatherScore] Fetched latest ERA5 composite scores for {found_era5_scores_count} UIDs. Example UID 0: {latest_era5_scores_array[0] if len(latest_era5_scores_array)>0 else 'N/A'}")

        W_day1 = 0.20
        W_era5 = 0.80
        combined_scores_array = (latest_day1_scores_array * W_day1) + (latest_era5_scores_array * W_era5)
        logger.info(f"[CombinedWeatherScore] Calculated combined scores. Example UID 0: {combined_scores_array[0] if len(combined_scores_array)>0 else 'N/A'}")

        mock_evaluation_results = []
        for uid_idx in range(256):
            mock_evaluation_results.append({
                "miner_uid": uid_idx,
                "final_score_for_uid": combined_scores_array[uid_idx]
            })
        
        id_for_combined_score_table_entry = 0 
        if run_id_trigger is not None:
            id_for_combined_score_table_entry = run_id_trigger
        elif day1_run_id_for_log != "N/A":
            try:
                id_for_combined_score_table_entry = int(day1_run_id_for_log)
            except ValueError:
                logger.warning(f"[CombinedWeatherScore] Could not parse day1_run_id_for_log ('{day1_run_id_for_log}') as int. Using 0 for combined score's task_id.")
        
        await self.build_score_row(
            run_id = id_for_combined_score_table_entry, 
            gfs_init_time = timestamp_for_combined_score_row,
            evaluation_results = mock_evaluation_results,
            task_name_prefix = "weather_combined_score"
        )
        logger.info(f"[CombinedWeatherScore] update_combined_weather_scores completed and called build_score_row for 'weather_combined_score' with task_id {id_for_combined_score_table_entry}.")

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
            logger.debug(f"prepare_miner_batch_from_payload returned: {type(result_batch)}")
            return result_batch
        except Exception as e:
             logger.error(f"Error calling prepare_miner_batch_from_payload: {e}", exc_info=True)
             return None

    async def miner_execute(self, data: Dict[str, Any], miner) -> Optional[Dict[str, Any]]:
        """
        Handles the initial request from the validator, preprocesses data,
        creates a job record, launches the background inference task,
        and returns an immediate 'Accepted' response.
        Checks for existing jobs for the same forecast time to avoid redundant runs.
        """
        logger.info("Miner execute called for WeatherTask")
        new_job_id = str(uuid.uuid4())

        if self.inference_runner is None:
             logger.error(f"[New Job Attempt {new_job_id}] Cannot execute: Inference Runner not available.")
             return {"status": "error", "message": "Miner inference component not ready"}
        if not data or 'data' not in data:
             logger.error(f"[New Job Attempt {new_job_id}] Invalid or missing payload data.")
             return {"status": "error", "message": "Invalid payload structure"}

        validator_hotkey = data.get("sender_hotkey", "unknown")
        payload_data = data['data']

        try:
            gfs_init_time = payload_data.get('forecast_start_time')
            if not isinstance(gfs_init_time, datetime):
                 try:
                     gfs_init_time_str = str(gfs_init_time)
                     if gfs_init_time_str.endswith('Z'):
                         gfs_init_time_str = gfs_init_time_str[:-1] + '+00:00'
                     gfs_init_time = datetime.fromisoformat(gfs_init_time_str)
                     if gfs_init_time.tzinfo is None:
                         gfs_init_time = gfs_init_time.replace(tzinfo=timezone.utc)
                     else:
                         gfs_init_time = gfs_init_time.astimezone(timezone.utc)

                 except (ValueError, TypeError) as parse_err:
                     logger.error(f"[New Job Attempt {new_job_id}] Invalid forecast_start_time format: {gfs_init_time}. Error: {parse_err}")
                     return {"status": "error", "message": f"Invalid forecast_start_time format: {parse_err}"}

            logger.info(f"Processing request for GFS init time: {gfs_init_time}")

            existing_job = await self.get_job_by_gfs_init_time(gfs_init_time)

            if existing_job:
                existing_job_id = existing_job['id']
                existing_status = existing_job['status']
                logger.info(f"[Job {existing_job_id}] Found existing {existing_status} job for GFS init time {gfs_init_time}. Reusing this job ID.")
                return {"status": "accepted", "job_id": existing_job_id, "message": f"Accepted. Reusing existing {existing_status} job."}

            job_id = new_job_id
            logger.info(f"[Job {job_id}] No suitable existing job found. Creating new job for GFS init time {gfs_init_time}.")

            logger.info(f"[Job {job_id}] Starting preprocessing...")
            preprocessing_start_time = time.time()
            initial_batch = await self.miner_preprocess(data=payload_data)
            if initial_batch is None:
                logger.error(f"[Job {job_id}] Preprocessing failed.")
                return {"status": "error", "message": "Failed to preprocess input data"}
            logger.info(f"[Job {job_id}] Preprocessing completed in {time.time() - preprocessing_start_time:.2f} seconds.")

            logger.info(f"[Job {job_id}] Creating initial job record in database.")
            insert_query = """
                INSERT INTO weather_miner_jobs (id, validator_request_time, validator_hotkey, gfs_init_time_utc, gfs_input_metadata, status, processing_start_time)
                VALUES (:id, :req_time, :val_hk, :gfs_init, :gfs_meta, :status, :proc_start)
            """
            if gfs_init_time.tzinfo is None:
                 gfs_init_time = gfs_init_time.replace(tzinfo=timezone.utc)

            await self.db_manager.execute(insert_query, {
                "id": job_id,
                "req_time": datetime.now(timezone.utc),
                "val_hk": validator_hotkey,
                "gfs_init": gfs_init_time,
                "gfs_meta": json.dumps(payload_data, default=str),
                "status": "received",
                "proc_start": datetime.now(timezone.utc)
            })
            logger.info(f"[Job {job_id}] Initial job record created.")

            logger.info(f"[Job {job_id}] Launching background inference task...")

            return {"status": "accepted", "job_id": job_id, "message": "Weather forecast job accepted for processing."}

        except Exception as e:
            job_id_for_error = new_job_id
            logger.error(f"[Job {job_id_for_error}] Error during initial miner_execute: {e}", exc_info=True)
            return {"status": "error", "job_id": None, "message": f"Failed to initiate job: {e}"}

    async def handle_kerchunk_request(self, job_id: str) -> Dict[str, Any]:
        """
        Handle a request for forecast data for a specific job.
        Now returns information about the Zarr store directly instead of Kerchunk JSON.
        
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
            query = """
            SELECT id as job_id, status, target_netcdf_path, verification_hash, error_message
            FROM weather_miner_jobs
            WHERE id = :job_id 
            """
            job = await self.db_manager.fetch_one(query, {"job_id": job_id})
            
            if not job:
                logger.warning(f"Job not found for job_id: {job_id}")
                return {"status": "not_found", "message": f"Job with ID {job_id} not found"}
                
            if job["status"] == "completed":
                zarr_path_str = job["target_netcdf_path"]
                verification_hash = job["verification_hash"]
                
                if not zarr_path_str or not verification_hash:
                    logger.error(f"Job {job_id} completed but missing required path/hash info. Zarr path: {zarr_path_str}, Hash: {verification_hash}")
                    return {"status": "error", "message": "Job completed but data paths or hash missing"}
                
                zarr_path = Path(zarr_path_str)
                zarr_dir_name = zarr_path.name
                
                if not zarr_dir_name.endswith(".zarr"):
                    logger.error(f"Expected Zarr directory but found: {zarr_dir_name}")
                    return {"status": "error", "message": "Invalid Zarr store format"}

                logger.info(f"[Job {job_id}] Using Zarr directory for JWT claim: {zarr_dir_name}")

                miner_jwt_secret_key = self.config.get('miner_jwt_secret_key', os.getenv("MINER_JWT_SECRET_KEY"))
                if not miner_jwt_secret_key:
                    logger.warning("MINER_JWT_SECRET_KEY not set in config or environment. Using default insecure key.")
                    miner_jwt_secret_key = "insecure_default_key_for_development_only"
                jwt_algorithm = self.config.get('jwt_algorithm', "HS256")
                token_expire_minutes = int(self.config.get('access_token_expire_minutes', 60))
                
                token_data = {
                    "job_id": job_id,
                    "file_path": zarr_dir_name,
                    "exp": datetime.now(timezone.utc) + timedelta(minutes=token_expire_minutes)
                }
                
                access_token = jwt.encode(
                    token_data,
                    miner_jwt_secret_key, 
                    algorithm=jwt_algorithm
                )
                
                zarr_url_for_response = f"/forecasts/{zarr_dir_name}"
                
                return {
                    "status": "completed",
                    "message": "Forecast completed and ready for access",
                    "zarr_store_url": zarr_url_for_response,
                    "verification_hash": verification_hash,
                    "access_token": access_token
                }
                
            elif job["status"] == "error":
                return {"status": "error", "message": f"Job failed: {job['error_message'] or 'Unknown error'}"}
            else:
                return {"status": "processing", "message": f"Job is currently in status: {job['status']}"}
                
        except Exception as e:
            logger.error(f"Error handling forecast data request for job_id {job_id}: {e}", exc_info=True)
            return {"status": "error", "message": f"Failed to process request: {str(e)}"}

    async def handle_initiate_fetch(self, request_data: 'WeatherInitiateFetchData') -> Dict[str, Any]:
        """
        Handles the /weather-initiate-fetch request.
        Creates a job record and launches the background task for fetching GFS and hashing.
        (Called by the API handler in subnet.py)
        """
        if self.node_type != 'miner':
            logger.error("handle_initiate_fetch called on non-miner node.")
            return {"status": "error", "job_id": None, "message": "Invalid node type"}

        job_id = str(uuid.uuid4())
        logger.info(f"[Miner Job {job_id}] Received initiate_fetch request for T0={request_data.forecast_start_time}")

        try:
            t0_run_time = request_data.forecast_start_time
            t_minus_6_run_time = request_data.previous_step_time

            if t0_run_time.tzinfo is None: t0_run_time = t0_run_time.replace(tzinfo=timezone.utc)
            if t_minus_6_run_time.tzinfo is None: t_minus_6_run_time = t_minus_6_run_time.replace(tzinfo=timezone.utc)

            insert_query = """
                INSERT INTO weather_miner_jobs
                (id, validator_request_time, gfs_init_time_utc, gfs_t_minus_6_time_utc, status)
                VALUES (:id, :req_time, :gfs_init, :gfs_t_minus_6, :status)
            """
            await self.db_manager.execute(insert_query, {
                "id": job_id,
                "req_time": datetime.now(timezone.utc),
                "gfs_init": t0_run_time,          # T=0h time
                "gfs_t_minus_6": t_minus_6_run_time, # T=-6h time
                "status": "fetch_queued"
            })
            logger.info(f"[Miner Job {job_id}] DB record created (status: fetch_queued). Launching background fetch/hash task.")

            asyncio.create_task(fetch_and_hash_gfs_task(
                task_instance=self,
                job_id=job_id,
                t0_run_time=t0_run_time,
                t_minus_6_run_time=t_minus_6_run_time
            ))

            return {"status": "fetch_accepted", "job_id": job_id, "message": "Fetch and hash process initiated."}

        except Exception as e:
            logger.error(f"[Miner Job {job_id}] Error during handle_initiate_fetch: {e}", exc_info=True)
            try:
                 await update_job_status(self, job_id, "error", f"Initiation failed: {e}")
            except: pass
            return {"status": "error", "job_id": job_id, "message": f"Failed to initiate fetch: {e}"}

    async def handle_get_input_status(self, job_id: str) -> Dict[str, Any]:
        """
        Handles the /weather-get-input-status request.
        Returns the current status and input hash (if computed) for the job.
        (Called by the API handler in subnet.py)
        """
        if self.node_type != 'miner':
            logger.error("handle_get_input_status called on non-miner node.")
            return {"status": "error", "job_id": job_id, "message": "Invalid node type"}

        logger.debug(f"[Miner Job {job_id}] Received get_input_status request.")
        try:
            query = "SELECT status, input_data_hash, error_message FROM weather_miner_jobs WHERE id = :job_id"
            result = await self.db_manager.fetch_one(query, {"job_id": job_id})

            if not result:
                logger.warning(f"[Miner Job {job_id}] Status requested for non-existent job.")
                return {"status": "not_found", "job_id": job_id, "message": "Job ID not found."}

            response = {
                "job_id": job_id,
                "status": result['status'],
                "input_data_hash": result.get('input_data_hash'), # Will be None if not computed yet
                "message": result.get('error_message')
            }
            logger.debug(f"[Miner Job {job_id}] Returning status: {response['status']}, Hash available: {response['input_data_hash'] is not None}")
            return response

        except Exception as e:
            logger.error(f"[Miner Job {job_id}] Error during handle_get_input_status: {e}", exc_info=True)
            return {"status": "error", "job_id": job_id, "message": f"Failed to get status: {e}"}

    async def handle_start_inference(self, job_id: str) -> Dict[str, Any]:
        """
        Handles the /weather-start-inference request.
        Verifies the job is ready and triggers the main inference background task.
        (Called by the API handler in subnet.py)
        """
        if self.node_type != 'miner':
            logger.error("handle_start_inference called on non-miner node.")
            return {"status": "error", "message": "Invalid node type"}

        logger.info(f"[Miner Job {job_id}] Received start_inference request...") 
        try:
            query = "SELECT status FROM weather_miner_jobs WHERE id = :job_id"
            job_details = await self.db_manager.fetch_one(query, {"job_id": job_id})

            if not job_details:
                logger.warning(f"[Miner Job {job_id}] Start inference requested for non-existent job.")
                return {"status": "error", "message": "Job ID not found."}

            if job_details['status'] != 'input_hashed_awaiting_validation':
                logger.warning(f"[Miner Job {job_id}] Start inference requested but job status is '{job_details['status']}' (expected 'input_hashed_awaiting_validation').")
                return {"status": "error", "message": f"Cannot start inference, job status is {job_details['status']}"}

            await update_job_status(self, job_id, "inference_queued")
            logger.info(f"[Miner Job {job_id}] Status updated to inference_queued. Launching background inference task.")

            asyncio.create_task(run_inference_background(
                 task_instance=self,
                 job_id=job_id,
            ))

            return {"status": "inference_started", "message": "Inference process initiated."}

        except Exception as e:
            logger.error(f"[Miner Job {job_id}] Error during handle_start_inference: {e}", exc_info=True)
            try:
                await update_job_status(self, job_id, "error", f"Inference start failed: {e}")
            except: pass
            return {"status": "error", "message": f"Failed to start inference: {e}"}

    ############################################################
    # Helper Methods
    ############################################################

    async def cleanup_resources(self):
        """
        Clean up resources like temporary files or reset database statuses
        in case of errors or shutdowns.
        """
        logger.info("Cleaning up weather task resources...")

        logger.info("Weather task cleanup completed")
       
    async def start_initial_scoring_workers(self, num_workers=1):
        """Start background workers for initial scoring processing."""
        if self.initial_scoring_worker_running:
            logger.info("Initial scoring workers already running")
            return
            
        self.initial_scoring_worker_running = True
        for _ in range(num_workers):
            worker = asyncio.create_task(initial_scoring_worker(self))
            self.initial_scoring_workers.append(worker)
        logger.info(f"Started {num_workers} initial scoring workers")
        
    async def stop_initial_scoring_workers(self):
        """Stop all background initial scoring workers."""
        if not self.initial_scoring_worker_running:
            return
            
        self.initial_scoring_worker_running = False
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
        for _ in range(num_workers):
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
        
    async def start_background_workers(self, num_ensemble_workers=1, num_initial_scoring_workers=1, num_final_scoring_workers=1, num_cleanup_workers=1):
         """Starts all background worker types."""
         await self.start_initial_scoring_workers(num_initial_scoring_workers)
         await self.start_final_scoring_workers(num_final_scoring_workers)
         await self.start_cleanup_workers(num_cleanup_workers)
         
    async def stop_background_workers(self):
        """Stops all background worker types."""

        try: await self.stop_initial_scoring_workers()
        except Exception as e: logger.error(f"Error stopping initial scoring workers: {e}")
        try: await self.stop_final_scoring_workers()
        except Exception as e: logger.error(f"Error stopping final scoring workers: {e}")
        try: await self.stop_cleanup_workers()
        except Exception as e: logger.error(f"Error stopping cleanup workers: {e}")
            
    async def miner_fetch_hash_worker(self):
        """Worker that periodically checks for jobs awaiting input hash verification."""
        CHECK_INTERVAL_SECONDS = 10 if self.test_mode else 60
        
        while getattr(self, 'miner_fetch_hash_worker_running', False):
            try:
                logger.info("Miner fetch hash worker checking for runs awaiting hash verification...")
                
                query = """
                SELECT id, gfs_init_time_utc
                FROM weather_forecast_runs 
                WHERE status = 'awaiting_input_hashes'
                ORDER BY run_initiation_time ASC
                LIMIT 5
                """

                runs = await self.db_manager.fetch_all(query)
                if not runs:
                    logger.debug(f"No runs awaiting hash verification. Sleeping for {CHECK_INTERVAL_SECONDS}s...")
                    await asyncio.sleep(CHECK_INTERVAL_SECONDS)
                    continue
                    
                for run in runs:
                    run_id = run['id']
                    gfs_init_time = run['gfs_init_time_utc']
                    current_time = datetime.now(timezone.utc)
                    elapsed_minutes = (current_time - gfs_init_time).total_seconds() / 60
                    
                    min_wait_minutes = getattr(self.config, 'verification_wait_minutes', 30)
                    
                    if elapsed_minutes < min_wait_minutes and not self.test_mode:
                        logger.info(f"[HashWorker] [Run {run_id}] Only {elapsed_minutes:.1f} minutes elapsed, "
                                    f"waiting for minimum {min_wait_minutes} minutes")
                        continue
                    elif self.test_mode and elapsed_minutes < 0.1:
                        logger.info(f"[HashWorker] [Run {run_id}] TEST MODE: Minimal wait of 0.1 minutes")
                        await asyncio.sleep(6)
                    
                    responses_query = """
                    SELECT id, miner_hotkey, job_id
                    FROM weather_miner_responses
                    WHERE run_id = :run_id
                    AND status = 'fetch_initiated'
                    """
                    miners_to_poll = await self.db_manager.fetch_all(responses_query, {"run_id": run_id})
                    logger.info(f"[HashWorker] [Run {run_id}] Polling {len(miners_to_poll)} miners for input hash status.")

                    for resp_rec in miners_to_poll:
                        resp_id = resp_rec['id']
                        miner_hk = resp_rec['miner_hotkey']
                        miner_job_id = resp_rec['job_id']
                        logger.debug(f"[HashWorker] [Run {run_id}] Querying miner {miner_hk[:8]} (Job: {miner_job_id}) for input status.")
                        try:
                            status_payload_data = WeatherGetInputStatusData(job_id=miner_job_id)
                            status_payload = {"nonce": str(uuid.uuid4()), "data": status_payload_data.model_dump()}
                            
                            responses_dict = await self.query_miners(
                                payload=status_payload,
                                endpoint="/weather-get-input-status",
                                hotkeys=[miner_hk] 
                            )
                            
                            status_response = responses_dict.get(miner_hk)

                            if status_response:
                                parsed_response = status_response
                                if isinstance(status_response, dict) and 'text' in status_response:
                                    try:
                                        parsed_response = json.loads(status_response['text'])
                                    except (json.JSONDecodeError, TypeError) as json_err:
                                        logger.warning(f"[Run {run_id}] Failed to parse status response text for {miner_hk[:8]}: {json_err}")
                                        parsed_response = {"status": "parse_error", "message": str(json_err)}
                                
                                logger.debug(f"[Run {run_id}] Received status from {miner_hk[:8]}: {parsed_response}")
                                return resp_id, parsed_response
                            else:
                                logger.warning(f"[Run {run_id}] No valid response received from {miner_hk[:8]} using query_miners.")
                                return resp_id, {"status": "validator_poll_failed", "message": "No response from miner via query_miners"}
                        except Exception as poll_err:
                            logger.error(f"[Run {run_id}] Error polling miner {miner_hk[:8]}: {poll_err}", exc_info=True)
                            return resp_id, {"status": "validator_poll_error", "message": str(poll_err)}

            except Exception as e:
                logger.error(f"Error in miner_fetch_hash_worker: {e}", exc_info=True)
                await asyncio.sleep(60)
            