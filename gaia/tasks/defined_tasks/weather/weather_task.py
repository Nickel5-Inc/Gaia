import asyncio
import traceback
from typing import Any, Dict, List, Optional, Union, Tuple
from uuid import uuid4
from datetime import datetime, timezone, timedelta
import os
import importlib.util
import json
from pydantic import Field
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
import fsspec
from kerchunk.hdf import SingleHdf5ToZarr
import pandas as pd

from .utils.era5_api import fetch_era5_data
from .utils.gfs_api import fetch_gfs_analysis_data, fetch_gfs_data
from .utils.hashing import compute_verification_hash, verify_forecast_hash
from .utils.kerchunk_utils import generate_kerchunk_json_from_local_file
from .utils.data_prep import create_aurora_batch_from_gfs
from .schemas.weather_metadata import WeatherMetadata
from .schemas.weather_inputs import WeatherInputs, WeatherForecastRequest, WeatherInputData
from .schemas.weather_outputs import WeatherOutputs, WeatherKerchunkResponseData
from .scoring.ensemble import create_weighted_ensemble, create_physics_aware_ensemble, ALL_EXPECTED_VARIABLES, _open_dataset_lazily
from .scoring.metrics import calculate_rmse
from .processing.weather_miner_preprocessing import WeatherMinerPreprocessing, prepare_miner_batch_from_payload
from .processing.weather_validator_preprocessing import WeatherValidatorPreprocessing
from .processing.weather_logic import (
    _update_run_status, build_score_row, get_ground_truth_data,
    _trigger_initial_scoring, _request_fresh_token,
    get_job_by_gfs_init_time, update_job_status, update_job_paths
)
from .processing.weather_workers import (
    initial_scoring_worker, 
    ensemble_worker, 
    finalize_scores_worker, 
    run_inference_background
)

from gaia.models.weather_basemodel import WeatherBaseModel
from gaia.tasks.defined_tasks.weather.weather_inference_runner import WeatherInferenceRunner

try:
    from aurora.core import Batch
    _AURORA_AVAILABLE = True
except ImportError:
    logger.warning("Aurora library not found. Batch type hinting and related functionality may fail.")
    Batch = Any
    _AURORA_AVAILABLE = False

logger = get_logger(__name__)

DEFAULT_FORECAST_DIR_BG = Path("./miner_forecasts/")
MINER_FORECAST_DIR_BG = Path(os.getenv("MINER_FORECAST_DIR", DEFAULT_FORECAST_DIR_BG))
MINER_FORECAST_DIR_BG.mkdir(parents=True, exist_ok=True)

# JWT Configuration
MINER_JWT_SECRET_KEY = os.getenv("MINER_JWT_SECRET_KEY")
if not MINER_JWT_SECRET_KEY:
    logger.warning("MINER_JWT_SECRET_KEY not set in environment. Using default insecure key.")
    MINER_JWT_SECRET_KEY = "insecure_default_key_for_development_only"
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 15 # Token validity duration

class WeatherTask(Task):
    """
    Task for weather forecasting using GFS inputs and generating NetCDF/Kerchunk outputs.
    Handles validator orchestration (requesting forecasts, verifying, scoring)
    and miner execution (running forecast model, providing data access).
    """

    db_manager: Union[ValidatorDatabaseManager, MinerDatabaseManager] = Field(
        ...,
        description="Database manager for the task",
    )
    model: Optional[WeatherBaseModel] = Field(
        default=None, description="The weather prediction model"
    )
    node_type: str = Field(
        default="validator",
        description="Type of node running the task (validator or miner)"
    )
    test_mode: bool = Field(
        default=False,
        description="Whether to run in test mode (e.g., different timing, sample data)"
    )
    validator: Optional[Any] = Field(
        default=None,
        description="Reference to the validator instance, set during execution"
    )
    gpu_semaphore: Optional[asyncio.Semaphore] = Field(
        default=None,
        description="Semaphore to limit concurrent GPU-intensive inference tasks."
    )
    inference_runner: Optional[WeatherInferenceRunner] = Field(
        default=None,
        description="Instance of the inference runner."
    )

    def __init__(self, db_manager=None, node_type=None, test_mode=False, **data):
        super().__init__(**data)
        self.db_manager = db_manager
        self.node_type = node_type
        self.test_mode = test_mode
        self.ensemble_task_queue = asyncio.Queue()
        self.ensemble_worker_running = False
        self.ensemble_workers = []
        self.initial_scoring_queue = asyncio.Queue()
        self.initial_scoring_worker_running = False
        self.initial_scoring_workers = []
        self.final_scoring_queue = asyncio.Queue()
        self.final_scoring_worker_running = False
        self.final_scoring_workers = []
        self.config = self._load_config()
        self.gpu_semaphore = asyncio.Semaphore(getattr(self.config, 'max_concurrent_inferences', 1))
        self.inference_runner = WeatherInferenceRunner(self.config)

        if self.node_type == "validator":
            logger.info("Initialized validator components for WeatherTask")
        else:
            logger.info("Initialized miner components for WeatherTask")
            if self.inference_runner is None:
                 logger.warning("WeatherInferenceRunner could not be initialized. Miner will likely fail.")


    ############################################################
    # Validator methods
    ############################################################

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
        
        # Start ensemble workers
        await self.start_ensemble_workers()
        logger.info("Started ensemble workers for asynchronous processing")

        RUN_HOUR_UTC = 12
        RUN_MINUTE_UTC = 0
        if self.test_mode:
             logger.warning("Running in TEST MODE: Execution will run once immediately.")

        while True:
            try:
                await validator.update_task_status('weather', 'active', 'waiting')
                now_utc = datetime.now(timezone.utc)

                if not self.test_mode:
                    target_run_time_today = now_utc.replace(hour=RUN_HOUR_UTC, minute=RUN_MINUTE_UTC, second=0, microsecond=0)
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
                    run_record = await self.db_manager.execute(run_insert_query, {
                         "init_time": now_utc,
                         "target_time": effective_forecast_start_time,
                         "gfs_init": effective_forecast_start_time,
                         "status": "fetching_gfs"
                    }, fetch_one=True)

                    if run_record and 'id' in run_record:
                        run_id = run_record['id']
                        logger.info(f"Created weather_forecast_runs record with ID: {run_id}")
                    else:
                         logger.warning("Could not retrieve run_id via RETURNING. Attempting fallback query.")
                         fallback_query = """
                              SELECT id FROM weather_forecast_runs
                              WHERE gfs_init_time_utc = :gfs_init ORDER BY run_initiation_time DESC LIMIT 1
                         """
                         fallback_record = await self.db_manager.fetch_one(fallback_query, {"gfs_init": effective_forecast_start_time})
                         if fallback_record: run_id = fallback_record['id']

                    if run_id is None:
                        raise RuntimeError("Failed to create or retrieve run_id for forecast run.")

                except Exception as db_err:
                     logger.error(f"Failed to create forecast run record in DB: {db_err}", exc_info=True)
                     await asyncio.sleep(60)
                     continue 

                await validator.update_task_status('weather', 'processing', 'fetching_gfs')
                logger.info(f"[Run {run_id}] Fetching GFS analysis data...")
                ds_t0 = None
                ds_t_minus_6 = None
                try:
                     logger.info(f"[Run {run_id}] Fetching T=0h data from GFS run: {gfs_t0_run_time}")
                     ds_t0 = await fetch_gfs_data(run_time=gfs_t0_run_time, lead_hours=[0])

                     logger.info(f"[Run {run_id}] Fetching T=-6h data from GFS run: {gfs_t_minus_6_run_time}")
                     ds_t_minus_6 = await fetch_gfs_data(run_time=gfs_t_minus_6_run_time, lead_hours=[0])

                     if ds_t_minus_6 is None or ds_t0 is None:
                         raise ValueError("Failed to retrieve one or both required GFS analysis datasets.")

                     logger.info(f"[Run {run_id}] Successfully fetched GFS analysis data for T=0h and T=-6h.")
                     await self._update_run_status(run_id, "serializing_gfs")

                except Exception as fetch_err:
                     logger.error(f"[Run {run_id}] Failed to fetch GFS data: {fetch_err}", exc_info=True)
                     await self._update_run_status(run_id, "error", error_message=f"GFS Fetch Failed: {fetch_err}")
                     await asyncio.sleep(60)
                     continue

                await validator.update_task_status('weather', 'processing', 'serializing_gfs')
                logger.info(f"[Run {run_id}] Serializing GFS data...")
                try:
                     # Timestep 1 for miner is T=-6h data
                     gfs_t_minus_6_serial = base64.b64encode(pickle.dumps(ds_t_minus_6)).decode('utf-8')
                     # Timestep 2 for miner is T=0h data
                     gfs_t0_serial = base64.b64encode(pickle.dumps(ds_t0)).decode('utf-8')

                     gfs_metadata = {
                         "t0_run_time": gfs_t0_run_time.isoformat(),
                         "t_minus_6_run_time": gfs_t_minus_6_run_time.isoformat(),
                         "variables_t0": list(ds_t0.data_vars.keys()),
                         "variables_t_minus_6": list(ds_t_minus_6.data_vars.keys())
                     }
                     await self._update_run_status(run_id, "querying_miners", gfs_metadata=gfs_metadata)
                     logger.info(f"[Run {run_id}] GFS data serialized.")

                except Exception as serial_err:
                     logger.error(f"[Run {run_id}] Failed to serialize GFS data: {serial_err}", exc_info=True)
                     await self._update_run_status(run_id, "error", error_message=f"GFS Serialization Failed: {serial_err}")
                     await asyncio.sleep(60)
                     continue

                await validator.update_task_status('weather', 'processing', 'querying_miners')
                payload_data = WeatherInputData(
                     forecast_start_time=effective_forecast_start_time,
                     gfs_timestep_1=gfs_t_minus_6_serial, # T-6h data (first history step)
                     gfs_timestep_2=gfs_t0_serial       # T=0h data (second history step, effective T=0)
                )
                payload = WeatherForecastRequest(
                     nonce=str(uuid.uuid4()),
                     data=payload_data
                )

                logger.info(f"[Run {run_id}] Querying miners with weather forecast request...")
                responses = await validator.query_miners(
                     payload=payload.model_dump(),
                     endpoint="/weather-forecast-request"
                )
                logger.info(f"[Run {run_id}] Received {len(responses)} initial responses from miners.")

                await validator.update_task_status('weather', 'processing', 'recording_acceptances')
                accepted_count = 0
                for miner_hotkey, response_data in responses.items():
                     try:
                         if isinstance(response_data, dict) and response_data.get("status") == "accepted":
                             miner_uid_result = await self.db_manager.fetch_one("SELECT uid FROM node_table WHERE hotkey = :hk", {"hk": miner_hotkey})
                             miner_uid = miner_uid_result['uid'] if miner_uid_result else -1

                             if miner_uid == -1:
                                  logger.warning(f"[Run {run_id}] Miner {miner_hotkey} accepted but UID not found in node_table.")
                                  continue

                             miner_job_id = response_data.get("job_id")

                             insert_resp_query = """
                                  INSERT INTO weather_miner_responses
                                  (run_id, miner_uid, miner_hotkey, response_time, status)
                                  VALUES (:run_id, :uid, :hk, :resp_time, :status)
                                  ON CONFLICT (run_id, miner_uid) DO UPDATE SET
                                  response_time = EXCLUDED.response_time, status = EXCLUDED.status
                             """
                             await self.db_manager.execute(insert_resp_query, {
                                  "run_id": run_id,
                                  "uid": miner_uid,
                                  "hk": miner_hotkey,
                                  "resp_time": datetime.now(timezone.utc),
                                  "status": "accepted"
                             })
                             accepted_count += 1
                             logger.debug(f"[Run {run_id}] Recorded acceptance from Miner UID {miner_uid} ({miner_hotkey}). Job ID: {miner_job_id}")
                         else:
                              logger.warning(f"[Run {run_id}] Miner {miner_hotkey} did not return successful acceptance status. Response: {response_data}")
                     except Exception as resp_proc_err:
                          logger.error(f"[Run {run_id}] Error processing response from {miner_hotkey}: {resp_proc_err}", exc_info=True)

                logger.info(f"[Run {run_id}] Completed processing initial responses. {accepted_count} miners accepted.")
                await self._update_run_status(run_id, "awaiting_results") # Stage 1 complete

                if self.test_mode:
                     logger.info("TEST MODE: Exiting validator loop after one run.")
                     break

            except Exception as loop_err:
                 logger.error(f"Error in validator_execute main loop: {loop_err}", exc_info=True)
                 await validator.update_task_status('weather', 'error')
                 if 'run_id' in locals() and run_id is not None:
                      try: await self._update_run_status(run_id, "error", error_message=f"Unhandled loop error: {loop_err}")
                      except: pass
                 await asyncio.sleep(300)



    async def validator_score(self, result=None):
        """
        Scores verified miner forecasts:
        1. Identify responses ready for scoring (verified, past delay if any).
        2. Use Kerchunk JSON to access specific data subsets from miners via Range Requests.
        3. Fetch corresponding ground truth/reference data (e.g., ERA5).
        4. Calculate metrics using the scoring mechanism.
        5. Compare against baseline model scores.
        6. Store scores in weather_miner_scores.
        7. Update historical weights.
        8. Build and store the global score row in score_table.
        """
        logger.info("Starting weather forecast validation and scoring...")
        
        query = """
        SELECT id, run_id, gfs_init_time_utc
        FROM weather_forecast_runs
        WHERE status = 'awaiting_results' 
        AND run_initiation_time < :cutoff_time
        ORDER BY run_initiation_time ASC
        LIMIT 10
        """
        
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=30)
        forecast_runs = await self.db_manager.fetch_all(query, {"cutoff_time": cutoff_time})
        
        if not forecast_runs:
            logger.info("No forecast runs ready for scoring.")
            return
        
        for run in forecast_runs:
            run_id = run['id']
            logger.info(f"Processing run {run_id} for scoring...")
            
            await self._update_run_status(run_id, "scoring")
            
            responses_query = """
            SELECT mr.id, mr.miner_hotkey, mr.status
            FROM weather_miner_responses mr
            WHERE mr.run_id = :run_id
            AND mr.status = 'accepted'
            """
            
            miner_responses = await self.db_manager.fetch_all(responses_query, {"run_id": run_id})
            logger.info(f"Found {len(miner_responses)} miner responses to process for run {run_id}")
            
            for response in miner_responses:
                response_id = response['id']
                miner_hotkey = response['miner_hotkey']
                
                logger.info(f"Requesting Kerchunk JSON from miner {miner_hotkey} for response {response_id}")
                
                try:
                    kerchunk_request_payload = {
                        "nonce": str(uuid.uuid4()),
                        "data": {
                            "job_id": f"forecast_{run['gfs_init_time_utc'].strftime('%Y%m%d%H')}_{miner_hotkey[:8]}"
                        }
                    }
                    
                    kerchunk_response = await self.validator.query_miner(
                        miner_hotkey=miner_hotkey,
                        payload=kerchunk_request_payload,
                        endpoint="/weather-kerchunk-request"
                    )
                    
                    if not kerchunk_response or "status" not in kerchunk_response:
                        logger.warning(f"Invalid response format from miner {miner_hotkey}")
                        continue
                    
                    if kerchunk_response["status"] == "completed":
                        kerchunk_json_url = kerchunk_response.get("kerchunk_json_url")
                        verification_hash_claimed = kerchunk_response.get("verification_hash")
                        access_token = kerchunk_response.get("access_token")
                        
                        miner_url = self.validator.get_miner_url(miner_hotkey)
                        if not miner_url:
                            logger.warning(f"Could not get URL for miner {miner_hotkey}")
                            continue
                        
                        full_kerchunk_url = f"{miner_url}{kerchunk_json_url}"
                        
                        update_query = """
                        UPDATE weather_miner_responses
                        SET kerchunk_json_url = :kerchunk_url,
                            verification_hash_claimed = :claimed_hash,
                            status = 'verifying',
                            response_time = :resp_time
                        WHERE id = :response_id
                        """
                        
                        await self.db_manager.execute(update_query, {
                            "kerchunk_url": full_kerchunk_url,
                            "claimed_hash": verification_hash_claimed,
                            "resp_time": datetime.now(timezone.utc),
                            "response_id": response_id
                        })
                        
                        logger.info(f"Verifying hash for response {response_id} from miner {miner_hotkey}")
                        
                        variables_to_check = ["2t", "10u", "10v", "msl", "z", "u", "v", "t", "q"]
                        metadata = {
                            "time": [run['gfs_init_time_utc']],
                            "source_model": "aurora",
                            "resolution": 0.25
                        }
                        
                        headers = {
                            "Authorization": f"Bearer {access_token}"
                        }
                        
                        try:
                            from .utils.hashing import verify_forecast_hash
                            
                            timesteps = list(range(40))
                            
                            verification_timeout = 1000
                            
                            verification_result = await asyncio.wait_for(
                                verify_forecast_hash(
                                    kerchunk_url=full_kerchunk_url,
                                    claimed_hash=verification_hash_claimed,
                                    metadata=metadata,
                                    variables=variables_to_check,
                                    timesteps=timesteps,
                                    headers=headers
                                ),
                                timeout=verification_timeout
                            )
                            
                            verification_update = """
                            UPDATE weather_miner_responses
                            SET verification_passed = :verified,
                                status = :new_status
                            WHERE id = :response_id
                            """
                            
                            new_status = "verified" if verification_result else "verification_failed"
                            
                            await self.db_manager.execute(verification_update, {
                                "verified": verification_result,
                                "new_status": new_status,
                                "response_id": response_id
                            })
                            
                            logger.info(f"Hash verification {'succeeded' if verification_result else 'failed'} for response {response_id}")
                            
                            if verification_result:
                                score_result = await self.scoring_mechanism.score_forecast(
                                    response_id=response_id,
                                    run_id=run_id,
                                    kerchunk_url=full_kerchunk_url,
                                    miner_hotkey=miner_hotkey,
                                    headers=headers
                                )
                                
                                if score_result:
                                    logger.info(f"Successfully scored response {response_id} from miner {miner_hotkey}")
                                else:
                                    logger.warning(f"Failed to score response {response_id} from miner {miner_hotkey}")
                            
                        except asyncio.TimeoutError:
                            logger.error(f"Verification timed out for response {response_id}")
                            await self.db_manager.execute("""
                            UPDATE weather_miner_responses
                            SET status = 'verification_timeout'
                            WHERE id = :response_id
                            """, {"response_id": response_id})
                        
                        except Exception as verify_err:
                            logger.error(f"Error during verification for response {response_id}: {verify_err}", exc_info=True)
                            await self.db_manager.execute("""
                            UPDATE weather_miner_responses
                            SET status = 'verification_error',
                                error_message = :error_msg
                            WHERE id = :response_id
                            """, {
                                "response_id": response_id,
                                "error_msg": str(verify_err)
                            })
                    
                    elif kerchunk_response["status"] == "processing":
                        await self.db_manager.execute("""
                        UPDATE weather_miner_responses
                        SET status = 'awaiting_completion'
                        WHERE id = :response_id
                        """, {"response_id": response_id})
                        
                        logger.info(f"Miner {miner_hotkey} is still processing for response {response_id}")
                    
                    else:
                        await self.db_manager.execute("""
                        UPDATE weather_miner_responses
                        SET status = 'failed',
                            error_message = :error_msg
                        WHERE id = :response_id
                        """, {
                            "response_id": response_id,
                            "error_msg": kerchunk_response.get("message", "Unknown error")
                        })
                        
                        logger.warning(f"Miner {miner_hotkey} reported error for response {response_id}: {kerchunk_response.get('message')}")
                
                except Exception as e:
                    logger.error(f"Error processing response {response_id}: {e}", exc_info=True)
                    await self.db_manager.execute("""
                    UPDATE weather_miner_responses
                    SET status = 'error',
                        error_message = :error_msg
                    WHERE id = :response_id
                    """, {
                        "response_id": response_id,
                        "error_msg": str(e)
                    })
            
            verified_responses_query = """
            SELECT COUNT(*) as count
            FROM weather_miner_responses
            WHERE run_id = :run_id
            AND verification_passed = TRUE
            """
            verified_count_result = await self.db_manager.fetch_one(verified_responses_query, {"run_id": run_id})
            verified_count = verified_count_result["count"] if verified_count_result else 0
            
            total_responses_query = "SELECT COUNT(*) as count FROM weather_miner_responses WHERE run_id = :run_id"
            total_responses_result = await self.db_manager.fetch_one(total_responses_query, {"run_id": run_id})
            total_responses = total_responses_result["count"] if total_responses_result else 0
            
            min_ensemble_members = getattr(self.config, 'min_ensemble_members', 3)
            
            current_run_status = await self.db_manager.fetch_one("SELECT status FROM weather_forecast_runs WHERE id = :run_id", {"run_id": run_id})
            if current_run_status and current_run_status['status'] == 'scoring': # Only proceed if still in scoring state
                if verified_count >= min_ensemble_members:
                    logger.info(f"Run {run_id} has {verified_count} verified responses (>= {min_ensemble_members}). Triggering initial scoring.")
                    await self._trigger_initial_scoring(run_id) 
                elif total_responses > 0:
                     if verified_count > 0:
                         logger.info(f"Run {run_id} has {verified_count}/{total_responses} verified responses (< {min_ensemble_members}). Setting to partially_verified.")
                         await self._update_run_status(run_id, "partially_verified")
                     else:
                         logger.info(f"Run {run_id} has 0/{total_responses} verified responses. Setting to verification_failed.")
                         await self._update_run_status(run_id, "verification_failed")
                else:
                    logger.warning(f"Run {run_id}: No responses processed. Setting status to verification_failed.")
                    await self._update_run_status(run_id, "verification_failed")
            else:
                 logger.warning(f"Run {run_id} status is not 'scoring' ({current_run_status['status'] if current_run_status else 'Not Found'}). Skipping status update logic in validator_score.")

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
            asyncio.create_task(
                run_inference_background(
                    task_instance=self,
                    initial_batch=initial_batch,
                    job_id=job_id,
                    gfs_init_time=gfs_init_time,
                    miner_hotkey=miner.keypair.ss58_address
                )
            )
            logger.info(f"[Job {job_id}] Background task launched.")

            return {"status": "accepted", "job_id": job_id, "message": "Weather forecast job accepted for processing."}

        except Exception as e:
            job_id_for_error = new_job_id
            logger.error(f"[Job {job_id_for_error}] Error during initial miner_execute: {e}", exc_info=True)
            return {"status": "error", "job_id": None, "message": f"Failed to initiate job: {e}"}

    async def handle_kerchunk_request(self, job_id: str) -> Dict[str, Any]:
        """
        Handle a request for Kerchunk JSON metadata for a specific forecast job.
        
        Args:
            job_id: The unique identifier for the job
            
        Returns:
            Dict containing status, message, and if completed:
            - kerchunk_json_url: URL to access the Kerchunk JSON
            - verification_hash: Hash to verify forecast integrity
            - access_token: JWT token for accessing forecast files
        """
        logger.info(f"Handling kerchunk request for job_id: {job_id}")
        
        try:
            query = """
            SELECT job_id, status, target_netcdf_path, kerchunk_json_path, verification_hash, error_message
            FROM weather_miner_jobs
            WHERE job_id = :job_id
            """
            job = await self.db_manager.fetch_one(query, {"job_id": job_id})
            
            if not job:
                logger.warning(f"Job not found for job_id: {job_id}")
                return {
                    "status": "error",
                    "message": f"Job with ID {job_id} not found"
                }
                
            if job["status"] == "completed":
                netcdf_path = job["target_netcdf_path"]
                if not netcdf_path:
                    return {
                        "status": "error",
                        "message": "NetCDF path not set for completed job"
                    }
                    
                filename = os.path.basename(netcdf_path)
                
                token_data = {
                    "job_id": job_id,
                    "file_path": filename,
                    "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
                }
                
                access_token = jwt.encode(
                    token_data,
                    MINER_JWT_SECRET_KEY,
                    algorithm=JWT_ALGORITHM
                )
                
                # Return the completed job information with token
                kerchunk_url = f"/forecasts/{os.path.basename(job['kerchunk_json_path'])}"
                
                return {
                    "status": "completed",
                    "message": "Forecast completed and ready for access",
                    "kerchunk_json_url": kerchunk_url,
                    "verification_hash": job["verification_hash"],
                    "access_token": access_token
                }
                
            elif job["status"] == "error":
                return {
                    "status": "error",
                    "message": f"Job failed: {job['error_message'] or 'Unknown error'}"
                }
                
            else:
                return {
                    "status": "processing",
                    "message": f"Job is currently in status: {job['status']}"
                }
                
        except Exception as e:
            logger.error(f"Error handling kerchunk request for job_id {job_id}: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Failed to process request: {str(e)}"
            }

    ############################################################
    # Helper Methods
    ############################################################

    async def cleanup_resources(self):
        """
        Clean up resources like temporary files or reset database statuses
        in case of errors or shutdowns.
        """
        logger.info("Cleaning up weather task resources...")
        
        if self.ensemble_worker_running:
            await self.stop_ensemble_workers()
            
        if not self.ensemble_task_queue.empty():
            logger.info("Waiting for pending ensemble tasks to complete...")
            try:
                await asyncio.wait_for(self.ensemble_task_queue.join(), timeout=10.0)
                logger.info("All pending ensemble tasks completed")
            except asyncio.TimeoutError:
                logger.warning("Timed out waiting for ensemble tasks to complete")
                
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
        
    async def start_background_workers(self, num_ensemble_workers=1, num_initial_scoring_workers=1, num_final_scoring_workers=1):
         """Starts all background worker types."""
         await self.start_ensemble_workers(num_ensemble_workers)
         await self.start_initial_scoring_workers(num_initial_scoring_workers)
         await self.start_final_scoring_workers(num_final_scoring_workers)
         
    async def stop_background_workers(self):
        """Stops all background worker types."""
        try: await self.stop_ensemble_workers()
        except Exception as e: logger.error(f"Error stopping ensemble workers: {e}")
        try: await self.stop_initial_scoring_workers()
        except Exception as e: logger.error(f"Error stopping initial scoring workers: {e}")
        try: await self.stop_final_scoring_workers()
        except Exception as e: logger.error(f"Error stopping final scoring workers: {e}")
            