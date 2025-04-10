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

try:
    from aurora.core import Batch
    _AURORA_AVAILABLE = True
except ImportError:
    logger.warning("Aurora library not found. Batch type hinting and related functionality may fail.")
    Batch = Any
    _AURORA_AVAILABLE = False

from gaia.tasks.defined_tasks.weather.utils.hashing import compute_verification_hash
from gaia.tasks.defined_tasks.weather.utils.kerchunk_utils import generate_kerchunk_json_from_local_file
from gaia.tasks.defined_tasks.weather.weather_metadata import WeatherMetadata
from gaia.tasks.defined_tasks.weather.weather_inputs import WeatherInputs, WeatherForecastRequest, WeatherInputData
from gaia.tasks.defined_tasks.weather.weather_outputs import WeatherOutputs, WeatherKerchunkResponseData
from gaia.tasks.defined_tasks.weather.weather_scoring_mechanism import WeatherScoringMechanism
from gaia.tasks.defined_tasks.weather.weather_miner_preprocessing import WeatherMinerPreprocessing
from gaia.tasks.defined_tasks.weather.weather_validator_preprocessing import WeatherValidatorPreprocessing
from gaia.models.weather_basemodel import WeatherBaseModel
from gaia.tasks.defined_tasks.weather.utils.data_prep import create_aurora_batch_from_gfs
from gaia.tasks.defined_tasks.weather.weather_inference_runner import WeatherInferenceRunner
from gaia.tasks.defined_tasks.weather.utils.gfs_api import fetch_gfs_data
from gaia.tasks.defined_tasks.weather.weather_scoring.ensemble import create_weighted_ensemble, create_physics_aware_ensemble

logger = get_logger(__name__)

# placeholder classes until I build them
class WeatherMinerPreprocessing: pass
class WeatherValidatorPreprocessing: pass
class WeatherBaseModel: pass

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
    miner_preprocessing: Optional[WeatherMinerPreprocessing] = Field(
        default=None,
        description="Preprocessing component for miner",
    )
    validator_preprocessing: Optional[WeatherValidatorPreprocessing] = Field(
        default=None,
        description="Preprocessing component for validator",
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
        if db_manager is None:
             raise ValueError("db_manager must be provided to WeatherTask")
        if node_type is None:
            raise ValueError("node_type must be provided to WeatherTask ('validator' or 'miner')")

        super().__init__(
            name="WeatherTask",
            description="Weather forecast generation and verification task",
            task_type="atomic",
            metadata=WeatherMetadata(),
            inputs=WeatherInputs(),
            outputs=WeatherOutputs(),
            scoring_mechanism=WeatherScoringMechanism(
                db_manager=db_manager,
                task=None
            ),
            **data
        )

        self.db_manager = db_manager
        self.node_type = node_type
        self.test_mode = test_mode
        self.scoring_mechanism.task = self
        self.validator = data.get('validator', None)
        
        self.ensemble_task_queue = asyncio.Queue()
        self.ensemble_workers = []
        self.ensemble_worker_running = False

        if self.node_type == "validator":
            self.validator_preprocessing = WeatherValidatorPreprocessing()
            logger.info("Initialized validator components for WeatherTask")
        else:
            self.miner_preprocessing = WeatherMinerPreprocessing()
            try:
                self.inference_runner = WeatherInferenceRunner(device="cuda")
                self.model = self.inference_runner.model
                self.gpu_semaphore = asyncio.Semaphore(1)
                logger.info("Initialized Inference Runner and GPU Semaphore (limit 1).")
            except Exception as e:
                logger.error(f"Failed to initialize WeatherInferenceRunner: {e}", exc_info=True)
                self.inference_runner = None
                self.model = None

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

    async def _update_run_status(self, run_id: int, status: str, error_message: Optional[str] = None, gfs_metadata: Optional[dict] = None):
        """Helper to update the forecast run status and optionally other fields."""
        logger.debug(f"[Run {run_id}] Updating run status to '{status}'.")
        update_fields = ["status = :status"]
        params = {"run_id": run_id, "status": status}

        if error_message is not None:
            update_fields.append("error_message = :error_msg")
            params["error_msg"] = error_message
        if gfs_metadata is not None:
             update_fields.append("gfs_input_metadata = :gfs_meta")
             params["gfs_meta"] = json.dumps(gfs_metadata, default=str)
        if status in ["completed", "error"]:
             update_fields.append("completion_time = :comp_time")
             params["comp_time"] = datetime.now(timezone.utc)

        query = f"""
            UPDATE weather_forecast_runs
            SET {', '.join(update_fields)}
            WHERE id = :run_id
        """
        try:
            await self.db_manager.execute(query, params)
        except Exception as db_err:
            logger.error(f"[Run {run_id}] Failed to update run status to '{status}': {db_err}", exc_info=True)

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
                            from gaia.tasks.defined_tasks.weather.utils.hashing import verify_forecast_hash
                            
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
            
            if verified_count >= 3:
                logger.info(f"Creating ensemble forecast for run {run_id} with {verified_count} verified responses")
                
                if not self.ensemble_worker_running:
                    await self.start_ensemble_workers()
                    
                await self.ensemble_task_queue.put(run_id)
                logger.info(f"Queued ensemble creation for run {run_id}")
                
                await self._update_run_status(run_id, "processing_ensemble")
            else:
                logger.info(f"Not enough verified responses ({verified_count}) for run {run_id} to create ensemble")
                if verified_count > 0:
                    await self._update_run_status(run_id, "partially_verified")
                else:
                    await self._update_run_status(run_id, "verification_failed")
        
        for run in forecast_runs:
            try:
                await self.build_score_row(run['id'])
            except Exception as e:
                logger.error(f"Error building score row for run {run['id']}: {e}", exc_info=True)

    ############################################################
    # Miner methods
    ############################################################

    async def miner_preprocess(
        self,
        preprocessing: Optional[WeatherMinerPreprocessing] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Optional[Batch]:
        """
        Preprocesses the input GFS data received from the validator.
        Decodes the data, combines timesteps, and creates an Aurora Batch object.

        Args:
            data: Dictionary containing the raw payload from the validator,
                  expected to have 'gfs_timestep_1' and 'gfs_timestep_2'
                  containing base64 encoded pickled xarray Datasets.

        Returns:
            An aurora.Batch object ready for model inference, or None if preprocessing fails.
        """
        if not data:
            logger.error("No data provided to miner_preprocess.")
            return None

        try:
            logger.info("Starting miner preprocessing...")

            if 'gfs_timestep_1' not in data or 'gfs_timestep_2' not in data:
                 logger.error("Missing 'gfs_timestep_1' or 'gfs_timestep_2' in input data.")
                 return None

            try:
                logger.debug("Decoding gfs_timestep_1 (historical, e.g., 00z)")
                ds_hist_bytes = base64.b64decode(data['gfs_timestep_1'])
                ds_hist = pickle.loads(ds_hist_bytes)
                if not isinstance(ds_hist, xr.Dataset):
                    raise TypeError("Decoded gfs_timestep_1 is not an xarray Dataset")

                logger.debug("Decoding gfs_timestep_2 (current, e.g., 06z)")
                ds_curr_bytes = base64.b64decode(data['gfs_timestep_2'])
                ds_curr = pickle.loads(ds_curr_bytes)
                if not isinstance(ds_curr, xr.Dataset):
                    raise TypeError("Decoded gfs_timestep_2 is not an xarray Dataset")

            except (TypeError, pickle.UnpicklingError, base64.binascii.Error) as decode_err:
                logger.error(f"Failed to decode/unpickle GFS data: {decode_err}")
                logger.error(traceback.format_exc())
                return None

            if 'time' not in ds_hist.dims or 'time' not in ds_curr.dims:
                 logger.error("Decoded datasets missing 'time' dimension.")
                 return None
                 
            if ds_hist.time.values[0] >= ds_curr.time.values[0]:
                logger.warning("gfs_timestep_1 (historical) time is not strictly before gfs_timestep_2 (current). Ensure correct order.")

            try:
                logger.info("Combining historical and current GFS timesteps.")
                combined_gfs_data = xr.concat([ds_hist, ds_curr], dim='time')
                combined_gfs_data = combined_gfs_data.sortby('time')
                logger.info(f"Combined dataset time range: {combined_gfs_data.time.min().values} to {combined_gfs_data.time.max().values}")
                if len(combined_gfs_data.time) != 2:
                    logger.warning(f"Expected 2 time steps after combining, found {len(combined_gfs_data.time)}")

            except Exception as combine_err:
                 logger.error(f"Failed to combine GFS datasets: {combine_err}")
                 logger.error(traceback.format_exc())
                 return None

            resolution = '0.25'
            static_download_dir = './static_data'

            logger.info(f"Creating Aurora Batch using {resolution} resolution settings.")
            aurora_batch = create_aurora_batch_from_gfs(
                gfs_data=combined_gfs_data,
                resolution=resolution,
                download_dir=static_download_dir,
                history_steps=2
            )

            if not isinstance(aurora_batch, Batch):
                 logger.error("Failed to create a valid Aurora Batch object.")
                 return None

            logger.info("Successfully finished miner preprocessing.")
            return aurora_batch

        except Exception as e:
            logger.error(f"Unhandled error in miner_preprocess: {e}")
            logger.error(traceback.format_exc())
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
                self._run_inference_background(
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

    async def build_score_row(self, forecast_run_id: int):
        """
        Aggregates individual miner scores for a completed forecast run
        and inserts/updates the corresponding row in the main score_table.
        """
        logger.info(f"Building score row for forecast run {forecast_run_id}")
        
        try:
            run_query = """
            SELECT id, gfs_init_time_utc
            FROM weather_forecast_runs
            WHERE id = :run_id
            """
            
            run = await self.db_manager.fetch_one(run_query, {"run_id": forecast_run_id})
            if not run:
                logger.error(f"Run {forecast_run_id} not found")
                return
            
            scores_query = """
            SELECT 
                ms.miner_response_id,
                mr.miner_hotkey,
                ms.total_score,
                ms.rmse_score,
                ms.bias_score,
                ms.pattern_correlation
            FROM 
                weather_miner_scores ms
            JOIN 
                weather_miner_responses mr ON ms.miner_response_id = mr.id
            WHERE 
                mr.run_id = :run_id
                AND mr.verification_passed = TRUE
            """
            
            scores = await self.db_manager.fetch_all(scores_query, {"run_id": forecast_run_id})
            
            if not scores:
                logger.warning(f"No scored responses found for run {forecast_run_id}")
                return
            
            miner_count = len(scores)
            avg_score = sum(s['total_score'] for s in scores) / miner_count if miner_count > 0 else 0
            max_score = max(s['total_score'] for s in scores) if miner_count > 0 else 0
            min_score = min(s['total_score'] for s in scores) if miner_count > 0 else 0
            
            best_miner = max(scores, key=lambda s: s['total_score']) if scores else None
            best_miner_hotkey = best_miner['miner_hotkey'] if best_miner else None
            
            ensemble_query = """
            SELECT ef.id
            FROM weather_ensemble_forecasts ef
            WHERE ef.forecast_run_id = :run_id AND ef.status = 'completed'
            """
            
            ensemble = await self.db_manager.fetch_one(ensemble_query, {"run_id": forecast_run_id})
            ensemble_score = None
            
            if ensemble:
                # TODO: Implement ensemble scoring here
                ensemble_score = avg_score * 1.1 # Will remove this
            score_data = {
                "task_name": "weather",
                "subtask_name": "forecast",
                "run_id": str(forecast_run_id),
                "run_timestamp": run['gfs_init_time_utc'].isoformat(),
                "avg_score": float(avg_score),
                "max_score": float(max_score),
                "min_score": float(min_score),
                "miner_count": miner_count,
                "best_miner": best_miner_hotkey,
                "ensemble_score": float(ensemble_score) if ensemble_score is not None else None,
                "metadata": {
                    "gfs_init_time": run['gfs_init_time_utc'].isoformat(),
                    "verified_count": miner_count,
                    "has_ensemble": ensemble is not None
                }
            }
            
            exists_query = """
            SELECT id FROM score_table
            WHERE task_name = 'weather' AND run_id = :run_id
            """
            
            existing = await self.db_manager.fetch_one(exists_query, {"run_id": str(forecast_run_id)})
            
            if existing:
                update_query = """
                UPDATE score_table
                SET 
                    avg_score = :avg_score,
                    max_score = :max_score,
                    min_score = :min_score,
                    miner_count = :miner_count,
                    best_miner = :best_miner,
                    ensemble_score = :ensemble_score,
                    metadata = :metadata
                WHERE 
                    id = :id
                """
                
                await self.db_manager.execute(update_query, {
                    "id": existing['id'],
                    "avg_score": score_data['avg_score'],
                    "max_score": score_data['max_score'],
                    "min_score": score_data['min_score'],
                    "miner_count": score_data['miner_count'],
                    "best_miner": score_data['best_miner'],
                    "ensemble_score": score_data['ensemble_score'],
                    "metadata": json.dumps(score_data['metadata'])
                })
                
                logger.info(f"Updated score row for run {forecast_run_id}")
                
            else:
                insert_query = """
                INSERT INTO score_table
                (task_name, subtask_name, run_id, run_timestamp, avg_score, max_score, min_score, 
                 miner_count, best_miner, ensemble_score, metadata)
                VALUES
                (:task_name, :subtask_name, :run_id, :run_timestamp, :avg_score, :max_score, :min_score,
                 :miner_count, :best_miner, :ensemble_score, :metadata)
                """
                
                await self.db_manager.execute(insert_query, {
                    "task_name": score_data['task_name'],
                    "subtask_name": score_data['subtask_name'],
                    "run_id": score_data['run_id'],
                    "run_timestamp": score_data['run_timestamp'],
                    "avg_score": score_data['avg_score'],
                    "max_score": score_data['max_score'],
                    "min_score": score_data['min_score'],
                    "miner_count": score_data['miner_count'],
                    "best_miner": score_data['best_miner'],
                    "ensemble_score": score_data['ensemble_score'],
                    "metadata": json.dumps(score_data['metadata'])
                })
                
                logger.info(f"Inserted new score row for run {forecast_run_id}")
                
        except Exception as e:
            logger.error(f"Error building score row for run {forecast_run_id}: {e}", exc_info=True)

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

    async def _run_inference_background(self, initial_batch: Batch, job_id: str, gfs_init_time: datetime, miner_hotkey: str):
        """
        Runs the weather forecast inference as a background task.
        
        Args:
            initial_batch: The preprocessed Aurora batch
            job_id: The unique job identifier
            gfs_init_time: The GFS initialization time
            miner_hotkey: The miner's hotkey
        """
        logger.info(f"[Job {job_id}] Starting background inference task...")
        
        try:
            await self.update_job_status(job_id, "processing")
            
            logger.info(f"[Job {job_id}] Waiting for GPU semaphore...")
            async with self.gpu_semaphore:
                logger.info(f"[Job {job_id}] Acquired GPU semaphore, running inference...")
                
                try:
                    selected_predictions_cpu = await asyncio.to_thread(
                        self.inference_runner.run_multistep_inference,
                        initial_batch,
                        steps=40  # 40 steps of 6h each = 10 days
                    )
                    logger.info(f"[Job {job_id}] Inference completed with {len(selected_predictions_cpu)} time steps")
                    
                except Exception as infer_err:
                    logger.error(f"[Job {job_id}] Inference failed: {infer_err}", exc_info=True)
                    await self.update_job_status(job_id, "error", error_message=f"Inference error: {infer_err}")
                    return
            
            try:
                MINER_FORECAST_DIR_BG.mkdir(parents=True, exist_ok=True)
                
                def _blocking_save_and_process():
                    if not selected_predictions_cpu:
                        raise ValueError("Inference returned no prediction steps.")

                    forecast_datasets = []
                    lead_times_hours = []
                    base_time = pd.to_datetime(initial_batch.metadata.time[0])

                    for i, batch in enumerate(selected_predictions_cpu):
                        step_index_original = i * 2 + 1
                        lead_time_hours = (step_index_original + 1) * 6
                        forecast_time = base_time + timedelta(hours=lead_time_hours)

                        ds_step = batch.to_xarray_dataset()
                        ds_step = ds_step.assign_coords(time=[forecast_time])
                        ds_step = ds_step.expand_dims('time')
                        forecast_datasets.append(ds_step)
                        lead_times_hours.append(lead_time_hours)

                    combined_forecast_ds = xr.concat(forecast_datasets, dim='time')
                    combined_forecast_ds = combined_forecast_ds.assign_coords(lead_time=('time', lead_times_hours))
                    logger.info(f"[Job {job_id}] Combined forecast dimensions: {combined_forecast_ds.dims}")

                    gfs_time_str = gfs_init_time.strftime('%Y%m%d%H')
                    unique_suffix = str(uuid.uuid4())[:8]
                    filename_nc = f"weather_forecast_{gfs_time_str}_{miner_hotkey[:8]}_{unique_suffix}.nc"
                    output_nc_path = MINER_FORECAST_DIR_BG / filename_nc
                    
                    combined_forecast_ds.to_netcdf(output_nc_path)
                    logger.info(f"[Job {job_id}] Saved forecast to NetCDF: {output_nc_path}")
                    
                    filename_json = f"{os.path.splitext(filename_nc)[0]}.json"
                    output_json_path = MINER_FORECAST_DIR_BG / filename_json
                    
                    from kerchunk.hdf import SingleHdf5ToZarr
                    h5chunks = SingleHdf5ToZarr(str(output_nc_path), inline_threshold=0)
                    kerchunk_metadata = h5chunks.translate()
                    
                    with open(output_json_path, 'w') as f:
                        json.dump(kerchunk_metadata, f)
                    logger.info(f"[Job {job_id}] Generated Kerchunk JSON: {output_json_path}")
                    
                    from gaia.tasks.defined_tasks.weather.utils.hashing import compute_verification_hash
                    
                    forecast_metadata = {
                        "time": [base_time],
                        "source_model": "aurora",
                        "resolution": 0.25
                    }
                    
                    variables_to_hash = ["2t", "10u", "10v", "msl", "z", "u", "v", "t", "q"]
                    timesteps_to_hash = list(range(len(forecast_datasets)))
                
                    data_for_hash = {
                        "surf_vars": {},
                        "atmos_vars": {}
                    }
                    
                    for var in combined_forecast_ds.data_vars:
                        if var in ["2t", "10u", "10v", "msl"]:
                            data_for_hash["surf_vars"][var] = combined_forecast_ds[var].values[np.newaxis, :]
                        elif var in ["z", "u", "v", "t", "q"]:
                            data_for_hash["atmos_vars"][var] = combined_forecast_ds[var].values[np.newaxis, :]
                    
                    verification_hash = compute_verification_hash(
                        data=data_for_hash,
                        metadata=forecast_metadata,
                        variables=[v for v in variables_to_hash if v in combined_forecast_ds.data_vars],
                        timesteps=timesteps_to_hash
                    )
                    
                    logger.info(f"[Job {job_id}] Computed verification hash: {verification_hash}")
                    
                    return str(output_nc_path), str(output_json_path), verification_hash
                
                nc_path, json_path, v_hash = await asyncio.to_thread(_blocking_save_and_process)
                
                await self.update_job_paths(
                    job_id=job_id, 
                    target_netcdf_path=nc_path, 
                    kerchunk_json_path=json_path, 
                    verification_hash=v_hash
                )
                
                await self.update_job_status(job_id, "completed")
                logger.info(f"[Job {job_id}] Background task completed successfully")
                
            except Exception as save_err:
                logger.error(f"[Job {job_id}] Failed to save results: {save_err}", exc_info=True)
                await self.update_job_status(job_id, "error", error_message=f"Processing error: {save_err}")
        
        except Exception as e:
            logger.error(f"[Job {job_id}] Background task failed: {e}", exc_info=True)
            await self.update_job_status(job_id, "error", error_message=f"Task error: {e}")

    async def _update_job_status(self, job_id: str, status: str, **kwargs):
        """Helper to update the job status in the database."""
        logger.debug(f"[Job {job_id}] Updating status to '{status}' with args: {kwargs}")
        update_fields = ["status = :status"]
        params = {"job_id": job_id, "status": status}

        if 'netcdf_path' in kwargs and kwargs['netcdf_path']:
            update_fields.append("output_netcdf_path = :netcdf_path")
            params["netcdf_path"] = kwargs['netcdf_path']
        if 'kerchunk_path' in kwargs and kwargs['kerchunk_path']:
            update_fields.append("output_kerchunk_path = :kerchunk_path")
            params["kerchunk_path"] = kwargs['kerchunk_path']
        if 'verification_hash' in kwargs and kwargs['verification_hash']:
            update_fields.append("verification_hash = :verification_hash")
            params["verification_hash"] = kwargs['verification_hash']
        if 'error_message' in kwargs:
            update_fields.append("error_message = :error_message")
            params["error_message"] = kwargs['error_message']
        if 'end_time' in kwargs and kwargs['end_time']:
            update_fields.append("processing_end_time = :end_time")
            params["end_time"] = kwargs['end_time']

        query = f"""
            UPDATE weather_miner_jobs
            SET {', '.join(update_fields)}
            WHERE job_id = :job_id
        """
        try:
            await self.db_manager.execute(query, params)
            logger.info(f"[Job {job_id}] Successfully updated status to '{status}'.")
        except Exception as db_err:
            logger.error(f"[Job {job_id}] Failed to update job status to '{status}': {db_err}", exc_info=True)

    async def get_job_by_gfs_init_time(self, gfs_init_time_utc: datetime) -> Optional[Dict[str, Any]]:
        """
        Check if a job exists for the given GFS initialization time.
        
        Args:
            gfs_init_time_utc: The GFS initialization time to check
            
        Returns:
            Job record if found, None otherwise
        """
        try:
            query = """
            SELECT id, job_id, status, target_netcdf_path, kerchunk_json_path 
            FROM weather_miner_jobs
            WHERE gfs_init_time_utc = :gfs_init_time
            ORDER BY id DESC
            LIMIT 1
            """
            job = await self.db_manager.fetch_one(query, {"gfs_init_time": gfs_init_time_utc})
            return job
        except Exception as e:
            logger.error(f"Error checking for existing job with GFS init time {gfs_init_time_utc}: {e}")
            return None

    async def update_job_status(self, job_id: str, status: str, error_message: Optional[str] = None) -> bool:
        """
        Update the status of a job in the database.
        
        Args:
            job_id: The job ID
            status: The new status
            error_message: Optional error message
            
        Returns:
            True if update was successful, False otherwise
        """
        try:
            update_fields = ["status = :status", "updated_at = :updated_at"]
            params = {
                "job_id": job_id,
                "status": status,
                "updated_at": datetime.now(timezone.utc)
            }
            
            if status == "processing":
                update_fields.append("processing_start_time = :proc_start")
                params["proc_start"] = datetime.now(timezone.utc)
            elif status == "completed":
                update_fields.append("processing_end_time = :proc_end")
                params["proc_end"] = datetime.now(timezone.utc)
            
            if error_message:
                update_fields.append("error_message = :error_msg")
                params["error_msg"] = error_message
                
            query = f"""
            UPDATE weather_miner_jobs
            SET {", ".join(update_fields)}
            WHERE job_id = :job_id
            """
            
            await self.db_manager.execute(query, params)
            logger.info(f"Updated job {job_id} status to {status}")
            return True
        except Exception as e:
            logger.error(f"Error updating job status for {job_id}: {e}")
            return False

    async def update_job_paths(self, job_id: str, target_netcdf_path: str, kerchunk_json_path: str, verification_hash: str) -> bool:
        """
        Update the file paths and verification hash for a completed job.
        
        Args:
            job_id: The job ID
            target_netcdf_path: Path to the NetCDF file
            kerchunk_json_path: Path to the Kerchunk JSON file
            verification_hash: Hash to verify the forecast files
            
        Returns:
            True if update was successful, False otherwise
        """
        try:
            query = """
            UPDATE weather_miner_jobs
            SET target_netcdf_path = :netcdf_path,
                kerchunk_json_path = :kerchunk_path,
                verification_hash = :hash,
                updated_at = :updated_at
            WHERE job_id = :job_id
            """
            
            params = {
                "job_id": job_id,
                "netcdf_path": target_netcdf_path,
                "kerchunk_path": kerchunk_json_path,
                "hash": verification_hash,
                "updated_at": datetime.now(timezone.utc)
            }
            
            await self.db_manager.execute(query, params)
            logger.info(f"Updated job {job_id} with file paths and verification hash")
            return True
        except Exception as e:
            logger.error(f"Error updating job paths for {job_id}: {e}")
            return False

    async def create_ensemble_forecast(self, run_id: int) -> bool:
        """Creates an ensemble forecast from verified miner forecasts."""
        logger.info(f"Creating ensemble forecast for run {run_id}")
        
        try:
            query = """
            SELECT 
                mr.id as response_id, 
                mr.miner_hotkey,
                mr.kerchunk_json_url,
                COALESCE(
                    (SELECT weight FROM weather_historical_weights 
                     WHERE miner_hotkey = mr.miner_hotkey 
                     ORDER BY last_updated DESC LIMIT 1),
                    0.5
                ) as miner_weight,
                fr.gfs_init_time_utc
            FROM 
                weather_miner_responses mr
            JOIN
                weather_forecast_runs fr ON mr.run_id = fr.id
            WHERE 
                mr.run_id = :run_id
                AND mr.verification_passed = TRUE
                AND mr.status = 'verified'
            """
            
            responses = await self.db_manager.fetch_all(query, {"run_id": run_id})
            
            if not responses or len(responses) < 3:
                logger.warning(f"Not enough verified responses for run {run_id} to create ensemble")
                return False
            
            create_ensemble_query = """
            INSERT INTO weather_ensemble_forecasts 
            (forecast_run_id, creation_time, status)
            VALUES (:run_id, :creation_time, 'processing')
            RETURNING id
            """
            
            ensemble_record = await self.db_manager.execute(
                create_ensemble_query, 
                {
                    "run_id": run_id, 
                    "creation_time": datetime.now(timezone.utc)
                },
                fetch_one=True
            )
            
            if not ensemble_record or 'id' not in ensemble_record:
                logger.error(f"Failed to create ensemble record for run {run_id}")
                return False
            
            ensemble_id = ensemble_record['id']
            
            predictions = {}
            weights = {}
            
            for response in responses:
                miner_id = response['miner_hotkey']
                weights[miner_id] = float(response.get('miner_weight', 0.5))
                
                await self.db_manager.execute("""
                INSERT INTO weather_ensemble_components
                (ensemble_id, response_id, weight)
                VALUES (:ensemble_id, :response_id, :weight)
                """, {
                    "ensemble_id": ensemble_id,
                    "response_id": response['response_id'],
                    "weight": weights[miner_id]
                })
                
                token_response = await self._request_fresh_token(miner_id, response['job_id'])
                if not token_response or 'access_token' not in token_response:
                    logger.warning(f"Could not get access token for miner {miner_id}")
                    continue
                    
                try:
                    headers = {"Authorization": f"Bearer {token_response['access_token']}"}
                    ds = await self._open_forecast_dataset(response['kerchunk_json_url'], headers)
                    
                    key_variables = ["2t", "10u", "10v", "msl", "z", "u", "v", "t", "q"]
                    miner_data = {}
                    
                    for var in key_variables:
                        if var in ds:
                            miner_data[var] = ds[var].values
                    
                    predictions[miner_id] = miner_data
                    
                except Exception as e:
                    logger.error(f"Error loading forecast from miner {miner_id}: {e}")
            
            ensemble_data = await create_weighted_ensemble(predictions, weights)
            
            if not ensemble_data:
                logger.error(f"Failed to create ensemble data for run {run_id}")
                await self.db_manager.execute("""
                UPDATE weather_ensemble_forecasts
                SET status = 'failed'
                WHERE id = :ensemble_id
                """, {"ensemble_id": ensemble_id})
                return False
            
            sample_ds = None
            for miner_id in predictions:
                if predictions[miner_id]:
                    sample_ds = await self._open_forecast_dataset(
                        responses[0]['kerchunk_json_url'], 
                        {"Authorization": f"Bearer {token_response['access_token']}"}
                    )
                    break
                    
            if not sample_ds:
                logger.error("Could not get dimension information from any dataset")
                return False
            
            ensemble_ds = xr.Dataset()
            
            for var, data in ensemble_data.items():
                if var in sample_ds:
                    ensemble_ds[var] = (sample_ds[var].dims, data)
                    
            for coord_name, coord in sample_ds.coords.items():
                if coord_name not in ensemble_ds.coords:
                    ensemble_ds.coords[coord_name] = coord
            
            gfs_init_time = responses[0]['gfs_init_time_utc']
            time_str = gfs_init_time.strftime('%Y%m%d%H') if hasattr(gfs_init_time, 'strftime') else str(gfs_init_time)
            
            ensemble_filename = f"ensemble_forecast_{time_str}.nc"
            ensemble_dir = Path("./validator_ensembles/")
            ensemble_dir.mkdir(parents=True, exist_ok=True)
            ensemble_path = ensemble_dir / ensemble_filename
            ensemble_ds.to_netcdf(str(ensemble_path))
            
            kerchunk_filename = f"{os.path.splitext(ensemble_filename)[0]}.json"
            kerchunk_path = ensemble_dir / kerchunk_filename
            
            from kerchunk.hdf import SingleHdf5ToZarr
            h5chunks = SingleHdf5ToZarr(str(ensemble_path), inline_threshold=0)
            kerchunk_metadata = h5chunks.translate()
            
            with open(kerchunk_path, 'w') as f:
                json.dump(kerchunk_metadata, f)
            
            from gaia.tasks.defined_tasks.weather.utils.hashing import compute_verification_hash
            
            ensemble_metadata = {
                "time": [gfs_init_time],
                "source_model": "ensemble",
                "resolution": 0.25
            }
            
            variables_to_hash = [var for var in ["2t", "10u", "10v", "msl", "z", "u", "v", "t", "q"] 
                                 if var in ensemble_ds]
            
            data_for_hash = {
                "surf_vars": {},
                "atmos_vars": {}
            }
            
            for var in variables_to_hash:
                if var in ["2t", "10u", "10v", "msl"]:
                    data_for_hash["surf_vars"][var] = ensemble_ds[var].values[np.newaxis, :]
                elif var in ["z", "u", "v", "t", "q"]:
                    data_for_hash["atmos_vars"][var] = ensemble_ds[var].values[np.newaxis, :]
            
            verification_hash = compute_verification_hash(
                data=data_for_hash,
                metadata=ensemble_metadata,
                variables=variables_to_hash,
                timesteps=list(range(len(ensemble_ds.time)))
            )
            
            await self.db_manager.execute("""
            UPDATE weather_ensemble_forecasts
            SET ensemble_path = :path,
                ensemble_kerchunk_path = :kerchunk_path,
                ensemble_verification_hash = :hash,
                status = 'completed'
            WHERE id = :ensemble_id
            """, {
                "path": str(ensemble_path),
                "kerchunk_path": str(kerchunk_path),
                "hash": verification_hash,
                "ensemble_id": ensemble_id
            })
            
            logger.info(f"Successfully created ensemble forecast for run {run_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating ensemble forecast for run {run_id}: {e}", exc_info=True)
            
            if 'ensemble_id' in locals():
                await self.db_manager.execute("""
                UPDATE weather_ensemble_forecasts
                SET status = 'error',
                    error_message = :error_msg
                WHERE id = :ensemble_id
                """, {
                    "error_msg": str(e),
                    "ensemble_id": ensemble_id
                })
            
            return False

    async def _request_fresh_token(self, miner_hotkey, job_id):
        """Request a fresh access token for a job."""
        try:
            kerchunk_request_payload = {
                "nonce": str(uuid.uuid4()),
                "data": {"job_id": job_id}
            }
            
            response = await self.validator.query_miner(
                miner_hotkey=miner_hotkey,
                payload=kerchunk_request_payload,
                endpoint="/weather-kerchunk-request"
            )
            
            if response and response.get("status") == "completed":
                return {
                    "access_token": response.get("access_token")
                }
            return None
        except Exception as e:
            logger.error(f"Error requesting token: {e}")
            return None

    async def _open_forecast_dataset(self, kerchunk_url, headers):
        """Open a forecast dataset using kerchunk JSON."""
        try:
            fs = fsspec.filesystem(
                "reference", 
                fo=kerchunk_url, 
                remote_options={"headers": headers}
            )
            
            mapper = fs.get_mapper("")
            ds = xr.open_dataset(mapper, engine="zarr", backend_kwargs={"consolidated": False})
            
            return ds
        except Exception as e:
            logger.error(f"Error opening forecast: {e}")
            raise
        
    async def start_ensemble_workers(self, num_workers=1):
        """Start background workers for ensemble processing."""
        if self.ensemble_worker_running:
            logger.info("Ensemble workers already running")
            return
            
        self.ensemble_worker_running = True
        for _ in range(num_workers):
            worker = asyncio.create_task(self._ensemble_worker())
            self.ensemble_workers.append(worker)
            
        logger.info(f"Started {num_workers} ensemble workers")
        
    async def stop_ensemble_workers(self):
        """Stop all background ensemble workers."""
        if not self.ensemble_worker_running:
            return
            
        self.ensemble_worker_running = False
        
        for worker in self.ensemble_workers:
            worker.cancel()
            
        self.ensemble_workers = []
        logger.info("Stopped all ensemble workers")
        
    async def _ensemble_worker(self):
        """Background worker that processes ensemble tasks using the advanced method."""
        while self.ensemble_worker_running:
            run_id = None
            ensemble_id = None
            try:
                try:
                    run_id = await asyncio.wait_for(self.ensemble_task_queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue

                logger.info(f"[EnsembleWorker] Processing ensemble for run {run_id}")

                query = """
                SELECT 
                    mr.miner_hotkey,
                    mr.kerchunk_json_url,
                    mr.job_id,  -- Need job_id for token requests
                    fr.gfs_init_time_utc,
                    ef.id as ensemble_id,
                    COALESCE(
                        (SELECT weight FROM weather_historical_weights 
                         WHERE miner_hotkey = mr.miner_hotkey 
                         ORDER BY last_updated DESC LIMIT 1),
                        0.5 -- Default weight if none found
                    ) as miner_weight
                FROM 
                    weather_miner_responses mr
                JOIN
                    weather_forecast_runs fr ON mr.run_id = fr.id
                JOIN
                    weather_ensemble_forecasts ef ON ef.forecast_run_id = fr.id
                WHERE 
                    mr.run_id = :run_id
                    AND mr.verification_passed = TRUE
                    AND mr.status = 'verified'
                """
                responses = await self.db_manager.fetch_all(query, {"run_id": run_id})

                if not responses or len(responses) < 3:
                    logger.warning(f"[EnsembleWorker] Not enough verified responses ({len(responses)}) for run {run_id} to create ensemble. Min required: {getattr(self.config, 'min_ensemble_members', 3)}")
                    await self.db_manager.execute(
                        "UPDATE weather_ensemble_forecasts SET status = 'failed', error_message = :msg WHERE id = :eid",
                        {"eid": ensemble_id, "msg": "Insufficient verified members"}
                    )
                    self.ensemble_task_queue.task_done()
                    continue
                    
                ensemble_id = responses[0]['ensemble_id']
                gfs_init_time = responses[0]['gfs_init_time_utc']

                miner_forecast_refs = {}
                miner_weights = {}
                skipped_miners = []

                async def _get_miner_ref(response):
                    miner_id = response['miner_hotkey']
                    job_id = response['job_id']
                    kerchunk_url = response['kerchunk_json_url']
                    weight = float(response['miner_weight'])
                    
                    token_response = await self._request_fresh_token(miner_id, job_id)
                    if not token_response or 'access_token' not in token_response:
                        logger.warning(f"[EnsembleWorker] Could not get access token for miner {miner_id}, job {job_id}. Skipping for ensemble.")
                        return None, miner_id
                        
                    access_token = token_response['access_token']
                    ref_spec = {
                        'url': kerchunk_url,
                        'protocol': 'http',
                        'options': {
                            'headers': {
                                'Authorization': f'Bearer {access_token}'
                            }
                        }
                    }
                    return (miner_id, ref_spec, weight), None

                tasks = [_get_miner_ref(resp) for resp in responses]
                results = await asyncio.gather(*tasks)

                for result, skipped_miner_id in results:
                     if skipped_miner_id:
                         skipped_miners.append(skipped_miner_id)
                     elif result:
                         miner_id, ref_spec, weight = result
                         miner_forecast_refs[miner_id] = ref_spec
                         miner_weights[miner_id] = weight

                if len(miner_forecast_refs) < getattr(self.config, 'min_ensemble_members', 3):
                    logger.warning(f"[EnsembleWorker] Not enough valid miners ({len(miner_forecast_refs)}) after token requests for run {run_id}. Min required: {getattr(self.config, 'min_ensemble_members', 3)}")
                    await self.db_manager.execute(
                        "UPDATE weather_ensemble_forecasts SET status = 'failed', error_message = :msg WHERE id = :eid",
                        {"eid": ensemble_id, "msg": "Insufficient members after token fetch"}
                    )
                    self.ensemble_task_queue.task_done()
                    continue

                top_k = getattr(self.config, 'top_k_ensemble', None)
                ensemble_ds = await create_physics_aware_ensemble(
                    miner_forecast_refs=miner_forecast_refs,
                    miner_weights=miner_weights,
                    top_k=top_k
                    #variables_to_process=... # optional subset
                    #consistency_checks=... # optional override default
                )

                if ensemble_ds:
                    logger.info(f"[EnsembleWorker] Successfully created ensemble dataset for run {run_id}")
                    
                    time_str = gfs_init_time.strftime('%Y%m%d%H')
                    ensemble_nc_filename = f"ensemble_run_{run_id}_{time_str}.nc"
                    ensemble_path = VALIDATOR_ENSEMBLE_DIR / ensemble_nc_filename
                    try:
                        encoding = {var: {'zlib': True, 'complevel': 5} for var in ensemble_ds.data_vars}
                        await asyncio.to_thread(ensemble_ds.to_netcdf, path=str(ensemble_path), encoding=encoding)
                        logger.info(f"[EnsembleWorker] Saved ensemble NetCDF: {ensemble_path}")
                    except Exception as e_save:
                         logger.error(f"[EnsembleWorker] Failed to save ensemble NetCDF for run {run_id}: {e_save}", exc_info=True)
                         await self.db_manager.execute(
                            "UPDATE weather_ensemble_forecasts SET status = 'failed', error_message = :msg WHERE id = :eid",
                            {"eid": ensemble_id, "msg": f"Failed to save NetCDF: {e_save}"}
                         )
                         await self._update_run_status(run_id, "ensemble_failed", error_message=f"Failed to save NetCDF")
                         self.ensemble_task_queue.task_done()
                         continue

                    kerchunk_filename = f"{os.path.splitext(ensemble_nc_filename)[0]}.json"
                    kerchunk_path = VALIDATOR_ENSEMBLE_DIR / kerchunk_filename
                    try:
                        h5chunks = SingleHdf5ToZarr(str(ensemble_path), inline_threshold=0)
                        kerchunk_metadata = h5chunks.translate()
                        with open(kerchunk_path, 'w') as f:
                            json.dump(kerchunk_metadata, f)
                        logger.info(f"[EnsembleWorker] Generated ensemble Kerchunk JSON: {kerchunk_path}")
                    except Exception as e_kc:
                        logger.error(f"[EnsembleWorker] Failed to generate Kerchunk JSON for run {run_id}: {e_kc}", exc_info=True)
                        kerchunk_path = None

                    verification_hash = None
                    try:
                        ensemble_metadata_for_hash = {
                            "time": [gfs_init_time],
                            "source_model": "physics_aware_ensemble",
                            "resolution": ensemble_ds.attrs.get('resolution', 0.25)
                        }
                        variables_to_hash = [v for v in ensemble_ds.data_vars if v in ALL_EXPECTED_VARIABLES]
                        
                        data_for_hash = {"surf_vars": {}, "atmos_vars": {}}
                        for var in variables_to_hash:
                            var_data = ensemble_ds[var].values
                            if 'time' not in ensemble_ds[var].dims:
                                var_data = np.expand_dims(var_data, axis=0)
                                
                            if var in AURORA_FUNDAMENTAL_SURFACE_VARIABLES + [dv for dv in AURORA_DERIVED_VARIABLES if dv in ensemble_ds]:
                                data_for_hash["surf_vars"][var] = var_data
                            elif var in AURORA_FUNDAMENTAL_ATMOS_VARIABLES:
                                data_for_hash["atmos_vars"][var] = var_data

                        if not data_for_hash["surf_vars"] and not data_for_hash["atmos_vars"]:
                             logger.warning(f"[EnsembleWorker] No variables found for hashing in ensemble for run {run_id}")
                        else:
                            verification_hash = compute_verification_hash(
                                data=data_for_hash,
                                metadata=ensemble_metadata_for_hash,
                                variables=variables_to_hash,
                                timesteps=list(range(len(ensemble_ds.time)))
                            )
                            logger.info(f"[EnsembleWorker] Computed ensemble verification hash for run {run_id}: {verification_hash[:10]}..."
                        )
                    except Exception as e_hash:
                         logger.error(f"[EnsembleWorker] Failed to compute verification hash for ensemble run {run_id}: {e_hash}", exc_info=True)

                    update_params = {
                        "eid": ensemble_id,
                        "status": "completed",
                        "path": str(ensemble_path),
                        "kpath": str(kerchunk_path) if kerchunk_path else None,
                        "hash": verification_hash,
                        "end_time": datetime.now(timezone.utc)
                    }
                    update_query = """
                    UPDATE weather_ensemble_forecasts
                    SET status = :status, 
                        ensemble_path = :path,
                        ensemble_kerchunk_path = :kpath,
                        ensemble_verification_hash = :hash,
                        processing_end_time = :end_time,
                        error_message = NULL
                    WHERE id = :eid
                    """
                    await self.db_manager.execute(update_query, update_params)
                    await self._update_run_status(run_id, "completed")
                    logger.info(f"[EnsembleWorker] Successfully completed and recorded ensemble for run {run_id}")

                else:
                    logger.error(f"[EnsembleWorker] create_physics_aware_ensemble failed for run {run_id}")
                    await self.db_manager.execute(
                        "UPDATE weather_ensemble_forecasts SET status = 'failed', error_message = :msg WHERE id = :eid",
                        {"eid": ensemble_id, "msg": "Ensemble creation function returned None"}
                    )
                    await self._update_run_status(run_id, "ensemble_failed", error_message="Ensemble function failed")

                self.ensemble_task_queue.task_done()

            except asyncio.CancelledError:
                logger.info("[EnsembleWorker] Worker cancelled")
                if run_id is not None and self.ensemble_task_queue._unfinished_tasks > 0:
                     self.ensemble_task_queue.task_done()
                break

            except Exception as e:
                logger.error(f"[EnsembleWorker] Unexpected error processing run {run_id}: {e}", exc_info=True)
                if run_id and ensemble_id:
                    try:
                        await self.db_manager.execute(
                            "UPDATE weather_ensemble_forecasts SET status = 'error', error_message = :msg WHERE id = :eid",
                            {"eid": ensemble_id, "msg": f"Worker error: {e}"}
                        )
                        await self._update_run_status(run_id, "ensemble_failed", error_message=f"Worker error: {e}")
                    except Exception as db_err:
                         logger.error(f"[EnsembleWorker] Failed to update DB on error: {db_err}")
                if run_id is not None and self.ensemble_task_queue._unfinished_tasks > 0:
                     self.ensemble_task_queue.task_done()
                await asyncio.sleep(1)
