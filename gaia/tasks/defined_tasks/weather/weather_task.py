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

from gaia.tasks.defined_tasks.weather.utils.hashing import compute_verification_hash
from gaia.tasks.defined_tasks.weather.utils.kerchunk_utils import generate_kerchunk_json_from_local_file
from gaia.tasks.defined_tasks.weather.weather_metadata import WeatherMetadata
from gaia.tasks.defined_tasks.weather.weather_inputs import WeatherInputs, WeatherForecastRequest, WeatherInputData
from gaia.tasks.defined_tasks.weather.weather_outputs import WeatherOutputs, WeatherKerchunkResponse
from gaia.tasks.defined_tasks.weather.weather_scoring_mechanism import WeatherScoringMechanism
from gaia.tasks.defined_tasks.weather.weather_miner_preprocessing import WeatherMinerPreprocessing
from gaia.tasks.defined_tasks.weather.weather_validator_preprocessing import WeatherValidatorPreprocessing
from gaia.models.weather_basemodel import WeatherBaseModel
from gaia.tasks.defined_tasks.weather.utils.data_prep import create_aurora_batch_from_gfs
from gaia.tasks.defined_tasks.weather.weather_inference_runner import WeatherInferenceRunner
from gaia.tasks.defined_tasks.weather.utils.gfs_api import fetch_gfs_data

logger = get_logger(__name__)

# placeholder classes until I build them
class WeatherMinerPreprocessing: pass
class WeatherValidatorPreprocessing: pass
class WeatherBaseModel: pass

DEFAULT_FORECAST_DIR_BG = Path("./miner_forecasts/")
MINER_FORECAST_DIR_BG = Path(os.getenv("MINER_FORECAST_DIR", DEFAULT_FORECAST_DIR_BG))
MINER_FORECAST_DIR_BG.mkdir(parents=True, exist_ok=True)

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
        pass

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
        """
        logger.info("Miner execute called for WeatherTask")
        job_id = str(uuid.uuid4())

        if self.inference_runner is None:
             logger.error(f"[Job {job_id}] Cannot execute: Inference Runner not available.")
             return {"status": "error", "message": "Miner inference component not ready"}
        if not data or 'data' not in data:
             logger.error(f"[Job {job_id}] Invalid or missing payload data.")
             return {"status": "error", "message": "Invalid payload structure"}

        validator_hotkey = miner.config.keypair.ss58_address
        payload_data = data['data']

        try:
            gfs_init_time = payload_data.get('forecast_start_time')
            if not isinstance(gfs_init_time, datetime):
                 try:
                     gfs_init_time = datetime.fromisoformat(str(gfs_init_time))
                 except (ValueError, TypeError):
                     logger.error(f"[Job {job_id}] Invalid forecast_start_time format: {gfs_init_time}")
                     return {"status": "error", "message": "Invalid forecast_start_time format"}

            logger.info(f"[Job {job_id}] Processing request for GFS init time: {gfs_init_time}")

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
            await self.db_manager.execute(insert_query, {
                "id": job_id,
                "req_time": datetime.now(timezone.utc),
                "val_hk": validator_hotkey, # Placeholder
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
            logger.error(f"[Job {job_id}] Error during initial miner_execute: {e}", exc_info=True)
            try:
                 await self._update_job_status(job_id, 'error', error_message=f"Initial processing error: {e}")
            except Exception as db_err:
                 logger.error(f"[Job {job_id}] Failed to update job status to error after exception: {db_err}")

            return {"status": "error", "job_id": job_id, "message": f"Failed to initiate job: {e}"}

    ############################################################
    # Helper Methods
    ############################################################

    async def build_score_row(self, forecast_run_id: int):
        """
        Aggregates individual miner scores for a completed forecast run
        and inserts/updates the corresponding row in the main score_table.
        """
        pass

    async def cleanup_resources(self):
        """
        Clean up resources like temporary files or reset database statuses
        in case of errors or shutdowns.
        """
        pass

    async def _run_inference_background(self, initial_batch: Batch, job_id: str, gfs_init_time: datetime, miner_hotkey: str):
        """
        Runs the heavy inference, saves results, and updates the DB job status.
        Designed to be run via asyncio.create_task.
        """
        start_time = time.time()
        logger.info(f"[Job {job_id}] Background inference task started.")

        if self.inference_runner is None or self.gpu_semaphore is None:
             logger.error(f"[Job {job_id}] Inference runner or semaphore not initialized. Aborting background task.")
             await self._update_job_status(job_id, 'error', error_message="Miner inference component not ready.")
             return

        output_netcdf_path_str = None
        output_kerchunk_path_str = None
        computed_hash = None
        final_status = 'error'
        error_message_log = "An unspecified error occurred in the background task."

        try:
            await self._update_job_status(job_id, 'processing')

            logger.info(f"[Job {job_id}] Waiting to acquire GPU semaphore...")
            async with self.gpu_semaphore:
                logger.info(f"[Job {job_id}] GPU semaphore acquired. Starting inference.")
                inference_start_time = time.time()

                # Inference (Blocking part in thread)
                try:
                    def _blocking_inference():
                        return self.inference_runner.run_multistep_inference(initial_batch, steps=40)

                    selected_predictions_cpu: List[Batch] = await asyncio.to_thread(_blocking_inference)
                    logger.info(f"[Job {job_id}] Inference completed in {time.time() - inference_start_time:.2f} seconds. Received {len(selected_predictions_cpu)} prediction steps.")

                except Exception as inference_err:
                     logger.error(f"[Job {job_id}] Error during model inference: {inference_err}", exc_info=True)
                     error_message_log = f"Model inference failed: {inference_err}"
                     raise

            logger.info(f"[Job {job_id}] Processing and saving results...")
            processing_start_time = time.time()

            try:
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

                    filename_json = f"{output_nc_path.stem}.json"
                    output_json_path = output_nc_path.with_suffix('.json')
                    kerchunk_refs = generate_kerchunk_json_from_local_file(
                         str(output_nc_path),
                         output_json_path=str(output_json_path)
                    )
                    if not output_json_path.exists():
                         raise RuntimeError("Kerchunk JSON file was not created.")
                    logger.info(f"[Job {job_id}] Generated Kerchunk JSON: {output_json_path}")

                    local_hash = compute_verification_hash(str(output_nc_path), str(output_json_path))
                    logger.info(f"[Job {job_id}] Computed verification hash: {local_hash}")

                    return str(output_nc_path), str(output_json_path), local_hash

                output_netcdf_path_str, output_kerchunk_path_str, computed_hash = await asyncio.to_thread(_blocking_save_and_process)
                final_status = 'completed'
                error_message_log = None
                logger.info(f"[Job {job_id}] Result processing completed in {time.time() - processing_start_time:.2f} seconds.")

            except Exception as proc_err:
                 logger.error(f"[Job {job_id}] Error processing/saving results: {proc_err}", exc_info=True)
                 error_message_log = f"Result processing/saving failed: {proc_err}"

        except Exception as bg_err:
             logger.error(f"[Job {job_id}] Unhandled error in background task: {bg_err}", exc_info=True)
             error_message_log = f"Background task failed: {bg_err}"

        finally:
             await self._update_job_status(
                 job_id,
                 final_status,
                 netcdf_path=output_netcdf_path_str,
                 kerchunk_path=output_kerchunk_path_str,
                 verification_hash=computed_hash,
                 error_message=error_message_log,
                 end_time=datetime.now(timezone.utc)
             )
             logger.info(f"[Job {job_id}] Background task finished with status '{final_status}' in {time.time() - start_time:.2f} seconds.")

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
            WHERE id = :job_id
        """
        try:
            await self.db_manager.execute(query, params)
            logger.info(f"[Job {job_id}] Successfully updated status to '{status}'.")
        except Exception as db_err:
            logger.error(f"[Job {job_id}] Failed to update job status to '{status}': {db_err}", exc_info=True)
