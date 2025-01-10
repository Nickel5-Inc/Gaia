"""
SoilMoistureTask implementation for processing and analyzing soil moisture data.

This task involves:
    - Querying miners for predictions
    - Processing soil moisture regions
    - Scoring predictions
    - Managing task history and scoring
"""

#############################################
# Standard Library Imports
#############################################
from typing import Dict, List, Optional, Tuple, Any, Union
from types import ModuleType
from importlib import util as importlib_util
from datetime import datetime, timedelta, timezone
import asyncio
import base64
import glob
import json
import math
import os
import tempfile
import traceback
from uuid import uuid4

#############################################
# Third Party Imports
#############################################
import numpy as np
from pydantic import Field
from sqlalchemy import text

#############################################
# Gaia Imports
#############################################
from fiber.logging_utils import get_logger
from gaia.tasks.base.task import Task
from gaia.tasks.base.components.metadata import Metadata
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.models.soil_moisture_basemodel import SoilModel
from gaia.validator.utils.task_decorator import task_step
from gaia.tasks.base.decorators import task_timer
from gaia.tasks.base.components.preprocessing import Preprocessing
from gaia.tasks.base.components.inputs import Inputs
from gaia.tasks.base.components.outputs import Outputs

# Task-specific imports
from gaia.tasks.defined_tasks.soilmoisture.soil_miner_preprocessing import (
    SoilMinerPreprocessing,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_scoring_mechanism import (
    SoilScoringMechanism,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_inputs import (
    SoilMoistureInputs,
    SoilMoisturePayload,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_outputs import (
    SoilMoistureOutputs,
    SoilMoisturePrediction,
)
from gaia.tasks.defined_tasks.soilmoisture.soil_metadata import SoilMoistureMetadata

#############################################
# Constants
#############################################
# Timeout constants
VALIDATOR_CYCLE_TIMEOUT = 3600  # 1 hour
SCORING_TIMEOUT = 300  # 5 minutes
DATA_PREP_TIMEOUT = 600  # 10 minutes
MINER_QUERY_TIMEOUT = 180  # 3 minutes
DB_OP_TIMEOUT = 60  # 1 minute

logger = get_logger(__name__)

# Conditional imports
if os.environ.get("NODE_TYPE") == "validator":
    from gaia.tasks.defined_tasks.soilmoisture.soil_validator_preprocessing import (
        SoilValidatorPreprocessing,
    )
else:
    SoilValidatorPreprocessing = None

class SoilMoistureTask(Task):
    """
    A task class for processing and analyzing soil moisture data.
    
    The task is organized into several logical sections:
    1. Class Setup & Initialization
       - Class initialization
       - Database setup
       - Component initialization
    
    2. Main Execution Flow
       - Validator execution cycle
       - Test mode handling
       - Normal mode handling
       - Scoring cycle execution
    
    3. Time Window Management
       - Window definitions
       - Time calculations
       - SMAP time mapping
       - IFS forecast timing
    
    4. Region Processing
       - Region preparation
       - Region validation
       - Region data processing
       - Daily region management
    
    5. Task Queue Management
       - Task addition
       - Task retrieval
       - Miner querying
       - Queue status updates
    
    6. Scoring & History
       - Task scoring
       - Score calculation
       - History management
       - Score aggregation
    
    7. Model Operations
       - Model inference
       - Data preprocessing
       - Model initialization
    
    8. Cleanup & Utilities
       - File cleanup
       - Resource management
       - Error handling
    """

    #############################################
    # 1. Class Setup & Initialization
    #############################################

    prediction_horizon: timedelta = Field(
        default_factory=lambda: timedelta(hours=6),
        description="Prediction horizon for the task",
    )
    scoring_delay: timedelta = Field(
        default_factory=lambda: timedelta(days=3),
        description="Delay before scoring due to SMAP data latency",
    )

    validator_preprocessing: Optional[Any] = None  # Type will be SoilValidatorPreprocessing
    miner_preprocessing: Optional[Any] = None  # Type will be SoilMinerPreprocessing
    model: Optional[SoilModel] = None
    db_manager: Any = Field(default=None)
    node_type: Optional[str] = Field(default="miner")
    test_mode: bool = Field(default=False)
    use_raw_preprocessing: bool = Field(default=False)
    scoring_mechanism: Optional[Any] = None  # Type will be SoilScoringMechanism

    def __init__(self, db_manager=None, node_type: Optional[str] = None, test_mode: bool = False, **data):
        super().__init__(
            name="SoilMoistureTask",
            description="Soil moisture prediction task",
            task_type="atomic",
            metadata=SoilMoistureMetadata(),
            inputs=SoilMoistureInputs(),
            outputs=SoilMoistureOutputs(),
            scoring_mechanism=SoilScoringMechanism(db_manager=db_manager),
            preprocessing=None,
            subtasks=None,
            db=db_manager,
        )

        self.db_manager = db_manager
        self.node_type = node_type if node_type is not None else "miner"
        self.test_mode = test_mode
        self.scoring_mechanism = SoilScoringMechanism(db_manager=db_manager)

        if self.node_type == "validator" and SoilValidatorPreprocessing is not None:
            self.validator_preprocessing = SoilValidatorPreprocessing(db_manager=self.db_manager)
        else:
            if SoilMinerPreprocessing is not None:
                self.miner_preprocessing = SoilMinerPreprocessing(task=self)

                custom_model_path = "gaia/models/custom_models/custom_soil_model.py"
                if os.path.exists(custom_model_path):
                    try:
                        spec = importlib_util.spec_from_file_location("custom_soil_model", custom_model_path)
                        if spec is not None and spec.loader is not None:
                            module = importlib_util.module_from_spec(spec)
                            spec.loader.exec_module(module)
                            self.model = module.CustomSoilModel()
                            self.use_raw_preprocessing = True
                            logger.info("Initialized custom soil model")
                    except Exception as e:
                        logger.error(f"Error loading custom model: {e}")
                        self.model = self.miner_preprocessing.model if self.miner_preprocessing else None
                        self.use_raw_preprocessing = False
                else:
                    self.model = self.miner_preprocessing.model if self.miner_preprocessing else None
                    self.use_raw_preprocessing = False
                    logger.info("Initialized base soil model")
                
                logger.info("Initialized miner components for SoilMoistureTask")

        self._prepared_regions = {}

    async def ensure_retry_columns_exist(self):
        """Ensure retry-related columns exist in soil_moisture_predictions table."""
        try:
            columns_check = await self.db_manager.fetch_one("""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name = 'soil_moisture_predictions' 
                    AND column_name = 'retry_count'
                );
            """)
            
            if not columns_check or not columns_check['exists']:
                logger.info("Adding retry columns to soil_moisture_predictions table")
                await self.db_manager.execute("""
                    ALTER TABLE soil_moisture_predictions 
                    ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS next_retry_time TIMESTAMP WITH TIME ZONE,
                    ADD COLUMN IF NOT EXISTS last_retry_at TIMESTAMP WITH TIME ZONE;
                """)
                logger.info("Successfully added retry columns")
            
        except Exception as e:
            logger.error(f"Error ensuring retry columns exist: {e}")
            logger.error(traceback.format_exc())

    #############################################
    # 2. Main Execution Flow
    #############################################

    @task_step(
        name="validator_execute_cycle",
        description="Execute main validator cycle for soil moisture task",
        timeout_seconds=VALIDATOR_CYCLE_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=30
    )
    async def validator_execute(self, validator):
        """Execute validator workflow with proper timeout handling."""
        if not hasattr(self, "db_manager") or self.db_manager is None:
            self.db_manager = validator.db_manager

        await self.ensure_retry_columns_exist()
        
        while True:
            try:
                current_time = datetime.now(timezone.utc)

                if current_time.minute % 1 == 0:
                    await self._run_scoring_cycle()

                if self.test_mode:
                    await self._handle_test_mode(validator, current_time)
                else:
                    await self._handle_normal_mode(validator, current_time)

            except Exception as e:
                logger.error(f"Error in validator_execute: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(60)

    @task_step(
        name="handle_test_mode",
        description="Handle soil moisture task test mode",
        timeout_seconds=DATA_PREP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=30
    )
    async def _handle_test_mode(self, validator, current_time):
        """Handle test mode execution with timeout handling."""
        try:
            logger.info("Running in test mode - bypassing window checks")
            target_smap_time = self.get_smap_time_for_validator(current_time)
            ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)

            await self._prepare_daily_regions(target_smap_time, ifs_forecast_time)

            regions = await self.db_manager.fetch_many(
                """
                SELECT * FROM soil_moisture_regions 
                WHERE status = 'pending'
                AND target_time = :target_time
                """,
                {"target_time": target_smap_time},
            )

            if regions:
                await self._process_regions(regions, validator, target_smap_time, current_time, ifs_forecast_time)

            logger.info("Test mode execution complete. Disabling test mode.")
            self.test_mode = False

        except Exception as e:
            logger.error(f"Error in test mode: {e}")
            logger.error(traceback.format_exc())

    @task_step(
        name="handle_normal_mode",
        description="Handle soil moisture task normal mode",
        timeout_seconds=DATA_PREP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=30
    )
    async def _handle_normal_mode(self, validator, current_time):
        """Handle normal mode execution with timeout handling."""
        try:
            windows = self.get_validator_windows()
            current_window = next(
                (w for w in windows if self.is_in_window(current_time, w)), None
            )

            if not current_window:
                logger.info(f"Not in any preparation or execution window: {current_time}")
                logger.info(f"Next soil task time: {self.get_next_preparation_time(current_time)}")
                logger.info(f"Sleeping for 60 seconds")
                await asyncio.sleep(60)
                return

            is_prep = current_window[1] == 30  # If minutes = 30, it's a prep window
            target_smap_time = self.get_smap_time_for_validator(current_time)
            ifs_forecast_time = self.get_ifs_time_for_smap(target_smap_time)

            if is_prep:
                await self._prepare_daily_regions(target_smap_time, ifs_forecast_time)
            else:
                # Execution window logic
                regions = await self.db_manager.fetch_many(
                    """
                    SELECT * FROM soil_moisture_regions
                    WHERE status = 'pending'
                    AND target_time = :target_time
                    """,
                    {"target_time": target_smap_time},
                )

                if regions:
                    await self._process_regions(regions, validator, target_smap_time, current_time, ifs_forecast_time)

            # Handle sleep until next window
            prep_windows = [w for w in self.get_validator_windows() if w[1] == 0]
            in_any_prep = any(self.is_in_window(current_time, w) for w in prep_windows)

            if not in_any_prep:
                next_prep_time = self.get_next_preparation_time(current_time)
                sleep_seconds = (next_prep_time - datetime.now(timezone.utc)).total_seconds()
                if sleep_seconds > 0:
                    logger.info(f"Sleeping until next soil task window: {next_prep_time}")
                    await asyncio.sleep(sleep_seconds)

        except Exception as e:
            logger.error(f"Error in normal mode: {e}")
            logger.error(traceback.format_exc())

    @task_step(
        name="run_scoring_cycle",
        description="Run the scoring cycle for soil moisture predictions",
        timeout_seconds=SCORING_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=15
    )
    async def _run_scoring_cycle(self):
        """Run scoring cycle with timeout and retry handling."""
        try:
            pending_tasks = await self.get_pending_tasks()
            if not pending_tasks:
                return {"status": "no_pending_tasks"}

            scoring_results = {}
            tasks_by_time = {}
            for task in pending_tasks:
                target_time = task["target_time"]
                if target_time not in tasks_by_time:
                    tasks_by_time[target_time] = []
                tasks_by_time[target_time].append(task)

            for target_time, tasks in tasks_by_time.items():
                await self._process_tasks_batch(tasks, target_time)

            return {"status": "success", "results": scoring_results}

        except Exception as e:
            logger.error(f"Error in scoring cycle: {e}")
            return {"status": "error", "message": str(e)}

    #############################################
    # 3. Time Window Management
    #############################################

    def get_validator_windows(self) -> List[Tuple[int, int, int, int]]:
        """Get validator windows for soil moisture task.
        
        Returns a list of tuples (hour_start, min_start, hour_end, min_end)
        representing the start and end times of each window.
        """
        return [
            (1, 30, 2, 0),    # Prep window for 1:30 SMAP time
            (2, 0, 2, 30),    # Execution window for 1:30 SMAP time
            (9, 30, 10, 0),   # Prep window for 7:30 SMAP time
            (10, 0, 10, 30),  # Execution window for 7:30 SMAP time
            (13, 30, 14, 0),  # Prep window for 13:30 SMAP time
            (14, 0, 14, 30),  # Execution window for 13:30 SMAP time
            (19, 30, 20, 0),  # Prep window for 19:30 SMAP time
            (20, 0, 20, 30),  # Execution window for 19:30 SMAP time
        ]

    def is_in_window(self, current_time: datetime, window: Tuple[int, int, int, int]) -> bool:
        """Check if current time is within a specific window."""
        start_hr, start_min, end_hr, end_min = window
        current_mins = current_time.hour * 60 + current_time.minute
        window_start_mins = start_hr * 60 + start_min
        window_end_mins = end_hr * 60 + end_min
        return window_start_mins <= current_mins < window_end_mins

    def get_next_preparation_time(self, current_time: datetime) -> datetime:
        """Get the next preparation window start time."""
        windows = self.get_validator_windows()
        current_mins = current_time.hour * 60 + current_time.minute

        for start_hr, start_min, _, _ in windows:
            window_start_mins = start_hr * 60 + start_min
            if window_start_mins > current_mins:
                return current_time.replace(
                    hour=start_hr, minute=start_min, second=0, microsecond=0
                )

        tomorrow = current_time + timedelta(days=1)
        first_window = windows[0]
        return tomorrow.replace(
            hour=first_window[0], minute=first_window[1], second=0, microsecond=0
        )

    def get_smap_time_for_validator(self, current_time: datetime) -> datetime:
        """Get SMAP time based on validator execution time."""
        if self.test_mode:
            smap_hours = [1, 7, 13, 19]
            current_hour = current_time.hour
            closest_hour = min(smap_hours, key=lambda x: abs(x - current_hour))
            return current_time.replace(
                hour=7, minute=30, second=0, microsecond=0
            )

        validator_to_smap = {
            1: 1,    # 1:30 prep → 1:30 SMAP
            2: 1,    # 2:00 execution → 1:30 SMAP
            9: 7,    # 9:30 prep → 7:30 SMAP
            10: 7,   # 10:00 execution → 7:30 SMAP
            13: 13,  # 13:30 prep → 13:30 SMAP
            14: 13,  # 14:00 execution → 13:30 SMAP
            19: 19,  # 19:30 prep → 19:30 SMAP
            20: 19,  # 20:00 execution → 19:30 SMAP
        }
        smap_hour = validator_to_smap.get(current_time.hour)
        if smap_hour is None:
            raise ValueError(f"No SMAP time mapping for validator hour {current_time.hour}")

        return current_time.replace(hour=smap_hour, minute=30, second=0, microsecond=0)

    def get_ifs_time_for_smap(self, smap_time: datetime) -> datetime:
        """Get corresponding IFS forecast time for SMAP target time."""
        smap_to_ifs = {
            1: 0,  # 01:30 uses 00:00 forecast
            7: 6,  # 07:30 uses 06:00 forecast
            13: 12,  # 13:30 uses 12:00 forecast
            19: 18,  # 19:30 uses 18:00 forecast
        }

        ifs_hour = smap_to_ifs.get(smap_time.hour)
        if ifs_hour is None:
            raise ValueError(f"Invalid SMAP time: {smap_time.hour}:30")

        return smap_time.replace(hour=ifs_hour, minute=0, second=0, microsecond=0)

    #############################################
    # 4. Region Processing
    #############################################

    @task_step(
        name="prepare_daily_regions",
        description="Prepare daily regions for soil moisture prediction",
        timeout_seconds=DATA_PREP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=30
    )
    async def _prepare_daily_regions(self, target_time, ifs_forecast_time):
        """Prepare daily regions with timeout handling."""
        try:
            if self.validator_preprocessing is None:
                logger.error("Validator preprocessing not initialized")
                return None
            return await self.validator_preprocessing.get_daily_regions(
                target_time=target_time,
                ifs_forecast_time=ifs_forecast_time,
            )
        except Exception as e:
            logger.error(f"Error preparing daily regions: {e}")
            logger.error(traceback.format_exc())
            return None

    @task_step(
        name="process_regions",
        description="Process soil moisture regions",
        timeout_seconds=DATA_PREP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=30
    )
    async def _process_regions(self, regions, validator, target_smap_time, current_time, ifs_forecast_time):
        """Process regions with timeout handling."""
        for region in regions:
            try:
                logger.info(f"Processing region {region['id']}")

                # Validate region data
                if not await self._validate_region_data(region):
                    continue

                # Prepare and encode data
                combined_data = region["combined_data"]
                encoded_data = base64.b64encode(combined_data)
                
                task_data = {
                    "region_id": region["id"],
                    "combined_data": encoded_data.decode("ascii"),
                    "sentinel_bounds": region["sentinel_bounds"],
                    "sentinel_crs": region["sentinel_crs"],
                    "target_time": target_smap_time.isoformat(),
                }

                payload = {"nonce": str(uuid4()), "data": task_data}

                # Query miners
                logger.info(f"Sending region {region['id']} to miners...")
                responses = await self._query_miners(validator, payload, region["id"])

                if responses:
                    metadata = {
                        "region_id": region["id"],
                        "target_time": target_smap_time,
                        "data_collection_time": current_time,
                        "ifs_forecast_time": ifs_forecast_time,
                    }
                    await self.add_task_to_queue(responses, metadata)

                    await self.db_manager.execute(
                        """
                        UPDATE soil_moisture_regions 
                        SET status = 'sent_to_miners'
                        WHERE id = :region_id
                        """,
                        {"region_id": region["id"]},
                    )

            except Exception as e:
                logger.error(f"Error processing region {region['id']}: {str(e)}")
                logger.error(traceback.format_exc())
                continue

    @task_step(
        name="validate_region_data",
        description="Validate region data",
        timeout_seconds=30,
        max_retries=1,
        retry_delay_seconds=5
    )
    async def _validate_region_data(self, region):
        """Validate region data with timeout handling."""
        try:
            if "combined_data" not in region:
                logger.error(f"Region {region['id']} missing combined_data field")
                return False

            if not region["combined_data"]:
                logger.error(f"Region {region['id']} has null combined_data")
                return False

            combined_data = region["combined_data"]
            if not isinstance(combined_data, bytes):
                logger.error(f"Region {region['id']} has invalid data type: {type(combined_data)}")
                return False

            if not (combined_data.startswith(b"II\x2A\x00") or combined_data.startswith(b"MM\x00\x2A")):
                logger.error(f"Region {region['id']} has invalid TIFF header")
                logger.error(f"First 16 bytes: {combined_data[:16].hex()}")
                return False

            logger.info(f"Region {region['id']} TIFF size: {len(combined_data) / (1024 * 1024):.2f} MB")
            logger.info(f"Region {region['id']} TIFF header: {combined_data[:4]}")
            logger.info(f"Region {region['id']} TIFF header hex: {combined_data[:16].hex()}")

            return True

        except Exception as e:
            logger.error(f"Error validating region data: {e}")
            logger.error(traceback.format_exc())
            return False

    async def get_todays_regions(self, target_time: datetime) -> List[Dict]:
        """Get regions already selected for today."""
        try:
            async with self.db_manager.get_connection() as conn:
                regions = await conn.fetch(
                    """
                    SELECT * FROM soil_moisture_regions
                    WHERE region_date = $1::date
                    AND status = 'pending'
                """,
                    target_time.date(),
                )
                return regions
        except Exception as e:
            logger.error(f"Error getting today's regions: {str(e)}")
            return []

    #############################################
    # 5. Task Queue Management
    #############################################

    @task_step(
        name="add_task_to_queue",
        description="Add task to queue",
        timeout_seconds=DB_OP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def add_task_to_queue(self, responses: Dict, metadata: Dict) -> None:
        """Add task to queue with timeout handling."""
        try:
            task_id = str(uuid4())
            predictions = []
            
            for hotkey, response in responses.items():
                try:
                    # Parse response
                    if isinstance(response, dict) and "text" in response:
                        prediction_data = json.loads(response["text"])
                    else:
                        logger.warning(f"Invalid response format from {hotkey}")
                        continue

                    # Get miner UID
                    miner_uid = None
                    query = "SELECT uid FROM node_table WHERE hotkey = :hotkey"
                    result = await self.db_manager.fetch_one(query, {"hotkey": hotkey})
                    if result:
                        miner_uid = result["uid"]
                    else:
                        logger.warning(f"No UID found for hotkey {hotkey}")
                        continue

                    predictions.append({
                        "miner_id": miner_uid,
                        "miner_hotkey": hotkey,
                        "prediction": prediction_data.get("prediction"),
                        "confidence": prediction_data.get("confidence", 0.0),
                    })

                except Exception as e:
                    logger.error(f"Error processing response from {hotkey}: {e}")
                    continue

            if predictions:
                # Insert task into database
                query = """
                INSERT INTO soil_moisture_predictions 
                (id, region_id, target_time, data_collection_time, ifs_forecast_time, predictions, status)
                VALUES (:id, :region_id, :target_time, :data_collection_time, :ifs_forecast_time, :predictions, :status)
                """
                params = {
                    "id": task_id,
                    "region_id": metadata["region_id"],
                    "target_time": metadata["target_time"],
                    "data_collection_time": metadata["data_collection_time"],
                    "ifs_forecast_time": metadata["ifs_forecast_time"],
                    "predictions": json.dumps(predictions),
                    "status": "pending"
                }
                await self.db_manager.execute(query, params)
                logger.info(f"Added task {task_id} to queue with {len(predictions)} predictions")

        except Exception as e:
            logger.error(f"Error adding task to queue: {e}")
            logger.error(traceback.format_exc())

    @task_step(
        name="get_pending_tasks",
        description="Get pending tasks",
        timeout_seconds=DB_OP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def get_pending_tasks(self) -> List[Dict]:
        """Get pending tasks with timeout handling."""
        try:
            query = """
            SELECT * FROM soil_moisture_predictions 
            WHERE status = 'pending'
            AND (retry_count < 3 OR retry_count IS NULL)
            AND (next_retry_time IS NULL OR next_retry_time <= CURRENT_TIMESTAMP)
            """
            tasks = await self.db_manager.fetch_many(query)
            
            if tasks:
                logger.info(f"Found {len(tasks)} pending tasks")
                return tasks
            else:
                logger.info("No pending tasks found")
                return []

        except Exception as e:
            logger.error(f"Error getting pending tasks: {e}")
            logger.error(traceback.format_exc())
            return []

    @task_step(
        name="query_miners",
        description="Query miners for predictions",
        timeout_seconds=MINER_QUERY_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=10
    )
    async def _query_miners(self, validator, payload, region_id):
        """Query miners with timeout handling."""
        try:
            responses = await validator.query_miners(
                payload=payload,
                endpoint="/soilmoisture-request"
            )
            
            if responses:
                logger.info(f"Received {len(responses)} responses for region {region_id}")
                return responses
            else:
                logger.warning(f"No responses received for region {region_id}")
                return None

        except Exception as e:
            logger.error(f"Error querying miners: {e}")
            logger.error(traceback.format_exc())
            return None

    #############################################
    # 6. Scoring & History
    #############################################

    @task_timer
    def validator_score(self, result: Optional[Union[Dict[str, Any], Inputs]] = None) -> Optional[Dict[str, Any]]:
        """
        Score the task results using the scoring mechanism.
        
        Args:
            result: The result to score, either as a dictionary or Inputs object
            
        Returns:
            The score for the result
        """
        try:
            if self.scoring_mechanism is None:
                logger.error("Scoring mechanism not initialized")
                return None
                
            # Convert dictionary input to Inputs type if needed
            if isinstance(result, dict):
                inputs = SoilMoistureInputs()
                inputs.inputs = result
                result = inputs
                
            if not isinstance(result, Inputs):
                logger.error(f"Input data must be of type Inputs or Dict, got {type(result)}")
                return None
                
            return self.scoring_mechanism.score(result)
            
        except Exception as e:
            logger.error(f"Error in validator_score: {e}")
            logger.error(traceback.format_exc())
            return None

    @task_timer
    def miner_preprocess(self, preprocessing: Optional[Preprocessing] = None, inputs: Optional[Union[Dict[str, Any], Inputs]] = None) -> Optional[Union[Dict[str, Any], Inputs]]:
        """
        Preprocess the inputs for the miner neuron.
        
        Args:
            preprocessing: Optional preprocessing component
            inputs: Optional inputs to preprocess, either as a dictionary or Inputs object
            
        Returns:
            Preprocessed inputs, either as a dictionary or Inputs object
        """
        try:
            if preprocessing is None:
                preprocessing = self.miner_preprocessing
                
            if preprocessing is None:
                logger.error("No preprocessing component available")
                return None
                
            if inputs is None:
                logger.error("No inputs provided")
                return None

            # Process the inputs using the preprocessing component
            return preprocessing.process_miner_data(inputs)
            
        except Exception as e:
            logger.error(f"Error in miner_preprocess: {e}")
            logger.error(traceback.format_exc())
            return None

    @task_timer
    def miner_execute(self, inputs: Optional[Union[Dict[str, Any], Inputs]] = None) -> Optional[Outputs]:
        """
        Execute the task from the miner neuron.
        
        Args:
            inputs: Optional inputs to process, either as a dictionary or Inputs object
            
        Returns:
            Task outputs
        """
        try:
            if inputs is None:
                logger.error("No inputs provided")
                return None
                
            if self.model is None:
                logger.error("No model available")
                return None
                
            # Convert dictionary input to Inputs type if needed
            if isinstance(inputs, dict):
                inputs_obj = SoilMoistureInputs()
                inputs_obj.inputs = inputs
                inputs = inputs_obj
                
            if not isinstance(inputs, Inputs):
                logger.error(f"Input data must be of type Inputs or Dict, got {type(inputs)}")
                return None
                
            return self.model.predict(inputs)
            
        except Exception as e:
            logger.error(f"Error in miner_execute: {e}")
            logger.error(traceback.format_exc())
            return None

    @task_timer
    def validator_prepare_subtasks(self, data: Optional[Union[Dict[str, Any], Inputs]] = None) -> List[Dict[str, Any]]:
        """
        Prepare subtasks for validation.
        
        Args:
            data: The data to prepare subtasks from, either as a dictionary or Inputs object
            
        Returns:
            List of prepared subtasks
        """
        try:
            if data is None:
                logger.error("No data provided")
                return []
                
            # Convert dictionary input to Inputs type if needed
            if isinstance(data, dict):
                inputs = SoilMoistureInputs()
                inputs.inputs = data
                data = inputs
                
            if not isinstance(data, Inputs):
                logger.error(f"Input data must be of type Inputs or Dict, got {type(data)}")
                return []
                
            data_dict = data.inputs
            if not isinstance(data_dict, dict):
                logger.error("Data must be a dictionary")
                return []
                
            # Extract region data
            region_id = data_dict.get("region_id")
            if region_id is None:
                logger.error("No region_id in data")
                return []
                
            # Create subtask for the region
            subtask = {
                "region_id": region_id,
                "bounds": data_dict.get("sentinel_bounds"),
                "crs": data_dict.get("sentinel_crs"),
                "target_time": data_dict.get("target_time"),
                "combined_data": data_dict.get("combined_data")
            }
            
            return [subtask]
            
        except Exception as e:
            logger.error(f"Error in validator_prepare_subtasks: {e}")
            logger.error(traceback.format_exc())
            return []

    @task_step(
        name="process_tasks_batch",
        description="Process a batch of tasks for scoring",
        timeout_seconds=SCORING_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=15
    )
    async def _process_tasks_batch(self, tasks: List[Dict[str, Any]], target_time: datetime) -> None:
        """Process a batch of tasks with timeout handling."""
        try:
            scored_tasks = []
            for task in tasks:
                try:
                    scores = {}
                    for prediction in task["predictions"]:
                        # Create prediction data as Inputs object
                        pred_data = SoilMoistureInputs()
                        pred_data.inputs = {
                            "bounds": task["sentinel_bounds"],
                            "crs": task["sentinel_crs"],
                            "predictions": prediction,
                            "target_time": target_time,
                            "region": {"id": task["id"]},
                            "miner_id": prediction["miner_id"],
                            "miner_hotkey": prediction["miner_hotkey"]
                        }
                        
                        if self.scoring_mechanism is None:
                            logger.error("Scoring mechanism not initialized")
                            continue
                            
                        # Get the score using the scoring mechanism
                        try:
                            score = await self.scoring_mechanism.score(pred_data)
                            if score:
                                scores = score
                                task["score"] = score
                                scored_tasks.append(task)
                        except Exception as e:
                            logger.error(f"Error in scoring mechanism: {e}")
                            continue

                except Exception as e:
                    logger.error(f"Error processing task: {e}")
                    logger.error(traceback.format_exc())
                    continue

            if scored_tasks:
                await self._handle_scored_tasks(scored_tasks, target_time)

        except Exception as e:
            logger.error(f"Error processing tasks batch: {e}")
            logger.error(traceback.format_exc())

    @task_step(
        name="handle_scored_tasks",
        description="Handle scored tasks",
        timeout_seconds=DB_OP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _handle_scored_tasks(self, scored_tasks: List[Dict[str, Any]], target_time: datetime) -> None:
        """Handle scored tasks with timeout handling."""
        try:
            for task in scored_tasks:
                try:
                    # Convert task data to Inputs object
                    task_data = SoilMoistureInputs()
                    task_data.inputs = {
                        "task_id": task["id"],
                        "region_id": task["region_id"],
                        "target_time": target_time,
                        "score": task["score"],
                        "predictions": task["predictions"]
                    }
                    
                    # Update task status in database
                    await self.db_manager.execute(
                        """
                        UPDATE soil_moisture_predictions 
                        SET status = 'scored', 
                            score = :score,
                            scored_at = CURRENT_TIMESTAMP
                        WHERE id = :task_id
                        """,
                        {
                            "task_id": task["id"],
                            "score": json.dumps(task["score"])
                        }
                    )
                    
                    logger.info(f"Updated task {task['id']} with score")
                    
                except Exception as e:
                    logger.error(f"Error handling scored task: {e}")
                    logger.error(traceback.format_exc())
                    continue
                    
        except Exception as e:
            logger.error(f"Error handling scored tasks: {e}")
            logger.error(traceback.format_exc())

    @task_step(
        name="build_score_row",
        description="Build score row",
        timeout_seconds=DB_OP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _build_score_row(self, target_time: datetime, scored_tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Build score row with timeout handling."""
        try:
            score_rows = []
            for task in scored_tasks:
                try:
                    # Convert task data to Inputs object
                    task_data = SoilMoistureInputs()
                    task_data.inputs = {
                        "task_id": task["id"],
                        "task_name": "SoilMoistureTask",
                        "target_time": target_time,
                        "score": task["score"],
                        "status": "scored"
                    }
                    
                    score_rows.append({
                        "task_name": "SoilMoistureTask",
                        "task_id": task["id"],
                        "score": json.dumps(task["score"]),
                        "status": "scored"
                    })
                    
                except Exception as e:
                    logger.error(f"Error building score row: {e}")
                    logger.error(traceback.format_exc())
                    continue
                    
            return score_rows
            
        except Exception as e:
            logger.error(f"Error building score rows: {e}")
            logger.error(traceback.format_exc())
            return []

    @task_step(
        name="move_task_to_history",
        description="Move task to history",
        timeout_seconds=DB_OP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _move_task_to_history(self, region: Dict[str, Any], predictions: List[Dict[str, Any]], ground_truth: Dict[str, Any], scores: Dict[str, Any]) -> None:
        """Move task to history with timeout handling."""
        try:
            # Convert task data to Inputs object
            task_data = SoilMoistureInputs()
            task_data.inputs = {
                "region_id": region["id"],
                "predictions": predictions,
                "ground_truth": ground_truth,
                "scores": scores,
                "target_time": region["target_time"],
                "data_collection_time": region["data_collection_time"],
                "ifs_forecast_time": region["ifs_forecast_time"]
            }
            
            # Insert into history table
            query = """
            INSERT INTO soil_moisture_history 
            (region_id, predictions, ground_truth, scores, target_time, data_collection_time, ifs_forecast_time)
            VALUES (:region_id, :predictions, :ground_truth, :scores, :target_time, :data_collection_time, :ifs_forecast_time)
            """
            params = {
                "region_id": region["id"],
                "predictions": json.dumps(predictions),
                "ground_truth": json.dumps(ground_truth),
                "scores": json.dumps(scores),
                "target_time": region["target_time"],
                "data_collection_time": region["data_collection_time"],
                "ifs_forecast_time": region["ifs_forecast_time"]
            }
            await self.db_manager.execute(query, params)
            
            # Update task status
            await self.db_manager.execute(
                """
                UPDATE soil_moisture_predictions 
                SET status = 'archived'
                WHERE region_id = :region_id
                """,
                {"region_id": region["id"]}
            )
            
            logger.info(f"Moved task for region {region['id']} to history")
            
        except Exception as e:
            logger.error(f"Error moving task to history: {e}")
            logger.error(traceback.format_exc())

    #############################################
    # 7. Model Operations
    #############################################

    def run_model_inference(self, processed_data: Optional[Union[Dict[str, Any], Inputs]] = None) -> Optional[Dict[str, Any]]:
        """
        Run model inference on processed data.
        
        Args:
            processed_data: The processed data to run inference on, either as a dictionary or Inputs object
            
        Returns:
            The model's predictions
        """
        if not self.model:
            raise RuntimeError(
                "Model not initialized. Are you running on a miner node?"
            )

        if not self.miner_preprocessing:
            raise RuntimeError("Miner preprocessing not initialized")
            
        # Convert dictionary input to Inputs type if needed
        if isinstance(processed_data, dict):
            inputs = SoilMoistureInputs()
            inputs.inputs = processed_data
            processed_data = inputs
            
        if not isinstance(processed_data, Inputs):
            logger.error(f"Input data must be of type Inputs or Dict, got {type(processed_data)}")
            return None
            
        return self.miner_preprocessing.predict_smap(processed_data, self.model)

    #############################################
    # 8. Cleanup & Utilities
    #############################################

    @task_step(
        name="cleanup_files",
        description="Clean up temporary files",
        timeout_seconds=30,
        max_retries=1,
        retry_delay_seconds=5
    )
    async def _cleanup_files(self):
        """Clean up temporary files with timeout handling."""
        try:
            temp_dir = tempfile.gettempdir()
            pattern = os.path.join(temp_dir, "soil_moisture_*.tif")
            
            for file_path in glob.glob(pattern):
                try:
                    os.remove(file_path)
                    logger.info(f"Removed temporary file: {file_path}")
                except Exception as e:
                    logger.error(f"Error removing file {file_path}: {e}")

        except Exception as e:
            logger.error(f"Error in cleanup: {e}")
            logger.error(traceback.format_exc())
