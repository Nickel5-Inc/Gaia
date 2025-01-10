"""
GeomagneticTask implementation for processing and analyzing geomagnetic data.

This task involves:
    - Querying miners for predictions
    - Adding predictions to a queue for scoring
    - Fetching ground truth data
    - Scoring predictions
    - Moving scored tasks to history
"""

#############################################
# Standard Library Imports
#############################################
import asyncio
import datetime
import importlib.util
import json
import os
import traceback
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4
import uuid
from datetime import datetime, timezone, timedelta
from sqlalchemy import text

#############################################
# Third Party Imports
#############################################
import numpy as np
import pandas as pd
import torch
from pydantic import Field

#############################################
# Gaia Imports
#############################################
from fiber.logging_utils import get_logger
from gaia.tasks.base.task import Task
from gaia.models.geomag_basemodel import GeoMagBaseModel
from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.validator.utils.task_decorator import task_step

# Task-specific imports
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_metadata import (
    GeomagneticMetadata,
)
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_inputs import GeomagneticInputs
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import (
    GeomagneticPreprocessing,
)
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_scoring_mechanism import (
    GeomagneticScoringMechanism,
)
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_outputs import GeomagneticOutputs
from gaia.tasks.defined_tasks.geomagnetic.utils.process_geomag_data import (
    get_latest_geomag_data,
)

#############################################
# Constants
#############################################
# Timeout constants
VALIDATOR_CYCLE_TIMEOUT = 3600  # 1 hour
SCORING_TIMEOUT = 300  # 5 minutes
DATA_FETCH_TIMEOUT = 180  # 3 minutes
MINER_QUERY_TIMEOUT = 180  # 3 minutes
DB_OP_TIMEOUT = 60  # 1 minute

logger = get_logger(__name__)

class GeomagneticTask(Task):
    """
    A task class for processing and analyzing geomagnetic data.
    
    The task is organized into several logical sections:
    1. Class Setup & Initialization
       - Class initialization
       - Field definitions
       - Component initialization
    
    2. Main Execution Flow
       - Validator execution cycle
       - Data fetching
       - Miner querying
       - Score processing
    
    3. Data Management
       - Ground truth fetching
       - Response processing
       - Queue management
    
    4. Scoring & History
       - Task scoring
       - History management
       - Score calculation
       - Score aggregation
    
    5. Task Queue Management
       - Task retrieval
       - Task addition
       - Queue status updates
    
    6. Model Operations
       - Data preprocessing
       - Subtask preparation
       - Score calculation
    """

    #############################################
    # 1. Class Setup & Initialization
    #############################################

    # Declare Pydantic fields
    db_manager: ValidatorDatabaseManager = Field(
        default_factory=ValidatorDatabaseManager,
        description="Database manager for the task",
    )
    miner_preprocessing: GeomagneticPreprocessing = Field(
        default_factory=GeomagneticPreprocessing,
        description="Preprocessing component for miner",
    )
    model: GeoMagBaseModel = Field(
        default_factory=GeoMagBaseModel, description="The geomagnetic prediction model"
    )

    def __init__(self, db_manager=None, **data):
        """Initialize the task."""
        # Initialize task-specific components first
        self.db_manager = db_manager or ValidatorDatabaseManager()
        self.model = GeoMagBaseModel()
        self.scoring_mechanism = GeomagneticScoringMechanism(db_manager=self.db_manager)

        # Initialize base Task class
        super().__init__(
            name="GeomagneticTask",
            description="Geomagnetic prediction task",
            task_type="atomic",
            metadata=GeomagneticMetadata(),
            inputs=GeomagneticInputs(),
            outputs=GeomagneticOutputs(),
            **data,
        )

        # Try to load custom model first
        try:
            custom_model_path = "gaia/models/custom_models/custom_geomagnetic_model.py"
            if os.path.exists(custom_model_path):
                spec = importlib.util.spec_from_file_location(
                    "custom_geomagnetic_model", custom_model_path
                )
                if spec is not None and spec.loader is not None:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    self.model = module.CustomGeomagneticModel()
                    logger.info("Successfully loaded custom geomagnetic model")
                else:
                    logger.warning("Could not load custom model spec, falling back to base model")
                    self.model = GeoMagBaseModel()
            else:
                # Fall back to base model
                self.model = GeoMagBaseModel()
                logger.info("No custom model found, using base model")
        except Exception as e:
            logger.warning(f"Error loading custom model: {e}, falling back to base model")
            self.model = GeoMagBaseModel()

    #############################################
    # 2. Main Execution Flow
    #############################################

    @task_step(
        name="validator_execute_cycle",
        description="Execute main validator cycle for geomagnetic task",
        timeout_seconds=VALIDATOR_CYCLE_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=30
    )
    async def validator_execute(self, validator):
        """Execute validator workflow with proper timeout handling."""
        while True:
            try:
                # Step 1: Align to the top of the next hour
                current_time = datetime.now(timezone.utc)
                next_hour = current_time.replace(
                    minute=0, second=0, microsecond=0
                ) + timedelta(hours=1)
                sleep_duration = (next_hour - current_time).total_seconds()

                logger.info(
                    f"Sleeping until the next top of the hour: {next_hour.isoformat()} (in {sleep_duration} seconds)"
                )
                await asyncio.sleep(sleep_duration)

                logger.info("Starting GeomagneticTask execution...")

                # Step 2: Fetch Latest Geomagnetic Data
                timestamp, dst_value, historical_data = await self._fetch_geomag_data()

                # Step 3: Query Miners
                current_hour_start = next_hour - timedelta(hours=1)
                await self._query_miners(
                    validator, timestamp, dst_value, historical_data, current_hour_start
                )

                # Step 4: Process Scores
                await self._process_scores(validator, current_hour_start, next_hour)

            except Exception as e:
                logger.error(f"Unexpected error in validator_execute loop: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(3600)

    @task_step(
        name="fetch_geomag_data",
        description="Fetch latest geomagnetic data",
        timeout_seconds=DATA_FETCH_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=10
    )
    async def _fetch_geomag_data(self):
        """Fetch latest geomagnetic data with timeout handling."""
        try:
            logger.info("Fetching latest geomagnetic data...")
            result = await get_latest_geomag_data(include_historical=True)
            
            # Handle error case
            if len(result) == 3 and result[0] == "N/A":
                logger.warning("Failed to fetch geomagnetic data")
                return "N/A", "N/A", None
                
            # Handle successful case with historical data
            if len(result) == 3:
                timestamp, dst_value, historical_data = result
                logger.info(f"Fetched latest geomagnetic data: timestamp={timestamp}, value={dst_value}")
                if historical_data is not None:
                    logger.info(f"Fetched historical data for the current month: {len(historical_data)} records")
                else:
                    logger.warning("No historical data available for the current month.")
                return timestamp, dst_value, historical_data
                
            # Handle case without historical data
            timestamp, dst_value = result
            logger.info(f"Fetched latest geomagnetic data: timestamp={timestamp}, value={dst_value}")
            return timestamp, dst_value, None
            
        except Exception as e:
            logger.error(f"Error fetching geomagnetic data: {e}")
            logger.error(traceback.format_exc())
            return "N/A", "N/A", None

    @task_step(
        name="query_miners",
        description="Query miners for predictions",
        timeout_seconds=MINER_QUERY_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=10
    )
    async def _query_miners(
        self, validator, timestamp, dst_value, historical_data, current_hour_start
    ):
        """Query miners with timeout handling."""
        try:
            if timestamp == "N/A" or dst_value == "N/A":
                logger.warning("Invalid geomagnetic data. Skipping miner queries.")
                return

            # Construct Payload for Miners
            nonce = str(uuid4())

            # Convert historical data to serializable format
            historical_records = []
            if historical_data is not None:
                for _, row in historical_data.iterrows():
                    historical_records.append(
                        {"timestamp": row["timestamp"].isoformat(), "Dst": row["Dst"]}
                    )

            payload_template = {
                "nonce": nonce,
                "data": {
                    "name": "Geomagnetic Data",
                    "timestamp": timestamp.isoformat(),
                    "value": dst_value,
                    "historical_values": historical_records,
                },
            }
            endpoint = "/geomagnetic-request"

            logger.info(f"Querying miners for geomagnetic predictions")
            responses = await validator.query_miners(payload_template, endpoint)
            logger.info(f"Collected responses from miners: {len(responses)}")

            await self.process_miner_responses(responses, current_hour_start, validator)
            logger.info(f"Added {len(responses)} predictions to the database")

        except Exception as e:
            logger.error(f"Error querying miners: {e}")
            logger.error(traceback.format_exc())

    @task_step(
        name="process_scores",
        description="Process and archive scores",
        timeout_seconds=SCORING_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=15
    )
    async def _process_scores(self, validator, current_hour_start, next_hour):
        """Process and archive scores with timeout handling."""
        try:
            # Fetch Ground Truth
            ground_truth_value = await self.fetch_ground_truth()
            if ground_truth_value is None:
                logger.warning("Ground truth data not available. Skipping scoring.")
                return

            # Score Predictions and Archive Results
            last_hour_start = current_hour_start
            last_hour_end = next_hour.replace(minute=0, second=0, microsecond=0)
            current_time = datetime.now(timezone.utc)

            logger.info(
                f"Fetching predictions between {last_hour_start} and {last_hour_end}"
            )
            tasks = await self.get_tasks_for_hour(last_hour_start, last_hour_end, validator)
            await self.score_tasks(tasks, ground_truth_value, current_time)

        except Exception as e:
            logger.error(f"Error processing scores: {e}")
            logger.error(traceback.format_exc())

    #############################################
    # 3. Data Management
    #############################################

    @task_step(
        name="fetch_ground_truth",
        description="Fetch ground truth DST value",
        timeout_seconds=DATA_FETCH_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=10
    )
    async def fetch_ground_truth(self):
        """Fetch ground truth with timeout handling."""
        try:
            current_time = datetime.now(timezone.utc)
            logger.info(f"Fetching ground truth for UTC hour: {current_time.hour}")

            result = await get_latest_geomag_data(include_historical=False)
            
            # Handle error case
            if len(result) >= 2 and result[0] == "N/A":
                logger.warning("No ground truth data available for the current hour.")
                return None
                
            # Extract timestamp and dst_value regardless of whether historical data is included
            timestamp, dst_value = result[:2]
            
            if timestamp == "N/A" or dst_value == "N/A":
                logger.warning("No ground truth data available for the current hour.")
                return None

            logger.info(f"Ground truth value for hour {current_time.hour}: {dst_value}")
            return dst_value

        except Exception as e:
            logger.error(f"Error fetching ground truth: {e}")
            logger.error(traceback.format_exc())
            return None

    async def process_miner_responses(self, responses: List[Dict[str, Any]], current_hour_start: datetime, validator) -> None:
        """Process miner responses and add them to the database."""
        try:
            async with self.db_manager.get_session() as session:
                for response in responses:
                    miner_hotkey = response["miner_hotkey"]
                    predicted_value = response["predicted_values"]
                    query_time = response["query_time"]

                    # Insert into predictions table
                    query = text("""
                        INSERT INTO geomagnetic_predictions 
                        (id, miner_uid, miner_hotkey, predicted_values, query_time, status)
                        VALUES (:id, :miner_uid, :miner_hotkey, :predicted_values, :query_time, :status)
                    """)
                    
                    params = {
                        "id": str(uuid.uuid4()),
                        "miner_uid": response["miner_uid"],
                        "miner_hotkey": miner_hotkey,
                        "predicted_values": predicted_value,
                        "query_time": query_time,
                        "status": "pending",
                    }
                    
                    await session.execute(query, params)
                    await session.commit()
                    logger.info(
                        f"Added prediction to queue: miner={miner_hotkey}, value={predicted_value}"
                    )

        except Exception as e:
            logger.error(f"Error processing miner responses: {e}")
            logger.error(traceback.format_exc())

    #############################################
    # 4. Scoring & History
    #############################################

    @task_step(
        name="score_tasks",
        description="Score tasks and update database",
        timeout_seconds=SCORING_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=15
    )
    async def score_tasks(self, tasks, ground_truth_value, current_time):
        """Score tasks with timeout handling."""
        if tasks:
            scored_tasks = []
            for task in tasks:
                try:
                    predicted_value = task["predicted_values"]
                    if self.scoring_mechanism is not None:
                        score = self.calculate_score(predicted_value, ground_truth_value)
                    else:
                        logger.error("Scoring mechanism not initialized")
                        score = 0.0
                        
                    await self.move_task_to_history(
                        task, ground_truth_value, score, current_time
                    )
                    task["score"] = score
                    scored_tasks.append(task)
                    logger.info(
                        f"Task scored and archived: task_id={task['id']}, score={score}"
                    )
                except Exception as e:
                    logger.error(f"Error processing task {task['id']}: {e}")
                    logger.error(traceback.format_exc())

            current_hour = datetime.now(timezone.utc).hour
            await self.build_score_row(current_hour, scored_tasks)
        else:
            logger.info("No predictions to score for the last hour.")

    @task_step(
        name="move_task_to_history",
        description="Archive completed task in history",
        timeout_seconds=DB_OP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def move_task_to_history(
        self, task: Dict[str, Any], ground_truth_value: float, score: float, score_time: datetime
    ) -> None:
        """Archive task with timeout handling."""
        try:
            async with self.db_manager.get_session() as session:
                # Insert into history table
                query = text("""
                    INSERT INTO geomagnetic_history 
                    (miner_uid, miner_hotkey, query_time, predicted_value, ground_truth_value, score, scored_at)
                    VALUES (:miner_uid, :miner_hotkey, :query_time, :predicted_value, :ground_truth_value, :score, :scored_at)
                """)

                params = {
                    "miner_uid": task["miner_uid"],
                    "miner_hotkey": task["miner_hotkey"],
                    "query_time": task["query_time"],
                    "predicted_value": task["predicted_values"],
                    "ground_truth_value": ground_truth_value,
                    "score": score,
                    "scored_at": score_time,
                }

                await session.execute(query, params)

                # Remove from predictions table
                delete_query = text("""
                    DELETE FROM geomagnetic_predictions 
                    WHERE id = :task_id
                """)
                await session.execute(delete_query, {"task_id": task["id"]})
                await session.commit()
                
                logger.info(f"Archived task to history and removed from predictions: {task['id']}")

        except Exception as e:
            logger.error(f"Error moving task to history: {e}")
            logger.error(traceback.format_exc())
            raise

    @task_step(
        name="build_score_row",
        description="Build score row from tasks",
        timeout_seconds=DB_OP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def build_score_row(self, current_hour, recent_tasks=None):
        """Build score row with timeout handling."""
        try:
            # Convert current_hour to datetime if it's an integer
            if isinstance(current_hour, int):
                current_time = datetime.now(timezone.utc)
                current_datetime = current_time.replace(
                    hour=current_hour, minute=0, second=0, microsecond=0
                )
                previous_datetime = current_datetime - timedelta(hours=1)
            else:
                current_datetime = current_hour
                previous_datetime = current_datetime - timedelta(hours=1)

            # Initialize scores array
            scores = [float("nan")] * 256

            async with self.db_manager.get_session() as session:
                # Get mapping of hotkeys to UIDs from node_table
                hotkey_query = text("""
                    SELECT uid, hotkey FROM node_table 
                    WHERE hotkey IS NOT NULL
                """)
                result = await session.execute(hotkey_query)
                miner_mappings = result.fetchall()
                hotkey_to_uid = {row._mapping["hotkey"]: row._mapping["uid"] for row in miner_mappings}

                # Check historical table for any tasks in this time period
                historical_query = text("""
                    SELECT miner_hotkey, score
                    FROM geomagnetic_history
                    WHERE query_time >= :start_time 
                    AND query_time < :end_time
                """)
                result = await session.execute(
                    historical_query,
                    {"start_time": previous_datetime, "end_time": current_datetime}
                )
                historical_tasks = result.fetchall()

                # Process historical tasks
                for task in historical_tasks:
                    miner_hotkey = task._mapping["miner_hotkey"]
                    if miner_hotkey in hotkey_to_uid:
                        uid = hotkey_to_uid[miner_hotkey]
                        scores[uid] = task._mapping["score"]

                # Process recent tasks (overwrite historical scores if exists)
                if recent_tasks:
                    for task in recent_tasks:
                        miner_hotkey = task["miner_hotkey"]
                        if miner_hotkey in hotkey_to_uid:
                            uid = hotkey_to_uid[miner_hotkey]
                            scores[uid] = task.get("score", float("nan"))

                # Build final score row
                score_row = {
                    "task_name": "GeomagneticTask",
                    "task_id": str(uuid.uuid4()),
                    "score": json.dumps(scores),
                    "status": "success"
                }

                # Insert score row into database
                insert_query = text("""
                    INSERT INTO score_table (task_name, task_id, score, status)
                    VALUES (:task_name, :task_id, :score, :status)
                """)
                await session.execute(insert_query, score_row)
                await session.commit()
                logger.info(f"Stored score row for hour {current_hour}")

                return score_row

        except Exception as e:
            logger.error(f"Error building score row: {e}")
            logger.error(traceback.format_exc())
            return None

    @task_step(
        name="calculate_score",
        description="Calculate score for prediction",
        timeout_seconds=30,
        max_retries=1,
        retry_delay_seconds=5
    )
    def calculate_score(self, predicted_value: float, ground_truth_value: float) -> float:
        """Calculate score with timeout handling."""
        try:
            if not isinstance(predicted_value, (int, float)) or not isinstance(
                ground_truth_value, (int, float)
            ):
                logger.error(
                    f"Invalid value types: predicted={type(predicted_value)}, ground_truth={type(ground_truth_value)}"
                )
                return 0.0

            # Calculate absolute error
            abs_error = abs(predicted_value - ground_truth_value)
            
            # Calculate score based on error (inverse relationship)
            # Score decreases as error increases
            max_error = 100  # Maximum expected error
            score = max(0.0, 1.0 - (abs_error / max_error))
            
            logger.info(
                f"Calculated score: {score:.4f} (pred={predicted_value}, truth={ground_truth_value})"
            )
            return score

        except Exception as e:
            logger.error(f"Error calculating score: {e}")
            logger.error(traceback.format_exc())
            return 0.0

    #############################################
    # 5. Task Queue Management
    #############################################

    @task_step(
        name="get_tasks_for_hour",
        description="Get tasks for specific hour",
        timeout_seconds=DB_OP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def get_tasks_for_hour(
        self,
        start_time: datetime,
        end_time: datetime,
        validator,
    ) -> List[Dict[str, Any]]:
        """Get tasks for specific hour with timeout handling."""
        try:
            async with self.db_manager.get_session() as session:
                query = text("""
                    SELECT * FROM geomagnetic_predictions 
                    WHERE query_time >= :start_time 
                    AND query_time < :end_time
                    AND status = 'pending'
                """)
                
                result = await session.execute(
                    query,
                    {"start_time": start_time, "end_time": end_time}
                )
                tasks = result.fetchall()

                if tasks:
                    logger.info(f"Found {len(tasks)} tasks to process")
                    return [dict(row._mapping) for row in tasks]
                else:
                    logger.info("No tasks found for the specified hour")
                    return []

        except Exception as e:
            logger.error(f"Error getting tasks for hour: {e}")
            logger.error(traceback.format_exc())
            return []

    @task_step(
        name="add_prediction_to_queue",
        description="Add prediction to queue",
        timeout_seconds=DB_OP_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def add_prediction_to_queue(
        self,
        miner_uid: str,
        miner_hotkey: str,
        predicted_value: float,
        query_time: datetime,
        status: str = "pending",
    ) -> None:
        """Add prediction to queue with timeout handling."""
        try:
            async with self.db_manager.get_session() as session:
                query = text("""
                    INSERT INTO geomagnetic_predictions 
                    (id, miner_uid, miner_hotkey, predicted_values, query_time, status)
                    VALUES (:id, :miner_uid, :miner_hotkey, :predicted_values, :query_time, :status)
                """)
                params = {
                    "id": str(uuid.uuid4()),
                    "miner_uid": miner_uid,
                    "miner_hotkey": miner_hotkey,
                    "predicted_values": predicted_value,
                    "query_time": query_time,
                    "status": status,
                }
                await session.execute(query, params)
                await session.commit()
                logger.info(
                    f"Added prediction to queue: miner={miner_hotkey}, value={predicted_value}"
                )

        except Exception as e:
            logger.error(f"Error adding prediction to queue: {e}")
            logger.error(traceback.format_exc())
            raise

    #############################################
    # 6. Model Operations
    #############################################

    def miner_preprocess(self, raw_data):
        """
        Preprocess raw geomagnetic data on the miner's side.

        Args:
            raw_data (dict): Raw data received by the miner.
        Returns:
            dict: Preprocessed data ready for prediction.
        """
        try:
            processed_data = {
                "timestamp": raw_data["timestamp"],
                "value": raw_data["value"] / 100.0,  # Normalize values
            }
            return processed_data
        except Exception as e:
            logger.error(f"Error in miner_preprocess: {e}")
            return None

    def validator_prepare_subtasks(self, data):
        """
        Prepare subtasks for validation.

        Args:
            data (dict): Data received by the validator.
        Returns:
            list: List of subtasks to process.
        """
        try:
            subtasks = [
                {"timestamp": data["timestamp"], "value": value}
                for value in data["values"]
            ]
            return subtasks
        except Exception as e:
            logger.error(f"Error in validator_prepare_subtasks: {e}")
            return []

    def validator_score(self, prediction, ground_truth):
        """
        Score a miner's prediction against the ground truth.

        Args:
            prediction (float): The predicted value.
            ground_truth (float): The actual ground truth value.
        Returns:
            float: A score indicating the accuracy of the prediction.
        """
        try:
            score = abs(prediction - ground_truth)
            return score
        except Exception as e:
            print(f"Error in validator_score: {e}")
            return float("inf")

    def run_model_inference(self, processed_data):
        """
        Run the GeoMag model inference.

        Args:
            processed_data (dict): Preprocessed input data for the model with timestamp and value fields.

        Returns:
            float: Predicted value.
        """
        try:
            # Convert to DataFrame if needed
            if isinstance(processed_data, dict):
                df = pd.DataFrame({
                    "ds": [pd.to_datetime(processed_data["timestamp"])],
                    "y": [float(processed_data["value"])]
                })
            elif isinstance(processed_data, pd.DataFrame):
                df = processed_data.copy()
                if "timestamp" in df.columns and "value" in df.columns:
                    df = df.rename(columns={"timestamp": "ds", "value": "y"})
            else:
                logger.error(f"Invalid input type: {type(processed_data)}")
                return 0.0

            # Perform prediction using the model
            prediction = self.model.predict(df)

            # Handle NaN or infinite values
            if np.isnan(prediction) or np.isinf(prediction):
                logger.warning("Model returned NaN/Inf, using fallback value")
                return float(df["y"].iloc[-1])  # Use input value as fallback

            return float(prediction)  # Ensure we return a Python float

        except Exception as e:
            logger.error(f"Error during model inference: {e}")
            logger.error(traceback.format_exc())
            if isinstance(processed_data, dict):
                return float(processed_data.get("value", 0.0))
            elif isinstance(processed_data, pd.DataFrame) and len(processed_data) > 0:
                return float(processed_data["value"].iloc[-1])
            return 0.0

    def miner_execute(self, data, miner):
        """
        Executes the miner workflow:
        - Preprocesses the received data along with historical data.
        - Uses the model to make predictions.
        - Returns formatted predictions.

        Args:
            data: Raw input data received from the request.
            miner: Miner instance executing the task.

        Returns:
            dict: Prediction results formatted as per requirements.
        """
        try:
            # Extract and validate data from the request payload
            if data and data.get("data"):
                # Process current data
                input_data = pd.DataFrame(
                    {
                        "timestamp": [pd.to_datetime(data["data"]["timestamp"])],
                        "value": [float(data["data"]["value"])],
                    }
                )

                # Check and process historical data if available
                if data["data"].get("historical_values"):
                    historical_df = pd.DataFrame(data["data"]["historical_values"])
                    historical_df = historical_df.rename(
                        columns={"Dst": "value"}
                    )  # Rename Dst to value
                    historical_df["timestamp"] = pd.to_datetime(
                        historical_df["timestamp"]
                    )
                    historical_df = historical_df[
                        ["timestamp", "value"]
                    ]  # Ensure correct columns
                    combined_df = pd.concat(
                        [historical_df, input_data], ignore_index=True
                    )
                else:
                    combined_df = input_data

                # Preprocess combined data
                processed_data = self.miner_preprocessing.process_miner_data(combined_df)
            else:
                logger.error("No data provided in request")
                return None

            # Run model inference using predict method
            try:
                raw_prediction = self.model.predict(processed_data)
                predictions = {
                    "predicted_value": float(raw_prediction),
                    "prediction_time": data["data"]["timestamp"]
                }
            except Exception as e:
                logger.warning(f"Error in model prediction: {e}, using fallback")
                predictions = {
                    "predicted_value": float(processed_data["value"].iloc[-1]),
                    "prediction_time": data["data"]["timestamp"]
                }

            # Format response as per MINER.md requirements
            return {
                "predicted_values": float(predictions.get("predicted_value", 0.0)),
                "timestamp": predictions.get("prediction_time", data["data"]["timestamp"]),
                "miner_hotkey": miner.keypair.ss58_address,
            }

        except Exception as e:
            logger.error(f"Error in miner execution: {str(e)}")
            logger.error(traceback.format_exc())
            # Fix datetime usage
            current_time = datetime.now(timezone.utc)
            return {
                "predicted_values": "N/A",
                "timestamp": current_time.isoformat(),
                "miner_hotkey": miner.keypair.ss58_address,
            }

    def query_miners(self):
        """
        Simulates querying miners and collecting predictions.

        Returns:
            dict: Simulated predictions and metadata.
        """
        try:
            # Simulate prediction values
            predictions = np.random.randint(-100, 100, size=256)  # Example predictions
            return {"predictions": predictions}
        except Exception as e:
            print(f"Error querying miners: {str(e)}")
            return None

    async def recalculate_recent_scores(self, uids: list):
        """
        Recalculate recent scores for the given UIDs across the 3-day scoring window
        and update all affected score_table rows.

        Args:
            uids (list): List of UIDs to recalculate scores for.
        """
        try:
            async with self.db_manager.get_session() as session:
                # Step 1: Delete predictions for the given UIDs
                delete_query = text("""
                    DELETE FROM geomagnetic_predictions
                    WHERE miner_uid = ANY(:uids)
                """)
                # Convert integer UIDs to strings
                str_uids = [str(uid) for uid in uids]
                await session.execute(delete_query, {"uids": str_uids})

                # Step 2: Set up time window (3 days)
                current_time = datetime.now(timezone.utc)
                history_window = current_time - timedelta(days=3)

                # Delete existing score rows for the period
                delete_scores_query = text("""
                    DELETE FROM score_table 
                    WHERE task_name = 'geomagnetic'
                    AND task_id::float >= :start_timestamp
                    AND task_id::float <= :end_timestamp
                """)
                await session.execute(
                    delete_scores_query,
                    {
                        "start_timestamp": history_window.timestamp(),
                        "end_timestamp": current_time.timestamp(),
                    }
                )

                # Get historical data
                history_query = text("""
                    SELECT 
                        miner_hotkey,
                        score,
                        query_time
                    FROM geomagnetic_history
                    WHERE query_time >= :history_window
                    ORDER BY query_time ASC
                """)
                result = await session.execute(
                    history_query, 
                    {"history_window": history_window}
                )
                history_results = result.fetchall()

                # Get mapping of hotkeys to UIDs
                hotkey_query = text("""
                    SELECT uid, hotkey FROM node_table 
                    WHERE hotkey IS NOT NULL
                """)
                result = await session.execute(hotkey_query)
                miner_mappings = result.fetchall()
                hotkey_to_uid = {row._mapping["hotkey"]: row._mapping["uid"] for row in miner_mappings}

                # Process historical data and rebuild scores
                scores_by_hour = {}
                for result in history_results:
                    hour = result._mapping["query_time"].replace(minute=0, second=0, microsecond=0)
                    if hour not in scores_by_hour:
                        scores_by_hour[hour] = [float("nan")] * 256

                    miner_hotkey = result._mapping["miner_hotkey"]
                    if miner_hotkey in hotkey_to_uid:
                        uid = hotkey_to_uid[miner_hotkey]
                        scores_by_hour[hour][uid] = result._mapping["score"]

                # Insert new score rows
                for hour, scores in scores_by_hour.items():
                    score_row = {
                        "task_name": "GeomagneticTask",
                        "task_id": str(hour.timestamp()),
                        "score": json.dumps(scores),
                        "status": "success"
                    }
                    insert_query = text("""
                        INSERT INTO score_table (task_name, task_id, score, status)
                        VALUES (:task_name, :task_id, :score, :status)
                    """)
                    await session.execute(insert_query, score_row)

                await session.commit()
                logger.info("Successfully recalculated and updated scores")

        except Exception as e:
            logger.error(f"Error recalculating scores: {e}")
            logger.error(traceback.format_exc())