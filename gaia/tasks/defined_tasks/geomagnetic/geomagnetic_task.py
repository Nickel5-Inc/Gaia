import traceback
from typing import Any, Dict, Optional, Union, List
import uuid
from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.tasks.base.task import Task
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
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.models.geomag_basemodel import GeoMagBaseModel
import torch
import datetime
import numpy as np
import pandas as pd
import asyncio
from uuid import uuid4
from fiber.logging_utils import get_logger
import json
from pydantic import Field
import os
import importlib.util
from sqlalchemy.sql import text

logger = get_logger(__name__)


class GeomagneticTask(Task):
    """
    A task class for processing and analyzing geomagnetic data, with
    execution methods for both miner and validator workflows.

    This task involves:
        - Querying miners for predictions
        - Adding predictions to a queue for scoring
        - Fetching ground truth data
        - Scoring predictions
        - Moving scored tasks to history

    Attributes:
        name (str): The name of the task, set as "GeomagneticTask".
        description (str): A description of the task's purpose.
        task_type (str): Specifies the type of task (e.g., "atomic").
        metadata (GeomagneticMetadata): Metadata associated with the task.
        inputs (GeomagneticInputs): Handles data loading and validation.
        preprocessing (GeomagneticPreprocessing): Processes raw data.
        scoring_mechanism (GeomagneticScoringMechanism): Computes scores.
        outputs (GeomagneticOutputs): Manages output formatting and saving.

    Example:
        task = GeomagneticTask()
        task.miner_execute()
        task.validator_execute()
    """

    # Declare Pydantic fields
    db_manager: Union[ValidatorDatabaseManager, MinerDatabaseManager] = Field(
        default_factory=ValidatorDatabaseManager,
        description="Database manager for the task",
    )
    miner_preprocessing: GeomagneticPreprocessing = Field(
        default_factory=GeomagneticPreprocessing,
        description="Preprocessing component for miner",
    )
    model: Optional[GeoMagBaseModel] = Field(
        default=None, description="The geomagnetic prediction model"
    )
    node_type: str = Field(
        default="validator",
        description="Type of node running the task (validator or miner)"
    )
    test_mode: bool = Field(
        default=False,
        description="Whether to run in test mode (immediate execution, limited scope)"
    )
    validator: Any = Field(
        default=None, 
        description="Reference to the validator instance"
    )


    def __init__(self, node_type: str, db_manager, test_mode: bool = False, **data):
        """Initialize the task."""
        super().__init__(
            name="GeomagneticTask",
            description="Geomagnetic prediction task",
            task_type="atomic",
            metadata=GeomagneticMetadata(),
            inputs=GeomagneticInputs(),
            outputs=GeomagneticOutputs(),
            db_manager=db_manager,
            scoring_mechanism=GeomagneticScoringMechanism(db_manager=db_manager),
            **data,
        )
        
        self.node_type = node_type
        self.test_mode = test_mode
        self.model = None
        
        if self.node_type == "miner":
            try:
                logger.info("Running as miner - loading model...")
                # Try to load custom model first
                custom_model_path = "gaia/models/custom_models/custom_geomagnetic_model.py"
                if os.path.exists(custom_model_path):
                    spec = importlib.util.spec_from_file_location(
                        "custom_geomagnetic_model", custom_model_path
                    )
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    self.model = module.CustomGeomagneticModel()
                    logger.info("Successfully loaded custom geomagnetic model")
                else:
                    # Fall back to base model
                    from gaia.models.geomag_basemodel import GeoMagBaseModel
                    self.model = GeoMagBaseModel()
                    logger.info("No custom model found, using base model")
            except Exception as e:
                logger.error(f"Error loading model: {e}")
                logger.error(traceback.format_exc())
                raise
        else:
            logger.info("Running as validator - skipping model loading")

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

    ############################################################
    # Validator execution method
    ############################################################

    async def validator_execute(self, validator):
        """
        Executes the validator workflow:
        - Aligns execution to start at the top of each UTC hour.
        - At hour N:
            - Query miners for new predictions
            - Score predictions collected during hour N-1
        - Runs in a continuous loop.
        """
        self.validator = validator
        
        while True:
            try:
                await validator.update_task_status('geomagnetic', 'active')
                
                # Step 1: Align to the top of the next hour (or wait 5 min in test mode)
                current_time = datetime.datetime.now(datetime.timezone.utc)
                if not self.test_mode:
                    next_hour = current_time.replace(
                        minute=0, second=0, microsecond=0
                    ) + datetime.timedelta(hours=1)
                    sleep_duration = (next_hour - current_time).total_seconds()

                    logger.info(
                        f"Sleeping until the next top of the hour: {next_hour.isoformat()} (in {sleep_duration} seconds)"
                    )
                    await validator.update_task_status('geomagnetic', 'idle')
                    await asyncio.sleep(sleep_duration)
                else:
                    next_hour = current_time
                    logger.info("Test mode: Running immediately, will sleep for 5 minutes after completion")

                logger.info("Starting GeomagneticTask execution...")

                # Step 2: Fetch Latest Geomagnetic Data
                await validator.update_task_status('geomagnetic', 'processing', 'data_fetch')
                timestamp, dst_value, historical_data = await self._fetch_geomag_data()

                # Step 3: Query Miners for predictions
                await validator.update_task_status('geomagnetic', 'processing', 'miner_query')
                await self._query_miners(
                    validator, timestamp, dst_value, historical_data, next_hour
                )
                logger.info(f"Collected predictions at hour {next_hour}")

                # Step 4: Score predictions from previous hour
                previous_hour = next_hour - datetime.timedelta(hours=1)
                await validator.update_task_status('geomagnetic', 'processing', 'scoring')
                await self._process_scores(validator, previous_hour)
                
                await validator.update_task_status('geomagnetic', 'idle')

                # In test mode, sleep for 5 minutes before next iteration
                if self.test_mode:
                    logger.info("Test mode: Sleeping for 5 minutes before next execution")
                    await asyncio.sleep(300)

            except Exception as e:
                logger.error(f"Unexpected error in validator_execute loop: {e}")
                logger.error(traceback.format_exc())
                await validator.update_task_status('geomagnetic', 'error')
                await asyncio.sleep(3600)

    async def _fetch_geomag_data(self):
        """Fetch latest geomagnetic data."""
        logger.info("Fetching latest geomagnetic data...")
        timestamp, dst_value, historical_data = await get_latest_geomag_data(
            include_historical=True
        )
        logger.info(
            f"Fetched latest geomagnetic data: timestamp={timestamp}, value={dst_value}"
        )
        if historical_data is not None:
            logger.info(
                f"Fetched historical data for the current month: {len(historical_data)} records"
            )
        else:
            logger.warning("No historical data available for the current month.")
        return timestamp, dst_value, historical_data

    async def _query_miners(
        self, validator, timestamp, dst_value, historical_data, current_hour_start
    ):
        """Query miners with current data and process responses."""
        if timestamp == "N/A" or dst_value == "N/A":
            logger.warning("Invalid geomagnetic data. Skipping miner queries.")
            return

        if timestamp == "N/A" or dst_value == "N/A":
            logger.warning("Invalid geomagnetic data. Skipping miner queries.")
            return
        
        task_id = str(current_hour_start.timestamp())
        logger.info(f"Running DST basemodel for scoring with task_id: {task_id} (timestamp: {current_hour_start})")
        
        validator.basemodel_evaluator.test_mode = self.test_mode
        
        await validator.basemodel_evaluator.predict_geo_and_store(
            historical_data,
            task_id
        )
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

    async def _process_scores(self, validator, query_hour):
        """Process task scores for a given hour."""
        try:
            current_time = datetime.datetime.now(datetime.timezone.utc)
            logger.info(
                f"Processing scores for hour starting at {query_hour.isoformat()}"
            )

            if self.test_mode:
                start_time = current_time - datetime.timedelta(minutes=10)
                end_time = current_time
                logger.info(f"TEST MODE: Using recent time window for immediate scoring: {start_time} to {end_time}")
            else:
                start_time = query_hour
                end_time = query_hour + datetime.timedelta(hours=1)

            tasks = await self.get_tasks_for_hour(
                start_time,
                end_time,
                validator=validator
            )

            if not tasks:
                logger.info(f"No tasks found for hour {query_hour.isoformat()}")
                return [], current_time

            # If all tasks are already scored, return early
            all_processed = all(task.get("status") == "scored" for task in tasks)
            if all_processed:
                logger.info(f"All tasks already scored for hour {query_hour.isoformat()}")
                # Return empty list since we don't need to score anything
                return [], current_time

            # Get ground truth data
            ground_truth_value = await self.fetch_ground_truth()
            if ground_truth_value is None:
                logger.warning("Could not fetch ground truth value, skipping scoring")
                return [], current_time

            logger.info(f"Ground truth value: {ground_truth_value}")
            
            scored_tasks = []
            for task in tasks:
                if task.get("status") == "scored":
                    logger.info(f"Task {task['id']} already scored, skipping")
                    continue

                try:
                    # Use validator's basemodel_evaluator to get the baseline score for comparison
                    baseline_score = None
                    if validator and hasattr(validator, 'basemodel_evaluator'):
                        try:
                            task_timestamp = task.get("timestamp", task.get("query_time"))
                            if task_timestamp:
                                task_id = str(task_timestamp.timestamp()) if isinstance(task_timestamp, datetime.datetime) else str(task_timestamp)
                                logger.debug(f"Looking up baseline prediction with task_id: {task_id}")
                                validator.basemodel_evaluator.test_mode = self.test_mode
                                
                                baseline_score = await validator.basemodel_evaluator.score_geo_baseline(
                                    task_id=task_id,
                                    ground_truth=ground_truth_value
                                )
                                if baseline_score is not None:
                                    logger.info(f"Retrieved baseline score for task_id {task_id}: {baseline_score:.4f}")
                                else:
                                    logger.info(f"No baseline score available for task_id {task_id}")
                            else:
                                logger.warning(f"Task has no timestamp, cannot retrieve baseline prediction")
                        except Exception as e:
                            logger.error(f"Error retrieving baseline score: {e}")
                            logger.error(traceback.format_exc())
                    
                    # Calculate score between prediction and ground truth
                    score = self.scoring_mechanism.calculate_score(
                        task["predicted_values"], ground_truth_value
                    )

                    if baseline_score is not None:
                        logger.info(f"Geomagnetic Task - Miner: {task_id} - Miner score: {score:.4f}, Baseline: {baseline_score:.4f}, Diff: {score - baseline_score:.4f}")

                        benchmark_score = 0.90
                        base_epsilon = 0.005
                        theoretical_max = 0.99
                        
                        if baseline_score > theoretical_max - 0.10:
                            epsilon = 0.002
                            logger.debug(f"Using reduced epsilon: {epsilon:.4f} (baseline near theoretical max)")
                        else:
                            epsilon = base_epsilon
                        
                        if score <= baseline_score + epsilon:
                            if score < baseline_score:
                                logger.info(f"Score zeroed - Below baseline: {score:.4f} < {baseline_score:.4f}")
                            else:
                                logger.info(f"Score zeroed - Insufficient improvement: {score:.4f} vs baseline {baseline_score:.4f} (needed > {baseline_score + epsilon:.4f})")
                            score = 0
                        elif score < benchmark_score:
                            logger.info(f"Score zeroed - Below benchmark: {score:.4f} < {benchmark_score:.4f}")
                            score = 0
                        else:
                            logger.info(f"Score valid - Exceeds baseline by {score - baseline_score:.4f} and meets benchmark {benchmark_score:.4f}")
                    else:
                        logger.info(f"No baseline comparison available - using raw score: {score:.4f}")
                    
                    # Mark task as scored in DB
                    await self.move_task_to_history(
                        task, ground_truth_value, score, current_time
                    )

                    # Add score to task dict for building score row later
                    task["score"] = score
                    scored_tasks.append(task)
                    logger.info(
                        f"Task {task['id']} scored and archived. Score: {score}"
                    )
                except Exception as e:
                    logger.error(f"Error processing task {task.get('id', 'unknown')}: {e}")
                    logger.error(traceback.format_exc())

            return scored_tasks, current_time

        except Exception as e:
            logger.error(f"Error in process_scores: {e}")
            logger.error(traceback.format_exc())
            return [], current_time

    async def get_tasks_for_hour(self, start_time, end_time, validator=None):
        """
        Fetches tasks submitted within a specific UTC time range from the database.
        Only returns the most recent task per miner.

        Args:
            start_time (datetime): Start of the time range (inclusive).
            end_time (datetime): End of the time range (exclusive).
            validator (optional): Validator instance containing metagraph.

        Returns:
            list: List of task dictionaries containing task details.
        """
        try:
            # Convert timestamps to UTC if they aren't already
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=datetime.timezone.utc)
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=datetime.timezone.utc)

            logger.info(f"Querying tasks with:")
            logger.info(f"  start_time: {start_time} (tzinfo: {start_time.tzinfo})")
            logger.info(f"  end_time: {end_time} (tzinfo: {end_time.tzinfo})")

            results = await self.db_manager.fetch_all(
                """
                WITH RankedTasks AS (
                    SELECT 
                        id,
                        miner_uid,
                        miner_hotkey,
                        predicted_value,
                        query_time,
                        ROW_NUMBER() OVER (
                            PARTITION BY miner_uid 
                            ORDER BY query_time DESC
                        ) as rn
                    FROM geomagnetic_predictions
                    WHERE query_time >= :start_time 
                    AND query_time < :end_time 
                    AND status = 'pending'
                )
                SELECT 
                    id,
                    miner_uid,
                    miner_hotkey,
                    predicted_value,
                    query_time
                FROM RankedTasks
                WHERE rn = 1
                """,
                {
                    "start_time": start_time,
                    "end_time": end_time
                }
            )
            
            tasks = []
            for row in results:
                task = {
                    "id": row["id"],
                    "miner_uid": row["miner_uid"],
                    "miner_hotkey": row["miner_hotkey"],
                    "predicted_values": row["predicted_value"],
                    "query_time": row["query_time"],
                    "timestamp": row["query_time"],
                }
                tasks.append(task)

            logger.info(f"Fetched {len(tasks)} tasks between {start_time} and {end_time}")

            # task validation - ensure that miner_hotkey is in the metagraph if validator is provided
            if validator:
                tasks = [
                    task
                    for task in tasks
                    if task["miner_hotkey"] in validator.metagraph.nodes
                ]

            return tasks

        except Exception as e:
            logger.error(f"Error fetching tasks for hour: {e}")
            logger.error(f"{traceback.format_exc()}")
            return []

    async def fetch_ground_truth(self):
        """
        Fetches the ground truth DST value for the current UTC hour.

        Returns:
            int: The real-time DST value, or None if fetching fails.
        """
        try:
            # Get the current UTC time
            current_time = datetime.datetime.now(datetime.timezone.utc)
            
            if self.test_mode:
                logger.info(f"TEST MODE: Fetching ground truth for current UTC hour: {current_time.hour}")
            else:
                logger.info(f"Fetching ground truth for UTC hour: {current_time.hour}")

            # Fetch the most recent geomagnetic data
            timestamp, dst_value = await get_latest_geomag_data(
                include_historical=False
            )

            if timestamp == "N/A" or dst_value == "N/A":
                logger.warning("No ground truth data available for the current hour.")
                return None

            logger.info(f"Ground truth value for hour {current_time.hour}: {dst_value}")
            return dst_value

        except Exception as e:
            logger.error(f"Error fetching ground truth: {e}")
            logger.error(f"{traceback.format_exc()}")
            return None

    async def move_task_to_history(
        self, task: dict, ground_truth_value: float, score: float, score_time: datetime
    ):
        """
        Archives a completed task in the history table.

        Args:
            task (dict): Task details including predicted_values and query_time
            ground_truth_value (float): The actual observed value
            score (float): The calculated score
            score_time (datetime): When the task was scored
        """
        try:
            await self.db_manager.execute(
                """
                INSERT INTO geomagnetic_history 
                (miner_uid, miner_hotkey, query_time, predicted_value, ground_truth_value, score, scored_at)
                VALUES (:miner_uid, :miner_hotkey, :query_time, :predicted_value, :ground_truth_value, :score, :scored_at)
                """,
                {
                    "miner_uid": task["miner_uid"],
                    "miner_hotkey": task["miner_hotkey"],
                    "query_time": task["query_time"],
                    "predicted_value": task["predicted_values"],
                    "ground_truth_value": ground_truth_value,
                    "score": score,
                    "scored_at": score_time,
                }
            )
            logger.info(f"Archived task to history: {task['id']}")

            await self.db_manager.execute(
                """
                DELETE FROM geomagnetic_predictions 
                WHERE id = :task_id
                """,
                {"task_id": task["id"]}
            )
            logger.info(f"Removed task from predictions: {task['id']}")

        except Exception as e:
            logger.error(f"Error moving task to history: {e}")
            logger.error(traceback.format_exc())
            raise

    ############################################################
    # Miner execution method
    ############################################################

    def run_model_inference(self, processed_data):
        """
        Run the GeoMag model inference.

        Args:
            processed_data (pd.DataFrame): Preprocessed input data for the model.

        Returns:
            float: Predicted value.
        """
        try:
            # Perform prediction using the model
            prediction = self.model.predict(processed_data)

            # Handle NaN or infinite values
            if np.isnan(prediction) or np.isinf(prediction):
                logger.warning("Model returned NaN/Inf, using fallback value")
                return float(
                    processed_data["value"].iloc[-1]
                )  # Use input value as fallback

            return float(prediction)  # Ensure we return a Python float

        except Exception as e:
            logger.error(f"Error during model inference: {e}")
            return float(
                processed_data["value"].iloc[-1]
            )  # Return input value as fallback

    def miner_execute(self, data, miner):
        """
        Executes the miner workflow:
        - Preprocesses the received data along with historical data.
        - Dynamically determines whether to use a custom or base model for inference.
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

            # Run model inference: Check for custom model first
            if hasattr(self.model, "run_inference"):
                logger.info("Using custom geomagnetic model for inference.")
                predictions = self.model.run_inference(processed_data)
            else:
                logger.info("Using base geomagnetic model for inference.")
                raw_prediction = self.run_model_inference(processed_data)
                predictions = {
                    "predicted_value": float(raw_prediction),
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
            # Return float('nan') instead of "N/A"
            current_time = datetime.datetime.now(datetime.timezone.utc)
            return {
                "predicted_values": float('nan'),  # Changed from "N/A" to float('nan')
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

    async def add_task_to_queue(self, predictions, query_time):
        """
        Adds a new task to the task queue.

        Args:
            predictions (np.ndarray or None): Array of predictions from miners.
            query_time (datetime): The time the task was added.
        """
        try:
            # Validate predictions
            if predictions is None:
                logger.warning("Received None predictions, skipping queue addition")
                return

            # Convert predictions to a dictionary or JSON-like structure
            if isinstance(predictions, np.ndarray):
                predicted_value = {"predictions": predictions.tolist()}
            else:
                predicted_value = {"predictions": predictions}

            # Add to the queue using execute
            await self.db_manager.execute(
                """
                INSERT INTO task_queue (task_name, miner_id, predicted_value, query_time)
                VALUES (:task_name, :miner_id, :predicted_value, :query_time)
                """,
                {
                    "task_name": "geomagnetic_prediction",
                    "miner_id": "example_miner_id",  # Replace with actual miner ID
                    "predicted_value": json.dumps(predicted_value),
                    "query_time": query_time
                }
            )
            logger.info(f"Task added to queue: geomagnetic_prediction at {query_time}")

        except Exception as e:
            logger.error(f"Error adding task to queue: {e}")
            raise

    async def add_prediction_to_queue(
        self,
        miner_uid: str,
        miner_hotkey: str,
        predicted_value: float,
        query_time: datetime,
        status: str = "pending",
    ) -> None:
        """
        Add a prediction to the geomagnetic_predictions table.

        Args:
            miner_id (str): ID of the miner submitting the prediction
            predicted_value (float): The predicted DST value
            query_time (datetime): Timestamp for when the prediction was made
            status (str, optional): Current status of the prediction. Defaults to "pending"
        """
        try:
            # Prepare parameters
            params = {
                "id": str(uuid.uuid4()),
                "miner_uid": miner_uid,
                "miner_hotkey": miner_hotkey,
                "predicted_value": float(predicted_value),  # Ensure float type
                "query_time": query_time,
                "status": status,
            }

            logger.info(f"Adding prediction to queue with params: {params}")

            # Execute the query
            await self.db_manager.execute(
                """
                INSERT INTO geomagnetic_predictions 
                (id, miner_uid, miner_hotkey, predicted_value, query_time, status)
                VALUES (:id, :miner_uid, :miner_hotkey, :predicted_value, :query_time, :status)
                """,
                params
            )
            logger.info(f"Added prediction from miner {miner_uid} to queue")

        except Exception as e:
            logger.error(f"Error adding prediction to queue: {e}")
            logger.error(f"{traceback.format_exc()}")
            raise

    def extract_prediction(self, response):
        """Recursively extract prediction from response, handling various formats."""
        if isinstance(response, dict):
            # Direct access to predicted values
            if "predicted_values" in response:
                return response["predicted_values"]
            if "predicted_value" in response:
                return response["predicted_value"]
            # If response has text field that might be JSON
            if "text" in response:
                try:
                    parsed = json.loads(response["text"])
                    return self.extract_prediction(parsed)
                except json.JSONDecodeError:
                    return None
        return None

    async def process_miner_responses(
        self,
        responses: Dict[str, Any],
        current_hour_start: datetime.datetime,
        validator,
    ) -> None:
        """Process responses from miners and add to queue."""
        try:
            if not responses:
                logger.warning("No responses received from miners")
                return

            for hotkey, response in responses.items():
                try:
                    logger.info(f"Raw response from miner {hotkey}: {response}")
                    
                    predicted_value = self.extract_prediction(response)
                    if predicted_value is None:
                        logger.error(f"No valid prediction found in response from {hotkey}")
                        continue

                    try:
                        predicted_value = float(predicted_value)
                    except (TypeError, ValueError) as e:
                        logger.error(f"Invalid prediction from {hotkey}: {predicted_value}")
                        continue

                    logger.info("=" * 50)
                    logger.info(f"Received prediction from miner:")
                    logger.info(f"Miner Hotkey: {hotkey}")
                    logger.info(f"Predicted Value: {predicted_value}")
                    logger.info(f"Timestamp: {current_hour_start}")
                    logger.info("=" * 50)

                    result = await self.db_manager.fetch_one(
                        """
                        SELECT uid FROM node_table 
                        WHERE hotkey = :miner_hotkey
                        """,
                        {"miner_hotkey": hotkey}
                    )
                    
                    if not result:
                        logger.warning(f"No UID found for hotkey {hotkey}")
                        continue
                    miner_uid = str(result["uid"])
                    logger.info(f"Found miner UID {miner_uid} for hotkey {hotkey}")

                    logger.info(f"Adding prediction to queue for {hotkey} with value {predicted_value}")
                    await self.add_prediction_to_queue(
                        miner_uid=miner_uid,
                        miner_hotkey=hotkey,
                        predicted_value=predicted_value,
                        query_time=current_hour_start,
                        status="pending",
                    )

                except Exception as e:
                    logger.error(f"Error processing response from {hotkey}: {e}")
                    logger.error(traceback.format_exc())
                    continue

        except Exception as e:
            logger.error(f"Error processing miner responses: {e}")
            logger.error(traceback.format_exc())

    async def score_tasks(self, tasks, ground_truth_value, current_time):
        if tasks:
            scored_tasks = []
            for task in tasks:
                try:
                    predicted_value = task["predicted_values"]
                    score = self.scoring_mechanism.calculate_score(
                        predicted_value, ground_truth_value
                    )
                    await self.move_task_to_history(
                        task, ground_truth_value, score, current_time
                    )
                    task["score"] = score  # Add score to task dict
                    scored_tasks.append(task)
                    logger.info(
                        f"Task scored and archived: task_id={task['id']}, score={score}"
                    )
                except Exception as e:
                    logger.error(f"Error processing task {task['id']}: {e}")
                    logger.error(traceback.format_exc())

            current_hour = datetime.datetime.now(datetime.timezone.utc).hour
            await self.build_score_row(current_hour, scored_tasks)
        else:
            logger.info("No predictions to score for the last hour.")

    async def build_score_row(self, current_hour, recent_tasks=None):
        """
        Build a score row from recent tasks and historical data.
        The task_id should be the verification time (when we can score the predictions).

        Args:
            current_hour (datetime): Current hour timestamp (when predictions were made)
            recent_tasks (list, optional): List of recently scored tasks

        Returns:
            dict: Dictionary containing task_name, task_id, and scores array
        """
        try:
            # Convert current_hour to datetime if it's an integer
            if isinstance(current_hour, int):
                current_time = datetime.datetime.now(datetime.timezone.utc)
                prediction_time = current_time.replace(
                    hour=current_hour, minute=0, second=0, microsecond=0
                )
            else:
                prediction_time = current_hour

            # Verification time is 1 hour after prediction time
            verification_time = prediction_time + datetime.timedelta(hours=1)

            # Initialize scores array with NaN values
            scores = [float("nan")] * 256

            # Get mapping of hotkeys to UIDs from node_table
            query = """
            SELECT uid, hotkey FROM node_table 
            WHERE hotkey IS NOT NULL
            """
            miner_mappings = await self.db_manager.fetch_all(query)
            hotkey_to_uid = {row["hotkey"]: row["uid"] for row in miner_mappings}

            # Check historical table for any tasks in this time period
            historical_query = """
            SELECT miner_hotkey, score
            FROM geomagnetic_history
            WHERE query_time = :prediction_time
            """
            historical_tasks = await self.db_manager.fetch_all(
                historical_query,
                {"prediction_time": prediction_time},
            )

            # Process historical tasks
            for task in historical_tasks:
                miner_hotkey = task["miner_hotkey"]
                if miner_hotkey in hotkey_to_uid:
                    uid = hotkey_to_uid[miner_hotkey]
                    scores[uid] = task["score"]

            # Process recent tasks (overwrite historical scores if exists)
            if recent_tasks:
                for task in recent_tasks:
                    miner_hotkey = task["miner_hotkey"]
                    if miner_hotkey in hotkey_to_uid:
                        uid = hotkey_to_uid[miner_hotkey]
                        scores[uid] = task.get("score", float("nan"))

            # Create score row using verification time as task_id
            score_row = {
                "task_name": "geomagnetic",
                "task_id": str(verification_time.timestamp()),
                "score": scores,
                "status": "completed",
            }

            # WRITE operation - use execute for inserting score
            await self.db_manager.execute(
                """
                INSERT INTO score_table (task_name, task_id, score, status)
                VALUES (:task_name, :task_id, :score, :status)
                """,
                score_row
            )

            logger.info(
                f"Built score row for predictions at {prediction_time} (verification time {verification_time}) with {len([s for s in scores if not np.isnan(s)])} scores"
            )
            return score_row

        except Exception as e:
            logger.error(f"Error building score row: {e}")
            logger.error(traceback.format_exc())
            return None

    async def recalculate_recent_scores(self, uids: List[int]) -> None:
        """
        Recalculate scores for specified miners over the last 3 days.
        
        Args:
            uids (List[int]): List of miner UIDs to recalculate scores for
            
        Raises:
            ValueError: If UIDs are invalid
            DatabaseError: If there is an error accessing the database
        """
        try:
            # Validate UIDs
            if not all(isinstance(uid, int) and 0 <= uid < 256 for uid in uids):
                raise ValueError("All UIDs must be integers between 0 and 255")

            current_time = datetime.datetime.now(datetime.timezone.utc)
            history_window = current_time - datetime.timedelta(days=3)
            logger.info(f"Recalculating scores for UIDs {uids} from {history_window} to {current_time}")

            # Convert UIDs to strings for database query
            str_uids = [str(uid) for uid in uids]

            # Delete existing predictions
            await self.db_manager.execute(
                """
                DELETE FROM geomagnetic_predictions
                WHERE miner_uid = ANY(:uids)
                """,
                {"uids": str_uids}
            )
            logger.info(f"Successfully deleted predictions for UIDs: {uids}")

            # Delete affected score rows
            await self.db_manager.execute(
                """
                DELETE FROM score_table 
                WHERE task_name = 'geomagnetic'
                AND task_id::float >= :start_timestamp
                AND task_id::float <= :end_timestamp
                """,
                {
                    "start_timestamp": history_window.timestamp(),
                    "end_timestamp": current_time.timestamp(),
                }
            )
            logger.info(f"Successfully deleted scores for time window")

            # Get history data
            history_results = await self.db_manager.fetch_all(
                """
                SELECT 
                    miner_hotkey,
                    miner_uid,
                    query_time,
                    score
                FROM geomagnetic_history
                WHERE query_time >= :history_window
                AND query_time <= :current_time
                AND miner_uid = ANY(:uids)
                ORDER BY query_time ASC
                """,
                {
                    "history_window": history_window,
                    "current_time": current_time,
                    "uids": str_uids
                }
            )

            if not history_results:
                logger.warning(f"No historical data found for UIDs {uids} in window {history_window} to {current_time}")
                return

            # Get current miner mappings
            miner_mappings = await self.db_manager.fetch_all(
                """
                SELECT uid, hotkey 
                FROM node_table 
                WHERE hotkey IS NOT NULL
                """
            )
            hotkey_to_uid: Dict[str, int] = {row["hotkey"]: row["uid"] for row in miner_mappings}

            # Group records by hour
            hourly_records: Dict[datetime.datetime, List[Dict[str, Any]]] = {}
            for record in history_results:
                hour_key = record["query_time"].replace(
                    minute=0, second=0, microsecond=0
                )
                if hour_key not in hourly_records:
                    hourly_records[hour_key] = []
                hourly_records[hour_key].append(record)

            # Process each hour
            for hour, records in hourly_records.items():
                try:
                    scores: List[float] = [float("nan")] * 256

                    # Calculate scores for this hour
                    for record in records:
                        try:
                            miner_hotkey = record["miner_hotkey"]
                            if miner_hotkey in hotkey_to_uid:
                                uid = hotkey_to_uid[miner_hotkey]
                                if record["score"] is not None:
                                    scores[uid] = float(record["score"])
                        except (ValueError, TypeError) as e:
                            logger.error(f"Error processing record for miner {record.get('miner_hotkey')}: {e}")
                            continue

                    # Insert score row
                    score_row = {
                        "task_name": "geomagnetic",
                        "task_id": str(hour.timestamp()),
                        "score": scores,
                        "status": "completed",
                    }

                    await self.db_manager.execute(
                        """
                        INSERT INTO score_table (task_name, task_id, score, status)
                        VALUES (:task_name, :task_id, :score, :status)
                        """,
                        score_row
                    )
                    logger.info(f"Recalculated and inserted score row for hour {hour}")

                except Exception as e:
                    logger.error(f"Error processing hour {hour}: {e}")
                    logger.error(traceback.format_exc())
                    continue

            logger.info(f"Completed recalculation of scores for UIDs: {uids} over 3-day window")

        except ValueError as e:
            logger.error(f"Invalid UIDs in recalculate_recent_scores: {e}")
            raise
        except Exception as e:
            logger.error(f"Error in recalculate_recent_scores: {e}")
            logger.error(traceback.format_exc())
            raise  # Re-raise to trigger error handling in deregistration loop

    async def cleanup_resources(self):
        """Clean up any resources used by the task during recovery."""
        try:
            await self.db_manager.execute(
                """
                UPDATE geomagnetic_predictions 
                SET status = 'pending'
                WHERE status = 'processing'
                """
            )
            logger.info("Reset in-progress prediction statuses")
            
            await self.db_manager.execute(
                """
                DELETE FROM score_table 
                WHERE task_name = 'geomagnetic' 
                AND status = 'processing'
                """
            )
            logger.info("Cleaned up incomplete scoring operations")
            logger.info("Completed geomagnetic task cleanup")
            
        except Exception as e:
            logger.error(f"Error during geomagnetic task cleanup: {e}")
            logger.error(traceback.format_exc())
            raise
            raise
