import traceback
from typing import Any, Dict
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
        super().__init__(
            name="GeomagneticTask",
            description="Geomagnetic prediction task",
            task_type="atomic",
            metadata=GeomagneticMetadata(),
            inputs=GeomagneticInputs(),
            outputs=GeomagneticOutputs(),
            scoring_mechanism=GeomagneticScoringMechanism(),
            **data,
        )
        if db_manager:
            self.db_manager = db_manager

        # Log whether the fallback model is being used
        if self.model.is_fallback:
            logger.warning("Using fallback GeoMag model for predictions.")
        else:
            logger.info("Using Hugging Face GeoMag model for predictions.")

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
        - Fetches predictions for the last UTC hour.
        - Fetches ground truth data for the current UTC hour.
        - Scores predictions against the ground truth.
        - Archives scored predictions in the history table or file.
        - Runs in a continuous loop.
        """
        while True:
            try:
                logger.info("Executing GeomagneticTask Loop...")

                # Step 1: Align to the top of the next hour
                current_time = datetime.datetime.now(datetime.timezone.utc)
                next_hour = current_time.replace(
                    minute=0, second=0, microsecond=0
                ) + datetime.timedelta(hours=1)
                sleep_duration = (next_hour - current_time).total_seconds()

                # Step 2: Fetch Latest Geomagnetic Data
                timestamp, dst_value, historical_data = await self._fetch_geomag_data()

                # Step 3: Query Miners
                current_hour_start = next_hour - datetime.timedelta(hours=1)
                await self._query_miners(
                    validator, timestamp, dst_value, historical_data, current_hour_start
                )

                # Step 4: Process Scores
                await self._process_scores(validator, current_hour_start, next_hour)

                # Sleep until next hour
                current_time = datetime.datetime.now(datetime.timezone.utc)
                sleep_duration = (next_hour - current_time).total_seconds()
                if sleep_duration > 0:
                    logger.info(
                        f"Sleeping until next hour: {next_hour.isoformat()} (in {sleep_duration} seconds)"
                    )
                    await asyncio.sleep(sleep_duration)

            except Exception as e:
                logger.error(f"Unexpected error in validator_execute loop: {e}")
                logger.error(f"{traceback.format_exc()}")
                await asyncio.sleep(60)

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

    async def _process_scores(self, validator, current_hour_start, next_hour):
        """Process and archive scores for the previous hour."""
        # Fetch Ground Truth
        ground_truth_value = await self.fetch_ground_truth()
        if ground_truth_value is None:
            logger.warning("Ground truth data not available. Skipping scoring.")
            return

        # Score Predictions and Archive Results
        last_hour_start = current_hour_start
        last_hour_end = next_hour.replace(minute=0, second=0, microsecond=0)
        current_time = datetime.datetime.now(datetime.timezone.utc)  # Add this line

        logger.info(
            f"Fetching predictions between {last_hour_start} and {last_hour_end}"
        )
        tasks = await self.get_tasks_for_hour(last_hour_start, last_hour_end, validator)
        await self.score_tasks(tasks, ground_truth_value, current_time)

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
            # Query the database for most recent tasks per miner in the time range
            query = """
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
                WHERE rn = 1;
            """
            # Convert timestamps to UTC if they aren't already
            if start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=datetime.timezone.utc)
            if end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=datetime.timezone.utc)

            logger.info(f"Querying tasks with:")
            logger.info(f"  start_time: {start_time} (tzinfo: {start_time.tzinfo})")
            logger.info(f"  end_time: {end_time} (tzinfo: {end_time.tzinfo})")

            params = {"start_time": start_time, "end_time": end_time}

            # Log the query parameters for debugging
            logger.info(f"Querying with start_time: {start_time}, end_time: {end_time}")

            results = await self.db_manager.fetch_many(query, params)

            # Log raw results for debugging
            logger.info(f"Raw query results: {results}")

            # Convert results to a list of task dictionaries
            tasks = []
            for row in results:
                task = {
                    "id": row["id"],
                    "miner_uid": row["miner_uid"],
                    "miner_hotkey": row["miner_hotkey"],
                    "predicted_values": row["predicted_value"],
                    "query_time": row["query_time"],
                }
                tasks.append(task)

            logger.info(
                f"Fetched {len(tasks)} tasks between {start_time} and {end_time}"
            )

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
            # Insert into history table
            query = """
                INSERT INTO geomagnetic_history 
                (miner_uid, miner_hotkey, query_time, predicted_value, ground_truth_value, score, scored_at)
                VALUES (:miner_uid, :miner_hotkey, :query_time, :predicted_value, :ground_truth_value, :score, :scored_at)
            """

            params = {
                "miner_uid": task["miner_uid"],
                "miner_hotkey": task["miner_hotkey"],
                "query_time": task["query_time"],
                "predicted_value": task["predicted_values"],
                "ground_truth_value": ground_truth_value,
                "score": score,
                "scored_at": score_time,
            }

            await self.db_manager.execute(query, params)
            logger.info(f"Archived task to history: {task['id']}")

            # Remove from predictions table
            delete_query = """
                DELETE FROM geomagnetic_predictions 
                WHERE id = :task_id
            """
            await self.db_manager.execute(delete_query, {"task_id": task["id"]})
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
        - Runs model inference.
        - Returns formatted predictions.
        """
        try:
            # Extract data from the request payload
            if data and data.get("data"):
                # Current data
                input_data = pd.DataFrame(
                    {
                        "timestamp": [pd.to_datetime(data["data"]["timestamp"])],
                        "value": [float(data["data"]["value"])],
                    }
                )

                # Check and process historical data
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
                processed_data = self.miner_preprocessing.process_miner_data(
                    combined_df
                )
            else:
                logger.error("No data provided in request")
                return None

            # Run model inference
            predictions = self.run_model_inference(processed_data)

            # Format response according to MINER.md requirements
            return {
                "predicted_values": float(predictions),
                "timestamp": data["data"]["timestamp"],
                "miner_hotkey": miner.keypair.ss58_address,  # Use the miner's keypair directly
            }

        except Exception as e:
            logger.error(f"Error in miner execution: {str(e)}")
            logger.error(f"{traceback.format_exc()}")
            return None

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

    def add_task_to_queue(self, predictions, query_time):
        """
        Adds a new task to the task queue.

        Args:
            predictions (np.ndarray or None): Array of predictions from miners.
            query_time (datetime): The time the task was added.
        """
        try:
            # Use MinerDatabaseManager to insert task into the database
            db_manager = MinerDatabaseManager()
            task_name = "geomagnetic_prediction"
            miner_id = (
                "example_miner_id"  # Replace with the actual miner ID if available
            )

            # Validate predictions
            if predictions is None:
                logger.warning("Received None predictions, skipping queue addition")
                return

            # Convert predictions to a dictionary or JSON-like structure
            if isinstance(predictions, np.ndarray):
                predicted_value = {"predictions": predictions.tolist()}
            else:
                predicted_value = {"predictions": predictions}

            # Add to the queue
            asyncio.run(
                db_manager.add_to_queue(
                    task_name=task_name,
                    miner_id=miner_id,
                    predicted_value=predicted_value,
                    query_time=query_time,
                )
            )
            logger.info(f"Task added to queue: {task_name} at {query_time}")

        except Exception as e:
            logger.error(f"Error adding task to queue: {e}")

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
            # Initialize the database manager
            db_manager = ValidatorDatabaseManager()

            # Construct the query based on schema.json
            query = """
                INSERT INTO geomagnetic_predictions 
                (id, miner_uid, miner_hotkey, predicted_value, query_time, status)
                VALUES (:id, :miner_uid, :miner_hotkey, :predicted_value, :query_time, :status)
            """

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
            await db_manager.execute(query, params)
            logger.info(f"Added prediction from miner {miner_uid} to queue")

        except Exception as e:
            logger.error(f"Error adding prediction to queue: {e}")
            logger.error(f"{traceback.format_exc()}")
            raise

    async def process_miner_responses(
        self,
        responses: Dict[str, Any],
        current_hour_start: datetime.datetime,
        validator,
    ) -> None:
        """Process responses from miners and add valid predictions to the queue."""
        logger.info(
            f"Processing responses with current_hour_start: {current_hour_start} (tzinfo: {current_hour_start.tzinfo})"
        )

        for hotkey, response_data in responses.items():
            try:
                logger.info(f"Processing response for hotkey {hotkey}: {response_data}")

                # Handle response object with 'text' field
                if isinstance(response_data, dict) and "text" in response_data:
                    try:
                        response = json.loads(response_data["text"])
                        logger.debug(f"Successfully parsed response: {response}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse response text for {hotkey}: {e}")
                        continue
                else:
                    logger.warning(
                        f"Invalid response format for {hotkey}: {response_data}"
                    )
                    continue

                # Extract values from response
                predicted_value = float(
                    response.get("predicted_values")
                )  # Ensure numeric
                miner_hotkey = hotkey  # Use the key from responses dict

                # Get miner UID from hotkey
                miner_uid = None
                query = "SELECT uid FROM node_table WHERE hotkey = :miner_hotkey"
                result = await self.db_manager.fetch_one(
                    query, {"miner_hotkey": miner_hotkey}
                )
                if result:
                    miner_uid = result["uid"]
                    logger.info(
                        f"Found miner UID {miner_uid} for hotkey {miner_hotkey}"
                    )
                else:
                    logger.warning(f"No UID found for hotkey {miner_hotkey}")
                    continue

                # Validate response
                if predicted_value is None:
                    logger.warning(f"Missing predicted value in response: {response}")
                    continue

                # Add to queue with proper timestamp handling
                logger.info(
                    f"Adding prediction to queue for {miner_hotkey} with value {predicted_value}"
                )
                await self.add_prediction_to_queue(
                    miner_uid=str(miner_uid),
                    miner_hotkey=miner_hotkey,
                    predicted_value=predicted_value,
                    query_time=current_hour_start,
                    status="pending",  # Explicitly set status
                )

            except Exception as e:
                logger.error(f"Error processing miner response for {hotkey}: {e}")
                logger.error(f"{traceback.format_exc()}")
                continue

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
        Build a score row from recent tasks and historical data

        Args:
            current_hour (datetime): Current hour timestamp
            recent_tasks (list, optional): List of recently scored tasks

        Returns:
            dict: Dictionary containing task_name, task_id, and scores array
        """
        try:
            # Convert current_hour to datetime if it's an integer
            if isinstance(current_hour, int):
                current_time = datetime.datetime.now(datetime.timezone.utc)
                current_datetime = current_time.replace(
                    hour=current_hour, minute=0, second=0, microsecond=0
                )
                previous_datetime = current_datetime - datetime.timedelta(hours=1)
            else:
                current_datetime = current_hour
                previous_datetime = current_datetime - datetime.timedelta(hours=1)

            # Initialize scores array with NaN values
            scores = [float("nan")] * 256

            # Get mapping of hotkeys to UIDs from node_table
            query = """
            SELECT uid, hotkey FROM node_table 
            WHERE hotkey IS NOT NULL
            """
            miner_mappings = await self.db_manager.fetch_many(query)
            hotkey_to_uid = {row["hotkey"]: row["uid"] for row in miner_mappings}

            # Check historical table for any tasks in this time period
            historical_query = """
            SELECT miner_hotkey, score
            FROM geomagnetic_history
            WHERE query_time >= :start_time 
            AND query_time < :end_time
            """
            historical_tasks = await self.db_manager.fetch_many(
                historical_query,
                {"start_time": previous_datetime, "end_time": current_datetime},
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

            # Create score row
            score_row = {
                "task_name": "geomagnetic",
                "task_id": str(current_datetime.timestamp()),
                "score": scores,
                "status": "completed",
            }

            # Insert into score_table
            query = """
            INSERT INTO score_table (task_name, task_id, score, status)
            VALUES (:task_name, :task_id, :score, :status)
            """
            await self.db_manager.execute(query, score_row)

            logger.info(
                f"Built score row for hour {current_datetime} with {len([s for s in scores if not np.isnan(s)])} scores"
            )
            return score_row

        except Exception as e:
            logger.error(f"Error building score row: {e}")
            logger.error(traceback.format_exc())
            return None

    async def recalculate_recent_scores(self, uids: list):
        """
        Recalculate recent scores for the given UIDs across the 3-day scoring window
        and update all affected score_table rows.

        Args:
            uids (list): List of UIDs to recalculate scores for.
        """
        try:
            # Step 1: Delete predictions for the given UIDs
            delete_query = """
            DELETE FROM geomagnetic_predictions
            WHERE miner_uid = ANY(:uids)
            """
            await self.db_manager.execute(delete_query, {"uids": uids})
            logger.info(f"Deleted predictions for UIDs: {uids}")

            # Step 2: Set up time window (3 days)
            current_time = datetime.datetime.now(datetime.timezone.utc)
            history_window = current_time - datetime.timedelta(days=3)

            # Delete existing score rows for the period
            delete_scores_query = """
            DELETE FROM score_table 
            WHERE task_name = 'geomagnetic'
            AND task_id::float >= :start_timestamp
            AND task_id::float <= :end_timestamp
            """
            await self.db_manager.execute(
                delete_scores_query,
                {
                    "start_timestamp": history_window.timestamp(),
                    "end_timestamp": current_time.timestamp(),
                },
            )
            logger.info(
                f"Deleted score rows for period {history_window} to {current_time}"
            )

            # Get historical data
            history_query = """
            SELECT 
                miner_hotkey,
                score,
                query_time
            FROM geomagnetic_history
            WHERE query_time >= :history_window
            ORDER BY query_time ASC
            """
            history_results = await self.db_manager.fetch_many(
                history_query, {"history_window": history_window}
            )

            # Get mapping of hotkeys to UIDs
            query = """
            SELECT uid, hotkey FROM node_table 
            WHERE hotkey IS NOT NULL
            """
            miner_mappings = await self.db_manager.fetch_many(query)
            hotkey_to_uid = {row["hotkey"]: row["uid"] for row in miner_mappings}

            # Group records by hour
            hourly_records = {}
            for record in history_results:
                hour_key = record["query_time"].replace(
                    minute=0, second=0, microsecond=0
                )
                if hour_key not in hourly_records:
                    hourly_records[hour_key] = []
                hourly_records[hour_key].append(record)

            # Process each hour
            for hour, records in hourly_records.items():
                scores = [float("nan")] * 256

                # Calculate scores for this hour
                for record in records:
                    miner_hotkey = record["miner_hotkey"]
                    if miner_hotkey in hotkey_to_uid:
                        uid = hotkey_to_uid[miner_hotkey]
                        scores[uid] = record["score"]

                # Create and insert score row for this hour
                score_row = {
                    "task_name": "geomagnetic",
                    "task_id": str(hour.timestamp()),
                    "score": scores,
                    "status": "completed",
                }

                insert_query = """
                INSERT INTO score_table (task_name, task_id, score, status)
                VALUES (:task_name, :task_id, :score, :status)
                """
                await self.db_manager.execute(insert_query, score_row)
                logger.info(f"Recalculated and inserted score row for hour {hour}")

            logger.info(
                f"Completed recalculation of scores for UIDs: {uids} over 3-day window"
            )

        except Exception as e:
            logger.error(f"Error recalculating recent scores: {e}")
            logger.error(traceback.format_exc())
