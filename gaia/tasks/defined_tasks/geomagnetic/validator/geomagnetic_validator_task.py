import asyncio
import datetime
from datetime import timedelta
import traceback
import os
import importlib.util
from typing import Any, Dict, List, Optional, Union, ClassVar
from uuid import uuid4
from prefect import flow, task
from prefect.tasks import task_input_hash
from pydantic import Field
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.miner.database.miner_database_manager import MinerDatabaseManager
from gaia.tasks.base.task import BaseTask
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_metadata import GeomagneticMetadata
from gaia.tasks.defined_tasks.geomagnetic.protocol import GeomagneticInputs, GeomagneticOutputs, GeomagneticPrediction, GeomagneticMeasurement
from gaia.tasks.defined_tasks.geomagnetic.utils.process_geomag_data import get_latest_geomag_data
from gaia.parallel.core.executor import get_dask_runner, parallel_context
from gaia.parallel.config.settings import TaskType
from gaia.models.geomag_basemodel import GeoMagBaseModel
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class GeomagneticValidatorTask(BaseTask):
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
        outputs (GeomagneticOutputs): Manages output formatting and saving.

    Example:
        task = GeomagneticValidatorTask()
        task.miner_execute()
        task.validator_execute()
    """

    # Declare Pydantic fields
    db_manager: Union[ValidatorDatabaseManager, MinerDatabaseManager] = Field(
        default_factory=ValidatorDatabaseManager,
        description="Database manager for the task",
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

    prepare_flow: ClassVar[Any]
    execute_flow: ClassVar[Any]
    score_flow: ClassVar[Any]
    
    get_tasks_for_hour: ClassVar[Any]
    fetch_ground_truth: ClassVar[Any]
    score_tasks: ClassVar[Any]
    _fetch_geomag_data: ClassVar[Any]
    _query_miners: ClassVar[Any]
    _process_scores: ClassVar[Any]
    move_task_to_history: ClassVar[Any]
    build_score_row: ClassVar[Any]

    def __init__(self, node_type: str, db_manager, test_mode: bool = False, **data):
        """Initialize the task."""
        metadata = GeomagneticMetadata()
        super().__init__(
            name="GeomagneticTask",
            description="Geomagnetic prediction task",
            task_type="atomic",
            metadata=metadata.model_dump(),
            inputs=GeomagneticInputs(),
            outputs=GeomagneticOutputs(),
            db_manager=db_manager,
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

    @flow(name="validator_prepare", retries=2)
    async def prepare_flow(self, validator) -> Dict[str, Any]:
        """Prepare the validator task by aligning to the next hour."""
        try:
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
                logger.info("Test mode: Running immediately")

            return {
                "current_time": current_time,
                "next_hour": next_hour
            }
        except Exception as e:
            logger.error(f"Error in prepare_flow: {e}")
            logger.error(traceback.format_exc())
            raise

    @flow(name="validator_execute", retries=2)
    async def execute_flow(self, validator: Any) -> None:
        """Execute the main validator workflow."""
        try:
            # Get task runner for parallel processing
            task_runner = get_dask_runner(TaskType.MIXED)
            logger.info(f"Initialized task runner with type: {task_runner.__class__.__name__}")
            
            # Prepare timing
            timing = await self.prepare_flow(validator)
            current_time = timing["current_time"]
            next_hour = timing["next_hour"]

            await validator.update_task_status('geomagnetic', 'active')
            logger.info("Starting GeomagneticTask execution...")

            # Step 2: Fetch Latest Geomagnetic Data
            await validator.update_task_status('geomagnetic', 'processing', 'data_fetch')
            geomag_data_future = task_runner.submit(
                self._fetch_geomag_data,
                pure=True,
                retries=3
            )
            timestamp, dst_value, historical_data = await geomag_data_future

            # Step 3: Query Miners for predictions
            await validator.update_task_status('geomagnetic', 'processing', 'miner_query')
            miner_query_future = task_runner.submit(
                self._query_miners,
                validator, timestamp, dst_value, historical_data, next_hour,
                pure=True,
                retries=3
            )
            responses_count = await miner_query_future
            logger.info(f"Collected {responses_count} predictions at hour {next_hour}")

            # Step 4: Score predictions from previous hour
            previous_hour = next_hour - datetime.timedelta(hours=1)
            await validator.update_task_status('geomagnetic', 'processing', 'scoring')
            
            # Process scores in parallel
            score_future = task_runner.submit(
                self._process_scores,
                validator, previous_hour,
                pure=True,
                retries=2
            )
            total_processed, success_count = await score_future
            logger.info(f"Processed {total_processed} predictions, {success_count} successful")
            
            await validator.update_task_status('geomagnetic', 'idle')

            if self.test_mode:
                logger.info("Test mode: Sleeping for 5 minutes before next execution")
                await asyncio.sleep(300)

        except Exception as e:
            logger.error(f"Error in execute_flow: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @flow(name="validator_score", retries=2)
    async def score_flow(self, validator: Any, query_hour: datetime.datetime) -> None:
        """Score predictions for a given hour using Dask for parallel processing."""
        try:
            # Get task runner for parallel processing
            task_runner = get_dask_runner(TaskType.CPU_BOUND)
            
            # Fetch Ground Truth (with Dask)
            ground_truth_future = task_runner.submit(
                self.fetch_ground_truth,
                pure=True,
                retries=3
            )
            ground_truth_value = await ground_truth_future
            
            if ground_truth_value is None:
                logger.warning("Ground truth data not available. Skipping scoring.")
                return

            # Get predictions collected during the specified hour
            hour_start = query_hour
            hour_end = query_hour + datetime.timedelta(hours=1)

            logger.info(
                f"Scoring predictions collected between {hour_start} and {hour_end}"
            )
            
            # Fetch tasks (with Dask)
            tasks_future = task_runner.submit(
                self.get_tasks_for_hour,
                hour_start, hour_end, validator,
                pure=True,
                retries=2
            )
            tasks = await tasks_future
            
            if not tasks:
                logger.info(f"No predictions found for collection hour {query_hour}")
                return

            current_time = datetime.datetime.now(datetime.timezone.utc)
            
            # Score tasks in parallel (with Dask)
            score_future = task_runner.submit(
                self.score_tasks,
                tasks, ground_truth_value, current_time,
                pure=True,
                retries=2
            )
            success_count = await score_future
            
            logger.info(f"Completed scoring {len(tasks)} predictions from hour {query_hour}")

        except Exception as e:
            logger.error(f"Error in score_flow: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @task(
        name="fetch_geomag_data",
        retries=3,
        retry_delay_seconds=30,
        cache_key_fn=task_input_hash,
        cache_expiration=timedelta(minutes=5),
        description="Fetch latest geomagnetic data including historical records"
    )
    async def _fetch_geomag_data(self):
        """
        Fetch latest geomagnetic data including historical records.
        
        Returns:
            tuple: (timestamp, dst_value, historical_data)
                - timestamp (datetime): Timestamp of the latest measurement
                - dst_value (float): Latest DST value
                - historical_data (list): Historical DST records for the current month
        
        Raises:
            Exception: If there's an error fetching the data
        """
        try:
            logger.info("Fetching latest geomagnetic data...")
            timestamp, dst_value, historical_data = await get_latest_geomag_data(
                include_historical=True
            )
            
            if historical_data is None:
                logger.warning("No historical data available for the current month.")
                historical_data = []
            else:
                logger.info(f"Fetched {len(historical_data)} historical records")
            
            logger.info(f"Latest measurement - timestamp: {timestamp}, DST value: {dst_value}")
            
            return timestamp, dst_value, historical_data
            
        except Exception as e:
            logger.error(f"Error fetching geomagnetic data: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @task(
        name="query_miners",
        retries=3,
        retry_delay_seconds=60,
        description="Query miners for geomagnetic predictions and process responses"
    )
    async def _query_miners(
        self, validator, timestamp, dst_value, historical_data, current_hour_start
    ):
        """
        Query miners with current data and process responses.
        
        Args:
            validator: The validator instance
            timestamp (datetime): Timestamp of the latest measurement
            dst_value (float): Latest DST value
            historical_data (pd.DataFrame): Historical DST records
            current_hour_start (datetime): Start of the current hour
            
        Returns:
            int: Number of processed miner responses
            
        Raises:
            Exception: If there's an error querying miners or processing responses
        """
        try:
            if timestamp == "N/A" or dst_value == "N/A":
                logger.warning("Invalid geomagnetic data. Skipping miner queries.")
                return 0

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

            logger.info("Querying miners for geomagnetic predictions")
            responses = await validator.query_miners(payload_template, endpoint)
            logger.info(f"Collected responses from miners: {len(responses)}")

            await self.process_miner_responses(responses, current_hour_start, validator)
            logger.info(f"Added {len(responses)} predictions to the database")
            
            return len(responses)

        except Exception as e:
            logger.error(f"Error querying miners: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @task(
        name="process_scores",
        retries=2,
        retry_delay_seconds=30,
        description="Process and archive scores for predictions from a specific hour"
    )
    async def _process_scores(self, validator, query_hour):
        """
        Process and archive scores for predictions collected during the specified hour.
        
        Args:
            validator: The validator instance
            query_hour (datetime): The hour during which predictions were collected
            
        Returns:
            tuple: (processed_count, success_count)
                - processed_count (int): Total number of predictions processed
                - success_count (int): Number of successfully scored predictions
                
        Raises:
            Exception: If there's an error processing scores
        """
        try:
            # Fetch Ground Truth
            ground_truth_value = await self.fetch_ground_truth()
            if ground_truth_value is None:
                logger.warning("Ground truth data not available. Skipping scoring.")
                return (0, 0)

            # Get predictions collected during the specified hour
            hour_start = query_hour
            hour_end = query_hour + datetime.timedelta(hours=1)

            logger.info(
                f"Scoring predictions collected between {hour_start} and {hour_end}"
            )
            
            tasks = await self.get_tasks_for_hour(hour_start, hour_end, validator)
            if not tasks:
                logger.info(f"No predictions found for collection hour {query_hour}")
                return (0, 0)

            current_time = datetime.datetime.now(datetime.timezone.utc)
            success_count = await self.score_tasks(tasks, ground_truth_value, current_time)
            logger.info(f"Completed scoring {len(tasks)} predictions from hour {query_hour}")
            
            return (len(tasks), success_count)

        except Exception as e:
            logger.error(f"Error processing scores: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @task(
        name="get_tasks_for_hour",
        retries=2,
        retry_delay_seconds=15,
        description="Fetch tasks submitted within a specific UTC time range from the database"
    )
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
            
        Raises:
            Exception: If there's an error fetching tasks from the database
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
            if results:
                for row in results:
                    task = {
                        "id": row["id"],
                        "miner_uid": row["miner_uid"],
                        "miner_hotkey": row["miner_hotkey"],
                        "predicted_value": row["predicted_value"],
                        "query_time": row["query_time"]
                    }
                    tasks.append(task)
                logger.info(f"Found {len(tasks)} tasks for the specified time range")
            else:
                logger.info("No tasks found for the specified time range")
            
            return tasks

        except Exception as e:
            logger.error(f"Error fetching tasks for hour: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @task(
        name="fetch_ground_truth",
        retries=3,
        retry_delay_seconds=30,
        description="Fetch the ground truth DST value for the current UTC hour"
    )
    async def fetch_ground_truth(self):
        """
        Fetches the ground truth DST value for the current UTC hour.

        Returns:
            float: The real-time DST value
            
        Raises:
            Exception: If there's an error fetching the ground truth data
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
            logger.error(f"Error fetching ground truth: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @task(
        name="move_task_to_history",
        retries=2,
        retry_delay_seconds=15,
        description="Archive a completed task in the history table"
    )
    async def move_task_to_history(
        self, task: dict, ground_truth_value: float, score: float, score_time: datetime.datetime
    ):
        """
        Archives a completed task in the history table.

        Args:
            task (dict): Task details including predicted_value and query_time
            ground_truth_value (float): The actual observed value
            score (float): The calculated score
            score_time (datetime): When the task was scored
            
        Raises:
            Exception: If there's an error archiving the task
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
                    "predicted_value": task["predicted_value"],
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
            logger.error(f"Error moving task to history: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    @task(
        name="score_tasks",
        retries=2,
        retry_delay_seconds=30,
        description="Score a batch of prediction tasks against ground truth"
    )
    async def score_tasks(self, tasks: List[dict], ground_truth_value: float, current_time: datetime.datetime) -> int:
        """
        Score a batch of prediction tasks against ground truth and archive them.

        Args:
            tasks (List[dict]): List of tasks to score
            ground_truth_value (float): The actual observed value
            current_time (datetime): Current timestamp for scoring
            
        Returns:
            int: Number of successfully scored tasks
            
        Raises:
            Exception: If there's an error scoring the tasks
        """
        try:
            if not tasks:
                logger.info("No predictions to score for the last hour.")
                return 0

            success_count = 0
            scored_tasks = []
            
            for task in tasks:
                try:
                    predicted_value = task["predicted_value"]
                    score = self.scoring_mechanism.calculate_score(
                        predicted_value, ground_truth_value
                    )
                    await self.move_task_to_history(
                        task, ground_truth_value, score, current_time
                    )
                    task["score"] = score  # Add score to task dict
                    scored_tasks.append(task)
                    success_count += 1
                    logger.info(
                        f"Task scored and archived: task_id={task['id']}, score={score}"
                    )
                except Exception as e:
                    logger.error(f"Error processing task {task['id']}: {str(e)}")
                    logger.error(traceback.format_exc())

            current_hour = datetime.datetime.now(datetime.timezone.utc).hour
            await self.build_score_row(current_hour, scored_tasks)
            
            return success_count

        except Exception as e:
            logger.error(f"Error scoring tasks batch: {str(e)}")
            logger.error(traceback.format_exc())
            raise

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

    @task(
        name="build_score_row",
        retries=2,
        retry_delay_seconds=15,
        description="Build a score row from recent tasks and historical data"
    )
    async def build_score_row(self, current_hour: Union[int, datetime.datetime], recent_tasks: Optional[List[dict]] = None) -> Dict[str, Any]:
        """
        Build a score row from recent tasks and historical data.
        The task_id should be the verification time (when we can score the predictions).

        Args:
            current_hour (Union[int, datetime.datetime]): Current hour timestamp or hour integer
            recent_tasks (List[dict], optional): List of recently scored tasks

        Returns:
            Dict[str, Any]: Dictionary containing task_name, task_id, and scores array
            
        Raises:
            Exception: If there's an error building the score row
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
            logger.info(f"Building score row for prediction_time: {prediction_time}, verification_time: {verification_time}")

            # Initialize scores array with NaN values
            scores = [float("nan")] * 256

            # Get mapping of hotkeys to UIDs from node_table
            query = """
            SELECT uid, hotkey FROM node_table 
            WHERE hotkey IS NOT NULL
            """
            miner_mappings = await self.db_manager.fetch_all(query)
            hotkey_to_uid = {row["hotkey"]: row["uid"] for row in miner_mappings}
            logger.info(f"Found {len(hotkey_to_uid)} miner mappings")

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
            logger.info(f"Found {len(historical_tasks)} historical tasks")

            # Process historical tasks
            historical_count = 0
            for task in historical_tasks:
                miner_hotkey = task["miner_hotkey"]
                if miner_hotkey in hotkey_to_uid:
                    uid = hotkey_to_uid[miner_hotkey]
                    scores[uid] = task["score"]
                    historical_count += 1

            # Process recent tasks (overwrite historical scores if exists)
            recent_count = 0
            if recent_tasks:
                for task in recent_tasks:
                    miner_hotkey = task["miner_hotkey"]
                    if miner_hotkey in hotkey_to_uid:
                        uid = hotkey_to_uid[miner_hotkey]
                        scores[uid] = task.get("score", float("nan"))
                        recent_count += 1

            logger.info(f"Processed {historical_count} historical scores and {recent_count} recent scores")
            
            score_row = {
                "task_name": "geomagnetic",
                "task_id": verification_time.timestamp(),
                "scores": scores
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
            logger.error(f"Error building score row: {str(e)}")
            logger.error(traceback.format_exc())
            raise

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
