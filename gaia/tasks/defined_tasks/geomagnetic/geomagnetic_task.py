import traceback
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
        description="Database manager for the task"
    )
    miner_preprocessing: GeomagneticPreprocessing = Field(
        default_factory=GeomagneticPreprocessing,
        description="Preprocessing component for miner"
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
            **data
        )
        self.model = GeoMagBaseModel()  # Initialize GeoMagBaseModel instance
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
                "value": raw_data["value"] / 100.0  # Normalize values
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
            subtasks = [{"timestamp": data["timestamp"], "value": value} for value in data["values"]]
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

                

                logger.info("Fetching latest geomagnetic data...")
                # Step 2: Fetch Latest Geomagnetic Data
                geomag_data = await get_latest_geomag_data()
                timestamp, dst_value = geomag_data  # Unpack after awaiting
                logger.info(
                    f"Fetched latest geomagnetic data: timestamp={timestamp}, value={dst_value}"
                )

                # Step 3: Construct Payload for Miners
                nonce = str(uuid4())  # Generate a unique nonce for this query
                payload_template = {
                    "nonce": nonce,
                    "data": {
                        "name": "Geomagnetic Data",
                        "timestamp": str(timestamp),
                        "value": dst_value,
                    },
                }
                endpoint = "/geomagnetic-request"
                
                logger.info(f"Querying miners with payload: {payload_template}")
                # Step 4: Query Miners
                responses = await validator.query_miners(payload_template, endpoint)
                logger.info(f"Collected responses from miners: {len(responses)}")

                # Step 5: Store Predictions in Queue
                current_hour_start = next_hour.replace(
                    minute=0, second=0, microsecond=0
                ) - datetime.timedelta(hours=1)
                for response in responses:
                    try:
                        # Parse the string response into a dictionary
                        response_data = json.loads(response)
                        predicted_value = response_data.get("predicted_values")
                        miner_id = response_data.get("miner_id")
                        
                        if predicted_value and miner_id:
                            await self.add_prediction_to_queue(
                                miner_id=miner_id,
                                predicted_value=predicted_value,
                                query_time=current_hour_start
                            )
                        else:
                            logger.warning(f"No predicted value in response: {response_data}")
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse miner response as JSON: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing miner response: {e}")
                        logger.error(f'{traceback.format_exc()}')
                        continue

                # Step 6: Fetch Ground Truth for Current Hour
                ground_truth_value = await self.fetch_ground_truth()
                if ground_truth_value is None:
                    logger.warning("Ground truth data not available. Skipping scoring.")
                    continue

                # Step 7: Score Predictions and Archive Results
                last_hour_start = current_hour_start
                last_hour_end = next_hour.replace(minute=0, second=0, microsecond=0)

                logger.info(
                    f"Fetching predictions between {last_hour_start} and {last_hour_end}"
                )
                tasks = await self.get_tasks_for_hour(last_hour_start, last_hour_end)

                if tasks:
                    for task in tasks:
                        try:
                            predicted_value = task["predicted_values"]
                            score = self.scoring_mechanism.calculate_score(
                                predicted_value, ground_truth_value
                            )
                            self.move_task_to_history(
                                task, ground_truth_value, score, current_time
                            )
                            logger.info(
                                f"Task scored and archived: task_id={task['id']}, score={score}"
                            )
                        except Exception as e:
                            logger.error(f"Error processing task {task['id']}: {e}")
                else:
                    logger.info("No predictions to score for the last hour.")
                
                logger.info(
                    f"Sleeping until the next hour: {next_hour.isoformat()} (in {sleep_duration} seconds)"
                )
                if sleep_duration > 0:
                    logger.info(f"Sleeping for {sleep_duration} seconds...")
                    await asyncio.sleep(
                        sleep_duration
                )  # Wait until the top of the next hour

            except Exception as e:
                logger.error(f"Unexpected error in validator_execute loop: {e}")
                logger.error(f'{traceback.format_exc()}')
                await asyncio.sleep(60)  # Retry after a short delay

    async def get_tasks_for_hour(self, start_time, end_time):
        """
        Fetches tasks submitted within a specific UTC time range from the database.

        Args:
            start_time (datetime): Start of the time range (inclusive).
            end_time (datetime): End of the time range (exclusive).

        Returns:
            list: List of task dictionaries containing task details.
        """
        try:
            # Initialize the database manager
            db_manager = ValidatorDatabaseManager()

            # Query the database for tasks in the specified time range
            query = """
                SELECT id, miner_id, predicted_value, query_time 
                FROM geomagnetic_predictions
                WHERE query_time >= :start_time AND query_time < :end_time AND status = 'pending';
            """
            # Pass parameters as a dictionary instead of a tuple
            params = {"start_time": start_time, "end_time": end_time}

            results = await db_manager.fetch_many(query, params)

            # Convert results to a list of task dictionaries
            tasks = [
                {
                    "id": row[0],
                    "miner_id": row[1],
                    "predicted_values": row[2],  # Assuming a single prediction per miner
                    "query_time": row[3],
                }
                for row in results
            ]

            logger.info(f"Fetched {len(tasks)} tasks between {start_time} and {end_time}")
            return tasks

        except Exception as e:
            logger.error(f"Error fetching tasks for hour: {e}")
            logger.error(f'{traceback.format_exc()}')
            return []

    async def fetch_ground_truth(self):
        """
        Fetches the ground truth DST value for the current UTC hour.

        Returns:
            int: The real-time DST value, or None if fetching fails.
        """
        try:
            # Get the current UTC time
            current_time = datetime.datetime.utcnow()
            logger.info(f"Fetching ground truth for UTC hour: {current_time.hour}")

            # Fetch the most recent geomagnetic data
            timestamp, dst_value = await get_latest_geomag_data()

            if timestamp == "N/A" or dst_value == "N/A":
                logger.warning("No ground truth data available for the current hour.")
                return None

            logger.info(
                f"Ground truth value for hour {current_time.hour}: {dst_value}"
            )
            return dst_value

        except Exception as e:
            logger.error(f"Error fetching ground truth: {e}")
            logger.error(f'{traceback.format_exc()}')
            return None

    def move_task_to_history(self, task, ground_truth_value, score, score_time):
        """
        Archives a completed task in the history table or file.

        Args:
            task (dict): Task details including `predicted_values` and `query_time`.
            ground_truth_value (int): The actual DST value for scoring.
            score (float): The calculated deviation score.
            score_time (datetime): The time the task was scored.
        """
        history_record = {
            "miner_id": task["miner_id"],
            "predicted_value": task["predicted_values"],  # Raw prediction
            "ground_truth_value": ground_truth_value,
            "score": score,
            "query_time": task["query_time"].isoformat(),
            "score_time": score_time.isoformat(),
        }

        # Insert into history table or save to JSON
        logger.info(f"Archiving task to history: {history_record}")
        # Example: db.insert("geomagnetic_history", history_record)

        # Remove task from queue
        logger.info(f"Removing task from queue: {task['id']}")
        # Example: db.delete("geomagnetic_predictions", where={"id": task["id"]})

    ############################################################
    # Miner execution method
    ############################################################

    def run_model_inference(self, processed_data):
        """
        Run the GeoMag model inference.

        Args:
            processed_data (dict): Preprocessed input data for the model.

        Returns:
            float: Predicted value.
        """
        try:
            # Prepare input tensor for the model
            input_tensor = torch.tensor(
                [[processed_data['value']]], dtype=torch.float32
            ).unsqueeze(0)  # Add batch and time dimensions

            # Perform prediction using the model
            prediction = self.model.predict(input_tensor)

            # Extract the prediction (convert tensor to float)
            return prediction.item()
        except Exception as e:
            logger.error(f"Error during model inference: {e}")
            return float("nan")  # Return NaN on failure

    def miner_execute(self, data=None):
        """
        Executes the miner workflow: preprocesses data, runs model inference,
        and returns predictions.
        """
        try:
            # Extract data from the request payload
            if data and data.get('data'):
                input_data = {
                    'timestamp': data['data']['timestamp'],
                    'value': float(data['data']['value'])
                }
                processed_data = self.miner_preprocessing.process_miner_data(input_data)
            else:
                logger.error("No data provided in request")
                return None

            # Run model inference
            predictions = self.run_model_inference(processed_data)

            # Format response according to MINER.md requirements
            return {
                "predicted_values": float(predictions),
                "timestamp": input_data['timestamp'],
                "miner_id": "your_miner_id_here"  # Replace with actual miner ID logic
            }

        except Exception as e:
            logger.error(f"Error in miner execution: {str(e)}")
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
            miner_id = "example_miner_id"  # Replace with the actual miner ID if available

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
        miner_id: str,
        predicted_value: float,
        query_time: datetime,
        status: str = "pending"
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
                (miner_id, predicted_value, query_time, status)
                VALUES (:miner_id, :predicted_value, :query_time, :status)
            """
            
            # Prepare parameters
            params = {
                "miner_id": miner_id,
                "predicted_value": float(predicted_value),  # Ensure float type
                "query_time": query_time,
                "status": status
            }

            # Execute the query
            await db_manager.execute(query, params)
            logger.info(f"Added prediction from miner {miner_id} to queue")

        except Exception as e:
            logger.error(f"Error adding prediction to queue: {e}")
            logger.error(f'{traceback.format_exc()}')
            raise

