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
import datetime
import numpy as np
import pandas as pd
import asyncio
from uuid import uuid4


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

    def __init__(self):
        super().__init__(
            name="GeomagneticTask",
            description="Task for geomagnetic data processing",
            task_type="atomic",
            metadata=GeomagneticMetadata(),
            inputs=GeomagneticInputs(),
            preprocessing=GeomagneticPreprocessing(),
            scoring_mechanism=GeomagneticScoringMechanism(),
            outputs=GeomagneticOutputs(),
        )

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
                # Step 1: Align to the top of the next hour
                current_time = datetime.datetime.now(datetime.timezone.utc)
                next_hour = current_time.replace(
                    minute=0, second=0, microsecond=0
                ) + datetime.timedelta(hours=1)
                sleep_duration = (next_hour - current_time).total_seconds()

                print(
                    f"Sleeping until the next hour: {next_hour.isoformat()} (in {sleep_duration} seconds)"
                )
                await asyncio.sleep(
                    sleep_duration
                )  # Wait until the top of the next hour

                # Step 2: Fetch Latest Geomagnetic Data
                timestamp, dst_value = await get_latest_geomag_data()
                print(
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
                endpoint = "geomagnetic-request/"

                # Step 4: Query Miners
                responses = await validator.query_miners(payload_template, endpoint)
                print(f"Collected responses from miners: {len(responses)}")

                # Step 5: Store Predictions in Queue
                current_hour_start = next_hour.replace(
                    minute=0, second=0, microsecond=0
                ) - datetime.timedelta(hours=1)
                for response in responses:
                    self.add_task_to_queue(
                        predictions=response.get("predicted_values"),
                        query_time=current_hour_start,
                    )

                # Step 6: Fetch Ground Truth for Current Hour
                ground_truth_value = self.fetch_ground_truth()
                if ground_truth_value is None:
                    print("Ground truth data not available. Skipping scoring.")
                    continue

                # Step 7: Score Predictions and Archive Results
                last_hour_start = current_hour_start
                last_hour_end = next_hour.replace(minute=0, second=0, microsecond=0)

                print(
                    f"Fetching predictions between {last_hour_start} and {last_hour_end}"
                )
                tasks = self.get_tasks_for_hour(last_hour_start, last_hour_end)

                if not tasks:
                    print("No predictions to score for the last hour.")
                    continue

                for task in tasks:
                    try:
                        predicted_value = task["predicted_values"]
                        score = self.scoring_mechanism.calculate_score(
                            predicted_value, ground_truth_value
                        )
                        self.move_task_to_history(
                            task, ground_truth_value, score, current_time
                        )
                        print(
                            f"Task scored and archived: task_id={task['id']}, score={score}"
                        )
                    except Exception as e:
                        print(f"Error processing task {task['id']}: {e}")

            except Exception as e:
                print(f"Unexpected error in validator_execute loop: {e}")
                await asyncio.sleep(60)  # Retry after a short delay

    def get_tasks_for_hour(self, start_time, end_time):
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
                WHERE query_time >= %s AND query_time < %s AND status = 'pending';
            """
            params = (start_time, end_time)

            # Execute the query and fetch results
            results = db_manager.execute_query(query, params)

            # Convert results to a list of task dictionaries
            tasks = [
                {
                    "id": row[0],
                    "miner_id": row[1],
                    "predicted_values": row[
                        2
                    ],  # Assuming a single prediction per miner
                    "query_time": row[3],
                }
                for row in results
            ]

            print(f"Fetched {len(tasks)} tasks between {start_time} and {end_time}")
            return tasks

        except Exception as e:
            print(f"Error fetching tasks for hour: {e}")
            return []

    def fetch_ground_truth(self):
        """
        Fetches the ground truth DST value for the current UTC hour.

        Returns:
            int: The real-time DST value, or None if fetching fails.
        """
        try:
            # Get the current UTC time
            current_time = datetime.datetime.utcnow()
            print(f"Fetching ground truth for UTC hour: {current_time.hour}")

            # Fetch the most recent geomagnetic data
            dst_data = get_latest_geomag_data()

            # Filter the data to find the record for the current hour
            current_hour_data = dst_data[
                dst_data["timestamp"].dt.hour == current_time.hour
            ]

            if current_hour_data.empty:
                print("No ground truth data available for the current hour.")
                return None

            # Extract the ground truth value
            ground_truth_value = int(current_hour_data["value"].iloc[0])
            print(
                f"Ground truth value for hour {current_time.hour}: {ground_truth_value}"
            )
            return ground_truth_value

        except Exception as e:
            print(f"Error fetching ground truth: {e}")
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
        print(f"Archiving task to history: {history_record}")
        # Example: db.insert("geomagnetic_history", history_record)

        # Remove task from queue
        print(f"Removing task from queue: {task['id']}")
        # Example: db.delete("geomagnetic_predictions", where={"id": task["id"]})

    ############################################################
    # Miner execution method
    ############################################################

    def miner_execute(self, data=None):
        """
        Executes the miner workflow: preprocesses data, runs model inference,
        and adds predictions to the task queue.
        """
        try:
            # Step 1: Preprocess data if provided
            if data:
                processed_data = self.miner_preprocessing.process_miner_data(data)
            else:
                processed_data = None  # Handle cases with no input data

            # Step 2: Run model inference
            predictions = self.run_model_inference(processed_data)

            # Step 3: Add predictions to the task queue
            query_time = datetime.datetime.utcnow()
            self.add_task_to_queue(predictions, query_time)

            print(f"Miner execution completed successfully at {query_time}")

        except Exception as e:
            print(f"Error in miner execution: {str(e)}")

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
            predictions (np.ndarray): Array of predictions from miners.
            query_time (datetime): The time the task was added.
        """
        try:
            # Use MinerDatabaseManager to insert task into the database
            db_manager = MinerDatabaseManager()
            task_name = "geomagnetic_prediction"
            miner_id = "example_miner_id"  # Replace with the actual miner ID if available

            # Convert predictions to a dictionary or JSON-like structure
            predicted_value = {"predictions": predictions.tolist()}  # Convert ndarray to list

            # Add to the queue
            asyncio.run(
                db_manager.add_to_queue(
                    task_name=task_name,
                    miner_id=miner_id,
                    predicted_value=predicted_value,
                    query_time=query_time,
                )
            )
            print(f"Task added to queue: {task_name} at {query_time}")

        except Exception as e:
            print(f"Error adding task to queue: {e}")

