from gaia.tasks.base.task import Task
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_metadata import GeomagneticMetadata
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_inputs import GeomagneticInputs
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import GeomagneticPreprocessing
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_scoring_mechanism import GeomagneticScoringMechanism
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_outputs import GeomagneticOutputs
from gaia.tasks.defined_tasks.geomagnetic.utils.process_geomag_data import get_latest_geomag_data
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
import datetime
import numpy as np
import pandas as pd


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
            outputs=GeomagneticOutputs()
        )

    ############################################################
    # Validator execution method
    ############################################################

    def validator_execute(self):
        """
        Executes the validator workflow:
        - Fetches predictions for the last UTC hour.
        - Fetches ground truth data for the current UTC hour.
        - Scores predictions against the ground truth.
        - Archives scored predictions in the history table or file.
        """
        current_time = datetime.datetime.utcnow()
        print(f"Validator executing at {current_time.isoformat()}")

        # Step 1: Calculate the time range for fetching tasks
        last_hour_start = current_time - datetime.timedelta(hours=1)
        last_hour_end = current_time
        print(f"Fetching predictions between {last_hour_start} and {last_hour_end}")

        # Step 2: Fetch tasks for the last hour
        tasks = self.get_tasks_for_hour(last_hour_start, last_hour_end)
        if not tasks:
            print("No predictions to score for the last hour.")
            return

        # Step 3: Fetch ground truth for the current hour
        ground_truth_value = self.fetch_ground_truth()
        if ground_truth_value is None:
            print("Failed to fetch ground truth data.")
            return

        # Step 4: Score predictions and move to history
        for task in tasks:
            try:
                predicted_value = task["predicted_values"]
                score = self.scoring_mechanism.calculate_score(predicted_value, ground_truth_value)
                self.move_task_to_history(task, ground_truth_value, score, current_time)
                print(f"Task scored and archived at {current_time.isoformat()}")
            except Exception as e:
                print(f"Error processing task {task['id']}: {e}")


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
                    "predicted_values": row[2],  # Assuming a single prediction per miner
                    "query_time": row[3]
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
            current_hour_data = dst_data[dst_data['timestamp'].dt.hour == current_time.hour]

            if current_hour_data.empty:
                print("No ground truth data available for the current hour.")
                return None

            # Extract the ground truth value
            ground_truth_value = int(current_hour_data['value'].iloc[0])
            print(f"Ground truth value for hour {current_time.hour}: {ground_truth_value}")
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
            "score_time": score_time.isoformat()
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

    def miner_execute(self):
        """
        Executes the miner workflow: queries miners, collects responses,
        and adds predictions to task queue.
        """
        # Step 1: Query miners and collect predictions
        predictions = self.query_miners()  # This would call miners to get their predictions

        # Step 2: Add predictions to task queue
        query_time = datetime.datetime.utcnow()
        self.add_task_to_queue(predictions, query_time)
        print(f"Added task to queue at {query_time} with {len(predictions)} predictions.")

    def query_miners(self):
        """
        Simulates querying miners and collecting predictions. Replace with actual miner querying logic.

        Returns:
            np.ndarray: Array of 256 predictions from miners.
        """
        # Simulate getting predictions from miners
        predictions = np.random.randint(-100, 100, size=256)  # Example of simulated predictions
        return predictions

    def add_task_to_queue(self, predictions, query_time):
        """
        Adds a new task to the task queue (database or other storage).

        Args:
            predictions (np.ndarray): Array of predictions from miners.
            query_time (datetime): The time the task was added.
        """
        # Database insertion logic here
        # Example: db.insert("task_queue", {"predicted_values": predictions, "query_time": query_time, "status": "pending"})
        pass  # Replace with actual database code
