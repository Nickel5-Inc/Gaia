from tasks.base.task import Task
from tasks.defined_tasks.geomagnetic.geomagnetic_metadata import GeomagneticMetadata
from tasks.defined_tasks.geomagnetic.geomagnetic_inputs import GeomagneticInputs
from tasks.defined_tasks.geomagnetic.geomagnetic_preprocessing import GeomagneticPreprocessing
from tasks.defined_tasks.geomagnetic.geomagnetic_scoring_mechanism import GeomagneticScoringMechanism
from tasks.defined_tasks.geomagnetic.geomagnetic_outputs import GeomagneticOutputs
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
        Executes the validator workflow: checks tasks ready for scoring,
        fetches ground truth, scores predictions, and moves completed
        tasks to the history table.
        """
        # Step 1: Check the task queue for tasks older than 1 hour
        tasks = self.get_pending_tasks()
        current_time = datetime.datetime.utcnow()

        for task in tasks:
            task_age = (current_time - task['query_time']).total_seconds() / 3600
            if task_age >= 1:
                # Step 2: Fetch ground truth data
                ground_truth_value = self.fetch_ground_truth()
                if ground_truth_value is not None:
                    # Step 3: Score predictions
                    scores = self.score_predictions(task['predicted_values'], ground_truth_value)

                    # Step 4: Move scored task to history and remove from queue
                    self.move_task_to_history(task, ground_truth_value, scores, current_time)
                    print(f"Task scored and moved to history at {current_time}")

    def get_pending_tasks(self):
        """
        Fetches pending tasks from the task queue that are ready for scoring.

        Returns:
            list: List of tasks with `predicted_values` and `query_time`.
        """
        # Fetch from database where status is "pending"
        return []  # Replace with actual database fetch

    def fetch_ground_truth(self):
        """
        Fetches the current ground truth DST value.

        Returns:
            int: The real-time DST value.
        """
        # Fetch real-time data from an API or other source
        # Example: ground_truth_value = api.get_latest_dst_value()
        ground_truth_value = np.random.randint(-100, 100)  # Simulated for example
        return ground_truth_value

    def score_predictions(self, predicted_values, ground_truth_value):
        """
        Scores the predictions using a distance function against the ground truth value.

        Args:
            predicted_values (np.ndarray): Array of predicted DST values.
            ground_truth_value (int): Actual DST value.

        Returns:
            np.ndarray: Array of scores for each prediction.
        """
        return np.abs(predicted_values - ground_truth_value)  # Example: absolute error

    def move_task_to_history(self, task, ground_truth_value, scores, score_time):
        """
        Moves a completed task from the queue to the history table.

        Args:
            task (dict): Task details including `predicted_values` and `query_time`.
            ground_truth_value (int): Actual DST value.
            scores (np.ndarray): Array of scores.
            score_time (datetime): The time the scoring was completed.
        """
        # Insert into history table and delete from queue table
        # Example: db.insert("history", {...}); db.delete("task_queue", task_id=task["id"])
        pass  # Replace with actual database code

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
