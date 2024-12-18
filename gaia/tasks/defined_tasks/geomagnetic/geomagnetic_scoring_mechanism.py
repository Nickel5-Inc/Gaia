from gaia.tasks.base.components.scoring_mechanism import ScoringMechanism


class GeomagneticScoringMechanism(ScoringMechanism):
    """
    Scoring mechanism for geomagnetic tasks.

    This mechanism scores miner predictions based on the deviation
    from the ground truth DST value.
    """

    def __init__(self):
        super().__init__(
            name="Geomagnetic Scoring",
            description="Scoring mechanism for geomagnetic tasks based on deviation from ground truth.",
        )

    def calculate_score(self, predicted_value, actual_value):
        """
        Calculates the score for a miner's prediction based on deviation.

        Args:
            predicted_value (float): The predicted DST value from the miner.
            actual_value (float): The ground truth DST value.

        Returns:
            float: The absolute deviation between the predicted value and the ground truth.
        """
        if not isinstance(predicted_value, (int, float)):
            raise ValueError("Predicted value must be an integer or a float.")
        if not isinstance(actual_value, (int, float)):
            raise ValueError("Actual value must be an integer or a float.")

        # Calculate the absolute deviation
        return abs(predicted_value - actual_value)

    def score(self, predictions, ground_truth):
        """
        Scores multiple predictions against the ground truth.

        Args:
            predictions (list[float]): List of predicted values from miners.
            ground_truth (float): The ground truth DST value.

        Returns:
            list[tuple]: List of tuples containing (predicted_value, score).
        """
        if not isinstance(predictions, list):
            raise ValueError("Predictions must be a list of float or int values.")
        if not isinstance(ground_truth, (int, float)):
            raise ValueError("Ground truth must be an integer or a float.")

        # Calculate scores for all predictions and return along with predictions
        return [(pred, self.calculate_score(pred, ground_truth)) for pred in predictions]

    def prepare_history_records(self, predictions, ground_truth, miner_metadata):
        """
        Prepare data for insertion into the geomagnetic_history table.

        Args:
            predictions (list[float]): List of predicted values from miners.
            ground_truth (float): The ground truth DST value.
            miner_metadata (list[dict]): List of dictionaries containing miner metadata (e.g., UID, hotkey, etc.).

        Returns:
            list[dict]: List of records ready to be inserted into the geomagnetic_history table.
        """
        if len(predictions) != len(miner_metadata):
            raise ValueError("Number of predictions must match number of miner metadata entries.")

        # Calculate scores
        scored_predictions = self.score(predictions, ground_truth)

        # Prepare records
        records = []
        for i, (predicted_value, score) in enumerate(scored_predictions):
            miner = miner_metadata[i]
            records.append({
                "miner_uid": miner["uid"],
                "miner_hotkey": miner["hotkey"],
                "predicted_value": predicted_value,
                "score": score,
                "ground_truth": ground_truth
            })

        return records
