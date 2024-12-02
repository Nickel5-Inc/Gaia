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
            actual_value (int): The ground truth DST value.

        Returns:
            float: The absolute deviation between the predicted value and the ground truth.
        """
        if not isinstance(predicted_value, (int, float)):
            raise ValueError("Predicted value must be an integer or a float.")
        if not isinstance(actual_value, int):
            raise ValueError("Actual value must be an integer.")

        # Calculate the absolute deviation
        score = abs(predicted_value - actual_value)
        return score

    def score(self, predictions, ground_truth):
        """
        Scores multiple predictions against the ground truth.

        Args:
            predictions (list[float]): List of predicted values from miners.
            ground_truth (int): The ground truth DST value.

        Returns:
            list[float]: List of scores for each prediction.
        """
        if not isinstance(predictions, list):
            raise ValueError("Predictions must be a list of float or int values.")
        if not isinstance(ground_truth, int):
            raise ValueError("Ground truth must be an integer.")

        # Calculate scores for all predictions
        return [self.calculate_score(pred, ground_truth) for pred in predictions]
