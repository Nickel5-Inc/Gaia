from gaia.tasks.base.components.scoring_mechanism import ScoringMechanism


class GeomagneticScoringMechanism(ScoringMechanism):
    """
    Scoring mechanism for geomagnetic tasks.

    This mechanism scores miner predictions based on the deviation
    from the ground truth DST value.
    """

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
