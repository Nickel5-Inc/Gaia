from tasks.base.components.scoring_mechanism import ScoringMechanism

class GeomagneticScoringMechanism(ScoringMechanism):
    def calculate_score(self, predicted_value: int, actual_value: int):
        """
        Calculates the score based on the absolute error between the miner's
        predicted DST value and the actual DST value.

        Args:
            predicted_value (int): The DST value predicted by the miner.
            actual_value (int): The real DST value provided by the validator.

        Returns:
            float or None: The absolute error as the score, or None if inputs are invalid.
        """
        if predicted_value is None or actual_value is None:
            print("Error: Predicted or actual value is None.")
            return None

        return abs(predicted_value - actual_value)

