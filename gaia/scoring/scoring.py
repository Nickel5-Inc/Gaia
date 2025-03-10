import math
from typing import List


def sigmoid(x: float, k: float = 20, x0: float = 0.93) -> float:
    """
    Apply a sigmoid transformation to the raw geo score.

    Parameters:
      x  : Raw geo score (expected in 0-1 range)
      k  : Controls the steepness of the sigmoid
      x0 : The inflection point

    Returns:
      Transformed score in the range (0,1)
    """
    return 1 / (1 + math.exp(-k * (x - x0)))


class Scoring:
    """
    Scoring Class - Instantiated by the validator.
    Handles aggregation of scores from multiple miners and calculates final weights.
    """

    def __init__(self, db_manager):
        self.db_manager = db_manager

    def score(self):
        pass

    def _fetch_task_scores(self):
        pass

    def _aggregate_scores(self):
        pass

    def _calculate_final_weights(self):
        pass

    def aggregate_task_scores(self, geomagnetic_scores: dict, soil_scores: dict) -> List[float]:
        """
        Combines scores from multiple tasks into final weights using a 60/40 weighting scheme,
        with a sigmoid transformation applied to geo scores.

        - Full miners (both scores available):
            Effective Score = 0.40 * sigmoid(geo_score) + 0.60 * soil_score
        - Immune miners (missing soil):
            Effective Score = 0.40 * sigmoid(geo_score)
        - If both scores are missing, the effective score is 0.
        """
        weights = [0.0] * 256

        for idx in range(256):
            geo_score = geomagnetic_scores.get(idx, float('nan'))
            soil_score = soil_scores.get(idx, float('nan'))

            if geo_score == 0.0 and soil_score == 0.0:
                weights[idx] = 0.0
                continue  # Skip further calculations

            if math.isnan(geo_score) and math.isnan(soil_score):
                weights[idx] = 0.0
            elif math.isnan(geo_score):
                # Only soil available
                weights[idx] = 0.60 * soil_score
            elif math.isnan(soil_score):
                # Only geo available; apply sigmoid and take 40%
                weights[idx] = 0.40 * sigmoid(geo_score, k=20, x0=0.93)
            else:
                # Both scores available
                weights[idx] = 0.40 * sigmoid(geo_score, k=20, x0=0.93) + 0.60 * soil_score

        return weights
