from gaia.tasks.base.components.scoring_mechanism import ScoringMechanism
from typing import List, Dict, Any
from prefect import task
import logging

logger = logging.getLogger(__name__)


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

    @task(
        name="calculate_score",
        retries=3,
        retry_delay_seconds=60,
        description="Calculate score for a single prediction"
    )
    def calculate_score(self, predicted_value: float, actual_value: float) -> float:
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
            raise ValueError("Actual value must be an integer or float.")

        # Calculate the absolute deviation
        score = abs(predicted_value - actual_value)
        return float(score)

    @task(
        name="score_predictions",
        retries=3,
        retry_delay_seconds=60,
        description="Score multiple predictions against ground truth"
    )
    def score_predictions(
        self,
        predictions: List[Dict[str, Any]],
        ground_truth: float,
        context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Scores multiple predictions against the ground truth.

        Args:
            predictions (list[Dict]): List of prediction dictionaries
            ground_truth (float): The ground truth DST value
            context (Dict): Execution context with timing info

        Returns:
            list[Dict]: List of predictions with scores added
        """
        scored_predictions = []
        
        for pred in predictions:
            try:
                score = self.calculate_score(pred['predicted_value'], ground_truth)
                scored_predictions.append({
                    **pred,
                    'score': score,
                    'ground_truth': ground_truth,
                    'scored_at': context['current_time']
                })
            except Exception as e:
                logger.error(f"Error scoring prediction: {e}")
                continue
                
        return scored_predictions

    @task(
        name="aggregate_scores",
        retries=3,
        retry_delay_seconds=60,
        description="Aggregate scores into final format"
    )
    def aggregate_scores(
        self,
        scored_predictions: List[Dict[str, Any]],
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aggregate scores into final format for weight setting.

        Args:
            scored_predictions (List[Dict]): List of scored predictions
            context (Dict): Execution context

        Returns:
            Dict: Aggregated scores in the format needed for weight setting
        """
        current_hour = context['current_time'].replace(
            minute=0, second=0, microsecond=0
        )
        
        # Initialize scores array
        scores = [float('nan')] * 256
        
        # Fill in scores for miners that responded
        for pred in scored_predictions:
            if pred and 'score' in pred:
                uid = int(pred['miner_uid'])
                scores[uid] = pred['score']
        
        return {
            'task_name': 'geomagnetic',
            'task_id': str(current_hour.timestamp()),
            'scores': scores,
            'status': 'completed'
        }
