from gaia.tasks.base.components.scoring_mechanism import ScoringMechanism
from fiber.logging_utils import get_logger
import math
import asyncio
from datetime import timezone, datetime
from pydantic import Field
from typing import Any, List, Dict, Optional
from prefect import task

logger = get_logger(__name__)

class GeomagneticScoringMechanism(ScoringMechanism):
    """
    Updated scoring mechanism for geomagnetic tasks.
    Scores are now inverted, so higher scores represent better predictions.
    Handles invalid scores (`NaN`) gracefully.
    Includes functionality to capture and save miner predictions and scores.
    """

    db_manager: Any = Field(
        None,
        description="Database manager for the scoring mechanism"
    )

    def __init__(self, db_manager):
        super().__init__(
            name="Geomagnetic Scoring",
            description="Updated scoring mechanism for geomagnetic tasks with improved normalization.",
            db_manager=db_manager
        )

    @task(
        name="calculate_score",
        retries=2,
        retry_delay_seconds=15,
        description="Calculate score for a single prediction"
    )
    def calculate_score(self, predicted_value: float, actual_value: float) -> float:
        """
        Calculates the score for a miner's prediction based on the deviation from ground truth.

        Args:
            predicted_value (float): The predicted DST value from the miner
            actual_value (float): The ground truth DST value

        Returns:
            float: A higher score indicates a better prediction
            
        Raises:
            ValueError: If inputs are not valid numbers
        """
        if not isinstance(predicted_value, (int, float)) or not isinstance(actual_value, (int, float)):
            logger.error(f"Invalid input types: predicted={type(predicted_value)}, actual={type(actual_value)}")
            return float("nan")

        try:
            # Normalize the actual value to the same scale as the prediction
            actual_value = actual_value / 100.0

            # Calculate the score
            return 1 / (1 + abs(predicted_value - actual_value))
        except Exception as e:
            logger.error(f"Error calculating score: {e}")
            return float("nan")

    @task(
        name="normalize_scores",
        retries=2,
        retry_delay_seconds=15,
        description="Normalize a batch of scores to [0,1] range"
    )
    def normalize_scores(self, scores: List[float]) -> List[float]:
        """
        Normalizes scores to the range [0, 1].

        Args:
            scores (List[float]): List of raw scores

        Returns:
            List[float]: Normalized scores
            
        Raises:
            ValueError: If scores list is empty or invalid
        """
        try:
            valid_scores = [score for score in scores if not math.isnan(score)]

            if not valid_scores:
                logger.warning("All scores are NaN. Returning default normalized scores.")
                return [0.0 for _ in scores]

            min_score = min(valid_scores)
            max_score = max(valid_scores)

            if max_score == min_score:
                return [1.0 for _ in scores]

            return [(score - min_score) / (max_score - min_score) if not math.isnan(score) else 0.0 for score in scores]
        except Exception as e:
            logger.error(f"Error normalizing scores: {e}")
            raise

    @task(
        name="save_predictions",
        retries=3,
        retry_delay_seconds=30,
        description="Save miner predictions to database"
    )
    async def save_predictions(self, miner_predictions: List[Dict[str, Any]]) -> None:
        """
        Save miner predictions to the geomagnetic_predictions table.

        Args:
            miner_predictions (List[Dict[str, Any]]): List of miner prediction dictionaries containing:
                - miner_uid
                - miner_hotkey
                - predicted_value
                
        Raises:
            Exception: If there's an error saving to database
        """
        try:
            query = """
            INSERT INTO geomagnetic_predictions (id, miner_uid, miner_hotkey, predicted_value, query_time, status)
            VALUES (:id, :miner_uid, :miner_hotkey, :predicted_value, CURRENT_TIMESTAMP, 'pending')
            ON CONFLICT (id) DO NOTHING
            """
            for prediction in miner_predictions:
                await self.db_manager.execute(query, {
                    "id": prediction["id"],
                    "miner_uid": prediction["miner_uid"],
                    "miner_hotkey": prediction["miner_hotkey"],
                    "predicted_value": prediction["predicted_value"]
                })
            logger.info(f"Successfully saved {len(miner_predictions)} predictions.")
        except Exception as e:
            logger.error(f"Error saving predictions to the database: {e}")
            raise

    @task(
        name="save_scores",
        retries=3,
        retry_delay_seconds=30,
        description="Save calculated scores to database"
    )
    async def save_scores(self, miner_scores: List[Dict[str, Any]]) -> None:
        """
        Save miner scores to the geomagnetic_history table.

        Args:
            miner_scores (List[Dict[str, Any]]): List of miner score dictionaries containing:
                - miner_uid
                - miner_hotkey
                - query_time
                - predicted_value
                - ground_truth_value
                - score
                
        Raises:
            Exception: If there's an error saving to database
        """
        try:
            for score in miner_scores:
                await self.db_manager.execute(
                    """
                    INSERT INTO geomagnetic_history 
                    (miner_uid, miner_hotkey, query_time, predicted_value, ground_truth_value, score, scored_at)
                    VALUES (:miner_uid, :miner_hotkey, :query_time, :predicted_value, :ground_truth_value, :score, CURRENT_TIMESTAMP)
                    """,
                    {
                        "miner_uid": score["miner_uid"],
                        "miner_hotkey": score["miner_hotkey"],
                        "query_time": score["query_time"],
                        "predicted_value": score["predicted_value"],
                        "ground_truth_value": score["ground_truth_value"],
                        "score": score["score"]
                    }
                )
                logger.debug(f"Saved score for miner {score['miner_hotkey']}")
            
            logger.info(f"Successfully saved {len(miner_scores)} scores.")
            
        except Exception as e:
            logger.error(f"Error saving scores to the database: {e}")
            raise

    @task(
        name="score_batch",
        retries=2,
        retry_delay_seconds=30,
        description="Score a batch of predictions against ground truth"
    )
    async def score(self, predictions: List[Dict[str, Any]], ground_truth: float) -> List[Dict[str, Any]]:
        """
        Scores multiple predictions against the ground truth and saves both predictions and scores.

        Args:
            predictions (List[Dict[str, Any]]): List of prediction dictionaries containing:
                - id
                - miner_uid
                - miner_hotkey
                - predicted_value
            ground_truth (float): The ground truth DST value

        Returns:
            List[Dict[str, Any]]: List of score dictionaries with scores and additional metadata
            
        Raises:
            ValueError: If predictions or ground truth are invalid
            Exception: If there's an error in scoring process
        """
        try:
            if not isinstance(predictions, list) or not isinstance(ground_truth, (int, float)):
                raise ValueError("Invalid predictions or ground truth format.")

            miner_scores = []
            for prediction in predictions:
                score_value = self.calculate_score(prediction["predicted_value"], ground_truth)
                miner_scores.append({
                    "miner_uid": prediction["miner_uid"],
                    "miner_hotkey": prediction["miner_hotkey"],
                    "query_time": prediction.get("query_time", datetime.now(timezone.utc)),
                    "predicted_value": prediction["predicted_value"],
                    "ground_truth_value": ground_truth,
                    "score": score_value
                })

            await self.save_predictions(predictions)
            await self.save_scores(miner_scores)

            return miner_scores
        except Exception as e:
            logger.error(f"Error in scoring process: {e}")
            raise
