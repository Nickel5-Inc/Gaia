from gaia.tasks.base.components.scoring_mechanism import ScoringMechanism
from fiber.logging_utils import get_logger
import math
import asyncio
from datetime import timezone, datetime
from pydantic import Field
from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text

logger = get_logger(__name__)

class GeomagneticScoringMechanism(ScoringMechanism):
    """
    Updated scoring mechanism for geomagnetic tasks.
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

    def calculate_score(self, predicted_value, actual_value):
        """
        Calculates the score for a miner's prediction based on the deviation from ground truth.
        Calculates the score for a miner's prediction based on the deviation from ground truth.

        Args:
            predicted_value (float): The predicted DST value from the miner.
            actual_value (float): The ground truth DST value.

        Returns:
            float: A higher score indicates a better prediction.
        """
        if not isinstance(predicted_value, (int, float)) or not isinstance(actual_value, (int, float)):
            return float("nan")

        try:
            # Normalize the actual value to the same scale as the prediction
            actual_value = actual_value / 100.0

            # Validate that the prediction is within the normalized range (-5, 5)
            if not (-5 <= predicted_value <= 5):
                logger.warning(f"Out-of-range prediction: {predicted_value}. Expected range: (-5, 5).")
                return 0.0  # Penalize invalid predictions

            # Calculate the score
            return max(0, 1 - abs(predicted_value - actual_value))
        except Exception as e:
            logger.error(f"Error calculating score: {e}")
            return float("nan")

    def normalize_scores(self, scores):
        """
        Normalizes scores to the range [0, 1].

        Args:
            scores (list[float]): List of raw scores.

        Returns:
            list[float]: Normalized scores.
        """
        valid_scores = [score for score in scores if not math.isnan(score)]

        if not valid_scores:
            logger.warning("All scores are NaN. Returning default normalized scores.")
            return [0.0 for _ in scores]

        min_score = min(valid_scores)
        max_score = max(valid_scores)

        if max_score == min_score:
            return [1.0 for _ in scores]

        return [(score - min_score) / (max_score - min_score) if not math.isnan(score) else 0.0 for score in scores]

    async def save_predictions(self, miner_predictions):
        """
        Save miner predictions to the geomagnetic_predictions table.

        Args:
            miner_predictions (list[dict]): List of miner prediction dictionaries containing:
                - miner_uid
                - miner_hotkey
                - predicted_value
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

    async def save_scores(self, miner_scores):
        """
        Save miner scores to the geomagnetic_history table.

        Args:
            miner_scores (list[dict]): List of miner score dictionaries containing:
                - miner_uid
                - miner_hotkey
                - query_time
                - predicted_value
                - ground_truth_value
                - score
        """
        try:
            # Save each score individually for better error handling and connection management
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

    async def score(self, predictions, ground_truth):
        """
        Scores multiple predictions against the ground truth and saves both predictions and scores.
        Scores multiple predictions against the ground truth and saves both predictions and scores.

        Args:
            predictions (list[dict]): List of prediction dictionaries containing:
                - id
                - miner_uid
                - miner_hotkey
                - predicted_value
            ground_truth (float): The ground truth DST value.
            predictions (list[dict]): List of prediction dictionaries containing:
                - id
                - miner_uid
                - miner_hotkey
                - predicted_value
            ground_truth (float): The ground truth DST value.

        Returns:
            list[dict]: List of score dictionaries with scores and additional metadata.
            list[dict]: List of score dictionaries with scores and additional metadata.
        """
        try:
            if not isinstance(predictions, list) or not isinstance(ground_truth, (int, float)):
                raise ValueError("Invalid predictions or ground truth format.")

            # Normalize the ground truth
            normalized_ground_truth = ground_truth / 100.0

            miner_scores = []
            for prediction in predictions:
                score_value = self.calculate_score(prediction["predicted_value"], normalized_ground_truth)
                miner_scores.append({
                    "miner_uid": prediction["miner_uid"],
                    "miner_hotkey": prediction["miner_hotkey"],
                    "query_time": prediction.get("query_time", datetime.now(timezone.utc)),
                    "predicted_value": prediction["predicted_value"],
                    "ground_truth_value": normalized_ground_truth,
                    "score": score_value
                })

            await self.save_predictions(predictions)

            await self.save_scores(miner_scores)

            return miner_scores
        except Exception as e:
            logger.error(f"Error in scoring process: {e}")
            return []
