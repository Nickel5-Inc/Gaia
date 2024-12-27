from pydantic import BaseModel, Field
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import math
import asyncio
from datetime import datetime, timezone
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class ScoringMechanism(BaseModel, ABC):
    """
    Base class for all scoring mechanisms.

    Provides a modular interface for scoring tasks with shared methods for:
    - Score calculation
    - Normalization
    - Prediction and score saving

    Should be extended to define specific scoring mechanisms for different tasks.
    """

    name: str = Field(..., description="Name of the scoring mechanism")
    description: str = Field(..., description="Description of how scoring works")
    normalize_score: bool = Field(default=True, description="Whether to normalize the score")
    max_score: float = Field(default=100.0, description="Maximum possible score")
    db_manager: Any = Field(default=None, description="Database manager for saving predictions and scores")

    @abstractmethod
    def calculate_score(self, predicted_value: float, actual_value: float) -> float:
        """
        Calculate the score for a single prediction.

        Args:
            predicted_value (float): The predicted value.
            actual_value (float): The ground truth value.

        Returns:
            float: The calculated score.
        """
        pass

    def normalize_scores(self, scores: List[float]) -> List[float]:
        """
        Normalize scores to the range [0, 1].

        Args:
            scores (List[float]): List of raw scores.

        Returns:
            List[float]: Normalized scores.
        """
        valid_scores = [score for score in scores if not math.isnan(score)]

        if not valid_scores:
            logger.warning("All scores are NaN. Returning default normalized scores.")
            return [0.0 for _ in scores]

        min_score = min(valid_scores)
        max_score = max(valid_scores)

        if max_score == min_score:
            return [1.0 for _ in scores]  # All valid scores are the same

        return [(score - min_score) / (max_score - min_score) if not math.isnan(score) else 0.0 for score in scores]

    async def save_predictions(self, predictions: List[Dict[str, Any]], table_name: str):
        """
        Save predictions to the specified database table.

        Args:
            predictions (List[Dict[str, Any]]): List of prediction dictionaries.
            table_name (str): Name of the table to save predictions.
        """
        try:
            query = f"""
            INSERT INTO {table_name} (id, miner_uid, miner_hotkey, predicted_value, query_time, status)
            VALUES (:id, :miner_uid, :miner_hotkey, :predicted_value, CURRENT_TIMESTAMP, 'pending')
            ON CONFLICT (id) DO NOTHING
            """
            for prediction in predictions:
                await self.db_manager.execute(query, prediction)
            logger.info(f"Successfully saved {len(predictions)} predictions to {table_name}.")
        except Exception as e:
            logger.error(f"Error saving predictions to {table_name}: {e}")

    async def save_scores(self, scores: List[Dict[str, Any]], table_name: str):
        """
        Save scores to the specified database table.

        Args:
            scores (List[Dict[str, Any]]): List of score dictionaries.
            table_name (str): Name of the table to save scores.
        """
        try:
            query = f"""
            INSERT INTO {table_name} (miner_uid, miner_hotkey, query_time, predicted_value, ground_truth_value, score, scored_at)
            VALUES (:miner_uid, :miner_hotkey, :query_time, :predicted_value, :ground_truth_value, :score, CURRENT_TIMESTAMP)
            """
            for score in scores:
                await self.db_manager.execute(query, score)
            logger.info(f"Successfully saved {len(scores)} scores to {table_name}.")
        except Exception as e:
            logger.error(f"Error saving scores to {table_name}: {e}")

    async def score(self, predictions: List[Dict[str, Any]], ground_truth: float, prediction_table: str, score_table: str):
        """
        Score multiple predictions, normalize scores, and save them.

        Args:
            predictions (List[Dict[str, Any]]): List of prediction dictionaries containing:
                - id
                - miner_uid
                - miner_hotkey
                - predicted_value
            ground_truth (float): The ground truth value.
            prediction_table (str): Table name for saving predictions.
            score_table (str): Table name for saving scores.

        Returns:
            List[Dict[str, Any]]: List of score dictionaries with scores and metadata.
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

            # Save predictions
            await self.save_predictions(predictions, prediction_table)

            # Save scores
            await self.save_scores(miner_scores, score_table)

            return miner_scores
        except Exception as e:
            logger.error(f"Error in scoring process: {e}")
            return []
