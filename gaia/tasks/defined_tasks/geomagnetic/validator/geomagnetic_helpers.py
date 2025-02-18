from typing import Any, Dict, List
import json
import numpy as np

from fiber.logging_utils import get_logger

logger = get_logger(__name__)


def prepare_subtasks(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Prepare subtasks from the data dictionary by pairing the timestamp with each value.
    """
    try:
        subtasks = [{"timestamp": data["timestamp"], "value": value} for value in data["values"]]
        return subtasks
    except Exception as e:
        logger.error(f"Error in prepare_subtasks: {e}")
        return []


def calculate_score(prediction: float, ground_truth: float) -> float:
    """
    Calculate the score as the absolute difference between prediction and ground truth.
    """
    try:
        return abs(prediction - ground_truth)
    except Exception as e:
        logger.error(f"Error in calculate_score: {e}")
        return float('inf')


async def add_task_to_queue(db_manager: Any, predictions: Any, query_time: Any) -> None:
    """
    Add a new task to the task queue using the provided database manager.

    Args:
        db_manager: The database manager to execute the query.
        predictions: The predictions data (can be a numpy array or other type).
        query_time: The time the task is added.
    """
    try:
        if predictions is None:
            logger.warning("Received None predictions, skipping queue addition")
            return

        if isinstance(predictions, np.ndarray):
            predicted_value = {"predictions": predictions.tolist()}
        else:
            predicted_value = {"predictions": predictions}

        await db_manager.execute(
            """
            INSERT INTO task_queue (task_name, miner_id, predicted_value, query_time)
            VALUES (:task_name, :miner_id, :predicted_value, :query_time)
            """,
            {
                "task_name": "geomagnetic_prediction",
                "miner_id": "example_miner_id",  # Replace with the actual miner ID if available
                "predicted_value": json.dumps(predicted_value),
                "query_time": query_time
            }
        )
        logger.info(f"Task added to queue: geomagnetic_prediction at {query_time}")
    except Exception as e:
        logger.error(f"Error adding task to queue: {e}")
        raise 

async def cleanup_resources(self):
    """Clean up any resources used by the task during recovery."""
    try:
        # Use a single transaction for all cleanup operations
        async with self.db_manager.transaction() as transaction:
            # Reset predictions status in batch
            await asyncio.wait_for(
                self.db_manager.execute(
                    """
                    UPDATE geomagnetic_predictions 
                    SET status = 'pending', retry_count = 0
                    WHERE status = 'processing'
                    """,
                    transaction=transaction
                ),
                timeout=10  # 10 second timeout
            )
            logger.info("Reset in-progress prediction statuses")
            
            # Clean up incomplete scoring operations
            await asyncio.wait_for(
                self.db_manager.execute(
                    """
                    DELETE FROM score_table 
                    WHERE task_name = 'geomagnetic' 
                    AND status = 'processing'
                    """,
                    transaction=transaction
                ),
                timeout=10  # 10 second timeout
            )
            logger.info("Cleaned up incomplete scoring operations")
        
        logger.info("Completed geomagnetic task cleanup")
        
    except asyncio.TimeoutError:
        logger.error("Cleanup operation timed out")
        # Don't raise here to allow partial cleanup
    except Exception as e:
        logger.error(f"Error during geomagnetic task cleanup: {e}")
        logger.error(traceback.format_exc())
        raise