"""
Weather Task Progress Tracking

Progress callback and status tracking functionality for weather tasks.
Extracted from the monolithic weather_task.py for better modularity.
"""

from typing import TYPE_CHECKING, Dict, List, Optional

from fiber.logging_utils import get_logger

if TYPE_CHECKING:
    from ..core.task import WeatherTask

from ..schemas.weather_outputs import WeatherProgressUpdate

logger = get_logger(__name__)


async def register_progress_callback(task: "WeatherTask", callback: callable) -> None:
    """Register a progress callback."""
    task.progress_callbacks.append(callback)
    logger.debug("Registered progress callback")


async def get_progress_status(task: "WeatherTask", job_id: Optional[str] = None) -> Dict[str, any]:
    """Get current progress status for a job or all operations."""
    if job_id:
        if job_id in task.current_operations:
            progress_update = task.current_operations[job_id]
            return {
                "job_id": job_id,
                "status": progress_update.status,
                "progress": progress_update.progress,
                "message": progress_update.message,
                "timestamp": progress_update.timestamp,
            }
        else:
            return {
                "job_id": job_id,
                "status": "not_found",
                "message": f"No progress information found for job {job_id}",
            }
    else:
        # Return all current operations
        all_operations = {}
        for op_job_id, progress_update in task.current_operations.items():
            all_operations[op_job_id] = {
                "status": progress_update.status,
                "progress": progress_update.progress,
                "message": progress_update.message,
                "timestamp": progress_update.timestamp,
            }
        return {
            "active_operations": len(all_operations),
            "operations": all_operations,
        }


async def emit_progress(task: "WeatherTask", update: WeatherProgressUpdate) -> None:
    """Emit a progress update to all registered callbacks."""
    # Store current operation status
    if update.job_id:
        task.current_operations[update.job_id] = update

    # Notify all callbacks
    for callback in task.progress_callbacks:
        try:
            await callback(update)
        except Exception as e:
            logger.warning(f"Progress callback failed: {e}")

    logger.debug(f"Progress update: {update.status} - {update.message}")


def clear_operation_progress(task: "WeatherTask", job_id: str) -> None:
    """Clear progress tracking for a completed operation."""
    if job_id in task.current_operations:
        del task.current_operations[job_id]
        logger.debug(f"Cleared progress tracking for job {job_id}")