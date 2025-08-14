"""
Poll individual miner to check if inference is complete.
This step runs in parallel for each miner that has accepted a request.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.pipeline.miner_communication import poll_miner_job_status
from gaia.tasks.defined_tasks.weather.schemas.weather_outputs import WeatherTaskStatus

logger = logging.getLogger(__name__)

# Maximum polling attempts before giving up
MAX_POLL_ATTEMPTS = 20  # 20 * 5 minutes = 100 minutes max
POLL_INTERVAL_MINUTES = 5


async def run_poll_miner_job(
    db: ValidatorDatabaseManager,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    response_id: int,
    job_id: str,
    attempt: int = 1,
    validator: Optional[Any] = None,
) -> bool:
    """
    Poll a miner to check if inference is complete.
    
    This job:
    1. Queries miner for job status
    2. Updates response status if complete
    3. Re-enqueues itself if still running
    4. Enqueues scoring job if ready
    
    Returns:
        True if handled (complete, failed, or re-enqueued)
        False if error occurred (will retry)
    """
    try:
        logger.info(
            f"[Run {run_id}] Polling miner {miner_hotkey[:8]} (attempt {attempt}/{MAX_POLL_ATTEMPTS})"
        )
        
        # Check current status
        response = await db.fetch_one(
            """
            SELECT id, status FROM weather_miner_responses
            WHERE id = :response_id
            """,
            {"response_id": response_id}
        )
        
        if not response:
            logger.error(f"[Run {run_id}] Response {response_id} not found")
            return True  # Don't retry for missing response
            
        current_status = response["status"]
        
        # Skip if already complete or failed
        if current_status in ["forecast_ready", "day1_scoring", "day1_scored", "failed"]:
            logger.info(
                f"[Run {run_id}] Miner {miner_hotkey[:8]} already in terminal status: {current_status}"
            )
            return True
            
        # Query miner for status using the new communication module
        try:
            result = await poll_miner_job_status(validator, miner_hotkey, job_id)
            
            if not result:
                logger.warning(f"[Run {run_id}] No status response from miner {miner_hotkey[:8]}")
                # Continue polling unless we've exceeded attempts
                if attempt >= MAX_POLL_ATTEMPTS:
                    await _mark_failed(db, response_id, "Polling timeout - no response from miner")
                    return True
                else:
                    return await _reschedule_poll(db, run_id, miner_uid, miner_hotkey, response_id, job_id, attempt + 1)
            
            # Check if request was successful
            if not result.get("success"):
                error_msg = result.get("error", "Unknown error")
                logger.warning(f"[Run {run_id}] Failed to poll miner {miner_hotkey[:8]}: {error_msg}")
                
                # Continue polling unless we've exceeded attempts
                if attempt >= MAX_POLL_ATTEMPTS:
                    await _mark_failed(db, response_id, f"Polling failed: {error_msg}")
                    return True
                else:
                    return await _reschedule_poll(db, run_id, miner_uid, miner_hotkey, response_id, job_id, attempt + 1)
                    
            # Parse successful response
            miner_status = result.get("data", {})
                    
            # Check status
            status_value = miner_status.get("status", "unknown")
            logger.info(
                f"[Run {run_id}] Poll response from {miner_hotkey[:8]}: {status_value}"
                f"\n  Progress: {miner_status.get('progress', 'N/A')}"
                f"\n  Response time: {result.get('response_time', 0):.2f}s"
            )
            
            if status_value in ["completed", WeatherTaskStatus.FETCH_COMPLETED]:
                # Inference complete, update status and enqueue scoring
                logger.info(f"[Run {run_id}] Miner {miner_hotkey[:8]} inference complete")
                
                await db.execute(
                    """
                    UPDATE weather_miner_responses
                    SET status = 'forecast_ready'
                    WHERE id = :id
                    """,
                    {"id": response_id}
                )
                
                # Enqueue day1 scoring job
                await db.enqueue_validator_job(
                    job_type="weather.day1",
                    payload={
                        "run_id": run_id,
                        "miner_uid": miner_uid,
                        "miner_hotkey": miner_hotkey,
                        "response_id": response_id,
                        "job_id": job_id,
                    },
                    priority=60,
                    run_id=run_id,
                    miner_uid=miner_uid,
                    response_id=response_id,
                )
                
                return True
                
            elif status_value in ["processing", "running", "inference_running", WeatherTaskStatus.FETCH_PROCESSING]:
                # Still running, reschedule poll
                if attempt >= MAX_POLL_ATTEMPTS:
                    await _mark_failed(db, response_id, f"Polling timeout - still {status_value} after {attempt} attempts")
                    return True
                else:
                    return await _reschedule_poll(db, run_id, miner_uid, miner_hotkey, response_id, job_id, attempt + 1)
                    
            elif status_value in ["error", "failed"]:
                # Miner reported failure
                error_msg = miner_status.get("message", "Miner reported failure")
                await _mark_failed(db, response_id, error_msg)
                return True
                
            else:
                # Unknown status, continue polling
                logger.warning(f"[Run {run_id}] Unknown status from miner {miner_hotkey[:8]}: {status_value}")
                if attempt >= MAX_POLL_ATTEMPTS:
                    await _mark_failed(db, response_id, f"Unknown status: {status_value}")
                    return True
                else:
                    return await _reschedule_poll(db, run_id, miner_uid, miner_hotkey, response_id, job_id, attempt + 1)
                    
        except Exception as e:
            logger.error(f"[Run {run_id}] Error polling miner {miner_hotkey[:8]}: {e}")
            # Reschedule unless we've exceeded attempts
            if attempt >= MAX_POLL_ATTEMPTS:
                await _mark_failed(db, response_id, f"Polling error: {str(e)}")
                return True
            else:
                return await _reschedule_poll(db, run_id, miner_uid, miner_hotkey, response_id, job_id, attempt + 1)
                
    except Exception as e:
        logger.error(f"[Run {run_id}] Poll miner job failed: {e}", exc_info=True)
        return False


async def _mark_failed(
    db: ValidatorDatabaseManager,
    response_id: int,
    error_message: str
) -> None:
    """Mark a miner response as failed."""
    await db.execute(
        """
        UPDATE weather_miner_responses
        SET status = 'failed', error_message = :error
        WHERE id = :id
        """,
        {"id": response_id, "error": error_message}
    )
    logger.info(f"Marked response {response_id} as failed: {error_message}")


async def _reschedule_poll(
    db: ValidatorDatabaseManager,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    response_id: int,
    job_id: str,
    next_attempt: int
) -> bool:
    """Reschedule polling job for later."""
    next_poll_time = datetime.now(timezone.utc) + timedelta(minutes=POLL_INTERVAL_MINUTES)
    
    await db.enqueue_validator_job(
        job_type="weather.poll_miner",
        payload={
            "run_id": run_id,
            "miner_uid": miner_uid,
            "miner_hotkey": miner_hotkey,
            "response_id": response_id,
            "job_id": job_id,
            "attempt": next_attempt,
        },
        priority=70,
        run_id=run_id,
        miner_uid=miner_uid,
        response_id=response_id,
        scheduled_at=next_poll_time,
    )
    
    logger.info(
        f"[Run {run_id}] Rescheduled poll for miner {miner_hotkey[:8]} "
        f"(attempt {next_attempt}) at {next_poll_time.strftime('%H:%M:%S')} UTC"
    )
    
    return True
