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
from gaia.validator.stats.weather_stats_manager import WeatherStatsManager
from gaia.tasks.defined_tasks.weather.pipeline.steps.step_logger import log_failure, log_success, log_start
from gaia.tasks.defined_tasks.weather.processing.weather_logic import verify_miner_response

from gaia.utils.custom_logger import get_logger
logger = get_logger(__name__)

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
        # Record step start per attempt
        try:
            await log_start(
                db,
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                step_name="inference",
                substep="poll",
                context={"attempt": attempt},
            )
        except Exception:
            pass
        
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
            result = await poll_miner_job_status(validator, miner_hotkey, job_id, db_manager=db)
            
            if not result:
                logger.warning(f"[Run {run_id}] No status response from miner {miner_hotkey[:8]}")
                # Continue polling unless we've exceeded attempts
                if attempt >= MAX_POLL_ATTEMPTS:
                    await _mark_failed(db, response_id, "Polling timeout - no response from miner",
                                     run_id, miner_uid, miner_hotkey, validator)
                    return True
                else:
                    return await _reschedule_poll(db, run_id, miner_uid, miner_hotkey, response_id, job_id, attempt + 1)
            
            # Check if request was successful
            if not result.get("success"):
                error_msg = result.get("error", "Unknown error")
                logger.warning(f"[Run {run_id}] Failed to poll miner {miner_hotkey[:8]}: {error_msg}")
                
                # Continue polling unless we've exceeded attempts
                if attempt >= MAX_POLL_ATTEMPTS:
                    await _mark_failed(db, response_id, f"Polling failed: {error_msg}",
                                     run_id, miner_uid, miner_hotkey, validator)
                    return True
                else:
                    return await _reschedule_poll(db, run_id, miner_uid, miner_hotkey, response_id, job_id, attempt + 1)
                    
            # Parse successful response
            miner_status = result.get("data", {})
                    
            # Check status
            status_value = miner_status.get("status", "unknown")
            progress = miner_status.get("progress", "N/A")
            logger.info(
                f"[Run {run_id}] Poll response from {miner_hotkey[:8]}: {status_value}"
                f"\n  Progress: {progress}"
                f"\n  Response time: {result.get('response_time', 0):.2f}s"
            )
            
            if status_value in ["completed", WeatherTaskStatus.FETCH_COMPLETED]:
                # Inference complete, update status and enqueue scoring
                logger.info(f"[Run {run_id}] Miner {miner_hotkey[:8]} inference complete - stopping poll loop")
                
                # Calculate inference time if we have the start time
                inference_time_seconds = None
                try:
                    start_time_result = await db.fetch_one(
                        """
                        SELECT inference_started_at 
                        FROM weather_miner_responses 
                        WHERE id = :id
                        """,
                        {"id": response_id}
                    )
                    if start_time_result and start_time_result["inference_started_at"]:
                        inference_duration = datetime.now(timezone.utc) - start_time_result["inference_started_at"]
                        inference_time_seconds = int(inference_duration.total_seconds())
                except Exception as e:
                    logger.debug(f"Could not calculate inference time: {e}")
                
                await db.execute(
                    """
                    UPDATE weather_miner_responses
                    SET status = 'forecast_ready',
                        inference_completed_at = NOW()
                    WHERE id = :id
                    """,
                    {"id": response_id}
                )

                # Freeze manifest immediately to eliminate swap window
                try:
                    # Create or reuse WeatherTask via validator singleton like other steps
                    task = None
                    if validator is not None:
                        task = getattr(validator, "weather_task_singleton", None)
                    if task is None:
                        from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask
                        task = WeatherTask(db_manager=db, node_type="validator", test_mode=True)
                        if validator is not None:
                            try:
                                setattr(validator, "weather_task_singleton", task)
                            except Exception:
                                pass

                    run_details = {"id": run_id}
                    response_details = {"id": response_id, "miner_hotkey": miner_hotkey, "job_id": job_id}
                    await verify_miner_response(task, run_details, response_details)
                except Exception as freeze_err:
                    logger.error(f"[Run {run_id}] Manifest freeze failed for miner {miner_hotkey[:8]}: {freeze_err}")
                
                # Update stats with inference time and status
                await db.execute(
                    """
                    UPDATE weather_forecast_stats wfs
                    SET forecast_inference_duration_seconds = COALESCE(:time, forecast_inference_duration_seconds),
                        current_forecast_status = 'inference_complete',
                        current_forecast_stage = 'awaiting_scoring',
                        last_error_message = NULL
                    FROM weather_forecast_runs wfr
                    WHERE wfs.run_id = wfr.id 
                    AND wfr.id = :run_id 
                    AND wfs.miner_uid = :miner_uid
                    """,
                    {"time": inference_time_seconds, "run_id": run_id, "miner_uid": miner_uid}
                )
                
                # Log step success for polling
                try:
                    await log_success(
                        db,
                        run_id=run_id,
                        miner_uid=miner_uid,
                        miner_hotkey=miner_hotkey,
                        step_name="inference",
                        substep="poll",
                        latency_ms=int(result.get("response_time", 0) * 1000),
                    )
                except Exception:
                    pass

                # Check if day1 job already exists
                existing_day1 = await db.fetch_one(
                    """
                    SELECT id FROM validator_jobs
                    WHERE job_type = 'weather.day1'
                    AND run_id = :run_id
                    AND miner_uid = :miner_uid
                    AND status IN ('pending', 'in_progress', 'retry_scheduled', 'completed')
                    """,
                    {"run_id": run_id, "miner_uid": miner_uid}
                )
                
                if not existing_day1:
                    # CRITICAL FIX: Use singleton job to prevent race condition duplicates
                    singleton_key = f"day1_score_run_{run_id}_miner_{miner_uid}"
                    job_id_created = await db.enqueue_singleton_job(
                        singleton_key=singleton_key,
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
                    )
                    if job_id_created:
                        logger.info(f"[Run {run_id}] ✓ Created Day1 singleton job {job_id_created} for miner {miner_uid}")
                    else:
                        logger.debug(f"[Run {run_id}] Day1 singleton job already exists for miner {miner_uid}, race condition prevented")
                else:
                    logger.debug(f"[Run {run_id}] Day1 job already exists for miner {miner_uid}, skipping")
                
                # Cancel any pending poll jobs for this miner since inference is complete
                try:
                    await db.execute(
                        """
                        UPDATE validator_jobs 
                        SET status = 'cancelled'
                        WHERE job_type = 'weather.poll_miner'
                        AND run_id = :run_id
                        AND miner_uid = :miner_uid
                        AND status IN ('pending', 'retry_scheduled')
                        """,
                        {"run_id": run_id, "miner_uid": miner_uid}
                    )
                    logger.info(f"[Run {run_id}] Cancelled any pending poll jobs for miner {miner_hotkey[:8]}")
                except Exception as e:
                    logger.debug(f"Could not cancel pending poll jobs: {e}")
                
                return True
                
            elif status_value in ["processing", "received"]:
                # Still running, update status and reschedule poll
                if validator:
                    stats = WeatherStatsManager(db, getattr(validator, "hotkey", "unknown"))
                    progress_str = f"_{progress}%" if progress != "N/A" else ""
                    await stats.update_pipeline_status(
                        run_id=run_id,
                        miner_uid=miner_uid,
                        stage=f"inference_running{progress_str}",
                        status="polling"
                    )
                
                if attempt >= MAX_POLL_ATTEMPTS:
                    await _mark_failed(db, response_id, f"Polling timeout - still {status_value} after {attempt} attempts",
                                     run_id, miner_uid, miner_hotkey, validator)
                    return True
                else:
                    return await _reschedule_poll(db, run_id, miner_uid, miner_hotkey, response_id, job_id, attempt + 1)
                    
            elif status_value in ["failed", "error", "fetch_error", "input_hash_mismatch", "input_hash_timeout", "input_poll_error"]:
                # Miner reported failure
                error_msg = miner_status.get("message", "Miner reported failure")
                await _mark_failed(db, response_id, error_msg,
                                 run_id, miner_uid, miner_hotkey, validator)
                return True
                
            else:
                # Unknown status, continue polling
                logger.warning(f"[Run {run_id}] Unknown status from miner {miner_hotkey[:8]}: {status_value}")
                if attempt >= MAX_POLL_ATTEMPTS:
                    await _mark_failed(db, response_id, f"Unknown status: {status_value}",
                                     run_id, miner_uid, miner_hotkey, validator)
                    return True
                else:
                    return await _reschedule_poll(db, run_id, miner_uid, miner_hotkey, response_id, job_id, attempt + 1)
                    
        except Exception as e:
            logger.error(f"[Run {run_id}] Error polling miner {miner_hotkey[:8]}: {e}")
            # Reschedule unless we've exceeded attempts
            if attempt >= MAX_POLL_ATTEMPTS:
                await _mark_failed(db, response_id, f"Polling error: {str(e)}",
                                 run_id, miner_uid, miner_hotkey, validator)
                return True
            else:
                return await _reschedule_poll(db, run_id, miner_uid, miner_hotkey, response_id, job_id, attempt + 1)
                
    except Exception as e:
        logger.error(f"[Run {run_id}] Poll miner job failed: {e}", exc_info=True)
        return False


async def _mark_failed(
    db: ValidatorDatabaseManager,
    response_id: int,
    error_message: str,
    run_id: int = None,
    miner_uid: int = None,
    miner_hotkey: str = None,
    validator: Any = None
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
    
    # Update pipeline status if we have the necessary info
    if run_id and miner_uid and validator:
        stats = WeatherStatsManager(db, getattr(validator, "hotkey", "unknown"))
        await stats.update_pipeline_status(
            run_id=run_id,
            miner_uid=miner_uid,
            stage="inference_failed",
            status="failed",
            error=error_message
        )
        if miner_hotkey:
            await log_failure(
                db,
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                step_name="poll",
                substep="status_check",
                error_json={"error": error_message}
            )


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
    
    # Update poll attempt tracking in weather_forecast_stats
    await db.execute(
        """
        UPDATE weather_forecast_stats
        SET current_forecast_status = 'polling_for_inference',
            last_error_message = :msg
        WHERE run_id = :run_id AND miner_uid = :miner_uid
        """,
        {
            "msg": f"Polling attempt {next_attempt}/20",
            "run_id": run_id,
            "miner_uid": miner_uid
        }
    )
    
    logger.info(
        f"[Run {run_id}] Rescheduled poll for miner {miner_hotkey[:8]} "
        f"(attempt {next_attempt}) at {next_poll_time.strftime('%H:%M:%S')} UTC"
    )
    
    return True
