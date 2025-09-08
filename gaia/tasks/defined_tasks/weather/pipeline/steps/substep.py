from __future__ import annotations

import json
from functools import wraps
from typing import Any, Callable, Dict, Optional

from loguru import logger

from ..retry_policy import compute_next_retry
from .step_logger import log_failure, log_start, log_success, schedule_retry


def substep(
    step_name: str,
    substep_name: Optional[str],
    *,
    should_retry: bool = True,
    retry_delay_seconds: int = 300,
    max_retries: int = 3,
    retry_backoff: str = "exponential",  # 'exponential' | 'linear' | 'none'
):
    """Decorator to standardize sub-step execution: logging, retry scheduling.

    The wrapped function must be async and accept (db, task, **kwargs) with at least run_id.
    For run-level substeps, pass miner_uid=0, miner_hotkey='coordinator'.
    """

    def decorator(func: Callable[..., Any]):
        @wraps(func)
        async def wrapper(db, task, **kwargs):
            run_id = kwargs.get("run_id")
            miner_uid = kwargs.get("miner_uid", 0)
            miner_hotkey = kwargs.get("miner_hotkey", "coordinator")
            # Normalize lead_hours for uniqueness to avoid accidental duplicate rows
            lead_hours = kwargs.get("lead_hours")
            if lead_hours is None:
                lead_hours = 0
            await log_start(
                db,
                run_id=run_id,
                miner_uid=miner_uid,
                miner_hotkey=miner_hotkey,
                step_name=step_name,
                substep=substep_name,
                lead_hours=lead_hours,
            )
            try:
                result = await func(db, task, **kwargs)
                # Only log success if the substep indicates success (truthy result)
                if result:
                    await log_success(
                        db,
                        run_id=run_id,
                        miner_uid=miner_uid,
                        miner_hotkey=miner_hotkey,
                        step_name=step_name,
                        substep=substep_name,
                        lead_hours=lead_hours,
                    )
                return result
            except Exception as e:
                # CRITICAL: Each substep owns its retry count from job payload
                current_retry_count = kwargs.get("retry_count", 1)
                
                # Log the failure with current attempt number
                await log_failure(
                    db,
                    run_id=run_id,
                    miner_uid=miner_uid,
                    miner_hotkey=miner_hotkey,
                    step_name=step_name,
                    substep=substep_name,
                    lead_hours=lead_hours,
                    error_json={"type": f"{step_name}_{substep_name}_failed", "message": str(e)},
                    retry_count=current_retry_count,
                    next_retry_time=None,  # No automatic retry scheduling
                )
                
                # Check if we should schedule a retry
                if should_retry and current_retry_count < max_retries:
                    next_retry_count = current_retry_count + 1
                    
                    # Test mode: drastically shorten retry delays for fast iteration  
                    is_test = bool(getattr(task, "test_mode", False))
                    base_delay = 5 if is_test else retry_delay_seconds
                    
                    try:
                        nrt = compute_next_retry(
                            attempt=current_retry_count,
                            base_delay_seconds=base_delay,
                            backoff_type=retry_backoff,
                        )
                    except Exception:
                        nrt = None
                    
                    if nrt:
                        # CRITICAL: Cancel current job to prevent duplicate processing
                        current_job_id = task.get("id")
                        if current_job_id:
                            try:
                                # Mark current job as failed to prevent it from continuing
                                await db.execute(
                                    """
                                    UPDATE validator_jobs 
                                    SET status = 'failed', 
                                        completed_at = NOW(),
                                        result = :result
                                    WHERE id = :job_id AND status = 'in_progress'
                                    """,
                                    {
                                        "job_id": current_job_id,
                                        "result": json.dumps({"cancelled_for_retry": True, "retry_count": next_retry_count})
                                    }
                                )
                                logger.debug(f"Cancelled job {current_job_id} to prevent duplicate processing during retry")
                            except Exception as cancel_err:
                                logger.warning(f"Failed to cancel current job {current_job_id}: {cancel_err}")
                        
                        # Create a new job for the retry with incremented retry_count
                        retry_payload = dict(kwargs)
                        retry_payload["retry_count"] = next_retry_count
                        
                        # Schedule the retry job
                        await db.enqueue_job(
                            job_type=task.get("job_type", f"weather.{step_name}"),
                            priority=task.get("priority", 100) + 10,  # Lower priority for retries
                            scheduled_at=nrt,
                            payload=retry_payload
                        )
                        
                        await schedule_retry(
                            db,
                            run_id=run_id,
                            miner_uid=miner_uid,
                            miner_hotkey=miner_hotkey,
                            step_name=step_name,
                            substep=substep_name,
                            lead_hours=lead_hours,
                            error_json={"type": f"{step_name}_{substep_name}_failed", "message": str(e)},
                            retry_count=next_retry_count,
                            next_retry_time=nrt,
                        )
                        
                        logger.info(
                            f"🔄 [{step_name}.{substep_name}] Scheduled retry {next_retry_count}/{max_retries} "
                            f"for run {run_id}, miner {miner_uid} at {nrt.strftime('%H:%M:%S')}"
                        )
                        return None  # Don't raise - retry scheduled
                else:
                    logger.warning(
                        f"[{step_name}.{substep_name}] Max retries ({max_retries}) exceeded for "
                        f"run {run_id}, miner {miner_uid} (attempt {current_retry_count}), giving up"
                    )
                
                # Either no retries configured or max retries exceeded
                raise

        return wrapper

    return decorator


