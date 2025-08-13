from __future__ import annotations

from functools import wraps
from typing import Optional, Callable, Any, Dict

from .step_logger import log_start, log_success, log_failure, schedule_retry
from ..retry_policy import compute_next_retry


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
                nrt = None
                if should_retry and max_retries > 0:
                    # Test mode: drastically shorten retry delays for fast iteration
                    is_test = bool(getattr(task, "test_mode", False))
                    base_delay = 5 if is_test else retry_delay_seconds
                    try:
                        nrt = compute_next_retry(
                            attempt=1,
                            base_delay_seconds=base_delay,
                            backoff_type=retry_backoff,
                        )
                    except Exception:
                        nrt = None
                await log_failure(
                    db,
                    run_id=run_id,
                    miner_uid=miner_uid,
                    miner_hotkey=miner_hotkey,
                    step_name=step_name,
                    substep=substep_name,
                    lead_hours=lead_hours,
                    error_json={"type": f"{step_name}_{substep_name}_failed", "message": str(e)},
                    retry_count=1 if nrt else None,
                    next_retry_time=nrt,
                )
                if nrt is not None:
                    await schedule_retry(
                        db,
                        run_id=run_id,
                        miner_uid=miner_uid,
                        miner_hotkey=miner_hotkey,
                        step_name=step_name,
                        substep=substep_name,
                        lead_hours=lead_hours,
                        error_json={"type": f"{step_name}_{substep_name}_failed"},
                        retry_count=1,
                        next_retry_time=nrt,
                    )
                raise

        return wrapper

    return decorator


