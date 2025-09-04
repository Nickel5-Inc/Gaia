from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Any, Dict

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.database.validator_schema import weather_forecast_steps_table
from gaia.utils.custom_logger import get_logger

logger = get_logger(__name__)


async def _upsert(
    db: ValidatorDatabaseManager,
    *,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    step_name: str,
    substep: Optional[str] = None,
    lead_hours: Optional[int] = None,
    status: str,
    latency_ms: Optional[int] = None,
    error_json: Optional[Dict[str, Any]] = None,
    context: Optional[Dict[str, Any]] = None,
    started_at: Optional[datetime] = None,
    completed_at: Optional[datetime] = None,
    retry_count: Optional[int] = None,
    next_retry_time: Optional[datetime] = None,
) -> None:
    # Normalize status
    allowed_status = {"in_progress", "succeeded", "failed", "retry_scheduled"}
    norm_status = (status or "").strip().lower()
    status = norm_status if norm_status in allowed_status else "in_progress"

    now = datetime.now(timezone.utc)
    row = {
        "run_id": run_id,
        "miner_uid": miner_uid,
        "miner_hotkey": miner_hotkey,
        "step_name": step_name,
        "substep": substep,
        "lead_hours": lead_hours,
        "status": status,
        "started_at": started_at,
        "completed_at": completed_at,
        # retry_count and next_retry_time only when provided to avoid overwriting with NULL/defaults
        "latency_ms": latency_ms,
        "error_json": error_json,
        "context": context,
    }
    if retry_count is not None:
        row["retry_count"] = retry_count
    if next_retry_time is not None:
        row["next_retry_time"] = next_retry_time
    stmt = insert(weather_forecast_steps_table).values(**row)
    # Only update columns that are explicitly provided (non-None),
    # so we don't erase timestamps or metrics with NULLs on later calls
    non_key_updates = {
        k: v
        for k, v in row.items()
        if k not in ("run_id", "miner_uid", "step_name", "substep", "lead_hours") and v is not None
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            weather_forecast_steps_table.c.run_id,
            weather_forecast_steps_table.c.miner_uid,
            weather_forecast_steps_table.c.step_name,
            weather_forecast_steps_table.c.substep,
            weather_forecast_steps_table.c.lead_hours,
        ],
        set_=non_key_updates,
    )
    await db.execute(stmt)


async def log_start(
    db: ValidatorDatabaseManager,
    *,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    step_name: str,
    substep: Optional[str] = None,
    lead_hours: Optional[int] = None,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    # Console logging for operational visibility
    substep_str = f".{substep}" if substep else ""
    lead_str = f" (L{lead_hours}h)" if lead_hours else ""
    logger.info(f"🚀 [Run {run_id}] Miner {miner_uid}: Starting {step_name}{substep_str}{lead_str}")
    
    await _upsert(
        db,
        run_id=run_id,
        miner_uid=miner_uid,
        miner_hotkey=miner_hotkey,
        step_name=step_name,
        substep=substep,
        lead_hours=lead_hours,
        status="in_progress",
        started_at=datetime.now(timezone.utc),
        context=context,
    )


async def log_success(
    db: ValidatorDatabaseManager,
    *,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    step_name: str,
    substep: Optional[str] = None,
    lead_hours: Optional[int] = None,
    latency_ms: Optional[int] = None,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    # Console logging for operational visibility
    substep_str = f".{substep}" if substep else ""
    lead_str = f" (L{lead_hours}h)" if lead_hours else ""
    latency_str = f" in {latency_ms}ms" if latency_ms else ""
    logger.success(f"✅ [Run {run_id}] Miner {miner_uid}: Completed {step_name}{substep_str}{lead_str}{latency_str}")
    
    await _upsert(
        db,
        run_id=run_id,
        miner_uid=miner_uid,
        miner_hotkey=miner_hotkey,
        step_name=step_name,
        substep=substep,
        lead_hours=lead_hours,
        status="succeeded",
        completed_at=datetime.now(timezone.utc),
        latency_ms=latency_ms,
        context=context,
    )


async def log_failure(
    db: ValidatorDatabaseManager,
    *,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    step_name: str,
    substep: Optional[str] = None,
    lead_hours: Optional[int] = None,
    latency_ms: Optional[int] = None,
    error_json: Optional[Dict[str, Any]] = None,
    retry_count: Optional[int] = None,
    next_retry_time: Optional[datetime] = None,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    # Console logging for operational visibility (include error context if provided)
    substep_str = f".{substep}" if substep else ""
    lead_str = f" (L{lead_hours}h)" if lead_hours else ""
    latency_str = f" after {latency_ms}ms" if latency_ms else ""
    retry_str = f" (retry {retry_count})" if retry_count else ""
    msg = error_json.get("message", "Unknown error") if isinstance(error_json, dict) else "Unknown error"
    tb = None
    if isinstance(error_json, dict) and isinstance(error_json.get("traceback"), str):
        tb = error_json["traceback"]
    # Compact context rendering to avoid massive logs
    ctx = None
    if isinstance(error_json, dict) and isinstance(error_json.get("context"), (dict, list)):
        try:
            import json as _json
            ctx = _json.dumps(error_json["context"])[:300]
        except Exception:
            ctx = str(error_json.get("context"))[:300]
    context_str = f" | context={ctx}" if ctx else ""
    if tb:
        logger.error(
            f"❌ [Run {run_id}] Miner {miner_uid}: Failed {step_name}{substep_str}{lead_str}{latency_str}: {msg}{context_str}{retry_str}\nTRACEBACK:\n{tb}"
        )
    else:
        logger.error(
            f"❌ [Run {run_id}] Miner {miner_uid}: Failed {step_name}{substep_str}{lead_str}{latency_str}: {msg}{context_str}{retry_str}"
        )
    
    await _upsert(
        db,
        run_id=run_id,
        miner_uid=miner_uid,
        miner_hotkey=miner_hotkey,
        step_name=step_name,
        substep=substep,
        lead_hours=lead_hours,
        status="failed",
        completed_at=datetime.now(timezone.utc),
        latency_ms=latency_ms,
        error_json=error_json,
        retry_count=retry_count,
        next_retry_time=next_retry_time,
        context=context,
    )


async def schedule_retry(
    db: ValidatorDatabaseManager,
    *,
    run_id: int,
    miner_uid: int,
    miner_hotkey: str,
    step_name: str,
    substep: Optional[str] = None,
    lead_hours: Optional[int] = None,
    error_json: Optional[Dict[str, Any]] = None,
    retry_count: int,
    next_retry_time,
) -> None:
    # Console logging for operational visibility
    substep_str = f".{substep}" if substep else ""
    lead_str = f" (L{lead_hours}h)" if lead_hours else ""
    retry_time_str = f" at {next_retry_time.strftime('%H:%M:%S')}" if next_retry_time else ""
    error_str = f": {error_json.get('message', 'Unknown error')}" if error_json else ""
    
    logger.warning(f"🔄 [Run {run_id}] Miner {miner_uid}: Retrying {step_name}{substep_str}{lead_str} (attempt {retry_count}){retry_time_str}{error_str}")
    
    await _upsert(
        db,
        run_id=run_id,
        miner_uid=miner_uid,
        miner_hotkey=miner_hotkey,
        step_name=step_name,
        substep=substep,
        lead_hours=lead_hours,
        status="retry_scheduled",
        error_json=error_json,
        retry_count=retry_count,
        next_retry_time=next_retry_time,
    )


