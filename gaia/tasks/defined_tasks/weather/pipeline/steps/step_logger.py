from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Any, Dict

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.database.validator_schema import weather_forecast_steps_table


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
        "retry_count": retry_count if retry_count is not None else 0,
        "next_retry_time": next_retry_time,
        "latency_ms": latency_ms,
        "error_json": error_json,
        "context": context,
    }
    stmt = insert(weather_forecast_steps_table).values(**row)
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            weather_forecast_steps_table.c.run_id,
            weather_forecast_steps_table.c.miner_uid,
            weather_forecast_steps_table.c.step_name,
            weather_forecast_steps_table.c.substep,
            weather_forecast_steps_table.c.lead_hours,
        ],
        set_={
            k: v
            for k, v in row.items()
            if k not in ("run_id", "miner_uid", "step_name", "substep", "lead_hours")
        },
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


