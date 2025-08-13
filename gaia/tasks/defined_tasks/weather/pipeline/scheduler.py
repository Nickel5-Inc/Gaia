from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import logging
import sqlalchemy as sa

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.database.validator_schema import (
    weather_miner_responses_table,
    weather_miner_scores_table,
    weather_forecast_steps_table,
)


@dataclass
class WorkItem:
    run_id: int
    miner_uid: int
    miner_hotkey: str
    response_id: Optional[int]
    step: str
    meta: Dict[str, Any]


class MinerWorkScheduler:
    """Select next per-miner work items using SKIP LOCKED-compatible predicates.

    This is phase-1 scaffolding: we log selections for dry-run validation first.
    """

    def __init__(self, db: ValidatorDatabaseManager):
        self.db = db

    # Verification phase removed; selection now begins directly at day1

    # Verification phase removed; no claim_to_verify

    async def next_day1(self, limit: int = 50) -> List[WorkItem]:
        # Day1 candidates: inference accepted/underway/submitted, but not day1-scored overall
        query = sa.text(
            """
            SELECT r.id as response_id, r.run_id, r.miner_uid, r.miner_hotkey
            FROM weather_miner_responses r
            WHERE r.status IN ('inference_triggered','awaiting_forecast_submission','forecast_submitted')
              AND NOT EXISTS (
                SELECT 1 FROM weather_miner_scores s
                WHERE s.run_id = r.run_id AND s.miner_uid = r.miner_uid
                  AND s.score_type = 'gfs_rmse' AND s.variable_level = 'overall_day1'
              )
            ORDER BY r.response_time DESC
            LIMIT :limit
            """
        )
        rows = await self.db.fetch_all(query, {"limit": limit})
        items = [
            WorkItem(
                run_id=r["run_id"],
                miner_uid=r["miner_uid"],
                miner_hotkey=r["miner_hotkey"],
                response_id=r["response_id"],
                step="day1",
                meta={},
            )
            for r in rows
        ]
        logging.info(f"[Scheduler] next_day1 -> {len(items)} candidates")
        return items

    async def claim_day1(self) -> Optional[WorkItem]:
        """Prefer step-based retry-scheduled day1 work; fallback to legacy selection."""
        now = datetime.now(timezone.utc)
        step_row = await self.db.fetch_one(
            sa.text(
                """
                SELECT s.run_id, s.miner_uid, s.miner_hotkey
                FROM weather_forecast_steps s
                WHERE s.step_name = 'day1' AND s.status = 'retry_scheduled' AND (s.next_retry_time IS NULL OR s.next_retry_time <= :now)
                ORDER BY s.next_retry_time NULLS FIRST
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """
            ),
            {"now": now},
        )
        if step_row:
            return WorkItem(
                run_id=step_row["run_id"],
                miner_uid=step_row["miner_uid"],
                miner_hotkey=step_row["miner_hotkey"],
                response_id=None,
                step="day1",
                meta={"claimed_via": "steps"},
            )
        query = sa.text(
            """
            WITH cte AS (
                SELECT r.id as response_id, r.run_id, r.miner_uid, r.miner_hotkey
                FROM weather_miner_responses r
                WHERE r.status IN ('inference_triggered','awaiting_forecast_submission','forecast_submitted')
                  AND NOT EXISTS (
                    SELECT 1 FROM weather_miner_scores s
                    WHERE s.run_id = r.run_id AND s.miner_uid = r.miner_uid
                      AND s.score_type = 'gfs_rmse' AND s.variable_level = 'overall_day1'
                  )
                ORDER BY r.response_time DESC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE weather_miner_responses r
            SET last_polled_time = NOW()
            FROM cte
            WHERE r.id = cte.response_id
            RETURNING r.id as response_id, r.run_id, r.miner_uid, r.miner_hotkey
            """
        )
        row = await self.db.fetch_one(query)
        if not row:
            return None
        return WorkItem(
            run_id=row["run_id"],
            miner_uid=row["miner_uid"],
            miner_hotkey=row["miner_hotkey"],
            response_id=row["response_id"],
            step="day1",
            meta={},
        )

    async def next_era5(self, limit: int = 50) -> List[WorkItem]:
        # Responses verified or day1-scored but missing some ERA5 leads
        # Heuristic: look for any r where an expected lead is absent in scores
        query = sa.text(
            """
            SELECT r.id as response_id, r.run_id, r.miner_uid, r.miner_hotkey
            FROM weather_miner_responses r
            WHERE r.status = 'day1_scored'
            ORDER BY r.response_time DESC
            LIMIT :limit
            """
        )
        rows = await self.db.fetch_all(query, {"limit": limit})
        items = [
            WorkItem(
                run_id=r["run_id"],
                miner_uid=r["miner_uid"],
                miner_hotkey=r["miner_hotkey"],
                response_id=r["response_id"],
                step="era5",
                meta={},
            )
            for r in rows
        ]
        logging.info(f"[Scheduler] next_era5 -> {len(items)} candidates (pre-filter)")
        return items

    async def claim_era5(self) -> Optional[WorkItem]:
        """Prefer step-based retry-scheduled ERA5 work; fallback to legacy selection."""
        now = datetime.now(timezone.utc)
        step_row = await self.db.fetch_one(
            sa.text(
                """
                SELECT s.run_id, s.miner_uid, s.miner_hotkey
                FROM weather_forecast_steps s
                WHERE s.step_name = 'era5' AND s.status = 'retry_scheduled' AND (s.next_retry_time IS NULL OR s.next_retry_time <= :now)
                ORDER BY s.next_retry_time NULLS FIRST
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """
            ),
            {"now": now},
        )
        if step_row:
            return WorkItem(
                run_id=step_row["run_id"],
                miner_uid=step_row["miner_uid"],
                miner_hotkey=step_row["miner_hotkey"],
                response_id=None,
                step="era5",
                meta={"claimed_via": "steps"},
            )
        query = sa.text(
            """
            WITH cte AS (
                SELECT r.id as response_id, r.run_id, r.miner_uid, r.miner_hotkey
                FROM weather_miner_responses r
                WHERE r.status = 'day1_scored'
                ORDER BY r.response_time DESC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE weather_miner_responses r
            SET last_polled_time = NOW()
            FROM cte
            WHERE r.id = cte.response_id
            RETURNING r.id as response_id, r.run_id, r.miner_uid, r.miner_hotkey
            """
        )
        row = await self.db.fetch_one(query)
        if not row:
            return None
        return WorkItem(
            run_id=row["run_id"],
            miner_uid=row["miner_uid"],
            miner_hotkey=row["miner_hotkey"],
            response_id=row["response_id"],
            step="era5",
            meta={},
        )

    async def claim_next(self) -> Optional[WorkItem]:
        """Return the next available work item by priority: day1 → era5."""
        # Opportunistically enqueue missing validator jobs to drive generic queue
        try:
            _ = await self.db.enqueue_weather_step_jobs(limit=200)
        except Exception:
            pass
        # verification removed
        item = await self.claim_day1()
        if item:
            return item
        item = await self.claim_era5()
        return item


