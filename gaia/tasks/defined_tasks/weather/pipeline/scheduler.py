from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import sqlalchemy as sa

from gaia.database.validator_schema import (weather_forecast_steps_table,
                                            weather_miner_responses_table,
                                            weather_miner_scores_table)
from gaia.validator.database.validator_database_manager import \
    ValidatorDatabaseManager


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

    # REMOVED: claim_day1() method - no longer used since scoring goes through generic job queue

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

    # REMOVED: claim_era5() method - no longer used since scoring goes through generic job queue

    async def claim_next(self) -> Optional[WorkItem]:
        """Legacy method - no longer functional since claim_day1/claim_era5 removed."""
        # Opportunistically enqueue missing validator jobs to drive generic queue
        # REMOVED: Bulk job enqueuing - replaced by self-managing pipeline where each step creates its successor
        # REMOVED: All claim methods removed since scoring goes through generic job queue
        return None


