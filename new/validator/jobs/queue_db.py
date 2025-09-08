from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from new.validator.jobs.base import AsyncJobQueue, Job, JobType
from new.validator.database.database_manager import DatabaseManager


class DbJobQueue(AsyncJobQueue):
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    async def enqueue(self, job: Job) -> None:
        async with self.db.session() as session:
            await self._insert_job(session, job)
            await session.commit()

    async def claim(self, lease_seconds: float) -> Optional[Job]:
        async with self.db.session() as session:
            row = await self._claim_one(session, lease_seconds)
            if row is None:
                return None
            await session.commit()
            return self._row_to_job(row)

    async def complete(self, job_id: str) -> None:
        async with self.db.session() as session:
            await session.execute(
                sa.text(
                    """
                    UPDATE validator_jobs
                    SET status='completed', completed_at=NOW()
                    WHERE id = :id::bigint OR id::text = :id
                    """
                ),
                {"id": job_id},
            )
            await session.commit()

    async def fail(self, job_id: str, error: str, *, delay_seconds: float) -> None:
        async with self.db.session() as session:
            await session.execute(
                sa.text(
                    """
                    UPDATE validator_jobs
                    SET status = CASE WHEN attempts + 1 >= max_attempts THEN 'failed' ELSE 'retry_scheduled' END,
                        attempts = attempts + 1,
                        last_error = :err,
                        next_retry_at = CASE WHEN attempts + 1 >= max_attempts THEN NULL ELSE NOW() + (:delay::interval) END
                    WHERE id = :id::bigint OR id::text = :id
                    """
                ),
                {"id": job_id, "err": error, "delay": f"{max(0,int(delay_seconds))} seconds"},
            )
            await session.commit()

    async def requeue(self, job_id: str, *, delay_seconds: float) -> None:
        async with self.db.session() as session:
            await session.execute(
                sa.text(
                    """
                    UPDATE validator_jobs
                    SET status='retry_scheduled', next_retry_at = NOW() + (:delay::interval)
                    WHERE id = :id::bigint OR id::text = :id
                    """
                ),
                {"id": job_id, "delay": f"{max(0,int(delay_seconds))} seconds"},
            )
            await session.commit()

    async def _insert_job(self, session: AsyncSession, job: Job) -> None:
        await session.execute(
            sa.text(
                """
                INSERT INTO validator_jobs (job_type, priority, status, payload, attempts, max_attempts, scheduled_at, singleton_key)
                VALUES (:job_type, :priority, 'pending', :payload::jsonb, 0, :max_attempts, to_timestamp(:scheduled/1000.0), :singleton_key)
                ON CONFLICT (singleton_key)
                DO NOTHING
                """
            ),
            {
                "job_type": job.job_type.value,
                "priority": job.priority,
                "payload": sa.text(f"'{sa.text(str(job.payload)).text}'"),  # safe json binding fallback
                "max_attempts": job.max_attempts,
                "scheduled": job.scheduled_at_ms or 0,
                "singleton_key": job.singleton_key,
            },
        )

    async def _claim_one(self, session: AsyncSession, lease_seconds: float):
        # Claim one due job using SKIP LOCKED to avoid contention
        result = await session.execute(
            sa.text(
                """
                WITH cte AS (
                  SELECT id
                  FROM validator_jobs
                  WHERE status IN ('pending','retry_scheduled')
                    AND (scheduled_at IS NULL OR scheduled_at <= NOW())
                  ORDER BY priority DESC, scheduled_at NULLS FIRST, created_at ASC
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                UPDATE validator_jobs j
                SET status='claimed', started_at=NOW(), lease_expires_at=NOW() + (:lease::interval)
                WHERE j.id IN (SELECT id FROM cte)
                RETURNING j.*
                """
            ),
            {"lease": f"{max(1,int(lease_seconds))} seconds"},
        )
        row = result.mappings().first()
        return row

    def _row_to_job(self, row) -> Job:
        return Job(
            id=str(row["id"]),
            job_type=JobType(row["job_type"]),
            payload=row.get("payload") or {},
            priority=int(row.get("priority", 100)),
            attempts=int(row.get("attempts", 0)),
            max_attempts=int(row.get("max_attempts", 5)),
            status=row.get("status", "claimed"),
            enqueued_at_ms=int(row.get("created_at").timestamp() * 1000) if row.get("created_at") else None,  # type: ignore[arg-type]
            scheduled_at_ms=int(row.get("scheduled_at").timestamp() * 1000) if row.get("scheduled_at") else None,  # type: ignore[arg-type]
            lease_until_ms=int(row.get("lease_expires_at").timestamp() * 1000) if row.get("lease_expires_at") else None,  # type: ignore[arg-type]
            last_error=row.get("last_error"),
            singleton_key=row.get("singleton_key"),
        )


