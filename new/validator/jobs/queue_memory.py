from __future__ import annotations

import heapq
import time
from typing import List, Optional, Tuple

from new.validator.jobs.base import AsyncJobQueue, Job


class InMemoryJobQueue(AsyncJobQueue):
    def __init__(self) -> None:
        # Min-heap ordered by (scheduled_at_ms, -priority, enqueued_at_ms)
        self._heap: List[Tuple[int, int, int, Job]] = []
        self._counter = 0
        self._jobs: dict[str, Job] = {}

    async def enqueue(self, job: Job) -> None:
        self._jobs[job.id] = job
        sched = job.scheduled_at_ms or int(time.time() * 1000)
        heapq.heappush(self._heap, (sched, -job.priority, self._counter, job))
        self._counter += 1

    async def claim(self, lease_seconds: float) -> Optional[Job]:
        now = int(time.time() * 1000)
        while self._heap:
            sched, _, _, job = heapq.heappop(self._heap)
            if sched > now:
                # Not ready yet; push back and exit
                heapq.heappush(self._heap, (sched, -job.priority, self._counter, job))
                self._counter += 1
                return None
            if job.status not in ("pending", "retry_scheduled"):
                continue
            job.status = "claimed"
            job.lease_until_ms = now + int(lease_seconds * 1000)
            return job
        return None

    async def complete(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if job is not None:
            job.status = "completed"

    async def fail(self, job_id: str, error: str, *, delay_seconds: float) -> None:
        job = self._jobs.get(job_id)
        if job is None:
            return
        job.attempts += 1
        job.last_error = error
        if job.attempts >= job.max_attempts:
            job.status = "failed"
            return
        await self._requeue_internal(job, delay_seconds)

    async def requeue(self, job_id: str, *, delay_seconds: float) -> None:
        job = self._jobs.get(job_id)
        if job is None:
            return
        await self._requeue_internal(job, delay_seconds)

    async def _requeue_internal(self, job: Job, delay_seconds: float) -> None:
        job.status = "retry_scheduled"
        job.lease_until_ms = None
        job.scheduled_at_ms = int(time.time() * 1000) + int(max(delay_seconds, 0) * 1000)
        heapq.heappush(self._heap, (job.scheduled_at_ms, -job.priority, self._counter, job))
        self._counter += 1


