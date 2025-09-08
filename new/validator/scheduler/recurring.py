from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List

from new.validator.jobs.base import AsyncJobQueue, Job, JobType


@dataclass
class RecurringSpec:
    name: str
    job_type: JobType
    interval_seconds: int
    jitter_seconds: int = 0
    payload: Dict[str, Any] = None  # type: ignore[assignment]


class RecurringScheduler:
    def __init__(self, *, queue: AsyncJobQueue, specs: List[RecurringSpec]) -> None:
        self.queue = queue
        self.specs = specs
        self._task: asyncio.Task[Any] | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _loop(self) -> None:
        # simple round-robin scheduler based on wall clock
        next_fire: Dict[str, float] = {}
        now = time.time()
        for spec in self.specs:
            next_fire[spec.name] = now + random.uniform(0, spec.jitter_seconds)

        while not self._stop.is_set():
            now = time.time()
            due = [s for s in self.specs if now >= next_fire[s.name]]
            for spec in due:
                job = Job.new(spec.job_type, spec.payload or {}, priority=100)
                await self.queue.enqueue(job)
                next_fire[spec.name] = now + spec.interval_seconds + random.uniform(0, spec.jitter_seconds)
            await asyncio.sleep(0.5)


