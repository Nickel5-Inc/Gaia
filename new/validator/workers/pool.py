from __future__ import annotations

import asyncio
import random
from typing import Any

from new.validator.jobs.base import AsyncJobQueue, Job, JobContext
from new.validator.jobs.registry import JobRegistry


class WorkerPool:
    def __init__(self, *, queue: AsyncJobQueue, registry: JobRegistry, yaml_config: dict, concurrency: int = 4, lease_seconds: float = 30.0, backoff_base: float = 1.5, backoff_cap_s: float = 60.0) -> None:
        self.queue = queue
        self.registry = registry
        self.yaml_config = yaml_config
        self.concurrency = max(1, concurrency)
        self.lease_seconds = lease_seconds
        self.backoff_base = backoff_base
        self.backoff_cap_s = backoff_cap_s
        self._tasks: list[asyncio.Task[Any]] = []
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        for _ in range(self.concurrency):
            self._tasks.append(asyncio.create_task(self._worker_loop()))

    async def stop(self) -> None:
        self._stopping.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _worker_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                job = await self.queue.claim(self.lease_seconds)
                if job is None:
                    await asyncio.sleep(0.2)
                    continue
                await self._process(job)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.5)

    async def _process(self, job: Job) -> None:
        handler = self.registry.get(job.job_type)
        ctx = JobContext(yaml_config=self.yaml_config, logger=None, neuron_config=None)
        try:
            result = await handler(ctx, job)
            if result.ok:
                await self.queue.complete(job.id)
                return
            # failure with suggested retry
            delay = self._next_delay(job.attempts)
            await self.queue.fail(job.id, result.error or "job_failed", delay_seconds=delay)
        except Exception as e:
            delay = self._next_delay(job.attempts)
            await self.queue.fail(job.id, str(e), delay_seconds=delay)

    def _next_delay(self, attempts: int) -> float:
        exp = min(self.backoff_cap_s, (self.backoff_base ** max(attempts, 1)))
        jitter = random.uniform(0, 0.2 * exp)
        return exp + jitter


