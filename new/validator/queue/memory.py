from __future__ import annotations

import asyncio
from typing import Generic, TypeVar

from new.validator.core.contracts import Job, Queue

PayloadT = TypeVar("PayloadT")


class InMemoryQueue(Queue[PayloadT], Generic[PayloadT]):
    def __init__(self, maxsize: int = 0):
        self._q: asyncio.Queue[Job[PayloadT]] = asyncio.Queue(maxsize=maxsize)

    async def put(self, job: Job[PayloadT]) -> None:
        await self._q.put(job)

    async def get(self) -> Job[PayloadT]:
        job = await self._q.get()
        return job

    async def ack(self, job: Job[PayloadT]) -> None:
        self._q.task_done()

    async def nack(self, job: Job[PayloadT], requeue: bool = True) -> None:
        if requeue:
            await self._q.put(job)
        self._q.task_done()

    async def close(self) -> None:
        return None


