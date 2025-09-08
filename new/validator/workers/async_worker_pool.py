from __future__ import annotations

import asyncio
from typing import Generic, TypeVar

from new.validator.core.contracts import (Job, Queue, TaskContext, TaskResult,
                                          Worker)

PayloadT = TypeVar("PayloadT")
ResultT = TypeVar("ResultT")


class AsyncWorkerPool(Generic[PayloadT, ResultT]):
    def __init__(self, queue: Queue[PayloadT], worker: Worker[PayloadT, ResultT], concurrency: int = 4):
        self._queue = queue
        self._worker = worker
        self._concurrency = max(1, concurrency)
        self._tasks: list[asyncio.Task] = []
        self._stopping = asyncio.Event()

    async def _worker_loop(self, index: int) -> None:
        context = TaskContext()
        while not self._stopping.is_set():
            job = await self._queue.get()
            try:
                result: TaskResult[ResultT] = await self._worker.process(job, context)
                await self._queue.ack(job)
            except Exception:
                try:
                    await self._queue.nack(job, requeue=True)
                finally:
                    await asyncio.sleep(0)

    async def start(self) -> None:
        if self._tasks:
            return
        self._stopping.clear()
        for i in range(self._concurrency):
            self._tasks.append(asyncio.create_task(self._worker_loop(i)))

    async def stop(self) -> None:
        self._stopping.set()
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()


