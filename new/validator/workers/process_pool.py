from __future__ import annotations

import asyncio
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Callable, Optional


class MemoryAwareProcessPool:
    def __init__(self, max_workers: Optional[int] = None) -> None:
        cpu_count = mp.cpu_count()
        self._max_workers = max_workers or max(1, min(cpu_count, 4))
        self._current_workers = min(self._max_workers, 2)
        self._executor: Optional[ProcessPoolExecutor] = None

    def _ensure_executor(self) -> None:
        if self._executor is None:
            ctx = mp.get_context("spawn")
            self._executor = ProcessPoolExecutor(max_workers=self._current_workers, mp_context=ctx)

    async def run(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        self._ensure_executor()
        assert self._executor is not None
        return await loop.run_in_executor(self._executor, lambda: func(*args, **kwargs))

    async def shutdown(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor = None


