from __future__ import annotations

import asyncio
import os
import signal
import multiprocessing as mp
from typing import Optional
import logging

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.defined_tasks.weather.pipeline.workers import process_one


def _prefix() -> str:
    pname = mp.current_process().name if mp.current_process() else "weather-w?"
    return f"[{pname}]"


async def worker_loop(db: ValidatorDatabaseManager, idle_sleep: float = 5.0) -> None:
    while True:
        try:
            processed = await process_one(db, validator=None)
            if not processed:
                await asyncio.sleep(idle_sleep)
        except asyncio.CancelledError:
            break
        except Exception:
            # brief backoff on unexpected errors
            await asyncio.sleep(1.0)


async def main() -> None:
    db = ValidatorDatabaseManager()
    await db.initialize_database()
    try:
        idle = float(os.getenv("WEATHER_WORKER_IDLE_SLEEP", "5"))
        # Install a logging filter to prefix messages with worker name
        class _WorkerTagFilter(logging.Filter):
            def __init__(self, tag: str) -> None:
                super().__init__()
                self._tag = tag

            def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
                try:
                    if getattr(record, "_worker_tagged", False):
                        return True
                    record.msg = f"{self._tag} {record.msg}"
                    setattr(record, "_worker_tagged", True)
                except Exception:
                    pass
                return True

        tag = _prefix()
        root_logger = logging.getLogger()
        root_logger.addFilter(_WorkerTagFilter(tag))
        for h in root_logger.handlers:
            h.addFilter(_WorkerTagFilter(tag))

        print(f"{tag} started (idle_sleep={idle}s)")
        await worker_loop(db, idle_sleep=idle)
    finally:
        await db.close_all_connections()


if __name__ == "__main__":
    # Simple signal-friendly runner
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    task: Optional[asyncio.Task] = None
    try:
        task = loop.create_task(main())

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: task.cancel() if task else None)

        loop.run_until_complete(task)
    finally:
        loop.close()


