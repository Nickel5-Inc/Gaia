from __future__ import annotations

import asyncio
import os
import signal
import multiprocessing as mp
from typing import Optional
import logging

from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager, DatabaseError
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


async def _install_worker_prefix_filters(tag: str) -> None:
    import logging

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

    root_logger = logging.getLogger()
    f = _WorkerTagFilter(tag)
    root_logger.addFilter(f)
    # Also attach to common app loggers
    for name in ("gaia", "fiber", "database_manager", "validator_database_manager"):
        logging.getLogger(name).addFilter(_WorkerTagFilter(tag))


async def main() -> None:
    db = ValidatorDatabaseManager()
    tag = _prefix()
    await _install_worker_prefix_filters(tag)
    # Retry DB init while Postgres is restarting or paused for maintenance
    max_tries = int(os.getenv("WEATHER_WORKER_DB_INIT_RETRIES", "30"))
    backoff = 1.0
    for attempt in range(1, max_tries + 1):
        try:
            await db.initialize_database()
            break
        except Exception as e:
            msg = str(e)
            if "shutting down" in msg or "CannotConnectNow" in msg or "Failed to initialize database engine" in msg:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 10.0)
                continue
            raise
    try:
        idle = float(os.getenv("WEATHER_WORKER_IDLE_SLEEP", "5"))
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


