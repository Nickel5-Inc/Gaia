from __future__ import annotations

import asyncio
import os
import yaml

from new.validator.jobs.base import JobType
from new.validator.jobs.queue_memory import InMemoryJobQueue
from new.validator.jobs.registry import JobRegistry
from new.validator.jobs.queue_db import DbJobQueue
from new.validator.database.database_manager import DatabaseManager
from new.validator.scheduler.recurring import RecurringScheduler, RecurringSpec
from new.validator.workers.pool import WorkerPool
from new.validator.handlers.query_miners import handle_query_miners
from new.validator.handlers.sync_metagraph import handle_sync_metagraph
from new.validator.handlers.process_deregistrations import handle_process_deregistrations

def _load_yaml(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


async def run_validator(config_path: str) -> None:
    yaml_cfg = _load_yaml(config_path)

    # Queue & registry
    db_url = os.environ.get("DATABASE_URL")
    if db_url:
        queue = DbJobQueue(DatabaseManager())
    else:
        queue = InMemoryJobQueue()
    registry = JobRegistry()
    registry.register(JobType.QUERY_MINERS, handle_query_miners)
    registry.register(JobType.SYNC_METAGRAPH, handle_sync_metagraph)
    registry.register(JobType.PROCESS_DEREGISTRATIONS, handle_process_deregistrations)

    # Scheduler specs from YAML (with defaults)
    sched_cfg = (yaml_cfg.get("scheduler") or {})
    recurring_cfg = sched_cfg.get("recurring") or [
        {"name": "query_miners", "job_type": "query_miners", "interval_seconds": 60, "jitter_seconds": 5, "payload": {"kind": "weather.forward", "payload": {}}},
        {"name": "sync_metagraph", "job_type": "sync_metagraph", "interval_seconds": 300, "jitter_seconds": 10, "payload": {}},
        {"name": "process_deregistrations", "job_type": "process_deregistrations", "interval_seconds": 600, "jitter_seconds": 15, "payload": {}},
    ]
    specs = [
        RecurringSpec(
            name=rc["name"],
            job_type=JobType(rc["job_type"]),
            interval_seconds=int(rc.get("interval_seconds", 60)),
            jitter_seconds=int(rc.get("jitter_seconds", 0)),
            payload=rc.get("payload", {}),
        )
        for rc in recurring_cfg
    ]
    scheduler = RecurringScheduler(queue=queue, specs=specs)

    # Workers
    workers_cfg = sched_cfg.get("workers") or {}
    pool = WorkerPool(
        queue=queue,
        registry=registry,
        yaml_config=yaml_cfg,
        concurrency=int(workers_cfg.get("concurrency", 4)),
        lease_seconds=float(workers_cfg.get("lease_seconds", 30.0)),
    )

    # Start runtime
    await scheduler.start()
    await pool.start()
    try:
        await asyncio.Event().wait()
    finally:
        await scheduler.stop()
        await pool.stop()


