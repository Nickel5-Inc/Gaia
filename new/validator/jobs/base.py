from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Optional, Protocol


class JobType(str, Enum):
    QUERY_MINERS = "query_miners"
    SYNC_METAGRAPH = "sync_metagraph"
    PROCESS_DEREGISTRATIONS = "process_deregistrations"


@dataclass
class Job:
    id: str
    job_type: JobType
    payload: Dict[str, Any] = field(default_factory=dict)
    priority: int = 100
    attempts: int = 0
    max_attempts: int = 5
    status: str = "pending"  # pending|claimed|retry_scheduled|completed|failed
    enqueued_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    scheduled_at_ms: Optional[int] = None
    lease_until_ms: Optional[int] = None
    last_error: Optional[str] = None
    singleton_key: Optional[str] = None

    @staticmethod
    def new(job_type: JobType, payload: Dict[str, Any], *, priority: int = 100, max_attempts: int = 5, singleton_key: Optional[str] = None, delay_ms: int = 0) -> "Job":
        now = int(time.time() * 1000)
        scheduled = now + max(delay_ms, 0)
        return Job(
            id=str(uuid.uuid4()),
            job_type=job_type,
            payload=payload,
            priority=priority,
            max_attempts=max_attempts,
            scheduled_at_ms=scheduled,
            singleton_key=singleton_key,
        )


@dataclass
class JobResult:
    ok: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class JobContext:
    def __init__(self, *, yaml_config: Dict[str, Any], logger: Any, neuron_config: Optional[Any] = None) -> None:
        self.yaml_config = yaml_config
        self.logger = logger
        self.neuron_config = neuron_config


Handler = Callable[[JobContext, Job], Awaitable[JobResult]]


class AsyncJobQueue(Protocol):
    async def enqueue(self, job: Job) -> None: ...
    async def claim(self, lease_seconds: float) -> Optional[Job]: ...
    async def complete(self, job_id: str) -> None: ...
    async def fail(self, job_id: str, error: str, *, delay_seconds: float) -> None: ...
    async def requeue(self, job_id: str, *, delay_seconds: float) -> None: ...


