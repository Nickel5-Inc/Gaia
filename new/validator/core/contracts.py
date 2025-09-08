from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Generic, Optional, Protocol, TypeVar

PayloadT = TypeVar("PayloadT")
ResultT = TypeVar("ResultT")


@dataclass(frozen=True)
class Job(Generic[PayloadT]):
    id: str
    payload: PayloadT
    metadata: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class TaskContext:
    correlation_id: Optional[str] = None
    span_id: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class TaskResult(Generic[ResultT]):
    job_id: str
    ok: bool
    value: Optional[ResultT] = None
    error: Optional[str] = None
    metrics: Optional[Dict[str, Any]] = None


class Worker(Protocol[PayloadT, ResultT]):
    async def process(self, job: Job[PayloadT], context: TaskContext) -> TaskResult[ResultT]:
        ...


class QueueReader(Protocol[PayloadT]):
    async def get(self) -> Job[PayloadT]:
        ...

    async def ack(self, job: Job[PayloadT]) -> None:
        ...

    async def nack(self, job: Job[PayloadT], requeue: bool = True) -> None:
        ...


class QueueWriter(Protocol[PayloadT]):
    async def put(self, job: Job[PayloadT]) -> None:
        ...


class Queue(QueueReader[PayloadT], QueueWriter[PayloadT], Protocol[PayloadT]):
    async def close(self) -> None:
        ...


# Convenience types
WorkerFn = Callable[[Job[PayloadT], TaskContext], Awaitable[TaskResult[ResultT]]]





