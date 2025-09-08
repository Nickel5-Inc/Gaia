from __future__ import annotations

from typing import Dict

from new.validator.jobs.base import Handler, JobType


class JobRegistry:
    def __init__(self) -> None:
        self._handlers: Dict[JobType, Handler] = {}

    def register(self, job_type: JobType, handler: Handler) -> None:
        if job_type in self._handlers:
            raise ValueError(f"Handler already registered for {job_type}")
        self._handlers[job_type] = handler

    def get(self, job_type: JobType) -> Handler:
        handler = self._handlers.get(job_type)
        if handler is None:
            raise KeyError(f"No handler for {job_type}")
        return handler


