from __future__ import annotations

from typing import Any, Dict

from new.core.utils.config import Config
from new.shared.networking.endpoint import Endpoint
from new.validator.jobs.base import Job, JobContext, JobResult
from new.validator.runtime.query import query_miners as _query_miners


async def handle_query_miners(ctx: JobContext, job: Job) -> JobResult:
    try:
        payload: Dict[str, Any] = job.payload or {}
        kind: str = payload.get("kind", "weather.forward")
        body: Dict[str, Any] = payload.get("payload", {})
        endpoints_cfg = ctx.yaml_config.get("endpoints") or []
        endpoints = [Endpoint(url=e) if isinstance(e, str) else Endpoint(**e) for e in endpoints_cfg]
        responses = await _query_miners(endpoints, kind, body, Config())
        return JobResult(ok=True, data={"count": len(responses)})
    except Exception as e:
        return JobResult(ok=False, error=str(e))


