# Shared protocol for validator/miner communication

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional


PROTO_VERSION = "1.0"


@dataclass
class ForwardRequest:
    """
    Minimal, transport-agnostic request body.

    kind: logical route for the single forward entrypoint (e.g., "weather.forward").
    payload: free-form dictionary specific to the kind.
    request_id: client-generated identifier for tracing.
    ts_unix_ms: timestamp for freshness/replay protection at the router boundary.
    proto_version: semantic versioning for wire compatibility.
    """

    kind: str
    payload: Dict[str, Any]
    request_id: str
    ts_unix_ms: int
    proto_version: str = PROTO_VERSION

    @staticmethod
    def new(kind: str, payload: Dict[str, Any]) -> "ForwardRequest":
        return ForwardRequest(
            kind=kind,
            payload=payload,
            request_id=str(uuid.uuid4()),
            ts_unix_ms=int(time.time() * 1000),
            proto_version=PROTO_VERSION,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ForwardRequest":
        return ForwardRequest(
            kind=data["kind"],
            payload=data.get("payload", {}),
            request_id=data["request_id"],
            ts_unix_ms=int(data["ts_unix_ms"]),
            proto_version=data.get("proto_version", PROTO_VERSION),
        )


@dataclass
class ForwardResponse:
    """
    Minimal, transport-agnostic response body.
    """

    request_id: str
    ok: bool
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    proto_version: str = PROTO_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ForwardResponse":
        return ForwardResponse(
            request_id=data.get("request_id", ""),
            ok=bool(data.get("ok", False)),
            result=data.get("result", None),
            error=data.get("error", None),
            proto_version=data.get("proto_version", PROTO_VERSION),
        )


def is_fresh(ts_unix_ms: int, window_ms: int = 120_000) -> bool:
    """Basic freshness check for replay protection at router boundary."""
    now = int(time.time() * 1000)
    return abs(now - int(ts_unix_ms)) <= window_ms
