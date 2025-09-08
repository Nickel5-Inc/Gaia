from __future__ import annotations

from typing import Awaitable, Callable, Dict

from bittensor import \
    Synapse  # Adapter surface only; handlers use our protocol

from new.shared.networking.protocol import (PROTO_VERSION, ForwardRequest,
                                            ForwardResponse, is_fresh)

Handler = Callable[[ForwardRequest], Awaitable[ForwardResponse]]


class ForwardRouter:
    def __init__(self, *, allowed_kinds: tuple[str, ...] = ()) -> None:
        self._handlers: Dict[str, Handler] = {}
        self._allowed = set(allowed_kinds) if allowed_kinds else None

    def register(self, kind: str, handler: Handler) -> None:
        if self._allowed is not None and kind not in self._allowed:
            raise ValueError(f"Kind '{kind}' not in allowlist")
        if kind in self._handlers:
            raise ValueError(f"Handler already registered for kind '{kind}'")
        self._handlers[kind] = handler

    async def dispatch(self, req: ForwardRequest) -> ForwardResponse:
        if req.proto_version != PROTO_VERSION:
            return ForwardResponse(request_id=req.request_id, ok=False, error="proto_version_mismatch")
        if not is_fresh(req.ts_unix_ms):
            return ForwardResponse(request_id=req.request_id, ok=False, error="stale_request")
        handler = self._handlers.get(req.kind)
        if handler is None:
            return ForwardResponse(request_id=req.request_id, ok=False, error="unknown_kind")
        return await handler(req)


# Axon adapter: converts Synapse <-> ForwardRequest/Response
async def forward(synapse: Synapse, router: ForwardRouter) -> Synapse:
    data = getattr(synapse, "body", None) or {}
    req = ForwardRequest.from_dict(data)
    resp = await router.dispatch(req)
    setattr(synapse, "body", resp.to_dict())
    return synapse


# Example handler and builder
async def handle_weather_forward(req: ForwardRequest) -> ForwardResponse:
    # Domain-specific logic goes here
    return ForwardResponse(request_id=req.request_id, ok=True, result={"echo": req.payload})


def build_default_router() -> ForwardRouter:
    router = ForwardRouter(allowed_kinds=("weather.forward",))
    router.register("weather.forward", handle_weather_forward)
    return router
