from __future__ import annotations

import asyncio
from typing import Any

from new.core.utils.config import Config
from new.shared.networking.protocol import ForwardRequest, ForwardResponse
from new.shared.networking.endpoint import Endpoint


class DendriteClientAdapter:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._bt = None  # late init of bittensor dendrite
        self._dendrite = None

    async def send(self, endpoint: Any, req: ForwardRequest) -> ForwardResponse:
        try:
            import bittensor as bt  # type: ignore
            if self._dendrite is None:
                self._dendrite = bt.Dendrite()
            # Build a synapse-like object with body dict compatible with our Axon adapter
            syn = bt.Synapse()
            setattr(syn, "body", req.to_dict())
            # Prefer native axon_info if provided, else fall back to URL/string
            target = None
            if isinstance(endpoint, Endpoint) and endpoint.axon_info is not None:
                target = endpoint.axon_info
            elif isinstance(endpoint, Endpoint) and endpoint.url is not None:
                target = endpoint.url
            else:
                target = endpoint

            # Some SDKs use dendrite.query, others call synapse directly; try both
            if hasattr(self._dendrite, "query"):
                resp_syn = await self._dendrite.query(target, syn)  # type: ignore[attr-defined]
            else:
                resp_syn = await self._dendrite(target, syn)  # type: ignore[operator]
            data = getattr(resp_syn, "body", {}) or {}
            return ForwardResponse.from_dict(data)
        except Exception:
            # Fallback: placeholder
            await asyncio.sleep(0)
            return ForwardResponse(request_id=req.request_id, ok=False, error="send_failed")


