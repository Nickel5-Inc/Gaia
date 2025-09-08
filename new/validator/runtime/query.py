from __future__ import annotations

from typing import Any, Dict, List

from new.core.utils.config import Config
from new.shared.networking.endpoint import Endpoint
from new.shared.networking.protocol import ForwardRequest, ForwardResponse
from new.validator.network.dendrite_client import DendriteClientAdapter


async def query_miners(endpoints: List[Endpoint | str], kind: str, payload: Dict[str, Any], config: Config) -> List[ForwardResponse]:
    client = DendriteClientAdapter(config)
    req = ForwardRequest.new(kind=kind, payload=payload)
    tasks = [client.send(ep, req) for ep in endpoints]
    return await __import__("asyncio").gather(*tasks)


