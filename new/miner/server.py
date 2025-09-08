from __future__ import annotations

import asyncio
import inspect
from typing import Callable, Optional

import bittensor as bt  # type: ignore

from new.miner.router.forward import ForwardRouter, build_default_router
from new.miner.router.forward import forward as axon_forward


class MinerServer:
    def __init__(self, host: str, port: int, router: Optional[ForwardRouter] = None) -> None:
        self.host = host
        self.port = port
        self.router = router or build_default_router()
        self.axon: any | None = None

    async def start(self) -> None:
        # NOTE: skeleton – wire actual Axon start and forward binding
        # Construct an Axon-like server from bittensor
        axon_ctor: Callable[..., any] | None = None
        if hasattr(bt, "Axon"):
            axon_ctor = getattr(bt, "Axon")
        elif hasattr(bt, "axon"):
            axon_ctor = getattr(bt, "axon")
        if axon_ctor is None:
            raise RuntimeError("No Axon constructor found in bittensor SDK")

        # Some SDKs use kwargs (ip, port), others prefer wallet/serving kwargs; start minimal
        try:
            self.axon = axon_ctor(ip=self.host, port=self.port)  # type: ignore[call-arg]
        except TypeError:
            # Fallback to default ctor then set fields if available
            self.axon = axon_ctor()  # type: ignore[call-arg]
            if hasattr(self.axon, "ip"):
                setattr(self.axon, "ip", self.host)
            if hasattr(self.axon, "port"):
                setattr(self.axon, "port", self.port)

        async def _cb(synapse):
            return await axon_forward(synapse, self.router)

        self.axon.forward = _cb  # type: ignore[attr-defined]

        # Start/serve axon depending on SDK version
        if hasattr(self.axon, "serve") and inspect.iscoroutinefunction(self.axon.serve):
            await self.axon.serve()
        elif hasattr(self.axon, "serve"):
            self.axon.serve()
        elif hasattr(self.axon, "start") and inspect.iscoroutinefunction(self.axon.start):
            await self.axon.start()
        elif hasattr(self.axon, "start"):
            self.axon.start()
        else:
            # As a last resort, yield control so outer loop can proceed
            await asyncio.sleep(0)

    async def stop(self) -> None:
        if self.axon is None:
            return
        # Attempt graceful shutdown
        if hasattr(self.axon, "close") and inspect.iscoroutinefunction(self.axon.close):
            await self.axon.close()
        elif hasattr(self.axon, "close"):
            self.axon.close()
        elif hasattr(self.axon, "stop") and inspect.iscoroutinefunction(self.axon.stop):
            await self.axon.stop()
        elif hasattr(self.axon, "stop"):
            self.axon.stop()
        self.axon = None


