from __future__ import annotations

import asyncio
from typing import Optional

import bittensor as bt  # type: ignore
import yaml

from new.core.neuron import BaseNeuron
from new.core.utils import config as cfg_utils
from new.miner.router.forward import build_default_router
from new.miner.server import MinerServer


def _load_yaml(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


class MinerNeuron(BaseNeuron):
    """Miner entrypoint.

    Responsibilities:
    - Read YAML config for proxy/ports
    - Build router and start Axon via MinerServer on the forwarding_port
    - Expose start/stop helpers
    """

    def __init__(self, config_path: str, args: Optional[dict] = None) -> None:
        super().__init__(args)
        self.config_path = config_path
        self.server: Optional[MinerServer] = None
        self._router = build_default_router()

    async def start(self) -> None:
        cfg = _load_yaml(self.config_path)
        proxy = cfg.get("proxy") or {}
        host = "127.0.0.1"
        port = int(proxy.get("forwarding_port", 44334))
        self.server = MinerServer(host=host, port=port, router=self._router)
        await self.server.start()

    async def stop(self) -> None:
        if self.server is not None:
            await self.server.stop()

    # BaseNeuron requires forward; Axon uses our server adapter, but we provide this for completeness
    async def forward(self, synapse: bt.Synapse) -> bt.Synapse:  # type: ignore[override]
        from new.miner.router.forward import forward as axon_forward
        return await axon_forward(synapse, self._router)

    # Wire node-specific config plumbing
    @classmethod
    def check_config(cls, config: "bt.Config"):
        return cfg_utils.check_config(cls, config)

    @classmethod
    def add_args(cls, parser):
        return cfg_utils.add_miner_args(cls, parser)

    @classmethod
    def config(cls):
        return cfg_utils.config(cls)


async def run_miner(config_path: str) -> None:
    miner = MinerNeuron(config_path, args=MinerNeuron.config())
    await miner.start()
    try:
        await asyncio.Event().wait()
    finally:
        await miner.stop()


