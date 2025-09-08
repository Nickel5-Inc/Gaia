from __future__ import annotations

from new.core.neuron import BaseNeuron
from new.core.utils import config as cfg_utils
import bittensor as bt  # type: ignore


class ValidatorNeuron(BaseNeuron):
    def __init__(self, args):
        super().__init__(args)
        # Endpoints are expected to be statically defined by the validator configuration.

    def run(self):
        # TODO: orchestrate job loop and miner queries
        ...

    def stop(self):
        pass

    def cleanup(self):
        pass

    # Wire node-specific config plumbing
    @classmethod
    def check_config(cls, config: "bt.Config"):
        return cfg_utils.check_config(cls, config)

    @classmethod
    def add_args(cls, parser):
        return cfg_utils.add_validator_args(cls, parser)

    @classmethod
    def config(cls):
        return cfg_utils.config(cls)


