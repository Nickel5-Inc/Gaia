from __future__ import annotations

from new.validator.runtime.entry import run_validator
from new.validator.runtime.neuron import ValidatorNeuron
from new.validator.runtime.query import query_miners

# Thin facade: import public entrypoints/classes from submodules to preserve existing import paths



if __name__ == "__main__":
    import argparse
    import asyncio
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="Path to validator YAML config (optional)")
    args = parser.parse_args()
    config_path = args.config or os.environ.get("VALIDATOR_CONFIG") or "/root/Gaia/new/config/validator.config.yaml"
    asyncio.run(run_validator(config_path))