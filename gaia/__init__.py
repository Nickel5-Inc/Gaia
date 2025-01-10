"""Gaia Validator Package.

This package implements the Gaia subnet for Bittensor, providing
validator and miner functionality for geospatial data validation.
"""

import configparser
from pathlib import Path

# Read version from setup.cfg
config = configparser.ConfigParser()
setup_cfg = Path(__file__).parent.parent / "setup.cfg"
config.read(setup_cfg)

__version__ = config["metadata"]["version"]
__database_version__ = config["metadata"]["database_version"]
