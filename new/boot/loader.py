from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


Role = Literal["validator", "miner"]


@dataclass(frozen=True)
class BootConfig:
    role: Role
    # path to node YAML for role-specific settings (proxy, hosting)
    config_path: str


def boot(config: BootConfig) -> None:
    # perform role-agnostic preflight (nginx/tls setup) before loading role modules
    from new.core.setup.proxy import ensure_proxy_config
    from new.core.setup.hosting import ensure_hosting_config
    ensure_proxy_config(config.config_path)
    ensure_hosting_config(config.config_path)
    if config.role == "validator":
        # Lazy import validator modules only
        # Placeholder imports removed; explicit imports should be added when needed
        return
    if config.role == "miner":
        # Placeholder imports removed; explicit imports should be added when needed
        return
    raise ValueError(f"Unknown role: {config.role}")


