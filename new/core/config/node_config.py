from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional


TLSMode = Literal["self_signed", "letsencrypt", "off"]
HostingMode = Literal["s3", "r2", "minio", "static"]
Role = Literal["validator", "miner"]


@dataclass(frozen=True)
class ProxyConfig:
    enabled: bool = True
    server_name: str = "example.com"
    server_ip: str = "127.0.0.1"
    public_port: int = 33333
    forwarding_port: int = 33334
    tls_mode: TLSMode = "self_signed"


@dataclass(frozen=True)
class HostingConfig:
    mode: HostingMode = "static"
    # static
    root_path: Optional[str] = None
    # s3/r2/minio
    endpoint: Optional[str] = None
    bucket: Optional[str] = None
    region: Optional[str] = None


@dataclass(frozen=True)
class NodeConfig:
    role: Role
    proxy: ProxyConfig


@dataclass(frozen=True)
class MinerConfig(NodeConfig):
    hosting: HostingConfig


@dataclass(frozen=True)
class ValidatorConfig(NodeConfig):
    pass


