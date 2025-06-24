"""
Network service for the Gaia Validator service architecture.

This module provides high-performance miner communication with connection pooling,
load balancing, and intelligent retry logic for maximum throughput.
"""

from .service import NetworkService

__all__ = [
    'NetworkService',
] 