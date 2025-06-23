"""
Network Operations for Gaia Validator
=====================================

Clean, functional network operations organized by responsibility.
All network communication functions are imported from here.
"""

from .miners import query_miners, cleanup_idle_connections
from .substrate import create_fresh_substrate_connection, sync_metagraph
from .metagraph import fetch_network_nodes, update_miner_information
from .http_clients import monitor_client_health

__all__ = [
    'query_miners',
    'cleanup_idle_connections',
    'create_fresh_substrate_connection',
    'sync_metagraph', 
    'fetch_network_nodes',
    'update_miner_information',
    'monitor_client_health'
] 