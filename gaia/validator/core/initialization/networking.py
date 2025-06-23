"""
Network Components Initialization
=================================

Clean functions for setting up all networking components including
substrate connections, HTTP clients, and metagraph synchronization.
"""

import ssl
from typing import Dict, Any, Tuple, NamedTuple
import httpx
from fiber.logging_utils import get_logger
from fiber.chain import chain_utils, interface
from fiber.chain.metagraph import Metagraph

from ..config.constants import NETWORK_CONSTANTS, DEFAULT_TIMEOUTS

logger = get_logger(__name__)


class NetworkComponents(NamedTuple):
    """Container for all network components."""
    substrate: Any
    metagraph: Metagraph
    keypair: Any
    validator_uid: int
    miner_client: httpx.AsyncClient
    api_client: httpx.AsyncClient


async def initialize_network_components(config: Dict[str, Any]) -> NetworkComponents:
    """
    Initialize all network components including substrate, metagraph, and HTTP clients.
    
    Args:
        config: Network configuration dictionary
        
    Returns:
        NetworkComponents: All initialized network components
    """
    logger.info("🌐 Initializing network components...")
    
    # Step 1: Load keypair
    keypair = _load_keypair(config)
    logger.info(f"✅ Keypair loaded for {config['wallet_name']}/{config['hotkey_name']}")
    
    # Step 2: Setup substrate connection
    substrate = _setup_substrate_connection(config)
    logger.info("✅ Substrate connection established")
    
    # Step 3: Initialize metagraph
    metagraph = _initialize_metagraph(substrate, config)
    logger.info(f"✅ Metagraph initialized with {len(metagraph.nodes) if metagraph.nodes else 0} nodes")
    
    # Step 4: Get validator UID
    validator_uid = _get_validator_uid(substrate, config, keypair)
    logger.info(f"✅ Validator UID: {validator_uid}")
    
    # Step 5: Setup HTTP clients
    miner_client, api_client = _setup_http_clients()
    logger.info("✅ HTTP clients initialized")
    
    logger.info("✅ All network components initialized successfully")
    
    return NetworkComponents(
        substrate=substrate,
        metagraph=metagraph,
        keypair=keypair,
        validator_uid=validator_uid,
        miner_client=miner_client,
        api_client=api_client
    )


def _load_keypair(config: Dict[str, Any]) -> Any:
    """Load the validator keypair from wallet."""
    return chain_utils.load_hotkey_keypair(
        config['wallet_name'], 
        config['hotkey_name']
    )


def _setup_substrate_connection(config: Dict[str, Any]) -> Any:
    """Setup substrate connection with proper configuration."""
    try:
        substrate = interface.get_substrate(
            subtensor_network=config['subtensor_network'],
            subtensor_address=config['subtensor_chain_endpoint']
        )
        logger.info("🔄 Created fresh substrate connection")
        return substrate
    except Exception as e:
        logger.error(f"Failed to initialize substrate: {e}", exc_info=True)
        raise


def _initialize_metagraph(substrate: Any, config: Dict[str, Any]) -> Metagraph:
    """Initialize and sync the metagraph."""
    try:
        metagraph = Metagraph(substrate=substrate, netuid=config['netuid'])
        metagraph.sync_nodes()
        return metagraph
    except Exception as e:
        logger.error(f"Failed to initialize metagraph: {e}", exc_info=True)
        raise


def _get_validator_uid(substrate: Any, config: Dict[str, Any], keypair: Any) -> int:
    """Get the validator UID from the substrate."""
    try:
        validator_uid = substrate.query(
            "SubtensorModule", 
            "Uids", 
            [config['netuid'], keypair.ss58_address]
        ).value
        return int(validator_uid) if validator_uid is not None else 0
    except Exception as e:
        logger.error(f"Failed to get validator UID: {e}", exc_info=True)
        return 0


def _setup_http_clients() -> Tuple[httpx.AsyncClient, httpx.AsyncClient]:
    """Setup HTTP clients for miner and API communication."""
    
    # SSL context for miner client (no verification)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Miner client configuration
    miner_client = httpx.AsyncClient(
        timeout=httpx.Timeout(
            connect=DEFAULT_TIMEOUTS['http']['connect'],
            read=DEFAULT_TIMEOUTS['http']['read'],
            write=DEFAULT_TIMEOUTS['http']['write'],
            pool=DEFAULT_TIMEOUTS['http']['pool']
        ),
        follow_redirects=True,
        verify=False,
        limits=httpx.Limits(
            max_connections=NETWORK_CONSTANTS['connection_limits']['max_connections'],
            max_keepalive_connections=NETWORK_CONSTANTS['connection_limits']['max_keepalive_connections'],
            keepalive_expiry=NETWORK_CONSTANTS['connection_limits']['keepalive_expiry']
        ),
        transport=httpx.AsyncHTTPTransport(
            retries=NETWORK_CONSTANTS['retry_config']['max_retries'],
            verify=False
        ),
    )
    
    # API client configuration (with SSL verification)
    api_client = httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        limits=httpx.Limits(
            max_connections=100,
            max_keepalive_connections=20,
            keepalive_expiry=30,
        ),
        transport=httpx.AsyncHTTPTransport(retries=3),
    )
    
    return miner_client, api_client


async def cleanup_network_components(components: NetworkComponents) -> None:
    """Clean up all network components."""
    try:
        # Close HTTP clients
        if components.miner_client and not components.miner_client.is_closed:
            await components.miner_client.aclose()
            logger.info("✅ Miner HTTP client closed")
        
        if components.api_client and not components.api_client.is_closed:
            await components.api_client.aclose()
            logger.info("✅ API HTTP client closed")
        
        # Close substrate connection if it has a close method
        if hasattr(components.substrate, 'close'):
            try:
                components.substrate.close()
                logger.info("✅ Substrate connection closed")
            except Exception as e:
                logger.debug(f"Error closing substrate: {e}")
        
        logger.info("✅ Network components cleanup completed")
        
    except Exception as e:
        logger.error(f"Error during network cleanup: {e}", exc_info=True) 