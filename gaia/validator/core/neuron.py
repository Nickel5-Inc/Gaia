"""
Validator Neuron Core Module

Handles neuron setup, substrate connections, metagraph synchronization,
and basic neuron operations.
"""

import os
import time
import logging
import asyncio
import traceback
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from substrateinterface import SubstrateInterface

from fiber.chain import chain_utils, weights as w
from fiber.chain.fetch_nodes import get_nodes_for_netuid
from fiber.chain.metagraph import Metagraph
from fiber.logging_utils import get_logger
from gaia.validator.utils.substrate_manager import SubstrateConnectionManager

logger = get_logger(__name__)


class ValidatorNeuron:
    """
    Handles core neuron functionality including setup, substrate connections,
    and metagraph synchronization.
    """
    
    def __init__(self, args):
        """Initialize the validator neuron with configuration."""
        self.args = args
        self.netuid = None
        self.subtensor_chain_endpoint = None
        self.subtensor_network = None
        self.wallet_name = None
        self.hotkey_name = None
        self.keypair = None
        self.substrate = None
        self.substrate_manager: Optional[SubstrateConnectionManager] = None
        self.metagraph: Optional[Metagraph] = None
        self.validator_uid = None
        self.current_block = 0
        self.last_set_weights_block = 0
        self.last_metagraph_sync = time.time()
        self.metagraph_sync_interval = 300  # 5 minutes
        
    def setup_neuron(self) -> bool:
        """
        Set up the neuron with necessary configurations and connections.
        """
        try:
            load_dotenv(".env")
            self.netuid = (
                self.args.netuid if self.args.netuid else int(os.getenv("NETUID", 237))
            )
            logger.info(f"Using netuid: {self.netuid}")

            self.subtensor_chain_endpoint = (
                self.args.subtensor.chain_endpoint
                if hasattr(self.args, "subtensor")
                   and hasattr(self.args.subtensor, "chain_endpoint")
                else os.getenv(
                    "SUBTENSOR_ADDRESS", "wss://test.finney.opentensor.ai:443/"
                )
            )

            self.subtensor_network = (
                self.args.subtensor.network
                if hasattr(self.args, "subtensor")
                   and hasattr(self.args.subtensor, "network")
                else os.getenv("SUBTENSOR_NETWORK", "test")
            )

            self.wallet_name = (
                self.args.wallet
                if self.args.wallet
                else os.getenv("WALLET_NAME", "default")
            )
            self.hotkey_name = (
                self.args.hotkey
                if self.args.hotkey
                else os.getenv("HOTKEY_NAME", "default")
            )
            self.keypair = chain_utils.load_hotkey_keypair(
                self.wallet_name, self.hotkey_name
            )

            # Apply substrate query wrapper
            self._setup_substrate_patches()

            # Initialize substrate connection manager
            self.substrate_manager = SubstrateConnectionManager(
                subtensor_network=self.subtensor_network,
                chain_endpoint=self.subtensor_chain_endpoint
            )

            try:
                self.substrate = self.substrate_manager.get_connection()
            except Exception as e_sub_init:
                logger.error(f"CRITICAL: Failed to initialize SubstrateInterface with endpoint {self.subtensor_chain_endpoint}: {e_sub_init}", exc_info=True)
                return False

            # Initialize Metagraph
            try:
                self.metagraph = Metagraph(substrate=self.substrate, netuid=self.netuid)
            except Exception as e_meta_init:
                logger.error(f"CRITICAL: Failed to initialize Metagraph: {e_meta_init}", exc_info=True)
                return False

            # Sync Metagraph
            try:
                self.substrate = self.substrate_manager.get_connection()
                self.metagraph.sync_nodes()
                logger.info(f"Successfully synced {len(self.metagraph.nodes) if self.metagraph.nodes else '0'} nodes from the network.")
            except Exception as e_meta_sync:
                logger.error(f"CRITICAL: Metagraph sync_nodes() FAILED: {e_meta_sync}", exc_info=True)
                return False

            # Get current block and set initial weights block
            resp = self.substrate.rpc_request("chain_getHeader", [])  
            hex_num = resp["result"]["number"]
            self.current_block = int(hex_num, 16)
            logger.info(f"Initial block number type: {type(self.current_block)}, value: {self.current_block}")
            self.last_set_weights_block = self.current_block - 300

            # Get validator UID
            if self.validator_uid is None:
                self.validator_uid = self.substrate.query(
                    "SubtensorModule", 
                    "Uids", 
                    [self.netuid, self.keypair.ss58_address]
                ).value

            return True
        except Exception as e:
            logger.error(f"Error setting up neuron: {e}")
            logger.error(traceback.format_exc())
            return False

    def _setup_substrate_patches(self):
        """Set up substrate query patches for proper data handling."""
        original_query = SubstrateInterface.query
        def query_wrapper(self, module, storage_function, params, block_hash=None):
            result = original_query(self, module, storage_function, params, block_hash)
            if hasattr(result, 'value'):
                if isinstance(result.value, list):
                    result.value = [int(x) if hasattr(x, '__int__') else x for x in result.value]
                elif hasattr(result.value, '__int__'):
                    result.value = int(result.value)
            return result
        
        SubstrateInterface.query = query_wrapper

        original_blocks_since = w.blocks_since_last_update
        def blocks_since_wrapper(substrate, netuid, node_id):
            resp = self.substrate.rpc_request("chain_getHeader", [])  
            hex_num = resp["result"]["number"]
            current_block = int(hex_num, 16)
            last_updated_value = substrate.query(
                "SubtensorModule",
                "LastUpdate",
                [netuid]
            ).value
            if last_updated_value is None or node_id >= len(last_updated_value):
                return None
            last_update = int(last_updated_value[node_id])
            return current_block - last_update
        
        w.blocks_since_last_update = blocks_since_wrapper

    async def sync_metagraph(self):
        """Sync the metagraph using managed substrate connection."""
        sync_start = time.time()
        old_substrate = getattr(self, 'substrate', None)
        self.substrate = self.substrate_manager.get_connection()
        
        # Ensure metagraph uses the managed connection
        if hasattr(self, 'metagraph') and self.metagraph:
            self.metagraph.substrate = self.substrate
        
        try:
            if hasattr(self, 'metagraph') and self.metagraph:
                # Use custom node fetching to prevent memory leaks
                logger.debug("Using ultra-aggressive caching with minimal substrate calls to prevent memory leaks")
                active_nodes_list = await self._fetch_nodes_managed(self.metagraph.netuid)
                
                if active_nodes_list:
                    # Update metagraph nodes manually
                    self.metagraph.nodes = {node.hotkey: node for node in active_nodes_list}
                    logger.info(f"✅ Custom metagraph sync: Updated with {len(self.metagraph.nodes)} nodes using managed connection")
                else:
                    logger.warning("No nodes returned from custom node fetching")
                    self.metagraph.nodes = {}
            else:
                logger.error("Metagraph not initialized, cannot sync nodes")
                return
        except Exception as e:
            logger.error(f"Error during custom metagraph sync: {e}")
            logger.error(traceback.format_exc())
            # Fallback to regular sync_nodes if custom method fails
            logger.warning("Falling back to regular metagraph.sync_nodes() - this may create memory leaks")
            if hasattr(self, 'metagraph') and self.metagraph:
                self.metagraph.sync_nodes()
            
        sync_duration = time.time() - sync_start
        self.last_metagraph_sync = time.time()
        
        # Enhanced logging
        if sync_duration > 30:
            logger.warning(f"Slow metagraph sync: {sync_duration:.2f}s")
        
        connection_changed = old_substrate != self.substrate
        logger.debug(f"Custom metagraph sync completed in {sync_duration:.2f}s using managed connection (connection changed: {connection_changed})")
        
        if connection_changed:
            logger.info("Substrate connection refreshed during metagraph sync")

    async def _fetch_nodes_managed(self, netuid):
        """
        Fetch nodes using get_nodes_for_netuid but with aggressive connection management.
        Use cached nodes when possible to avoid substrate calls.
        """
        try:
            logger.debug(f"Fetching nodes for netuid {netuid} using ultra-aggressive memory management")
            
            # First, try to use cached nodes from metagraph if available and recent
            if (hasattr(self, 'metagraph') and self.metagraph and 
                hasattr(self.metagraph, 'nodes') and self.metagraph.nodes and
                hasattr(self, 'last_metagraph_sync') and 
                time.time() - self.last_metagraph_sync < 300):
                
                logger.info(f"✅ Using cached metagraph nodes ({len(self.metagraph.nodes)} nodes) - NO substrate calls needed")
                # Convert metagraph nodes dict to list format
                cached_nodes = []
                for hotkey, node in self.metagraph.nodes.items():
                    if hasattr(node, 'node_id'):
                        cached_nodes.append(node)
                    else:
                        # Create a simple node object if needed
                        simple_node = type('Node', (), {
                            'node_id': getattr(node, 'uid', 0),
                            'hotkey': hotkey,
                            'ip': getattr(node, 'ip', '0.0.0.0'),
                            'port': getattr(node, 'port', 0),
                            'ip_type': getattr(node, 'ip_type', 4),
                            'protocol': getattr(node, 'protocol', 4),
                            'placeholder1': 0,
                            'placeholder2': 0,
                        })()
                        cached_nodes.append(simple_node)
                return cached_nodes
            
            # If no cache available, make the substrate call
            logger.warning("No cached nodes available - making substrate call (potential memory leak)")
            
            # Patch to use managed connection
            import fiber.chain.interface as fiber_interface
            import fiber.chain.fetch_nodes as fetch_nodes_module
            
            original_get_substrate = fiber_interface.get_substrate
            original_fetch_get_substrate = getattr(fetch_nodes_module, 'get_substrate', None)
            
            def ultra_patched_get_substrate(*args, **kwargs):
                logger.warning("!!! SUBSTRATE CONNECTION INTERCEPTED - using managed connection instead !!!")
                return self.substrate
            
            # Apply patches
            fiber_interface.get_substrate = ultra_patched_get_substrate
            if original_fetch_get_substrate:
                fetch_nodes_module.get_substrate = ultra_patched_get_substrate
            
            try:
                # Force substrate manager to use fresh connection
                self.substrate = self.substrate_manager.get_connection()
                nodes = get_nodes_for_netuid(self.substrate, netuid)
                logger.info(f"⚠️ Fetched {len(nodes) if nodes else 0} nodes with substrate call")
                return nodes
            finally:
                # Always restore originals
                fiber_interface.get_substrate = original_get_substrate
                if original_fetch_get_substrate:
                    fetch_nodes_module.get_substrate = original_fetch_get_substrate
                
        except Exception as e:
            logger.error(f"Error in ultra-aggressive node fetching: {e}")
            logger.error(traceback.format_exc())
            # Final fallback
            logger.error("CRITICAL: All node fetching approaches failed - using direct call")
            return get_nodes_for_netuid(self.substrate, netuid)

    async def update_last_weights_block(self):
        """Update the last weights block number."""
        try:
            resp = self.substrate.rpc_request("chain_getHeader", [])  
            hex_num = resp["result"]["number"]
            block_number = int(hex_num, 16)
            self.last_set_weights_block = block_number
        except Exception as e:
            logger.error(f"Error updating last weights block: {e}")

    def get_current_block(self) -> int:
        """Get the current block number."""
        try:
            resp = self.substrate.rpc_request("chain_getHeader", [])  
            hex_num = resp["result"]["number"]
            self.current_block = int(hex_num, 16)
            return self.current_block
        except Exception as e:
            logger.error(f"Error getting current block: {e}")
            return self.current_block

    def cleanup(self):
        """Clean up neuron resources."""
        try:
            if hasattr(self, 'substrate_manager') and self.substrate_manager:
                self.substrate_manager.cleanup()
                logger.info("Cleaned up substrate connection manager")
        except Exception as e:
            logger.debug(f"Error cleaning up substrate manager: {e}")