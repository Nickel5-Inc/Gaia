import os
import time
import asyncio
import traceback
from typing import Optional

from fiber.chain import chain_utils
from fiber.chain.metagraph import Metagraph
from substrateinterface import SubstrateInterface

from gaia.validator.utils.substrate_manager import SubstrateConnectionManager
from fiber.chain.fetch_nodes import get_nodes_for_netuid
from fiber.chain import weights as w
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class Neuron:
    def __init__(self, config):
        self.config = config
        self.substrate_manager: Optional[SubstrateConnectionManager] = None
        self.substrate: Optional[SubstrateInterface] = None
        self.metagraph: Optional[Metagraph] = None
        self.last_metagraph_sync = 0
        self.netuid = self.config.netuid
        self.wallet_name = self.config.wallet
        self.hotkey_name = self.config.hotkey
        self.keypair = None
        self.validator_uid = None

    def setup_neuron(self) -> bool:
        """
        Set up the neuron with necessary configurations and connections.
        """
        try:
            self.subtensor_chain_endpoint = (
                self.config.subtensor.chain_endpoint
                if hasattr(self.config, "subtensor")
                   and hasattr(self.config.subtensor, "chain_endpoint")
                else os.getenv(
                    "SUBTENSOR_ADDRESS", "wss://test.finney.opentensor.ai:443/"
                )
            )

            self.subtensor_network = (
                self.config.subtensor.network
                if hasattr(self.config, "subtensor")
                   and hasattr(self.config.subtensor, "network")
                else os.getenv("SUBTENSOR_NETWORK", "test")
            )

            self.keypair = chain_utils.load_hotkey_keypair(
                self.wallet_name, self.hotkey_name
            )

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

            self.substrate_manager = SubstrateConnectionManager(
                subtensor_network=self.subtensor_network,
                chain_endpoint=self.subtensor_chain_endpoint
            )

            try:
                self.substrate = self.substrate_manager.get_connection()
            except Exception as e_sub_init:
                logger.error(f"CRITICAL: Failed to initialize SubstrateInterface with endpoint {self.subtensor_chain_endpoint}: {e_sub_init}", exc_info=True)
                return False

            try:
                self.metagraph = Metagraph(substrate=self.substrate, netuid=self.netuid)
            except Exception as e_meta_init:
                logger.error(f"CRITICAL: Failed to initialize Metagraph: {e_meta_init}", exc_info=True)
                return False

            try:
                self.substrate = self.substrate_manager.get_connection()
                self.metagraph.sync_nodes()
                logger.info(f"Successfully synced {len(self.metagraph.nodes) if self.metagraph.nodes else '0'} nodes from the network.")
            except Exception as e_meta_sync:
                logger.error(f"CRITICAL: Metagraph sync_nodes() FAILED: {e_meta_sync}", exc_info=True)
                return False

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

    async def _fetch_nodes_managed(self, netuid):
        try:
            logger.debug(f"Fetching nodes for netuid {netuid} using ultra-aggressive memory management")
            
            if (self.metagraph and 
                hasattr(self.metagraph, 'nodes') and self.metagraph.nodes and
                time.time() - self.last_metagraph_sync < 300):
                
                logger.info(f"✅ Using cached metagraph nodes ({len(self.metagraph.nodes)} nodes) - NO substrate calls needed")
                cached_nodes = []
                for hotkey, node in self.metagraph.nodes.items():
                    if hasattr(node, 'node_id'):
                        cached_nodes.append(node)
                    else:
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
            
            logger.warning("No cached nodes available - making substrate call (potential memory leak)")
            
            import fiber.chain.interface as fiber_interface
            import fiber.chain.fetch_nodes as fetch_nodes_module
            
            original_get_substrate = fiber_interface.get_substrate
            original_fetch_get_substrate = getattr(fetch_nodes_module, 'get_substrate', None)
            
            def ultra_patched_get_substrate(*args, **kwargs):
                logger.warning("!!! SUBSTRATE CONNECTION INTERCEPTED - using managed connection instead !!!")
                return self.substrate
            
            fiber_interface.get_substrate = ultra_patched_get_substrate
            if original_fetch_get_substrate:
                fetch_nodes_module.get_substrate = ultra_patched_get_substrate
            
            try:
                self.substrate = self.substrate_manager.get_connection()
                nodes = get_nodes_for_netuid(self.substrate, netuid)
                logger.info(f"⚠️ Fetched {len(nodes) if nodes else 0} nodes with substrate call (check for new connections in logs)")
                return nodes
            finally:
                fiber_interface.get_substrate = original_get_substrate
                if original_fetch_get_substrate:
                    fetch_nodes_module.get_substrate = original_fetch_get_substrate
                
        except Exception as e:
            logger.error(f"Error in ultra-aggressive node fetching: {e}")
            logger.error(traceback.format_exc())
            logger.error("CRITICAL: All node fetching approaches failed - using direct call")
            return get_nodes_for_netuid(self.substrate, netuid)

    async def sync_metagraph(self, metagraph_sync_interval):
        current_time = time.time()
        if self.metagraph is None or current_time - self.last_metagraph_sync > metagraph_sync_interval:
            logger.info(f"Metagraph not initialized or sync interval ({metagraph_sync_interval}s) exceeded. Syncing metagraph. Last sync: {current_time - self.last_metagraph_sync if self.metagraph else 'Never'}s ago.")
            try:
                await asyncio.wait_for(self._sync_metagraph_logic(), timeout=60.0) 
            except asyncio.TimeoutError:
                logger.error("Metagraph sync timed out. Proceeding with potentially stale metagraph.")
            except Exception as e_sync:
                logger.error(f"Error during metagraph sync: {e_sync}. Proceeding with potentially stale metagraph.")
        else:
            logger.debug(f"Metagraph recently synced. Skipping sync. Last sync: {current_time - self.last_metagraph_sync:.2f}s ago.")

    async def _sync_metagraph_logic(self):
        """Sync the metagraph using managed substrate connection with custom implementation to prevent memory leaks."""
        sync_start = time.time()
        old_substrate = self.substrate
        self.substrate = self.substrate_manager.get_connection()
        
        if self.metagraph:
            self.metagraph.substrate = self.substrate
        
        try:
            if self.metagraph:
                logger.debug("Using ultra-aggressive caching with minimal substrate calls to prevent memory leaks")
                active_nodes_list = await self._fetch_nodes_managed(self.metagraph.netuid)
                
                if active_nodes_list:
                    self.metagraph.nodes = {node.hotkey: node for node in active_nodes_list}
                    logger.info(f"✅ Custom metagraph sync: Updated with {len(self.metagraph.nodes)} nodes using managed connection (NO memory leak)")
                else:
                    logger.warning("No nodes returned from custom node fetching")
                    self.metagraph.nodes = {}
            else:
                logger.error("Metagraph not initialized, cannot sync nodes")
                return
        except Exception as e:
            logger.error(f"Error during custom metagraph sync: {e}", exc_info=True)
            logger.warning("Falling back to regular metagraph.sync_nodes() - this may create memory leaks")
            if self.metagraph:
                self.metagraph.sync_nodes()
            
        sync_duration = time.time() - sync_start
        self.last_metagraph_sync = time.time()
        
        if sync_duration > 30:
            logger.warning(f"Slow metagraph sync: {sync_duration:.2f}s")
        
        connection_changed = old_substrate != self.substrate
        logger.debug(f"Custom metagraph sync completed in {sync_duration:.2f}s using managed connection (connection changed: {connection_changed})")
        
        if connection_changed:
            logger.info("Substrate connection refreshed during metagraph sync")

    async def get_current_block(self):
        try:
            resp = self.substrate.rpc_request("chain_getHeader", [])
            hex_num = resp["result"]["number"]
            return int(hex_num, 16)
        except Exception as e:
            logger.error(f"Error fetching current block: {e}")
            self.substrate = self.substrate_manager.get_connection() # Attempt to reconnect
            return 0 