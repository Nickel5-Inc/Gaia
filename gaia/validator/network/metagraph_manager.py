import time
import asyncio
from typing import Dict, Optional, List, Any
from datetime import datetime, timezone

from fiber.logging_utils import get_logger
from fiber.chain.metagraph import Metagraph
from gaia.validator.utils.task_decorator import task_step
from gaia.validator.core.constants import (
    NETWORK_OP_TIMEOUT,
    MAX_MINERS,
    DEFAULT_NETUID
)

logger = get_logger(__name__)

class MetagraphManager:
    """
    Manages metagraph state and synchronization.
    
    This class handles:
    1. Metagraph state tracking
    2. Node synchronization
    3. Weight management
    4. Network state queries
    5. Miner registration tracking
    """
    
    def __init__(
        self,
        network_manager,
        netuid: int = DEFAULT_NETUID,
        sync_interval: int = 60,
        max_miners: int = MAX_MINERS
    ):
        self.network_manager = network_manager
        self.netuid = netuid
        self.sync_interval = sync_interval
        self.max_miners = max_miners
        
        # Initialize metagraph with substrate
        self.metagraph = Metagraph(
            substrate=self.network_manager.substrate,
            netuid=str(netuid)
        )
        self.last_sync = 0
        self.sync_errors = 0
        self.nodes_cache = {}
        
    @task_step(
        name="sync_metagraph",
        description="Synchronize metagraph state",
        timeout_seconds=NETWORK_OP_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=5
    )
    async def sync_metagraph(self) -> bool:
        """
        Synchronize the metagraph state.
        
        Returns:
            bool: True if sync successful, False otherwise
        """
        try:
            # Check if sync needed
            current_time = time.time()
            if current_time - self.last_sync < self.sync_interval:
                return True
                
            # Sync nodes
            self.metagraph.sync_nodes()
            self.last_sync = current_time
            self.sync_errors = 0
            
            # Update nodes cache
            self._update_nodes_cache()
            
            logger.info(
                f"Metagraph synced: {len(self.metagraph.nodes)} nodes, "
                f"netuid={self.netuid}"
            )
            return True
            
        except Exception as e:
            self.sync_errors += 1
            logger.error(f"Failed to sync metagraph: {e}")
            return False

    def _update_nodes_cache(self):
        """Update the nodes cache with current metagraph state."""
        try:
            self.nodes_cache = {
                hotkey: {
                    'ip': getattr(node, 'ip', None),
                    'port': getattr(node, 'port', None),
                    'uid': getattr(node, 'uid', None),
                    'stake': float(getattr(node, 'stake', 0.0)),
                    'last_update': getattr(node, 'last_update', None),
                    'active': bool(getattr(node, 'active', False)),
                    'validator_permit': bool(getattr(node, 'validator_permit', False))
                }
                for hotkey, node in self.metagraph.nodes.items()
            }
        except Exception as e:
            logger.error(f"Error updating nodes cache: {e}")

    @task_step(
        name="get_active_nodes",
        description="Get list of active nodes",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def get_active_nodes(self) -> List[str]:
        """
        Get list of currently active node hotkeys.
        
        Returns:
            list: Active node hotkeys
        """
        try:
            await self.sync_metagraph()
            return [
                hotkey
                for hotkey, info in self.nodes_cache.items()
                if info['active']
            ]
        except Exception as e:
            logger.error(f"Error getting active nodes: {e}")
            return []

    @task_step(
        name="get_validator_permit",
        description="Check if hotkey has validator permit",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def get_validator_permit(self, hotkey: str) -> bool:
        """
        Check if a hotkey has validator permit.
        
        Args:
            hotkey: Node hotkey to check
            
        Returns:
            bool: True if node has permit, False otherwise
        """
        try:
            await self.sync_metagraph()
            return self.nodes_cache.get(hotkey, {}).get('validator_permit', False)
        except Exception as e:
            logger.error(f"Error checking validator permit: {e}")
            return False

    @task_step(
        name="get_stake",
        description="Get stake for hotkey",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def get_stake(self, hotkey: str) -> float:
        """
        Get stake amount for a hotkey.
        
        Args:
            hotkey: Node hotkey to check
            
        Returns:
            float: Stake amount
        """
        try:
            await self.sync_metagraph()
            return self.nodes_cache.get(hotkey, {}).get('stake', 0.0)
        except Exception as e:
            logger.error(f"Error getting stake: {e}")
            return 0.0

    @task_step(
        name="get_node_info",
        description="Get complete node information",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def get_node_info(self, hotkey: str) -> Optional[Dict]:
        """
        Get complete information for a node.
        
        Args:
            hotkey: Node hotkey to query
            
        Returns:
            Optional[Dict]: Node information if found
        """
        try:
            await self.sync_metagraph()
            return self.nodes_cache.get(hotkey)
        except Exception as e:
            logger.error(f"Error getting node info: {e}")
            return None

    @task_step(
        name="set_weights",
        description="Set weights for nodes",
        timeout_seconds=NETWORK_OP_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=5
    )
    async def set_weights(
        self,
        weights: Dict[str, float],
        keypair: Any
    ) -> bool:
        """
        Set weights for nodes.
        
        Args:
            weights: Mapping of hotkeys to weights
            keypair: Keypair for signing transaction
            
        Returns:
            bool: True if weights set successfully
        """
        try:
            # Validate weights
            if not weights or len(weights) > self.max_miners:
                logger.error(
                    f"Invalid weights: {len(weights)} nodes "
                    f"(max: {self.max_miners})"
                )
                return False
                
            # Convert weights to UIDs format
            uid_weights = {}
            for hotkey, weight in weights.items():
                node_info = await self.get_node_info(hotkey)
                if node_info and node_info['active']:
                    uid_weights[node_info['uid']] = weight
                    
            if not uid_weights:
                logger.error("No valid nodes found for weight setting")
                return False
                
            # Submit transaction
            success = await self.network_manager.substrate.submit_extrinsic(
                call_module="SubtensorModule",
                call_function="set_weights",
                call_params={
                    "netuid": self.netuid,
                    "dests": list(uid_weights.keys()),
                    "weights": list(uid_weights.values())
                },
                keypair=keypair,
                wait_for_inclusion=True
            )
            
            if success:
                logger.info(
                    f"Successfully set weights for {len(uid_weights)} nodes"
                )
                return True
            else:
                logger.error("Failed to set weights")
                return False
                
        except Exception as e:
            logger.error(f"Error setting weights: {e}")
            return False

    def get_metagraph_state(self) -> Dict:
        """Get current metagraph state."""
        return {
            "netuid": self.netuid,
            "n_nodes": len(self.metagraph.nodes) if self.metagraph else 0,
            "last_sync": datetime.fromtimestamp(
                self.last_sync,
                tz=timezone.utc
            ) if self.last_sync else None,
            "sync_errors": self.sync_errors,
            "nodes": self.nodes_cache
        } 