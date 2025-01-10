from datetime import datetime, timezone
import time
import asyncio
from typing import Optional, Dict, Any

from fiber.logging_utils import get_logger
from fiber.chain.metagraph import Metagraph
from substrateinterface import SubstrateInterface
from gaia.validator.utils.task_decorator import task_step
from gaia.validator.core.constants import STATUS_UPDATE_TIMEOUT, STATUS_SLEEP

logger = get_logger(__name__)

class StatusMonitor:
    """
    Monitors and logs validator status information.
    
    This class handles:
    1. Regular status updates and logging
    2. Chain block monitoring
    3. Node count tracking
    4. Weight setting monitoring
    5. Performance metrics collection
    """
    
    def __init__(
        self,
        metagraph: Metagraph,
        substrate: SubstrateInterface,
        chain_endpoint: str,
    ):
        self.metagraph = metagraph
        self.substrate = substrate
        self.chain_endpoint = chain_endpoint
        self._running = False
        
        # State tracking
        self.current_block = 0
        self.last_set_weights_block = 0
        self.last_status_check = time.time()
        self.performance_metrics: Dict[str, Any] = {}

    @task_step(
        name="fetch_chain_status",
        description="Fetch current chain status",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _fetch_chain_status(self) -> Optional[Dict[str, int]]:
        """
        Fetch current chain status information.
        
        Returns:
            Optional[Dict[str, int]]: Chain status info if successful, None otherwise
        """
        try:
            block = self.substrate.get_block()
            if not block or "header" not in block or "number" not in block["header"]:
                logger.error("Invalid block data received")
                return None
                
            self.current_block = block["header"]["number"]
            blocks_since_weights = (
                self.current_block - self.last_set_weights_block
            )
            
            return {
                "current_block": self.current_block,
                "blocks_since_weights": blocks_since_weights
            }
            
        except Exception as e:
            logger.error(f"Failed to get block: {e}")
            try:
                # Attempt substrate reconnection
                self.substrate = SubstrateInterface(
                    url=self.chain_endpoint
                )
                logger.info("Successfully reconnected to substrate")
            except Exception as reconnect_error:
                logger.error(f"Failed to reconnect to substrate: {reconnect_error}")
            return None

    @task_step(
        name="collect_metrics",
        description="Collect validator performance metrics",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def _collect_metrics(self) -> Dict[str, Any]:
        """
        Collect current validator performance metrics.
        
        Returns:
            Dict[str, Any]: Performance metrics including node counts and timing info
        """
        try:
            active_nodes = len(self.metagraph.nodes) if self.metagraph else 0
            
            metrics = {
                "active_nodes": active_nodes,
                "max_nodes": 256,
                "node_percentage": (active_nodes / 256) * 100 if active_nodes > 0 else 0,
                "last_status_check_age": time.time() - self.last_status_check,
                "chain_endpoint": self.chain_endpoint,
                "is_healthy": await self.check_health()
            }
            
            self.performance_metrics.update(metrics)
            return metrics
            
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            return {
                "active_nodes": 0,
                "max_nodes": 256,
                "node_percentage": 0,
                "last_status_check_age": time.time() - self.last_status_check,
                "chain_endpoint": self.chain_endpoint,
                "is_healthy": False
            }

    @task_step(
        name="log_status_update",
        description="Generate and log status update",
        timeout_seconds=STATUS_UPDATE_TIMEOUT,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def log_status_update(self) -> bool:
        """
        Generate and log a comprehensive status update.
        
        Returns:
            bool: True if status update successful, False otherwise
        """
        try:
            current_time_utc = datetime.now(timezone.utc)
            formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S")
            
            # Fetch chain status
            chain_status = await self._fetch_chain_status()
            if not chain_status:
                logger.error("Failed to fetch chain status")
                return False
                
            # Collect metrics
            metrics = await self._collect_metrics()
            
            # Log comprehensive status
            logger.info(
                f"\n"
                f"=== Status Update ===\n"
                f"Time (UTC): {formatted_time}\n"
                f"Chain Status:\n"
                f"  - Block: {chain_status['current_block']}\n"
                f"  - Blocks Since Weights: {chain_status['blocks_since_weights']}\n"
                f"Network Status:\n"
                f"  - Active Nodes: {metrics['active_nodes']}/{metrics['max_nodes']} "
                f"({metrics['node_percentage']:.1f}%)\n"
                f"  - Chain Endpoint: {metrics['chain_endpoint']}\n"
                f"Performance:\n"
                f"  - Last Check Age: {metrics['last_status_check_age']:.1f}s\n"
                f"  - Health Check: {'Passed' if metrics['is_healthy'] else 'Failed'}"
            )
            
            self.last_status_check = time.time()
            return True
            
        except Exception as e:
            logger.error(f"Error in status update: {e}")
            logger.error(f"Error details: {e.__class__.__name__}")
            return False

    async def start(self) -> None:
        """Start the status monitor."""
        self._running = True
        await self.start_monitoring()

    async def stop(self) -> None:
        """Stop the status monitor."""
        self._running = False

    async def start_monitoring(self) -> None:
        """Start monitoring validator status."""
        while self._running:
            try:
                await self.log_status_update()
                await asyncio.sleep(STATUS_SLEEP)
            except Exception as e:
                logger.error(f"Error in status monitoring: {e}")
                await asyncio.sleep(10)  # Shorter sleep on error

    async def check_health(self) -> bool:
        """
        Check health of monitored components.
        
        Returns:
            bool: True if all components are healthy, False otherwise
        """
        try:
            # Check substrate connection
            if not self.substrate:
                logger.error("Substrate connection not initialized")
                return False
                
            # Check metagraph
            if not self.metagraph:
                logger.error("Metagraph not initialized")
                return False
                
            # Check if we can get current block
            block = self.substrate.get_block()
            if not block or "header" not in block or "number" not in block["header"]:
                logger.error("Unable to fetch valid block data")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def update_weights_block(self, block_number: int) -> None:
        """
        Update the last weights block number.
        
        Args:
            block_number: The block number where weights were last set
        """
        self.last_set_weights_block = block_number
        
    def get_current_metrics(self) -> Dict[str, Any]:
        """
        Get the current performance metrics.
        
        Returns:
            Dict[str, Any]: Copy of current performance metrics
        """
        return self.performance_metrics.copy() 