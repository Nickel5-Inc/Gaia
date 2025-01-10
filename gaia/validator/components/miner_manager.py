from typing import Dict, List, Optional
import time
import asyncio
from datetime import datetime

from fiber.logging_utils import get_logger
from fiber.chain.metagraph import Metagraph
from substrateinterface import SubstrateInterface
from gaia.validator.utils.task_decorator import task_step
from gaia.validator.core.constants import MAX_MINERS

logger = get_logger(__name__)

class MinerManager:
    """
    Manages miner-related operations for the validator.
    
    This class handles:
    1. Miner table updates and synchronization
    2. Deregistration detection and handling
    3. Miner state management
    4. Score recalculation for deregistered miners
    """
    
    def __init__(
        self,
        database_manager,
        metagraph: Metagraph,
        substrate: SubstrateInterface,
        soil_task,
        geomagnetic_task
    ):
        self.database_manager = database_manager
        self.metagraph = metagraph
        self.substrate = substrate
        self.soil_task = soil_task
        self.geomagnetic_task = geomagnetic_task
        
        # State
        self.nodes = {}  # In-memory node table state
        self.last_dereg_check_start = time.time()
        self.last_successful_dereg_check = time.time()

    @task_step(
        name="sync_active_miners",
        description="Synchronize and fetch current active miners from the network",
        timeout_seconds=60,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def sync_active_miners(self) -> Dict:
        """
        Sync metagraph and get current active miners.
        
        Returns:
            dict: Mapping of UIDs to miner information
        """
        self.metagraph.sync_nodes()
        return {
            idx: {"hotkey": hotkey, "uid": idx}
            for idx, (hotkey, _) in enumerate(self.metagraph.nodes.items())
        }

    @task_step(
        name="fetch_registered_miners",
        description="Fetch previously registered miners from database",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def fetch_registered_miners(self) -> Dict:
        """
        Fetch previously registered miners from the database.
        
        Returns:
            dict: Mapping of UIDs to miner information
        """
        if not self.nodes:
            query = "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL"
            rows = await self.database_manager.fetch_many(query)
            self.nodes = {
                row["uid"]: {"hotkey": row["hotkey"], "uid": row["uid"]}
                for row in rows
            }
        return self.nodes

    @task_step(
        name="process_deregistered_miners",
        description="Process and handle deregistered miners",
        timeout_seconds=90,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def process_deregistered_miners(self, active_miners: Dict, registered_miners: Dict) -> List[int]:
        """
        Find and process deregistered miners.
        
        Args:
            active_miners (dict): Currently active miners
            registered_miners (dict): Previously registered miners
            
        Returns:
            list: UIDs of deregistered miners that were processed
        """
        deregistered_miners = []
        for uid, registered in registered_miners.items():
            if active_miners[uid]["hotkey"] != registered["hotkey"]:
                deregistered_miners.append(registered)
                self.nodes[uid]["hotkey"] = active_miners[uid]["hotkey"]

        if deregistered_miners:
            logger.info(f"Found {len(deregistered_miners)} deregistered miners")
            uids = [int(miner["uid"]) for miner in deregistered_miners]
            
            # Recalculate scores
            await self.soil_task.recalculate_recent_scores(uids)
            await self.geomagnetic_task.recalculate_recent_scores(uids)
            
            logger.info(f"Successfully recalculated scores for {len(uids)} miners")
            return uids
        return []

    @task_step(
        name="update_miner_table",
        description="Sync and update miner information from metagraph",
        timeout_seconds=120,  # 2 minutes
        max_retries=3,
        retry_delay_seconds=10
    )
    async def update_miner_table(self):
        """
        Update the miner table with the latest miner information from the metagraph.
        
        This method:
        1. Syncs the metagraph
        2. Updates the database with current miner information
        3. Updates the in-memory state
        
        Raises:
            Exception: If update fails after retries
        """
        try:
            if self.metagraph is None:
                logger.error("Metagraph not initialized")
                return

            self.metagraph.sync_nodes()
            logger.info(f"Synced {len(self.metagraph.nodes)} nodes from the network")

            for index, (hotkey, node) in enumerate(self.metagraph.nodes.items()):
                await self.database_manager.update_miner_info(
                    index=index,
                    hotkey=node.hotkey,
                    coldkey=node.coldkey,
                    ip=node.ip,
                    ip_type=str(node.ip_type),
                    port=node.port,
                    incentive=float(node.incentive),
                    stake=float(node.stake),
                    trust=float(node.trust),
                    vtrust=float(node.vtrust),
                    protocol=str(node.protocol),
                )
                self.nodes[index] = {"hotkey": node.hotkey, "uid": index}
                logger.debug(f"Updated information for node {index}")

            logger.info("Successfully updated miner table and in-memory state")

        except Exception as e:
            logger.error(f"Error updating miner table: {str(e)}")
            logger.error(f"Error details: {e.__class__.__name__}")
            raise

    async def check_deregistrations(self):
        """
        Run a complete deregistration check cycle.
        
        This method:
        1. Syncs current active miners
        2. Fetches registered miners
        3. Processes any deregistrations
        
        Returns:
            list: UIDs of any deregistered miners that were processed
        """
        try:
            self.last_dereg_check_start = time.time()
            
            # Stage 1: Sync active miners
            active_miners = await self.sync_active_miners()
            
            # Stage 2: Get registered miners
            registered_miners = await self.fetch_registered_miners()
            
            # Stage 3: Process deregistrations
            processed_uids = await self.process_deregistered_miners(
                active_miners, 
                registered_miners
            )
            
            if processed_uids:
                logger.info(
                    f"Deregistration cycle completed successfully. "
                    f"Processed UIDs: {processed_uids}"
                )
            else:
                logger.debug("No deregistrations found in this cycle")
            
            self.last_successful_dereg_check = time.time()
            return processed_uids
            
        except Exception as e:
            logger.error(f"Error in deregistration check: {e}")
            logger.error(f"Error details: {e.__class__.__name__}")
            
            # Attempt recovery
            try:
                await self.database_manager.reset_pool()
                self.metagraph.sync_nodes()
                logger.info("Successfully recovered from deregistration error")
            except Exception as recovery_error:
                logger.error(f"Failed to recover from deregistration error: {recovery_error}")
            
            return []

    def get_active_miners_count(self) -> int:
        """Get the current count of active miners."""
        return len(self.metagraph.nodes) if self.metagraph else 0

    def get_miner_info(self, uid: int) -> Optional[Dict]:
        """Get information for a specific miner by UID."""
        return self.nodes.get(uid) 