import time
import asyncio
from typing import Dict, Optional, Any
from datetime import datetime, timezone

from fiber.logging_utils import get_logger
from gaia.validator.utils.task_decorator import task_step
from gaia.validator.core.constants import (
    NETWORK_OP_TIMEOUT,
    DEFAULT_NETUID,
    DEFAULT_WALLET,
    DEFAULT_HOTKEY
)

logger = get_logger(__name__)

class ValidatorState:
    """
    Manages validator registration and state.
    
    This class handles:
    1. Validator registration
    2. State management
    3. Wallet operations
    4. Registration verification
    5. State persistence
    """
    
    def __init__(
        self,
        network_manager,
        metagraph_manager,
        database_manager,
        wallet_name: str = DEFAULT_WALLET,
        hotkey_name: str = DEFAULT_HOTKEY,
        netuid: int = DEFAULT_NETUID
    ):
        self.network_manager = network_manager
        self.metagraph_manager = metagraph_manager
        self.database_manager = database_manager
        self.wallet_name = wallet_name
        self.hotkey_name = hotkey_name
        self.netuid = netuid
        self.uid = None
        self.stake = None
        self.validator_permit = False
        
        # State tracking
        self.is_registered = False
        self.registration_block = None
        self.last_update = None
        
    @task_step(
        name="load_state",
        description="Load validator state from database",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def load_state(self) -> bool:
        """
        Load validator state from database.
        
        Returns:
            bool: True if state loaded successfully
        """
        try:
            # Load validator state from database
            state = await self.database_manager.fetch_one(
                """
                SELECT uid, stake, validator_permit, registration_block, last_update
                FROM validator_state 
                WHERE wallet_name = :wallet AND hotkey_name = :hotkey
                """,
                {
                    "wallet": self.wallet_name,
                    "hotkey": self.hotkey_name
                }
            )
            if state:
                self.uid = state["uid"]
                self.stake = state["stake"]
                self.validator_permit = state["validator_permit"]
                self.registration_block = state["registration_block"]
                self.last_update = state["last_update"]
                self.is_registered = True if self.uid is not None else False
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            return False

    @task_step(
        name="save_state",
        description="Save validator state to database",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def save_state(self) -> bool:
        """
        Save current validator state to database.
        
        Returns:
            bool: True if state saved successfully
        """
        try:
            query = """
                INSERT INTO validator_state (
                    wallet_name,
                    hotkey_name,
                    registration_block,
                    last_update,
                    validator_permit,
                    stake,
                    uid
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (wallet_name, hotkey_name) DO UPDATE
                SET registration_block = $3,
                    last_update = $4,
                    validator_permit = $5,
                    stake = $6,
                    uid = $7
            """
            
            await self.database_manager.execute(
                query,
                self.wallet_name,
                self.hotkey_name,
                self.registration_block,
                self.last_update,
                self.validator_permit,
                self.stake,
                self.uid
            )
            
            logger.info("Saved validator state to database")
            return True
            
        except Exception as e:
            logger.error(f"Error saving validator state: {e}")
            return False

    @task_step(
        name="check_registration",
        description="Check validator registration status",
        timeout_seconds=NETWORK_OP_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=5
    )
    async def check_registration(self, keypair: Any) -> bool:
        """
        Check if validator is registered on network.
        
        Args:
            keypair: Validator keypair
            
        Returns:
            bool: True if registered
        """
        try:
            # Check if validator is registered
            self.uid = await self._get_validator_uid(keypair)
            if self.uid is not None:
                self.stake = await self._get_validator_stake(self.uid)
                self.validator_permit = await self._check_validator_permit(self.uid)
                return True
            return False
        except Exception as e:
            logger.error(f"Error checking registration: {e}")
            return False

    async def _update_validator_info(self, hotkey: str):
        """Update validator information from network."""
        try:
            # Get validator permit
            self.validator_permit = await self.metagraph_manager.get_validator_permit(
                hotkey
            )
            
            # Get stake
            self.stake = await self.metagraph_manager.get_stake(hotkey)
            
            # Get UID
            node_info = await self.metagraph_manager.get_node_info(hotkey)
            if node_info:
                self.uid = node_info['uid']
                
            self.last_update = int(time.time())
            
        except Exception as e:
            logger.error(f"Error updating validator info: {e}")

    @task_step(
        name="register_validator",
        description="Register validator on network",
        timeout_seconds=NETWORK_OP_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=5
    )
    async def register_validator(self, keypair: Any) -> bool:
        """
        Register validator on the network.
        
        Args:
            keypair: Validator keypair
            
        Returns:
            bool: True if registration successful
        """
        try:
            # Register validator if not already registered
            if not await self.check_registration(keypair):
                success = await self._register_new_validator(keypair)
                if success:
                    return await self.check_registration(keypair)
            return False
        except Exception as e:
            logger.error(f"Error registering validator: {e}")
            return False

    def get_state(self) -> Dict:
        """Get current validator state."""
        return {
            "is_registered": self.is_registered,
            "registration_block": self.registration_block,
            "last_update": datetime.fromtimestamp(
                self.last_update,
                tz=timezone.utc
            ) if self.last_update else None,
            "validator_permit": self.validator_permit,
            "stake": self.stake,
            "uid": self.uid,
            "wallet": self.wallet_name,
            "hotkey": self.hotkey_name,
            "netuid": self.netuid
        } 

    async def _get_validator_uid(self, keypair: Any) -> Optional[int]:
        """Get validator UID from network."""
        try:
            return await self.network_manager.substrate.query(
                "SubtensorModule",
                "Uids",
                [self.netuid, keypair.ss58_address]
            ).value
        except Exception as e:
            logger.error(f"Error getting validator UID: {e}")
            return None

    async def _get_validator_stake(self, uid: int) -> float:
        """Get validator stake from network."""
        try:
            stake = await self.network_manager.substrate.query(
                "SubtensorModule",
                "Stake",
                [self.netuid, uid]
            ).value
            return float(stake) if stake is not None else 0.0
        except Exception as e:
            logger.error(f"Error getting validator stake: {e}")
            return 0.0

    async def _check_validator_permit(self, uid: int) -> bool:
        """Check if validator has permit."""
        try:
            permit = await self.network_manager.substrate.query(
                "SubtensorModule",
                "ValidatorPermit",
                [self.netuid, uid]
            ).value
            return bool(permit)
        except Exception as e:
            logger.error(f"Error checking validator permit: {e}")
            return False

    async def _register_new_validator(self, keypair: Any) -> bool:
        """Register new validator on network."""
        try:
            # Get registration cost
            registration_cost = await self.network_manager.substrate.query(
                "SubtensorModule",
                "ValidatorRegistrationCost",
                [self.netuid]
            ).value

            if registration_cost is None:
                logger.error("Could not get registration cost")
                return False

            # Register validator
            success = await self.network_manager.substrate.submit_extrinsic(
                call_module="SubtensorModule",
                call_function="register",
                call_params={
                    "netuid": self.netuid,
                    "block_number": self.network_manager.substrate.get_block_number(),
                    "nonce": registration_cost,
                    "work": None,  # No POW required for validators
                    "hotkey": keypair.ss58_address,
                    "coldkey": keypair.ss58_address
                },
                keypair=keypair,
                wait_for_inclusion=True
            )

            if success:
                self.registration_block = self.network_manager.substrate.get_block_number()
                self.last_update = int(time.time())
                await self.save_state()
                return True
                
            return False

        except Exception as e:
            logger.error(f"Error registering new validator: {e}")
            return False 