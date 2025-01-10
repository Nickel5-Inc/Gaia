import asyncio
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from fiber.logging_utils import get_logger
from fiber.chain import interface
from substrateinterface import SubstrateInterface
from gaia.validator.utils.task_decorator import task_step
from gaia.validator.core.constants import (
    NETWORK_OP_TIMEOUT,
    DEFAULT_CHAIN_ENDPOINT,
    DEFAULT_NETWORK
)

logger = get_logger(__name__)

class NetworkManager:
    """
    Manages substrate/chain interactions.
    
    This class handles:
    1. Chain connection management
    2. Network operation retries
    3. Block monitoring
    4. Transaction submission
    5. Network state queries
    """
    
    def __init__(
        self,
        chain_endpoint: str = DEFAULT_CHAIN_ENDPOINT,
        network: str = DEFAULT_NETWORK,
        max_retries: int = 3
    ):
        self.chain_endpoint = chain_endpoint
        self.network = network
        self.max_retries = max_retries
        
        # Initialize substrate connection
        self.substrate: Optional[SubstrateInterface] = None
        self.last_block = 0
        self.last_connection = None
        self.connection_errors = 0
        self.keypair = None
        self._is_connected = False
        
    @task_step(
        name="connect_substrate",
        description="Connect to substrate network",
        timeout_seconds=NETWORK_OP_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=5
    )
    async def connect(self) -> bool:
        """
        Connect to the substrate network.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.substrate = SubstrateInterface(url=self.chain_endpoint)
            if not self.substrate:
                logger.error("Failed to initialize substrate connection")
                self._is_connected = False
                return False
                
            # Test connection by getting block
            block = self.substrate.get_block()
            if not block or "header" not in block:
                logger.error("Invalid block data from new connection")
                self._is_connected = False
                return False
                
            self._is_connected = True
            self.last_connection = datetime.now(timezone.utc)
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to substrate: {e}")
            self._is_connected = False
            return False

    @task_step(
        name="check_connection",
        description="Check substrate connection health",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def check_connection(self) -> bool:
        """
        Check if substrate connection is healthy.
        
        Returns:
            bool: True if connection healthy, False otherwise
        """
        try:
            if not self.substrate:
                return await self.connect()
                
            # Try to get current block
            block = self.substrate.get_block()
            if not block or "header" not in block or "number" not in block["header"]:
                logger.error("Invalid block data during health check")
                self.connection_errors += 1
                return await self.connect()
                
            self.last_block = block["header"]["number"]
            return True
            
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            self.connection_errors += 1
            return False

    @task_step(
        name="execute_substrate_call",
        description="Execute substrate call with retries",
        timeout_seconds=NETWORK_OP_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=5
    )
    async def execute_substrate_call(
        self,
        call_name: str,
        *args,
        **kwargs
    ) -> Optional[Any]:
        """
        Execute a substrate call with retries.
        
        Args:
            call_name: Name of substrate call to execute
            *args: Positional arguments for call
            **kwargs: Keyword arguments for call
            
        Returns:
            Optional[Any]: Call result if successful, None otherwise
        """
        for attempt in range(self.max_retries):
            try:
                if not await self.check_connection():
                    logger.error("No substrate connection for call execution")
                    continue
                    
                if not self.substrate:
                    logger.error("Substrate not initialized")
                    continue
                    
                # Get the substrate call method
                call_method = getattr(self.substrate, call_name, None)
                if not call_method:
                    logger.error(f"Invalid substrate call: {call_name}")
                    return None
                    
                # Execute the call
                result = call_method(*args, **kwargs)
                return result
                
            except Exception as e:
                logger.error(
                    f"Substrate call failed (attempt {attempt + 1}/{self.max_retries}): {e}"
                )
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(5)  # Wait before retry
                    
        return None

    @task_step(
        name="get_network_state",
        description="Get current network state",
        timeout_seconds=30,
        max_retries=2,
        retry_delay_seconds=5
    )
    async def get_network_state(self) -> Dict[str, Any]:
        """
        Get current network state information.
        
        Returns:
            Dict[str, Any]: Network state info
        """
        try:
            if not await self.check_connection():
                return {
                    "current_block": self.last_block,
                    "network": self.network,
                    "endpoint": self.chain_endpoint,
                    "connection_errors": self.connection_errors,
                    "last_connection": self.last_connection,
                    "is_connected": False
                }
                
            if not self.substrate:
                logger.error("Substrate not initialized")
                return {}
                
            state = {
                "current_block": self.last_block,
                "network": self.network,
                "endpoint": self.chain_endpoint,
                "connection_errors": self.connection_errors,
                "last_connection": self.last_connection,
                "is_connected": self._is_connected
            }
            
            # Add chain-specific info
            try:
                if hasattr(self.substrate, "get_runtime_version"):
                    runtime_version = self.substrate.get_runtime_version()  # type: ignore
                    state["runtime_version"] = runtime_version
            except Exception as e:
                logger.error(f"Failed to get runtime version: {e}")
                
            return state
            
        except Exception as e:
            logger.error(f"Failed to get network state: {e}")
            return {}

    @task_step(
        name="submit_transaction",
        description="Submit transaction to network",
        timeout_seconds=NETWORK_OP_TIMEOUT,
        max_retries=3,
        retry_delay_seconds=5
    )
    async def submit_transaction(
        self,
        call_module: str,
        call_function: str,
        params: Dict[str, Any],
        keypair: Any
    ) -> Optional[str]:
        """
        Submit a transaction to the network.
        
        Args:
            call_module: Substrate module name
            call_function: Function name to call
            params: Transaction parameters
            keypair: Keypair for signing
            
        Returns:
            Optional[str]: Transaction hash if successful
        """
        try:
            if not await self.check_connection():
                logger.error("No substrate connection for transaction")
                return None
                
            if not self.substrate:
                logger.error("Substrate not initialized")
                return None
                
            # Verify substrate has required methods
            required_methods = ["compose_call", "create_signed_extrinsic", "submit_extrinsic"]
            for method in required_methods:
                if not hasattr(self.substrate, method):
                    logger.error(f"Substrate missing required method: {method}")
                    return None
                
            # Create call
            call = self.substrate.compose_call(
                call_module=call_module,
                call_function=call_function,
                call_params=params
            )
            
            if not call:
                logger.error("Failed to compose call")
                return None
                
            # Create and sign transaction
            extrinsic = self.substrate.create_signed_extrinsic(
                call=call,
                keypair=keypair
            )
            
            if not extrinsic:
                logger.error("Failed to create signed extrinsic")
                return None
                
            # Submit transaction
            receipt = self.substrate.submit_extrinsic(
                extrinsic,
                wait_for_inclusion=True
            )
            
            if not receipt or not hasattr(receipt, "extrinsic_hash"):
                logger.error("Invalid transaction receipt")
                return None
                
            logger.info(
                f"Transaction submitted: module={call_module}, "
                f"function={call_function}, hash={receipt.extrinsic_hash}"
            )
            
            return receipt.extrinsic_hash
            
        except Exception as e:
            logger.error(f"Transaction submission failed: {e}")
            return None

    async def close(self) -> None:
        """Close the network connection."""
        try:
            if self.substrate:
                # Clean up substrate connection
                self.substrate = None
                self._is_connected = False
                logger.info("Network connection closed")
        except Exception as e:
            logger.error(f"Error closing network connection: {e}") 

    def is_connected(self) -> bool:
        """
        Check if network is currently connected.
        
        Returns:
            bool: True if connected, False otherwise
        """
        return self._is_connected 