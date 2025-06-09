import time
from typing import Optional
from fiber.chain.interface import get_substrate
from substrateinterface import SubstrateInterface
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class SubstrateConnectionManager:
    """
    Manages substrate connections to prevent memory leaks from frequent reconnections.
    Reuses connections when possible and properly cleans up old ones.
    """
    
    def __init__(self, subtensor_network: str, chain_endpoint: str):
        self.subtensor_network = subtensor_network
        self.chain_endpoint = chain_endpoint
        self._connection: Optional[SubstrateInterface] = None
        self._last_used = 0
        self._max_age = 3600  # 1 hour before connection refresh
        self._connection_count = 0
        logger.info(f"SubstrateConnectionManager initialized for network: {subtensor_network}")
    
    def get_connection(self) -> SubstrateInterface:
        """Get a substrate connection, reusing existing one if recent enough."""
        now = time.time()
        
        # Check if we need a new connection
        needs_new_connection = (
            self._connection is None or 
            (now - self._last_used > self._max_age)
        )
        
        if needs_new_connection:
            # Clean up old connection first
            if self._connection is not None:
                try:
                    logger.debug(f"Cleaning up old substrate connection (age: {now - self._last_used:.1f}s)")
                    self._connection.close()
                except Exception as e:
                    logger.debug(f"Error cleaning up old substrate connection: {e}")
                finally:
                    self._connection = None
            
            # Create new connection
            try:
                logger.debug(f"Creating new substrate connection #{self._connection_count + 1}")
                self._connection = get_substrate(
                    subtensor_network=self.subtensor_network,
                    subtensor_address=self.chain_endpoint
                )
                self._connection_count += 1
                logger.info(f"Successfully created substrate connection #{self._connection_count}")
            except Exception as e:
                logger.error(f"Failed to create substrate connection: {e}")
                raise
        
        self._last_used = now
        return self._connection
    
    def force_reconnect(self) -> SubstrateInterface:
        """Force a new connection (useful for error recovery)."""
        logger.info("Forcing substrate reconnection...")
        # Clean up current connection
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception as e:
                logger.debug(f"Error during forced cleanup: {e}")
            finally:
                self._connection = None
        
        # Reset age to force new connection
        self._last_used = 0
        return self.get_connection()
    
    def cleanup(self):
        """Clean up the connection manager resources."""
        if self._connection is not None:
            try:
                logger.info("Cleaning up substrate connection manager...")
                self._connection.close()
            except Exception as e:
                logger.debug(f"Error during connection manager cleanup: {e}")
            finally:
                self._connection = None
    
    @property
    def connection_age(self) -> float:
        """Get the age of the current connection in seconds."""
        if self._connection is None:
            return 0
        return time.time() - self._last_used
    
    @property 
    def connection_count(self) -> int:
        """Get the total number of connections created."""
        return self._connection_count
    
    def get_stats(self) -> dict:
        """Get connection manager statistics."""
        return {
            "connection_count": self._connection_count,
            "connection_age": self.connection_age,
            "has_connection": self._connection is not None,
            "last_used": self._last_used,
            "max_age": self._max_age
        } 