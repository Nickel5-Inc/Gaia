import time
import gc
from typing import Optional, Dict, Tuple
from fiber.chain.interface import get_substrate
from substrateinterface import SubstrateInterface
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class SubstrateConnectionManager:
    """
    Manages substrate connections to prevent memory leaks while ensuring fresh chain state.
    Always creates fresh connections to avoid caching stale state, but manages cleanup
    to prevent connection stacking and memory leaks.
    Implements singleton pattern to ensure only one manager per network/endpoint combination.
    """
    
    _instances: Dict[Tuple[str, str], 'SubstrateConnectionManager'] = {}
    _lock = None
    
    def __new__(cls, subtensor_network: str, chain_endpoint: str):
        """Ensure singleton behavior per network/endpoint combination."""
        import threading
        
        # Initialize lock if not exists (thread-safe)
        if cls._lock is None:
            cls._lock = threading.Lock()
        
        key = (subtensor_network, chain_endpoint)
        
        with cls._lock:
            if key not in cls._instances:
                instance = super().__new__(cls)
                cls._instances[key] = instance
                logger.info(f"Created new SubstrateConnectionManager singleton for {subtensor_network}@{chain_endpoint}")
            else:
                logger.debug(f"Reusing existing SubstrateConnectionManager for {subtensor_network}@{chain_endpoint}")
            
            return cls._instances[key]
    
    def __init__(self, subtensor_network: str, chain_endpoint: str):
        # Prevent re-initialization of singleton instances
        if hasattr(self, '_initialized'):
            return
            
        self.subtensor_network = subtensor_network
        self.chain_endpoint = chain_endpoint
        self._last_connection: Optional[SubstrateInterface] = None
        self._connection_count = 0
        self._last_cleanup = time.time()
        self._cleanup_interval = 60  # Cleanup every 1 minute for aggressive memory management
        self._initialized = True
        logger.info(f"SubstrateConnectionManager initialized for network: {subtensor_network} (fresh state mode)")
    
    def get_fresh_connection(self) -> SubstrateInterface:
        """
        Always creates a fresh substrate connection to ensure latest chain state.
        Properly cleans up any previous connection to prevent memory leaks.
        """
        now = time.time()
        
        # Always clean up the previous connection first
        if self._last_connection is not None:
            try:
                logger.debug(f"Cleaning up previous substrate connection #{self._connection_count}")
                self._cleanup_connection(self._last_connection)
            except Exception as e:
                logger.debug(f"Error cleaning up previous substrate connection: {e}")
            finally:
                self._last_connection = None
        
        # Perform periodic aggressive cleanup
        if now - self._last_cleanup > self._cleanup_interval:
            self._force_garbage_collection()
            self._last_cleanup = now
        
        # Always create a fresh connection
        try:
            self._connection_count += 1
            logger.debug(f"Creating fresh substrate connection #{self._connection_count}")
            new_connection = get_substrate(
                subtensor_network=self.subtensor_network,
                subtensor_address=self.chain_endpoint
            )
            self._last_connection = new_connection
            logger.info(f"✅ Created fresh substrate connection #{self._connection_count} with latest chain state")
            return new_connection
        except Exception as e:
            logger.error(f"Failed to create fresh substrate connection: {e}")
            raise
    
    def get_connection(self) -> SubstrateInterface:
        """Alias for get_fresh_connection to maintain backward compatibility."""
        return self.get_fresh_connection()
    
    def _cleanup_connection(self, connection: SubstrateInterface):
        """Enhanced cleanup for a specific substrate connection."""
        try:
            # Clear substrate interface caches before closing
            if hasattr(connection, 'metadata_cache'):
                connection.metadata_cache.clear()
            if hasattr(connection, 'runtime_configuration'):
                connection.runtime_configuration = None
            if hasattr(connection, '_request_cache'):
                connection._request_cache.clear()
            
            # Close websocket first
            if hasattr(connection, 'websocket') and connection.websocket:
                try:
                    connection.websocket.close()
                except Exception:
                    pass
            
            # Close main connection
            connection.close()
            
        except Exception as e:
            logger.debug(f"Error during connection cleanup: {e}")
    
    def _force_garbage_collection(self):
        """Aggressive garbage collection to clean up scalecodec objects and prevent memory leaks."""
        try:
            import gc
            import sys
            
            # Clear scalecodec and substrate module caches
            cleared_caches = 0
            for module_name in list(sys.modules.keys()):
                if any(pattern in module_name.lower() for pattern in ['scalecodec', 'substrate', 'metadata']):
                    module = sys.modules.get(module_name)
                    if hasattr(module, '__dict__'):
                        for attr_name in list(module.__dict__.keys()):
                            if any(cache_pattern in attr_name.lower() for cache_pattern in 
                                   ['cache', 'registry', '_cached', '_memo', '_lru']):
                                try:
                                    cache_obj = getattr(module, attr_name)
                                    if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                        cache_obj.clear()
                                        cleared_caches += 1
                                    elif isinstance(cache_obj, (dict, list, set)):
                                        cache_obj.clear()
                                        cleared_caches += 1
                                except Exception:
                                    pass
            
            # Force multiple garbage collection passes
            collected_total = 0
            for _ in range(3):
                collected = gc.collect()
                collected_total += collected
                if collected == 0:
                    break
            
            if cleared_caches > 0 or collected_total > 0:
                logger.debug(f"Aggressive cleanup: cleared {cleared_caches} caches, GC freed {collected_total} objects")
                
        except Exception as e:
            logger.debug(f"Error during aggressive garbage collection: {e}")
    
    def force_reconnect(self) -> SubstrateInterface:
        """
        Force a new connection (useful for error recovery).
        Same as get_fresh_connection since we always create fresh connections.
        """
        logger.info("Forcing fresh substrate reconnection...")
        return self.get_fresh_connection()
    
    def cleanup(self):
        """Clean up the connection manager resources."""
        if self._last_connection is not None:
            try:
                logger.info("Cleaning up substrate connection manager...")
                self._cleanup_connection(self._last_connection)
            except Exception as e:
                logger.debug(f"Error during connection manager cleanup: {e}")
            finally:
                self._last_connection = None
        
        # Force final garbage collection
        self._force_garbage_collection()
    
    @property
    def connection_count(self) -> int:
        """Get the total number of connections created."""
        return self._connection_count
    
    @property
    def has_active_connection(self) -> bool:
        """Check if there's currently an active connection."""
        return self._last_connection is not None
    
    def get_stats(self) -> dict:
        """Get connection manager statistics."""
        return {
            "connection_count": self._connection_count,
            "has_active_connection": self.has_active_connection,
            "last_cleanup": self._last_cleanup,
            "time_since_cleanup": time.time() - self._last_cleanup,
            "mode": "fresh_state_always",
            "cleanup_interval": self._cleanup_interval
        }
    
    @classmethod
    def get_all_instances(cls) -> Dict[Tuple[str, str], 'SubstrateConnectionManager']:
        """Get all active singleton instances (useful for debugging/monitoring)."""
        return cls._instances.copy()
    
    @classmethod
    def cleanup_all(cls):
        """Clean up all singleton instances (useful for shutdown)."""
        if cls._lock is None:
            return
            
        with cls._lock:
            for key, instance in cls._instances.items():
                try:
                    instance.cleanup()
                    logger.info(f"Cleaned up SubstrateConnectionManager for {key[0]}@{key[1]}")
                except Exception as e:
                    logger.error(f"Error cleaning up instance {key}: {e}")
            cls._instances.clear()
            logger.info("All SubstrateConnectionManager instances cleaned up")
            # Final garbage collection after all cleanup
            gc.collect() 