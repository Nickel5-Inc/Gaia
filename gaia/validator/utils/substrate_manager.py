import time
import gc
from typing import Optional, Dict, Tuple
from fiber.chain.interface import get_substrate
from substrateinterface import SubstrateInterface
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class SubstrateConnectionManager:
    """
    Manages substrate connections to prevent memory leaks from frequent reconnections.
    Reuses connections when possible and properly cleans up old ones.
    Implements singleton pattern to ensure only one manager per network/endpoint combination.
    Enhanced with aggressive memory management to handle scalecodec memory leaks.
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
        self._connection: Optional[SubstrateInterface] = None
        self._last_used = 0
        self._max_age = 600  # Further reduced to 10 minutes for more frequent refreshes
        self._connection_count = 0
        self._query_count = 0  # Track number of queries to detect heavy usage
        self._last_cleanup = time.time()
        self._cleanup_interval = 180  # Force cleanup every 3 minutes (reduced from 5)
        self._initialized = True
        logger.info(f"SubstrateConnectionManager initialized for network: {subtensor_network}")
    
    def get_connection(self) -> SubstrateInterface:
        """Get a substrate connection, reusing existing one if recent enough with enhanced cleanup."""
        now = time.time()
        
        # Check if we need periodic cleanup or a new connection
        needs_cleanup = (now - self._last_cleanup > self._cleanup_interval)
        needs_new_connection = (
            self._connection is None or 
            (now - self._last_used > self._max_age) or
            self._query_count > 150  # Further reduced from 250 for more aggressive refreshes
        )
        
        if needs_cleanup:
            self._force_garbage_collection()
            
        if needs_new_connection:
            # Clean up old connection first with enhanced cleanup
            if self._connection is not None:
                try:
                    logger.debug(f"Cleaning up old substrate connection (age: {now - self._last_used:.1f}s, queries: {self._query_count})")
                    
                    # More aggressive cleanup for scalecodec objects
                    if hasattr(self._connection, 'websocket') and self._connection.websocket:
                        try:
                            self._connection.websocket.close()
                        except Exception:
                            pass
                    
                    # Clear any cached data in the connection
                    if hasattr(self._connection, '_request_cache'):
                        self._connection._request_cache.clear()
                    
                    # Clear substrate interface internal caches if they exist
                    if hasattr(self._connection, 'metadata_cache'):
                        self._connection.metadata_cache.clear()
                    if hasattr(self._connection, 'runtime_configuration'):
                        self._connection.runtime_configuration = None
                    
                    # Close the main connection
                    self._connection.close()
                    
                except Exception as e:
                    logger.debug(f"Error cleaning up old substrate connection: {e}")
                finally:
                    self._connection = None
                    self._query_count = 0
                    # Force aggressive garbage collection after connection cleanup
                    collected = gc.collect()
                    logger.debug(f"Post-connection cleanup GC freed {collected} objects")
            
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
        self._query_count += 1
        return self._connection
    
    def _force_garbage_collection(self):
        """Force garbage collection to clean up scalecodec objects with enhanced cleanup."""
        try:
            # More aggressive cleanup - import gc locally to ensure it's available
            import gc
            
            # Clear any module-level caches that might accumulate scalecodec objects
            try:
                # Clear substrate interface module caches if accessible
                import sys
                for module_name in list(sys.modules.keys()):
                    if 'scalecodec' in module_name or 'substrate' in module_name:
                        module = sys.modules.get(module_name)
                        if hasattr(module, '__dict__'):
                            # Clear any cached objects in the module
                            for attr_name in list(module.__dict__.keys()):
                                if attr_name.startswith('_cache') or attr_name.endswith('_cache'):
                                    try:
                                        cache_obj = getattr(module, attr_name)
                                        if hasattr(cache_obj, 'clear'):
                                            cache_obj.clear()
                                    except Exception:
                                        pass
            except Exception:
                pass  # Fail silently for module cache cleanup
            
            # Force multiple garbage collection passes for more thorough cleanup
            collected_total = 0
            for _ in range(3):  # Multiple passes can catch circular references
                collected = gc.collect()
                collected_total += collected
                if collected == 0:
                    break  # No more objects to collect
            
            self._last_cleanup = time.time()
            if collected_total > 0:
                logger.debug(f"Enhanced garbage collection freed {collected_total} objects")
        except Exception as e:
            logger.debug(f"Error during enhanced garbage collection: {e}")
    
    def force_reconnect(self) -> SubstrateInterface:
        """Force a new connection (useful for error recovery) with enhanced cleanup."""
        logger.info("Forcing substrate reconnection...")
        # Clean up current connection
        if self._connection is not None:
            try:
                self._connection.close()
                if hasattr(self._connection, 'websocket') and self._connection.websocket:
                    self._connection.websocket.close()
            except Exception as e:
                logger.debug(f"Error during forced cleanup: {e}")
            finally:
                self._connection = None
                self._query_count = 0
        
        # Force garbage collection
        self._force_garbage_collection()
        
        # Reset age to force new connection
        self._last_used = 0
        return self.get_connection()
    
    def cleanup(self):
        """Clean up the connection manager resources with enhanced cleanup."""
        if self._connection is not None:
            try:
                logger.info("Cleaning up substrate connection manager...")
                
                # Enhanced cleanup for scalecodec objects
                try:
                    # Clear substrate interface caches before closing
                    if hasattr(self._connection, 'metadata_cache'):
                        self._connection.metadata_cache.clear()
                    if hasattr(self._connection, 'runtime_configuration'):
                        self._connection.runtime_configuration = None
                    if hasattr(self._connection, '_request_cache'):
                        self._connection._request_cache.clear()
                    
                    # Close websocket first
                    if hasattr(self._connection, 'websocket') and self._connection.websocket:
                        self._connection.websocket.close()
                    
                    # Close main connection
                    self._connection.close()
                    
                    # Clear any scalecodec module-level caches
                    import sys
                    for module_name in list(sys.modules.keys()):
                        if 'scalecodec' in module_name.lower():
                            module = sys.modules.get(module_name)
                            if hasattr(module, '__dict__'):
                                for attr_name in list(module.__dict__.keys()):
                                    if 'cache' in attr_name.lower() or 'registry' in attr_name.lower():
                                        try:
                                            cache_obj = getattr(module, attr_name)
                                            if hasattr(cache_obj, 'clear'):
                                                cache_obj.clear()
                                            elif isinstance(cache_obj, dict):
                                                cache_obj.clear()
                                        except Exception:
                                            pass
                            
                except Exception as e:
                    logger.debug(f"Error during enhanced substrate cleanup: {e}")
                finally:
                    self._connection = None
                    self._query_count = 0
                    
            except Exception as e:
                logger.debug(f"Error during connection manager cleanup: {e}")
            finally:
                self._connection = None
                self._query_count = 0
        
        # Force final garbage collection with focus on scalecodec objects
        self._force_garbage_collection()
    
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
    
    @property
    def query_count(self) -> int:
        """Get the total number of queries made on current connection."""
        return self._query_count
    
    def get_stats(self) -> dict:
        """Get connection manager statistics."""
        return {
            "connection_count": self._connection_count,
            "query_count": self._query_count,
            "connection_age": self.connection_age,
            "has_connection": self._connection is not None,
            "last_used": self._last_used,
            "max_age": self._max_age,
            "last_cleanup": self._last_cleanup,
            "time_since_cleanup": time.time() - self._last_cleanup
        }
    
    @classmethod
    def get_all_instances(cls) -> Dict[Tuple[str, str], 'SubstrateConnectionManager']:
        """Get all active singleton instances (useful for debugging/monitoring)."""
        return cls._instances.copy()
    
    @classmethod
    def cleanup_all(cls):
        """Clean up all singleton instances (useful for shutdown)."""
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