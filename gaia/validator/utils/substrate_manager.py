import time
import gc
import threading
from typing import Optional
from contextlib import contextmanager
from fiber.chain.interface import get_substrate
from substrateinterface import SubstrateInterface
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class SubstrateManager:
    """
    Substrate connection manager that prevents memory leaks with always-fresh connections.
    
    Key principles:
    1. ALWAYS create fresh connections (dispose old, create new) to prevent memory leaks
    2. Manager instance stays stable - only recreated when network changes
    3. Thorough cleanup of scalecodec caches when disposing connections  
    4. Thread-safe operations
    5. Handles endpoint changes gracefully without manager recreation
    
    This manager prioritizes memory leak prevention over connection reuse.
    """
    
    def __init__(self, subtensor_network: str, chain_endpoint: str):
        self.subtensor_network = subtensor_network
        self.chain_endpoint = chain_endpoint
        self._current_connection: Optional[SubstrateInterface] = None
        self._connection_created_at: Optional[float] = None
        self._lock = threading.Lock()
        self._connection_count = 0
        
        logger.info(f"SubstrateManager initialized for {subtensor_network}@{chain_endpoint}")
    
    def get_fresh_connection(self) -> SubstrateInterface:
        """
        Get a fresh substrate connection. Always disposes old connection and creates new one.
        This prevents connection stacking and memory accumulation.
        """
        with self._lock:
            # ALWAYS dispose existing connection first - no reuse, always fresh
            if self._current_connection is not None:
                logger.debug("🧹 Disposing old connection to create fresh one")
                self._dispose_connection_thoroughly(self._current_connection)
                self._current_connection = None
            
            # Create fresh connection
            try:
                logger.debug(f"Creating fresh substrate connection #{self._connection_count + 1}")
                
                # Handle empty/None chain_endpoint properly
                if self.chain_endpoint and self.chain_endpoint.strip():
                    self._current_connection = get_substrate(
                        subtensor_network=self.subtensor_network,
                        subtensor_address=self.chain_endpoint
                    )
                else:
                    # Use default endpoint when chain_endpoint is empty/None
                    self._current_connection = get_substrate(
                        subtensor_network=self.subtensor_network
                    )
                
                self._connection_count += 1
                self._connection_created_at = time.time()
                logger.debug(f"✅ Fresh substrate connection #{self._connection_count} created")
                return self._current_connection
                
            except Exception as e:
                logger.error(f"❌ Failed to create fresh substrate connection: {e}")
                raise
    

    
    def _dispose_connection_thoroughly(self, connection: SubstrateInterface):
        """
        Thoroughly dispose of a substrate connection with full cleanup.
        This is the key to preventing scalecodec memory leaks.
        """
        try:
            logger.debug("🧹 Starting thorough substrate connection disposal...")
            
            # 1. Close websocket first (if exists)
            try:
                if hasattr(connection, 'websocket') and connection.websocket:
                    connection.websocket.close()
                    logger.debug("   ✓ Websocket closed")
            except Exception as e:
                logger.debug(f"   ⚠ Error closing websocket: {e}")
            
            # 2. Clear connection-level caches
            try:
                cache_attrs = ['metadata_cache', '_request_cache', 'runtime_configuration', 
                              '_metadata', '_runtime_info', '_type_registry']
                for attr in cache_attrs:
                    if hasattr(connection, attr):
                        cache_obj = getattr(connection, attr)
                        if cache_obj is not None:
                            if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                cache_obj.clear()
                            elif isinstance(cache_obj, dict):
                                cache_obj.clear()
                            setattr(connection, attr, None)
                logger.debug("   ✓ Connection caches cleared")
            except Exception as e:
                logger.debug(f"   ⚠ Error clearing connection caches: {e}")
            
            # 3. Close main connection
            try:
                connection.close()
                logger.debug("   ✓ Main connection closed")
            except Exception as e:
                logger.debug(f"   ⚠ Error closing main connection: {e}")
            
            # 4. Scalecodec module cleanup
            try:
                self._clear_scalecodec_caches()
                logger.debug("   ✓ Scalecodec caches cleared")
            except Exception as e:
                logger.debug(f"   ⚠ Error clearing scalecodec caches: {e}")
            
            # 5. Force garbage collection
            try:
                collected = gc.collect()
                logger.debug(f"   ✓ Garbage collection freed {collected} objects")
            except Exception as e:
                logger.debug(f"   ⚠ Error during garbage collection: {e}")
            
            logger.debug("🧹 Thorough substrate disposal completed")
            
        except Exception as e:
            logger.debug(f"❌ Error during thorough disposal: {e}")
    
    def _clear_scalecodec_caches(self):
        """
        Clear scalecodec module caches thoroughly.
        This is critical for preventing memory accumulation.
        """
        import sys
        
        cleared_count = 0
        scalecodec_patterns = [
            'scalecodec', 'substrate', 'scale_info', 'metadata', 
            'substrateinterface', 'scale_codec'
        ]
        
        for module_name in list(sys.modules.keys()):
            if any(pattern in module_name.lower() for pattern in scalecodec_patterns):
                module = sys.modules.get(module_name)
                if module and hasattr(module, '__dict__'):
                    # Clear all cache-like attributes
                    for attr_name in list(module.__dict__.keys()):
                        if any(cache_word in attr_name.lower() for cache_word in 
                               ['cache', 'registry', '_cached', '_memo', '_lru', '_store', 
                                '_type_registry', '_metadata', '_runtime', '_version']):
                            try:
                                cache_obj = getattr(module, attr_name)
                                if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                    cache_obj.clear()
                                    cleared_count += 1
                                elif isinstance(cache_obj, (dict, list, set)):
                                    cache_obj.clear()
                                    cleared_count += 1
                            except Exception:
                                pass
        
        if cleared_count > 0:
            logger.debug(f"   Cleared {cleared_count} scalecodec cache objects")
    
    def force_cleanup(self):
        """
        Force cleanup of current connection and caches.
        Useful for emergency memory cleanup.
        """
        with self._lock:
            if self._current_connection is not None:
                logger.info("🚨 Force cleanup of substrate connection")
                self._dispose_connection_thoroughly(self._current_connection)
                self._current_connection = None
            else:
                # Even if no connection, clear scalecodec caches
                logger.debug("🧹 Force cleanup of scalecodec caches (no active connection)")
                self._clear_scalecodec_caches()
                gc.collect()
    
    @contextmanager
    def fresh_connection(self):
        """
        Context manager for automatic connection cleanup.
        
        Usage:
            with manager.fresh_connection() as substrate:
                # Use substrate connection
                result = substrate.query(...)
            # Connection automatically disposed here
        """
        connection = None
        try:
            connection = self.get_fresh_connection()
            yield connection
        finally:
            # Always cleanup, even on exceptions
            if connection is not None:
                with self._lock:
                    if self._current_connection == connection:
                        self._dispose_connection_thoroughly(connection)
                        self._current_connection = None
    
    def get_stats(self) -> dict:
        """Get manager statistics."""
        with self._lock:
            connection_age = None
            
            if self._current_connection is not None and self._connection_created_at is not None:
                connection_age = time.time() - self._connection_created_at
            
            return {
                "connection_count": self._connection_count,
                "has_active_connection": self._current_connection is not None,
                "connection_age_seconds": connection_age,
                "network": self.subtensor_network,
                "endpoint": self.chain_endpoint
            }
    
    def shutdown(self):
        """Complete shutdown with cleanup."""
        logger.info("🛑 Shutting down SubstrateManager...")
        with self._lock:
            if self._current_connection is not None:
                self._dispose_connection_thoroughly(self._current_connection)
                self._current_connection = None
        logger.info("✅ SubstrateManager shutdown complete")


# Global manager instance (singleton per process)
_global_manager: Optional[SubstrateManager] = None
_global_lock = threading.Lock()


def get_substrate_manager(subtensor_network: str, chain_endpoint: str) -> SubstrateManager:
    """
    Get the global substrate manager (singleton).
    Creates one only if it doesn't exist or if the network actually changed.
    The manager handles different endpoints gracefully without recreation.
    """
    global _global_manager
    
    with _global_lock:
        # Only create new manager if none exists or network truly changed
        # Don't recreate for endpoint differences - the manager can handle that
        needs_new_manager = (
            _global_manager is None or 
            _global_manager.subtensor_network != subtensor_network
        )
        
        if needs_new_manager:
            # Cleanup old manager if exists
            if _global_manager is not None:
                logger.info(f"Network changed: {_global_manager.subtensor_network} → {subtensor_network}, recreating manager")
                _global_manager.shutdown()
            
            # Create new manager - normalize endpoint
            normalized_endpoint = chain_endpoint.strip() if chain_endpoint else ""
            _global_manager = SubstrateManager(subtensor_network, normalized_endpoint)
            logger.info(f"🔄 Created new global SubstrateManager for {subtensor_network}")
        else:
            # Just update endpoint if it's different, but don't recreate manager
            normalized_endpoint = chain_endpoint.strip() if chain_endpoint else ""
            if _global_manager.chain_endpoint != normalized_endpoint:
                logger.debug(f"Updating endpoint: {_global_manager.chain_endpoint} → {normalized_endpoint}")
                _global_manager.chain_endpoint = normalized_endpoint
                # Force disposal of current connection since endpoint changed
                _global_manager.force_cleanup()
        
        return _global_manager


def cleanup_global_substrate_manager():
    """Cleanup the global substrate manager."""
    global _global_manager
    
    with _global_lock:
        if _global_manager is not None:
            _global_manager.shutdown()
            _global_manager = None
            logger.info("🧹 Global substrate manager cleaned up")


def get_fresh_substrate_connection(subtensor_network: str, chain_endpoint: str) -> SubstrateInterface:
    """
    Convenience function to get a fresh substrate connection.
    Always creates a new connection and disposes of any previous one.
    """
    manager = get_substrate_manager(subtensor_network, chain_endpoint)
    return manager.get_fresh_connection()


def force_substrate_cleanup(subtensor_network: str, chain_endpoint: str):
    """
    Force cleanup of substrate connections and caches.
    Useful for emergency memory management.
    """
    manager = get_substrate_manager(subtensor_network, chain_endpoint)
    manager.force_cleanup()


def get_substrate_manager_stats(subtensor_network: str, chain_endpoint: str) -> dict:
    """
    Get substrate manager statistics without creating new connections.
    """
    global _global_manager
    
    with _global_lock:
        if _global_manager is None:
            return {"status": "no_manager", "has_manager": False}
        
        try:
            stats = _global_manager.get_stats()
            stats["has_manager"] = True
            stats["status"] = "active"
            return stats
        except Exception as e:
            return {"status": "error", "has_manager": True, "error": str(e)} 