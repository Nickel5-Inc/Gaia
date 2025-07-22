"""
Global Memory Management Coordinator

Coordinates memory cleanup across all threads, background workers, and processes
to prevent sneaky cache growth in areas not covered by the main validator cleanup.
"""

import threading
import weakref
import gc
import time
from typing import Callable, Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class GlobalMemoryManager:
    """
    Thread-safe global coordinator for memory cleanup across all application threads.
    
    Allows threads/workers to register cleanup callbacks that get triggered during
    coordinated memory cleanup operations.
    """
    
    def __init__(self):
        self._lock = threading.RLock()
        self._cleanup_callbacks: Dict[str, Callable[[], None]] = {}
        self._thread_registrations: Dict[threading.Thread, Set[str]] = {}
        self._last_cleanup_time = 0
        self._cleanup_stats = {
            "total_cleanups": 0,
            "last_cleanup_duration": 0,
            "registered_callbacks": 0,
            "active_threads": 0
        }
    
    def register_cleanup_callback(self, name: str, callback: Callable[[], None], 
                                 thread: Optional[threading.Thread] = None) -> None:
        """
        Register a cleanup callback that will be called during coordinated cleanup.
        
        Args:
            name: Unique name for this cleanup callback
            callback: Function to call during cleanup (should be thread-safe)
            thread: Optional thread this callback belongs to (for auto-cleanup)
        """
        with self._lock:
            if thread is None:
                thread = threading.current_thread()
            
            # Use weak reference to avoid keeping thread alive
            if thread not in self._thread_registrations:
                self._thread_registrations[thread] = set()
            
            self._cleanup_callbacks[name] = callback
            self._thread_registrations[thread].add(name)
            self._cleanup_stats["registered_callbacks"] = len(self._cleanup_callbacks)
            
            logger.debug(f"Registered cleanup callback '{name}' for thread {thread.name}")
    
    def unregister_cleanup_callback(self, name: str) -> bool:
        """Remove a cleanup callback by name."""
        with self._lock:
            if name in self._cleanup_callbacks:
                del self._cleanup_callbacks[name]
                
                # Remove from thread registrations
                for thread_callbacks in self._thread_registrations.values():
                    thread_callbacks.discard(name)
                
                self._cleanup_stats["registered_callbacks"] = len(self._cleanup_callbacks)
                logger.debug(f"Unregistered cleanup callback '{name}'")
                return True
            return False
    
    def trigger_coordinated_cleanup(self, context: str = "global") -> Dict[str, any]:
        """
        Trigger cleanup across all registered callbacks.
        
        Args:
            context: Context string for logging (e.g., "periodic", "emergency")
            
        Returns:
            Dictionary with cleanup statistics
        """
        start_time = time.time()
        
        with self._lock:
            # Clean up dead thread registrations first
            self._cleanup_dead_threads()
            
            active_callbacks = list(self._cleanup_callbacks.items())
            self._cleanup_stats["active_threads"] = len(self._thread_registrations)
        
        stats = {
            "context": context,
            "callbacks_attempted": 0,
            "callbacks_succeeded": 0,
            "callbacks_failed": 0,
            "errors": []
        }
        
        # Execute callbacks outside the lock to avoid deadlocks
        for name, callback in active_callbacks:
            try:
                stats["callbacks_attempted"] += 1
                callback()
                stats["callbacks_succeeded"] += 1
                logger.debug(f"Global cleanup: '{name}' succeeded")
            except Exception as e:
                stats["callbacks_failed"] += 1
                stats["errors"].append(f"{name}: {str(e)}")
                logger.warning(f"Global cleanup callback '{name}' failed: {e}")
        
        # Perform global garbage collection
        try:
            collected = gc.collect()
            stats["gc_collected"] = collected
        except Exception as e:
            logger.warning(f"Global GC failed during coordinated cleanup: {e}")
            stats["gc_collected"] = 0
        
        duration = time.time() - start_time
        
        with self._lock:
            self._last_cleanup_time = start_time
            self._cleanup_stats["total_cleanups"] += 1
            self._cleanup_stats["last_cleanup_duration"] = duration
        
        if stats["callbacks_attempted"] > 0:
            logger.info(f"Global memory cleanup ({context}): {stats['callbacks_succeeded']}/{stats['callbacks_attempted']} "
                       f"callbacks succeeded, GC collected {stats['gc_collected']} objects in {duration:.3f}s")
        
        if stats["callbacks_failed"] > 0:
            logger.warning(f"Global cleanup had {stats['callbacks_failed']} failures: {stats['errors']}")
        
        return stats
    
    def _cleanup_dead_threads(self) -> None:
        """Remove registrations for dead threads."""
        dead_threads = []
        for thread in self._thread_registrations:
            if not thread.is_alive():
                dead_threads.append(thread)
        
        for dead_thread in dead_threads:
            callback_names = self._thread_registrations[dead_thread]
            for name in callback_names:
                self._cleanup_callbacks.pop(name, None)
            del self._thread_registrations[dead_thread]
            logger.debug(f"Cleaned up registrations for dead thread {dead_thread.name}")
    
    def get_stats(self) -> Dict[str, any]:
        """Get current memory manager statistics."""
        with self._lock:
            return self._cleanup_stats.copy()
    
    def create_thread_local_cleanup(self) -> "ThreadLocalCleanup":
        """Create a thread-local cleanup helper for the current thread."""
        return ThreadLocalCleanup(self)


class ThreadLocalCleanup:
    """
    Helper class for registering cleanup callbacks from a specific thread.
    Automatically unregisters callbacks when the thread dies.
    """
    
    def __init__(self, global_manager: GlobalMemoryManager):
        self.manager = global_manager
        self.thread = threading.current_thread()
        self.registered_callbacks: Set[str] = set()
    
    def register_cache_cleanup(self, name: str, cache_obj, clear_method: str = "clear") -> None:
        """
        Register cleanup for a cache object.
        
        Args:
            name: Unique name for this cache
            cache_obj: Object with a clear method (dict, list, set, etc.)
            clear_method: Name of the clear method (default: "clear")
        """
        def cleanup_func():
            try:
                if hasattr(cache_obj, clear_method):
                    getattr(cache_obj, clear_method)()
                    logger.debug(f"Cleared cache '{name}' in thread {self.thread.name}")
            except Exception as e:
                logger.debug(f"Failed to clear cache '{name}': {e}")
        
        full_name = f"{self.thread.name}_{name}"
        self.manager.register_cleanup_callback(full_name, cleanup_func, self.thread)
        self.registered_callbacks.add(full_name)
    
    def register_custom_cleanup(self, name: str, cleanup_func: Callable[[], None]) -> None:
        """Register a custom cleanup function."""
        full_name = f"{self.thread.name}_{name}"
        self.manager.register_cleanup_callback(full_name, cleanup_func, self.thread)
        self.registered_callbacks.add(full_name)
    
    def cleanup_all_local(self) -> None:
        """Manually trigger cleanup for all registered callbacks in this thread."""
        for name in self.registered_callbacks:
            try:
                callback = self.manager._cleanup_callbacks.get(name)
                if callback:
                    callback()
            except Exception as e:
                logger.warning(f"Thread-local cleanup for '{name}' failed: {e}")


# Global singleton instance
_global_memory_manager: Optional[GlobalMemoryManager] = None
_manager_lock = threading.Lock()


def get_global_memory_manager() -> GlobalMemoryManager:
    """Get the global memory manager singleton."""
    global _global_memory_manager
    
    if _global_memory_manager is None:
        with _manager_lock:
            if _global_memory_manager is None:
                _global_memory_manager = GlobalMemoryManager()
                logger.info("Initialized global memory manager")
    
    return _global_memory_manager


def register_thread_cleanup(name: str, callback: Callable[[], None]) -> None:
    """Convenience function to register a cleanup callback for the current thread."""
    manager = get_global_memory_manager()
    manager.register_cleanup_callback(name, callback)


def trigger_global_cleanup(context: str = "manual") -> Dict[str, any]:
    """Convenience function to trigger coordinated cleanup across all threads."""
    manager = get_global_memory_manager()
    return manager.trigger_coordinated_cleanup(context)


def create_thread_cleanup_helper() -> ThreadLocalCleanup:
    """Convenience function to create a thread-local cleanup helper."""
    manager = get_global_memory_manager()
    return manager.create_thread_local_cleanup() 