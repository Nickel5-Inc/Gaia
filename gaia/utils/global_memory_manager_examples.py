"""
Examples of how to use the Global Memory Manager to extend memory cleanup 
across all background threads in the application.

This helps catch "sneaky cache growth" in threads not covered by the main
validator memory cleanup.
"""

import threading
import time
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor
from gaia.utils.global_memory_manager import (
    create_thread_cleanup_helper,
    register_thread_cleanup,
    get_global_memory_manager
)
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


# Example 1: Simple cache cleanup in a worker thread
def example_worker_thread_with_cache():
    """Example of how a worker thread can register its cache for cleanup."""
    
    # This thread maintains some caches
    local_cache = {}
    computation_cache = {}
    
    # Register cleanup for these caches
    cleanup_helper = create_thread_cleanup_helper()
    cleanup_helper.register_cache_cleanup("local_cache", local_cache)
    cleanup_helper.register_cache_cleanup("computation_cache", computation_cache)
    
    # Worker thread logic...
    logger.info(f"Worker thread {threading.current_thread().name} registered cache cleanup")
    
    # Simulate work that accumulates cache
    for i in range(100):
        local_cache[f"key_{i}"] = f"value_{i}"
        computation_cache[f"result_{i}"] = i * i
        time.sleep(0.01)
    
    logger.info(f"Worker thread accumulated {len(local_cache)} cache entries")


# Example 2: Custom cleanup function
def example_custom_cleanup():
    """Example of registering a custom cleanup function."""
    
    # Some complex state that needs custom cleanup
    complex_state = {
        "connections": [],
        "buffers": [],
        "temp_files": []
    }
    
    def custom_cleanup():
        # Custom cleanup logic
        complex_state["connections"].clear()
        complex_state["buffers"].clear() 
        complex_state["temp_files"].clear()
        logger.debug("Performed custom cleanup in thread")
    
    # Register the custom cleanup
    register_thread_cleanup("custom_cleanup", custom_cleanup)
    
    # Simulate accumulating state
    complex_state["connections"].extend([f"conn_{i}" for i in range(50)])
    complex_state["buffers"].extend([f"buffer_{i}" for i in range(30)])


# Example 3: HTTP Client cleanup
def example_http_client_cleanup():
    """Example of how to register cleanup for HTTP clients in background threads."""
    
    # Simulate an HTTP client with caches
    class MockHTTPClient:
        def __init__(self):
            self.response_cache = {}
            self.connection_pool = []
        
        def clear_caches(self):
            self.response_cache.clear()
            self.connection_pool.clear()
    
    client = MockHTTPClient()
    
    # Register cleanup for the client
    cleanup_helper = create_thread_cleanup_helper()
    cleanup_helper.register_cache_cleanup("http_response_cache", client.response_cache)
    cleanup_helper.register_custom_cleanup("http_client_full", client.clear_caches)


# Example 4: Thread Pool with cleanup registration
class ManagedThreadPool:
    """Example of a thread pool that registers cleanup for all its worker threads."""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.executor = None
        self.worker_caches: Dict[str, Dict] = {}
    
    def start(self):
        """Start the thread pool and register cleanup for worker threads."""
        self.executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="managed_worker"
        )
        
        # Pre-register cleanup for worker threads that will be created
        for i in range(self.max_workers):
            worker_name = f"managed_worker_{i}"
            self.worker_caches[worker_name] = {}
            
            def make_cleanup(cache_dict):
                def cleanup():
                    cache_dict.clear()
                    logger.debug(f"Cleared cache for {worker_name}")
                return cleanup
            
            register_thread_cleanup(f"{worker_name}_cache", make_cleanup(self.worker_caches[worker_name]))
    
    def submit_work(self, func, *args, **kwargs):
        """Submit work to the thread pool."""
        if not self.executor:
            self.start()
        return self.executor.submit(func, *args, **kwargs)
    
    def shutdown(self):
        """Shutdown the thread pool."""
        if self.executor:
            self.executor.shutdown(wait=True)


# Example 5: Background monitoring task with cleanup
def example_monitoring_task():
    """Example of a background monitoring task that accumulates data and needs cleanup."""
    
    monitoring_data = {
        "metrics": [],
        "alerts": [],
        "history": {}
    }
    
    def cleanup_monitoring():
        # Keep only recent data, clear the rest
        monitoring_data["metrics"] = monitoring_data["metrics"][-100:]  # Keep last 100
        monitoring_data["alerts"].clear()
        monitoring_data["history"].clear()
        logger.debug("Cleaned up monitoring data")
    
    register_thread_cleanup("monitoring_cleanup", cleanup_monitoring)
    
    # Simulate monitoring work
    while True:
        monitoring_data["metrics"].append({"timestamp": time.time(), "value": 42})
        monitoring_data["alerts"].append(f"alert_{time.time()}")
        monitoring_data["history"][time.time()] = "some_data"
        
        time.sleep(1)


# Example 6: How to manually trigger cleanup for testing
def example_manual_testing():
    """Example of how to manually trigger cleanup for testing purposes."""
    
    from gaia.utils.global_memory_manager import trigger_global_cleanup
    
    # Create some test caches
    test_cache = {f"key_{i}": f"value_{i}" for i in range(1000)}
    
    register_thread_cleanup("test_cache", lambda: test_cache.clear())
    
    print(f"Before cleanup: {len(test_cache)} items")
    
    # Trigger cleanup manually
    stats = trigger_global_cleanup("manual_test")
    
    print(f"After cleanup: {len(test_cache)} items")
    print(f"Cleanup stats: {stats}")


# Example 7: Integration with existing worker classes
class ExampleWorkerClass:
    """Example of how to integrate global memory cleanup into existing worker classes."""
    
    def __init__(self, worker_name: str):
        self.worker_name = worker_name
        self.cache = {}
        self.buffers = []
        self.temp_data = {}
        
        # Register cleanup in constructor
        self._setup_memory_cleanup()
    
    def _setup_memory_cleanup(self):
        """Setup memory cleanup for this worker."""
        cleanup_helper = create_thread_cleanup_helper()
        
        # Register individual caches
        cleanup_helper.register_cache_cleanup("worker_cache", self.cache)
        cleanup_helper.register_cache_cleanup("worker_buffers", self.buffers)
        cleanup_helper.register_cache_cleanup("worker_temp_data", self.temp_data)
        
        # Register custom cleanup for more complex operations
        def worker_cleanup():
            # Perform any additional cleanup specific to this worker
            self.cache.clear()
            self.buffers.clear()
            self.temp_data.clear()
            # Could also close files, connections, etc.
            logger.debug(f"Performed comprehensive cleanup for {self.worker_name}")
        
        cleanup_helper.register_custom_cleanup("worker_comprehensive", worker_cleanup)
    
    def do_work(self):
        """Simulate work that accumulates data."""
        self.cache[f"result_{len(self.cache)}"] = "some_result"
        self.buffers.append(f"buffer_{len(self.buffers)}")
        self.temp_data[f"temp_{len(self.temp_data)}"] = {"data": "value"}


# Usage examples in real code:
if __name__ == "__main__":
    # Example usage:
    
    # 1. Create some worker threads with cache cleanup
    threads = []
    for i in range(3):
        t = threading.Thread(target=example_worker_thread_with_cache, name=f"worker_{i}")
        threads.append(t)
        t.start()
    
    # 2. Create a managed thread pool
    pool = ManagedThreadPool(max_workers=2)
    pool.start()
    
    # 3. Create some worker instances
    workers = [ExampleWorkerClass(f"worker_{i}") for i in range(3)]
    
    # 4. Let them accumulate some data
    time.sleep(2)
    
    for worker in workers:
        worker.do_work()
    
    # 5. Check global memory manager stats
    manager = get_global_memory_manager()
    stats = manager.get_stats()
    print(f"Global memory manager stats: {stats}")
    
    # 6. Trigger manual cleanup
    cleanup_stats = trigger_global_cleanup("example_test")
    print(f"Manual cleanup stats: {cleanup_stats}")
    
    # 7. Wait for threads to finish
    for t in threads:
        t.join()
    
    pool.shutdown() 