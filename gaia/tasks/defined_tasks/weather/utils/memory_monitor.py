"""
Memory monitoring utilities for the weather task to prevent OOM kills.
"""

import psutil
import os
import gc
import time
from typing import Optional, Dict, Any
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

class MemoryMonitor:
    """Monitor and manage memory usage to prevent OOM kills."""
    
    def __init__(self, emergency_threshold_mb: float = 12000, warning_threshold_mb: float = 8000):
        """
        Initialize memory monitor.
        
        Args:
            emergency_threshold_mb: Memory threshold in MB to trigger emergency cleanup
            warning_threshold_mb: Memory threshold in MB to trigger warnings
        """
        self.emergency_threshold_mb = emergency_threshold_mb
        self.warning_threshold_mb = warning_threshold_mb
        self.process = psutil.Process()
        
    def get_memory_info(self) -> Dict[str, float]:
        """Get current memory information."""
        try:
            memory_info = self.process.memory_info()
            system_memory = psutil.virtual_memory()
            
            return {
                'rss_mb': memory_info.rss / (1024 * 1024),
                'vms_mb': memory_info.vms / (1024 * 1024),
                'system_total_mb': system_memory.total / (1024 * 1024),
                'system_available_mb': system_memory.available / (1024 * 1024),
                'system_percent': system_memory.percent
            }
        except Exception as e:
            logger.warning(f"Failed to get memory info: {e}")
            return {}
    
    def check_memory_pressure(self, operation_name: str = "operation") -> bool:
        """
        Check if memory pressure is too high.
        
        Returns:
            True if safe to continue, False if emergency threshold exceeded
        """
        memory_info = self.get_memory_info()
        if not memory_info:
            return True  # Assume safe if we can't check
            
        rss_mb = memory_info.get('rss_mb', 0)
        
        if rss_mb > self.emergency_threshold_mb:
            logger.error(f"🚨 EMERGENCY MEMORY PRESSURE: {rss_mb:.1f} MB during {operation_name} (threshold: {self.emergency_threshold_mb} MB)")
            self.emergency_cleanup()
            return False
        elif rss_mb > self.warning_threshold_mb:
            logger.warning(f"⚠️  HIGH MEMORY USAGE: {rss_mb:.1f} MB during {operation_name} (warning threshold: {self.warning_threshold_mb} MB)")
            self.light_cleanup()
            
        return True
    
    def emergency_cleanup(self):
        """Perform emergency memory cleanup."""
        logger.info("🆘 Performing emergency memory cleanup...")
        
        # Force multiple garbage collection passes
        collected_total = 0
        for i in range(5):
            collected = gc.collect()
            collected_total += collected
            if collected == 0:
                break
                
        logger.info(f"Emergency cleanup collected {collected_total} objects")
        
        # Try to force memory defragmentation on Linux
        try:
            import ctypes
            libc = ctypes.CDLL("libc.so.6")
            libc.malloc_trim(0)
            logger.info("Performed malloc_trim")
        except Exception:
            pass
    
    def light_cleanup(self):
        """Perform light memory cleanup."""
        collected = gc.collect()
        if collected > 0:
            logger.info(f"Light cleanup collected {collected} objects")
    
    def log_memory_status(self, operation_name: str = ""):
        """Log current memory status."""
        memory_info = self.get_memory_info()
        if memory_info:
            logger.info(f"Memory status {operation_name}: RSS={memory_info.get('rss_mb', 0):.1f}MB, "
                       f"System={memory_info.get('system_percent', 0):.1f}% used, "
                       f"Available={memory_info.get('system_available_mb', 0):.1f}MB")

# Global memory monitor instance
_global_monitor: Optional[MemoryMonitor] = None

def get_memory_monitor(emergency_threshold_mb: float = 12000, warning_threshold_mb: float = 8000) -> MemoryMonitor:
    """Get or create global memory monitor instance with custom thresholds."""
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = MemoryMonitor(emergency_threshold_mb, warning_threshold_mb)
    return _global_monitor

def get_weather_task_monitor(task_instance) -> MemoryMonitor:
    """Get memory monitor configured with weather task thresholds."""
    emergency_threshold = task_instance.config.get('memory_emergency_threshold_mb', 12000)
    warning_threshold = task_instance.config.get('memory_warning_threshold_mb', 8000)
    return get_memory_monitor(emergency_threshold, warning_threshold)

def check_memory_safe(operation_name: str = "operation") -> bool:
    """
    Quick check if it's safe to continue with memory-intensive operation.
    
    Returns:
        True if safe, False if should abort to prevent OOM
    """
    monitor = get_memory_monitor()
    return monitor.check_memory_pressure(operation_name)

def log_memory_usage(operation_name: str = ""):
    """Log current memory usage."""
    monitor = get_memory_monitor()
    monitor.log_memory_status(operation_name) 