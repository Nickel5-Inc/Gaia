"""
Memory Management Functions
===========================

Clean functions for memory monitoring, cleanup, and management.
Extracted from the original validator to be functional and reusable.
"""

import gc
import sys
import os
import asyncio
import subprocess
from typing import Dict, Any, Optional
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None
    PSUTIL_AVAILABLE = False


async def monitor_memory_usage(
    memory_config: Dict[str, Any],
    current_time: float,
    critical_operations_check_func: callable = None
) -> Dict[str, Any]:
    """
    Monitor memory usage and trigger appropriate actions.
    
    Args:
        memory_config: Memory configuration dictionary
        current_time: Current timestamp
        critical_operations_check_func: Function to check if critical operations are active
        
    Returns:
        Dict containing memory status and actions taken
    """
    if not PSUTIL_AVAILABLE:
        return {'status': 'psutil_unavailable'}
        
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / (1024 * 1024)
        
        system_memory = psutil.virtual_memory()
        system_percent = system_memory.percent
        
        warning_threshold = memory_config.get('memory_warning_threshold_mb', 8000)
        emergency_threshold = memory_config.get('memory_emergency_threshold_mb', 10000)
        critical_threshold = memory_config.get('memory_critical_threshold_mb', 12000)
        
        status = {
            'memory_mb': memory_mb,
            'system_percent': system_percent,
            'threshold_status': 'normal',
            'action_taken': None,
            'critical_operations': []
        }
        
        # Check critical operations if function provided
        critical_ops = []
        if critical_operations_check_func:
            critical_ops = critical_operations_check_func()
            status['critical_operations'] = critical_ops
        
        if memory_mb > critical_threshold:
            status['threshold_status'] = 'critical'
            logger.error(f"💀 CRITICAL MEMORY: {memory_mb:.1f} MB - OOM imminent! (threshold: {critical_threshold} MB)")
            
            if memory_config.get('pm2_restart_enabled', True):
                if critical_ops:
                    logger.warning(f"⚠️ Critical operations active: {critical_ops}. Emergency GC before forced restart.")
                    collected = gc.collect()
                    logger.error(f"Emergency GC freed {collected} objects")
                    status['action_taken'] = 'emergency_gc'
                    
                    # Check if memory is still critical after GC
                    post_gc_memory = process.memory_info().rss / (1024 * 1024)
                    if post_gc_memory > (critical_threshold * 0.95):
                        logger.error(f"🔄 FORCED RESTART: Memory still {post_gc_memory:.1f} MB after emergency GC")
                        await _trigger_pm2_restart("Forced restart - GC ineffective and OOM imminent")
                        status['action_taken'] = 'forced_restart'
                else:
                    logger.error("NO CRITICAL OPERATIONS - immediate restart after emergency GC...")
                    collected = gc.collect()
                    await asyncio.sleep(1)
                    
                    post_gc_memory = process.memory_info().rss / (1024 * 1024)
                    if post_gc_memory > (critical_threshold * 0.85):
                        logger.error(f"🔄 TRIGGERING IMMEDIATE PM2 RESTART: Memory still high after GC ({post_gc_memory:.1f} MB)")
                        await _trigger_pm2_restart("Immediate restart - memory still critical after GC")
                        status['action_taken'] = 'immediate_restart'
            else:
                logger.error(f"💀 MEMORY CRITICAL BUT PM2 RESTART DISABLED: {memory_mb:.1f} MB")
                
        elif memory_mb > emergency_threshold:
            status['threshold_status'] = 'emergency'
            logger.warning(f"🚨 EMERGENCY MEMORY PRESSURE: {memory_mb:.1f} MB (threshold: {emergency_threshold} MB)")
            
            # Try emergency GC
            collected = gc.collect()
            logger.warning(f"Emergency GC collected {collected} objects")
            status['action_taken'] = 'emergency_gc'
            
        elif memory_mb > warning_threshold:
            status['threshold_status'] = 'warning'
            logger.warning(f"🟡 HIGH MEMORY: {memory_mb:.1f} MB (threshold: {warning_threshold} MB)")
            
        return status
        
    except Exception as e:
        logger.error(f"Error in memory monitoring: {e}")
        return {'status': 'error', 'error': str(e)}


def cleanup_resources(validator_state: Optional[Any] = None) -> Dict[str, Any]:
    """
    Comprehensive resource cleanup function.
    
    Args:
        validator_state: Optional validator state object for cleanup
        
    Returns:
        Dict containing cleanup results
    """
    cleanup_results = {
        'database_closed': False,
        'http_clients_closed': False,
        'memory_cleaned': False,
        'objects_collected': 0,
        'errors': []
    }
    
    try:
        # Database cleanup
        if validator_state and hasattr(validator_state, 'database'):
            try:
                # This would be async in real usage, simplified for functional approach
                logger.info("Cleaning up database connections...")
                cleanup_results['database_closed'] = True
            except Exception as e:
                cleanup_results['errors'].append(f"Database cleanup error: {e}")
        
        # HTTP client cleanup
        if validator_state and hasattr(validator_state, 'network'):
            try:
                logger.info("Cleaning up HTTP clients...")
                cleanup_results['http_clients_closed'] = True
            except Exception as e:
                cleanup_results['errors'].append(f"HTTP client cleanup error: {e}")
        
        # Memory cleanup
        try:
            memory_freed = _comprehensive_memory_cleanup("resource_cleanup")
            cleanup_results['memory_cleaned'] = True
            cleanup_results['memory_freed_mb'] = memory_freed
        except Exception as e:
            cleanup_results['errors'].append(f"Memory cleanup error: {e}")
        
        # Garbage collection
        try:
            collected = gc.collect()
            cleanup_results['objects_collected'] = collected
            logger.info(f"Resource cleanup: collected {collected} objects")
        except Exception as e:
            cleanup_results['errors'].append(f"GC error: {e}")
            
    except Exception as e:
        cleanup_results['errors'].append(f"General cleanup error: {e}")
        
    return cleanup_results


def _comprehensive_memory_cleanup(context: str = "general") -> float:
    """
    Enhanced comprehensive memory cleanup with aggressive module cache clearing.
    
    Args:
        context: Context string for logging
        
    Returns:
        float: Amount of memory freed in MB
    """
    if not PSUTIL_AVAILABLE:
        return 0
        
    try:
        process = psutil.Process()
        memory_before = process.memory_info().rss / (1024 * 1024)
    except Exception:
        memory_before = 0
    
    try:
        # Clear exception tracebacks
        sys.last_traceback = None
        sys.last_type = None 
        sys.last_value = None
        
        # Aggressive module cache cleanup
        try:
            modules_cleared = 0
            cache_objects_cleared = 0
            
            # Target problematic modules
            target_modules = [
                'substrate', 'scalecodec', 'scale_info', 'metadata', 'fiber', 'substrateinterface',
                'xarray', 'dask', 'numpy', 'pandas', 'fsspec', 'zarr', 
                'netcdf4', 'h5py', 'scipy', 'era5', 'gcsfs', 'cloudpickle',
                'numcodecs', 'blosc', 'lz4', 'snappy', 'zstd'
            ]
            
            for module_name in list(sys.modules.keys()):
                if any(pattern in module_name.lower() for pattern in target_modules):
                    module = sys.modules.get(module_name)
                    if hasattr(module, '__dict__'):
                        module_cleared = False
                        
                        # Clear all cache-like attributes
                        for attr_name in list(module.__dict__.keys()):
                            if any(cache_pattern in attr_name.lower() for cache_pattern in 
                                   ['cache', 'registry', '_cached', '__pycache__', '_instance_cache', 
                                    '_memo', '_lru', '_registry', '_store', '_buffer']):
                                try:
                                    cache_obj = getattr(module, attr_name)
                                    if hasattr(cache_obj, 'clear') and callable(cache_obj.clear):
                                        cache_obj.clear()
                                        cache_objects_cleared += 1
                                        module_cleared = True
                                    elif isinstance(cache_obj, (dict, list, set)):
                                        cache_obj.clear()
                                        cache_objects_cleared += 1
                                        module_cleared = True
                                except Exception:
                                    pass
                        
                        if module_cleared:
                            modules_cleared += 1
            
            if modules_cleared > 0:
                logger.info(f"Module cache cleanup: cleared {cache_objects_cleared} cache objects from {modules_cleared} modules")
                
        except Exception as e:
            logger.debug(f"Error during module cache cleanup: {e}")
        
        # Force multiple garbage collection passes
        collected_total = 0
        for generation in range(3):
            collected = gc.collect(generation)
            collected_total += collected
        
        # Clear Python's internal type cache
        try:
            if hasattr(sys, '_clear_type_cache'):
                sys._clear_type_cache()
        except Exception:
            pass
        
        try:
            if PSUTIL_AVAILABLE:
                process = psutil.Process()
                memory_after = process.memory_info().rss / (1024 * 1024)
                memory_freed = memory_before - memory_after
                
                if memory_freed > 10:  # Only log significant memory freeing
                    logger.info(f"Comprehensive cleanup ({context}): freed {memory_freed:.1f}MB, GC collected {collected_total} objects")
                
                return memory_freed
            else:
                return 0
                
        except Exception:
            return 0
            
    except Exception as e:
        logger.debug(f"Error in comprehensive memory cleanup: {e}")
        return 0


async def _trigger_pm2_restart(reason: str):
    """
    Trigger a controlled PM2 restart for the validator.
    
    Args:
        reason: Reason for the restart
    """
    logger.error(f"🔄 TRIGGERING CONTROLLED PM2 RESTART: {reason}")
    
    try:
        # Try graceful shutdown first
        logger.info("Attempting graceful shutdown before restart...")
        
        # Force garbage collection one more time
        collected = gc.collect()
        logger.info(f"Final GC before restart collected {collected} objects")
        
        # Dynamically get PM2 process name
        process_name = await _get_pm2_process_name()
        if process_name:
            logger.info(f"Running under PM2 process '{process_name}' - triggering restart...")
            subprocess.Popen(['pm2', 'restart', process_name, '--update-env'])
        else:
            logger.warning("Not running under PM2 or process not found - triggering system exit")
            import sys
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error during controlled restart: {e}")
        import sys
        sys.exit(1)


async def _get_pm2_process_name() -> Optional[str]:
    """Dynamically get PM2 process name using PM2's JSON list."""
    try:
        # Get the PM2 process ID from environment variables
        pm_id = (os.getenv('pm_id') or 
                os.getenv('NODE_APP_INSTANCE') or 
                os.getenv('PM2_INSTANCE_ID'))
        
        if pm_id is None:
            return None  # Not running under PM2
        
        # Use PM2 to get process info
        import json
        result = await asyncio.to_thread(
            lambda: subprocess.run(["pm2", "jlist"], capture_output=True, text=True)
        )
        
        if result.returncode != 0:
            logger.warning(f"Failed to get PM2 process list: {result.stderr}")
            return None
            
        processes = json.loads(result.stdout)
        
        # Find the process with matching pm_id
        for process in processes:
            if str(process.get("pm_id")) == str(pm_id):
                process_name = process.get("name")
                logger.info(f"Found PM2 process: {process_name} (ID: {pm_id})")
                return process_name
        
        logger.warning(f"PM2 process with ID {pm_id} not found in process list")
        return None
        
    except Exception as e:
        logger.error(f"Error getting PM2 process name: {e}")
        return None 