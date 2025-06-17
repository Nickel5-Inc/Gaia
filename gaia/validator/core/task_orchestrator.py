"""
Task Orchestrator Core Module

Handles task scheduling, health monitoring, watchdog operations,
task recovery, and overall task coordination for the validator.
"""

import os
import time
import asyncio
import traceback
import signal
import gc
from typing import Dict, Optional, Any
from datetime import datetime, timezone

from fiber.logging_utils import get_logger

logger = get_logger(__name__)

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None 
    PSUTIL_AVAILABLE = False


class TaskOrchestrator:
    """
    Handles task scheduling, health monitoring, and watchdog operations
    for the validator's various tasks.
    """
    
    def __init__(self, args):
        """Initialize the task orchestrator."""
        self.args = args
        self.watchdog_timeout = 3600  # 1 hour default timeout
        self.db_check_interval = 300  # 5 minutes
        self.metagraph_sync_interval = 300  # 5 minutes
        self.max_consecutive_errors = 3
        self.watchdog_running = False
        
        # Task health tracking
        self.task_health = {
            'scoring': {
                'last_success': time.time(),
                'errors': 0,
                'status': 'idle',
                'current_operation': None,
                'operation_start': None,
                'timeouts': {
                    'default': 1800,  # 30 minutes
                    'weight_setting': 300,  # 5 minutes
                },
                'resources': {
                    'memory_start': 0,
                    'memory_peak': 0,
                    'cpu_percent': 0,
                    'open_files': 0,
                    'threads': 0,
                    'last_update': None
                }
            },
            'deregistration': {
                'last_success': time.time(),
                'errors': 0,
                'status': 'idle',
                'current_operation': None,
                'operation_start': None,
                'timeouts': {
                    'default': 1800,  # 30 minutes
                    'db_check': 300,  # 5 minutes
                },
                'resources': {
                    'memory_start': 0,
                    'memory_peak': 0,
                    'cpu_percent': 0,
                    'open_files': 0,
                    'threads': 0,
                    'last_update': None
                }
            },
            'geomagnetic': {
                'last_success': time.time(),
                'errors': 0,
                'status': 'idle',
                'current_operation': None,
                'operation_start': None,
                'timeouts': {
                    'default': 1800,  # 30 minutes
                    'data_fetch': 300,  # 5 minutes
                    'miner_query': 600,  # 10 minutes
                },
                'resources': {
                    'memory_start': 0,
                    'memory_peak': 0,
                    'cpu_percent': 0,
                    'open_files': 0,
                    'threads': 0,
                    'last_update': None
                }
            },
            'soil': {
                'last_success': time.time(),
                'errors': 0,
                'status': 'idle',
                'current_operation': None,
                'operation_start': None,
                'timeouts': {
                    'default': 3600,  # 1 hour
                    'data_download': 1800,  # 30 minutes
                    'miner_query': 1800,  # 30 minutes
                    'region_processing': 900,  # 15 minutes
                },
                'resources': {
                    'memory_start': 0,
                    'memory_peak': 0,
                    'cpu_percent': 0,
                    'open_files': 0,
                    'threads': 0,
                    'last_update': None
                }
            },
            'weather': {
                'last_success': time.time(),
                'errors': 0,
                'status': 'idle',
                'current_operation': None,
                'operation_start': None,
                'timeouts': {
                    'default': 1800,  # 30 minutes
                    'data_fetch': 300,  # 5 minutes
                    'miner_query': 600,  # 10 minutes
                },
                'resources': {
                    'memory_start': 0,
                    'memory_peak': 0,
                    'cpu_percent': 0,
                    'open_files': 0,
                    'threads': 0,
                    'last_update': None
                }
            }
        }
        
        # Memory monitoring configuration
        self.memory_monitor_enabled = os.getenv('VALIDATOR_MEMORY_MONITORING_ENABLED', 'true').lower() in ['true', '1', 'yes']
        self.pm2_restart_enabled = os.getenv('VALIDATOR_PM2_RESTART_ENABLED', 'true').lower() in ['true', '1', 'yes']
        
        # Memory thresholds in MB - higher defaults for validators (24GB = 24576MB)
        self.memory_warning_threshold_mb = int(os.getenv('VALIDATOR_MEMORY_WARNING_THRESHOLD_MB', '16000'))  # 16GB
        self.memory_emergency_threshold_mb = int(os.getenv('VALIDATOR_MEMORY_EMERGENCY_THRESHOLD_MB', '20000'))  # 20GB  
        self.memory_critical_threshold_mb = int(os.getenv('VALIDATOR_MEMORY_CRITICAL_THRESHOLD_MB', '24000'))  # 24GB
        
        # Memory monitoring state
        self.last_memory_log_time = 0
        self.memory_log_interval = 300  # Log memory status every 5 minutes
        self.last_emergency_gc_time = 0
        self.emergency_gc_cooldown = 60  # Minimum 60 seconds between emergency GC attempts

    async def update_task_status(self, task_name: str, status: str, operation: Optional[str] = None):
        """Update task status and operation tracking."""
        if task_name in self.task_health:
            health = self.task_health[task_name]
            health['status'] = status
            
            if operation:
                if operation != health.get('current_operation'):
                    health['current_operation'] = operation
                    health['operation_start'] = time.time()
                    # Track initial resource usage when operation starts
                    try:
                        if PSUTIL_AVAILABLE:
                            process = psutil.Process()
                            health['resources']['memory_start'] = process.memory_info().rss
                            health['resources']['memory_peak'] = health['resources']['memory_start']
                            health['resources']['cpu_percent'] = process.cpu_percent()
                            health['resources']['open_files'] = len(process.open_files())
                            health['resources']['threads'] = process.num_threads()
                            health['resources']['last_update'] = time.time()
                            logger.info(
                                f"Task {task_name} started operation: {operation} | "
                                f"Initial Memory: {health['resources']['memory_start'] / (1024*1024):.2f}MB"
                            )
                    except Exception:
                        logger.warning("psutil not available for resource tracking")
            elif status == 'idle':
                if health.get('current_operation'):
                    # Log resource usage when operation completes
                    try:
                        if PSUTIL_AVAILABLE:
                            process = psutil.Process()
                            current_memory = process.memory_info().rss
                            memory_change = current_memory - health['resources']['memory_start']
                            peak_memory = max(current_memory, health['resources'].get('memory_peak', 0))
                            logger.info(
                                f"Task {task_name} completed operation: {health['current_operation']} | "
                                f"Memory Change: {memory_change / (1024*1024):.2f}MB | "
                                f"Peak Memory: {peak_memory / (1024*1024):.2f}MB"
                            )
                    except Exception:
                        pass
                health['current_operation'] = None
                health['operation_start'] = None
                health['last_success'] = time.time()

    async def start_watchdog(self):
        """Start the watchdog in a separate thread."""
        if not self.watchdog_running:
            self.watchdog_running = True
            logger.info("Started watchdog")
            
            # Log memory monitoring configuration
            if self.memory_monitor_enabled:
                try:
                    if PSUTIL_AVAILABLE:
                        system_memory = psutil.virtual_memory()
                        logger.info(f"🔍 Validator memory monitoring enabled:")
                        logger.info(f"  System memory: {system_memory.total / (1024**3):.1f} GB total, {system_memory.available / (1024**3):.1f} GB available")
                        logger.info(f"  Memory thresholds: Warning={self.memory_warning_threshold_mb}MB, Emergency={self.memory_emergency_threshold_mb}MB, Critical={self.memory_critical_threshold_mb}MB")
                        logger.info(f"  PM2 restart enabled: {self.pm2_restart_enabled}")
                        if self.pm2_restart_enabled:
                            pm2_id = os.getenv('pm2_id', 'not detected')
                            logger.info(f"  PM2 instance ID: {pm2_id}")
                except Exception:
                    logger.warning("psutil not available - memory monitoring will be disabled")
                    self.memory_monitor_enabled = False
            else:
                logger.info("Validator memory monitoring disabled by configuration")
            
            asyncio.create_task(self._watchdog_loop())

    async def _watchdog_loop(self):
        """Run the watchdog monitoring in the main event loop."""
        while self.watchdog_running:
            try:
                # Add timeout to prevent long execution
                try:
                    await asyncio.wait_for(
                        self._watchdog_check(),
                        timeout=30  # 30 second timeout
                    )
                except asyncio.TimeoutError:
                    logger.error("Watchdog check timed out after 30 seconds")
                except Exception as e:
                    logger.error(f"Error in watchdog loop: {e}")
                    logger.error(traceback.format_exc())
            except Exception as outer_e:
                logger.error(f"Outer error in watchdog: {outer_e}")
                logger.error(traceback.format_exc())
            await asyncio.sleep(60)  # Check every minute

    async def stop_watchdog(self):
        """Stop the watchdog."""
        if self.watchdog_running:
            self.watchdog_running = False
            logger.info("Stopped watchdog")

    async def _watchdog_check(self):
        """Perform a single watchdog check iteration."""
        try:
            current_time = time.time()
            
            # Memory monitoring check (first priority)
            if self.memory_monitor_enabled:
                try:
                    await asyncio.wait_for(
                        self._check_memory_usage(current_time),
                        timeout=5  # 5 second timeout for memory check
                    )
                except asyncio.TimeoutError:
                    logger.error("Memory usage check timed out")
                except Exception as e:
                    logger.error(f"Error checking memory usage: {e}")
            
            # Update resource usage for all active tasks
            try:
                await asyncio.wait_for(
                    self._check_resource_usage(current_time),
                    timeout=10  # 10 second timeout
                )
            except asyncio.TimeoutError:
                logger.error("Resource usage check timed out")
            except Exception as e:
                logger.error(f"Error checking resource usage: {e}")

            # Check task health with timeout
            try:
                await asyncio.wait_for(
                    self._check_task_health(current_time),
                    timeout=10  # 10 second timeout
                )
            except asyncio.TimeoutError:
                logger.error("Task health check timed out")
            except Exception as e:
                logger.error(f"Error checking task health: {e}")

        except Exception as e:
            logger.error(f"Error in watchdog check: {e}")
            logger.error(traceback.format_exc())

    async def _check_memory_usage(self, current_time):
        """Check overall validator memory usage and trigger restart if needed."""
        try:
            if not PSUTIL_AVAILABLE:
                return
                
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            
            system_memory = psutil.virtual_memory()
            system_percent = system_memory.percent
            
            # Regular memory status logging
            if current_time - self.last_memory_log_time > self.memory_log_interval:
                logger.info(f"🔍 Validator memory status: {memory_mb:.1f} MB RSS ({system_percent:.1f}% system memory)")
                self.last_memory_log_time = current_time
            
            # Check if we're in a critical operation that shouldn't be interrupted
            critical_operations_active = self._check_critical_operations_active()
            
            # Memory threshold checks
            if memory_mb > self.memory_critical_threshold_mb:
                logger.error(f"💀 CRITICAL MEMORY: {memory_mb:.1f} MB - OOM imminent! (threshold: {self.memory_critical_threshold_mb} MB)")
                
                if critical_operations_active:
                    logger.warning(f"⚠️  Critical operations active: {critical_operations_active}. Delaying restart until operations complete.")
                    # Try emergency GC but don't restart yet
                    if current_time - self.last_emergency_gc_time > self.emergency_gc_cooldown:
                        logger.error("Attempting emergency garbage collection while critical operations are active...")
                        collected = gc.collect()
                        logger.error(f"Emergency GC freed {collected} objects")
                        self.last_emergency_gc_time = current_time
                else:
                    logger.error("Attempting emergency garbage collection before potential restart...")
                    try:
                        collected = gc.collect()
                        logger.error(f"Emergency GC freed {collected} objects")
                        await asyncio.sleep(2)  # Brief pause after emergency GC
                        
                        # Check memory again after GC
                        post_gc_memory = process.memory_info().rss / (1024 * 1024)
                        if post_gc_memory > (self.memory_critical_threshold_mb * 0.9):  # Still >90% of critical
                            if self.pm2_restart_enabled:
                                logger.error(f"🔄 TRIGGERING PM2 RESTART: Memory still critical after GC ({post_gc_memory:.1f} MB)")
                                await self._trigger_pm2_restart("Critical memory pressure after GC")
                                return  # Exit the monitoring
                            else:
                                logger.error(f"💀 MEMORY CRITICAL BUT PM2 RESTART DISABLED: {post_gc_memory:.1f} MB - system may be killed by OOM")
                        else:
                            logger.info(f"✅ Memory reduced to {post_gc_memory:.1f} MB after GC - continuing")
                    except Exception as gc_err:
                        logger.error(f"Emergency GC failed: {gc_err}")
                        if self.pm2_restart_enabled and not critical_operations_active:
                            await self._trigger_pm2_restart("Emergency GC failed and memory critical")
                            return
                        else:
                            logger.error("💀 GC FAILED AND RESTART CONDITIONS NOT MET - system may crash")
                            
            elif memory_mb > self.memory_emergency_threshold_mb:
                logger.warning(f"🚨 EMERGENCY MEMORY PRESSURE: {memory_mb:.1f} MB - OOM risk HIGH! (threshold: {self.memory_emergency_threshold_mb} MB)")
                # Try light GC at emergency level
                if current_time - self.last_emergency_gc_time > self.emergency_gc_cooldown:
                    collected = gc.collect()
                    logger.warning(f"Emergency light GC collected {collected} objects")
                    self.last_emergency_gc_time = current_time
                    
            elif memory_mb > self.memory_warning_threshold_mb:
                # Only log warning once per interval to avoid spam
                if current_time - self.last_memory_log_time > (self.memory_log_interval / 2):  # Half interval for warnings
                    logger.warning(f"🟡 HIGH MEMORY: Validator process using {memory_mb:.1f} MB ({system_percent:.1f}% of system) (threshold: {self.memory_warning_threshold_mb} MB)")
                    
        except Exception as e:
            logger.error(f"Error in memory monitoring: {e}", exc_info=True)

    def _check_critical_operations_active(self):
        """Check if any critical operations are currently active that shouldn't be interrupted."""
        critical_ops = []
        
        for task_name, health in self.task_health.items():
            if health['status'] in ['processing'] and health.get('current_operation'):
                # Define critical operations that shouldn't be interrupted
                critical_operation_patterns = [
                    'weight_setting',  # Never interrupt weight setting
                    'miner_query',     # Don't interrupt while querying miners
                    'scoring',         # Don't interrupt scoring calculations
                    'db_sync',         # Don't interrupt database sync
                    'data_fetch',      # Don't interrupt data fetching
                ]
                
                current_op = health.get('current_operation', '')
                if any(pattern in current_op for pattern in critical_operation_patterns):
                    critical_ops.append(f"{task_name}:{current_op}")
        
        return critical_ops

    async def _trigger_pm2_restart(self, reason: str):
        """Trigger a controlled PM2 restart for the validator."""
        if not self.pm2_restart_enabled:
            logger.error(f"🚨 PM2 restart disabled - would restart for: {reason}")
            return
            
        logger.error(f"🔄 TRIGGERING CONTROLLED PM2 RESTART: {reason}")
        
        try:
            # Force garbage collection one more time
            collected = gc.collect()
            logger.info(f"Final GC before restart collected {collected} objects")
            
            # Check if we're running under PM2
            pm2_instance_id = os.getenv('pm2_id')
            if pm2_instance_id:
                logger.info(f"Running under PM2 instance {pm2_instance_id} - triggering restart...")
                # Use pm2 restart command
                import subprocess
                subprocess.Popen(['pm2', 'restart', pm2_instance_id])
            else:
                logger.warning("Not running under PM2 - triggering system exit")
                # If not under pm2, exit gracefully
                import sys
                sys.exit(1)
                
        except Exception as e:
            logger.error(f"Error during controlled restart: {e}")
            # Last resort - force exit
            import sys
            sys.exit(1)

    async def _check_resource_usage(self, current_time):
        """Check resource usage for active tasks."""
        if not PSUTIL_AVAILABLE:
            return
            
        process = psutil.Process()
        for task_name, health in self.task_health.items():
            if health['status'] != 'idle':
                current_memory = process.memory_info().rss
                health['resources']['memory_peak'] = max(
                    current_memory,
                    health['resources'].get('memory_peak', 0)
                )
                health['resources']['cpu_percent'] = process.cpu_percent()
                health['resources']['open_files'] = len(process.open_files())
                health['resources']['threads'] = process.num_threads()
                health['resources']['last_update'] = current_time
                
                # Log if memory usage has increased significantly
                if current_memory > health['resources']['memory_peak'] * 1.5:  # 50% increase
                    logger.warning(
                        f"High memory usage in task {task_name} | "
                        f"Current: {current_memory / (1024*1024):.2f}MB | "
                        f"Previous Peak: {health['resources']['memory_peak'] / (1024*1024):.2f}MB"
                    )

    async def _check_task_health(self, current_time):
        """Check health of all tasks."""
        for task_name, health in self.task_health.items():
            if health['status'] == 'idle':
                continue
                
            timeout = health['timeouts'].get(
                health.get('current_operation'),
                health['timeouts']['default']
            )
            
            if health['operation_start'] and current_time - health['operation_start'] > timeout:
                operation_duration = current_time - health['operation_start']
                logger.warning(
                    f"TIMEOUT_ALERT - Task: {task_name} | "
                    f"Operation: {health.get('current_operation')} | "
                    f"Duration: {operation_duration:.2f}s | "
                    f"Timeout: {timeout}s | "
                    f"Status: {health['status']} | "
                    f"Errors: {health['errors']}"
                )
                
                if health['status'] != 'processing':
                    logger.error(
                        f"FREEZE_DETECTED - Task {task_name} appears frozen - "
                        f"Last Operation: {health.get('current_operation')} - "
                        f"Starting recovery"
                    )
                    try:
                        await self.recover_task(task_name)
                        health['errors'] = 0
                        logger.info(f"Successfully recovered task {task_name}")
                    except Exception as e:
                        logger.error(f"Failed to recover task {task_name}: {e}")
                        logger.error(traceback.format_exc())
                        health['errors'] += 1

    async def recover_task(self, task_name: str):
        """Enhanced task recovery with specific handling for each task type."""
        logger.warning(f"Attempting to recover {task_name}")
        try:
            # Reset task health
            health = self.task_health[task_name]
            health['errors'] = 0
            health['last_success'] = time.time()
            health['status'] = 'idle'
            health['current_operation'] = None
            health['operation_start'] = None
            
            logger.info(f"Successfully recovered task {task_name}")
            
        except Exception as e:
            logger.error(f"Failed to recover {task_name}: {e}")
            logger.error(traceback.format_exc())

    async def status_logger(self):
        """Log the status of the validator periodically."""
        while True:
            try:
                current_time_utc = datetime.now(timezone.utc)
                formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S")

                # Get substrate connection manager stats
                substrate_stats = "Substrate: N/A"

                logger.info(
                    f"\n"
                    f"---Status Update ---\n"
                    f"Time (UTC): {formatted_time} | \n"
                    f"Tasks Active: {sum(1 for h in self.task_health.values() if h['status'] != 'idle')} | \n"
                    f"Memory Monitor: {'Enabled' if self.memory_monitor_enabled else 'Disabled'} | \n"
                    f"{substrate_stats}"
                )

            except Exception as e:
                logger.error(f"Error in status logger: {e}")
                logger.error(f"{traceback.format_exc()}")
            finally:
                await asyncio.sleep(60)

    async def aggressive_memory_cleanup(self):
        """Aggressive memory cleanup to prevent memory leaks."""
        while True:
            try:
                await asyncio.sleep(120)  # Run every 2 minutes
                
                # Get memory before cleanup
                if PSUTIL_AVAILABLE:
                    process = psutil.Process()
                    memory_before = process.memory_info().rss / (1024 * 1024)
                    
                    # Perform garbage collection
                    collected = gc.collect()
                    
                    # Clear any module-level caches if available
                    try:
                        # Clear any fsspec caches
                        try:
                            import fsspec
                            if hasattr(fsspec, 'config') and hasattr(fsspec.config, 'conf'):
                                fsspec.config.conf.clear()
                            if hasattr(fsspec, 'filesystem') and hasattr(fsspec.filesystem, '_cache'):
                                fsspec.filesystem._cache.clear()
                        except ImportError:
                            pass
                        
                        # Clear any xarray caches
                        try:
                            import xarray as xr
                            if hasattr(xr, 'backends') and hasattr(xr.backends, 'list_engines'):
                                # Force cleanup of any xarray backend caches
                                pass
                        except ImportError:
                            pass
                            
                    except Exception as cache_clear_error:
                        logger.debug(f"Error clearing caches: {cache_clear_error}")
                    
                    # Get memory after cleanup
                    memory_after = process.memory_info().rss / (1024 * 1024)
                    memory_freed = memory_before - memory_after
                    
                    if collected > 50 or memory_freed > 10:
                        logger.info(f"Memory cleanup: GC collected {collected} objects, freed {memory_freed:.1f}MB")
                    else:
                        logger.debug(f"Memory cleanup: GC collected {collected} objects, freed {memory_freed:.1f}MB")
                        
            except asyncio.CancelledError:
                logger.info("Aggressive memory cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in aggressive memory cleanup: {e}")
                await asyncio.sleep(30)  # Shorter retry on error

    async def periodic_substrate_cleanup(self):
        """Periodically force substrate connection cleanup to prevent memory leaks."""
        while True:
            try:
                await asyncio.sleep(300)  # Run every 5 minutes
                
                # Force garbage collection for substrate cleanup
                collected = gc.collect()
                if collected > 20:
                    logger.debug(f"Substrate cleanup: collected {collected} objects")
                
            except asyncio.CancelledError:
                logger.info("Periodic substrate cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in periodic substrate cleanup: {e}")
                await asyncio.sleep(60)  # Shorter retry on error

    async def cleanup(self):
        """Clean up task orchestrator resources."""
        try:
            if self.watchdog_running:
                await self.stop_watchdog()
                
            logger.info("Task orchestrator cleanup completed")
        except Exception as e:
            logger.error(f"Error during task orchestrator cleanup: {e}")