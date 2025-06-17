"""
Task Orchestrator module for the Gaia validator.

This module contains task management, monitoring, and background task functionality
extracted from the original validator.py implementation.
"""

import asyncio
import gc
import logging
import os
import time
import traceback
import tracemalloc
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, Dict, List

from gaia.validator.utils.auto_updater import perform_update

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None 
    PSUTIL_AVAILABLE = False

logger = logging.getLogger(__name__)


class TaskOrchestrator:
    """
    Manages and orchestrates background tasks for the validator.
    
    Handles task health monitoring, memory management, database monitoring,
    system updates, and other background operations.
    """
    
    def __init__(self, validator_instance):
        """Initialize the task orchestrator."""
        self.validator = validator_instance
        
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

        # For database monitor plotting
        self.db_monitor_history = []
        self.db_monitor_history_lock = asyncio.Lock()
        self.DB_MONITOR_HISTORY_MAX_SIZE = 120 # e.g., 2 hours of data if monitor runs every minute

        self.tracemalloc_snapshot1: Optional[tracemalloc.Snapshot] = None

    async def database_monitor(self):
        """Periodically query and log database statistics from a consistent snapshot."""
        logger.info("Starting database monitor task...")
        while not self.validator._shutdown_event.is_set():
            await asyncio.sleep(60) # Check every 60 seconds

            current_timestamp_iso = datetime.now(timezone.utc).isoformat()
            collected_stats = {
                "timestamp": current_timestamp_iso,
                "connection_summary": "[Query Failed or No Data]",
                "null_state_connection_details": [],
                "idle_in_transaction_details": [],
                "lock_details": "[Query Failed or No Data]",
                "active_query_wait_events": [],
                "session_manager_stats": "[Query Failed or No Data]",
                "error": None
            }

            try:
                # 1. Fetch all pg_stat_activity data at once for a consistent snapshot
                activity_snapshot = []
                activity_query = "SELECT pid, usename, application_name, client_addr, backend_start, state, backend_type, query, state_change, query_start, wait_event_type, wait_event FROM pg_stat_activity;"
                try:
                    activity_snapshot = await self.validator.database_manager.fetch_all(activity_query, timeout=45.0)
                except Exception as e:
                    if "DatabaseTimeout" in str(type(e)):
                        collected_stats["error"] = "Timeout fetching pg_stat_activity snapshot."
                        logger.warning(f"[DB Monitor] {collected_stats['error']}")
                    else:
                        collected_stats["error"] = f"Error fetching pg_stat_activity snapshot: {type(e).__name__}"
                        logger.warning(f"[DB Monitor] {collected_stats['error']} - {e}")

                if collected_stats["error"]:
                    await self.validator.database_stats._log_and_store_db_stats(collected_stats)
                    continue

                # 2. Process the snapshot in memory
                summary, null_state_details, idle_in_transaction_details, active_query_details = self.validator.database_stats._process_activity_snapshot(activity_snapshot)
                
                collected_stats["connection_summary"] = summary
                collected_stats["null_state_connection_details"] = null_state_details
                collected_stats["idle_in_transaction_details"] = idle_in_transaction_details
                collected_stats["active_query_wait_events"] = active_query_details

                # 3. Fetch other stats
                try:
                    collected_stats["session_manager_stats"] = self.validator.database_manager.get_session_stats()
                except Exception as e:
                    collected_stats["session_manager_stats"] = f"[Error fetching session manager stats: {type(e).__name__}]"

                lock_details_query = """
                SELECT
                    activity.pid,
                    activity.usename,
                    activity.query,
                    blocking_locks.locktype AS blocking_locktype,
                    blocking_activity.query AS blocking_query,
                    blocking_activity.pid AS blocking_pid,
                    blocking_activity.usename AS blocking_usename,
                    age(now(), activity.query_start) as query_age
                FROM pg_stat_activity AS activity
                JOIN pg_locks AS blocking_locks ON blocking_locks.pid = activity.pid AND NOT blocking_locks.granted
                JOIN pg_locks AS granted_locks ON granted_locks.locktype = blocking_locks.locktype AND granted_locks.pid != activity.pid AND granted_locks.granted
                JOIN pg_stat_activity AS blocking_activity ON blocking_activity.pid = granted_locks.pid
                WHERE activity.wait_event_type = 'Lock';
                """
                try:
                    collected_stats["lock_details"] = await self.validator.database_manager.fetch_all(lock_details_query, timeout=45.0)
                except Exception as e:
                    if "DatabaseTimeout" in str(type(e)):
                        collected_stats["lock_details"] = "[Query Timed Out]"
                    else:
                        collected_stats["lock_details"] = f"[Query Error: {type(e).__name__}]"

            except Exception as e_outer:
                collected_stats["error"] = f"Outer error in database_monitor: {str(e_outer)}"
                logger.error(f"[DB Monitor] Outer error: {e_outer}", exc_info=True)

            await self.validator.database_stats._log_and_store_db_stats(collected_stats)
            gc.collect()

    async def memory_snapshot_taker(self):
        """Periodically takes memory snapshots and logs differences."""
        logger.info("Starting memory snapshot taker task...")
        
        snapshot_interval_seconds = 300 # 5 minutes
        logger.info(f"Memory snapshots will be taken every {snapshot_interval_seconds} seconds.")

        while not self.validator._shutdown_event.is_set():
            try:
                await asyncio.sleep(snapshot_interval_seconds)
                if self.validator._shutdown_event.is_set():
                    break

                logger.info("--- Taking Tracemalloc Snapshot ---")
                current_snapshot = tracemalloc.take_snapshot()
                
                logger.info("Top 10 current memory allocations (by line number):")
                for stat in current_snapshot.statistics('lineno')[:10]:
                    logger.info(f"  {stat}")

                if self.tracemalloc_snapshot1:
                    logger.info("Comparing to previous snapshot...")
                    top_stats = current_snapshot.compare_to(self.tracemalloc_snapshot1, 'lineno')
                    logger.info("Top 10 memory differences since last snapshot:")
                    for stat in top_stats[:10]:
                        logger.info(f"  {stat}")
                
                self.tracemalloc_snapshot1 = current_snapshot
                logger.info("--- Tracemalloc Snapshot Processed ---")

            except asyncio.CancelledError:
                logger.info("Memory snapshot taker task cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in memory_snapshot_taker: {e}", exc_info=True)
                await asyncio.sleep(60) # Wait a bit before retrying if an error occurs

    async def check_for_updates(self):
        """Check for and apply updates every 5 minutes."""
        while True:
            try:
                logger.info("Checking for updates...")
                # Add timeout to prevent hanging
                try:
                    update_successful = await asyncio.wait_for(
                        perform_update(self.validator),
                        timeout=180  # 3 minute timeout to allow for cleanup and restart
                    )

                    if update_successful:
                        logger.info("Update completed successfully")
                    else:
                        logger.debug("No updates available or update failed")

                except asyncio.TimeoutError:
                    logger.warning("Update check timed out after 3 minutes")
                except Exception as e:
                    if "500" in str(e):
                        logger.warning(f"GitHub temporarily unavailable (500 error): {e}")
                    else:
                        logger.error(f"Error in update checker: {e}")
                        logger.error(traceback.format_exc())

            except Exception as outer_e:
                logger.error(f"Outer error in update checker: {outer_e}")
                logger.error(traceback.format_exc())

            await asyncio.sleep(300)  # Check every 5 minutes

    async def plot_database_metrics_periodically(self):
        """Periodically generates and saves database metrics plots."""
        while not self.validator._shutdown_event.is_set():
            await asyncio.sleep(20 * 60) # Plot every 20 minutes
            logger.info("Requesting database performance plot generation...")
            history_copy_for_plot = []
            async with self.db_monitor_history_lock:
                if not self.db_monitor_history or len(self.db_monitor_history) < 2:
                    logger.info("Not enough data in db_monitor_history to generate plots. Skipping this cycle.")
                    continue
                history_copy_for_plot = list(self.db_monitor_history) 
            
            if not history_copy_for_plot:
                 logger.info("History copy for plotting is empty, skipping plot generation.")
                 continue

            try:
                # Offload the plotting to the synchronous helper method in an executor thread
                await asyncio.to_thread(self.validator.database_stats._generate_and_save_plot_sync, history_copy_for_plot)
            except Exception as e:
                logger.error(f"Error occurred when calling plot generation in executor: {e}")
                logger.error(traceback.format_exc())

    def _comprehensive_memory_cleanup(self, context: str = "general"):
        """
        Comprehensive memory cleanup that goes beyond standard garbage collection.
        Targets C extension memory, file handles, module caches, and other persistent references.
        Made more conservative to avoid interfering with running tasks.
        """
        
        # Skip cleanup during early startup to prevent initialization issues
        if not hasattr(self.validator, 'substrate_manager') or self.validator.substrate_manager is None:
            logger.debug(f"Skipping comprehensive cleanup for {context} - validator not fully initialized")
            return 0
            
        logger.info(f"Starting comprehensive memory cleanup for context: {context}")
        try:
            initial_memory = self._get_memory_usage()
        except Exception:
            # Fallback if memory monitoring isn't available yet
            initial_memory = 0
        
        try:
            # 1. Clear exception tracebacks (major memory holder)
            import sys
            sys.last_traceback = None
            sys.last_type = None 
            sys.last_value = None
            
            # 2. Clear module-level caches that accumulate over time (more conservative)
            try:
                # Only clear specific known-safe cache attributes
                for module_name in list(sys.modules.keys()):
                    if any(pattern in module_name.lower() for pattern in 
                           ['substrate', 'scalecodec', 'scale_info', 'metadata']):
                        module = sys.modules.get(module_name)
                        if hasattr(module, '__dict__'):
                            # Only clear known safe cache attributes, avoid _registry which might be critical
                            for attr_name in list(module.__dict__.keys()):
                                if attr_name in ['_cache', '__pycache__', '_instance_cache']:
                                    try:
                                        cache_obj = getattr(module, attr_name)
                                        if hasattr(cache_obj, 'clear') and isinstance(cache_obj, dict):
                                            cache_obj.clear()
                                    except Exception:
                                        pass
            except Exception as e:
                logger.debug(f"Error clearing module caches: {e}")
            
            # 3. Clear numpy/xarray memory more aggressively
            try:
                try:
                    import numpy as np
                    # Force numpy to release cached memory
                    if hasattr(np, '_get_ndarray_cache'):
                        np._get_ndarray_cache().clear()
                except ImportError:
                    pass
                    
                # Clear any xarray caches
                try:
                    import xarray as xr
                    if hasattr(xr.backends, 'plugins') and hasattr(xr.backends.plugins, 'clear'):
                        xr.backends.plugins.clear()
                except ImportError:
                    pass
            except Exception as e:
                logger.debug(f"Error clearing numpy/xarray caches: {e}")
            
            # 4. Clear HTTP client caches (only if they exist)
            try:
                if hasattr(self.validator, 'miner_client') and getattr(self.validator, 'miner_client', None):
                    # Clear any response caches
                    if hasattr(self.validator.miner_client, '_transport'):
                        transport = self.validator.miner_client._transport
                        if hasattr(transport, '_pool'):
                            transport._pool.clear()
            except Exception as e:
                logger.debug(f"Error clearing HTTP client caches: {e}")
            
            # 5. Clear database connection pool caches (only if fully initialized)
            try:
                if hasattr(self.validator, 'database_manager') and getattr(self.validator, 'database_manager', None):
                    # Force database manager to clear any cached connections/results
                    if hasattr(self.validator.database_manager, 'pool') and getattr(self.validator.database_manager, 'pool', None):
                        # Clear any cached query results or metadata
                        pool = self.validator.database_manager.pool
                        if hasattr(pool, '_queue') and hasattr(pool._queue, 'empty'):
                            # Clear connection pool queue
                            try:
                                while not pool._queue.empty():
                                    conn = pool._queue.get_nowait()
                                    if hasattr(conn, 'invalidate'):
                                        conn.invalidate()
                            except:
                                pass  # Queue might be empty or connection pool not ready
            except Exception as e:
                logger.debug(f"Error clearing database caches: {e}")
            
            # 6. Force multiple garbage collection passes with different strategies
            import gc
            collected_total = 0
            
            # First pass: standard collection
            collected = gc.collect()
            collected_total += collected
            
            # Second pass: collect generation 0 (youngest objects)
            collected = gc.collect(0)
            collected_total += collected
            
            # Third pass: collect all generations
            for generation in range(3):
                collected = gc.collect(generation)
                collected_total += collected
            
            try:
                final_memory = self._get_memory_usage()
                memory_freed = initial_memory - final_memory
            except Exception:
                # Fallback if memory monitoring fails
                final_memory = 0
                memory_freed = 0
            
            logger.info(f"Comprehensive cleanup for {context}: "
                       f"freed {memory_freed:.1f}MB, collected {collected_total} objects")
            
            return memory_freed
            
        except Exception as e:
            logger.error(f"Error during comprehensive memory cleanup: {e}")
            return 0

    def _get_memory_usage(self):
        """Get current memory usage in MB."""
        if PSUTIL_AVAILABLE and psutil is not None:
            try:
                process = psutil.Process()
                return process.memory_info().rss / (1024 * 1024)
            except Exception:
                return 0
        return 0