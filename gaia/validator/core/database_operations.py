import gc
import logging
import sys
from datetime import datetime, timezone, timedelta
import os
import time
import threading
import concurrent.futures
import glob
import signal
import tracemalloc
import memray
import asyncio
import traceback
import math

from gaia.database.database_manager import DatabaseTimeout
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None 
    PSUTIL_AVAILABLE = False
    print("psutil not found, memory logging will be skipped.")

import ssl
import random
from typing import Any, Optional, List, Dict, Set
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import httpx
from fiber.chain import chain_utils, interface
from fiber.chain import weights as w
from fiber.chain.fetch_nodes import get_nodes_for_netuid
from fiber.chain.chain_utils import query_substrate
from fiber.logging_utils import get_logger
from fiber.encrypted.validator import client as vali_client, handshake
from fiber.chain.metagraph import Metagraph
from fiber.chain.interface import get_substrate
from substrateinterface import SubstrateInterface
from argparse import ArgumentParser
import pandas as pd
import json
import base64
import numpy as np
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

logger = get_logger(__name__)


class DatabaseOperationsManager:
    """Manages database operations, monitoring, and scoring for the validator."""
    
    def __init__(self, validator_instance):
        self.validator = validator_instance
        self.database_manager = validator_instance.database_manager
        self._shutdown_event = validator_instance._shutdown_event
        self.db_monitor_history = validator_instance.db_monitor_history
        self.db_monitor_history_lock = validator_instance.db_monitor_history_lock
        self.DB_MONITOR_HISTORY_MAX_SIZE = validator_instance.DB_MONITOR_HISTORY_MAX_SIZE
        self.task_weight_schedule = validator_instance.task_weight_schedule

    async def database_monitor(self):
        """Periodically query and log database statistics from a consistent snapshot."""
        logger.info("Starting database monitor task...")
        while not self._shutdown_event.is_set():
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
                    activity_snapshot = await self.database_manager.fetch_all(activity_query, timeout=45.0)
                except DatabaseTimeout:
                    collected_stats["error"] = "Timeout fetching pg_stat_activity snapshot."
                    logger.warning(f"[DB Monitor] {collected_stats['error']}")
                except Exception as e:
                    collected_stats["error"] = f"Error fetching pg_stat_activity snapshot: {type(e).__name__}"
                    logger.warning(f"[DB Monitor] {collected_stats['error']} - {e}")

                if collected_stats["error"]:
                    await self._log_and_store_db_stats(collected_stats)
                    continue

                # 2. Process the snapshot in memory
                summary, null_state_details, idle_in_transaction_details, active_query_details = self._process_activity_snapshot(activity_snapshot)
                
                collected_stats["connection_summary"] = summary
                collected_stats["null_state_connection_details"] = null_state_details
                collected_stats["idle_in_transaction_details"] = idle_in_transaction_details
                collected_stats["active_query_wait_events"] = active_query_details

                # 3. Fetch other stats
                try:
                    collected_stats["session_manager_stats"] = self.database_manager.get_session_stats()
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
                    collected_stats["lock_details"] = await self.database_manager.fetch_all(lock_details_query, timeout=45.0)
                except DatabaseTimeout:
                    collected_stats["lock_details"] = "[Query Timed Out]"
                except Exception as e:
                    collected_stats["lock_details"] = f"[Query Error: {type(e).__name__}]"

            except Exception as e_outer:
                collected_stats["error"] = f"Outer error in database_monitor: {str(e_outer)}"
                logger.error(f"[DB Monitor] Outer error: {e_outer}", exc_info=True)

            await self._log_and_store_db_stats(collected_stats)
            gc.collect()

    def _process_activity_snapshot(self, activity_snapshot):
        summary = {}
        null_state_details = []
        idle_in_transaction_details = []
        active_query_details = []
        
        # Note: To perfectly exclude the monitor's own query, another query for its pid would be needed.
        # This implementation omits that for simplicity, so the monitor's query may appear in active queries.
        
        for row_proxy in activity_snapshot:
            row = dict(row_proxy)
            state = row.get('state')
            summary[state] = summary.get(state, 0) + 1

            if state is None:
                null_state_details.append(row)
            elif state == 'idle in transaction':
                idle_in_transaction_details.append(row)
            elif state == 'active' and row.get('backend_type') == 'client backend':
                if row.get('query_start'):
                    row['query_age'] = datetime.now(timezone.utc) - row.get('query_start')
                else:
                    row['query_age'] = timedelta(0)
                active_query_details.append(row)

        idle_in_transaction_details.sort(key=lambda r: r.get('state_change') or datetime.min.replace(tzinfo=timezone.utc))
        active_query_details.sort(key=lambda r: r.get('query_start') or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        
        summary_list = [{"state": s if s is not None else "null", "count": c} for s, c in summary.items()]
        
        return summary_list, null_state_details, idle_in_transaction_details, active_query_details

    async def _log_and_store_db_stats(self, collected_stats: dict):
        """Helper to store stats in history and log them."""
        async with self.db_monitor_history_lock:
            self.db_monitor_history.append(collected_stats)
            if len(self.db_monitor_history) > self.DB_MONITOR_HISTORY_MAX_SIZE:
                self.db_monitor_history.pop(0)

        log_output = "[DB Monitor] Stats:\n"
        for key, value in collected_stats.items():
            if key == "error" and value is None:
                continue
            try:
                title = key.replace('_', ' ').title()
                if isinstance(value, list):
                    log_output += f"  {title}:\n"
                    if not value:
                        log_output += "  []\n"
                    else:
                        # Show up to 5 entries before truncating for brevity
                        for i, item in enumerate(value[:5]):
                            item_dict = dict(item)
                            log_output += "  {\n"
                            for k, v in item_dict.items():
                                v_str = str(v)
                                if isinstance(v, datetime):
                                    v_str = v.isoformat()
                                elif isinstance(v, timedelta):
                                    v_str = str(v)
                                log_output += f"    {k}: {v_str}\n"
                            log_output += "  }\n"
                        if len(value) > 5:
                            log_output += f"  ... and {len(value) - 5} more ...\n"
                else:
                    log_output += f"  {title}: {json.dumps(value, indent=2, default=str)}\n"
            except Exception as e_log:
                log_output += f"  Error formatting log for {key}: {e_log}\n"
        
        logger.info(log_output)

    def _generate_and_save_plot_sync(self, history_copy):
        """Synchronous helper to generate and save database metrics plots."""
        try:
            import matplotlib
            matplotlib.use('Agg') # Use Agg backend for non-interactive plotting
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
        except ImportError as e:
            logger.error(f"Matplotlib import error in sync plot generation: {e}. Plotting will be disabled for this cycle.")
            return

        if not history_copy or len(history_copy) < 2:
            logger.info("Not enough data in history_copy for sync plot generation. Skipping.")
            return

        timestamps = []
        avg_session_times = []
        min_session_times = []
        max_session_times = []
        total_connections_list = []

        for record in history_copy:
            try:
                ts = datetime.fromisoformat(record.get("timestamp"))
                timestamps.append(ts)

                session_stats = record.get("session_manager_stats", {})
                if isinstance(session_stats, dict):
                    avg_session_times.append(session_stats.get("avg_session_time_ms", float('nan')))
                    min_session_times.append(session_stats.get("min_session_time_ms", float('nan')))
                    max_session_times.append(session_stats.get("max_session_time_ms", float('nan')))
                else:
                    avg_session_times.append(float('nan'))
                    min_session_times.append(float('nan'))
                    max_session_times.append(float('nan'))

                connection_summary = record.get("connection_summary", [])
                current_total_connections = 0
                if isinstance(connection_summary, list):
                    for conn_info in connection_summary:
                        if isinstance(conn_info, dict) and "count" in conn_info and isinstance(conn_info["count"], (int, float)):
                            current_total_connections += conn_info["count"]
                total_connections_list.append(current_total_connections)
            except Exception as e:
                logger.warning(f"Skipping record in sync plot generation due to parsing error: {e} - Record: {record}")
                continue
        
        if not timestamps or len(timestamps) < 2:
            logger.info("Not enough valid data points for sync plot generation after parsing. Skipping.")
            return

        fig, axs = plt.subplots(2, 1, figsize=(15, 12), sharex=True)
        fig.suptitle('Database Performance Monitor', fontsize=16)

        axs[0].plot(timestamps, avg_session_times, label='Avg Session Time (ms)', marker='.', linestyle='-', color='blue')
        axs[0].plot(timestamps, min_session_times, label='Min Session Time (ms)', marker='.', linestyle=':', color='green')
        axs[0].plot(timestamps, max_session_times, label='Max Session Time (ms)', marker='.', linestyle=':', color='red')
        axs[0].set_ylabel('Session Time (ms)')
        axs[0].set_title('DB Session Durations')
        axs[0].legend()
        axs[0].grid(True)

        ax2 = axs[1]
        ax2.plot(timestamps, total_connections_list, label='Total Connections', marker='.', linestyle='-', color='purple')
        ax2.set_ylabel('Number of Connections')
        ax2.set_title('DB Connection Count')
        ax2.legend()
        ax2.grid(True)

        fig.autofmt_xdate()
        xfmt = mdates.DateFormatter('%Y-%m-%d\n%H:%M:%S')
        for ax_item in axs:
            ax_item.xaxis.set_major_formatter(xfmt)
        
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        plot_filename = "database_performance_plot.png"
        try:
            plt.savefig(plot_filename)
            logger.info(f"Database performance plot saved to {plot_filename}")
        except Exception as e_save:
            logger.error(f"Error saving plot to {plot_filename}: {e_save}")
        finally:
            plt.close(fig)
            # Explicitly trigger garbage collection after plotting if memory is a concern
            # However, plt.close(fig) should handle most of it.
            import gc
            gc.collect()

    async def plot_database_metrics_periodically(self):
        """Periodically generates and saves database metrics plots."""
        # Matplotlib imports are now inside _generate_and_save_plot_sync
        # to ensure they are only imported in the executor thread if needed.

        while not self._shutdown_event.is_set():
            await asyncio.sleep(20 * 60) # Plot every 20 minutes
            logger.info("Requesting database performance plot generation...")
            history_copy_for_plot = []
            async with self.db_monitor_history_lock:
                if not self.db_monitor_history or len(self.db_monitor_history) < 2:
                    logger.info("Not enough data in db_monitor_history to generate plots. Skipping this cycle.")
                    continue
                history_copy_for_plot = list(self.db_monitor_history) 
            
            if not history_copy_for_plot:
                 logger.info("History copy for plotting is empty, skipping plot generation.") # Should be caught by above check too
                 continue

            try:
                # Offload the plotting to the synchronous helper method in an executor thread
                await asyncio.to_thread(self._generate_and_save_plot_sync, history_copy_for_plot)
            except Exception as e:
                logger.error(f"Error occurred when calling plot generation in executor: {e}")
                logger.error(traceback.format_exc())
            # No finally gc.collect() here, it's in the sync method

    def get_current_task_weights(self) -> Dict[str, float]:
        now_utc = datetime.now(timezone.utc)
        
        # Default to the first set of weights in the schedule if current time is before any scheduled change
        # or if the schedule is somehow empty (though it's hardcoded not to be).
        active_weights = self.task_weight_schedule[0][1] 
        
        # Iterate through the schedule to find the latest applicable weights
        # The schedule is assumed to be sorted by datetime.
        for dt_threshold, weights_at_threshold in self.task_weight_schedule:
            if now_utc >= dt_threshold:
                active_weights = weights_at_threshold
            else:
                # Since the list is sorted, once we pass a threshold that's in the future,
                # the previously set active_weights are correct for the current time.
                break 
                
        # logger.debug(f"Using task weights for current time {now_utc.isoformat()}: {active_weights}")
        return active_weights.copy() # Return a copy to prevent modification of the schedule

    async def manage_earthdata_token(self):
        """Manage and refresh earthdata token for NASA data access."""
        while not self._shutdown_event.is_set():
            try:
                from gaia.validator.utils.earthdata_tokens import ensure_valid_earthdata_token
                ensure_valid_earthdata_token()
                logger.info("Earthdata token validation check completed successfully")
            except Exception as e:
                logger.error(f"Error during earthdata token validation: {e}")
            
            # Check every 12 hours
            await asyncio.sleep(12 * 3600)

    async def _initialize_db_sync_components(self):
        """Initialize database synchronization components using AutoSyncManager."""
        try:
            # Get the AutoSyncManager instance (singleton pattern)
            self.auto_sync_manager = get_auto_sync_manager()
            
            # Optionally configure it if needed
            # For now, the AutoSyncManager uses sensible defaults from environment
            # The database_manager and other dependencies are automatically passed
            
            logger.info("Database sync components initialized with AutoSyncManager")
            
        except Exception as e:
            logger.error(f"Failed to initialize database sync components: {e}")
            self.auto_sync_manager = None 