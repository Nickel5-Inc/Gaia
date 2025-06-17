"""
Database Statistics module for the Gaia validator.

This module contains database monitoring, statistics collection,
and performance tracking functionality.
"""

import asyncio
import gc
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


class DatabaseStatsManager:
    """
    Manages database statistics collection and monitoring.
    
    Handles database performance monitoring, statistics collection,
    and plot generation for the validator.
    """
    
    def __init__(self, database_manager):
        """Initialize the database stats manager."""
        self.database_manager = database_manager
        
        # For database monitor plotting
        self.db_monitor_history = []
        self.db_monitor_history_lock = asyncio.Lock()
        self.DB_MONITOR_HISTORY_MAX_SIZE = 120 # e.g., 2 hours of data if monitor runs every minute

    def _process_activity_snapshot(self, activity_snapshot: List[Any]) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
        """Process database activity snapshots."""
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
                query_start = row.get('query_start')
                if query_start and isinstance(query_start, datetime):
                    row['query_age'] = datetime.now(timezone.utc) - query_start
                else:
                    row['query_age'] = timedelta(0)
                active_query_details.append(row)

        idle_in_transaction_details.sort(key=lambda r: r.get('state_change') or datetime.min.replace(tzinfo=timezone.utc))
        active_query_details.sort(key=lambda r: r.get('query_start') or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        
        summary_list = [{"state": s if s is not None else "null", "count": c} for s, c in summary.items()]
        
        return summary_list, null_state_details, idle_in_transaction_details, active_query_details

    async def _log_and_store_db_stats(self, collected_stats: dict):
        """Log and store database statistics."""
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

    def _generate_and_save_plot_sync(self, history_copy: List[Dict]):
        """Generate and save database performance plots."""
        try:
            try:
                import matplotlib
                matplotlib.use('Agg') # Use Agg backend for non-interactive plotting
                import matplotlib.pyplot as plt
                import matplotlib.dates as mdates
            except ImportError as e:
                logger.error(f"Matplotlib import error in sync plot generation: {e}. Plotting will be disabled for this cycle.")
                return
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
                timestamp = record.get("timestamp")
                if not timestamp or not isinstance(timestamp, str):
                    continue
                    
                ts = datetime.fromisoformat(timestamp)
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

        try:
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
            import numpy as np

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
                gc.collect()
        except ImportError as e:
            logger.error(f"Error importing plotting dependencies: {e}")
        except Exception as e:
            logger.error(f"Error generating plot: {e}")
            import traceback
            logger.error(traceback.format_exc())