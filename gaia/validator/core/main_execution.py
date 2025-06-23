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

from gaia.validator.database.utils import handle_db_wipe

logger = get_logger(__name__)


class MainExecutor:
    """Handles the main execution loop and orchestration for the validator."""
    
    def __init__(self, validator_instance):
        self.validator = validator_instance
        self._shutdown_event = validator_instance._shutdown_event
        self._cleanup_done = validator_instance._cleanup_done
        self.memray_tracker = getattr(validator_instance, 'memray_tracker', None)

    async def main(self):
        """Main execution loop for the validator."""

        memray_active = False
        memray_output_file_path = "validator_memray_output.bin" 

        if os.getenv("ENABLE_MEMRAY_TRACKING", "false").lower() == "true":
            try:
                # memray is already imported at the top
                logger.info(f"Programmatic Memray tracking enabled. Output will be saved to: {memray_output_file_path}")
                self.memray_tracker = memray.Tracker(
                    destination=memray.FileDestination(path=memray_output_file_path, overwrite=True),
                    native_traces=True 
                )
                memray_active = True
            except ImportError: # Should not happen if import is at top, but good for safety
                logger.warning("Memray library seemed to be missing despite top-level import. Programmatic Memray tracking is disabled.")
            except Exception as e:
                logger.error(f"Failed to initialize Memray tracker: {e}")
                self.memray_tracker = None # Ensure it's None if init fails
        
        async def run_validator_logic():
            # Suppress gcsfs/aiohttp cleanup warnings that can block PM2 restart
            def custom_excepthook(exc_type, exc_value, exc_traceback):
                # Suppress specific gcsfs/aiohttp cleanup errors
                if (exc_type == RuntimeWarning and 
                    ('coroutine' in str(exc_value) and 'never awaited' in str(exc_value)) or
                    ('Non-thread-safe operation' in str(exc_value))):
                    return  # Silently ignore these warnings
                # Call the default handler for other exceptions
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
            
            sys.excepthook = custom_excepthook

            # --- Alembic check removed from here ---
            #test
            try:
                logger.info("Setting up neuron...")
                if not self.validator.setup_neuron():
                    logger.error("Failed to setup neuron, exiting...")
                    return

                logger.info("Neuron setup complete.")

                logger.info("Checking metagraph initialization...")
                if self.validator.metagraph is None:
                    logger.error("Metagraph not initialized, exiting...")
                    return

                logger.info("Metagraph initialized.")

                logger.info("Initializing database connection...") 
                await self.validator.database_manager.initialize_database()
                logger.info("Database tables initialized.")
                
                # Initialize DB Sync Components - AFTER DB init
                await self.validator._initialize_db_sync_components()

                #logger.warning(" CHECKING FOR DATABASE WIPE TRIGGER ")
                await handle_db_wipe(self.validator.database_manager)
                
                # Perform startup history cleanup AFTER db init and wipe check
                await self.validator.cleanup_stale_history_on_startup()

                # Lock storage to prevent any writes
                self.validator.database_manager._storage_locked = False
                if self.validator.database_manager._storage_locked:
                    logger.warning("Database storage is locked - no data will be stored until manually unlocked")

                logger.info("Checking HTTP clients...")
                # Only create clients if they don't exist or are closed
                if not hasattr(self.validator, 'miner_client') or self.validator.miner_client.is_closed:
                    self.validator.miner_client = httpx.AsyncClient(
                        timeout=30.0, follow_redirects=True, verify=False
                    )
                    logger.info("Created new miner client")
                if not hasattr(self.validator, 'api_client') or self.validator.api_client.is_closed:
                    self.validator.api_client = httpx.AsyncClient(
                        timeout=30.0,
                        follow_redirects=True,
                        limits=httpx.Limits(
                            max_connections=100,
                            max_keepalive_connections=20,
                            keepalive_expiry=30,
                        ),
                        transport=httpx.AsyncHTTPTransport(retries=3),
                    )
                    logger.info("Created new API client")
                logger.info("HTTP clients ready.")

                logger.info("Starting watchdog...")
                await self.validator.start_watchdog()
                logger.info("Watchdog started.")

                if not memray_active: # Start tracemalloc only if memray is not active
                    logger.info("Starting tracemalloc for memory analysis...")
                    tracemalloc.start(25) # Start tracemalloc, 25 frames for traceback
                
                logger.info("Initializing baseline models...")
                await self.validator.basemodel_evaluator.initialize_models()
                logger.info("Baseline models initialization complete")
                
                # Start auto-updater as independent task (not in main loop to avoid self-cancellation)
                logger.info("Starting independent auto-updater task...")
                auto_updater_task = asyncio.create_task(self.validator.check_for_updates())
                logger.info("Auto-updater task started independently")
                
                tasks_lambdas = [ # Renamed to avoid conflict if tasks variable is used elsewhere
                    lambda: self.validator.geomagnetic_task.validator_execute(self.validator),
                    lambda: self.validator.soil_task.validator_execute(self.validator),
                    lambda: self.validator.weather_task.validator_execute(self.validator),
                    lambda: self.validator.status_logger(),
                    lambda: self.validator.main_scoring(),
                    lambda: self.validator.handle_miner_deregistration_loop(),
                    # The MinerScoreSender task will be added conditionally below
                    lambda: self.validator.manage_earthdata_token(),
                    lambda: self.validator.monitor_client_health(),  # Added HTTP client monitoring
                    #lambda: self.validator.database_monitor(),
                    lambda: self.validator.periodic_substrate_cleanup(),  # Added substrate cleanup task
                    lambda: self.validator.aggressive_memory_cleanup(),  # Added aggressive memory cleanup task
                    #lambda: self.validator.plot_database_metrics_periodically() # Added plotting task
                ]
                if not memray_active: # Add tracemalloc snapshot taker only if memray is not active
                    tasks_lambdas.append(lambda: self.validator.memory_snapshot_taker())


                # Add DB Sync tasks conditionally
                if self.validator.auto_sync_manager:
                    logger.info(f"AutoSyncManager is active - Starting setup and scheduling...")
                    logger.info(f"DB Sync Configuration: Primary={self.validator.is_source_validator_for_db_sync}")
                    
                    # Setup AutoSyncManager (includes system configuration AND scheduling)
                    try:
                        logger.info("🚀 Setting up AutoSyncManager (includes system config and scheduling)...")
                        setup_success = await self.validator.auto_sync_manager.setup()
                        if setup_success:
                            logger.info("✅ AutoSyncManager setup and scheduling completed successfully!")
                        else:
                            logger.warning("⚠️ AutoSyncManager setup failed - attempting fallback scheduling for basic monitoring...")
                            # If setup failed, try just starting scheduling for monitoring
                            try:
                                await self.validator.auto_sync_manager.start_scheduling()
                                logger.info("✅ AutoSyncManager fallback scheduling started successfully!")
                            except Exception as fallback_e:
                                logger.error(f"❌ AutoSyncManager fallback scheduling also failed: {fallback_e}")
                                self.validator.auto_sync_manager = None
                    except Exception as e:
                        logger.error(f"❌ AutoSyncManager setup failed with exception: {e}")
                        logger.info("🔄 Attempting fallback scheduling for basic monitoring...")
                        # If setup completely failed, try just starting scheduling
                        try:
                            await self.validator.auto_sync_manager.start_scheduling()
                            logger.info("✅ AutoSyncManager fallback scheduling started successfully!")
                        except Exception as fallback_e:
                            logger.error(f"❌ AutoSyncManager fallback scheduling also failed: {fallback_e}")
                            logger.error("🚫 AutoSyncManager will be completely disabled")
                            self.validator.auto_sync_manager = None
                else:
                    logger.info("AutoSyncManager is not active for this node (initialization failed or not configured).")

                
                # Conditionally add miner_score_sender task
                score_sender_on_str = os.getenv("SCORE_SENDER_ON", "False")
                if score_sender_on_str.lower() == "true":
                    logger.info("SCORE_SENDER_ON is True, enabling MinerScoreSender task.")
                    tasks_lambdas.insert(5, lambda: self.validator.miner_score_sender.run_async())

                active_service_tasks = []  # Define here for access in except CancelledError
                shutdown_waiter = None # Define here for access in except CancelledError
                try:
                    logger.info(f"Creating {len(tasks_lambdas)} main service tasks...")
                    active_service_tasks = [asyncio.create_task(t()) for t in tasks_lambdas]
                    logger.info(f"All {len(active_service_tasks)} main service tasks created.")

                    shutdown_waiter = asyncio.create_task(self._shutdown_event.wait())
                    
                    # Tasks to monitor are all service tasks plus the shutdown_waiter
                    all_tasks_being_monitored = active_service_tasks + [shutdown_waiter]

                    while not self._shutdown_event.is_set():
                        # Filter out already completed tasks from the list we pass to asyncio.wait
                        current_wait_list = [t for t in all_tasks_being_monitored if not t.done()]
                        
                        if not current_wait_list: 
                            # This means all tasks (services + shutdown_waiter) are done.
                            logger.info("All monitored tasks have completed.")
                            if not self._shutdown_event.is_set():
                                 logger.warning("All tasks completed but shutdown event was not explicitly set. Setting it now to ensure proper cleanup.")
                                 self._shutdown_event.set() # Ensure shutdown is triggered
                            break # Exit the while loop

                        done, pending = await asyncio.wait(
                            current_wait_list,
                            return_when=asyncio.FIRST_COMPLETED
                        )
                        
                        # If shutdown_event is set (e.g. by signal handler) or shutdown_waiter completed, break the loop.
                        if self._shutdown_event.is_set() or shutdown_waiter.done():
                            logger.info("Shutdown signaled or shutdown_waiter completed. Breaking main monitoring loop.")
                            break 

                        # If we are here, one of the active_service_tasks completed. Log it.
                        for task in done:
                            if task in active_service_tasks: # Check if it's one of our main service tasks
                                try:
                                    result = task.result() # Access result to raise exception if task failed
                                    logger.warning(f"Main service task {task.get_name()} completed unexpectedly with result: {result}. It will not be automatically restarted by this loop.")
                                except asyncio.CancelledError:
                                    logger.info(f"Main service task {task.get_name()} was cancelled.")
                                except Exception as e:
                                    logger.error(f"Main service task {task.get_name()} failed with exception: {e}", exc_info=True)
                    
                    # --- After the while loop (either by break or _shutdown_event being set before loop start) ---
                    logger.info("Main monitoring loop finished. Initiating cancellation of any remaining active tasks for shutdown.")
                    
                    # Cancel all original service tasks if not already done
                    for task_to_cancel in active_service_tasks:
                        if not task_to_cancel.done():
                            logger.info(f"Cancelling service task: {task_to_cancel.get_name()}")
                            task_to_cancel.cancel()
                    
                    # Cancel shutdown_waiter if not done
                    # (e.g., if loop broke because all service tasks finished before shutdown_event was set)
                    if shutdown_waiter and not shutdown_waiter.done():
                        logger.info("Cancelling shutdown_waiter task.")
                        shutdown_waiter.cancel()
                    
                    # Await all of them to ensure they are properly cleaned up
                    await asyncio.gather(*(active_service_tasks + ([shutdown_waiter] if shutdown_waiter else [])), return_exceptions=True)
                    logger.info("All main service tasks and the shutdown waiter have been processed (awaited/cancelled).")
                    
                except asyncio.CancelledError:
                    logger.info("Main task execution block was cancelled. Ensuring child tasks are also cancelled.")
                    # This block handles if validator.main() itself is cancelled from outside.
                    tasks_to_ensure_cancelled = []
                    if 'active_service_tasks' in locals(): # Check if list was initialized
                        tasks_to_ensure_cancelled.extend(active_service_tasks)
                    if 'shutdown_waiter' in locals() and shutdown_waiter: # Check if waiter was initialized
                        tasks_to_ensure_cancelled.append(shutdown_waiter)
                    
                    for task_to_cancel in tasks_to_ensure_cancelled:
                        if task_to_cancel and not task_to_cancel.done():
                            logger.info(f"Cancelling task due to main cancellation: {task_to_cancel.get_name()}")
                            task_to_cancel.cancel()
                    await asyncio.gather(*tasks_to_ensure_cancelled, return_exceptions=True)
                    logger.info("Child tasks cancellation process completed due to main cancellation.")

            except Exception as e:
                logger.error(f"Error in main: {e}")
                logger.error(traceback.format_exc())
            finally:
                if not self._cleanup_done:
                    await self.validator._initiate_shutdown()

        if memray_active and self.memray_tracker:
            with self.memray_tracker: # This starts the tracking
                logger.info("Memray tracker is active and wrapping validator logic.")
                await run_validator_logic()
            logger.info(f"Memray tracking finished. Output file '{memray_output_file_path}' should be written.")
        else:
            logger.info("Memray tracking is not active. Running validator logic directly.")
            await run_validator_logic()

    async def status_logger(self):
        """Log the status of the validator periodically."""
        while True:
            try:
                current_time_utc = datetime.now(timezone.utc)
                formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S")

                try:
                    resp = self.validator.substrate.rpc_request("chain_getHeader", [])  
                    hex_num = resp["result"]["number"]
                    self.validator.current_block = int(hex_num, 16)
                    blocks_since_weights = (
                            self.validator.current_block - self.validator.last_set_weights_block
                    )
                except Exception as block_error:

                    try:
                        # Use managed substrate connection for status logger recovery
                        self.validator.substrate = self.validator.substrate_manager.force_reconnect()
                        logger.info("🔄 Status logger using managed fresh substrate connection")
                        
                        # Clear scalecodec caches after status logger reconnection
                        try:
                            import sys
                            import gc
                            cleared_count = 0
                            
                            for module_name in list(sys.modules.keys()):
                                if 'scalecodec' in module_name.lower() or 'substrate' in module_name.lower():
                                    module = sys.modules.get(module_name)
                                    if hasattr(module, '__dict__'):
                                        for attr_name in list(module.__dict__.keys()):
                                            if 'cache' in attr_name.lower() or 'registry' in attr_name.lower():
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
                                collected = gc.collect()
                                logger.debug(f"Status logger substrate cleanup: cleared {cleared_count} cache objects, GC collected {collected}")
                                
                        except Exception:
                            pass
                            
                    except Exception as e:
                        logger.error(f"Failed to reconnect to substrate: {e}")

                active_nodes = len(self.validator.metagraph.nodes) if self.validator.metagraph else 0

                # Substrate manager enabled with fresh connections
                substrate_stats = f"Substrate Manager: ENABLED ({self.validator.substrate_manager.get_stats()['connection_count']} connections created)" if self.validator.substrate_manager else "Substrate Manager: NOT INITIALIZED"

                logger.info(
                    f"\n"
                    f"---Status Update ---\n"
                    f"Time (UTC): {formatted_time} | \n"
                    f"Block: {self.validator.current_block} | \n"
                    f"Nodes: {active_nodes}/256 | \n"
                    f"Weights Set: {blocks_since_weights} blocks ago\n"
                    f"{substrate_stats}"
                )

            except Exception as e:
                logger.error(f"Error in status logger: {e}")
                logger.error(f"{traceback.format_exc()}")
            finally:
                await asyncio.sleep(60)

    async def check_for_updates(self):
        """Check for updates and handle auto-restart if enabled."""
        while True:
            try:
                # Only run auto-updater checks in production (not test mode)
                if not self.validator.args.test:
                    auto_update_enabled = os.getenv("ENABLE_AUTO_UPDATE", "false").lower() == "true"
                    
                    if auto_update_enabled:
                        logger.info("Auto-update is enabled. Checking for updates...")
                        try:
                            # Add update checking logic here
                            # For now, we'll just log the check
                            logger.info("No update mechanism implemented yet")
                            
                        except Exception as e:
                            logger.error(f"Error checking for updates: {e}")
                    else:
                        logger.debug("Auto-update is disabled. Skipping update check.")
                else:
                    logger.debug("Test mode active, auto-updater disabled")
                    
                # Check every 6 hours
                await asyncio.sleep(6 * 3600)
                
            except asyncio.CancelledError:
                logger.info("Auto-updater task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in auto-updater: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying on error

    async def manage_earthdata_token(self):
        """Periodically checks and refreshes the Earthdata token."""
        logger.info("🌍 Earthdata token management task started - running initial check immediately...")
        
        while not self._shutdown_event.is_set():
            try:
                logger.info("🔍 Running Earthdata token check...")
                # Import here to avoid circular imports
                from gaia.tasks.defined_tasks.weather.utils.earthdata_auth import ensure_valid_earthdata_token
                
                token = await ensure_valid_earthdata_token()
                if token:
                    logger.info(f"✅ Earthdata token check successful. Current token (first 10 chars): {token[:10]}...")
                else:
                    logger.warning("⚠️ Earthdata token check failed or no token available.")

                logger.info("⏰ Earthdata token check complete. Sleeping for 24 hours until next check...")
                await asyncio.sleep(86400) # Check daily

            except asyncio.CancelledError:
                logger.info("🛑 Earthdata token management task cancelled.")
                break
            except Exception as e:
                logger.error(f"❌ Error in Earthdata token management task: {e}", exc_info=True)
                logger.info("🔄 Retrying Earthdata token check in 1 hour due to error...")
                await asyncio.sleep(3600) # Retry in an hour if there was an error 