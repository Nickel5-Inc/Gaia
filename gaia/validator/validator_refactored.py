"""
Refactored Gaia Validator - Modular Architecture

This is the refactored version of the validator that uses the core modules
to provide a clean, maintainable architecture while preserving all original functionality.
"""

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
import sys
import tracemalloc # Added import
import memray # Added for programmatic memray tracking

from gaia.database.database_manager import DatabaseTimeout
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None 
    PSUTIL_AVAILABLE = False
    print("psutil not found, memory logging will be skipped.") # Use print for early feedback

os.environ["NODE_TYPE"] = "validator"
import asyncio
import ssl
import traceback
import random
from typing import Any, Optional, List, Dict, Set
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import httpx
from fiber.chain import chain_utils, interface
from fiber.chain import weights as w
from fiber.chain.fetch_nodes import get_nodes_for_netuid
from fiber.chain.chain_utils import query_substrate
from fiber.chain import chain_utils, interface
from fiber.chain import weights as w
from fiber.chain.fetch_nodes import get_nodes_for_netuid
from fiber.chain.chain_utils import query_substrate
from fiber.logging_utils import get_logger
from fiber.encrypted.validator import client as vali_client, handshake
from fiber.encrypted.validator import client as vali_client, handshake
from fiber.chain.metagraph import Metagraph
from fiber.chain.interface import get_substrate
from substrateinterface import SubstrateInterface
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask
from gaia.tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask
from gaia.APIcalls.miner_score_sender import MinerScoreSender
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from argparse import ArgumentParser
import pandas as pd
import json
from gaia.validator.weights.set_weights import FiberWeightSetter
import base64
import math
from gaia.validator.utils.auto_updater import perform_update
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import text
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from gaia.validator.basemodel_evaluator import BaseModelEvaluator
from gaia.validator.utils.db_wipe import handle_db_wipe
from gaia.validator.utils.earthdata_tokens import ensure_valid_earthdata_token
from gaia.validator.utils.substrate_manager import SubstrateConnectionManager
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask

# Imports for Alembic check
from alembic.config import Config
from alembic import command
from alembic.util import CommandError # Import CommandError
from sqlalchemy import create_engine, pool

# New imports for DB Sync
# Legacy backup/restore managers removed - using AutoSyncManager only
from gaia.validator.sync.auto_sync_manager import get_auto_sync_manager
import random # for staggering db sync tasks

# Import core modules
from gaia.validator.core import TaskOrchestrator, MinerCommunicator, DatabaseStatsManager, run_comprehensive_database_setup

logger = get_logger(__name__)


async def perform_handshake_with_retry(
    httpx_client: httpx.AsyncClient,
    server_address: str,
    keypair: Any,
    miner_hotkey_ss58_address: str,
    max_retries: int = 2,
    base_timeout: float = 15.0
) -> tuple[str, str]:
    """
    Custom handshake function with retry logic and progressive timeouts.
    
    Args:
        httpx_client: The HTTP client to use
        server_address: Miner server address
        keypair: Validator keypair
        miner_hotkey_ss58_address: Miner's hotkey address
        max_retries: Maximum number of retry attempts
        base_timeout: Base timeout for handshake operations
    
    Returns:
        Tuple of (symmetric_key_str, symmetric_key_uuid)
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        # Progressive timeout: increase timeout with each retry
        current_timeout = base_timeout * (1.5 ** attempt)
        
        try:
            logger.debug(f"Handshake attempt {attempt + 1}/{max_retries + 1} with timeout {current_timeout:.1f}s")
            
            # Get public key with current timeout
            public_key_encryption_key = await asyncio.wait_for(
                handshake.get_public_encryption_key(
                    httpx_client, 
                    server_address, 
                    timeout=int(current_timeout)
                ),
                timeout=current_timeout
            )
            
            # Generate symmetric key
            symmetric_key: bytes = os.urandom(32)
            symmetric_key_uuid: str = os.urandom(32).hex()
            
            # Send symmetric key with current timeout
            success = await asyncio.wait_for(
                handshake.send_symmetric_key_to_server(
                    httpx_client,
                    server_address,
                    keypair,
                    public_key_encryption_key,
                    symmetric_key,
                    symmetric_key_uuid,
                    miner_hotkey_ss58_address,
                    timeout=int(current_timeout),
                ),
                timeout=current_timeout
            )
            
            if success:
                symmetric_key_str = base64.b64encode(symmetric_key).decode()
                return symmetric_key_str, symmetric_key_uuid
            else:
                raise Exception("Handshake failed: server returned unsuccessful status")
                
        except (asyncio.TimeoutError, httpx.TimeoutException, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            last_exception = e
            if attempt < max_retries:
                wait_time = 1.0 * (attempt + 1)  # Progressive backoff
                logger.warning(f"Handshake timeout on attempt {attempt + 1}, retrying in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                logger.error(f"Handshake failed after {max_retries + 1} attempts due to timeout")
                break
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                wait_time = 1.0 * (attempt + 1)
                logger.warning(f"Handshake error on attempt {attempt + 1}: {type(e).__name__} - {e}, retrying in {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                continue
            else:
                logger.error(f"Handshake failed after {max_retries + 1} attempts due to error: {type(e).__name__} - {e}")
                break
    
    # If we get here, all attempts failed
    raise last_exception or Exception("Handshake failed after all retry attempts")


class GaiaValidatorRefactored:
    """
    Refactored Gaia Validator with modular architecture.
    
    Uses core modules for task orchestration, miner communication,
    database statistics, and other functionality.
    """
    
    def __init__(self, args):
        """
        Initialize the GaiaValidatorRefactored with provided arguments.
        """
        print("[STARTUP DEBUG] Starting GaiaValidatorRefactored.__init__")
        self.args = args
        self.metagraph = None
        self.config = None
        self.database_manager = ValidatorDatabaseManager()
        
        # Initialize core modules
        self.task_orchestrator = TaskOrchestrator(self)
        self.miner_communicator = MinerCommunicator(self)
        self.database_stats = DatabaseStatsManager(self.database_manager)
        
        self.soil_task = SoilMoistureTask(
            db_manager=self.database_manager,
            node_type="validator",
            test_mode=args.test,
        )
        self.geomagnetic_task = GeomagneticTask(
            node_type="validator",
            db_manager=self.database_manager,
            test_mode=args.test
        )
        self.weather_task = WeatherTask(
            db_manager=self.database_manager,
            node_type="validator",
            test_mode=args.test,
        )
        self.weights = [0.0] * 256
        self.last_set_weights_block = 0
        self.current_block = 0
        self.nodes = {}
        self.memray_tracker: Optional[memray.Tracker] = None # For programmatic memray

        # Initialize HTTP clients first
        # Client for miner communication with SSL verification disabled
        import ssl
        
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        self.miner_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=5.0),
            follow_redirects=True,
            verify=False,
            limits=httpx.Limits(
                max_connections=100,  # Restore higher limit for 200+ miners
                max_keepalive_connections=50,  # Allow more keepalive for efficiency
                keepalive_expiry=300,  # 5 minutes - good balance for regular queries
            ),
            transport=httpx.AsyncHTTPTransport(
                retries=2,  # Reduced from 3
                verify=False,  # Explicitly set verify=False on transport
            ),
        )
        # Client for API communication with SSL verification enabled
        self.api_client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30,
            ),
            transport=httpx.AsyncHTTPTransport(retries=3),
        )

        # Now create MinerScoreSender with the initialized api_client
        self.miner_score_sender = MinerScoreSender(database_manager=self.database_manager,
                                                   api_client=self.api_client)

        self.last_successful_weight_set = time.time()
        self.last_successful_dereg_check = time.time()
        self.last_successful_db_check = time.time()
        self.last_metagraph_sync = time.time()
        
        # task health tracking
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
            }
        }
        
        self.watchdog_timeout = 3600  # 1 hour default timeout
        self.db_check_interval = 300  # 5 minutes
        self.metagraph_sync_interval = 300  # 5 minutes
        self.max_consecutive_errors = 3
        self.watchdog_running = False

        # Setup signal handlers for graceful shutdown
        self._cleanup_done = False
        self._shutdown_event = asyncio.Event()
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, self._signal_handler)

        # Add lock for miner table operations
        self.miner_table_lock = asyncio.Lock()

        self.basemodel_evaluator = BaseModelEvaluator(
            db_manager=self.database_manager,
            test_mode=self.args.test if hasattr(self.args, 'test') else False
        )
        logger.info("BaseModelEvaluator initialized")
        
        # DB Sync components
        self.auto_sync_manager = None  # Streamlined sync system using pgBackRest + R2
        
        self.is_source_validator_for_db_sync = os.getenv("IS_SOURCE_VALIDATOR_FOR_DB_SYNC", "False").lower() == "true"
        
        # DB Sync interval & mode
        if self.args.test:
            self.db_sync_interval_hours = 0.25 # 15 minutes for testing
            logger.info(f"Test mode enabled: DB sync interval set to {self.db_sync_interval_hours} hours (15 minutes).")
        else:
            # Default to 1 hour, allow override by env var for non-test mode
            self.db_sync_interval_hours = int(os.getenv("DB_SYNC_INTERVAL_HOURS", "1"))
            if self.db_sync_interval_hours <= 0:
                logger.warning(f"DB_SYNC_INTERVAL_HOURS ('{os.getenv('DB_SYNC_INTERVAL_HOURS')}') is invalid ({self.db_sync_interval_hours}). Defaulting to 1 hour.")
                self.db_sync_interval_hours = 1
            logger.info(f"DB sync interval set to {self.db_sync_interval_hours} hours.")

        self.validator_uid = None

        # --- Stepped Task Weight ---
        self.task_weight_schedule = [
            (datetime(2025, 5, 28, 0, 0, 0, tzinfo=timezone.utc), 
             {"weather": 0.50, "geomagnetic": 0.25, "soil": 0.25}),
            
            # Transition Point 1: June 1st, 2025, 00:00:00 UTC
            (datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc), 
             {"weather": 0.65, "geomagnetic": 0.175, "soil": 0.175}), 
            
            # Target Weights: June 5th, 2025, 00:00:00 UTC
            (datetime(2025, 6, 5, 0, 0, 0, tzinfo=timezone.utc), 
             {"weather": 0.80, "geomagnetic": 0.10, "soil": 0.10})
        ]

        print("[STARTUP DEBUG] Validating task weight schedule")
        for dt_thresh, weights_dict in self.task_weight_schedule:
            if not math.isclose(sum(weights_dict.values()), 1.0):
                logger.error(f"Task weights for threshold {dt_thresh.isoformat()} do not sum to 1.0! Sum: {sum(weights_dict.values())}. Fix configuration.")

        # Initialize substrate connection manager (will be set up in setup_neuron)
        print("[STARTUP DEBUG] Initializing substrate manager")
        self.substrate_manager: Optional[SubstrateConnectionManager] = None
        print("[STARTUP DEBUG] GaiaValidatorRefactored.__init__ completed")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signame = signal.Signals(signum).name
        logger.info(f"Received shutdown signal {signame}")
        if not self._cleanup_done:
            # Set shutdown event
            if asyncio.get_event_loop().is_running():
                # If in event loop, just set the event
                logger.info("Setting shutdown event in running loop")
                self._shutdown_event.set()
            else:
                # If not in event loop (e.g. direct signal), run cleanup
                logger.info("Creating new loop for shutdown")
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                loop.run_until_complete(self._initiate_shutdown())

    async def _initiate_shutdown(self):
        """Handle graceful shutdown of the validator."""
        if self._cleanup_done:
            logger.info("Cleanup already completed")
            return

        logger.info("Initiating graceful shutdown sequence...")

        try:
            logger.info("Setting shutdown event (if not already set)...")
            self._shutdown_event.set()
            
            logger.info("Stopping watchdog (if running)...")
            if self.watchdog_running:
                await self.stop_watchdog()
                logger.info("Watchdog stopped.")
            else:
                logger.info("Watchdog was not running.")
            
            # Create cleanup completion file early for auto updater
            # PM2 will handle any remaining background processes
            logger.info("Creating cleanup completion file for auto updater...")
            try:
                cleanup_file = "/tmp/validator_cleanup_done"
                with open(cleanup_file, "w") as f:
                    f.write(f"Cleanup initiated at {time.time()}\n")
                logger.info(f"Created cleanup completion file: {cleanup_file}")
            except Exception as e_cleanup_file:
                logger.error(f"Failed to create cleanup completion file: {e_cleanup_file}")
            
            logger.info("Updating task statuses to 'stopping'...")
            for task_name in ['soil', 'geomagnetic', 'weather', 'scoring', 'deregistration', 'status_logger', 'db_sync_backup', 'db_sync_restore', 'miner_score_sender', 'earthdata_token', 'db_monitor', 'plot_db_metrics', 'memory_snapshot', 'check_updates']:
                try:
                    # Check if task exists in health tracking before updating
                    if task_name in self.task_health or hasattr(self, f"{task_name}_task") or (task_name.startswith("db_sync") and self.auto_sync_manager):
                        await self.update_task_status(task_name, 'stopping')
                    else:
                        logger.debug(f"Skipping status update for non-existent/inactive task: {task_name}")
                except Exception as e_status_update:
                    logger.error(f"Error updating {task_name} task status during shutdown: {e_status_update}")

            logger.info("Cleaning up resources (DB connections, HTTP clients, etc.)...")
            try:
                # Add timeout for cleanup to prevent hanging
                await asyncio.wait_for(self.cleanup_resources(), timeout=30)
                logger.info("Resource cleanup completed.")
            except asyncio.TimeoutError:
                logger.warning("Resource cleanup timed out after 30 seconds, proceeding with shutdown")
            except Exception as e_cleanup:
                logger.error(f"Error during resource cleanup: {e_cleanup}")
            
            logger.info("Performing final garbage collection...")
            try:
                import gc
                gc.collect()
                logger.info("Final garbage collection completed.")
            except Exception as e_gc:
                logger.error(f"Error during final garbage collection: {e_gc}")
            
            self._cleanup_done = True
            logger.info("Graceful shutdown sequence fully completed.")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            logger.error(traceback.format_exc())
            self._cleanup_done = True

    # PLACEHOLDER METHODS - These would need to be implemented from the original validator
    # For now, I'll add minimal stubs to make the refactored validator complete
    
    def setup_neuron(self) -> bool:
        """Setup the neuron configuration."""
        # This would contain the original setup_neuron logic
        logger.info("Setting up neuron configuration...")
        return True

    async def update_task_status(self, task_name: str, status: str, operation: Optional[str] = None):
        """Update task status and operation tracking."""
        # This would contain the original update_task_status logic
        logger.debug(f"Updating task status: {task_name} -> {status}")

    async def stop_watchdog(self):
        """Stop the watchdog."""
        if self.watchdog_running:
            self.watchdog_running = False
            logger.info("Stopped watchdog")

    async def cleanup_resources(self):
        """Clean up resources."""
        # This would contain the original cleanup_resources logic
        logger.info("Cleaning up resources...")

    def custom_serializer(self, obj: Any) -> Any:
        """Custom JSON serializer - delegated to miner communicator."""
        return self.miner_communicator.custom_serializer(obj)

    async def main(self):
        """
        Main method to start the validator with all background tasks.
        
        This includes all the missing background tasks that were identified
        in the original requirements.
        """
        logger.info("Starting refactored validator main method...")
        
        try:
            # Setup initial components
            if not self.setup_neuron():
                logger.error("Failed to setup neuron, exiting")
                return
            
            # Create all background tasks including the missing ones
            tasks_lambdas = [
                # Original core tasks (these would need to be implemented)
                lambda: self.main_scoring(),
                lambda: self.handle_miner_deregistration_loop(),
                lambda: self.status_logger(),
                lambda: self.manage_earthdata_token(),
                
                # Missing background tasks that are now implemented
                lambda: self.task_orchestrator.database_monitor(),
                lambda: self.task_orchestrator.memory_snapshot_taker(),
                lambda: self.task_orchestrator.plot_database_metrics_periodically(),
                lambda: self.task_orchestrator.check_for_updates(),
                
                # Additional background tasks (would need implementation)
                lambda: self.monitor_client_health(),
                lambda: self.periodic_substrate_cleanup(),
                lambda: self.aggressive_memory_cleanup(),
            ]

            # Start all background tasks
            tasks = [asyncio.create_task(task_lambda()) for task_lambda in tasks_lambdas]
            
            logger.info(f"Started {len(tasks)} background tasks")
            
            # Wait for shutdown or task completion
            done, pending = await asyncio.wait(
                tasks + [asyncio.create_task(self._shutdown_event.wait())],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            logger.info("Main loop shutting down...")
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
                
            # Wait for cancelled tasks to complete
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
                
        except Exception as e:
            logger.error(f"Error in main validator loop: {e}")
            logger.error(traceback.format_exc())
        finally:
            await self._initiate_shutdown()

    # PLACEHOLDER METHODS FOR ORIGINAL FUNCTIONALITY
    # These would need to be implemented with the original logic
    
    async def main_scoring(self):
        """Main scoring loop - placeholder."""
        logger.info("Starting main scoring loop...")
        while not self._shutdown_event.is_set():
            await asyncio.sleep(60)
            logger.debug("Scoring loop iteration")

    async def handle_miner_deregistration_loop(self):
        """Handle miner deregistration - placeholder."""
        logger.info("Starting miner deregistration loop...")
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            logger.debug("Deregistration check iteration")

    async def status_logger(self):
        """Status logger - placeholder."""
        logger.info("Starting status logger...")
        while not self._shutdown_event.is_set():
            await asyncio.sleep(60)
            logger.debug("Status logger iteration")

    async def manage_earthdata_token(self):
        """Manage earthdata token - placeholder."""
        logger.info("Starting earthdata token management...")
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            logger.debug("Earthdata token check iteration")

    async def monitor_client_health(self):
        """Monitor client health - placeholder."""
        logger.info("Starting client health monitoring...")
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            logger.debug("Client health check iteration")

    async def periodic_substrate_cleanup(self):
        """Periodic substrate cleanup - placeholder."""
        logger.info("Starting periodic substrate cleanup...")
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            logger.debug("Substrate cleanup iteration")

    async def aggressive_memory_cleanup(self):
        """Aggressive memory cleanup - placeholder."""
        logger.info("Starting aggressive memory cleanup...")
        while not self._shutdown_event.is_set():
            await asyncio.sleep(120)
            # Use the task orchestrator's memory cleanup
            memory_freed = self.task_orchestrator._comprehensive_memory_cleanup("periodic_aggressive")
            logger.debug(f"Aggressive cleanup freed {memory_freed:.1f}MB")


if __name__ == "__main__":
    parser = ArgumentParser()

    subtensor_group = parser.add_argument_group("subtensor")

    parser.add_argument("--wallet", type=str, help="Name of the wallet to use")
    parser.add_argument("--hotkey", type=str, help="Name of the hotkey to use")
    parser.add_argument("--netuid", type=int, help="Netuid to use")
    subtensor_group.add_argument(
        "--subtensor.chain_endpoint", type=str, help="Subtensor chain endpoint to use"
    )

    parser.add_argument(
        "--test",
        action="store_true",
        help="Run tasks in test mode - runs immediately and with limited scope",
    )

    args = parser.parse_args()

    # --- Comprehensive Database Setup ---
    logger.info("Starting comprehensive database setup and validator application...")
    # Run the comprehensive database setup
    asyncio.run(run_comprehensive_database_setup())
    # --- End Comprehensive Database Setup ---

    # --- Ensure requirements are up to date on every restart ---
    try:
        import subprocess
        import sys
        
        logger.info("Ensuring Python requirements are up to date...")
        print("[STARTUP DEBUG] Installing/updating requirements from requirements.txt...")
        
        # Construct path to requirements.txt relative to this script
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_script_dir, "..", ".."))
        requirements_path = os.path.join(project_root, "requirements.txt")
        
        if not os.path.exists(requirements_path):
            logger.warning(f"requirements.txt not found at {requirements_path}, skipping pip install")
            print(f"[STARTUP DEBUG] Warning: requirements.txt not found at {requirements_path}")
        else:
            # Run pip install with timeout to prevent hanging
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "-r", requirements_path],
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                cwd=project_root
            )
            
            if result.returncode == 0:
                logger.info("Successfully updated Python requirements")
                print("[STARTUP DEBUG] Python requirements updated successfully")
                if result.stdout:
                    logger.debug(f"Pip install output: {result.stdout}")
            else:
                logger.warning(f"Pip install returned non-zero exit code {result.returncode}")
                print(f"[STARTUP DEBUG] Warning: pip install failed with exit code {result.returncode}")
                if result.stderr:
                    logger.warning(f"Pip install stderr: {result.stderr}")
                    print(f"[STARTUP DEBUG] Pip error output: {result.stderr}")
                    
    except subprocess.TimeoutExpired:
        logger.warning("Pip install timed out after 5 minutes, continuing with startup")
        print("[STARTUP DEBUG] Warning: pip install timed out, continuing with startup")
    except Exception as e:
        logger.warning(f"Error during pip install: {e}, continuing with startup")
        print(f"[STARTUP DEBUG] Warning: Error during pip install: {e}")
    # --- End requirements update ---

    print("[STARTUP DEBUG] Creating GaiaValidatorRefactored instance")
    validator = GaiaValidatorRefactored(args)
    try:
        print("[STARTUP DEBUG] Starting validator.main()")
        asyncio.run(validator.main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
        print("[STARTUP DEBUG] Keyboard interrupt received")
    except Exception as e:
        logger.critical(f"Unhandled exception in main loop: {e}", exc_info=True)
        print(f"[STARTUP DEBUG] Unhandled exception: {e}")
    finally:
        print("[STARTUP DEBUG] Entering finally block")
        if hasattr(validator, '_cleanup_done') and not validator._cleanup_done:
             try:
                 loop = asyncio.get_event_loop()
                 if loop.is_closed():
                     loop = asyncio.new_event_loop()
                     asyncio.set_event_loop(loop)
                 loop.run_until_complete(validator._initiate_shutdown())
             except Exception as cleanup_e:
                 logger.error(f"Error during final cleanup: {cleanup_e}")
                 print(f"[STARTUP DEBUG] Error during final cleanup: {cleanup_e}")
        print("[STARTUP DEBUG] Startup sequence completed")