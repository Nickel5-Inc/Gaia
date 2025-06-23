# this file handles the basic functionality of the validator neuron. Setup, metagraph, etc.

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

# === WEIGHT TRACING INTEGRATION ===
try:
    import sys
    import os
    # Add the root directory to Python path to find runtime_weight_tracer
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    if root_dir not in sys.path:
        sys.path.insert(0, root_dir)
    
    import runtime_weight_tracer
    print("🔍 [MAIN] Weight tracing available - enabling...")
    runtime_weight_tracer.enable_weight_tracing()
    print("✅ [MAIN] Weight tracing enabled successfully")
except Exception as e:
    print(f"⚠️ [MAIN] Weight tracing not available: {e}")
# === END WEIGHT TRACING INTEGRATION ===

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
from fiber.logging_utils import get_logger
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
# Import the substrate connection manager
from gaia.validator.core.substrate_manager import SubstrateConnectionManager
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


class GaiaValidatorNeuron:
    """Core neuron setup and configuration for the Gaia validator."""
    
    def _clear_pycache_files(self):
        """Clear all Python bytecode cache files in the repository to prevent caching issues."""
        try:
            import subprocess
            import os
            
            # Get the repository root (where this file is located, go up to find gaia root)
            repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            
            print(f"[STARTUP] Clearing Python cache files in {repo_root}...")
            
            # Clear .pyc files
            cmd_pyc = f"find {repo_root} -name '*.pyc' -delete"
            result_pyc = subprocess.run(cmd_pyc, shell=True, capture_output=True, text=True)
            
            # Clear __pycache__ directories  
            cmd_pycache = f"find {repo_root} -name '__pycache__' -type d -exec rm -rf {{}} + 2>/dev/null || true"
            result_pycache = subprocess.run(cmd_pycache, shell=True, capture_output=True, text=True)
            
            # Clear Python import cache
            import importlib
            if hasattr(importlib, 'invalidate_caches'):
                importlib.invalidate_caches()
            
            print("[STARTUP] ✅ Python cache cleanup completed")
            
        except Exception as e:
            print(f"[STARTUP] ⚠️ Warning: Failed to clear Python cache: {e}")
            # Don't fail startup if cache clearing fails
            
    def __init__(self, args):
        """
        Initialize the GaiaValidator with provided arguments.
        """
        print("[STARTUP DEBUG] Starting GaiaValidator.__init__")
        
        # Clear Python bytecode cache on startup to prevent caching issues
        if os.getenv('VALIDATOR_CLEAR_PYCACHE_ON_STARTUP', 'true').lower() in ['true', '1', 'yes']:
            self._clear_pycache_files()
        
        self.args = args
        self.metagraph = None
        self.config = None
        self.database_manager = ValidatorDatabaseManager()
        
        # Initialize substrate connection manager (will be set up in setup_neuron)
        self.substrate_manager = None
        
        # MEMORY LEAK FIX: Task tracking for proper cleanup
        self._background_tasks = set()  # Track all background tasks
        self._task_cleanup_lock = asyncio.Lock()
        
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

        # Memory monitoring configuration
        self.memory_monitor_enabled = os.getenv('VALIDATOR_MEMORY_MONITORING_ENABLED', 'true').lower() in ['true', '1', 'yes']
        self.pm2_restart_enabled = os.getenv('VALIDATOR_PM2_RESTART_ENABLED', 'true').lower() in ['true', '1', 'yes']
        
        # AGGRESSIVE MEMORY THRESHOLDS - Lower defaults for more aggressive restart behavior
        self.memory_warning_threshold_mb = int(os.getenv('VALIDATOR_MEMORY_WARNING_THRESHOLD_MB', '8000'))  # 8GB (reduced from 10GB)
        self.memory_emergency_threshold_mb = int(os.getenv('VALIDATOR_MEMORY_EMERGENCY_THRESHOLD_MB', '10000'))  # 10GB (reduced from 12GB)  
        self.memory_critical_threshold_mb = int(os.getenv('VALIDATOR_MEMORY_CRITICAL_THRESHOLD_MB', '12000'))  # 12GB (reduced from 15GB)
        
        # Memory monitoring state
        self.last_memory_log_time = 0
        self.memory_log_interval = 300  # Log memory status every 5 minutes
        self.last_emergency_gc_time = 0
        self.emergency_gc_cooldown = 60  # Minimum 60 seconds between emergency GC attempts

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

        # For database monitor plotting
        self.db_monitor_history = []
        self.db_monitor_history_lock = asyncio.Lock()
        self.DB_MONITOR_HISTORY_MAX_SIZE = 120 # e.g., 2 hours of data if monitor runs every minute

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

        self.tracemalloc_snapshot1: Optional[tracemalloc.Snapshot] = None # Initialize for the snapshot taker task

        print("[STARTUP DEBUG] Validating task weight schedule")
        for dt_thresh, weights_dict in self.task_weight_schedule:
            if not math.isclose(sum(weights_dict.values()), 1.0):
                logger.error(f"Task weights for threshold {dt_thresh.isoformat()} do not sum to 1.0! Sum: {sum(weights_dict.values())}. Fix configuration.")

        # Initialize substrate connection manager (will be set up in setup_neuron)
        print("[STARTUP DEBUG] Initializing substrate manager")
        
        print("[STARTUP DEBUG] GaiaValidator.__init__ completed")

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
            for task_name in ['soil', 'geomagnetic', 'weather', 'scoring', 'deregistration', 'status_logger', 'db_sync_backup', 'db_sync_restore', 'miner_score_sender', 'earthdata_token', 'db_monitor', 'plot_db_metrics']:
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
            
        except Exception as e_shutdown_main:
            logger.error(f"Error during main shutdown sequence: {e_shutdown_main}", exc_info=True)
            # Ensure cleanup_done is set even if part of the main shutdown fails, to prevent re-entry
            self._cleanup_done = True 
            logger.warning("Graceful shutdown sequence partially completed due to error.")
            
            # Still try to create cleanup completion file even if shutdown had errors
            try:
                cleanup_file = "/tmp/validator_cleanup_done"
                with open(cleanup_file, "w") as f:
                    f.write(f"Cleanup completed with errors at {time.time()}\n")
                logger.info(f"Created cleanup completion file (with errors): {cleanup_file}")
            except Exception as e_cleanup_file:
                logger.error(f"Failed to create cleanup completion file after error: {e_cleanup_file}")

    def setup_neuron(self) -> bool:
        """
        Set up the neuron with necessary configurations and connections.
        """
        try:
            load_dotenv(".env")
            self.netuid = (
                self.args.netuid if self.args.netuid else int(os.getenv("NETUID", 237))
            )
            logger.info(f"Using netuid: {self.netuid}")

            self.subtensor_chain_endpoint = (
                self.args.subtensor.chain_endpoint
                if hasattr(self.args, "subtensor")
                   and hasattr(self.args.subtensor, "chain_endpoint")
                else os.getenv(
                    "SUBTENSOR_ADDRESS", "wss://test.finney.opentensor.ai:443/"
                )
            )

            self.subtensor_network = (
                self.args.subtensor.network
                if hasattr(self.args, "subtensor")
                   and hasattr(self.args.subtensor, "network")
                else os.getenv("SUBTENSOR_NETWORK", "test")
            )

            self.wallet_name = (
                self.args.wallet
                if self.args.wallet
                else os.getenv("WALLET_NAME", "default")
            )
            self.hotkey_name = (
                self.args.hotkey
                if self.args.hotkey
                else os.getenv("HOTKEY_NAME", "default")
            )
            self.keypair = chain_utils.load_hotkey_keypair(
                self.wallet_name, self.hotkey_name
            )

            original_query = SubstrateInterface.query
            def query_wrapper(self, module, storage_function, params, block_hash=None):
                result = original_query(self, module, storage_function, params, block_hash)
                if hasattr(result, 'value'):
                    if isinstance(result.value, list):
                        result.value = [int(x) if hasattr(x, '__int__') else x for x in result.value]
                    elif hasattr(result.value, '__int__'):
                        result.value = int(result.value)
                return result
            
            SubstrateInterface.query = query_wrapper

            original_blocks_since = w.blocks_since_last_update
            def blocks_since_wrapper(substrate, netuid, node_id):
                resp = self.substrate.rpc_request("chain_getHeader", [])  
                hex_num = resp["result"]["number"]
                current_block = int(hex_num, 16)
                last_updated_value = substrate.query(
                    "SubtensorModule",
                    "LastUpdate",
                    [netuid]
                ).value
                if last_updated_value is None or node_id >= len(last_updated_value):
                    return None
                last_update = int(last_updated_value[node_id])
                return current_block - last_update
            
            w.blocks_since_last_update = blocks_since_wrapper

            # Initialize substrate connection manager
            try:
                self.substrate_manager = SubstrateConnectionManager(
                    subtensor_network=self.subtensor_network,
                    chain_endpoint=self.subtensor_chain_endpoint
                )
                self.substrate = self.substrate_manager.get_fresh_connection()
                logger.info("✅ Initialized substrate connection manager with fresh connection")
            except Exception as e_sub_init:
                logger.error(f"CRITICAL: Failed to initialize SubstrateConnectionManager with endpoint {self.subtensor_chain_endpoint}: {e_sub_init}", exc_info=True)
                return False

            # Standard Metagraph Initialization
            try:
                self.metagraph = Metagraph(substrate=self.substrate, netuid=self.netuid)
            except Exception as e_meta_init:
                logger.error(f"CRITICAL: Failed to initialize Metagraph: {e_meta_init}", exc_info=True)
                return False

            # Standard Metagraph Sync
            try:
                # Use managed connection for metagraph sync
                self.substrate = self.substrate_manager.get_fresh_connection()
                logger.info("🔄 Using managed fresh connection for metagraph sync")
                self.metagraph.sync_nodes()  # Sync nodes after initialization
                logger.info(f"Successfully synced {len(self.metagraph.nodes) if self.metagraph.nodes else '0'} nodes from the network.")
            except Exception as e_meta_sync:
                logger.error(f"CRITICAL: Metagraph sync_nodes() FAILED: {e_meta_sync}", exc_info=True)
                return False # Sync failure is critical for neuron operation

            resp = self.substrate.rpc_request("chain_getHeader", [])  
            hex_num = resp["result"]["number"]
            self.current_block = int(hex_num, 16)
            logger.info(f"Initial block number type: {type(self.current_block)}, value: {self.current_block}")
            self.last_set_weights_block = self.current_block - 300

            if self.validator_uid is None:
                self.validator_uid = self.substrate.query(
                    "SubtensorModule", 
                    "Uids", 
                    [self.netuid, self.keypair.ss58_address]
                ).value
            validator_uid = self.validator_uid

            return True
        except Exception as e:
            logger.error(f"Error setting up neuron: {e}")
            logger.error(traceback.format_exc())
            return False

    def custom_serializer(self, obj):
        """Custom JSON serializer for handling datetime objects and bytes."""
        if isinstance(obj, (pd.Timestamp, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return {
                "_type": "bytes",
                "encoding": "base64",
                "data": base64.b64encode(obj).decode("ascii"),
            }
        raise TypeError(f"Type {type(obj)} not serializable")

    async def _initialize_db_sync_components(self):
        logger.info("Attempting to initialize DB Sync components...")
        
        db_sync_enabled_str = os.getenv("DB_SYNC_ENABLED", "True") # Default to True if not set
        if db_sync_enabled_str.lower() != "true":
            logger.info("DB_SYNC_ENABLED is not 'true'. Database synchronization feature will be disabled.")
            self.auto_sync_manager = None
            return

        # Initialize AutoSyncManager (streamlined sync system using pgBackRest + R2)
        try:
            logger.info("Initializing AutoSyncManager (streamlined sync system)...")
            self.auto_sync_manager = await get_auto_sync_manager(test_mode=self.args.test)
            if self.auto_sync_manager:
                logger.info("✅ AutoSyncManager initialized successfully")
                logger.info("🔧 AutoSyncManager provides automated setup and application-controlled scheduling")
                logger.info("📝 To set up database sync, run: python gaia/validator/sync/setup_auto_sync.py --primary (or --replica)")
                return
            else:
                logger.warning("AutoSyncManager failed to initialize - check environment variables")
        except Exception as e:
            logger.warning(f"AutoSyncManager initialization failed: {e}")
            logger.info("💡 To enable DB sync, configure PGBACKREST_R2_* environment variables")
            logger.info("   - PGBACKREST_R2_BUCKET")
            logger.info("   - PGBACKREST_R2_ENDPOINT") 
            logger.info("   - PGBACKREST_R2_ACCESS_KEY_ID")
            logger.info("   - PGBACKREST_R2_SECRET_ACCESS_KEY")
        
        logger.info("DB Sync initialization completed (not active).")

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
