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
from gaia.tasks.defined_tasks.weather.weather_task import WeatherTask

# Imports for Alembic check
from alembic.config import Config
from alembic import command
from alembic.util import CommandError # Import CommandError
from sqlalchemy import create_engine, pool

# New imports for DB Sync
from gaia.validator.sync.azure_blob_utils import get_azure_blob_manager_for_db_sync, AzureBlobManager
from gaia.validator.sync.backup_manager import get_backup_manager, BackupManager
from gaia.validator.sync.restore_manager import get_restore_manager, RestoreManager
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


class GaiaValidator:
    def __init__(self, args):
        """
        Initialize the GaiaValidator with provided arguments.
        """
        self.args = args
        self.metagraph = None
        self.config = None
        self.database_manager = ValidatorDatabaseManager()
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
                max_connections=50,  # Reduced from 100
                max_keepalive_connections=15,  # Reduced from 20
                keepalive_expiry=60,  # Increased from 30
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
        self.azure_blob_manager_for_sync: AzureBlobManager | None = None
        self.backup_manager: BackupManager | None = None
        self.restore_manager: RestoreManager | None = None
        
        self.is_source_validator_for_db_sync = os.getenv("IS_SOURCE_VALIDATOR_FOR_DB_SYNC", "False").lower() == "true"
        self.db_sync_interval_hours = int(os.getenv("DB_SYNC_INTERVAL_HOURS", "1")) # Default to 1 hour
        if self.db_sync_interval_hours <= 0 : # Ensure positive interval
            logger.warning(f"DB_SYNC_INTERVAL_HOURS is {self.db_sync_interval_hours}, defaulting to 1 hour for safety.")
            self.db_sync_interval_hours = 1

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

        for dt_thresh, weights_dict in self.task_weight_schedule:
            if not math.isclose(sum(weights_dict.values()), 1.0):
                logger.error(f"Task weights for threshold {dt_thresh.isoformat()} do not sum to 1.0! Sum: {sum(weights_dict.values())}. Fix configuration.")


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
                    if task_name in self.task_health or hasattr(self, f"{task_name}_task") or (task_name.startswith("db_sync") and (self.backup_manager or self.restore_manager)):
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

            try:
                self.substrate = get_substrate(
                    subtensor_network=self.subtensor_network,
                    subtensor_address=self.subtensor_chain_endpoint)
            except Exception as e_sub_init:
                logger.error(f"CRITICAL: Failed to initialize SubstrateInterface with endpoint {self.subtensor_chain_endpoint}: {e_sub_init}", exc_info=True)
                return False

            # Standard Metagraph Initialization
            try:
                self.metagraph = Metagraph(substrate=self.substrate, netuid=self.netuid)
            except Exception as e_meta_init:
                logger.error(f"CRITICAL: Failed to initialize Metagraph: {e_meta_init}", exc_info=True)
                return False

            # Standard Metagraph Sync
            try:
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

    async def query_miners(self, payload: Dict, endpoint: str, hotkeys: Optional[List[str]] = None) -> Dict:
        """Query miners with the given payload in parallel with batch retry logic."""
        try:
            logger.info(f"Querying miners for endpoint {endpoint} with payload size: {len(str(payload))} bytes. Specified hotkeys: {hotkeys if hotkeys else 'All/Default'}")
            if "data" in payload and "combined_data" in payload["data"]:
                logger.debug(f"TIFF data size before serialization: {len(payload['data']['combined_data'])} bytes")
                if isinstance(payload["data"]["combined_data"], bytes):
                    logger.debug(f"TIFF header before serialization: {payload['data']['combined_data'][:4]}")

            responses = {}
            
            current_time = time.time()
            if self.metagraph is None or current_time - self.last_metagraph_sync > self.metagraph_sync_interval:
                logger.info(f"Metagraph not initialized or sync interval ({self.metagraph_sync_interval}s) exceeded. Syncing metagraph before querying miners. Last sync: {current_time - self.last_metagraph_sync if self.metagraph else 'Never'}s ago.")
                try:
                    await asyncio.wait_for(self._sync_metagraph(), timeout=60.0) 
                except asyncio.TimeoutError:
                    logger.error("Metagraph sync timed out within query_miners. Proceeding with potentially stale metagraph.")
                except Exception as e_sync:
                    logger.error(f"Error during metagraph sync in query_miners: {e_sync}. Proceeding with potentially stale metagraph.")
            else:
                logger.debug(f"Metagraph recently synced. Skipping sync for this query_miners call. Last sync: {current_time - self.last_metagraph_sync:.2f}s ago.")

            if not self.metagraph or not self.metagraph.nodes:
                logger.error("Metagraph not available or no nodes in metagraph after sync attempt. Cannot query miners.")
                return {}

            nodes_to_consider = self.metagraph.nodes
            miners_to_query = {}

            if hotkeys:
                logger.info(f"Targeting specific hotkeys: {hotkeys}")
                for hk in hotkeys:
                    if hk in nodes_to_consider:
                        miners_to_query[hk] = nodes_to_consider[hk]
                    else:
                        logger.warning(f"Specified hotkey {hk} not found in current metagraph. Skipping.")
                if not miners_to_query:
                    logger.warning(f"No specified hotkeys found in metagraph. Querying will be empty for endpoint: {endpoint}")
                    return {}
            else:
                miners_to_query = nodes_to_consider
                if self.args.test and len(miners_to_query) > 10:
                    selected_hotkeys_for_test = list(miners_to_query.keys())[-10:]
                    miners_to_query = {k: miners_to_query[k] for k in selected_hotkeys_for_test}
                    logger.info(f"Test mode: Selected the last {len(miners_to_query)} miners to query for endpoint: {endpoint} (no specific hotkeys provided).")
                elif not self.args.test:
                    logger.info(f"Querying all {len(miners_to_query)} available miners for endpoint: {endpoint} (no specific hotkeys provided).")

            if not miners_to_query:
                logger.warning(f"No miners to query for endpoint {endpoint} after filtering. Hotkeys: {hotkeys}")
                return {}

            # Use the existing miner_client instead of creating a new one
            if not hasattr(self, 'miner_client') or self.miner_client.is_closed:
                logger.warning("Miner client not available or closed, creating new one")
                # Create SSL context that doesn't verify certificates
                import ssl
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                self.miner_client = httpx.AsyncClient(
                    timeout=httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=5.0),
                    follow_redirects=True,
                    verify=False,
                    limits=httpx.Limits(
                        max_connections=50,  # Reduced from 100
                        max_keepalive_connections=15,  # Reduced from 20 
                        keepalive_expiry=60,  # Increased from 30
                    ),
                    transport=httpx.AsyncHTTPTransport(
                        retries=2,  # Reduced from 3
                        verify=False,  # Explicitly set verify=False on transport
                    ),
                )

            # Reduce concurrency based on number of miners to avoid overwhelming the system
            max_concurrent = min(5, len(miners_to_query))  # Much more conservative
            semaphore = asyncio.Semaphore(max_concurrent)
            logger.info(f"Using concurrency limit of {max_concurrent} for {len(miners_to_query)} miners")

            # Batch retry configuration
            max_retry_rounds = 2  # Total of 3 attempts (1 initial + 2 retries)
            retry_delay = 2.0  # Delay between retry rounds
            
            # Track miners for retry
            miners_remaining = dict(miners_to_query)  # Copy for first attempt
            
            for retry_round in range(max_retry_rounds + 1):  # 0 = initial attempt, 1+ = retry rounds
                if not miners_remaining:
                    break
                    
                round_type = "Initial attempt" if retry_round == 0 else f"Retry round {retry_round}"
                logger.info(f"{round_type}: Querying {len(miners_remaining)} miners")
                
                async def query_single_miner_no_retry(miner_hotkey: str, node: Any, attempt_num: int) -> Optional[Dict]:
                    """Query a single miner without internal retries (batch retry handled at higher level)."""
                    base_url = f"https://{node.ip}:{node.port}"
                    process = psutil.Process() if PSUTIL_AVAILABLE else None
                    try:
                        async with semaphore:  # Control concurrency
                            logger.debug(f"[{round_type}] Initiating handshake with miner {miner_hotkey} at {base_url}")
                            
                            # Perform handshake with a single attempt (no retry at this level)
                            handshake_start_time = time.time()
                            symmetric_key_str, symmetric_key_uuid = None, None
                            try:
                                # Use a single attempt with longer timeout for retries
                                current_timeout = 15.0 + (attempt_num * 5.0)  # Increase timeout with retry attempts
                                
                                # Get public key
                                public_key_encryption_key = await asyncio.wait_for(
                                    handshake.get_public_encryption_key(
                                        self.miner_client, 
                                        base_url, 
                                        timeout=int(current_timeout)
                                    ),
                                    timeout=current_timeout
                                )
                                
                                # Generate symmetric key
                                symmetric_key: bytes = os.urandom(32)
                                symmetric_key_uuid: str = os.urandom(32).hex()
                                
                                # Send symmetric key
                                success = await asyncio.wait_for(
                                    handshake.send_symmetric_key_to_server(
                                        self.miner_client,
                                        base_url,
                                        self.keypair,
                                        public_key_encryption_key,
                                        symmetric_key,
                                        symmetric_key_uuid,
                                        miner_hotkey,
                                        timeout=int(current_timeout),
                                    ),
                                    timeout=current_timeout
                                )
                                
                                if success:
                                    symmetric_key_str = base64.b64encode(symmetric_key).decode()
                                else:
                                    raise Exception("Handshake failed: server returned unsuccessful status")
                                    
                            except Exception as hs_err:
                                logger.debug(f"[{round_type}] Handshake failed for miner {miner_hotkey} at {base_url}: {type(hs_err).__name__} - {hs_err}")
                                return None

                            handshake_duration = time.time() - handshake_start_time
                            logger.debug(f"[{round_type}] Handshake with {miner_hotkey} completed in {handshake_duration:.2f}s")

                            if not symmetric_key_str or not symmetric_key_uuid:
                                logger.debug(f"[{round_type}] Failed handshake with miner {miner_hotkey} (no key/UUID returned)")
                                return None

                            logger.debug(f"[{round_type}] Handshake successful with miner {miner_hotkey}")
                            if process:
                                logger.debug(f"Memory after handshake ({miner_hotkey}): {process.memory_info().rss / (1024*1024):.2f} MB")
                            
                            fernet = Fernet(symmetric_key_str)
                            
                            try:
                                import pickle
                                payload_bytes = pickle.dumps(payload)
                                payload_size_bytes = len(payload_bytes)
                                logger.debug(f"Preparing to send request to miner {miner_hotkey} at {base_url}{endpoint}. Raw payload size: {payload_size_bytes / (1024*1024):.2f} MB")
                                del payload_bytes
                                import gc
                                gc.collect()
                            except Exception as size_err:
                                logger.warning(f"Could not accurately determine payload size for {miner_hotkey}: {size_err}")
                                payload_size_bytes = -1
                                                            
                            resp = None
                            try:
                                logger.debug(f"[{round_type}] Calling vali_client.make_non_streamed_post for {miner_hotkey}...")
                                if process:
                                    mem_before = process.memory_info().rss / (1024*1024)
                                    logger.debug(f"Memory BEFORE request call ({miner_hotkey}): {process.memory_info().rss / (1024*1024):.2f} MB")
                                request_start_time = time.time()
                                resp = await asyncio.wait_for(
                                    vali_client.make_non_streamed_post(
                                        httpx_client=self.miner_client,  # Use existing client
                                        server_address=base_url,
                                        fernet=fernet,
                                        keypair=self.keypair,
                                        symmetric_key_uuid=symmetric_key_uuid,
                                        validator_ss58_address=self.keypair.ss58_address,
                                        miner_ss58_address=miner_hotkey,
                                        payload=payload,
                                        endpoint=endpoint,
                                    ),
                                    timeout=240.0  # Increased from 180s for large payloads
                                )
                                request_duration = time.time() - request_start_time
                                if process:
                                    mem_after = process.memory_info().rss / (1024*1024)
                                    logger.debug(f"Memory AFTER successful request call ({miner_hotkey}): {mem_after:.2f} MB (Delta: {mem_after - mem_before:.2f} MB)")
                                if resp:
                                    logger.info(f"[{round_type}] Successfully called make_non_streamed_post for {miner_hotkey}. Response status: {resp.status_code}. Duration: {request_duration:.2f}s. Response content length: {len(resp.content) if resp.content else 0} bytes.")
                                else:
                                    logger.warning(f"[{round_type}] Call to make_non_streamed_post for {miner_hotkey} completed without error but response object is None. Duration: {request_duration:.2f}s")

                            except Exception as request_error:
                                if process:
                                    logger.error(f"Memory during request EXCEPTION ({miner_hotkey}): {process.memory_info().rss / (1024*1024):.2f} MB")
                                logger.debug(f"[{round_type}] Error during make_non_streamed_post for {miner_hotkey}: {type(request_error).__name__} - {request_error}")
                                return None
                            
                            if resp is None:
                                 logger.debug(f"[{round_type}] No response object received from {miner_hotkey} despite no exception, likely due to prior error.")
                                 return None
                            elif resp.status_code >= 400:
                                logger.debug(f"[{round_type}] Miner {miner_hotkey} returned error status {resp.status_code}. Response text: {resp.text[:500]}...")
                                return None

                            response_data = {
                                "text": resp.text,
                                "status_code": resp.status_code,
                                "hotkey": miner_hotkey,
                                "port": node.port,
                                "ip": node.ip,
                                "content_length": len(resp.content) if resp.content else 0
                            }
                            logger.info(f"[{round_type}] Successfully received response from miner {miner_hotkey}")
                            return response_data

                    except asyncio.TimeoutError:
                        logger.debug(f"[{round_type}] Timeout querying miner {miner_hotkey} at {base_url}")
                        return None
                    except Exception as e:
                        logger.debug(f"[{round_type}] Failed request to miner {miner_hotkey} at {base_url}: {type(e).__name__} - {str(e)}")
                        return None

                # Query all remaining miners in parallel for this round
                tasks = []
                for hotkey, node in miners_remaining.items():
                    if node.ip and node.port:  # Only query miners with valid IP/port
                        tasks.append(query_single_miner_no_retry(hotkey, node, retry_round))
                    else:
                        logger.warning(f"Skipping miner {hotkey} - missing IP or port")

                # Gather responses with timeout
                miner_responses = await asyncio.gather(*tasks, return_exceptions=True)

                # Process responses and track failures for next round
                miners_for_next_round = {}
                successful_this_round = 0
                
                for (hotkey, node), response in zip(miners_remaining.items(), miner_responses):
                    if response is not None and not isinstance(response, Exception):
                        responses[response['hotkey']] = response
                        successful_this_round += 1
                    else:
                        # Add to retry list for next round
                        miners_for_next_round[hotkey] = node

                logger.info(f"{round_type} completed: {successful_this_round} successful, {len(miners_for_next_round)} failed")
                
                # Update miners_remaining for next round
                miners_remaining = miners_for_next_round
                
                # If we have more rounds and miners to retry, wait before next round
                if miners_remaining and retry_round < max_retry_rounds:
                    logger.info(f"Waiting {retry_delay}s before retry round {retry_round + 1} with {len(miners_remaining)} miners")
                    await asyncio.sleep(retry_delay)

            logger.info(f"Batch query completed: Received {len(responses)} total valid responses from miners")
            return responses

        except Exception as e:
            logger.error(f"Error querying miners: {e}")
            logger.error(traceback.format_exc())
            return {}

    async def check_for_updates(self):
        """Check for and apply updates every 5 minutes."""
        while True:
            try:
                logger.info("Checking for updates...")
                # Add timeout to prevent hanging
                try:
                    update_successful = await asyncio.wait_for(
                        perform_update(self),
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
                        import psutil
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
                    except ImportError:
                        logger.warning("psutil not available for resource tracking")
            elif status == 'idle':
                if health.get('current_operation'):
                    # Log resource usage when operation completes
                    try:
                        import psutil
                        process = psutil.Process()
                        current_memory = process.memory_info().rss
                        memory_change = current_memory - health['resources']['memory_start']
                        peak_memory = max(current_memory, health['resources'].get('memory_peak', 0))
                        logger.info(
                            f"Task {task_name} completed operation: {health['current_operation']} | "
                            f"Memory Change: {memory_change / (1024*1024):.2f}MB | "
                            f"Peak Memory: {peak_memory / (1024*1024):.2f}MB"
                        )
                    except ImportError:
                        pass
                health['current_operation'] = None
                health['operation_start'] = None
                health['last_success'] = time.time()

    async def start_watchdog(self):
        """Start the watchdog in a separate thread."""
        if not self.watchdog_running:
            self.watchdog_running = True
            logger.info("Started watchdog")
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
            
            # Check metagraph sync health with timeout
            if current_time - self.last_metagraph_sync > self.metagraph_sync_interval:
                try:
                    await asyncio.wait_for(
                        self._sync_metagraph(),
                        timeout=10  # 10 second timeout
                    )
                except asyncio.TimeoutError:
                    logger.error("Metagraph sync timed out")
                except Exception as e:
                    logger.error(f"Metagraph sync failed: {e}")

        except Exception as e:
            logger.error(f"Error in watchdog check: {e}")
            logger.error(traceback.format_exc())

    async def _check_resource_usage(self, current_time):
        """Check resource usage for active tasks."""
        import psutil
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

    async def _sync_metagraph(self):
        """Sync the metagraph."""
        sync_start = time.time()
        self.metagraph.sync_nodes()
        sync_duration = time.time() - sync_start
        self.last_metagraph_sync = time.time()
        if sync_duration > 30:  # Log slow syncs
            logger.warning(f"Slow metagraph sync: {sync_duration:.2f}s")

    async def cleanup_resources(self):
        """Clean up any resources used by the validator during recovery."""
        try:
            # First clean up database resources
            if hasattr(self, 'database_manager'):
                await self.database_manager.execute(
                    """
                    UPDATE geomagnetic_predictions 
                    SET status = 'pending'
                    WHERE status = 'processing'
                    """
                )
                logger.info("Reset in-progress prediction statuses")
                
                await self.database_manager.execute(
                    """
                    DELETE FROM score_table 
                    WHERE task_name = 'geomagnetic' 
                    AND status = 'processing'
                    """
                )
                logger.info("Cleaned up incomplete scoring operations")
                
                # Close database connections
                await self.database_manager.close_all_connections()
                logger.info("Closed database connections")

            # Clean up HTTP clients
            if hasattr(self, 'miner_client') and self.miner_client and not self.miner_client.is_closed:
                await self.miner_client.aclose()
                logger.info("Closed miner HTTP client")
            
            if hasattr(self, 'api_client') and self.api_client and not self.api_client.is_closed:
                await self.api_client.aclose()
                logger.info("Closed API HTTP client")

            # Clean up task-specific resources
            if hasattr(self, 'miner_score_sender'):
                if hasattr(self.miner_score_sender, 'cleanup'):
                    await self.miner_score_sender.cleanup()
                logger.info("Cleaned up miner score sender resources")

            # Clean up WeatherTask resources that might be using gcsfs
            try:
                if hasattr(self, 'weather_task'):
                    await self.weather_task.cleanup_resources()
                    logger.info("Cleaned up WeatherTask resources")
            except Exception as e:
                logger.debug(f"Error cleaning up WeatherTask: {e}")

            # Aggressive fsspec/gcsfs cleanup to prevent session errors blocking PM2 restart
            try:
                logger.info("Performing aggressive fsspec/gcsfs cleanup...")
                
                # Suppress all related warnings and errors that could block PM2 restart
                import logging
                import warnings
                logging.getLogger('fsspec').setLevel(logging.CRITICAL)
                logging.getLogger('gcsfs').setLevel(logging.CRITICAL)
                logging.getLogger('aiohttp').setLevel(logging.CRITICAL)
                logging.getLogger('asyncio').setLevel(logging.CRITICAL)
                warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*coroutine.*never awaited.*')
                warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*Non-thread-safe operation.*')
                
                # Force clear fsspec caches and registries
                import fsspec
                fsspec.config.conf.clear()
                if hasattr(fsspec.filesystem, '_cache'):
                    fsspec.filesystem._cache.clear()
                
                # Try to close any active gcsfs sessions more aggressively
                try:
                    import gcsfs
                    # Clear any cached filesystems
                    if hasattr(gcsfs, '_fs_cache'):
                        gcsfs._fs_cache.clear()
                    if hasattr(gcsfs.core, '_fs_cache'):
                        gcsfs.core._fs_cache.clear()
                except ImportError:
                    pass
                except Exception:
                    pass  # Ignore any errors during aggressive cleanup
                
                # Force garbage collection to help clean up lingering references
                import gc
                gc.collect()
                
                # Set environment variable to suppress aiohttp warnings
                import os
                os.environ['PYTHONWARNINGS'] = 'ignore::RuntimeWarning'
                
                logger.info("Aggressive fsspec/gcsfs cleanup completed")
                
            except ImportError:
                logger.debug("fsspec not available for cleanup")
            except Exception as e:
                # Don't let cleanup errors block shutdown
                logger.debug(f"Non-critical error during aggressive cleanup: {e}")
            
            logger.info("Completed resource cleanup")
            
        except Exception as e:
            logger.error(f"Error during resource cleanup: {e}")
            # Don't raise the exception - let shutdown continue for PM2 restart
            logger.info("Continuing shutdown despite cleanup errors to allow PM2 restart")

    async def recover_task(self, task_name: str):
        """Enhanced task recovery with specific handling for each task type."""
        logger.warning(f"Attempting to recover {task_name}")
        try:
            # First clean up resources
            await self.cleanup_resources()
            
            # Task-specific recovery
            if task_name == "soil":
                await self.soil_task.cleanup_resources()
            elif task_name == "geomagnetic":
                await self.geomagnetic_task.cleanup_resources()
            elif task_name == "scoring":
                self.substrate = interface.get_substrate(subtensor_network=self.subtensor_network)
                self.metagraph.sync_nodes()
            elif task_name == "deregistration":
                self.metagraph.sync_nodes()
                self.nodes = {}
            
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

    async def cleanup_stale_history_on_startup(self):
        """
        Compares historical prediction table hotkeys against the current metagraph
        and cleans up data for UIDs where hotkeys have changed or the UID is gone.
        Runs once on validator startup.
        """
        logger.info("Starting cleanup of stale miner history based on current metagraph...")
        try:
            if not self.metagraph:
                logger.warning("Metagraph not initialized, cannot perform stale history cleanup.")
                return
            if not self.database_manager:
                logger.warning("Database manager not initialized, cannot perform stale history cleanup.")
                return

            # 1. Fetch Current Metagraph State
            logger.info("Syncing metagraph for stale history check...")
            self.metagraph.sync_nodes()
            
            # Fetch the list of nodes directly since Metagraph object doesn't store a UID-indexed list
            try:
                active_nodes_list = get_nodes_for_netuid(self.substrate, self.metagraph.netuid)
                if active_nodes_list is None:
                    active_nodes_list = [] # Ensure it's an iterable if None is returned
                    logger.warning("get_nodes_for_netuid returned None, proceeding with empty list for stale history check.")
            except Exception as e_fetch_nodes:
                logger.error(f"Failed to fetch nodes for stale history check: {e_fetch_nodes}", exc_info=True)
                active_nodes_list = [] # Proceed with empty list to avoid further errors here

            # Build current_nodes_info mapping node_id (UID) to Node object
            current_nodes_info = {node.node_id: node for node in active_nodes_list}
            logger.info(f"Built current_nodes_info with {len(current_nodes_info)} active UIDs for stale history check.")

            # 2. Fetch Historical Data (Distinct uid, miner_hotkey pairs)
            geo_history_query = "SELECT DISTINCT miner_uid, miner_hotkey FROM geomagnetic_history WHERE miner_hotkey IS NOT NULL;"
            soil_history_query = "SELECT DISTINCT miner_uid, miner_hotkey FROM soil_moisture_history WHERE miner_hotkey IS NOT NULL;"
            
            all_historical_pairs = set()
            try:
                geo_results = await self.database_manager.fetch_all(geo_history_query)
                all_historical_pairs.update((row['miner_uid'], row['miner_hotkey']) for row in geo_results)
                logger.info(f"Found {len(geo_results)} distinct (miner_uid, hotkey) pairs in geomagnetic_history.")
            except Exception as e:
                logger.warning(f"Could not query geomagnetic_history (may not exist yet): {e}")

            try:
                soil_results = await self.database_manager.fetch_all(soil_history_query)
                all_historical_pairs.update((row['miner_uid'], row['miner_hotkey']) for row in soil_results)
                logger.info(f"Found {len(soil_results)} distinct (miner_uid, hotkey) pairs in soil_moisture_history.")
            except Exception as e:
                logger.warning(f"Could not query soil_moisture_history (may not exist yet): {e}")

            if not all_historical_pairs:
                logger.info("No historical data found to check. Skipping stale history cleanup.")
                return

            logger.info(f"Found {len(all_historical_pairs)} total distinct historical (miner_uid, hotkey) pairs to check.")

            # 3. Identify Mismatches
            uids_to_cleanup = defaultdict(set)  # uid -> {stale_historical_hotkey1, stale_historical_hotkey2, ...}
            
            # --- DIAGNOSTIC LOGGING START ---
            current_node_keys = list(current_nodes_info.keys())
            logger.info(f"Diagnostic: current_nodes_info has {len(current_node_keys)} keys. Sample keys: {current_node_keys[:5]} (Type: {type(current_node_keys[0]) if current_node_keys else 'N/A'})")
            # --- DIAGNOSTIC LOGGING END ---
            
            for hist_uid_str, hist_hotkey in all_historical_pairs:
                try:
                    hist_uid = int(hist_uid_str) # Convert hist_uid from string to int
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert historical UID '{hist_uid_str}' to int. Skipping.")
                    continue
                    
                # --- DIAGNOSTIC LOGGING START ---
                logger.info(f"Diagnostic: Checking hist_uid: {hist_uid} (Type: {type(hist_uid)}), hist_hotkey: {hist_hotkey}")
                # --- DIAGNOSTIC LOGGING END ---
                current_node = current_nodes_info.get(hist_uid)
                # We assume if a UID exists in history, it MUST exist in the current metagraph
                # because slots are typically always filled. The important check is hotkey mismatch.
                if current_node is None:
                     # This case is highly unlikely if metagraph slots are always filled.
                     # Log a warning but don't trigger specific cleanup based on None node.
                     logger.warning(f"Found historical entry for miner_uid {hist_uid} (Hotkey: {hist_hotkey}), but node is unexpectedly None in current metagraph sync. Skipping direct cleanup based on this, hotkey mismatch check will handle if applicable.")
                     continue # Skip to next historical pair

                if current_node.hotkey != hist_hotkey:
                    # Hotkey mismatch: This is the primary condition for cleanup.
                    logger.warning(f"Mismatch found: miner_uid {hist_uid} historical hotkey {hist_hotkey} != current metagraph hotkey {current_node.hotkey}. Marking for cleanup.")
                    uids_to_cleanup[hist_uid].add(hist_hotkey)

            if not uids_to_cleanup:
                logger.info("No stale historical entries found requiring cleanup.")
                return

            logger.info(f"Identified {len(uids_to_cleanup)} UIDs with stale history/scores.")

            # 4. Perform Cleanup
            # Get relevant task names for score zeroing
            distinct_task_names_rows = await self.database_manager.fetch_all("SELECT DISTINCT task_name FROM score_table")
            all_task_names_in_scores = [row['task_name'] for row in distinct_task_names_rows if row['task_name']]
            tasks_for_score_cleanup = [
                name for name in all_task_names_in_scores 
                if name == 'geomagnetic' or name.startswith('soil_moisture')
            ]
            if not tasks_for_score_cleanup:
                 logger.warning("No relevant task names (geomagnetic, soil_moisture*) found in score_table for cleanup.")
                 # Proceed with history deletion and node_table update anyway

            async with self.miner_table_lock: # Use lock to coordinate with deregistration loop
                for uid, stale_hotkeys in uids_to_cleanup.items():
                    logger.info(f"Cleaning up miner_uid {uid} associated with stale historical hotkeys: {stale_hotkeys}")
                    current_node = current_nodes_info.get(uid) # Get current info again

                    # Process each stale hotkey individually for precise score zeroing
                    for stale_hk in stale_hotkeys:
                        logger.info(f"Processing stale hotkey {stale_hk} for miner_uid {uid}.")
                        
                        # 4.1 Determine time window for the stale hotkey
                        min_ts: Optional[datetime] = None
                        max_ts: Optional[datetime] = None
                        timestamps_found = False
                        
                        # Query both history tables using 'scored_at'
                        history_tables_and_ts_cols = {
                            "geomagnetic_history": "scored_at", # Use scored_at
                            "soil_moisture_history": "scored_at" # Use scored_at
                        }
                        
                        all_min_ts = []
                        all_max_ts = []
                        
                        for table, ts_col in history_tables_and_ts_cols.items():
                            try:
                                ts_query = f"""
                                    SELECT MIN({ts_col}) as min_ts, MAX({ts_col}) as max_ts 
                                    FROM {table} 
                                    WHERE miner_uid = :uid_str AND miner_hotkey = :stale_hk
                                """
                                result = await self.database_manager.fetch_one(ts_query, {"uid_str": str(uid), "stale_hk": stale_hk})
                                
                                if result and result['min_ts'] is not None and result['max_ts'] is not None:
                                    all_min_ts.append(result['min_ts'])
                                    all_max_ts.append(result['max_ts'])
                                    timestamps_found = True
                                    logger.info(f"  Found time range in {table} for ({uid}, {stale_hk}): {result['min_ts']} -> {result['max_ts']}")
                                    
                            except Exception as e_ts:
                                logger.warning(f"Could not query timestamps from {table} for miner_uid {uid}, Hotkey {stale_hk}: {e_ts}")
                        
                        # Determine overall min/max across tables
                        if all_min_ts:
                             min_ts = min(all_min_ts)
                        if all_max_ts:
                             max_ts = max(all_max_ts)

                        # 4.2 Delete historical predictions associated with this specific stale hotkey
                        logger.info(f"  Deleting history entries for ({uid}, {stale_hk})")
                        try:
                            await self.database_manager.execute(
                                "DELETE FROM geomagnetic_history WHERE miner_uid = :uid_str AND miner_hotkey = :stale_hk",
                                {"uid_str": str(uid), "stale_hk": stale_hk}
                            )
                        except Exception as e_del_geo:
                             logger.warning(f"  Could not delete from geomagnetic_history for miner_uid {uid}, Hotkey {stale_hk}: {e_del_geo}")
                        try:
                            await self.database_manager.execute(
                                "DELETE FROM soil_moisture_history WHERE miner_uid = :uid_str AND miner_hotkey = :stale_hk",
                                {"uid_str": str(uid), "stale_hk": stale_hk}
                            )
                        except Exception as e_del_soil:
                            logger.warning(f"  Could not delete from soil_moisture_history for miner_uid {uid}, Hotkey {stale_hk}: {e_del_soil}")
                        
                        # 4.3 Zero out scores in score_table *only for the determined time window*
                        if tasks_for_score_cleanup and timestamps_found and min_ts and max_ts:
                            logger.info(f"  Zeroing scores for miner_uid {uid} in tasks {tasks_for_score_cleanup} within window {min_ts} -> {max_ts}")
                            await self.database_manager.remove_miner_from_score_tables(
                                uids=[uid],
                                task_names=tasks_for_score_cleanup,
                                filter_start_time=min_ts,
                                filter_end_time=max_ts
                            )
                        elif not timestamps_found:
                             logger.warning(f"  Skipping score zeroing for miner_uid {uid}, Hotkey {stale_hk} - could not determine time window from history tables.")
                        elif not tasks_for_score_cleanup:
                             logger.info(f"  Skipping score zeroing for miner_uid {uid}, Hotkey {stale_hk} - no relevant task names found in score_table.")
                        else: # Should not happen if timestamps_found is true, but defensive check
                            logger.warning(f"  Skipping score zeroing for miner_uid {uid}, Hotkey {stale_hk} due to missing min/max timestamps.")

                    # 4.4 Update node_table (Done once per UID after processing all its stale hotkeys)
                    # Since we only proceed if a mismatch was found, current_node should exist.
                    current_node_for_update = current_nodes_info.get(uid)
                    if current_node_for_update:
                        logger.info(f"Updating node_table info for miner_uid {uid} to match current metagraph hotkey {current_node_for_update.hotkey}.")
                        try:
                            await self.database_manager.update_miner_info(
                                index=uid, hotkey=current_node_for_update.hotkey, coldkey=current_node_for_update.coldkey,
                                ip=current_node_for_update.ip, ip_type=str(current_node_for_update.ip_type), port=current_node_for_update.port,
                                incentive=float(current_node_for_update.incentive), stake=float(current_node_for_update.stake),
                                trust=float(current_node_for_update.trust), vtrust=float(current_node_for_update.vtrust),
                                protocol=str(current_node_for_update.protocol)
                            )
                        except Exception as e_update:
                             logger.error(f"Failed to update node_table for miner_uid {uid}: {e_update}")
                    else:
                        # This case should now be extremely unlikely given the check adjustments above.
                        logger.error(f"Critical inconsistency: Attempted cleanup for miner_uid {uid}, but node became None before final update. Skipping node_table update.")
                        # We avoid calling clear_miner_info here as the state is unexpected.

            logger.info("Completed cleanup of stale miner history.")

        except Exception as e:
            logger.error(f"Error during stale history cleanup: {e}")
            logger.error(traceback.format_exc())

    async def main(self):
        """Main execution loop for the validator."""

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
            if not self.setup_neuron():
                logger.error("Failed to setup neuron, exiting...")
                return

            logger.info("Neuron setup complete.")

            logger.info("Checking metagraph initialization...")
            if self.metagraph is None:
                logger.error("Metagraph not initialized, exiting...")
                return

            logger.info("Metagraph initialized.")

            logger.info("Initializing database connection...") 
            await self.database_manager.initialize_database()
            logger.info("Database tables initialized.")
            
            # Initialize DB Sync Components - AFTER DB init
            # await self._initialize_db_sync_components()

            #logger.warning(" CHECKING FOR DATABASE WIPE TRIGGER ")
            await handle_db_wipe(self.database_manager)
            
            # Perform startup history cleanup AFTER db init and wipe check
            await self.cleanup_stale_history_on_startup()

            # Lock storage to prevent any writes
            self.database_manager._storage_locked = False
            if self.database_manager._storage_locked:
                logger.warning("Database storage is locked - no data will be stored until manually unlocked")

            logger.info("Checking HTTP clients...")
            # Only create clients if they don't exist or are closed
            if not hasattr(self, 'miner_client') or self.miner_client.is_closed:
                self.miner_client = httpx.AsyncClient(
                    timeout=30.0, follow_redirects=True, verify=False
                )
                logger.info("Created new miner client")
            if not hasattr(self, 'api_client') or self.api_client.is_closed:
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
                logger.info("Created new API client")
            logger.info("HTTP clients ready.")

            logger.info("Starting watchdog...")
            await self.start_watchdog()
            logger.info("Watchdog started.")
            
            logger.info("Initializing baseline models...")
            await self.basemodel_evaluator.initialize_models()
            logger.info("Baseline models initialization complete")
            
            # Start auto-updater as independent task (not in main loop to avoid self-cancellation)
            logger.info("Starting independent auto-updater task...")
            auto_updater_task = asyncio.create_task(self.check_for_updates())
            logger.info("Auto-updater task started independently")
            
            tasks = [
                #lambda: self.geomagnetic_task.validator_execute(self),
                #lambda: self.soil_task.validator_execute(self),
                lambda: self.weather_task.validator_execute(self),
                lambda: self.status_logger(),
                lambda: self.main_scoring(),
                lambda: self.handle_miner_deregistration_loop(),
                # The MinerScoreSender task will be added conditionally below
                lambda: self.manage_earthdata_token(),
                lambda: self.monitor_client_health(),  # Added HTTP client monitoring
                #lambda: self.database_monitor(),
                #lambda: self.plot_database_metrics_periodically() # Added plotting task
            ]

            # Add DB Sync tasks conditionally
            # if self.is_source_validator_for_db_sync and self.backup_manager:
            #     logger.info(f"Adding DB Sync Backup task (interval: {self.db_sync_interval_hours}h).")
            #     # Using insert to make it one of the earlier tasks, but order might not be critical.
            #     tasks.insert(0, lambda: self.backup_manager.start_periodic_backups(self.db_sync_interval_hours))
            # elif not self.is_source_validator_for_db_sync and self.restore_manager:
            #     logger.info(f"Adding DB Sync Restore task (interval: {self.db_sync_interval_hours}h).")
            #     tasks.insert(0, lambda: self.restore_manager.start_periodic_restores(self.db_sync_interval_hours))
            # else:
            #     logger.info("DB Sync is not active for this node (either source or replica manager failed to init, not configured, or Azure manager failed).")

            
            # Conditionally add miner_score_sender task
            score_sender_on_str = os.getenv("SCORE_SENDER_ON", "False")
            if score_sender_on_str.lower() == "true":
                logger.info("SCORE_SENDER_ON is True, enabling MinerScoreSender task.")
                tasks.insert(5, lambda: self.miner_score_sender.run_async())

            active_service_tasks = []  # Define here for access in except CancelledError
            shutdown_waiter = None # Define here for access in except CancelledError
            try:
                logger.info(f"Creating {len(tasks)} main service tasks...")
                active_service_tasks = [asyncio.create_task(t()) for t in tasks]
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
                await self._initiate_shutdown()

    async def main_scoring(self):
        """Run scoring every subnet tempo blocks."""
        weight_setter = FiberWeightSetter(
            netuid=self.netuid,
            wallet_name=self.wallet_name,
            hotkey_name=self.hotkey_name,
            network=self.subtensor_network,
        )

        while True:
            try:
                await self.update_task_status('scoring', 'active')
                
                async def scoring_cycle():
                    try:
                        validator_uid = self.validator_uid
                        
                        if validator_uid is None:
                            try:
                                self.validator_uid = self.substrate.query(
                                    "SubtensorModule", 
                                    "Uids", 
                                    [self.netuid, self.keypair.ss58_address]
                                ).value
                                validator_uid = int(self.validator_uid)
                            except Exception as e:
                                logger.error(f"Error getting validator UID: {e}")
                                logger.error(traceback.format_exc())
                                await self.update_task_status('scoring', 'error')
                                return False
                            

                        validator_uid = int(validator_uid)
                        last_updated_value = self.substrate.query(
                            "SubtensorModule",
                            "LastUpdate",
                            [self.netuid]
                        ).value
                        if last_updated_value is not None and validator_uid < len(last_updated_value):
                            last_updated = int(last_updated_value[validator_uid])
                            resp = self.substrate.rpc_request("chain_getHeader", [])  
                            hex_num = resp["result"]["number"]
                            current_block = int(hex_num, 16)
                            blocks_since_update = current_block - last_updated
                            logger.info(f"Calculated blocks since update: {blocks_since_update} (current: {current_block}, last: {last_updated})")
                        else:
                            blocks_since_update = None
                            logger.warning("Could not determine last update value")

                        min_interval = w.min_interval_to_set_weights(
                            self.substrate, 
                            self.netuid
                        )
                        if min_interval is not None:
                            min_interval = int(min_interval)
                        resp = self.substrate.rpc_request("chain_getHeader", [])  
                        hex_num = resp["result"]["number"]
                        current_block = int(hex_num, 16)                     
                        if current_block - self.last_set_weights_block < min_interval:
                            logger.info(f"Recently set weights {current_block - self.last_set_weights_block} blocks ago")
                            await self.update_task_status('scoring', 'idle', 'waiting')
                            await asyncio.sleep(60)
                            return True

                        # Only enter weight_setting state when actually setting weights
                        if (min_interval is None or 
                            (blocks_since_update is not None and blocks_since_update >= min_interval)):
                            logger.info(f"Setting weights: {blocks_since_update}/{min_interval} blocks")
                            can_set = w.can_set_weights(
                                self.substrate, 
                                self.netuid, 
                                validator_uid
                            )
                            
                            if can_set:
                                await self.update_task_status('scoring', 'processing', 'weight_setting')
                                
                                # Calculate weights with timeout
                                normalized_weights = await asyncio.wait_for(
                                    self._calc_task_weights(),
                                    timeout=120
                                )
                                
                                if normalized_weights:
                                    # Set weights with timeout
                                    success = await asyncio.wait_for(
                                        weight_setter.set_weights(normalized_weights),
                                        timeout=480
                                    )
                                    
                                    if success:
                                        await self.update_last_weights_block()
                                        self.last_successful_weight_set = time.time()
                                        logger.info("✅ Successfully set weights")
                                        await self.update_task_status('scoring', 'idle')
                                        
                                        # Clean up any stale operations
                                        await self.database_manager.cleanup_stale_operations('score_table')
                        else:
                            logger.info(
                                f"Waiting for weight setting: {blocks_since_update}/{min_interval} blocks"
                            )
                            await self.update_task_status('scoring', 'idle', 'waiting')

                        return True
                        
                    except asyncio.TimeoutError as e:
                        logger.error(f"Timeout in scoring cycle: {str(e)}")
                        return False
                    except Exception as e:
                        logger.error(f"Error in scoring cycle: {str(e)}")
                        logger.error(traceback.format_exc())
                        return False
                    finally:
                        # Sleep removed - now handled in main loop for consistent timing
                        pass

                # Run scoring cycle with overall timeout
                await asyncio.wait_for(scoring_cycle(), timeout=900)

            except asyncio.TimeoutError:
                logger.error("Weight setting operation timed out - restarting cycle")
                await self.update_task_status('scoring', 'error')
                try:
                    self.substrate = await interface.async_get_substrate(subtensor_network=self.subtensor_network)
                except Exception as e:
                    logger.error(f"Failed to reconnect to substrate: {e}")
                await asyncio.sleep(12)
                continue
            except Exception as e:
                logger.error(f"Error in main_scoring: {e}")
                logger.error(traceback.format_exc())
                await self.update_task_status('scoring', 'error')
                await asyncio.sleep(12)
                continue
            
            # Add sleep to prevent rapid cycling when scoring completes quickly
            # This ensures consistent timing regardless of scoring outcome
            await asyncio.sleep(60)

    async def status_logger(self):
        """Log the status of the validator periodically."""
        while True:
            try:
                current_time_utc = datetime.now(timezone.utc)
                formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S")

                try:
                    resp = self.substrate.rpc_request("chain_getHeader", [])  
                    hex_num = resp["result"]["number"]
                    self.current_block = int(hex_num, 16)
                    blocks_since_weights = (
                            self.current_block - self.last_set_weights_block
                    )
                except Exception as block_error:

                    try:
                        self.substrate = get_substrate(
                            subtensor_network=self.subtensor_network,
                            subtensor_address=self.subtensor_chain_endpoint
                        )
                    except Exception as e:
                        logger.error(f"Failed to reconnect to substrate: {e}")

                active_nodes = len(self.metagraph.nodes) if self.metagraph else 0

                logger.info(
                    f"\n"
                    f"---Status Update ---\n"
                    f"Time (UTC): {formatted_time} | \n"
                    f"Block: {self.current_block} | \n"
                    f"Nodes: {active_nodes}/256 | \n"
                    f"Weights Set: {blocks_since_weights} blocks ago"
                )

            except Exception as e:
                logger.error(f"Error in status logger: {e}")
                logger.error(f"{traceback.format_exc()}")
            finally:
                await asyncio.sleep(60)

    async def handle_miner_deregistration_loop(self) -> None:
        logger.info("Starting miner state synchronization loop (handles hotkey changes, new miners, and info updates)")
        
        while True:
            processed_uids = set() # Keep track of UIDs processed in this cycle
            try:
                # Ensure metagraph is up to date
                if not self.metagraph:
                    logger.warning("Metagraph object not initialized, cannot sync miner state.")
                    await asyncio.sleep(600)  # Sleep before retrying
                    continue # Wait for metagraph to be initialized
                
                logger.info("Syncing metagraph for miner state update...")
                self.metagraph.sync_nodes()
                if not self.metagraph.nodes:
                    logger.warning("Metagraph empty after sync, skipping miner state update.")
                    await asyncio.sleep(600)  # Sleep before retrying
                    continue

                async with self.miner_table_lock:
                    logger.info("Performing miner hotkey change check and info update...")
                    
                    # Get current UIDs and hotkeys from the chain's metagraph
                    try:
                        active_nodes_list = get_nodes_for_netuid(self.substrate, self.metagraph.netuid)
                        if active_nodes_list is None:
                            active_nodes_list = [] # Ensure it's an iterable
                            logger.warning("get_nodes_for_netuid returned None in handle_miner_deregistration_loop.")
                    except Exception as e_fetch_nodes_dereg:
                        logger.error(f"Failed to fetch nodes in handle_miner_deregistration_loop: {e_fetch_nodes_dereg}", exc_info=True)
                        active_nodes_list = []
                    
                    # Build chain_nodes_info mapping node_id (UID) to Node object
                    chain_nodes_info = {node.node_id: node for node in active_nodes_list}

                    # Get UIDs and hotkeys from our local database
                    db_miner_query = "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL;"
                    db_miners_rows = await self.database_manager.fetch_all(db_miner_query)
                    db_miners_info = {row["uid"]: row["hotkey"] for row in db_miners_rows}

                    uids_to_clear_and_update = {} # uid: new_chain_node
                    uids_to_update_info = {} # uid: new_chain_node (for existing miners with potentially changed info)

                    # --- Step 1: Check existing DB miners against the chain --- 
                    for db_uid, db_hotkey in db_miners_info.items():
                        processed_uids.add(db_uid) # Mark as processed
                        chain_node_for_uid = chain_nodes_info.get(db_uid)

                        if chain_node_for_uid is None:
                            # This case *shouldn't* happen if metagraph always fills slots,
                            # but handle defensively. Might indicate UID truly removed.
                            logger.warning(f"UID {db_uid} (DB hotkey: {db_hotkey}) not found in current metagraph sync. Potential deregistration missed? Skipping.")
                            # Consider adding to a separate cleanup list if this persists.
                            continue 
                            
                        if chain_node_for_uid.hotkey != db_hotkey:
                            # Hotkey for this UID has changed!
                            logger.info(f"UID {db_uid} hotkey changed. DB: {db_hotkey}, Chain: {chain_node_for_uid.hotkey}. Marking for data cleanup and update.")
                            uids_to_clear_and_update[db_uid] = chain_node_for_uid
                        else:
                            # Hotkey matches, but other info might have changed. Mark for potential update.
                            uids_to_update_info[db_uid] = chain_node_for_uid

                    # --- Step 2: Process UIDs with changed hotkeys ---
                    if uids_to_clear_and_update:
                        logger.info(f"Cleaning old data and updating hotkeys for UIDs: {list(uids_to_clear_and_update.keys())}")
                        for uid_to_process, new_chain_node_data in uids_to_clear_and_update.items():
                            original_hotkey = db_miners_info.get(uid_to_process) # Get the old hotkey from DB cache
                            if not original_hotkey:
                                logger.warning(f"Could not find original hotkey in DB cache for UID {uid_to_process}. Skipping cleanup for this UID.")
                                continue
                                
                            logger.info(f"Processing hotkey change for UID {uid_to_process}: Old={original_hotkey}, New={new_chain_node_data.hotkey}")
                            try:
                                # 1. Delete from prediction tables by UID
                                prediction_tables_by_uid = ["geomagnetic_predictions", "soil_moisture_predictions"]
                                for table_name in prediction_tables_by_uid:
                                    try:
                                        table_exists_res = await self.database_manager.fetch_one(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')")
                                        if table_exists_res and table_exists_res['exists']:
                                            # Delete using the original hotkey associated with the UID
                                            await self.database_manager.execute(f"DELETE FROM {table_name} WHERE miner_hotkey = :hotkey", {"hotkey": original_hotkey})
                                            logger.info(f"  Deleted from {table_name} for old hotkey {original_hotkey} (UID {uid_to_process}) due to hotkey change.")
                                        # No else needed, if table doesn't exist, we just skip
                                    except Exception as e_pred_del:
                                        logger.warning(f"  Could not clear {table_name} for UID {uid_to_process}: {e_pred_del}")

                                # 2. Delete from history tables by OLD hotkey
                                history_tables_by_hotkey = ["geomagnetic_history", "soil_moisture_history"]
                                for table_name in history_tables_by_hotkey:
                                    try:
                                        table_exists_res = await self.database_manager.fetch_one(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')")
                                        if table_exists_res and table_exists_res['exists']:
                                            await self.database_manager.execute(f"DELETE FROM {table_name} WHERE miner_hotkey = :hotkey", {"hotkey": original_hotkey})
                                            logger.info(f"  Deleted from {table_name} for old hotkey {original_hotkey} (UID {uid_to_process}) due to hotkey change.")
                                    except Exception as e_hist_del:
                                         logger.warning(f"  Could not clear {table_name} for old hotkey {original_hotkey}: {e_hist_del}")

                                # 3. Zero out ALL scores for the UID in score_table
                                distinct_task_names_rows = await self.database_manager.fetch_all("SELECT DISTINCT task_name FROM score_table")
                                all_task_names_in_scores = [row['task_name'] for row in distinct_task_names_rows if row['task_name']]
                                if all_task_names_in_scores:
                                    logger.info(f"  Zeroing all scores in score_table for UID {uid_to_process} across tasks: {all_task_names_in_scores}")
                                    await self.database_manager.remove_miner_from_score_tables(
                                        uids=[uid_to_process],
                                        task_names=all_task_names_in_scores,
                                        filter_start_time=None, filter_end_time=None # Affect all history
                                    )
                                else:
                                    logger.info(f"  No task names found in score_table to zero-out for UID {uid_to_process}.")

                                # 4. Update node_table with NEW info
                                logger.info(f"  Updating node_table info for UID {uid_to_process} with new hotkey {new_chain_node_data.hotkey}.")
                                await self.database_manager.update_miner_info(
                                    index=uid_to_process, hotkey=new_chain_node_data.hotkey, coldkey=new_chain_node_data.coldkey,
                                    ip=new_chain_node_data.ip, ip_type=str(new_chain_node_data.ip_type), port=new_chain_node_data.port,
                                    incentive=float(new_chain_node_data.incentive), stake=float(new_chain_node_data.stake),
                                    trust=float(new_chain_node_data.trust), vtrust=float(new_chain_node_data.vtrust),
                                    protocol=str(new_chain_node_data.protocol)
                                )
                                
                                # Update in-memory state as well
                                if uid_to_process in self.nodes:
                                     del self.nodes[uid_to_process] # Remove old entry if exists
                                self.nodes[uid_to_process] = {"hotkey": new_chain_node_data.hotkey, "uid": uid_to_process}
                                logger.info(f"Successfully processed hotkey change for UID {uid_to_process}.")
                                
                            except Exception as e:
                                logger.error(f"Error processing hotkey change for UID {uid_to_process}: {str(e)}", exc_info=True)

                    # --- Step 3: Update info for existing miners where hotkey didn't change ---
                    if uids_to_update_info:
                        logger.info(f"Updating potentially changed info (stake, IP, etc.) for {len(uids_to_update_info)} existing UIDs...")
                        
                        batch_updates = []
                        for uid_to_update, chain_node_data in uids_to_update_info.items():
                            if uid_to_update in uids_to_clear_and_update: 
                                continue
                                
                            batch_updates.append({
                                "index": uid_to_update,
                                "hotkey": chain_node_data.hotkey,
                                "coldkey": chain_node_data.coldkey,
                                "ip": chain_node_data.ip,
                                "ip_type": str(chain_node_data.ip_type),
                                "port": chain_node_data.port,
                                "incentive": float(chain_node_data.incentive),
                                "stake": float(chain_node_data.stake),
                                "trust": float(chain_node_data.trust),
                                "vtrust": float(chain_node_data.vtrust),
                                "protocol": str(chain_node_data.protocol)
                            })
                            
                            self.nodes[uid_to_update] = {"hotkey": chain_node_data.hotkey, "uid": uid_to_update}
                        
                        if batch_updates:
                            try:
                                await self.database_manager.batch_update_miners(batch_updates)
                                logger.info(f"Successfully batch updated {len(batch_updates)} existing miners")
                            except Exception as e:
                                logger.error(f"Error in batch update of existing miners: {str(e)}")
                                for update_data in batch_updates:
                                    try:
                                        uid = update_data["index"]
                                        await self.database_manager.update_miner_info(**update_data)
                                        logger.debug(f"Successfully updated info for existing UID {uid} (fallback)")
                                    except Exception as individual_e:
                                        logger.error(f"Error updating info for existing UID {uid}: {str(individual_e)}", exc_info=True)

                    new_miners_detected = 0
                    new_miner_updates = []
                    
                    for chain_uid, chain_node in chain_nodes_info.items():
                        if chain_uid not in processed_uids:
                            logger.info(f"New miner detected on chain: UID {chain_uid}, Hotkey {chain_node.hotkey}. Adding to DB.")
                            
                            new_miner_updates.append({
                                "index": chain_uid,
                                "hotkey": chain_node.hotkey,
                                "coldkey": chain_node.coldkey,
                                "ip": chain_node.ip,
                                "ip_type": str(chain_node.ip_type),
                                "port": chain_node.port,
                                "incentive": float(chain_node.incentive),
                                "stake": float(chain_node.stake),
                                "trust": float(chain_node.trust),
                                "vtrust": float(chain_node.vtrust),
                                "protocol": str(chain_node.protocol)
                            })
                            
                            self.nodes[chain_uid] = {"hotkey": chain_node.hotkey, "uid": chain_uid}
                            new_miners_detected += 1
                            processed_uids.add(chain_uid)
                    
                    if new_miner_updates:
                        try:
                            await self.database_manager.batch_update_miners(new_miner_updates)
                            logger.info(f"Successfully batch added {new_miners_detected} new miners to the database")
                        except Exception as e:
                            logger.error(f"Error in batch update of new miners: {str(e)}")
                            successful_adds = 0
                            for update_data in new_miner_updates:
                                try:
                                    uid = update_data["index"]
                                    await self.database_manager.update_miner_info(**update_data)
                                    successful_adds += 1
                                except Exception as individual_e:
                                    logger.error(f"Error adding new miner UID {uid} (Hotkey: {update_data['hotkey']}): {str(individual_e)}", exc_info=True)
                            if successful_adds > 0:
                                logger.info(f"Added {successful_adds} new miners to the database (fallback)")

                    logger.info("Miner state synchronization cycle completed.")

            except asyncio.CancelledError:
                logger.info("Miner state synchronization loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Error in miner state synchronization loop: {e}", exc_info=True)
            
            # Sleep at the end of the loop - runs immediately on first iteration, then every 10 minutes
            await asyncio.sleep(600)  # Run every 10 minutes

    def _perform_weight_calculations_sync(self, weather_results, geomagnetic_results, soil_results, now, validator_nodes_by_uid_list):
        """
        Synchronous helper to perform CPU-bound weight calculations.
        """
        logger.info("Synchronous weight calculation: Processing fetched scores...")
        # Initialize score arrays
        weather_scores = np.full(256, np.nan)
        geomagnetic_scores = np.full(256, np.nan)
        soil_scores = np.full(256, np.nan)

        # Count raw scores per UID
        weather_counts = [0] * 256
        geo_counts = [0] * 256
        soil_counts = [0] * 256
        
        if weather_results:
            for result in weather_results:
                # Ensure 'score' key exists and is a list of appropriate length
                scores = result.get('score', [np.nan]*256)
                if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
                for uid in range(256):
                    if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                        weather_counts[uid] += 1
        
        if geomagnetic_results:
            for result in geomagnetic_results:
                scores = result.get('score', [np.nan]*256)
                if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
                for uid in range(256):
                    if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                        geo_counts[uid] += 1

        if soil_results:
            for result in soil_results:
                scores = result.get('score', [np.nan]*256)
                if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
                for uid in range(256):
                    if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                        soil_counts[uid] += 1

        if geomagnetic_results:
            geo_scores_by_uid = [[] for _ in range(256)]
            zero_scores_count = 0
            for result in geomagnetic_results:
                age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                decay = np.exp(-age_days * np.log(2))
                scores = result.get('score', [np.nan]*256)
                if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
                for uid in range(256):
                    if isinstance(scores[uid], str) or np.isnan(scores[uid]): scores[uid] = 0.0
                    geo_scores_by_uid[uid].append((scores[uid], decay))
                    if scores[uid] == 0.0: zero_scores_count += 1
            logger.info(f"Loaded {len(geomagnetic_results)} geomagnetic score records with {zero_scores_count} zero scores")
            zeros_after = 0
            for uid in range(256):
                if geo_scores_by_uid[uid]:
                    s_vals, d_weights = zip(*geo_scores_by_uid[uid])
                    s_arr, w_arr = np.array(s_vals), np.array(d_weights)
                    zero_mask, total_count = (s_arr == 0.0), len(s_arr)
                    if np.all(zero_mask): geomagnetic_scores[uid], zeros_after = 0.0, zeros_after + 1; logger.debug(f"UID {uid}: All {total_count} geo scores zeroed"); continue
                    masked_s, masked_w = s_arr[~zero_mask], w_arr[~zero_mask]
                    if np.any(zero_mask): logger.debug(f"UID {uid}: Masked {np.sum(zero_mask)}/{total_count} zero geo scores")
                    weight_sum = np.sum(masked_w)
                    if weight_sum > 0: geomagnetic_scores[uid] = np.sum(masked_s * masked_w) / weight_sum; zeros_after += (geomagnetic_scores[uid] == 0.0)
                    else: geomagnetic_scores[uid], zeros_after = 0.0, zeros_after + 1
            logger.info(f"Geomagnetic masked array processing: {zeros_after} UIDs have zero final score")

        if weather_results:
            latest_result = weather_results[0]
            scores = latest_result.get('score', [np.nan]*256)
            if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
            score_age_days = (now - latest_result['created_at']).total_seconds() / (24 * 3600)
            logger.info(f"Using latest weather score from {latest_result['created_at']} ({score_age_days:.1f} days ago)")
            for uid in range(256):
                if isinstance(scores[uid], str) or np.isnan(scores[uid]): weather_scores[uid] = 0.0
                else: weather_scores[uid] = scores[uid]; weather_counts[uid] += (scores[uid] != 0.0)
            logger.info(f"Weather scores: {sum(1 for s in weather_scores if s == 0.0)} UIDs have zero score")

        if soil_results:
            soil_scores_by_uid = [[] for _ in range(256)]
            for result in soil_results:
                age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                decay = np.exp(-age_days * np.log(2))
                scores = result.get('score', [np.nan]*256)
                if not isinstance(scores, list) or len(scores) != 256: scores = [np.nan]*256 # Defensive
                for uid in range(256):
                    if isinstance(scores[uid], str) or np.isnan(scores[uid]): scores[uid] = 0.0
                    soil_scores_by_uid[uid].append((scores[uid], decay))
            zero_soil_scores = sum(s == 0.0 for res in soil_results for s_list in [res.get('score')] if isinstance(s_list, list) for s in s_list if isinstance(s, (int, float)))
            logger.info(f"Loaded {len(soil_results)} soil records with {zero_soil_scores}/{len(soil_results)*256} zero scores")
            zeros_after = 0
            for uid in range(256):
                if soil_scores_by_uid[uid]:
                    s_vals, d_weights = zip(*soil_scores_by_uid[uid])
                    s_arr, w_arr = np.array(s_vals), np.array(d_weights)
                    zero_mask, total_count = (s_arr == 0.0), len(s_arr)
                    if np.all(zero_mask): soil_scores[uid], zeros_after = 0.0, zeros_after + 1; logger.debug(f"UID {uid}: All {total_count} soil scores zeroed"); continue
                    masked_s, masked_w = s_arr[~zero_mask], w_arr[~zero_mask]
                    if np.any(zero_mask): logger.debug(f"UID {uid}: Masked {np.sum(zero_mask)}/{total_count} zero soil scores")
                    weight_sum = np.sum(masked_w)
                    if weight_sum > 0: soil_scores[uid] = np.sum(masked_s * masked_w) / weight_sum; zeros_after += (soil_scores[uid] == 0.0)
                    else: soil_scores[uid], zeros_after = 0.0, zeros_after + 1
            logger.info(f"Soil task: {zeros_after} UIDs have zero final score after masked array processing")

        logger.info("Aggregate scores calculated. Proceeding to weight normalization...")
        def sigmoid(x, k=20, x0=0.93): return 1 / (1 + math.exp(-k * (x - x0)))
        weights_final = np.zeros(256)
        for idx in range(256):
            w_s, g_s, sm_s = weather_scores[idx], geomagnetic_scores[idx], soil_scores[idx]
            if np.isnan(w_s) or w_s==0: w_s=np.nan
            if np.isnan(g_s) or g_s==0: g_s=np.nan
            if np.isnan(sm_s) or sm_s==0: sm_s=np.nan
            node_obj, hk_chain = validator_nodes_by_uid_list[idx] if idx < len(validator_nodes_by_uid_list) else None, "N/A"
            if node_obj: hk_chain = node_obj.hotkey
            if np.isnan(w_s) and np.isnan(g_s) and np.isnan(sm_s): weights_final[idx] = 0.0
            else:
                wc, gc, sc, total_w_avail = 0.0,0.0,0.0,0.0
                if not np.isnan(w_s): wc,total_w_avail = 0.70*w_s, total_w_avail+0.70
                if not np.isnan(g_s): gc,total_w_avail = 0.15*sigmoid(g_s), total_w_avail+0.15
                if not np.isnan(sm_s): sc,total_w_avail = 0.15*sm_s, total_w_avail+0.15
                weights_final[idx] = (wc+gc+sc)/total_w_avail if total_w_avail>0 else 0.0
            logger.info(f"UID {idx} (HK: {hk_chain}): Wea={w_s if not np.isnan(w_s) else '-'} ({weather_counts[idx]} scores), Geo={g_s if not np.isnan(g_s) else '-'} ({geo_counts[idx]} scores), Soil={sm_s if not np.isnan(sm_s) else '-'} ({soil_counts[idx]} scores), AggW={weights_final[idx]:.4f}")
        logger.info(f"Weights before normalization: Min={np.min(weights_final):.4f}, Max={np.max(weights_final):.4f}, Mean={np.mean(weights_final):.4f}")
        
        non_zero_mask = weights_final != 0.0
        if not np.any(non_zero_mask): logger.warning("No non-zero weights to normalize!"); return None
        
        nz_weights = weights_final[non_zero_mask]
        max_w_val = np.max(nz_weights)
        if max_w_val == 0: logger.warning("Max weight is 0, cannot normalize."); return None
        
        norm_weights = np.copy(weights_final); norm_weights[non_zero_mask] /= max_w_val
        positives = norm_weights[norm_weights > 0]
        if not positives.any(): logger.warning("No positive weights after initial normalization!"); return None
        
        M = np.percentile(positives, 80); logger.info(f"Using 80th percentile ({M:.8f}) as curve midpoint")
        b,Q,v,k,a,slope = 70,8,0.3,0.98,0.0,0.01
        transformed_w = np.zeros_like(weights_final)
        nz_indices = np.where(weights_final > 0.0)[0]
        if not nz_indices.any(): logger.warning("No positive weight indices for transformation!"); return None

        for idx in nz_indices:
            sig_p = a+(k-a)/np.power(1+Q*np.exp(-b*(norm_weights[idx]-M)),1/v)
            transformed_w[idx] = sig_p + slope*norm_weights[idx]
        
        trans_nz = transformed_w[transformed_w > 0]
        if trans_nz.any(): logger.info(f"Transformed weights: Min={np.min(trans_nz):.4f}, Max={np.max(trans_nz):.4f}, Mean={np.mean(trans_nz):.4f}")
        else: logger.warning("No positive weights after sigmoid transformation!"); # Continue to rank-based if needed or return None

        if len(trans_nz) > 1 and np.std(trans_nz) < 0.01:
            logger.warning(f"Transformed weights too uniform (std={np.std(trans_nz):.4f}), switching to rank-based.")
            sorted_indices = np.argsort(-weights_final); transformed_w = np.zeros_like(weights_final)
            pos_count = np.sum(weights_final > 0)
            for i,idx_val in enumerate(sorted_indices[:pos_count]): transformed_w[idx_val] = 1.0/((i+1)**1.2)
            rank_nz = transformed_w[transformed_w > 0]
            if rank_nz.any(): logger.info(f"Rank-based: Min={np.min(rank_nz):.4f}, Max={np.max(rank_nz):.4f}, Mean={np.mean(rank_nz):.4f}")
        
        final_sum = np.sum(transformed_w); final_weights_list = None
        if final_sum > 0:
            transformed_w /= final_sum
            final_nz_vals = transformed_w[transformed_w > 0]
            if final_nz_vals.any():
                logger.info(f"Final Norm Weights: Count={len(final_nz_vals)}, Min={np.min(final_nz_vals):.4f}, Max={np.max(final_nz_vals):.4f}, Std={np.std(final_nz_vals):.4f}")
                if len(np.unique(final_nz_vals)) < len(final_nz_vals)/2 : logger.warning(f"Low unique weights! {len(np.unique(final_nz_vals))}/{len(final_nz_vals)}")
                if final_nz_vals.max() > 0.90: logger.warning(f"Max weight {final_nz_vals.max():.4f} is very high!")
            final_weights_list = transformed_w.tolist()
            logger.info("Final normalized weights calculated.")
        else: logger.warning("Sum of weights is zero, cannot normalize! Returning None.")
        return final_weights_list

    async def _calc_task_weights(self):
        """Calculate weights based on recent task scores. Async part fetches data."""
        try:
            now = datetime.now(timezone.utc)
            one_day_ago = now - timedelta(days=1)
            
            query = """
            SELECT score, created_at 
            FROM score_table 
            WHERE task_name = :task_name AND created_at >= :start_time ORDER BY created_at DESC
            """
            weather_query = """
            SELECT score, created_at 
            FROM score_table 
            WHERE task_name = :task_name ORDER BY created_at DESC LIMIT 1
            """
            
            weather_results = await self.database_manager.fetch_all(weather_query, {"task_name": "weather"})
            logger.info(f"Fetched {len(weather_results)} weather score rows")
            geomagnetic_results = await self.database_manager.fetch_all(query, {"task_name": "geomagnetic", "start_time": one_day_ago})
            logger.info(f"Fetched {len(geomagnetic_results)} geomagnetic score rows")
            soil_results = await self.database_manager.fetch_all(query, {"task_name": "soil_moisture", "start_time": one_day_ago})
            logger.info(f"Fetched {len(soil_results)} soil moisture score rows")
            
            if not weather_results and not geomagnetic_results and not soil_results:
                logger.info("No scores found in DB - returning zero weights (no scores to base weights on)")
                # Return zero weights array instead of equal weights
                weights_arr = np.zeros(256)
                logger.info("Initialized zero weights for all nodes (no scoring data available)")
                return weights_arr.tolist()

            active_nodes_list_sync = []
            try:
                # Ensure this is a synchronous call if get_nodes_for_netuid can be blocking.
                # For now, assuming it's relatively quick or primarily I/O bound with Substrate.
                active_nodes_list_from_chain = get_nodes_for_netuid(self.substrate, self.metagraph.netuid)
                if active_nodes_list_from_chain is None: active_nodes_list_from_chain = []
                active_nodes_list_sync = active_nodes_list_from_chain # Use this list for the sync method
            except Exception as e_fetch_nodes:
                logger.error(f"Failed to fetch nodes for weight calc: {e_fetch_nodes}", exc_info=True)
                # active_nodes_list_sync remains empty, sync function should handle this
            
            validator_nodes_by_uid_list_sync = [None] * 256
            for node in active_nodes_list_sync:
                if node.node_id < 256:
                    validator_nodes_by_uid_list_sync[node.node_id] = node
            
            return await asyncio.to_thread(
                self._perform_weight_calculations_sync,
                weather_results, geomagnetic_results, soil_results, now, validator_nodes_by_uid_list_sync
            )
        except Exception as e:
            logger.error(f"Error in _calc_task_weights (async part): {e}", exc_info=True)
            return None

    async def update_last_weights_block(self):
        try:
            resp = self.substrate.rpc_request("chain_getHeader", [])  
            hex_num = resp["result"]["number"]
            block_number = int(hex_num, 16)
            self.last_set_weights_block = block_number
        except Exception as e:
            logger.error(f"Error updating last weights block: {e}")

    async def manage_earthdata_token(self):
        """Periodically checks and refreshes the Earthdata token."""
        while not self._shutdown_event.is_set():
            try:
                logger.info("Running Earthdata token check...")
                token = await ensure_valid_earthdata_token()
                if token:
                    logger.info(f"Earthdata token check successful. Current token (first 10 chars): {token[:10]}...")
                else:
                    logger.warning("Earthdata token check failed or no token available.")

                await asyncio.sleep(86400) # Check daily

            except asyncio.CancelledError:
                logger.info("Earthdata token management task cancelled.")
            except Exception as e:
                logger.error(f"Error in Earthdata token management task: {e}", exc_info=True)
                await asyncio.sleep(3600) # Retry in an hour if there was an error

    async def _initialize_db_sync_components(self):
        logger.info("Attempting to initialize DB Sync components...")
        
        db_sync_enabled_str = os.getenv("DB_SYNC_ENABLED", "True") # Default to True if not set
        if db_sync_enabled_str.lower() != "true":
            logger.info("DB_SYNC_ENABLED is not 'true'. Database synchronization feature will be disabled.")
            self.azure_blob_manager_for_sync = None
            self.backup_manager = None
            self.restore_manager = None
            return

        self.azure_blob_manager_for_sync = await get_azure_blob_manager_for_db_sync()
        if not self.azure_blob_manager_for_sync:
            logger.error("Failed to initialize AzureBlobManager for DB Sync. DB Sync will be disabled.")
            return

        if self.is_source_validator_for_db_sync:
            logger.info("This node is configured as the SOURCE for DB Sync.")
            self.backup_manager = await get_backup_manager(self.azure_blob_manager_for_sync)
            if not self.backup_manager:
                logger.error("Failed to initialize BackupManager. DB Sync (source) will be disabled.")
        else:
            logger.info("This node is configured as a REPLICA for DB Sync.")
            self.restore_manager = await get_restore_manager(self.azure_blob_manager_for_sync)
            if not self.restore_manager:
                logger.error("Failed to initialize RestoreManager. DB Sync (replica) will be disabled.")
        logger.info("DB Sync components initialization attempt finished.")

    async def database_monitor(self):
        """Periodically query and log database statistics."""
        logger.info("Starting database monitor task...")
        while not self._shutdown_event.is_set():
            await asyncio.sleep(60) # Check every 60 seconds

            current_timestamp_iso = datetime.now(timezone.utc).isoformat()
            collected_stats = {
                "timestamp": current_timestamp_iso, # Add timestamp at the start
                "connection_summary": "[Query Failed or No Data]",
                "null_state_connection_details": "[Query Failed or No Data for NULL state details]",
                "idle_in_transaction_details": "[Query Failed or No Data]",
                "lock_details": "[Query Failed or No Data]",
                "active_query_wait_events": "[Query Failed or No Data]",
                "session_manager_stats": "[Query Failed or No Data]", # New key for our session stats
                "error": None
            }

            try:
                # Fetch general operation/session stats from DatabaseManager
                try:
                    # session_manager_specific_stats = self.database_manager.get_session_stats()
                    # Corrected call for all stats including session stats:
                    all_db_manager_stats = self.database_manager.get_session_stats() # Renamed for clarity
                    collected_stats["session_manager_stats"] = all_db_manager_stats

                except Exception as e:
                    collected_stats["session_manager_stats"] = f"[Error fetching session manager stats: {type(e).__name__}]"
                    logger.warning(f"[DB Monitor] Error fetching session manager stats: {e}")

                # Query overall connection summary
                conn_summary_query = "SELECT state, count(*) FROM pg_stat_activity GROUP BY state;"
                try:
                    collected_stats["connection_summary"] = await self.database_manager.fetch_all(conn_summary_query, timeout=45.0)
                except DatabaseTimeout:
                    collected_stats["connection_summary"] = "[Query Timed Out]"
                    logger.warning("[DB Monitor] Timeout fetching connection summary.")
                except Exception as e:
                    collected_stats["connection_summary"] = f"[Query Error: {type(e).__name__}]"
                    logger.warning(f"[DB Monitor] Error fetching connection summary: {e}")

                # Query details for NULL state connections
                null_state_query = """
                SELECT pid, usename, application_name, client_addr, backend_start, state, backend_type, query
                FROM pg_stat_activity 
                WHERE state IS NULL;
                """
                try:
                    collected_stats["null_state_connection_details"] = await self.database_manager.fetch_all(null_state_query, timeout=45.0)
                except DatabaseTimeout:
                    collected_stats["null_state_connection_details"] = "[Query Timed Out for NULL state details]"
                    logger.warning("[DB Monitor] Timeout fetching NULL state connection details.")
                except Exception as e:
                    collected_stats["null_state_connection_details"] = f"[Query Error for NULL state details: {type(e).__name__}]"
                    logger.warning(f"[DB Monitor] Error fetching NULL state connection details: {e}")
                
                # Query details for 'idle in transaction' connections
                idle_in_transaction_query = """
                SELECT pid, usename, application_name, client_addr, backend_start, state_change, query_start, query 
                FROM pg_stat_activity 
                WHERE state = 'idle in transaction' 
                ORDER BY state_change ASC;
                """
                try:
                    collected_stats["idle_in_transaction_details"] = await self.database_manager.fetch_all(idle_in_transaction_query, timeout=45.0)
                except DatabaseTimeout:
                    collected_stats["idle_in_transaction_details"] = "[Query Timed Out]"
                    logger.warning("[DB Monitor] Timeout fetching idle in transaction details.")
                except Exception as e:
                    collected_stats["idle_in_transaction_details"] = f"[Query Error: {type(e).__name__}]"
                    logger.warning(f"[DB Monitor] Error fetching idle in transaction details: {e}")

                # Query for lock contention
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
                    logger.warning("[DB Monitor] Timeout fetching lock details.")
                except Exception as e:
                    collected_stats["lock_details"] = f"[Query Error: {type(e).__name__}]"
                    logger.warning(f"[DB Monitor] Error fetching lock details: {e}")

                # Query for top active queries by wait events (excluding background workers and idle)
                active_query_wait_events_query = """
                SELECT 
                    pid, 
                    usename, 
                    application_name,
                    query, 
                    wait_event_type, 
                    wait_event,
                    age(now(), query_start) as query_age,
                    state
                FROM pg_stat_activity 
                WHERE state = 'active'
                  AND backend_type = 'client backend'
                  AND pid != pg_backend_pid()
                ORDER BY query_start ASC;
                """
                try:
                    collected_stats["active_query_wait_events"] = await self.database_manager.fetch_all(active_query_wait_events_query, timeout=45.0)
                except DatabaseTimeout:
                    collected_stats["active_query_wait_events"] = "[Query Timed Out]"
                    logger.warning("[DB Monitor] Timeout fetching active query wait events.")
                except Exception as e:
                    collected_stats["active_query_wait_events"] = f"[Query Error: {type(e).__name__}]"
                    logger.warning(f"[DB Monitor] Error fetching active query wait events: {e}")

            except Exception as e_outer:
                collected_stats["error"] = f"Outer error in database_monitor: {str(e_outer)}"
                logger.error(f"[DB Monitor] Outer error: {e_outer}")
                logger.error(traceback.format_exc())
            
            # Store collected stats
            async with self.db_monitor_history_lock:
                self.db_monitor_history.append(collected_stats)
                if len(self.db_monitor_history) > self.DB_MONITOR_HISTORY_MAX_SIZE:
                    self.db_monitor_history.pop(0) # Remove the oldest entry

            # Log all collected stats
            log_output = "[DB Monitor] Stats:\n"
            for key, value in collected_stats.items():
                if key == "error" and value is None: 
                    continue
                try:
                    # Pretty print if it's a list of dicts (likely query results)
                    if isinstance(value, list) and all(isinstance(item, dict) for item in value):
                        log_output += f"  {key.replace('_', ' ').title()}:\n"
                        if not value: # Empty list
                             log_output += "  []\n"
                        else:
                            for item_dict in value:
                                log_output += "  {\n"
                                for k_item, v_item in item_dict.items():
                                    if isinstance(v_item, datetime):
                                        v_item_str = v_item.isoformat()
                                    else:
                                        v_item_str = str(v_item)
                                    log_output += f"    {k_item}: {v_item_str}\n"
                                log_output += "  }\n"
                            if len(value) > 1 : log_output += "  ...\n" # Indicate if more items not shown in detail for brevity if needed
                        if isinstance(value, list) and len(value) > 0 and not all(isinstance(item, dict) for item in value): # list of non-dicts
                             log_output += f"  {json.dumps(value, indent=2, default=str)}\n"
                    elif isinstance(value, str) and (value.startswith("[Query Timed Out") or value.startswith("[Query Error") or value.startswith("[Query Failed")):
                        log_output += f"  {key.replace('_', ' ').title()}: {value}\n"

                    else: # For simple values or non-list-of-dict structures
                        log_output += f"  {key.replace('_', ' ').title()}: {json.dumps(value, indent=2, default=str)}\n"
                except Exception as e_log:
                    log_output += f"  Error formatting log for {key}: {e_log}\n"
            
            logger.info(log_output)
            gc.collect()

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

    async def monitor_client_health(self):
        """Monitor HTTP client connection pool health."""
        while not self._shutdown_event.is_set():
            try:
                if hasattr(self, 'miner_client') and hasattr(self.miner_client, '_transport'):
                    transport = self.miner_client._transport
                    if hasattr(transport, '_pool'):
                        pool = transport._pool
                        if hasattr(pool, '_connections'):
                            connections = pool._connections
                            total_connections = len(connections)
                            
                            # Handle both list and dict cases for _connections
                            if hasattr(connections, 'values'):  # It's a dict-like object
                                keepalive_connections = sum(1 for conn in connections.values() 
                                                          if hasattr(conn, '_keepalive_expiry') and conn._keepalive_expiry)
                            else:  # It's a list-like object
                                keepalive_connections = sum(1 for conn in connections 
                                                          if hasattr(conn, '_keepalive_expiry') and conn._keepalive_expiry)
                            
                            # Get pool limit - check multiple possible locations
                            pool_limit = "unknown"
                            if hasattr(self.miner_client, '_limits') and hasattr(self.miner_client._limits, 'max_connections'):
                                pool_limit = self.miner_client._limits.max_connections
                            elif hasattr(pool, '_max_connections'):
                                pool_limit = pool._max_connections
                            elif hasattr(pool, 'max_connections'):
                                pool_limit = pool.max_connections
                            
                            logger.debug(f"HTTP Client Pool Health - Total: {total_connections}, "
                                       f"Keepalive: {keepalive_connections}, "
                                       f"Pool limit: {pool_limit}")
                await asyncio.sleep(300)  # Check every 5 minutes
            except Exception as e:
                logger.debug(f"Error monitoring client health: {e}")
                await asyncio.sleep(300)


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

    # --- Alembic check code for Validator ---
    # No need to set DB_TARGET since we have separate configurations now
    logger.info(f"[Startup] Using validator-specific Alembic configuration")

    try:
        print("[Startup] Checking database schema version using Alembic...")
        # Construct path relative to this script file to find alembic_validator.ini at project root
        # Assumes validator.py is in project_root/gaia/validator/validator.py
        current_script_dir_val = os.path.dirname(os.path.abspath(__file__))
        project_root_val = os.path.abspath(os.path.join(current_script_dir_val, "..", ".."))
        alembic_ini_path_val = os.path.join(project_root_val, "alembic_validator.ini")

        if not os.path.exists(alembic_ini_path_val):
            print(f"[Startup] ERROR: alembic_validator.ini not found at expected path: {alembic_ini_path_val}")
            sys.exit("Alembic validator configuration not found.")

        alembic_cfg_val = Config(alembic_ini_path_val)
        print(f"[Startup] Validator: Using Alembic configuration from: {alembic_ini_path_val}")

        # Diagnostic block for validator
        try:
            from alembic.script import ScriptDirectory
            script_dir_instance_val = ScriptDirectory.from_config(alembic_cfg_val)
            print(f"[Startup] Validator DIAGNOSTIC: ScriptDirectory main path: {script_dir_instance_val.dir}")
            print(f"[Startup] Validator DIAGNOSTIC: ScriptDirectory version_locations: {script_dir_instance_val.version_locations}")
        except Exception as e_diag_val:
            print(f"[Startup] Validator DIAGNOSTIC: Error: {e_diag_val}")

        alembic_auto_upgrade_val = os.getenv("ALEMBIC_AUTO_UPGRADE", "True").lower() in ["true", "1", "yes"]
        if alembic_auto_upgrade_val:
            print(f"[Startup] Validator: ALEMBIC_AUTO_UPGRADE is True. Attempting to upgrade to head...")
            
            # Construct database URL for Alembic check
            db_host = os.getenv("DB_HOST", "localhost")
            db_port = os.getenv("DB_PORT", "5432")
            db_name = os.getenv("DB_NAME", "validator_db")
            db_user = os.getenv("DB_USER", "postgres")
            db_password = os.getenv("DB_PASSWORD", "postgres")
            db_url = f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            
            # Safety check: Verify no data-destructive migrations are pending
            try:
                from alembic.script import ScriptDirectory
                from alembic.runtime.migration import MigrationContext
                
                script_dir = ScriptDirectory.from_config(alembic_cfg_val)
                
                # Check current revision
                with create_engine(db_url, poolclass=pool.NullPool).connect() as conn:
                    context = MigrationContext.configure(conn)
                    current_rev = context.get_current_revision()
                    
                print(f"[Startup] Current database revision: {current_rev}")
                print(f"[Startup] Target revision: head")
                
                # Get pending migrations
                if current_rev:
                    pending_revisions = script_dir.get_revisions(current_rev, "head")
                    if pending_revisions:
                        print(f"[Startup] Found {len(pending_revisions)} pending migration(s)")
                        for rev in pending_revisions:
                            print(f"[Startup] Pending: {rev.revision} - {rev.doc}")
                    else:
                        print("[Startup] No pending migrations - database is up to date")
                        
            except Exception as e:
                print(f"[Startup] Warning: Could not check migration status: {e}")
            
            command.upgrade(alembic_cfg_val, "head")
            print("[Startup] Validator: Database schema is up-to-date (or upgrade attempted).")
        else:
            print("[Startup] Validator: ALEMBIC_AUTO_UPGRADE is False. Skipping auto schema upgrade.")

    except CommandError as e_val:
         print(f"[Startup] Validator: ERROR: Alembic command failed: {e_val}")
    except Exception as e_val_outer: # Catch other potential errors like path issues
        print(f"[Startup] Validator: ERROR during Alembic setup: {e_val_outer}", exc_info=True)

    logger.info("Validator Alembic check complete. Starting main validator application...")
    # --- End Alembic check code for Validator ---

    validator = GaiaValidator(args)
    try:
        asyncio.run(validator.main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    except Exception as e:
        logger.critical(f"Unhandled exception in main loop: {e}", exc_info=True)
    finally:

        if hasattr(validator, '_cleanup_done') and not validator._cleanup_done:
             try:
                 loop = asyncio.get_event_loop()
                 if loop.is_closed():
                     loop = asyncio.new_event_loop()
                     asyncio.set_event_loop(loop)
                 loop.run_until_complete(validator._initiate_shutdown())
             except Exception as cleanup_e:
                 logger.error(f"Error during final cleanup: {cleanup_e}")
