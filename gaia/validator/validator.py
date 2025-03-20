from datetime import datetime, timezone, timedelta
import os
import time
import threading
import concurrent.futures
import glob
import signal
import sys

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

logger = get_logger(__name__)


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
        self.weights = [0.0] * 256
        self.last_set_weights_block = 0
        self.current_block = 0
        self.nodes = {}

        # Initialize HTTP clients first
        # Client for miner communication with SSL verification disabled
        self.miner_client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            verify=False,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30,
            ),
            transport=httpx.AsyncHTTPTransport(retries=3),
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
                                                   loop=asyncio.get_event_loop(),
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

        try:
            logger.info("Initiating graceful shutdown...")
            
            # Set shutdown event first to prevent new operations
            self._shutdown_event.set()
            
            # Stop the watchdog if running
            if self.watchdog_running:
                await self.stop_watchdog()
                logger.info("Stopped watchdog")
            
            # Stop any running tasks
            for task_name in ['soil', 'geomagnetic']:
                try:
                    await self.update_task_status(task_name, 'stopping')
                except Exception as e:
                    logger.error(f"Error updating {task_name} task status: {e}")

            # Clean up all resources
            await self.cleanup_resources()
            
            # Final cleanup steps
            try:
                # Force final garbage collection
                import gc
                gc.collect()
                logger.info("Completed final garbage collection")
            except Exception as e:
                logger.error(f"Error during final garbage collection: {e}")
            
            self._cleanup_done = True
            logger.info("Graceful shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            logger.error(traceback.format_exc())
            # Don't raise here - we want to complete as much cleanup as possible

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
                current_block = int(substrate.get_block()["header"]["number"])
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

            self.substrate = SubstrateInterface(url=self.subtensor_chain_endpoint)
            self.metagraph = Metagraph(substrate=self.substrate, netuid=self.netuid)
            self.metagraph.sync_nodes()  # Sync nodes after initialization
            logger.info(f"Synced {len(self.metagraph.nodes)} nodes from the network")

            self.current_block = int(self.substrate.get_block()["header"]["number"])
            logger.info(f"Initial block number type: {type(self.current_block)}, value: {self.current_block}")
            self.last_set_weights_block = self.current_block - 300


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

    async def query_miners(self, payload: Dict, endpoint: str) -> Dict:
        """Query miners with the given payload in parallel."""
        try:
            logger.info(f"Querying miners with payload size: {len(str(payload))} bytes")
            if "data" in payload and "combined_data" in payload["data"]:
                logger.debug(f"TIFF data size before serialization: {len(payload['data']['combined_data'])} bytes")
                if isinstance(payload["data"]["combined_data"], bytes):
                    logger.debug(f"TIFF header before serialization: {payload['data']['combined_data'][:4]}")

            responses = {}
            self.metagraph.sync_nodes()

            # In test mode, select 10 random miners
            miners_to_query = self.metagraph.nodes
            if self.args.test and len(miners_to_query) > 10:
                hotkeys = random.sample(list(miners_to_query.keys()), 10)
                miners_to_query = {k: miners_to_query[k] for k in hotkeys}
                logger.info(f"Test mode: Selected {len(miners_to_query)} random miners to query")

            # Create a shared client with optimized connection pooling
            limits = httpx.Limits(max_keepalive_connections=20, max_connections=30, keepalive_expiry=30.0)
            async with httpx.AsyncClient(verify=False, timeout=30.0, limits=limits) as client:
                # Use semaphore to control concurrent requests
                semaphore = asyncio.Semaphore(10)  # Limit concurrent requests to 10

                async def query_single_miner(miner_hotkey: str, node: Any) -> Optional[Dict]:
                    """Query a single miner with proper handshake and error handling."""
                    base_url = f"https://{node.ip}:{node.port}"
                    try:
                        async with semaphore:  # Control concurrency
                            logger.info(f"Initiating handshake with miner {miner_hotkey} at {base_url}")
                            # Perform handshake with timeout
                            symmetric_key_str, symmetric_key_uuid = await asyncio.wait_for(
                                handshake.perform_handshake(
                                    keypair=self.keypair,
                                    httpx_client=client,  # Use shared client
                                    server_address=base_url,
                                    miner_hotkey_ss58_address=miner_hotkey,
                                ),
                                timeout=30.0
                            )

                            if not symmetric_key_str or not symmetric_key_uuid:
                                logger.warning(f"Failed handshake with miner {miner_hotkey}")
                                return None

                            logger.info(f"Handshake successful with miner {miner_hotkey}")
                            fernet = Fernet(symmetric_key_str)

                            # Make request with timeout using vali_client
                            logger.info(f"Sending request to miner {miner_hotkey} at {base_url}{endpoint}")
                            resp = await asyncio.wait_for(
                                vali_client.make_non_streamed_post(
                                    httpx_client=client,
                                    server_address=base_url,
                                    fernet=fernet,
                                    keypair=self.keypair,
                                    symmetric_key_uuid=symmetric_key_uuid,
                                    validator_ss58_address=self.keypair.ss58_address,
                                    miner_ss58_address=miner_hotkey,
                                    payload=payload,
                                    endpoint=endpoint,
                                ),
                                timeout=180.0
                            )

                            response_data = {
                                "text": resp.text,
                                "hotkey": miner_hotkey,
                                "port": node.port,
                                "ip": node.ip,
                            }
                            logger.info(f"Successfully received response from miner {miner_hotkey}")
                            return response_data

                    except asyncio.TimeoutError:
                        logger.warning(f"Timeout querying miner {miner_hotkey} at {base_url}")
                        return None
                    except Exception as e:
                        logger.warning(f"Failed request to miner {miner_hotkey} at {base_url}: {str(e)}")
                        return None

                # Query all miners in parallel with shared client
                tasks = []
                for hotkey, node in miners_to_query.items():
                    if node.ip and node.port:  # Only query miners with valid IP/port
                        tasks.append(query_single_miner(hotkey, node))
                    else:
                        logger.warning(f"Skipping miner {hotkey} - missing IP or port")

                # Gather responses with timeout
                miner_responses = await asyncio.gather(*tasks, return_exceptions=True)

                # Process responses
                for hotkey, response in zip(miners_to_query.keys(), miner_responses):
                    if response is not None and not isinstance(response, Exception):
                        responses[response['hotkey']] = response

            logger.info(f"Received {len(responses)} valid responses from miners")
            return responses

        except Exception as e:
            logger.error(f"Error querying miners: {e}")
            logger.error(traceback.format_exc())
            return {}

    async def check_for_updates(self):
        """Check for and apply updates every 2 minutes."""
        while True:
            try:
                logger.info("Checking for updates...")
                # Add timeout to prevent hanging
                try:
                    update_successful = await asyncio.wait_for(
                        perform_update(self),
                        timeout=60  # 60 second timeout
                    )

                    if update_successful:
                        logger.info("Update completed successfully")
                    else:
                        logger.debug("No updates available or update failed")

                except asyncio.TimeoutError:
                    logger.warning("Update check timed out after 30 seconds")
                except Exception as e:
                    if "500" in str(e):
                        logger.warning(f"GitHub temporarily unavailable (500 error): {e}")
                    else:
                        logger.error(f"Error in update checker: {e}")
                        logger.error(traceback.format_exc())

            except Exception as outer_e:
                logger.error(f"Outer error in update checker: {outer_e}")
                logger.error(traceback.format_exc())

            await asyncio.sleep(120)  # Check every 2 minutes

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
            
            logger.info("Completed resource cleanup")
            
        except Exception as e:
            logger.error(f"Error during resource cleanup: {e}")
            logger.error(traceback.format_exc())
            raise

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

    async def main(self):
        """Main execution loop for the validator."""
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

            logger.info("Initializing database tables...")
            await self.database_manager.initialize_database()
            logger.info("Database tables initialized.")

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

            logger.info("Updating miner table...")
            await self.update_miner_table()
            logger.info("Miner table updated.")

            logger.info("Starting watchdog...")
            await self.start_watchdog()
            logger.info("Watchdog started.")
            
            logger.info("Initializing baseline models...")
            await self.basemodel_evaluator.initialize_models()
            logger.info("Baseline models initialization complete")
            
            tasks = [
                lambda: self.geomagnetic_task.validator_execute(self),
                lambda: self.soil_task.validator_execute(self),
                lambda: self.status_logger(),
                lambda: self.main_scoring(),
                lambda: self.handle_miner_deregistration_loop(),
                #lambda: self.miner_score_sender.run_async(),
                lambda: self.check_for_updates()
            ]
            
            try:
                running_tasks = []
                while not self._shutdown_event.is_set():
                    running_tasks = [asyncio.create_task(t()) for t in tasks]
                    
                    # Wait for either shutdown event or task completion
                    done, pending = await asyncio.wait(
                        running_tasks + [asyncio.create_task(self._shutdown_event.wait())],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # If shutdown event is set, cancel remaining tasks
                    if self._shutdown_event.is_set():
                        logger.info("Shutdown event detected in main loop")
                        for task in running_tasks:
                            if not task.done():
                                task.cancel()
                        # Wait for tasks to cancel
                        await asyncio.gather(*running_tasks, return_exceptions=True)
                        break
                
                # Cleanup after loop exit
                logger.info("Main loop exited, initiating cleanup")
                await self._initiate_shutdown()
                
            except asyncio.CancelledError:
                logger.info("Tasks cancelled, proceeding with cleanup")
                if not self._cleanup_done:
                    await self._initiate_shutdown()
            except Exception as e:
                logger.error(f"Error in main task loop: {e}")
                logger.error(traceback.format_exc())
                if not self._cleanup_done:
                    await self._initiate_shutdown()
                
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
                        validator_uid = self.substrate.query(
                            "SubtensorModule", 
                            "Uids", 
                            [self.netuid, self.keypair.ss58_address]
                        ).value
                        
                        if validator_uid is None:
                            logger.error("Validator not found on chain")
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
                            current_block = int(self.substrate.get_block()["header"]["number"])
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
                        current_block = int(self.substrate.get_block()["header"]["number"])                        
                        if current_block - self.last_set_weights_block < min_interval:
                            logger.info(f"Recently set weights {current_block - self.last_set_weights_block} blocks ago")
                            await self.update_task_status('scoring', 'idle', 'waiting')
                            return True

                        # Only enter weight_setting state when actually setting weights
                        if (min_interval is None or 
                            (blocks_since_update is not None and blocks_since_update >= min_interval)):
                            
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
                        await asyncio.sleep(12)

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

    async def status_logger(self):
        """Log the status of the validator periodically."""
        while True:
            try:
                current_time_utc = datetime.now(timezone.utc)
                formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S")

                try:
                    block = self.substrate.get_block()
                    self.current_block = block["header"]["number"]
                    blocks_since_weights = (
                            self.current_block - self.last_set_weights_block
                    )
                except Exception as block_error:

                    try:
                        self.substrate = SubstrateInterface(
                            url=self.subtensor_chain_endpoint
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

    async def update_miner_table(self):
        """Update the miner table with the latest miner information from the metagraph."""
        try:
            if self.metagraph is None:
                logger.error("Metagraph not initialized")
                return

            async with self.miner_table_lock:
                self.metagraph.sync_nodes()
                total_nodes = len(self.metagraph.nodes)
                logger.info(f"Synced {total_nodes} nodes from the network")

                # Process miners in chunks of 32 to prevent memory bloat
                chunk_size = 32
                nodes_items = list(self.metagraph.nodes.items())

                for chunk_start in range(0, total_nodes, chunk_size):
                    chunk_end = min(chunk_start + chunk_size, total_nodes)
                    chunk = nodes_items[chunk_start:chunk_end]
                    
                    try:
                        for index, (hotkey, node) in enumerate(chunk, start=chunk_start):
                            await self.database_manager.update_miner_info(
                                index=index,
                                hotkey=node.hotkey,
                                coldkey=node.coldkey,
                                ip=node.ip,
                                ip_type=str(node.ip_type),
                                port=node.port,
                                incentive=float(node.incentive),
                                stake=float(node.stake),
                                trust=float(node.trust),
                                vtrust=float(node.vtrust),
                                protocol=str(node.protocol),
                            )
                            self.nodes[index] = {"hotkey": node.hotkey, "uid": index}
                            logger.debug(f"Updated information for node {index}")
                        
                        logger.info(f"Processed nodes {chunk_start} to {chunk_end-1}")
                        
                        # Small delay between chunks to prevent overwhelming the database
                        await asyncio.sleep(0.1)
                        
                    except Exception as chunk_error:
                        logger.error(f"Error processing chunk {chunk_start}-{chunk_end}: {str(chunk_error)}")
                        logger.error(traceback.format_exc())
                        # Continue with next chunk instead of failing completely
                        continue

                logger.info("Successfully updated miner table and in-memory state")

        except Exception as e:
            logger.error(f"Error updating miner table: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def handle_miner_deregistration_loop(self) -> None:
        logger.info("Starting deregistration loop")
        
        while True:
            try:
                # Ensure metagraph is up to date
                if not self.metagraph or not self.metagraph.nodes:
                    logger.info("Metagraph not ready, syncing nodes...")
                    self.metagraph.sync_nodes()

                async with self.miner_table_lock:
                    # Get set of all active hotkeys on chain
                    chain_hotkeys = {node.hotkey for node in self.metagraph.nodes.values()}
                    
                    # Get miners from our local state
                    query = "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL;"
                    rows = await self.database_manager.fetch_all(query)
                    db_hotkeys = {row["hotkey"] for row in rows}
                    
                    # Get in-memory state hotkeys
                    memory_hotkeys = {data["hotkey"] for data in self.nodes.values() if "hotkey" in data}
                    
                    # Check sync status
                    chain_count = len(chain_hotkeys)
                    db_count = len(db_hotkeys)
                    memory_count = len(memory_hotkeys)
                    
                    # Find any differences between states
                    db_not_in_chain = db_hotkeys - chain_hotkeys
                    memory_not_in_chain = memory_hotkeys - chain_hotkeys
                    memory_not_in_db = memory_hotkeys - db_hotkeys
                    
                    if not any([db_not_in_chain, memory_not_in_chain, memory_not_in_db]):
                        logger.info(f"All states in sync! Chain: {chain_count}, DB: {db_count}, Memory: {memory_count} miners")
                    else:
                        if db_not_in_chain:
                            logger.info(f"Found {len(db_not_in_chain)} miners in DB not on chain")
                        if memory_not_in_chain:
                            logger.info(f"Found {len(memory_not_in_chain)} miners in memory not on chain")
                        if memory_not_in_db:
                            logger.info(f"Found {len(memory_not_in_db)} miners in memory not in DB")
                    
                    # Find miners in our DB that are no longer on chain
                    for row in rows:
                        uid = row["uid"]
                        hotkey = row["hotkey"]
                        if hotkey not in chain_hotkeys:
                            logger.info(f"Detected deregistered miner: UID={uid}, hotkey={hotkey}")
                            try:
                                success = await self.database_manager.clear_miner_info(uid)
                                if success:
                                    logger.info(f"Cleared info for deregistered miner {uid} (hotkey: {hotkey})")
                                    # Update in-memory state to reflect removal
                                    if uid in self.nodes:
                                        del self.nodes[uid]
                                
                                # Update with chain's truth for this index
                                node = self.metagraph.nodes.get(uid)
                                if node:
                                    await self.database_manager.update_miner_info(
                                        index=uid,
                                        hotkey=node.hotkey,
                                        coldkey=node.coldkey,
                                        ip=node.ip,
                                        ip_type=str(node.ip_type),
                                        port=node.port,
                                        incentive=float(node.incentive),
                                        stake=float(node.stake),
                                        trust=float(node.trust),
                                        vtrust=float(node.vtrust),
                                        protocol=str(node.protocol),
                                    )
                                    if node.hotkey in chain_hotkeys:
                                        self.nodes[uid] = {"hotkey": node.hotkey, "uid": uid}
                                        logger.info(f"Updated index {uid} with new chain info")
                            except Exception as e:
                                logger.error(f"Error clearing miner {uid}: {str(e)}")
                                logger.error(traceback.format_exc())

                # Periodic sleep (5 minutes)
                await asyncio.sleep(300)

            except Exception as e:
                logger.error(f"Error in deregistration loop: {str(e)}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(60)

    async def _calc_task_weights(self):
        """Calculate weights based on recent task scores."""
        try:
            now = datetime.now(timezone.utc)
            one_day_ago = now - timedelta(days=1)
            
            query = """
            SELECT score, created_at 
            FROM score_table 
            WHERE task_name = :task_name
            AND created_at >= :start_time
            ORDER BY created_at DESC
            """
            
            # Fetch and analyze geomagnetic scores
            geomagnetic_results = await self.database_manager.fetch_all(
                query, {"task_name": "geomagnetic", "start_time": one_day_ago}
            )
            logger.info(f"Found {len(geomagnetic_results)} geomagnetic score rows")
            
            # Fetch soil moisture scores
            soil_results = await self.database_manager.fetch_all(
                query, {"task_name": "soil_moisture", "start_time": one_day_ago}
            )
            logger.info(f"Found {len(soil_results)} soil moisture score rows")
            
            # If no scores exist yet, return equal weights for all active nodes
            if not geomagnetic_results and not soil_results:
                logger.info("No scores found in database - initializing equal weights for active nodes")
                try:
                    # Get active nodes from metagraph
                    if not self.metagraph or not self.metagraph.nodes:
                        logger.warning("No active nodes found in metagraph")
                        return None
                        
                    active_nodes = len(self.metagraph.nodes)
                    # Create equal weights for active nodes
                    weights = np.zeros(256)
                    weight_value = 1.0 / active_nodes
                    
                    # Use the index in the nodes dictionary as the UID
                    for uid, (hotkey, node) in enumerate(self.metagraph.nodes.items()):
                        weights[uid] = weight_value
                        logger.debug(f"Set weight {weight_value:.4f} for node {hotkey} at index {uid}")
                    
                    logger.info(f"Initialized equal weights ({weight_value:.4f}) for {active_nodes} active nodes")
                    return weights.tolist()
                except Exception as e:
                    logger.error(f"Error initializing equal weights: {e}")
                    logger.error(traceback.format_exc())
                    return None

            # Initialize score arrays
            geomagnetic_scores = np.full(256, np.nan)
            soil_scores = np.full(256, np.nan)

            # Count raw scores per UID
            geo_counts = [0] * 256
            soil_counts = [0] * 256
            
            if geomagnetic_results:
                for result in geomagnetic_results:
                    scores = result['score']
                    for uid in range(256):
                        if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                            geo_counts[uid] += 1

            if soil_results:
                for result in soil_results:
                    scores = result['score']
                    for uid in range(256):
                        if not isinstance(scores[uid], str) and not np.isnan(scores[uid]) and scores[uid] != 0.0:
                            soil_counts[uid] += 1

            if geomagnetic_results:
                geo_scores_by_uid = [[] for _ in range(256)]
                zero_scores_count = 0
                all_zeros_count = 0
                
                for result in geomagnetic_results:
                    age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                    decay = np.exp(-age_days * np.log(2))  # Decay by half each day
                    scores = result['score']
                    for uid in range(256):
                        # Replace NaN with 0 for API compatibility
                        if isinstance(scores[uid], str) or np.isnan(scores[uid]):
                            scores[uid] = 0.0
                        geo_scores_by_uid[uid].append((scores[uid], decay))
                        if scores[uid] == 0.0:
                            zero_scores_count += 1
                
                logger.info(f"Loaded {len(geomagnetic_results)} geomagnetic score records with {zero_scores_count} zero scores")
                
                zeros_before = 0
                zeros_after = 0
                
                for uid in range(256):
                    if geo_scores_by_uid[uid]:
                        scores, decay_weights = zip(*geo_scores_by_uid[uid])
                        scores_array = np.array(scores)
                        weights_array = np.array(decay_weights)
                        
                        zero_mask = (scores_array == 0.0)
                        zero_count = np.sum(zero_mask)
                        total_count = len(scores_array)
                        
                        if np.all(zero_mask):
                            geomagnetic_scores[uid] = 0.0
                            zeros_after += 1
                            logger.debug(f"UID {uid}: All {total_count} geo scores were zero - final score zeroed")
                            continue
                            
                        masked_scores = scores_array[~zero_mask]
                        masked_weights = weights_array[~zero_mask]
                        
                        if np.any(zero_mask):
                            masked_count = np.sum(zero_mask)
                            total_count = len(zero_mask)
                            logger.debug(f"UID {uid}: Masked {masked_count}/{total_count} zero geo scores from averaging")
                        
                        weight_sum = np.sum(masked_weights)
                        if weight_sum > 0:
                            geomagnetic_scores[uid] = np.sum(masked_scores * masked_weights) / weight_sum
                            if geomagnetic_scores[uid] == 0.0:
                                zeros_after += 1
                
                for uid in range(256):
                    if not np.isnan(geomagnetic_scores[uid]) and geomagnetic_scores[uid] == 0.0:
                        zeros_before += 1
                
                logger.info(f"Geomagnetic masked array processing: {zeros_after} UIDs have zero final score")

            if soil_results:
                soil_scores_by_uid = [[] for _ in range(256)]
                for result in soil_results:
                    age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                    decay = np.exp(-age_days * np.log(2))
                    scores = result['score']
                    for uid in range(256):
                        if isinstance(scores[uid], str) or np.isnan(scores[uid]):
                            scores[uid] = 0.0
                        soil_scores_by_uid[uid].append((scores[uid], decay))
                
                total_soil_scores = len(soil_results) * 256
                zero_soil_scores = sum(scores[uid] == 0.0 for result in soil_results for uid in range(256) 
                                      if isinstance(scores := result['score'], list) and uid < len(scores))
                logger.info(f"Loaded {len(soil_results)} soil score records with {zero_soil_scores}/{total_soil_scores} zero scores")
                
                zeros_before = sum(1 for uid in range(256) if all(score == 0.0 for score, _ in soil_scores_by_uid[uid]))
                zeros_after = 0
                for uid in range(256):
                    if soil_scores_by_uid[uid]:
                        scores, decay_weights = zip(*soil_scores_by_uid[uid])
                        scores_array = np.array(scores)
                        weights_array = np.array(decay_weights)
                        zero_mask = (scores_array == 0.0)
                        
                        if np.all(zero_mask):
                            soil_scores[uid] = 0.0
                            zeros_after += 1
                            logger.debug(f"UID {uid}: All soil scores were zero, setting final score to zero")
                            continue
                            
                        masked_scores = scores_array[~zero_mask]
                        masked_weights = weights_array[~zero_mask]
                        
                        if np.any(zero_mask):
                            masked_count = np.sum(zero_mask)
                            total_count = len(zero_mask)
                            logger.debug(f"UID {uid}: Masked {masked_count}/{total_count} zero soil scores from averaging")
                        
                        weight_sum = np.sum(masked_weights)
                        if weight_sum > 0:
                            soil_scores[uid] = np.sum(masked_scores * masked_weights) / weight_sum
                        else:
                            soil_scores[uid] = 0.0
                            zeros_after += 1
                
                logger.info(f"Soil task: {zeros_before} UIDs had all zero scores before processing, {zeros_after} UIDs have zero final score after masked array processing")

            logger.info("Recent scores fetched and decay-weighted. Calculating aggregate scores...")

            # Use a sigmoid transformation for geo scores
            def sigmoid(x, k=20, x0=0.93):
                return 1 / (1 + math.exp(-k * (x - x0)))

            weights = np.zeros(256)
            for idx in range(256):
                geomagnetic_score = geomagnetic_scores[idx]
                soil_score = soil_scores[idx]

                # Treat 0.0 scores the same as NaN
                if np.isnan(geomagnetic_score) or geomagnetic_score == 0.0:
                    geomagnetic_score = np.nan
                if np.isnan(soil_score) or soil_score == 0.0:
                    soil_score = np.nan

                if np.isnan(geomagnetic_score) and np.isnan(soil_score):
                    weights[idx] = 0.0
                elif np.isnan(geomagnetic_score):
                    weights[idx] = 0.60 * soil_score
                elif np.isnan(soil_score):
                    weights[idx] = 0.40 * sigmoid(geomagnetic_score, k=20, x0=0.93)
                else:
                    weights[idx] = 0.40 * sigmoid(geomagnetic_score, k=20, x0=0.93) + 0.60 * soil_score

                logger.info(f"UID {idx}: geo={geomagnetic_score} ({geo_counts[idx]} scores), soil={soil_score} ({soil_counts[idx]} scores), weight={weights[idx]}")

            logger.info(f"Weights before normalization: {weights}")

            # generalized logistic curve
            non_zero_mask = weights != 0.0
            if np.any(non_zero_mask):
                non_zero_weights = weights[non_zero_mask]
                logger.info(f"Found {len(non_zero_weights)} non-zero weights out of 256 miners")
                logger.info(f"Non-zero weights stats: min={np.min(non_zero_weights):.8f}, max={np.max(non_zero_weights):.8f}, mean={np.mean(non_zero_weights):.8f}")
                max_weight = np.max(weights[non_zero_mask])
                normalized_weights = np.copy(weights)  # Start with a copy to preserve zeros
                normalized_weights[non_zero_mask] = weights[non_zero_mask] / max_weight  # Only normalize non-zeros
                
                positives = normalized_weights[normalized_weights > 0]
                if len(positives) > 0:
                    M = np.percentile(positives, 80)
                    logger.info(f"Using 80th percentile of non-zero weights: {M:.8f} as curve midpoint")
                else:
                    logger.warning("No positive weights found!")
                    return None
                    
                b = 30    # Growth rate (higher = steeper curve)
                Q = 5     # Initial value parameter (higher = sharper transition)
                v = 0.5   # Asymmetry (lower = more asymmetric curve)
                k = 0.95  # Upper asymptote (higher max value)
                a = 0.0   # Lower asymptote (keep at 0 to preserve zeros)
                slope = 0.02  # Tilt (small value to emphasize sigmoid shape)

                new_weights = np.zeros_like(weights)
                non_zero_indices = np.where(weights > 0.0)[0]
                
                if len(non_zero_indices) == 0:
                    logger.warning("No positive weight indices found!")
                    return None
                
                logger.info(f"Applying curve transformation to {len(non_zero_indices)} positive weights")
                
                for idx in non_zero_indices:
                    normalized_weight = normalized_weights[idx]
                    sigmoid_part = a + (k - a) / np.power(1 + Q * np.exp(-b * (normalized_weight - M)), 1/v)
                    linear_part = slope * normalized_weight
                    new_weights[idx] = sigmoid_part + linear_part
                
                transformed_non_zeros = new_weights[new_weights > 0]
                if len(transformed_non_zeros) > 0:
                    logger.info(f"Transformed weights stats: min={np.min(transformed_non_zeros):.8f}, max={np.max(transformed_non_zeros):.8f}, mean={np.mean(transformed_non_zeros):.8f}, unique={len(np.unique(transformed_non_zeros))}")
                
                if len(transformed_non_zeros) > 1 and np.std(transformed_non_zeros) < 0.01:
                    logger.warning(f"Transformed weights still too uniform! std={np.std(transformed_non_zeros):.8f}")
                    
                    logger.info("Switching to rank-based power law distribution")
                    sorted_indices = np.argsort(-weights)
                    new_weights = np.zeros_like(weights)
                    
                    positive_count = np.sum(weights > 0)
                    for i, idx in enumerate(sorted_indices[:positive_count]):
                        # Power-law formula: score ~ 1/rank^alpha
                        rank = i + 1
                        alpha = 0.7
                        new_weights[idx] = 1.0 / (rank ** alpha)
                    
                    rank_based_non_zeros = new_weights[new_weights > 0]
                    logger.info(f"Rank-based weights: min={np.min(rank_based_non_zeros):.8f}, max={np.max(rank_based_non_zeros):.8f}, mean={np.mean(rank_based_non_zeros):.8f}, std={np.std(rank_based_non_zeros):.8f}")

                non_zero_sum = np.sum(new_weights[new_weights > 0])
                if non_zero_sum > 0:
                    new_weights[new_weights > 0] = new_weights[new_weights > 0] / non_zero_sum
                    
                    final_non_zeros = new_weights[new_weights > 0]
                    if len(final_non_zeros) > 0:
                        logger.info(f"Final normalized weights: count={len(final_non_zeros)}, min={np.min(final_non_zeros):.8f}, max={np.max(final_non_zeros):.8f}, std={np.std(final_non_zeros):.8f}")
                        
                        unique_weights = np.unique(final_non_zeros)
                        logger.info(f"Found {len(unique_weights)} unique non-zero weight values")
                        
                        if len(unique_weights) < len(final_non_zeros) / 2:
                            logger.warning(f"Too few unique weights! {len(unique_weights)} unique values for {len(final_non_zeros)} non-zero miners")
                        
                        if np.max(final_non_zeros) > 0.90:
                            logger.warning(f"Warning: Max weight is very high: {np.max(final_non_zeros):.4f} - might concentrate too much influence")
                    
                    logger.info("Final normalized weights calculated")
                    return new_weights.tolist()
                else:
                    logger.warning("No positive weights after transformation!")
                    return None
            else:
                logger.warning("No non-zero weights found to normalize!")
                return None

        except Exception as e:
            logger.error(f"Error calculating weights: {e}")
            logger.error(traceback.format_exc())
            return None

    async def update_last_weights_block(self):
        try:
            block = self.substrate.get_block()
            block_number = int(block["header"]["number"])
            self.last_set_weights_block = block_number
        except Exception as e:
            logger.error(f"Error updating last weights block: {e}")


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

    validator = GaiaValidator(args)
    asyncio.run(validator.main())
