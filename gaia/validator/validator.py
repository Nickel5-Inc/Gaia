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

        self.miner_score_sender = MinerScoreSender(database_manager=self.database_manager,
                                                   loop=asyncio.get_event_loop())

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
        self.httpx_client = httpx.AsyncClient(
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

        self.watchdog_running = False

        # Setup signal handlers for graceful shutdown
        self._cleanup_done = False
        self._shutdown_event = asyncio.Event()
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, self._signal_handler)

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
        """Initiate graceful shutdown sequence."""
        if self._cleanup_done:
            logger.info("Cleanup already done, skipping")
            return
            
        try:
            logger.info("Initiating graceful shutdown...")
            self._shutdown_event.set()
            
            # Stop accepting new tasks
            logger.info("Stopping task acceptance...")
            await self.update_task_status('all', 'stopping')
            
            # Cancel any running tasks in the main loop
            for task in asyncio.all_tasks():
                if task != asyncio.current_task():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        logger.error(f"Error cancelling task: {e}")
            
            # Wait briefly for operations to complete
            await asyncio.sleep(5)
            
            # Cleanup resources
            await self.cleanup_resources()
            
            # Stop the watchdog
            await self.stop_watchdog()
            
            # Close database connections
            if hasattr(self, 'database_manager'):
                logger.info("Closing database connections...")
                await self.database_manager.close()
            
            self._cleanup_done = True
            
            # Create cleanup completion flag file
            cleanup_file = "/tmp/validator_cleanup_done"
            try:
                with open(cleanup_file, 'w') as f:
                    f.write(str(time.time()))
                logger.info("Created cleanup completion flag")
            except Exception as e:
                logger.error(f"Failed to create cleanup flag file: {e}")
            
            logger.info("Graceful shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            logger.error(traceback.format_exc())
        finally:
            # Only exit if not in a running event loop
            if not asyncio.get_event_loop().is_running():
                sys.exit(0)

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

            self.substrate = SubstrateInterface(url=self.subtensor_chain_endpoint)
            self.metagraph = Metagraph(substrate=self.substrate, netuid=self.netuid)
            self.metagraph.sync_nodes()  # Sync nodes after initialization
            logger.info(f"Synced {len(self.metagraph.nodes)} nodes from the network")

            self.current_block = self.substrate.get_block()["header"]["number"]
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
        """Query miners with the given payload."""
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

            for miner_hotkey, node in miners_to_query.items():
                base_url = f"https://{node.ip}:{node.port}"

                try:
                    symmetric_key_str, symmetric_key_uuid = await asyncio.wait_for(
                        handshake.perform_handshake(
                            keypair=self.keypair,
                            httpx_client=self.httpx_client,
                            server_address=base_url,
                            miner_hotkey_ss58_address=miner_hotkey,
                        ),
                        timeout=30.0
                    )

                    if symmetric_key_str and symmetric_key_uuid:
                        logger.info(f"Handshake successful with miner {miner_hotkey}")
                        fernet = Fernet(symmetric_key_str)

                        resp = await asyncio.wait_for(
                            vali_client.make_non_streamed_post(
                                httpx_client=self.httpx_client,
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
                        responses[miner_hotkey] = response_data
                        logger.info(f"Completed request to {miner_hotkey}")
                    else:
                        logger.warning(f"Failed handshake with miner {miner_hotkey}")

                except asyncio.TimeoutError as e:
                    logger.warning(f"Timeout for miner {miner_hotkey}: {e}")
                    continue
                except httpx.HTTPStatusError as e:
                    logger.warning(f"HTTP error from miner {miner_hotkey}: {e}")
                    continue
                except httpx.RequestError as e:
                    logger.warning(f"Request error from miner {miner_hotkey}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error with miner {miner_hotkey}: {e}")
                    logger.error(f"Error details: {traceback.format_exc()}")
                    continue

            return responses

        except Exception as e:
            logger.error(f"Error in query_miners: {str(e)}")
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
        """Clean up resources used by the validator."""
        logger.info("Starting comprehensive resource cleanup")
        
        try:
            # 1. Clean up temporary files
            temp_dir = "/tmp"
            temp_patterns = ["*.h5", "*.tif", "*.tiff", "*.tmp", "*.temp"]
            for pattern in temp_patterns:
                try:
                    for f in glob.glob(os.path.join(temp_dir, pattern)):
                        try:
                            os.unlink(f)
                            logger.debug(f"Cleaned up temp file: {f}")
                        except Exception as e:
                            logger.error(f"Failed to remove temp file {f}: {e}")
                except Exception as e:
                    logger.error(f"Error cleaning up {pattern} files: {e}")

            # 2. Clean up task resources
            for task_name in ['soil', 'geomagnetic']:
                try:
                    if task_name == 'soil':
                        await self.soil_task.cleanup_resources()
                    elif task_name == 'geomagnetic':
                        await self.geomagnetic_task.cleanup_resources()
                except Exception as e:
                    logger.error(f"Error cleaning up {task_name} task resources: {e}")

            # 3. Clean up database resources
            try:
                # Reset any hanging operations using database manager methods
                update_queries = [
                    "UPDATE soil_moisture_predictions SET status = 'pending' WHERE status = 'processing'",
                    "UPDATE geomagnetic_predictions SET status = 'pending' WHERE status = 'processing'",
                    "UPDATE score_table SET status = 'pending' WHERE status = 'processing'"
                ]
                
                for query in update_queries:
                    await self.database_manager.execute(query)
                
                logger.info("Reset hanging database operations")
                
                # Reset connection pool
                await self.database_manager.reset_pool()
                logger.info("Reset database connection pool")
                
            except Exception as e:
                logger.error(f"Error cleaning up database resources: {e}")
                logger.error(traceback.format_exc())

            # 4. Clean up task states
            for task_name, health in self.task_health.items():
                try:
                    health['status'] = 'idle'
                    health['current_operation'] = None
                    health['operation_start'] = None
                    health['errors'] = 0
                    health['resources'] = {
                        'memory_start': 0,
                        'memory_peak': 0,
                        'cpu_percent': 0,
                        'open_files': 0,
                        'threads': 0,
                        'last_update': None
                    }
                except Exception as e:
                    logger.error(f"Error resetting task state for {task_name}: {e}")

            # 5. Force garbage collection
            try:
                import gc
                gc.collect()
                logger.info("Forced garbage collection")
            except Exception as e:
                logger.error(f"Error during garbage collection: {e}")

            # 6. Clean up HTTP client
            try:
                await self.httpx_client.aclose()
                self.httpx_client = httpx.AsyncClient(
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
                logger.info("Reset HTTP client")
            except Exception as e:
                logger.error(f"Error resetting HTTP client: {e}")

            logger.info("Completed resource cleanup")

        except Exception as e:
            logger.error(f"Error during resource cleanup: {e}")
            logger.error(traceback.format_exc())

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
            await self.database_manager._initialize_validator_database()

            logger.info("Database tables initialized.")

            logger.info("Setting up HTTP client...")
            self.httpx_client = httpx.AsyncClient(
                timeout=30.0, follow_redirects=True, verify=False
            )
            logger.info("HTTP client setup complete.")

            logger.info("Updating miner table...")
            await self.update_miner_table()
            logger.info("Miner table updated.")

            logger.info("Starting watchdog...")
            await self.start_watchdog()
            logger.info("Watchdog started.")
            
            # Create all tasks in the same event loop
            tasks = [
                self.geomagnetic_task.validator_execute(self),
                self.soil_task.validator_execute(self),
                self.status_logger(),
                self.main_scoring(),
                self.handle_miner_deregistration_loop(),
                #self.miner_score_sender.run_async(), This sends miner data to the website - right now, data should be mostly identical across all validator nodes, so it is redudant for every validator node to send it until we expand to unique data for each validator.
                self.check_for_updates()
            ]
            
            try:
                running_tasks = []
                while not self._shutdown_event.is_set():
                    # Start all tasks
                    running_tasks = [asyncio.create_task(t) for t in tasks]
                    
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

                        blocks_since_update = w.blocks_since_last_update(
                            self.substrate, 
                            self.netuid, 
                            validator_uid
                        )

                        min_interval = w.min_interval_to_set_weights(
                            self.substrate, 
                            self.netuid
                        )

                            
                        # Get current block number synchronously
                        current_block = self.substrate.get_block()["header"]["number"]
                        
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
                                    timeout=60
                                )
                                
                                if normalized_weights:
                                    # Set weights with timeout
                                    success = await asyncio.wait_for(
                                        weight_setter.set_weights(normalized_weights),
                                        timeout=340
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
                await asyncio.wait_for(scoring_cycle(), timeout=600)

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
        """
        Continuously monitor for deregistered miners and handle cleanup of their data.
        """
        logger.info("Starting deregistration loop")
        
        # Initialize set for tracking active miners
        self._previously_active_miners: Set[int] = set()
        
        while True:
            try:
                # Ensure metagraph is synced before accessing
                if not hasattr(self.metagraph, 'nodes') or not self.metagraph.nodes:
                    logger.info("Metagraph not ready, syncing nodes...")
                    self.metagraph.sync_nodes()
                    logger.info(f"Synced {len(self.metagraph.nodes)} nodes from the network")
                
                # Get current set of active miners and track hotkey changes
                try:
                    current_active_miners: Set[int] = set()
                    hotkey_changes: List[int] = []
                    
                    for idx, (hotkey, node) in enumerate(self.metagraph.nodes.items()):
                        # Verify the node exists in our database with matching hotkey
                        miner_info = await self.database_manager.get_miner_info(idx)
                        if miner_info:
                            if miner_info["hotkey"] != node.hotkey:
                                logger.warning(f"Mismatch for index {idx}: DB hotkey != metagraph hotkey")
                                hotkey_changes.append(idx)
                            else:
                                current_active_miners.add(idx)
                        else:
                            current_active_miners.add(idx)  # New miner
                            
                except (TypeError, ValueError) as e:
                    logger.error(f"Error getting active miners from metagraph: {e}")
                    await asyncio.sleep(60)
                    continue
                
                # Skip first run to initialize tracking
                if not self._previously_active_miners:
                    self._previously_active_miners = current_active_miners
                    await asyncio.sleep(300)
                    continue
                
                # Find deregistered miners
                deregistered_miners: Set[int] = self._previously_active_miners - current_active_miners
                
                # Process both deregistered miners and hotkey changes
                miners_to_process = list(deregistered_miners) + hotkey_changes
                if miners_to_process:
                    logger.info(f"Processing {len(miners_to_process)} miners: {len(deregistered_miners)} deregistered, {len(hotkey_changes)} hotkey changes")
                    
                    # Process miners in chunks to prevent memory bloat
                    chunk_size = 10
                    for i in range(0, len(miners_to_process), chunk_size):
                        chunk = miners_to_process[i:i + chunk_size]
                        logger.info(f"Processing miner chunk: {chunk}")
                        
                        try:
                            # Recalculate scores for affected miners
                            recalc_tasks = []
                            if hasattr(self, 'soil_task'):
                                recalc_tasks.append(self.soil_task.recalculate_recent_scores(chunk))
                            if hasattr(self, 'geomagnetic_task'):
                                recalc_tasks.append(self.geomagnetic_task.recalculate_recent_scores(chunk))
                            
                            if recalc_tasks:
                                await asyncio.gather(*recalc_tasks)
                            
                            # Clear miner info from database
                            for miner_id in chunk:
                                try:
                                    if miner_id in hotkey_changes:
                                        # For hotkey changes, pass the new hotkey information
                                        new_node = self.metagraph.nodes[miner_id]
                                        await self.database_manager.clear_miner_info(
                                            miner_id,
                                            new_hotkey=new_node.hotkey,
                                            new_coldkey=new_node.coldkey
                                        )
                                        logger.info(f"Updated hotkey for miner {miner_id}")
                                    else:
                                        # For deregistered miners, just clear everything
                                        await self.database_manager.clear_miner_info(miner_id)
                                        logger.info(f"Cleared info for deregistered miner {miner_id}")
                                except Exception as db_error:
                                    logger.error(f"Error clearing miner {miner_id} info: {db_error}")
                                    continue
                                    
                            logger.info(f"Successfully processed miners: {chunk}")
                            
                        except Exception as e:
                            logger.error(f"Error processing miner chunk {chunk}: {str(e)}")
                            logger.error(traceback.format_exc())
                            continue
                        
                        # Small delay between chunks
                        await asyncio.sleep(1)
                
                # Update previously active miners
                self._previously_active_miners = current_active_miners
                
                # Sleep for 5 minutes before checking again
                await asyncio.sleep(300)
                
            except Exception as e:
                logger.error(f"Error in deregistration loop: {str(e)}")
                logger.error(traceback.format_exc())
                # Sleep for 1 minute on error before retrying
                await asyncio.sleep(60)
                continue

    async def _calc_task_weights(self):
        """Calculate weights based on recent task scores."""
        try:
            now = datetime.now(timezone.utc)
            three_days_ago = now - timedelta(days=3)
            
            query = """
            SELECT score, created_at 
            FROM score_table 
            WHERE task_name = :task_name
            AND created_at >= :start_time
            ORDER BY created_at DESC
            """
            
            geomagnetic_results = await self.database_manager.fetch_all(
                query, {"task_name": "geomagnetic", "start_time": three_days_ago}
            )
            soil_results = await self.database_manager.fetch_all(
                query, {"task_name": "soil_moisture", "start_time": three_days_ago}
            )

            # Initialize score arrays
            geomagnetic_scores = np.full(256, np.nan)
            soil_scores = np.full(256, np.nan)

            if geomagnetic_results:
                geo_scores_by_uid = [[] for _ in range(256)]
                for result in geomagnetic_results:
                    age_days = (now - result['created_at']).total_seconds() / (24 * 3600)
                    decay = np.exp(-age_days * np.log(2))  # Decay by half each day
                    scores = result['score']
                    for uid in range(256):
                        # Replace NaN with 0 for API compatibility
                        if isinstance(scores[uid], str) or np.isnan(scores[uid]):
                            scores[uid] = 0.0
                        geo_scores_by_uid[uid].append((scores[uid], decay))
                
                # Calculate weighted averages
                for uid in range(256):
                    if geo_scores_by_uid[uid]:
                        scores, weights = zip(*geo_scores_by_uid[uid])
                        scores = np.array(scores)
                        weights = np.array(weights)
                        weight_sum = np.sum(weights)
                        if weight_sum > 0:
                            geomagnetic_scores[uid] = np.sum(scores * weights) / weight_sum

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
                
                # Calculate weighted averages
                for uid in range(256):
                    if soil_scores_by_uid[uid]:
                        scores, weights = zip(*soil_scores_by_uid[uid])
                        scores = np.array(scores)
                        weights = np.array(weights)
                        weight_sum = np.sum(weights)
                        if weight_sum > 0:
                            soil_scores[uid] = np.sum(scores * weights) / weight_sum

            logger.info("Recent scores fetched and decay-weighted. Calculating aggregate scores...")

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
                    logger.debug(f"Both scores invalid - setting weight to 0")
                elif np.isnan(geomagnetic_score):
                    weights[idx] = 0.5 * soil_score
                    logger.debug(f"Geo score invalid - using soil score: {weights[idx]}")
                elif np.isnan(soil_score):
                    weights[idx] = 0.5 * geomagnetic_score
                    logger.debug(f"UID {idx}: Soil score invalid - geo score: {geomagnetic_score} -> weight: {weights[idx]}")
                else:
                    weights[idx] = (0.5 * geomagnetic_score) + (0.5 * soil_score)
                    logger.debug(f"UID {idx}: Both scores valid - geo: {geomagnetic_score}, soil: {soil_score} -> weight: {weights[idx]}")

                logger.info(f"UID {idx}: geo={geomagnetic_score}, soil={soil_score}, weight={weights[idx]}")

            logger.info(f"Weights before normalization: {weights}")

            # generalized logistic curve
            non_zero_mask = weights != 0.0
            if np.any(non_zero_mask):
                max_weight = np.max(weights[non_zero_mask])
                normalized_weights = np.where(non_zero_mask, weights/max_weight, 0.0)
                M = np.percentile(normalized_weights[normalized_weights > 0], 90)
                b = 25    # Growth rate
                Q = 4     # Initial value parameter
                v = 0.6   # Asymmetry
                k = 0.90  # Upper asymptote
                a = 0     # Lower asymptote
                slope = 0.029  # Tilt

                new_weights = np.zeros_like(weights)
                non_zero_indices = np.where(weights != 0.0)[0]
                for idx in non_zero_indices:
                    normalized_weight = weights[idx] / max_weight
                    sigmoid_part = a + (k - a) / np.power(1 + Q * np.exp(-b * (normalized_weight - M)), 1/v)
                    linear_part = slope * normalized_weight
                    new_weights[idx] = sigmoid_part + linear_part

                # Normalize final weights to sum to 1
                weight_sum = np.sum(new_weights)
                if weight_sum > 0:
                    new_weights = new_weights / weight_sum
                    logger.info(f"Final normalized weights calculated")
                    return new_weights.tolist()
            
            logger.warning("No valid weights calculated")
            return None

        except Exception as e:
            logger.error(f"Error calculating weights: {e}")
            logger.error(traceback.format_exc())
            return None

    async def update_last_weights_block(self):
        try:
            block = self.substrate.get_block()
            self.last_set_weights_block = block["header"]["number"]
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
