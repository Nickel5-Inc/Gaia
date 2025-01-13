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
from typing import Any, Optional, List, Dict
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
            test_mode=args.test_soil,
        )
        self.geomagnetic_task = GeomagneticTask(db_manager=self.database_manager)
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
            # Set shutdown event - will be processed in main loop
            asyncio.create_task(self._initiate_shutdown())
            
    async def _initiate_shutdown(self):
        """Initiate graceful shutdown sequence."""
        try:
            logger.info("Initiating graceful shutdown...")
            self._shutdown_event.set()
            
            # Stop accepting new tasks
            logger.info("Stopping task acceptance...")
            await self.update_task_status('all', 'stopping')
            
            # Wait briefly for current operations to complete
            await asyncio.sleep(5)
            
            # Cleanup resources
            await self.cleanup_resources()
            
            # Stop the watchdog
            await self.stop_watchdog()
            
            # Close database connections
            if hasattr(self, 'database_manager'):
                logger.info("Closing database connections...")
                await self.database_manager.close_all_connections()
            
            self._cleanup_done = True
            logger.info("Graceful shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            logger.error(traceback.format_exc())
        finally:
            # Force exit after cleanup
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
                logger.debug(
                    f"TIFF data size before serialization: {len(payload['data']['combined_data'])} bytes"
                )
                if isinstance(payload["data"]["combined_data"], bytes):
                    logger.debug(
                        f"TIFF header before serialization: {payload['data']['combined_data'][:4]}"
                    )

            responses = {}
            self.metagraph.sync_nodes()
            
            for miner_hotkey, node in self.metagraph.nodes.items():
                base_url = f"https://{node.ip}:{node.port}"

                try:
                    # Handshake with timeout
                    handshake_timeout = 30  # 30 seconds for handshake
                    handshake_result = await asyncio.wait_for(
                        handshake.perform_handshake(
                            keypair=self.keypair,
                            httpx_client=self.httpx_client,
                            server_address=base_url,
                            miner_hotkey_ss58_address=miner_hotkey,
                        ),
                        timeout=handshake_timeout
                    )
                    
                    symmetric_key_str, symmetric_key_uuid = handshake_result

                    if symmetric_key_str and symmetric_key_uuid:
                        logger.info(f"Handshake successful with miner {miner_hotkey}")
                        fernet = Fernet(symmetric_key_str)

                        # Query with timeout
                        query_timeout = 180  # 3 minutes for actual query
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
                            timeout=query_timeout
                        )

                        response_data = {
                            "text": resp.text,
                            "hotkey": miner_hotkey,
                            "port": node.port,
                            "ip": node.ip,
                        }
                        responses[miner_hotkey] = response_data
                        logger.info(f"Successfully completed request to {miner_hotkey}")
                    else:
                        logger.warning(f"Failed handshake with miner {miner_hotkey}")

                except asyncio.TimeoutError as e:
                    logger.warning(f"Timeout for miner {miner_hotkey}: {str(e)}")
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

            if not responses:
                logger.warning("No successful responses from any miners")
            else:
                logger.info(f"Successfully queried {len(responses)} miners")

            return responses

        except Exception as e:
            logger.error(f"Error in query_miners: {str(e)}")
            logger.error(traceback.format_exc())
            return {}

    async def check_for_updates(self):
        """Check for and apply updates every 2 minutes."""
        while True:
            try:
                logger.info("Checking for updates...")
                update_successful = await perform_update(self)

                if update_successful:
                    logger.info("Update completed successfully")
                else:
                    logger.debug("No updates available or update failed")

            except Exception as e:
                logger.error(f"Error in update checker: {e}")
                logger.error(traceback.format_exc())

            await asyncio.sleep(120)

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
                await self._watchdog_check()
            except Exception as e:
                logger.error(f"Error in watchdog loop: {e}")
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
            except ImportError:
                pass

            for task_name, health in self.task_health.items():
                if health['status'] == 'idle':
                    continue
                    
                timeout = health['timeouts'].get(
                    health.get('current_operation'),
                    health['timeouts']['default']
                )
                
                if health['operation_start'] and current_time - health['operation_start'] > timeout:
                    # Enhanced logging for timeout detection
                    operation_duration = current_time - health['operation_start']
                    logger.warning(
                        f"TIMEOUT_ALERT - Task: {task_name} | "
                        f"Operation: {health.get('current_operation')} | "
                        f"Duration: {operation_duration:.2f}s | "
                        f"Timeout: {timeout}s | "
                        f"Status: {health['status']} | "
                        f"Errors: {health['errors']}"
                    )
                    
                    # Log memory usage when timeout occurs
                    try:
                        import psutil
                        process = psutil.Process()
                        memory_info = process.memory_info()
                        logger.warning(
                            f"Memory Usage at Timeout - "
                            f"RSS: {memory_info.rss / 1024 / 1024:.2f}MB | "
                            f"VMS: {memory_info.vms / 1024 / 1024:.2f}MB"
                        )
                    except ImportError:
                        logger.warning("psutil not available for memory tracking")
                    
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
                    else:
                        logger.info(
                            f"Task {task_name} is still processing - "
                            f"Operation: {health.get('current_operation')} - "
                            f"Duration: {operation_duration:.2f}s"
                        )
            
            # Check database health
            if current_time - self.last_successful_db_check > self.db_check_interval:
                try:
                    db_check_start = time.time()
                    async with self.database_manager.async_session() as session:
                        await session.execute(text("SELECT 1"))
                    db_check_duration = time.time() - db_check_start
                    self.last_successful_db_check = time.time()
                    if db_check_duration > 5:  # Log slow DB checks
                        logger.warning(f"Slow DB health check: {db_check_duration:.2f}s")
                except Exception as e:
                    logger.error(f"Database health check failed: {e}")
                    try:
                        await self.database_manager.reset_pool()
                        logger.info("Successfully reset database pool")
                    except Exception as reset_error:
                        logger.error(f"Failed to reset database pool: {reset_error}")

            # Check metagraph sync health
            if current_time - self.last_metagraph_sync > self.metagraph_sync_interval:
                try:
                    sync_start = time.time()
                    self.metagraph.sync_nodes()
                    sync_duration = time.time() - sync_start
                    self.last_metagraph_sync = time.time()
                    if sync_duration > 30:  # Log slow syncs
                        logger.warning(f"Slow metagraph sync: {sync_duration:.2f}s")
                except Exception as e:
                    logger.error(f"Metagraph sync failed: {e}")

        except Exception as e:
            logger.error(f"Error in watchdog check: {e}")
            logger.error(traceback.format_exc())

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
                # Reset any hanging operations
                async with self.database_manager.async_session() as session:
                    # Reset processing predictions
                    await session.execute(text("""
                        UPDATE soil_moisture_predictions 
                        SET status = 'pending'
                        WHERE status = 'processing'
                    """))
                    await session.execute(text("""
                        UPDATE geomagnetic_predictions 
                        SET status = 'pending'
                        WHERE status = 'processing'
                    """))
                    # Clean up incomplete scoring operations
                    await session.execute(text("""
                        DELETE FROM score_table 
                        WHERE status = 'processing'
                    """))
                    await session.commit()
                logger.info("Reset hanging database operations")
                
                # Reset connection pool
                await self.database_manager.reset_pool()
                logger.info("Reset database connection pool")
                
            except Exception as e:
                logger.error(f"Error cleaning up database resources: {e}")

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
            await self.database_manager.initialize_database()

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
                self.check_for_updates()
            ]
            
            while not self._shutdown_event.is_set():
                try:
                    # Run all tasks concurrently in the same event loop
                    await asyncio.gather(*tasks)
                except Exception as e:
                    logger.error(f"Error in main task loop: {e}")
                    logger.error(traceback.format_exc())
                    await self.cleanup_resources()
                
                # Check shutdown event
                if self._shutdown_event.is_set():
                    break
                    
                await asyncio.sleep(1)
                
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
                    # Check conditions first
                    validator_uid = await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(
                            None,
                            lambda: self.substrate.query(
                                "SubtensorModule", 
                                "Uids", 
                                [self.netuid, self.keypair.ss58_address]
                            ).value
                        ),
                        timeout=30
                    )
                    
                    if validator_uid is None:
                        logger.error("Validator not found on chain")
                        await self.update_task_status('scoring', 'error')
                        await asyncio.sleep(12)
                        return False

                    blocks_since_update = await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(
                            None,
                            lambda: w.blocks_since_last_update(
                                self.substrate, 
                                self.netuid, 
                                validator_uid
                            )
                        ),
                        timeout=30
                    )

                    min_interval = await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(
                            None,
                            lambda: w.min_interval_to_set_weights(
                                self.substrate, 
                                self.netuid
                            )
                        ),
                        timeout=30
                    )

                    # Check if we recently set weights
                    current_block = self.substrate.get_block()["header"]["number"]
                    if current_block - self.last_set_weights_block < min_interval:
                        logger.info(f"Recently set weights {current_block - self.last_set_weights_block} blocks ago")
                        await self.update_task_status('scoring', 'idle', 'waiting')
                        await asyncio.sleep(12)
                        return True

                    # Only enter weight_setting state when actually setting weights
                    if (min_interval is None or 
                        (blocks_since_update is not None and blocks_since_update >= min_interval)):
                        
                        can_set = await asyncio.wait_for(
                            asyncio.get_event_loop().run_in_executor(
                                None,
                                lambda: w.can_set_weights(
                                    self.substrate, 
                                    self.netuid, 
                                    validator_uid
                                )
                            ),
                            timeout=30
                        )
                        
                        if can_set:
                            await self.update_task_status('scoring', 'processing', 'weight_setting')
                            normalized_weights = await self._calc_task_weights()
                            if normalized_weights:
                                success = await asyncio.wait_for(
                                    weight_setter.set_weights(normalized_weights),
                                    timeout=180
                                )
                                if success:
                                    self.last_set_weights_block = current_block
                                    self.last_successful_weight_set = time.time()
                                    logger.info("✅ Successfully set weights")
                                    await self.update_task_status('scoring', 'idle')
                                    await asyncio.sleep(30)
                                    await self.update_last_weights_block()
                    else:
                        logger.info(
                            f"Waiting for weight setting: {blocks_since_update}/{min_interval} blocks"
                        )
                        await self.update_task_status('scoring', 'idle', 'waiting')

                    await asyncio.sleep(12)
                    return True

                await asyncio.wait_for(scoring_cycle(), timeout=600)

            except asyncio.TimeoutError:
                logger.error("Weight setting operation timed out - restarting cycle")
                await self.update_task_status('scoring', 'error')
                try:
                    self.substrate = interface.get_substrate(subtensor_network=self.subtensor_network)
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

    async def handle_miner_deregistration_loop(self):
        """Run miner deregistration checks every 60 seconds."""
        while True:
            try:
                await self.update_task_status('deregistration', 'active')
                
                # Replace asyncio.timeout with a task + wait_for pattern
                async def check_deregistration():
                    self.last_dereg_check_start = time.time()
                    
                    await self.update_task_status('deregistration', 'processing', 'db_check')
                    self.metagraph.sync_nodes()
                    active_miners = {
                        idx: {"hotkey": hotkey, "uid": idx}
                        for idx, (hotkey, _) in enumerate(self.metagraph.nodes.items())
                    }

                    if not self.nodes:
                        try:
                            query = "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL"
                            rows = await asyncio.wait_for(
                                self.database_manager.fetch_many(query),
                                timeout=60
                            )
                            self.nodes = {
                                row["uid"]: {"hotkey": row["hotkey"], "uid": row["uid"]}
                                for row in rows
                            }
                        except asyncio.TimeoutError:
                            logger.error("Database query timed out")
                            await self.update_task_status('deregistration', 'error')
                            return

                    deregistered_miners = []
                    for uid, registered in self.nodes.items():
                        if active_miners[uid]["hotkey"] != registered["hotkey"]:
                            deregistered_miners.append(registered)

                    if deregistered_miners:
                        logger.info(f"Found {len(deregistered_miners)} deregistered miners")
                        
                        for miner in deregistered_miners:
                            self.nodes[miner["uid"]]["hotkey"] = active_miners[miner["uid"]]["hotkey"]

                        uids = [int(miner["uid"]) for miner in deregistered_miners]
                        
                        for idx in uids:
                            self.weights[idx] = 0.0
                        
                        # Process UIDs in chunks of 10 to prevent memory bloat
                        chunk_size = 10
                        for i in range(0, len(uids), chunk_size):
                            uid_chunk = uids[i:i + chunk_size]
                            logger.info(f"Processing score recalculation for UIDs {uid_chunk}")
                            
                            await self.update_task_status('deregistration', 'processing', f'recalculating_scores_{i}-{i+len(uid_chunk)}')
                            try:
                                await asyncio.wait_for(
                                    self.soil_task.recalculate_recent_scores(uid_chunk),
                                    timeout=300
                                )
                                await asyncio.wait_for(
                                    self.geomagnetic_task.recalculate_recent_scores(uid_chunk),
                                    timeout=300
                                )
                                logger.info(f"Successfully recalculated scores for UIDs {uid_chunk}")
                                
                                # Small delay between chunks
                                await asyncio.sleep(1)
                                
                                # Force garbage collection after each chunk
                                import gc
                                gc.collect()
                                
                            except asyncio.TimeoutError:
                                logger.error(f"Timeout recalculating scores for UIDs {uid_chunk}")
                                continue
                            except Exception as e:
                                logger.error(f"Error recalculating scores for UIDs {uid_chunk}: {e}")
                                continue
                        
                        logger.info(f"Successfully recalculated all scores for {len(uids)} miners")
                        self.last_successful_recalc = time.time()
                    
                    self.last_successful_dereg_check = time.time()
                    await self.update_task_status('deregistration', 'idle')

                await asyncio.wait_for(check_deregistration(), timeout=300)

            except asyncio.TimeoutError:
                logger.error("Deregistration loop timed out - restarting loop")
                await self.update_task_status('deregistration', 'error')
                continue
            except Exception as e:
                logger.error(f"Error in deregistration loop: {e}")
                logger.error(traceback.format_exc())
                await self.update_task_status('deregistration', 'error')
            finally:
                await asyncio.sleep(60)

    async def _calc_task_weights(self) -> Optional[List[float]]:
        """Calculate and normalize weights from task scores."""
        logger.info("Syncing metagraph nodes...")
        self.metagraph.sync_nodes()
        logger.info("Metagraph synced. Fetching recent scores...")

        three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
        
        query = """
        SELECT score 
        FROM score_table 
        WHERE task_name = :task_name
        AND created_at >= :start_time
        ORDER BY created_at DESC 
        LIMIT 1
        """
        
        geomagnetic_result = await self.database_manager.fetch_one(
            query, {"task_name": "geomagnetic", "start_time": three_days_ago}
        )
        soil_result = await self.database_manager.fetch_one(
            query, {"task_name": "soil_moisture", "start_time": three_days_ago}
        )

        geomagnetic_scores = geomagnetic_result["score"] if geomagnetic_result else [float("nan")] * 256
        soil_scores = soil_result["score"] if soil_result else [float("nan")] * 256

        logger.info("Recent scores fetched. Calculating aggregate scores...")

        weights = [0.0] * 256
        for idx in range(256):
            geomagnetic_score = geomagnetic_scores[idx]
            soil_score = soil_scores[idx]

            if math.isnan(geomagnetic_score) and math.isnan(soil_score):
                weights[idx] = 0.0
                logger.debug(f"Both scores nan - setting weight to 0")
            elif math.isnan(geomagnetic_score):
                weights[idx] = 0.5 * soil_score
                logger.debug(f"Geo score nan - using soil score: {weights[idx]}")
            elif math.isnan(soil_score):
                geo_normalized = math.exp(-abs(geomagnetic_score) / 10)
                weights[idx] = 0.5 * geo_normalized
                logger.debug(f"UID {idx}: Soil score nan - normalized geo score: {geo_normalized} -> weight: {weights[idx]}")
            else:
                geo_normalized = math.exp(-abs(geomagnetic_score) / 10)
                weights[idx] = (0.5 * geo_normalized) + (0.5 * soil_score)
                logger.debug(f"UID {idx}: Both scores valid - geo_norm: {geo_normalized}, soil: {soil_score} -> weight: {weights[idx]}")

            logger.info(f"UID {idx}: geo={geomagnetic_score} (norm={geo_normalized if 'geo_normalized' in locals() else 'nan'}), soil={soil_score}, weight={weights[idx]}")

        logger.info(f"Weights before normalization: {weights}")

        non_zero_weights = [w for w in weights if w != 0.0]
        if non_zero_weights:
            sorted_indices = sorted(
                range(len(weights)),
                key=lambda k: (weights[k] if weights[k] != 0.0 else float("-inf")),
            )

            new_weights = [0.0] * len(weights)
            for rank, idx in enumerate(sorted_indices):
                if weights[idx] != 0.0:
                    try:
                        normalized_rank = 1.0 - (rank / len(non_zero_weights))
                        exponent = max(min(-20 * (normalized_rank - 0.5), 709), -709)
                        new_weights[idx] = 1 / (1 + math.exp(exponent))
                    except OverflowError:
                        logger.warning(f"Overflow prevented for rank {rank}, idx {idx}")
                        if normalized_rank > 0.5:
                            new_weights[idx] = 1.0
                        else:
                            new_weights[idx] = 0.0

            total = sum(new_weights)
            if total > 0:
                return [w / total for w in new_weights]
            else:
                logger.warning("No positive weights after normalization")
                return None
        else:
            logger.warning("All weights are zero or nan, skipping weight setting")
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
        "--test-soil",
        action="store_true",
        help="Run soil moisture task immediately without waiting for windows",
    )

    args = parser.parse_args()

    validator = GaiaValidator(args)
    asyncio.run(validator.main())
