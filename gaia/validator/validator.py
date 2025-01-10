from datetime import datetime, timezone, timedelta
import os
import time

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
                    symmetric_key_str, symmetric_key_uuid = (
                        await handshake.perform_handshake(
                            keypair=self.keypair,
                            httpx_client=self.httpx_client,
                            server_address=base_url,
                            miner_hotkey_ss58_address=miner_hotkey,
                        )
                    )

                    if symmetric_key_str and symmetric_key_uuid:
                        logger.info(f"Handshake successful with miner {miner_hotkey}")
                        fernet = Fernet(symmetric_key_str)

                        resp = await vali_client.make_non_streamed_post(
                            httpx_client=self.httpx_client,
                            server_address=base_url,
                            fernet=fernet,
                            keypair=self.keypair,
                            symmetric_key_uuid=symmetric_key_uuid,
                            validator_ss58_address=self.keypair.ss58_address,
                            miner_ss58_address=miner_hotkey,
                            payload=payload,
                            endpoint=endpoint,
                        )

                        # resp.raise_for_status()
                        # logger.debug(f"Response from miner {miner_hotkey}: {resp}")
                        # logger.debug(f"Response text from miner {miner_hotkey}: {resp.headers}")
                        # Create a dictionary with both response text and metadata
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
                except httpx.HTTPStatusError as e:
                    logger.warning(f"HTTP error from miner {miner_hotkey}: {e}")
                    # logger.debug(f"Error details: {traceback.format_exc()}")
                    continue

                except httpx.RequestError as e:
                    logger.warning(f"Request error from miner {miner_hotkey}: {e}")
                    # logger.debug(f"Error details: {traceback.format_exc()}")
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
                    logger.info(f"Task {task_name} started operation: {operation}")
            elif status == 'idle':
                health['current_operation'] = None
                health['operation_start'] = None
                health['last_success'] = time.time()

    async def watchdog(self):
        """Enhanced watchdog to monitor validator health and recover from freezes."""
        while True:
            try:
                current_time = time.time()
                
                for task_name, health in self.task_health.items():
                    if health['status'] == 'idle':
                        continue
                        
                    timeout = health['timeouts'].get(
                        health.get('current_operation'),
                        health['timeouts']['default']
                    )
                    
                    if (health['operation_start'] and 
                        current_time - health['operation_start'] > timeout):
                        logger.warning(
                            f"Task {task_name} operation '{health.get('current_operation')}' "
                            f"exceeded timeout of {timeout} seconds"
                        )
                        
                        if health['status'] != 'processing':
                            logger.error(f"Task {task_name} appears frozen - triggering recovery")
                            try:
                                await self.recover_task(task_name)
                                health['errors'] = 0
                            except Exception as e:
                                logger.error(f"Failed to recover task {task_name}: {e}")
                                health['errors'] += 1
                        else:
                            logger.info(f"Task {task_name} is still processing - monitoring")
                
                # Check database health
                if current_time - self.last_successful_db_check > self.db_check_interval:
                    try:
                        async with self.database_manager.async_session() as session:
                            await session.execute(text("SELECT 1"))
                        self.last_successful_db_check = time.time()
                    except Exception as e:
                        logger.error(f"Database health check failed: {e}")
                        try:
                            await self.database_manager.reset_pool()
                        except Exception as reset_error:
                            logger.error(f"Failed to reset database pool: {reset_error}")

                # Check metagraph sync health
                if current_time - self.last_metagraph_sync > self.metagraph_sync_interval:
                    try:
                        self.metagraph.sync_nodes()
                        self.last_metagraph_sync = time.time()
                    except Exception as e:
                        logger.error(f"Metagraph sync failed: {e}")

            except Exception as e:
                logger.error(f"Error in watchdog: {e}")
            finally:
                await asyncio.sleep(60)

    async def recover_task(self, task_name: str):
        """Enhanced task recovery with specific handling for each task type."""
        logger.warning(f"Attempting to recover {task_name}")
        try:
            if task_name == "soil":
                await self.soil_task.cleanup_resources()
                await self.database_manager.reset_pool()
            elif task_name == "geomagnetic":
                await self.geomagnetic_task.cleanup_resources()
                await self.database_manager.reset_pool()
            elif task_name == "scoring":
                self.substrate = interface.get_substrate(subtensor_network=self.subtensor_network)
                self.metagraph.sync_nodes()
                await self.database_manager.reset_pool()
            elif task_name == "deregistration":
                self.metagraph.sync_nodes()
                await self.database_manager.reset_pool()
                self.nodes = {}
            
            health = self.task_health[task_name]
            health['errors'] = 0
            health['last_success'] = time.time()
            health['status'] = 'idle'
            health['current_operation'] = None
            health['operation_start'] = None
            
        except Exception as e:
            logger.error(f"Failed to recover {task_name}: {e}")
            logger.error(traceback.format_exc())

    async def main(self):
        """
        Main execution loop for the validator.
        """
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

        watchdog_task = asyncio.create_task(self.watchdog())
        
        tasks = {
            'geomagnetic': asyncio.create_task(self.geomagnetic_task.validator_execute(self)),
            'soil': asyncio.create_task(self.soil_task.validator_execute(self)),
            'status_logger': asyncio.create_task(self.status_logger()),
            'scoring': asyncio.create_task(self.main_scoring()),
            'deregistration': asyncio.create_task(self.handle_miner_deregistration_loop()),
            'updates': asyncio.create_task(self.check_for_updates())
            #TODO: add api sender back in here
        }
        
        while True:
            try:
                done, pending = await asyncio.wait(
                    tasks.values(),
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=300
                )

                for task in done:
                    task_name = next(name for name, t in tasks.items() if t == task)
                    try:
                        await task
                        logger.info(f"Task {task_name} completed normally")
                    except Exception as e:
                        logger.error(f"Task {task_name} failed with error: {e}")
                        logger.error(traceback.format_exc())
                    
                    # Restart the completed/failed task
                    if task_name == 'geomagnetic':
                        tasks[task_name] = asyncio.create_task(self.geomagnetic_task.validator_execute(self))
                    elif task_name == 'soil':
                        tasks[task_name] = asyncio.create_task(self.soil_task.validator_execute(self))
                    elif task_name == 'status_logger':
                        tasks[task_name] = asyncio.create_task(self.status_logger())
                    elif task_name == 'scoring':
                        tasks[task_name] = asyncio.create_task(self.main_scoring())
                    elif task_name == 'deregistration':
                        tasks[task_name] = asyncio.create_task(self.handle_miner_deregistration_loop())
                    elif task_name == 'updates':
                        tasks[task_name] = asyncio.create_task(self.check_for_updates())
                    
                    logger.info(f"Restarted task {task_name}")

                # Check health of pending tasks
                for task in pending:
                    if task.done():
                        task_name = next(name for name, t in tasks.items() if t == task)
                        try:
                            await task
                        except Exception as e:
                            logger.error(f"Pending task {task_name} failed with error: {e}")
                
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                logger.error(traceback.format_exc())
            
            await asyncio.sleep(5)

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
            logger.info(f"Synced {len(self.metagraph.nodes)} nodes from the network")

            for index, (hotkey, node) in enumerate(self.metagraph.nodes.items()):
                await self.database_manager.update_miner_info(
                    index=index,  # Use the enumerated index
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
                        
                        await self.update_task_status('deregistration', 'processing', 'recalculating_scores')
                        await asyncio.wait_for(
                            self.soil_task.recalculate_recent_scores(uids),
                            timeout=300
                        )
                        await asyncio.wait_for(
                            self.geomagnetic_task.recalculate_recent_scores(uids),
                            timeout=300
                        )
                        logger.info(f"Successfully recalculated scores for {len(uids)} miners")
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
