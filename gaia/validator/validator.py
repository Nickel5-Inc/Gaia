from datetime import datetime, timezone, timedelta
import os

os.environ["NODE_TYPE"] = "validator"
import asyncio
import ssl
import traceback
from typing import Any, Optional, List, Dict, Union
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import httpx
from fiber.chain import chain_utils, interface, metagraph, post_ip_to_chain, fetch_nodes
from fiber.logging_utils import get_logger
from fiber.validator import client as vali_client, handshake
from fiber.chain.metagraph import Metagraph
from substrateinterface import SubstrateInterface
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask
from gaia.tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from argparse import ArgumentParser
import pandas as pd
import json
from gaia.validator.weights.set_weights import set_node_weights, _get_hyperparameter, _can_set_weights
from fiber.chain.models import Node
import base64
from gaia import __spec_version__
import math
from gaia.validator.utils.auto_updater import perform_update
import contextlib

logger = get_logger(__name__)

class SubstratePool:
    """A simple connection pool for substrate connections."""
    def __init__(self, network: str, endpoint: str, max_connections: int = 3):
        self.network = network
        self.endpoint = endpoint
        self.max_connections = max_connections
        self.pool: List[SubstrateInterface] = []
        self.in_use: Dict[SubstrateInterface, bool] = {}
        self._lock = asyncio.Lock()
        
    async def get_connection(self) -> SubstrateInterface:
        """Get a connection from the pool or create a new one if possible."""
        async with self._lock:
            # First try to find an available connection
            for conn in self.pool:
                if not self.in_use[conn]:
                    if not conn.websocket:  # Connection died
                        self.pool.remove(conn)
                        del self.in_use[conn]
                        continue
                    self.in_use[conn] = True
                    return conn
            
            # If we have room, create a new connection
            if len(self.pool) < self.max_connections:
                conn = interface.get_substrate(
                    subtensor_network=self.network,
                    subtensor_address=self.endpoint
                )
                self.pool.append(conn)
                self.in_use[conn] = True
                return conn
            
            # Wait for a connection to become available
            while True:
                for conn in self.pool:
                    if not self.in_use[conn]:
                        if not conn.websocket:  # Connection died
                            self.pool.remove(conn)
                            del self.in_use[conn]
                            continue
                        self.in_use[conn] = True
                        return conn
                await asyncio.sleep(0.1)
    
    def release_connection(self, conn: SubstrateInterface):
        """Release a connection back to the pool."""
        if conn in self.in_use:
            self.in_use[conn] = False
    
    async def close_all(self):
        """Close all connections in the pool."""
        async with self._lock:
            for conn in self.pool:
                try:
                    conn.close()
                except:
                    pass
            self.pool.clear()
            self.in_use.clear()

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
        self._nodes: List[Node] = []
        self._nodes_lock = asyncio.Lock()
        self._weights_lock = asyncio.Lock()
        self._substrate_lock = asyncio.Lock()
        self.version_key = __spec_version__
        self._initialized = False
        self._weights_set_rate_limit = None
        self._substrate_health_check_task = None
        self._substrate_pool = None  # Will be initialized in setup_neuron
        
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

    @property
    def nodes(self) -> List[Node]:
        """Get the list of Node objects."""
        return self._nodes

    @nodes.setter
    def nodes(self, value: List[Node]):
        """Set the nodes list."""
        self._nodes = value

    def get_node(self, key: Union[int, str]) -> Optional[Node]:
        """Get a node by either UID or hotkey."""
        if isinstance(key, int):
            return next((node for node in self._nodes if node.node_id == key), None)
        elif isinstance(key, str):
            return next((node for node in self._nodes if node.hotkey == key), None)
        return None

    async def _load_nodes_from_database(self) -> List[Node]:
        """Load nodes from database into Node objects."""
        query = """
        SELECT * FROM node_table 
        WHERE hotkey IS NOT NULL 
        ORDER BY uid ASC
        """
        rows = await self.database_manager.fetch_many(query)
        current_time = datetime.now(timezone.utc).timestamp()
        return [
            Node(
                node_id=row["uid"],
                hotkey=row["hotkey"],
                coldkey=row["coldkey"],
                ip=row["ip"],
                ip_type=row["ip_type"],
                port=row["port"],
                stake=float(row["stake"]),
                trust=float(row["trust"]),
                vtrust=float(row["vtrust"]),
                incentive=float(row["incentive"]),
                protocol=str(row["protocol"]),
                netuid=self.netuid,
                last_updated=float(row["last_updated"].timestamp() if row.get("last_updated") else current_time)
            )
            for row in rows
        ]

    async def initialize_state(self):
        """Initialize validator state, handling database and chain state properly."""
        if self._initialized:
            return

        async with self._nodes_lock:
            try:
                # First load state from database
                db_nodes = await self._load_nodes_from_database()
                if db_nodes:
                    logger.debug(f"Loaded {len(db_nodes)} nodes from database")
                    self._nodes = db_nodes
                
                # Then get current chain state
                conn = await self._substrate_pool.get_connection()
                try:
                    chain_nodes = fetch_nodes.get_nodes_for_netuid(conn, self.netuid)
                    logger.debug(f"Fetched {len(chain_nodes)} nodes from chain")

                    # Check for deregistration events between db state and chain state
                    deregistered_miners = []
                    db_nodes_dict = {node.node_id: node.hotkey for node in self._nodes}
                    chain_nodes_dict = {node.node_id: node.hotkey for node in chain_nodes}

                    for uid, hotkey in db_nodes_dict.items():
                        if uid in chain_nodes_dict and chain_nodes_dict[uid] != hotkey:
                            deregistered_miners.append({
                                'uid': uid,
                                'hotkey': hotkey,
                                'new_hotkey': chain_nodes_dict[uid]
                            })

                    # Update to chain state
                    self._nodes = chain_nodes

                    # Handle any deregistration events found
                    if deregistered_miners:
                        logger.info(f"Found {len(deregistered_miners)} deregistered miners during initialization")
                        logger.debug("Deregistered miners details:", extra={
                            'miners': [f"UID {m['uid']}: {m['hotkey']} -> {m['new_hotkey']}" 
                                     for m in deregistered_miners]
                        })

                        uids = [int(miner['uid']) for miner in deregistered_miners]
                        for idx in uids:
                            self.weights[idx] = 0.0

                        await self.soil_task.recalculate_recent_scores(uids)
                        await self.geomagnetic_task.recalculate_recent_scores(uids)

                        logger.debug(f"Processed {len(deregistered_miners)} deregistered miners")

                    # Update database with current chain state
                    for node in self._nodes:
                        await self.database_manager.update_miner_info(
                            index=node.node_id,
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
                finally:
                    self._substrate_pool.release_connection(conn)

                self._initialized = True

            except Exception as e:
                logger.error(f"Error initializing validator state: {str(e)}")
                logger.error(traceback.format_exc())
                raise

    async def _check_substrate_health(self):
        """Periodically check substrate connection health and reconnect if needed."""
        while True:
            try:
                async with self._substrate_lock:
                    conn = await self._substrate_pool.get_connection()
                    try:
                        # Test the connection with a simple query
                        _ = conn.get_block_number(None)
                    finally:
                        self._substrate_pool.release_connection(conn)
            except Exception as e:
                logger.error(f"Substrate health check failed: {e}")
            await asyncio.sleep(30)

    async def _update_weights_set_rate_limit(self):
        """Update the weights set rate limit from chain."""
        try:
            async with self._substrate_lock:
                conn = await self._substrate_pool.get_connection()
                try:
                    rate_limit = _get_hyperparameter(conn, "WeightsSetRateLimit", self.netuid)
                    if rate_limit is not None and isinstance(rate_limit, int):
                        self._weights_set_rate_limit = rate_limit
                        logger.info(f"Updated weights set rate limit to {rate_limit} blocks")
                finally:
                    self._substrate_pool.release_connection(conn)
        except Exception as e:
            logger.error(f"Failed to update weights set rate limit: {e}")

    async def get_current_block_atomic(self) -> int:
        """Get current block number atomically."""
        async with self._substrate_lock:
            conn = await self._substrate_pool.get_connection()
            try:
                block_num = conn.get_block_number(None)
                return block_num
            finally:
                self._substrate_pool.release_connection(conn)

    async def setup_neuron(self) -> bool:
        """Set up the neuron with necessary configurations and connections."""
        try:
            load_dotenv(".env")
            self.netuid = (
                self.args.netuid if self.args.netuid else int(os.getenv("NETUID", 237))
            )
            logger.debug(f"Using netuid: {self.netuid}")

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

            # Initialize the connection pool
            self._substrate_pool = SubstratePool(
                network=self.subtensor_network,
                endpoint=self.subtensor_chain_endpoint,
                max_connections=3
            )
            
            # Get initial connection for setup
            conn = await self._substrate_pool.get_connection()
            try:
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

                self.metagraph = Metagraph(substrate=conn, netuid=self.netuid)
                
                # Get validator UID
                uids_result = conn.query("SubtensorModule", "Uids", [self.netuid, self.keypair.ss58_address])
                if uids_result.value is None:
                    logger.error("Failed to get validator UID - not registered on network")
                    return False
                self.uid = uids_result.value

                # Use atomic block fetching
                self.current_block = await self.get_current_block_atomic()
                self.last_set_weights_block = self.current_block - 300

                # Start substrate health check
                self._substrate_health_check_task = asyncio.create_task(self._check_substrate_health())

                # Get initial weights set rate limit
                await self._update_weights_set_rate_limit()

                return True
            finally:
                self._substrate_pool.release_connection(conn)

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

    async def main_scoring(self, task_wrapper=None):
        """
        Run scoring and weight setting based on the chain's rate limit.
        A block is produced approximately every 12 seconds.
        """
        while True:
            try:
                # Use atomic block fetching and sync with chain state
                async with self._substrate_lock:
                    conn = await self._substrate_pool.get_connection()
                    try:
                        self.current_block = conn.get_block_number(None)
                        # Get current chain state for last weight setting
                        _, blocks_since_last_update = _can_set_weights(conn, self.netuid, self.uid)
                        if blocks_since_last_update is not None:
                            self.last_set_weights_block = self.current_block - blocks_since_last_update
                            if task_wrapper:
                                async with task_wrapper.activity() as task_log:
                                    task_log.debug(f"Synced last_set_weights_block to {self.last_set_weights_block} (current block: {self.current_block}, blocks since last update: {blocks_since_last_update})")
                    finally:
                        self._substrate_pool.release_connection(conn)
                
                # Get current rate limit if not set
                if self._weights_set_rate_limit is None:
                    await self._update_weights_set_rate_limit()
                    if self._weights_set_rate_limit is None:
                        logger.error("Failed to get weights set rate limit, using default of 300")
                        self._weights_set_rate_limit = 300

                # Calculate next weight setting block based on last set block
                rate_limit = self._weights_set_rate_limit
                blocks_since_last = self.current_block - self.last_set_weights_block
                blocks_until_next = rate_limit - (blocks_since_last % rate_limit)
                next_weight_block = self.current_block + blocks_until_next
                
                # Calculate sleep time (12 seconds per block, minus 2 blocks for processing time)
                sleep_time = max(1, (blocks_until_next - 2) * 12)
                
                logger.info(f"Current block: {self.current_block}")
                logger.info(f"Next weight setting at block: {next_weight_block} (in {blocks_until_next} blocks)")
                logger.info(f"Sleeping for {sleep_time} seconds")
                
                # If it's not time yet, sleep until close to next weight setting block
                if blocks_until_next > 2:
                    if task_wrapper:
                        async with task_wrapper.activity(timeout_override=sleep_time + 60):
                            await asyncio.sleep(sleep_time)
                    else:
                        await asyncio.sleep(sleep_time)
                    continue

                # Time to set weights - sync metagraph and get scores
                if task_wrapper:
                    async with task_wrapper.activity() as task_log:
                        task_log.info("Syncing metagraph nodes...")
                        self.metagraph.sync_nodes()
                        task_log.info("Metagraph synced. Fetching recent scores...")

                    three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
                    
                if task_wrapper:
                    async with task_wrapper.activity() as task_log:
                        query = """
                        SELECT score 
                        FROM score_table 
                        WHERE task_name = :task_name
                        AND created_at >= :start_time
                        ORDER BY created_at DESC 
                        LIMIT 1
                        """
                        
                        geomagnetic_result = await self.database_manager.fetch_one(
                            query, 
                            {"task_name": "geomagnetic", "start_time": three_days_ago}
                        )
                        soil_result = await self.database_manager.fetch_one(
                            query,
                            {"task_name": "soil_moisture", "start_time": three_days_ago}
                        )

                        geomagnetic_scores = geomagnetic_result["score"] if geomagnetic_result else [float("nan")] * 256
                        soil_scores = soil_result["score"] if soil_result else [float("nan")] * 256

                if task_wrapper:
                    async with task_wrapper.activity() as task_log:
                        task_log.info("Recent scores fetched. Calculating aggregate scores...")

                        weights = [0.0] * 256
                        for idx in range(256):
                            geomagnetic_score = geomagnetic_scores[idx]
                            soil_score = soil_scores[idx]

                            if math.isnan(geomagnetic_score) and math.isnan(soil_score):
                                weights[idx] = 0.0
                                task_log.debug(f"Both scores nan - setting weight to 0")
                            elif math.isnan(geomagnetic_score):
                                weights[idx] = 0.5 * soil_score
                                task_log.debug(
                                    f"Geo score nan - using soil score: {weights[idx]}"
                                )
                            elif math.isnan(soil_score):
                                geo_normalized = math.exp(-abs(geomagnetic_score) / 10)
                                weights[idx] = 0.5 * geo_normalized
                                task_log.debug(
                                    f"UID {idx}: Soil score nan - normalized geo score: {geo_normalized} -> weight: {weights[idx]}"
                                )
                            else:
                                geo_normalized = math.exp(-abs(geomagnetic_score) / 10)
                                weights[idx] = (0.5 * geo_normalized) + (0.5 * soil_score)
                                task_log.debug(
                                    f"UID {idx}: Both scores valid - geo_norm: {geo_normalized}, soil: {soil_score} -> weight: {weights[idx]}"
                                )

                            task_log.info(
                                f"UID {idx}: geo={geomagnetic_score} (norm={geo_normalized if 'geo_normalized' in locals() else 'nan'}), soil={soil_score}, weight={weights[idx]}"
                            )

                        task_log.info(f"Weights before normalization: {weights}")

                        non_zero_weights = [w for w in weights if w != 0.0]
                        if non_zero_weights:
                            # Sort indices by weight for ranking (negative scores = higher error = lower rank)
                            sorted_indices = sorted(
                                range(len(weights)),
                                key=lambda k: (
                                    weights[k] if weights[k] != 0.0 else float("-inf")
                                ),
                            )

                            new_weights = [0.0] * len(weights)
                            for rank, idx in enumerate(sorted_indices):
                                if weights[idx] != 0.0:
                                    try:
                                        normalized_rank = 1.0 - (rank / len(non_zero_weights))
                                        exponent = max(
                                            min(-20 * (normalized_rank - 0.5), 709), -709
                                        )
                                        new_weights[idx] = 1 / (1 + math.exp(exponent))
                                    except OverflowError:
                                        task_log.warning(
                                            f"Overflow prevented for rank {rank}, idx {idx}"
                                        )

                                        if normalized_rank > 0.5:
                                            new_weights[idx] = 1.0
                                        else:
                                            new_weights[idx] = 0.0

                            total = sum(new_weights)
                            if total > 0:
                                self.weights = [w / total for w in new_weights]

                                top_20_weight = sum(
                                    sorted(self.weights, reverse=True)[
                                        : int(len(weights) * 0.2)
                                    ]
                                )
                                task_log.info(
                                    f"Weight distribution: top 20% of nodes hold {top_20_weight*100:.1f}% of total weight"
                                )

                                async with task_wrapper.activity() as task_log:
                                    task_log.info("Attempting to set weights...")
                                    success = await self.set_weights()
                                    if success:
                                        task_log.info(f"Successfully set weights at block {self.current_block}")
                                        # Sleep for most of the rate limit period
                                        sleep_time = (rate_limit - 2) * 12
                                        task_log.info(f"Sleeping for {sleep_time} seconds until next weight setting interval")
                                        await asyncio.sleep(sleep_time)
                                    else:
                                        task_log.error(f"Error setting weights at block {self.current_block}")
                                        # Sleep for a shorter time before retrying
                                        await asyncio.sleep(60)
                            else:
                                task_log.warning("No positive weights after normalization")
                                await asyncio.sleep(60)
                        else:
                            task_log.warning("All weights are zero or nan, skipping weight setting")
                            await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"Error in main_scoring: {e}")
                logger.error(traceback.format_exc())
                if task_wrapper:
                    async with task_wrapper.activity(timeout_override=120):
                        await asyncio.sleep(60)
                else:
                    await asyncio.sleep(60)

    async def set_weights(self) -> bool:
        """Set weights on the chain with timeout and retry logic."""
        try:
            async with self._weights_lock:  # Ensure only one weight setting operation at a time
                async with self._substrate_lock:  # Ensure substrate operations are atomic
                    conn = await self._substrate_pool.get_connection()
                    try:
                        # Use consistent block number fetching
                        self.current_block = conn.get_block_number(None)

                        # Update rate limit if not set
                        if self._weights_set_rate_limit is None:
                            rate_limit = _get_hyperparameter(conn, "WeightsSetRateLimit", self.netuid)
                            if rate_limit is not None and isinstance(rate_limit, int):
                                self._weights_set_rate_limit = rate_limit

                        # First check chain-level constraints
                        can_set, blocks_since_last_update = _can_set_weights(conn, self.netuid, self.uid)
                        if not can_set:
                            logger.info("Cannot set weights yet due to chain constraints")
                            # Calculate the actual last set weights block from blocks_since_last_update
                            if blocks_since_last_update is not None:
                                self.last_set_weights_block = self.current_block - blocks_since_last_update
                                logger.info(f"Updated last_set_weights_block to {self.last_set_weights_block} (current block: {self.current_block}, blocks since last update: {blocks_since_last_update})")
                            return False

                        # Then check our own timing constraints
                        if self.last_set_weights_block:
                            blocks_since_last = self.current_block - self.last_set_weights_block
                            if blocks_since_last < self._weights_set_rate_limit:
                                blocks_remaining = self._weights_set_rate_limit - blocks_since_last
                                next_block = self.last_set_weights_block + self._weights_set_rate_limit
                                logger.info(f"Waiting for block interval - {blocks_remaining} blocks remaining")
                                logger.info(f"Last set: block {self.last_set_weights_block}")
                                logger.info(f"Next possible: block {next_block} (current: {self.current_block})")
                                return True

                        logger.info(f"Attempting to set weights at block {self.current_block}...")
                        success = set_node_weights(
                            substrate=conn,
                            keypair=self.keypair,
                            node_ids=list(range(256)),
                            node_weights=self.weights,
                            netuid=self.netuid,
                            validator_node_id=self.uid,
                            version_key=self.version_key,
                            wait_for_inclusion=True
                        )

                        if success:
                            logger.info(f"Successfully set weights at block {self.current_block}")
                            self.last_set_weights_block = self.current_block
                            return True
                        else:
                            logger.error(f"Error setting weights at block {self.current_block}, trying again in 50 blocks")
                            return False
                    finally:
                        self._substrate_pool.release_connection(conn)

        except Exception as e:
            logger.error(f"Unexpected error in set_weights: {e}")
            logger.error(traceback.format_exc())
            return False

    async def status_logger(self, task_wrapper=None):
        """Log the status of the validator periodically."""
        task_log = logger
        while True:
            try:
                if task_wrapper:
                    async with task_wrapper.activity() as task_log:
                        current_time_utc = datetime.now(timezone.utc)
                        formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S")

                        try:
                            # Use atomic block fetching
                            self.current_block = await self.get_current_block_atomic()
                            blocks_since_weights = (
                                self.current_block - self.last_set_weights_block
                            )
                        except Exception as e:
                            task_log.error(f"Failed to get current block: {e}")

                        active_nodes = len(self._nodes)

                        task_log.info(
                            f"\n"
                            f"---Status Update ---\n"
                            f"Time (UTC): {formatted_time} | \n"
                            f"Block: {self.current_block} | \n"
                            f"Nodes: {active_nodes}/256 | \n"
                            f"Weights Set: {blocks_since_weights} blocks ago"
                        )

                    # Use explicit timeout for sleep period
                    async with task_wrapper.activity(timeout_override=120):
                        await asyncio.sleep(60)
                else:
                    current_time_utc = datetime.now(timezone.utc)
                    formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S")

                    try:
                        self.current_block = await self.get_current_block_atomic()
                        blocks_since_weights = (
                            self.current_block - self.last_set_weights_block
                        )
                    except Exception as e:
                        logger.error(f"Failed to get current block: {e}")

                    active_nodes = len(self._nodes)

                    logger.info(
                        f"\n"
                        f"---Status Update ---\n"
                        f"Time (UTC): {formatted_time} | \n"
                        f"Block: {self.current_block} | \n"
                        f"Nodes: {active_nodes}/256 | \n"
                        f"Weights Set: {blocks_since_weights} blocks ago"
                    )
                    await asyncio.sleep(60)

            except Exception as e:
                if task_wrapper:
                    async with task_wrapper.activity() as task_log:
                        task_log.error(f"Error in status logger: {e}")
                        task_log.error(traceback.format_exc())
                    # Use explicit timeout for error recovery sleep
                    async with task_wrapper.activity(timeout_override=120):
                        await asyncio.sleep(60)
                else:
                    logger.error(f"Error in status logger: {e}")
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(60)

    async def handle_miner_deregistration_loop(self, task_wrapper=None):
        """Run miner deregistration checks every 60 seconds."""
        task_log = logger
        while True:
            try:
                if task_wrapper:
                    async with task_wrapper.activity() as task_log:
                        task_log.info("Checking for deregistered miners...")
                        conn = await self._substrate_pool.get_connection()
                        try:
                            new_nodes = fetch_nodes.get_nodes_for_netuid(conn, self.netuid)
                            
                            # Check for deregistration events while updating nodes
                            deregistered_miners = await self._update_nodes_and_check_deregistration(new_nodes)

                            if deregistered_miners:
                                task_log.info(f"Found {len(deregistered_miners)} deregistered miners in monitoring loop:")
                                for miner in deregistered_miners:
                                    task_log.info(
                                        f"UID {miner['uid']}: {miner['hotkey']} -> {miner['new_hotkey']}"
                                    )

                                uids = [int(miner['uid']) for miner in deregistered_miners]
                                for idx in uids:
                                    self.weights[idx] = 0.0

                                await self.soil_task.recalculate_recent_scores(uids)
                                await self.geomagnetic_task.recalculate_recent_scores(uids)

                                task_log.info(f"Processed {len(deregistered_miners)} deregistered miners")
                            else:
                                task_log.debug("No deregistered miners found")
                        finally:
                            self._substrate_pool.release_connection(conn)

                    # Use explicit timeout for sleep period
                    async with task_wrapper.activity(timeout_override=120):
                        await asyncio.sleep(60)
                else:
                    logger.info("Checking for deregistered miners...")
                    conn = await self._substrate_pool.get_connection()
                    try:
                        new_nodes = fetch_nodes.get_nodes_for_netuid(conn, self.netuid)
                        deregistered_miners = await self._update_nodes_and_check_deregistration(new_nodes)
                        if deregistered_miners:
                            logger.info(f"Found {len(deregistered_miners)} deregistered miners")
                            for miner in deregistered_miners:
                                logger.info(f"UID {miner['uid']}: {miner['hotkey']} -> {miner['new_hotkey']}")
                            uids = [int(miner['uid']) for miner in deregistered_miners]
                            for idx in uids:
                                self.weights[idx] = 0.0
                            await self.soil_task.recalculate_recent_scores(uids)
                            await self.geomagnetic_task.recalculate_recent_scores(uids)
                            logger.info(f"Processed {len(deregistered_miners)} deregistered miners")
                        else:
                            logger.debug("No deregistered miners found")
                    finally:
                        self._substrate_pool.release_connection(conn)
                    await asyncio.sleep(60)

            except Exception as e:
                if task_wrapper:
                    async with task_wrapper.activity() as task_log:
                        task_log.error(f"Error in deregistration loop: {e}")
                        task_log.error(traceback.format_exc())
                    # Use explicit timeout for error recovery sleep
                    async with task_wrapper.activity(timeout_override=120):
                        await asyncio.sleep(60)
                else:
                    logger.error(f"Error in deregistration loop: {e}")
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(60)

    async def check_for_updates(self, task_wrapper=None):
        """Check for and apply updates every 2 minutes."""
        task_log = logger
        while True:
            try:
                if task_wrapper:
                    async with task_wrapper.activity() as task_log:
                        task_log.info("Checking for updates...")
                        update_successful = await perform_update(self)

                        if update_successful:
                            task_log.info("Update completed successfully")
                        else:
                            task_log.debug("No updates available or update failed")

                    # Use explicit timeout for sleep period
                    async with task_wrapper.activity(timeout_override=180):
                        await asyncio.sleep(120)  # Wait 2 minutes before next check
                else:
                    logger.info("Checking for updates...")
                    update_successful = await perform_update(self)
                    if update_successful:
                        logger.info("Update completed successfully")
                    else:
                        logger.debug("No updates available or update failed")
                    await asyncio.sleep(120)  # Wait 2 minutes before next check

            except Exception as e:
                if task_wrapper:
                    async with task_wrapper.activity() as task_log:
                        task_log.error(f"Error in update checker: {e}")
                        task_log.error(traceback.format_exc())
                    # Use explicit timeout for error recovery sleep
                    async with task_wrapper.activity(timeout_override=180):
                        await asyncio.sleep(120)
                else:
                    logger.error(f"Error in update checker: {e}")
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(120)

    async def main(self):
        """
        Main execution loop for the validator.
        Sets up the validator and runs all tasks concurrently with failure handling and task recreation.
        """
        logger.info("Setting up neuron...")
        if not await self.setup_neuron():
            logger.error("Failed to setup neuron, exiting...")
            return

        logger.info("Neuron setup complete.")

        logger.info("Initializing validator state...")
        await self.initialize_state()
        logger.info("Validator state initialized.")

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

        # Task configuration with timeouts and retry limits
        task_configs = {
            "geomagnetic": {
                "name": "Geomagnetic Task",
                "heartbeat_timeout": 300,  
                "max_retries": 3,
                "retry_delay": 60,
                "creator": lambda validator, task_wrapper: validator.geomagnetic_task.validator_execute(validator=validator, task_wrapper=task_wrapper)
            },
            "soil": {
                "name": "Soil Moisture Task",
                "heartbeat_timeout": 300,
                "max_retries": 3,
                "retry_delay": 60,
                "creator": lambda validator, task_wrapper: validator.soil_task.validator_execute(validator=validator, task_wrapper=task_wrapper)
            },
            "status": {
                "name": "Status Logger",
                "heartbeat_timeout": 60,
                "max_retries": 5,
                "retry_delay": 30,
                "creator": lambda validator, task_wrapper: validator.status_logger(task_wrapper=task_wrapper)
            },
            "scoring": {
                "name": "Main Scoring",
                "heartbeat_timeout": 300,
                "max_retries": 3,
                "retry_delay": 60,
                "creator": lambda validator, task_wrapper: validator.main_scoring(task_wrapper=task_wrapper)
            },
            "deregistration": {
                "name": "Deregistration Monitor",
                "heartbeat_timeout": 60,
                "max_retries": 3,
                "retry_delay": 30,
                "creator": lambda validator, task_wrapper: validator.handle_miner_deregistration_loop(task_wrapper=task_wrapper)
            },
            "updates": {
                "name": "Update Checker",
                "heartbeat_timeout": 60,
                "max_retries": 3,
                "retry_delay": 60,
                "creator": lambda validator, task_wrapper: validator.check_for_updates(task_wrapper=task_wrapper)
            }
        }

        # Task state tracking
        task_states = {
            name: {
                "retries": 0,
                "last_error": None,
                "last_heartbeat": None,
                "task_future": None
            } 
            for name in task_configs
        }
        active_tasks = {}

        async def handle_task_failure(task_name, error):
            """Handle task failure with retries and logging."""
            state = task_states[task_name]
            config = task_configs[task_name]
            
            state["last_error"] = error
            state["retries"] += 1
            
            if state["retries"] <= config["max_retries"]:
                logger.warning(
                    f"{config['name']} failed ({state['retries']}/{config['max_retries']} retries). "
                    f"Error: {error}. Retrying in {config['retry_delay']} seconds..."
                )
                await asyncio.sleep(config["retry_delay"])
                return True  # Should retry
            else:
                logger.error(
                    f"{config['name']} failed after {config['max_retries']} retries. "
                    f"Last error: {error}"
                )
                return False  # Should not retry

        class TaskLogger:
            """Custom logger that updates task heartbeat on any logging activity."""
            def __init__(self, task_wrapper, original_logger):
                self.task_wrapper = task_wrapper
                self.original_logger = original_logger
                self._last_update = None
                self._min_update_interval = 1.0  # Minimum seconds between heartbeat updates

            def _should_update(self):
                """Check if enough time has passed to update heartbeat."""
                now = datetime.now(timezone.utc)
                if self._last_update is None:
                    return True
                return (now - self._last_update).total_seconds() >= self._min_update_interval

            async def _update_if_needed(self):
                """Update heartbeat if enough time has passed."""
                if self._should_update():
                    self._last_update = datetime.now(timezone.utc)
                    await self.task_wrapper.update_heartbeat()

            def debug(self, msg, *args, **kwargs):
                asyncio.create_task(self._update_if_needed())
                self.original_logger.debug(msg, *args, **kwargs)

            def info(self, msg, *args, **kwargs):
                asyncio.create_task(self._update_if_needed())
                self.original_logger.info(msg, *args, **kwargs)

            def warning(self, msg, *args, **kwargs):
                asyncio.create_task(self._update_if_needed())
                self.original_logger.warning(msg, *args, **kwargs)

            def error(self, msg, *args, **kwargs):
                asyncio.create_task(self._update_if_needed())
                self.original_logger.error(msg, *args, **kwargs)

            def critical(self, msg, *args, **kwargs):
                asyncio.create_task(self._update_if_needed())
                self.original_logger.critical(msg, *args, **kwargs)

        class TaskWrapper:
            """Wrapper to handle task execution with heartbeat monitoring."""
            def __init__(self, name, config, state, task_creator, validator):
                self.name = name
                self.config = config
                self.state = state
                self.task_creator = task_creator
                self.validator = validator
                self.heartbeat_event = asyncio.Event()
                self.should_stop = False
                self._current_timeout = config["heartbeat_timeout"]
                self._last_activity = None
                # Create task-specific logger that updates heartbeat
                self.logger = TaskLogger(self, logger.getChild(name))

            @contextlib.asynccontextmanager
            async def activity(self, timeout_override: Optional[int] = None):
                """Context manager to track any task activity and update heartbeat."""
                try:
                    if timeout_override is not None:
                        old_timeout = self._current_timeout
                        self._current_timeout = timeout_override
                    await self.update_heartbeat()
                    yield self.logger  # Yield the task logger
                finally:
                    if timeout_override is not None:
                        self._current_timeout = old_timeout
                    await self.update_heartbeat()

            async def heartbeat_monitor(self):
                """Monitor task heartbeat and cancel if no updates received."""
                while not self.should_stop:
                    try:
                        try:
                            await asyncio.wait_for(
                                self.heartbeat_event.wait(),
                                timeout=self._current_timeout
                            )
                            self.heartbeat_event.clear()
                        except asyncio.TimeoutError:
                            if self.state["task_future"] and not self.state["task_future"].done():
                                time_since_last = datetime.now(timezone.utc) - (self._last_activity or datetime.now(timezone.utc))
                                self.logger.error(
                                    f"Heartbeat timeout after {self._current_timeout} seconds "
                                    f"(last activity: {time_since_last.total_seconds():.1f}s ago)"
                                )
                                self.state["task_future"].cancel()
                            break
                    except Exception as e:
                        self.logger.error(f"Error in heartbeat monitor: {e}")
                        break

            async def update_heartbeat(self):
                """Update the task's heartbeat."""
                self._last_activity = datetime.now(timezone.utc)
                self.state["last_heartbeat"] = self._last_activity
                self.heartbeat_event.set()

            async def wrapped_coro(self):
                """Wrap the original coroutine with heartbeat updates."""
                monitor_task = asyncio.create_task(self.heartbeat_monitor())
                try:
                    # Create a new task with self as task_wrapper
                    self.state["task_future"] = asyncio.create_task(
                        self.task_creator(self.validator, self)
                    )
                    
                    # Wait for the task to complete
                    await self.state["task_future"]
                finally:
                    self.should_stop = True
                    self.heartbeat_event.set()  # Ensure monitor task exits
                    await monitor_task

        async def create_task_with_timeout(task_name):
            """Create a task with heartbeat monitoring."""
            config = task_configs[task_name]
            state = task_states[task_name]

            while True:
                try:
                    # Create wrapped task with heartbeat monitoring
                    wrapper = TaskWrapper(
                        task_name,
                        config,
                        state,
                        config["creator"],
                        self  # Pass validator instance
                    )
                    
                    # Run the wrapped task
                    await wrapper.wrapped_coro()

                except asyncio.CancelledError:
                    logger.warning(f"{config['name']} was cancelled due to heartbeat timeout")
                    raise
                
                except Exception as e:
                    logger.error(f"Error in {config['name']}: {str(e)}")
                    logger.error(traceback.format_exc())
                    
                    should_retry = await handle_task_failure(task_name, e)
                    if not should_retry:
                        raise  # Re-raise the error to trigger task recreation
                    continue  # Retry the task

        try:
            # Initial task creation
            for task_name in task_configs:
                active_tasks[task_name] = asyncio.create_task(
                    create_task_with_timeout(task_name)
                )

            while True:
                # Wait for any task to complete or fail
                done, pending = await asyncio.wait(
                    active_tasks.values(),
                    return_when=asyncio.FIRST_COMPLETED
                )

                # Handle completed/failed tasks
                for task in done:
                    # Find which task completed
                    task_name = next(
                        name for name, t in active_tasks.items() if t == task
                    )
                    
                    try:
                        await task  # This will raise any exception that occurred
                    except Exception as e:
                        logger.error(
                            f"{task_configs[task_name]['name']} failed with error: {e}"
                        )
                    finally:
                        # Reset retry count on successful completion
                        task_states[task_name]["retries"] = 0
                        # Recreate the task
                        active_tasks[task_name] = asyncio.create_task(
                            create_task_with_timeout(task_name)
                        )

        except Exception as e:
            logger.error(f"Critical error in main loop: {e}")
            logger.error(traceback.format_exc())
        
        finally:
            # Cleanup
            await self.cleanup()
            
            # Cancel all tasks
            for task in active_tasks.values():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

    async def _update_nodes_and_check_deregistration(self, new_nodes: List[Node]) -> List[Dict[str, Any]]:
        """
        Update nodes and check for deregistration events atomically.
        Returns list of deregistered miners if any are found.
        """
        async with self._nodes_lock:
            # Create lookup dictionaries for efficient comparison
            current_nodes = {node.node_id: node.hotkey for node in self._nodes}
            new_nodes_dict = {node.node_id: node.hotkey for node in new_nodes}

            deregistered_miners = []
            for uid, hotkey in current_nodes.items():
                if uid in new_nodes_dict and new_nodes_dict[uid] != hotkey:
                    deregistered_miners.append({
                        'uid': uid, 
                        'hotkey': hotkey,
                        'new_hotkey': new_nodes_dict[uid]
                    })

            # Update nodes list with new nodes
            self._nodes = new_nodes
            return deregistered_miners

    async def update_miner_table(self):
        """Update the miner table with the latest miner information."""
        try:
            conn = await self._substrate_pool.get_connection()
            try:
                new_nodes = fetch_nodes.get_nodes_for_netuid(conn, self.netuid)
                logger.debug(f"Fetched {len(new_nodes)} nodes from the network")

                # Check for deregistration events while updating nodes
                deregistered_miners = await self._update_nodes_and_check_deregistration(new_nodes)

                # Update database for all nodes
                for node in self._nodes:
                    await self.database_manager.update_miner_info(
                        index=node.node_id,
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

                # Handle any deregistration events
                if deregistered_miners:
                    logger.info(f"Found {len(deregistered_miners)} deregistered miners during table update")
                    logger.debug("Deregistered miners details:", extra={
                        'miners': [f"UID {m['uid']}: {m['hotkey']} -> {m['new_hotkey']}" 
                                 for m in deregistered_miners]
                    })

                    uids = [int(miner['uid']) for miner in deregistered_miners]
                    for idx in uids:
                        self.weights[idx] = 0.0

                    await self.soil_task.recalculate_recent_scores(uids)
                    await self.geomagnetic_task.recalculate_recent_scores(uids)

                    logger.debug(f"Processed {len(deregistered_miners)} deregistered miners")

                logger.debug("Successfully updated miner table and in-memory state")
            finally:
                self._substrate_pool.release_connection(conn)

        except Exception as e:
            logger.error(f"Error updating miner table: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    async def cleanup(self):
        """Cleanup resources before shutdown."""
        if self._substrate_health_check_task:
            self._substrate_health_check_task.cancel()
            try:
                await self._substrate_health_check_task
            except asyncio.CancelledError:
                pass

        if self._substrate_pool:
            await self._substrate_pool.close_all()

        if self.httpx_client:
            await self.httpx_client.aclose()


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
