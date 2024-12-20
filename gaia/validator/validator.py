from datetime import datetime, timezone, timedelta
import os

os.environ["NODE_TYPE"] = "validator"
import asyncio
import ssl
import traceback
from typing import Any, Optional, List, Dict
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import httpx
from fiber.chain import chain_utils
from fiber.logging_utils import get_logger
from fiber.validator import client as vali_client
from fiber.chain.metagraph import Metagraph
from substrateinterface import SubstrateInterface
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask
from gaia.tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from argparse import ArgumentParser
import pandas as pd
import json
from gaia.validator.weights.set_weights import FiberWeightSetter
import base64
import math
from gaia.validator.utils.auto_updater import perform_update

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
        self.nodes = {}  # Initialize the in-memory node table state

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

            # print the entire set of environment variables
            # logger.info(f"{os.environ}")

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
                    async with vali_client.create_client(
                        keypair=self.keypair,
                        server_url=base_url,
                        miner_hotkey=miner_hotkey
                    ) as client:
                        logger.info(f"Established connection with miner {miner_hotkey}")
                        
                        resp = await client.post(
                            endpoint=endpoint,
                            payload=payload
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

            await asyncio.sleep(120)  # Wait 2 minutes before next check

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

        while True:
            try:
                workers = [
                    asyncio.create_task(self.geomagnetic_task.validator_execute(self)),
                    asyncio.create_task(self.soil_task.validator_execute(self)),
                    asyncio.create_task(self.status_logger()),
                    asyncio.create_task(self.main_scoring()),
                    asyncio.create_task(self.handle_miner_deregistration_loop()),
                   # asyncio.create_task(self.check_for_updates()),
                ]

                await asyncio.gather(*workers, return_exceptions=True)
            except Exception as e:
                logger.error(f"Main loop error: {e}")
            await asyncio.sleep(300)

    async def main_scoring(self):
        """
        Run scoring every 300 blocks, with weight setting 50 blocks after scoring.
        """
        while True:
            try:
                self.metagraph.sync_nodes()
                block = self.substrate.get_block()
                self.current_block = block["header"]["number"]
                
                next_weight_block = ((self.current_block // 300) * 300) + 50
                blocks_until_weights = next_weight_block - self.current_block
                
                logger.info(f"Fetched current block: {self.current_block}")
                logger.info(f"Next weight setting at block: {next_weight_block}")
                
                await asyncio.sleep(30)

                if self.current_block >= next_weight_block:
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
                        query, 
                        {"task_name": "geomagnetic", "start_time": three_days_ago}
                    )
                    soil_result = await self.database_manager.fetch_one(
                        query,
                        {"task_name": "soil_moisture", "start_time": three_days_ago}
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
                            logger.debug(
                                f"Geo score nan - using soil score: {weights[idx]}"
                            )
                        elif math.isnan(soil_score):
                            geo_normalized = math.exp(-abs(geomagnetic_score) / 10)
                            weights[idx] = 0.5 * geo_normalized
                            logger.debug(
                                f"UID {idx}: Soil score nan - normalized geo score: {geo_normalized} -> weight: {weights[idx]}"
                            )
                        else:
                            geo_normalized = math.exp(-abs(geomagnetic_score) / 10)
                            weights[idx] = (0.5 * geo_normalized) + (0.5 * soil_score)
                            logger.debug(
                                f"UID {idx}: Both scores valid - geo_norm: {geo_normalized}, soil: {soil_score} -> weight: {weights[idx]}"
                            )

                        logger.info(
                            f"UID {idx}: geo={geomagnetic_score} (norm={geo_normalized if 'geo_normalized' in locals() else 'nan'}), soil={soil_score}, weight={weights[idx]}"
                        )

                    logger.info(f"Weights before normalization: {weights}")

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
                                    logger.warning(
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
                            logger.info(
                                f"Weight distribution: top 20% of nodes hold {top_20_weight*100:.1f}% of total weight"
                            )

                            logger.info("Attempting to set weights...")
                            success = await self.set_weights(self.weights)
                            if success:
                                if self.current_block == self.last_set_weights_block:
                                    logger.info(f"Successfully set weights at block {self.current_block}")
                                else:
                                    logger.info("Waiting for next weight setting interval")
                            else:
                                logger.error(f"Error setting weights at block {self.current_block}")
                        else:
                            logger.warning("No positive weights after normalization")
                    else:
                        logger.warning(
                            "All weights are zero or nan, skipping weight setting"
                        )

            except Exception as e:
                logger.error(f"Error in main_scoring: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(60)

    async def set_weights(self, weights: List[float], timeout: int = 30, max_retries: int = 3) -> bool:
        """
        Set weights on the chain with timeout and retry logic.

        Args:
            weights (List[float]): List of weights aligned with UIDs.
            timeout (int): Maximum time to wait for the operation (in seconds).
            max_retries (int): Maximum number of retries if the operation fails.

        Returns:
            bool: True if weights were set successfully, False otherwise.
        """
        try:
            block = self.substrate.get_block()
            self.current_block = block["header"]["number"]

            if self.last_set_weights_block:
                blocks_since_last = self.current_block - self.last_set_weights_block
                if blocks_since_last < 300:
                    blocks_remaining = 300 - blocks_since_last
                    next_block = self.last_set_weights_block + 300
                    logger.info(f"Waiting for block interval - {blocks_remaining} blocks remaining")
                    logger.info(f"Last set: block {self.last_set_weights_block}")
                    logger.info(f"Next possible: block {next_block} (current: {self.current_block})")
                    return True

            weight_setter = FiberWeightSetter(
                netuid=self.netuid,
                wallet_name=self.wallet_name,
                hotkey_name=self.hotkey_name,
                network=self.subtensor_network,
                last_set_block=self.last_set_weights_block,
                current_block=self.current_block
            )

            attempt = 0
            delay = 1
            while attempt < max_retries:
                try:
                    success = await asyncio.wait_for(weight_setter.set_weights(weights), timeout=timeout)
                    if success:
                        self.last_set_weights_block = self.current_block
                        logger.info(f"✅ Successfully set weights at block {self.current_block}")
                        logger.info(f"Next weight set possible at block {self.current_block + 300}")
                        return True
                    else:
                        logger.warning("❌ Failed to set weights, retrying...")
                except asyncio.TimeoutError:
                    logger.debug(f"⏳ Timeout occurred while setting weights (attempt {attempt + 1}/{max_retries})")
                except Exception as e:
                    logger.error(f"❌ Error during weight setting (attempt {attempt + 1}/{max_retries}): {e}")
                    logger.error(traceback.format_exc())

                attempt += 1
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10)

            return False

        except Exception as e:
            logger.error(f"Unexpected error in set_weights: {e}")
            logger.error(traceback.format_exc())
            return False

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
                self.metagraph.sync_nodes()
                active_miners = {
                    idx: {"hotkey": hotkey, "uid": idx}
                    for idx, (hotkey, _) in enumerate(self.metagraph.nodes.items())
                }

                if not self.nodes:
                    query = (
                        "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL"
                    )
                    rows = await self.database_manager.fetch_many(query)
                    self.nodes = {
                        row["uid"]: {"hotkey": row["hotkey"], "uid": row["uid"]}
                        for row in rows
                    }

                deregistered_miners = []
                for uid, registered in self.nodes.items():
                    if active_miners[uid]["hotkey"] != registered["hotkey"]:
                        deregistered_miners.append(registered)

                if deregistered_miners:
                    logger.info(
                        f"Found {len(deregistered_miners)} deregistered miners:"
                    )
                    for miner in deregistered_miners:
                        logger.info(
                            f"UID {miner['uid']}: {miner['hotkey']} -> {active_miners[miner['uid']]['hotkey']}"
                        )

                    for miner in deregistered_miners:
                        self.nodes[miner["uid"]]["hotkey"] = active_miners[
                            miner["uid"]
                        ]["hotkey"]

                    uids = [int(miner["uid"]) for miner in deregistered_miners]

                    for idx in uids:
                        self.weights[idx] = 0.0

                    await self.soil_task.recalculate_recent_scores(uids)
                    await self.geomagnetic_task.recalculate_recent_scores(uids)

                    logger.info(
                        f"Processed {len(deregistered_miners)} deregistered miners"
                    )
                else:
                    logger.debug("No deregistered miners found")

            except Exception as e:
                logger.error(f"Error in deregistration loop: {e}")
                logger.error(traceback.format_exc())
            finally:
                await asyncio.sleep(60)


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
