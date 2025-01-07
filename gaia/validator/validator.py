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

        # Initialize MinerScoreSender
        self.miner_score_sender = MinerScoreSender(database_manager=self.database_manager,
                                                   loop=asyncio.get_event_loop())

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

        self.last_successful_weight_set = time.time()
        self.watchdog_timeout = 1800  # 30 minutes

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

    async def watchdog(self):
        """Monitor validator health and force restart if frozen for any reason"""
        while True:
            try:
                current_time = time.time()
                
                # Check weight setting health
                if current_time - self.last_successful_weight_set > self.watchdog_timeout:
                    logger.warning("Validator appears frozen - forcing restart of scoring cycle")
                    self.substrate = interface.get_substrate(subtensor_network=self.subtensor_network)
                    await self.database_manager.reset_pool()
                    self.metagraph.sync_nodes()
                    self.weight_setter = FiberWeightSetter(
                        netuid=self.netuid,
                        wallet_name=self.wallet_name,
                        hotkey_name=self.hotkey_name,
                        network=self.subtensor_network
                    )
                
                # Check deregistration loop health
                if hasattr(self, 'last_dereg_check_start'):
                    dereg_duration = current_time - self.last_dereg_check_start
                    if dereg_duration > 600:  # 10 minutes
                        logger.warning("Deregistration loop appears stuck - forcing reset")
                        await self.database_manager.reset_pool()
                        self.metagraph.sync_nodes()
                
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Error in watchdog: {e}")
                await asyncio.sleep(60)

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
        asyncio.create_task(self.watchdog())

        while True:
            try:
                workers = [
                    asyncio.create_task(self.geomagnetic_task.validator_execute(self)),
                    asyncio.create_task(self.soil_task.validator_execute(self)),
                    asyncio.create_task(self.status_logger()),
                    asyncio.create_task(self.main_scoring()),
                    asyncio.create_task(self.handle_miner_deregistration_loop()),
                   # asyncio.create_task(self.check_for_updates()),
                    #asyncio.create_task(self.miner_score_sender.run_async()),
                ]

                await asyncio.gather(*workers, return_exceptions=True)
            except Exception as e:
                logger.error(f"Main loop error: {e}")
            await asyncio.sleep(300)

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
                async def scoring_cycle():
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

                    # Add check for recent successful weight set
                    current_block = self.substrate.get_block()["header"]["number"]
                    if current_block - self.last_set_weights_block < min_interval:
                        logger.info(f"Recently set weights {current_block - self.last_set_weights_block} blocks ago")
                        await asyncio.sleep(12)
                        return True

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
                                    await asyncio.sleep(30)
                    else:
                        logger.info(
                            f"Waiting for weight setting: {blocks_since_update}/{min_interval} blocks"
                        )

                    await asyncio.sleep(12)
                    return True

                await asyncio.wait_for(scoring_cycle(), timeout=600)

            except asyncio.TimeoutError:
                logger.error("Weight setting operation timed out - restarting cycle")
                try:
                    self.substrate = interface.get_substrate(subtensor_network=self.subtensor_network)
                except Exception as e:
                    logger.error(f"Failed to reconnect to substrate: {e}")
                await asyncio.sleep(12)
                continue
            except Exception as e:
                logger.error(f"Error in main_scoring: {e}")
                logger.error(traceback.format_exc())
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
                async with asyncio.timeout(300):
                    self.last_dereg_check_start = time.time()
                    
                    self.metagraph.sync_nodes()
                    active_miners = {
                        idx: {"hotkey": hotkey, "uid": idx}
                        for idx, (hotkey, _) in enumerate(self.metagraph.nodes.items())
                    }

                    if not self.nodes:
                        try:
                            async with asyncio.timeout(60):
                                query = "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL"
                                rows = await self.database_manager.fetch_many(query)
                                self.nodes = {
                                    row["uid"]: {"hotkey": row["hotkey"], "uid": row["uid"]}
                                    for row in rows
                                }
                        except asyncio.TimeoutError:
                            logger.error("Database query timed out")
                            continue

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
                        try:
                            async with asyncio.timeout(300):
                                await self.soil_task.recalculate_recent_scores(uids)
                                await self.geomagnetic_task.recalculate_recent_scores(uids)
                                logger.info(f"Successfully recalculated scores for {len(uids)} miners")
                                self.last_successful_recalc = time.time()
                        except asyncio.TimeoutError:
                            logger.error("Score recalculation timed out")
                            continue
                    
                    self.last_successful_dereg_check = time.time()
                    
            except asyncio.TimeoutError:
                logger.error("Deregistration loop timed out - restarting loop")
                continue
            except Exception as e:
                logger.error(f"Error in deregistration loop: {e}")
                logger.error(traceback.format_exc())
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
