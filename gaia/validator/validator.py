import datetime
import os
import asyncio
import ssl
import traceback
from typing import Any, Optional, List
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import httpx
from fiber.chain import chain_utils
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
from gaia.validator.weights.set_weights import FiberWeightSetter

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
        self.soil_task = SoilMoistureTask(db_manager=self.database_manager, node_type="validator")
        self.geomagnetic_task = GeomagneticTask(db_manager=self.database_manager)
        self.weights = [0.0] * 256
        self.last_set_weights_block = 0
        self.current_block = 0
        
    def setup_neuron(self) -> bool:
        """
        Set up the neuron with necessary configurations and connections.
        """
        try:
            load_dotenv(".env")
            # Set netuid and chain endpoint
            self.netuid = (
                self.args.netuid if self.args.netuid else int(os.getenv("NETUID", 237))
            )
            logger.info(f"Using netuid: {self.netuid}")

            # Load chain endpoint from args or env
            self.subtensor_chain_endpoint = (
                self.args.subtensor.chain_endpoint
                if hasattr(self.args, "subtensor") and hasattr(
                    self.args.subtensor, "chain_endpoint"
                )
                else os.getenv("SUBSTRATE_CHAIN_ENDPOINT", "wss://test.finney.opentensor.ai:443/")
            )

            # Load wallet and keypair
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

            # Setup substrate interface and metagraph
            self.substrate = SubstrateInterface(url=self.subtensor_chain_endpoint)
            self.metagraph = Metagraph(substrate=self.substrate, netuid=self.netuid)

            #print the entire set of environment variables
            logger.info(f"{os.environ}")

            return True
        except Exception as e:
            logger.error(f"Error setting up neuron: {e}")
            return False

    async def query_miners(self, payload: Any = None, endpoint: str = None):
        """
        Handle the miner querying logic.
        Returns a list of responses from miners.
        """
        self.metagraph.sync_nodes()
        responses = []  # Initialize empty list

        # Convert payload to JSON-serializable format
        if payload and isinstance(payload, dict):
            try:
                # Use custom serializer for JSON dumps
                payload = json.loads(json.dumps(payload, default=self.serialize_datetime))
            except Exception as e:
                logger.error(f"Error serializing payload: {e}")
                return responses

        for miner_hotkey, node in self.metagraph.nodes.items():
            # Construct base URL properly
            base_url = f"https://{node.ip}:{node.port}"
            
            try:
                symmetric_key_str, symmetric_key_uuid = await handshake.perform_handshake(
                    keypair=self.keypair,
                    httpx_client=self.httpx_client,
                    server_address=base_url,
                    miner_hotkey_ss58_address=miner_hotkey,
                )

                if symmetric_key_str and symmetric_key_uuid:
                    logger.info(f"Handshake successful with miner {miner_hotkey}")
                    fernet = Fernet(symmetric_key_str)

                    # Use json.dumps with custom encoder for the payload
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
                    
                    resp.raise_for_status()
                    logger.debug(f"Response from miner {miner_hotkey}: {resp}")
                    logger.debug(f"Response text from miner {miner_hotkey}: {resp.headers}")
                    # Create a dictionary with both response text and metadata
                    response_data = {
                        'text': resp.text,
                        'hotkey': miner_hotkey,
                        'port': node.port,
                        'ip': node.ip
                    }
                    responses.append(response_data)
                    logger.info(f"Request sent to {miner_hotkey}! Response: {response_data}")
                else:
                    logger.warning(f"Failed handshake with miner {miner_hotkey}")

            except Exception as e:
                logger.error(f"Error with miner {miner_hotkey}: {e}")
                logger.error(f"Error details: {traceback.format_exc()}")
                continue  # Continue to next miner on error

        return responses  # Always return the list, even if empty

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
                # Execute tasks in parallel
                workers = [
                    asyncio.create_task(self.geomagnetic_task.validator_execute(self)),
                    asyncio.create_task(self.soil_task.validator_execute(self)),
                    asyncio.create_task(self.status_logger()),
                    asyncio.create_task(self.main_scoring()),  # Add scoring task
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
                # Update current block
                block = self.substrate.get_block()
                self.current_block = block['header']['number']
                
                # Calculate blocks until next scoring round (aligns to 300 blocks, no offset)
                blocks_until_scoring = 300 - (self.current_block % 300)
                
                logger.info(f"Current block: {self.current_block}, Scoring in {blocks_until_scoring} blocks")
                await asyncio.sleep(blocks_until_scoring * 12)
                
                # Sync metagraph and calculate scores
                self.metagraph.sync()
                geomagnetic_scores = await self.database_manager.get_recent_scores('geomagnetic')
                soil_scores = await self.database_manager.get_recent_scores('soil')
                
                # Initialize and calculate weights
                weights = [0.0] * len(self.metagraph.uids)
                for idx, uid in enumerate(self.metagraph.uids):
                    hotkey = self.metagraph.hotkeys[idx]
                    geo_score = geomagnetic_scores.get(hotkey, 0.0)
                    soil_score = soil_scores.get(hotkey, 0.0)
                    aggregate_score = (0.5 * geo_score) + (0.5 * soil_score)
                    weights[idx] = max(0.0, min(1.0, aggregate_score))
                
                # Store weights for later setting
                self.weights = weights
                logger.info("Scores calculated, waiting 50 blocks to set weights")
                
                # Wait 50 blocks before setting weights
                await asyncio.sleep(50 * 12)
                
                # Set weights if enough blocks have passed since last setting
                blocks_since_last = self.current_block - self.last_set_weights_block
                if blocks_since_last >= 250:  # Only set weights if enough blocks have passed
                    success = await self.set_weights(self.weights)
                    if success:
                        logger.info(f"Successfully set weights at block {self.current_block}")
                    else:
                        logger.error(f"Failed to set weights at block {self.current_block}")
                else:
                    logger.warning(f"Skipping weight setting, only {blocks_since_last} blocks since last set")
                
            except Exception as e:
                logger.error(f"Error in main_scoring: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(60)

    async def set_weights(self, weights: List[float]) -> bool:
        """
        Set weights on the chain.
        
        Args:
            weights (List[float]): List of weights aligned with UIDs
            
        Returns:
            bool: True if weights were set successfully, False otherwise
        """
        try:
            weight_setter = FiberWeightSetter(
                netuid=self.netuid,
                wallet_name=self.wallet_name,
                hotkey_name=self.hotkey_name,
                network=self.subtensor_network
            )
            
            await weight_setter.set_weights()
            self.last_set_weights_block = self.current_block
            return True
            
        except Exception as e:
            logger.error(f"Error setting weights: {e}")
            logger.error(traceback.format_exc())
            return False

    async def status_logger(self):
        """Log the status of the validator periodically."""
        while True:
            try:
                current_time_utc = datetime.datetime.now(datetime.timezone.utc)
                formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S")
                
                # Try to get current block, handle connection errors
                try:
                    block = self.substrate.get_block()
                    current_block = block['header']['number']
                    blocks_since_weights = current_block - self.last_set_weights_block
                except Exception as block_error:
                    logger.error(f"Failed to get current block: {block_error}")
                    # Try to reconnect to substrate
                    try:
                        self.substrate = SubstrateInterface(url=self.subtensor_chain_endpoint)
                        logger.info("Successfully reconnected to substrate")
                    except Exception as e:
                        logger.error(f"Failed to reconnect to substrate: {e}")
                    current_block = 0
                    blocks_since_weights = 0
                
                active_nodes = len(self.metagraph.nodes) if self.metagraph else 0
                
                logger.info(f"Status Update:")
                logger.info(f"Current time (UTC): {formatted_time}")
                logger.info(f"Current block: {current_block}")
                logger.info(f"Active nodes: {active_nodes}/256")
                logger.info(f"Last set weights: {blocks_since_weights} blocks ago")
                
            except Exception as e:
                logger.error(f"Error in status logger: {e}")
                logger.error(f'{traceback.format_exc()}')
            finally:
                await asyncio.sleep(60)  # Sleep at the end to ensure we always get the interval
    
    def serialize_datetime(self, obj):
        """Custom JSON serializer for handling datetime objects."""
        if isinstance(obj, (pd.Timestamp, datetime.datetime)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
    
    async def update_miner_table(self):
        """Update the miner table with the latest miner information from the metagraph."""
        try:
            # Ensure metagraph is initialized
            if self.metagraph is None:
                logger.error("Metagraph not initialized")
                return

            # Sync metagraph to get latest node information
            self.metagraph.sync_nodes()
            logger.info(f"Synced {len(self.metagraph.nodes)} nodes from the network")

            # Use enumerate to get the correct index for each node
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
                    protocol=str(node.protocol)
                )
                logger.debug(f"Updated information for node {index}")

            logger.info("Successfully updated miner table")

        except Exception as e:
            logger.error(f"Error updating miner table: {str(e)}")
            logger.error(traceback.format_exc())
            raise


if __name__ == "__main__":
    parser = ArgumentParser()

    # Create a subtensor group
    subtensor_group = parser.add_argument_group("subtensor")

    # Required arguments
    parser.add_argument("--wallet", type=str, help="Name of the wallet to use")
    parser.add_argument("--hotkey", type=str, help="Name of the hotkey to use")
    parser.add_argument("--netuid", type=int, help="Netuid to use")
    subtensor_group.add_argument(
        "--subtensor.chain_endpoint", type=str, help="Subtensor chain endpoint to use"
    )

    # Parse arguments
    args = parser.parse_args()

    validator = GaiaValidator(args)
    asyncio.run(validator.main())
