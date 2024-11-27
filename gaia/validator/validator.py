import datetime
import os
import asyncio
import ssl
import traceback
from typing import Any, Optional, List, Dict
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
import base64

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
            transport=httpx.AsyncHTTPTransport(
                retries=3
            ),
        )
        
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

    def custom_serializer(self, obj):
        """Custom JSON serializer for handling datetime objects and bytes."""
        if isinstance(obj, (pd.Timestamp, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            # For bytes, return a dictionary with encoding information
            return {
                '_type': 'bytes',
                'encoding': 'base64',
                'data': base64.b64encode(obj).decode('ascii')
            }
        raise TypeError(f"Type {type(obj)} not serializable")

    async def query_miners(self, payload: Dict, endpoint: str) -> Dict:
        """Query miners with the given payload."""
        try:
            logger.info(f"Querying miners with payload size: {len(str(payload))} bytes")
            if 'data' in payload and 'combined_data' in payload['data']:
                logger.debug(f"TIFF data size before serialization: {len(payload['data']['combined_data'])} bytes")
                if isinstance(payload['data']['combined_data'], bytes):
                    logger.debug(f"TIFF header before serialization: {payload['data']['combined_data'][:4]}")

            
            responses = {}
            self.metagraph.sync_nodes()

            for miner_hotkey, node in self.metagraph.nodes.items():
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

                        # Pass the original payload dict
                        resp = await vali_client.make_non_streamed_post(
                            httpx_client=self.httpx_client,
                            server_address=base_url,
                            fernet=fernet,
                            keypair=self.keypair,
                            symmetric_key_uuid=symmetric_key_uuid,
                            validator_ss58_address=self.keypair.ss58_address,
                            miner_ss58_address=miner_hotkey,
                            payload=payload,  # Pass the original dict
                            endpoint=endpoint,
                        )
                        
                        #resp.raise_for_status()
                        #logger.debug(f"Response from miner {miner_hotkey}: {resp}")
                        #logger.debug(f"Response text from miner {miner_hotkey}: {resp.headers}")
                        # Create a dictionary with both response text and metadata
                        response_data = {
                            'text': resp.text,
                            'hotkey': miner_hotkey,
                            'port': node.port,
                            'ip': node.ip
                        }
                        responses[miner_hotkey] = response_data
                        logger.info(f"Completed request to {miner_hotkey}")
                    else:
                        logger.warning(f"Failed handshake with miner {miner_hotkey}")
                except httpx.HTTPStatusError as e:
                    logger.warning(f"HTTP error from miner {miner_hotkey}: {e}")
                    #logger.debug(f"Error details: {traceback.format_exc()}")
                    continue  # Continue to next miner on error

                except httpx.RequestError as e:
                    logger.warning(f"Request error from miner {miner_hotkey}: {e}")
                    #logger.debug(f"Error details: {traceback.format_exc()}")
                    continue  # Continue to next miner on error

                except Exception as e:
                    logger.error(f"Error with miner {miner_hotkey}: {e}")
                    logger.error(f"Error details: {traceback.format_exc()}")
                    continue  # Continue to next miner on error
            

            return responses  # Always return the list, even if empty

        except Exception as e:
            logger.error(f"Error in query_miners: {str(e)}")
            return {}

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
                    asyncio.create_task(self.main_scoring()),
                    asyncio.create_task(self.handle_miner_deregistration_loop())
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
                self.metagraph.sync_nodes()
                geomagnetic_scores = await self.database_manager.get_recent_scores('geomagnetic')
                soil_scores = await self.database_manager.get_recent_scores('soil')
                
                # Calculate weights
                weights = [0.0] * 256
                for idx in range(256):
                    aggregate_score = (0.5 * -geomagnetic_scores[idx]) + (0.5 * soil_scores[idx]) # invert geomagnetic scores as higher is worse
                    weights[idx] = aggregate_score
                
                # Normalize weights to sum to 1
                total_weight = sum(weights)
                if total_weight > 0:
                    weights = [weight / total_weight for weight in weights]
                
                # Sort indices by weight for ranking
                sorted_indices = sorted(range(len(weights)), key=lambda k: weights[k], reverse=True)
                
                # Apply sigmoid transformation
                def sigmoid_transform(x, steepness=20):  # Increased steepness from 10 to 20
                    """
                    Transform normalized rank (0-1) to weight using sigmoid.
                    Adjusted steepness to better approximate 80/20 rule:
                    - steepness=10 -> top 20% get ~70% of weight
                    - steepness=20 -> top 20% get ~80% of weight
                    - steepness=30 -> top 20% get ~85% of weight
                    """
                    import math
                    return 1 / (1 + math.exp(-steepness * (x - 0.5)))

                # Calculate new weights based on rank position
                new_weights = [0.0] * len(weights)
                for rank, idx in enumerate(sorted_indices):
                    normalized_rank = 1.0 - (rank / len(weights))
                    new_weights[idx] = sigmoid_transform(normalized_rank)
                
                # Normalize to ensure sum = 1
                total = sum(new_weights)
                self.weights = [w / total for w in new_weights]
                
                # Log distribution stats
                top_20_weight = sum(sorted(self.weights, reverse=True)[:int(len(weights)*0.2)])
                logger.info(f"Weight distribution: top 20% of nodes hold {top_20_weight*100:.1f}% of total weight")
                
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
            
            success = await weight_setter.set_weights()
            if success:
                # Update last_set_weights_block only on successful weight setting
                self.last_set_weights_block = self.current_block
                logger.info(f"Successfully set weights at block {self.current_block}")
            return success
            
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
                    self.current_block = block['header']['number']
                    blocks_since_weights = (
                        self.current_block - self.last_set_weights_block 
                        if self.last_set_weights_block > 0 
                        else 0
                    )
                except Exception as block_error:
                    #logger.warning(f"Failed to get current block: {block_error}")
                    # Try to reconnect to substrate
                    try:
                        self.substrate = SubstrateInterface(url=self.subtensor_chain_endpoint)
                        #logger.info("Successfully reconnected to substrate")
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
                logger.error(f'{traceback.format_exc()}')
            finally:
                await asyncio.sleep(60)
    
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
                # Update in-memory state
                self.nodes[index] = {'hotkey': node.hotkey, 'uid': index}
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
                # Sync metagraph to get current network state
                self.metagraph.sync_nodes()
                
                # Create mapping of current network state
                active_miners = {
                    idx: {'hotkey': hotkey, 'uid': idx} 
                    for idx, (hotkey, _) in enumerate(self.metagraph.nodes.items())
                }
                
                # Check for changes in the node table
                if not self.nodes:
                    # Initial load of node table state
                    query = "SELECT uid, hotkey FROM node_table WHERE hotkey IS NOT NULL"
                    rows = await self.database_manager.fetch_many(query)
                    self.nodes = {row['uid']: {'hotkey': row['hotkey'], 'uid': row['uid']} for row in rows}
                
                # Find deregistered miners by comparing hotkeys at each UID
                deregistered_miners = []
                for uid, registered in self.nodes.items():
                    # If UID has a different hotkey now, miner was deregistered
                    if active_miners[uid]['hotkey'] != registered['hotkey']:
                        deregistered_miners.append(registered)
                
                if deregistered_miners:
                    logger.info(f"Found {len(deregistered_miners)} deregistered miners:")
                    for miner in deregistered_miners:
                        logger.info(f"UID {miner['uid']}: {miner['hotkey']} -> {active_miners[miner['uid']]['hotkey']}")
                    
                    # Update in-memory node table state
                    for miner in deregistered_miners:
                        self.nodes[miner['uid']]['hotkey'] = active_miners[miner['uid']]['hotkey']
                    
                    # Get list of UIDs for recalculation
                    uids = [int(miner['uid']) for miner in deregistered_miners]
                    
                    # Set weights to 0 for deregistered miners
                    for idx in uids:
                        self.weights[idx] = 0.0
                        
                    # Recalculate scores for both tasks
                    await self.soil_task.recalculate_recent_scores(uids)
                    await self.geomagnetic_task.recalculate_recent_scores(uids)
                    
                    logger.info(f"Processed {len(deregistered_miners)} deregistered miners")
                else:
                    logger.debug("No deregistered miners found")
                
            except Exception as e:
                logger.error(f"Error in deregistration loop: {e}")
                logger.error(traceback.format_exc())
            finally:
                await asyncio.sleep(60)

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
