import datetime
import os
import asyncio
import ssl
from typing import Any, Optional
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import httpx
from fiber.chain import chain_utils
from fiber.logging_utils import get_logger
from fiber.validator import client as vali_client, handshake
from fiber.chain.metagraph import Metagraph
from substrateinterface import SubstrateInterface
from gaia.tasks.defined_tasks.geomagnetic.geomagnetic_task import GeomagneticTask
from gaia.tasks.defined_tasks.geomagnetic.utils.process_geomag_data import get_latest_geomag_data
from gaia.validator.database.validator_database_manager import ValidatorDatabaseManager
from gaia.tasks.base.components.inputs import Inputs
from gaia.tasks.base.components.outputs import Outputs
from argparse import ArgumentParser
from gaia.tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask


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
        self.weights = None # TODO: Implement weights
        self.soil_task = SoilMoistureTask()
        #self.geomagnetic_task = GeomagneticTask()

    
    def setup_neuron(self) -> bool:
        """
        Set up the neuron with necessary configurations and connections.
        This method is responsible for:
        - Loading environment variables
        - Setting up the netuid and chain endpoint
        - Loading the wallet and keypair
        - Initializing the substrate interface and metagraph
        """
        try:
            load_dotenv(".env")
            # Set netuid and chain endpoint
            self.netuid = self.args.netuid if self.args.netuid else int(os.getenv("NETUID", 1))
            
            # Load chain endpoint from args or env
            if hasattr(self.args, 'subtensor') and hasattr(self.args.subtensor, 'chain_endpoint'):
                self.subtensor_chain_endpoint = self.args.subtensor.chain_endpoint
            else:
                self.subtensor_chain_endpoint = os.getenv("SUBSTRATE_CHAIN_ENDPOINT", "wss://test.finney.opentensor.ai:443/")

            # Load wallet and keypair
            self.wallet_name = self.args.wallet if self.args.wallet else os.getenv("WALLET_NAME", "default")
            self.hotkey_name = self.args.hotkey if self.args.hotkey else os.getenv("HOTKEY_NAME", "default")
            self.keypair = chain_utils.load_hotkey_keypair(self.wallet_name, self.hotkey_name)

            # Setup substrate interface and metagraph
            self.substrate = SubstrateInterface(url=self.subtensor_chain_endpoint)
            self.metagraph = Metagraph(substrate=self.substrate, netuid=self.netuid)

            return True

        except Exception as e:
            logger.error(f"Error setting up neuron: {e}")
            return False

    async def query_miners(self, payload: Optional[Inputs] = None, endpoint: str = None) -> Outputs:
        """Handle the miner querying logic"""
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

                    # Payload and endpoint should be passed in from the validator_execute() method


                    # timestamp, dst_value = get_latest_geomag_data()
                    # payload = {
                    #     "nonce": "12345",
                    #     "data": {
                    #         "name": "Geomagnetic data",
                    #         "timestamp": str(timestamp),
                    #         "value": dst_value,
                    #     }
                    # }

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
                        endpoint=endpoint
                    )
                    resp.raise_for_status()
                    logger.info(f"Geomagnetic request sent to {miner_hotkey}! Response: {resp.text}")
                else:
                    logger.warning(f"Failed handshake with miner {miner_hotkey}")

            except Exception as e:
                logger.error(f"Error with miner {miner_hotkey}: {e}")

  

    

    async def main(self):
        if not self.setup_neuron():
            logger.error("Failed to setup neuron, exiting...")
            return

        if self.metagraph is None:
            logger.error("Metagraph not initialized, exiting...")
            return

        # Initialize database tables
        await self.database_manager.initialize_database()

        # Create a custom SSL context that ignores hostname mismatches
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE  # This is necessary for self-signed certs

        self.httpx_client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            verify=False
        )

        while True:
            try:
                current_time = datetime.datetime.now(datetime.timezone.utc)

                #schedule each task execution loop in a separate thread
                # NOTE: this is a beta implementation that will be replaced with a more sophisticated scheduler soon
                workers = [
                    asyncio.create_task(self.soil_task.validator_execute()),
                    asyncio.create_task(self.geomagnetic_task.validator_execute())
                ]


                await self.status_logger()
                await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(300)
            finally:
                await asyncio.gather(*workers, return_exceptions=True)

     
    async def status_logger(self):
        """Log the status of the validator."""
        current_time_utc = datetime.datetime.now(datetime.timezone.utc)
        formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Current time (UTC): {formatted_time}")

if __name__ == "__main__":
    parser = ArgumentParser()
    
    # Create a subtensor group
    subtensor_group = parser.add_argument_group('subtensor')
    
    # Required arguments
    parser.add_argument("--wallet", type=str, help="Name of the wallet to use")
    parser.add_argument("--hotkey", type=str, help="Name of the hotkey to use")
    parser.add_argument("--netuid", type=int, help="Netuid to use")
    subtensor_group.add_argument("--subtensor.chain_endpoint", type=str, help="Subtensor chain endpoint to use")

    # Parse arguments
    args = parser.parse_args()

    validator = GaiaValidator(args)
    asyncio.run(validator.main())