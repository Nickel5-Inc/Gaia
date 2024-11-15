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
from tasks.defined_tasks.geomagnetic.utils.process_geomag_data import get_latest_geomag_data
from validator.database.validator_database_manager import ValidatorDatabaseManager
from tasks.base.components.inputs import Inputs
from tasks.base.components.outputs import Outputs
from argparse import ArgumentParser

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

    async def query_miners(self, httpx_client, payload: Optional[Inputs] = None) -> Outputs:
        """Handle the miner querying logic"""
        self.metagraph.sync_nodes()

        for miner_hotkey, node in self.metagraph.nodes.items():
            base_url = f"https://{node.ip}:{node.port}"

            try:
                symmetric_key_str, symmetric_key_uuid = await handshake.perform_handshake(
                    keypair=self.keypair,
                    httpx_client=httpx_client,
                    server_address=base_url,
                    miner_hotkey_ss58_address=miner_hotkey,
                )

                if symmetric_key_str and symmetric_key_uuid:
                    logger.info(f"Handshake successful with miner {miner_hotkey}")

                    timestamp, dst_value = get_latest_geomag_data()
                    payload = {
                        "nonce": "12345",
                        "data": {
                            "name": "Geomagnetic data",
                            "timestamp": str(timestamp),
                            "value": dst_value,
                        }
                    }

                    fernet = Fernet(symmetric_key_str)

                    resp = await vali_client.make_non_streamed_post(
                        httpx_client=httpx_client,
                        server_address=base_url,
                        fernet=fernet,
                        keypair=self.keypair,
                        symmetric_key_uuid=symmetric_key_uuid,
                        validator_ss58_address=self.keypair.ss58_address,
                        miner_ss58_address=miner_hotkey,
                        payload=payload,
                        endpoint="/geomagnetic-request"
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

        while True:  # Main loop
            try:

                #START SERVER


                #PROCESS HANDLING


                # Check process queue for processes to execute



                # Execute processes
                



                # Schedule new processes, if necessary







                await self.status_logger()

                logger.info("Starting new iteration of miner queries...")
                
                # Create a custom SSL context that ignores hostname mismatches
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE  # This is necessary for self-signed certs
                
                async with httpx.AsyncClient(
                    timeout=30.0,
                    follow_redirects=True,
                    verify=ssl_context,
                ) as httpx_client:
                    await self.query_miners(httpx_client)
                
                logger.info("Completed iteration. Waiting 5 minutes before next round...")
                await asyncio.sleep(300)  # 5 minutes = 300 seconds
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                logger.info("Will retry in 5 minutes...")
                await asyncio.sleep(300)

    async def status_logger(self):
        """
        Log the status of the validator.

        
        """
        current_time_utc = datetime.datetime.now(datetime.timezone.utc)
        formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S")



        # 
        logger.info(f"Current time (UTC): {formatted_time}")

        pass

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