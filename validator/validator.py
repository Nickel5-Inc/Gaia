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
from tasks.defined_tasks.soilmoisture.soil_task import SoilMoistureTask


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

    async def run_soil_task(self):
        """Run soil moisture task execution loop"""
        while True:
            try:
                await self.soil_task.validator_execute()
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Soil task error: {e}")
                await asyncio.sleep(60)

    async def main(self):
        if not self.setup_neuron():
            logger.error("Failed to setup neuron, exiting...")
            return

        if self.metagraph is None:
            logger.error("Metagraph not initialized, exiting...")
            return

        # Initialize database tables
        await self.database_manager.initialize_database()

        # async def network_worker():
        #     """Handle network operations (miner queries, API calls)"""
        #     while True:
        #         try:
        #             task = await self.database_manager.get_next_task('network')
        #             if task:
        #                 try:
        #                     ssl_context = ssl.create_default_context()
        #                     ssl_context.check_hostname = False
        #                     ssl_context.verify_mode = ssl.CERT_NONE
                            
        #                     async with httpx.AsyncClient(
        #                         timeout=30.0,
        #                         follow_redirects=True,
        #                         verify=ssl_context,
        #                     ) as client:
        #                         if task['process_name'] == 'query_miners':
        #                             await self.query_miners(client, task['payload'])
        #                         elif task['process_name'] == 'sync_metagraph':
        #                             await self.metagraph.sync_nodes()
        #                         elif task['process_name'] == 'fetch_ground_truth':
        #                             await self._fetch_ground_truth(client, task)
                                
        #                         await self.database_manager.complete_task(task['id'])
        #                 except Exception as e:
        #                     await self.database_manager.complete_task(task['id'], str(e))
        #         except Exception as e:
        #             logger.error(f"Network worker error: {e}")
        #         await asyncio.sleep(1)

        # async def compute_worker():
        #     """Handle compute-intensive processes"""
        #     while True:
        #         try:
        #             task = await self.database_manager.get_next_task('compute')
        #             if task:
        #                 try:
        #                     if task['process_name'] == 'score_predictions':
        #                         await self._score_predictions(task)
        #                     elif task['process_name'] == 'process_region_data':
        #                         await self._process_region_data(task)
                            
        #                     await self.database_manager.complete_task(task['id'])
        #                 except Exception as e:
        #                     await self.database_manager.complete_task(task['id'], str(e))
        #         except Exception as e:
        #             logger.error(f"Compute worker error: {e}")
        #         await asyncio.sleep(1)

        # # Start workers
        # workers = [
        #     asyncio.create_task(network_worker()),
        #     asyncio.create_task(compute_worker())
        # ]

        # Main scheduling loop
        while True:
            try:
                current_time = datetime.datetime.now(datetime.timezone.utc)

                # Schedule metagraph sync
                await self.database_manager.add_to_queue(
                    process_type='network',
                    process_name='sync_metagraph',
                    payload=None,
                    priority=1
                )

                # Check for tasks ready for scoring
                for task_type in ['geomagnetic', 'soil_moisture']:
                    pending_tasks = await self.database_manager.get_tasks_ready_for_scoring(
                        task_type=task_type,
                        current_time=current_time
                    )
                    
                    for task in pending_tasks:
                        # Schedule ground truth fetching
                        await self.database_manager.add_to_queue(
                            process_type='network',
                            process_name='fetch_ground_truth',
                            payload={'task_id': task['id']},
                            priority=2
                        )

                # Schedule new process creation if needed
                tasks_count = await self.database_manager.get_active_tasks_count()
                if tasks_count < self.max_concurrent_tasks:
                    await self._schedule_new_task()

                await self.status_logger()
                await asyncio.sleep(300)

            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(300)

        # Wait for workers to complete
        for worker in workers:
            worker.cancel()
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