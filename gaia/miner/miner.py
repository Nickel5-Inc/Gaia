import os
import traceback
from dotenv import load_dotenv
import argparse
from fiber import SubstrateInterface
import uvicorn
from fiber.logging_utils import get_logger
from fiber.miner import server
from fiber.miner.core import configuration
from fiber.miner.middleware import configure_extra_logging_middleware
from fiber.chain import chain_utils
from gaia.miner.utils.subnet import factory_router
from gaia.miner.database.miner_database_manager import MinerDatabaseManager


class Miner:
    """
    Miner class that sets up the neuron and processes tasks.
    """

    def __init__(self, args):
        self.args = args
        self.logger = get_logger(__name__)
        
        # Load environment variables
        load_dotenv("dev.env")
        
        # Load wallet and network settings from args or env
        self.wallet = args.wallet if args.wallet else os.getenv("WALLET_NAME", "default")
        self.hotkey = args.hotkey if args.hotkey else os.getenv("HOTKEY_NAME", "default")
        self.netuid = args.netuid if args.netuid else int(os.getenv("NETUID", 237))
        self.port = args.port if args.port else int(os.getenv("PORT", 8091))
        # Load chain endpoint from args or env
        self.subtensor_chain_endpoint = (
            self.args.subtensor.chain_endpoint
            if hasattr(self.args, "subtensor") and hasattr(self.args.subtensor, "chain_endpoint")
            else os.getenv("SUBSTRATE_CHAIN_ENDPOINT", "wss://test.finney.opentensor.ai:443/")
        )
        
        self.subtensor_network = (
            self.args.subtensor.network 
            if hasattr(self.args, "subtensor") and hasattr(self.args.subtensor, "network")
            else os.getenv("SUBTENSOR_NETWORK", "test")
        )
        
        self.database_manager = MinerDatabaseManager()

    def setup_neuron(self) -> bool:
        """
        Set up the miner neuron with necessary configurations and connections.

        Returns:
            bool: True if setup is successful, False otherwise.
        """



        # Add neuron setup logic here if needed
        self.logger.info("Setting up miner neuron...")

        

        self.keypair = chain_utils.load_hotkey_keypair(
            self.wallet, self.hotkey
        )
        self.logger.info(f"Keypair: {self.keypair}")
        self.logger.info(f"Subtensor chain endpoint: {self.subtensor_chain_endpoint}")
        
        #print current env (for debugging)
        self.logger.info(f"Current env: {os.environ}")
        #Check config
        self.config = configuration.factory_config()
        self.logger.info(f"Config: {self.config}")

        return True

    def run(self):
        """
        Run the miner application with a FastAPI server.
        """
        try:
            if not self.setup_neuron():
                self.logger.error("Failed to setup neuron")
                return
            self.logger.info("Starting miner server...")
            app = server.factory_app(debug=True)
            app.include_router(factory_router())
            # Use the port argument from the CLI/env
            uvicorn.run(app, host="0.0.0.0", port=self.port)
        except Exception as e:
            self.logger.error(f"Error starting miner: {e}")
            self.logger.error(traceback.format_exc())
            raise e

        while True:
            # Main miner loop for processing tasks
            # Listen to routes for new tasks and process them
            pass


if __name__ == "__main__":
    # Add arguments
    parser = argparse.ArgumentParser(description="Start the miner with optional flags.")

    # Create a subtensor group
    subtensor_group = parser.add_argument_group("subtensor")

    # Wallet and network arguments
    parser.add_argument("--wallet", type=str, help="Name of the wallet to use")
    parser.add_argument("--hotkey", type=str, help="Name of the hotkey to use")
    parser.add_argument("--netuid", type=int, help="Netuid to use")

    # Optional arguments
    parser.add_argument(
        "--port", type=int, default=8091, help="Port to run the miner on"
    )
    parser.add_argument(
        "--use_base_model", action="store_true", help="Enable base model usage"
    )

    # Subtensor arguments
    subtensor_group.add_argument(
        "--subtensor.chain_endpoint", type=str, help="Subtensor chain endpoint to use"
    )
    subtensor_group.add_argument(
        "--subtensor.network", type=str, default="test", help="Subtensor network to use"
    )

    # Parse arguments and start the miner
    args = parser.parse_args()
    miner = Miner(args)
    miner.run()
